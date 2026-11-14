using System.Threading.RateLimiting;
using PureGate.WebSocket.Diagnostics;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.Serialization;

namespace PureGate.WebSocket.Hosting;

enum OperationKind
{
  Unknown,
  Request,
  ClientEvent,
  Stream,
  Upload,
}

// Something a browser can call or send by name. Start runs on the receive loop: it reads the payload while the
// message is still in the buffer, then starts the handler, which the receive loop runs once the message is
// processed (WebSocketConnection.RunHandler).
abstract class Operation(string name, OperationKind kind)
{
  // What every name without a handler resolves to, so the name table never keeps a client's text.
  public static readonly Operation Unknown = new UnknownOperation();

  public string Name { get; } = name;

  public OperationKind Kind { get; } = kind;

  // What the browser sends and gets back, for the contract. NoPayload means nothing.
  public virtual Type RequestType => typeof(NoPayload);

  public virtual Type ResponseType => typeof(NoPayload);

  // The authorization policies every call must pass, "" for the default one; null for none.
  public string[] Policies { get; set; }

  // From OperationBuilder.AddFilter, outermost first; null for none.
  public List<WebSocketFilter> Filters { get; set; }

  // Whether calls go through authorization or filters before the handler. Without either, they go straight to it.
  public bool HasPipeline => Policies is not null || Filters is not null;

  public abstract void Start(WebSocketConnection connection, scoped in Frame frame);

  // Authorization, then the filters in the order they were added, then the handler. A refused call fails with
  // unauthorized or forbidden, and its handler never runs.
  public async ValueTask RunPipelineAsync(WebSocketConnection connection, WebSocketCall call, Func<ValueTask> handler)
  {
    if (Policies is not null && await connection.AuthorizeAsync(Policies) is { } refused)
      throw refused;

    if (Filters is null)
    {
      await handler();
      return;
    }

    await Next(0);

    ValueTask Next(int index) => index == Filters.Count ? handler() : Filters[index](call, () => Next(index + 1));
  }

  sealed class UnknownOperation() : Operation("", OperationKind.Unknown)
  {
    public override void Start(WebSocketConnection connection, scoped in Frame frame)
    {
    }
  }
}

sealed class RequestOperation<TRequest, TResponse>(
    string name,
    PayloadCodec<TRequest>[] request,
    PayloadCodec<TResponse>[] response,
    Func<TRequest, WebSocketCall, CancellationToken, ValueTask<TResponse>> handler)
  : Operation(name, OperationKind.Request)
{
  // By the connection's format (EndpointState.Formats).
  public PayloadCodec<TRequest>[] Request { get; } = request;

  public PayloadCodec<TResponse>[] Response { get; } = response;

  public override Type RequestType => typeof(TRequest);

  public override Type ResponseType => typeof(TResponse);

  public override void Start(WebSocketConnection connection, scoped in Frame frame)
  {
    TRequest value;
    try
    {
      value = Request[connection.Format].Read(connection.PayloadOf(frame.Payload), frame.Has(FrameFlags.Binary));
    }
    catch (InvalidPayloadException exception)
    {
      connection.RejectCall(frame.Id, ErrorCode.InvalidPayload, exception.Message);
      return;
    }

    var call = connection.BeginCall(this, in frame);
    connection.RunHandler(new Run(this, connection, call, value));
  }

  sealed class Run(
      RequestOperation<TRequest, TResponse> operation,
      WebSocketConnection connection,
      WebSocketCall call,
      TRequest value) : IThreadPoolWorkItem
  {
    public void Execute() => _ = RunAsync();

    async Task RunAsync()
    {
      call.Activity = Telemetry.StartActivity(operation.Name, call.TraceParent, connection.Id);
      TResponse result;
      try
      {
        result = operation.HasPipeline ? await RunPipelineAsync() : await operation.Invoke(value, call);
      }
      catch (Exception exception)
      {
        await connection.FailCallAsync(call, exception);
        return;
      }

      await connection.CompleteCallAsync(call, operation.Response[connection.Format], result);
    }

    // Apart, so the closure its result needs is created only for operations with a pipeline.
    async ValueTask<TResponse> RunPipelineAsync()
    {
      TResponse result = default;
      await operation.RunPipelineAsync(connection, call, async () => result = await operation.Invoke(value, call));
      return result;
    }
  }

  ValueTask<TResponse> Invoke(TRequest value, WebSocketCall call) => handler(value, call, call.CancellationToken);
}

sealed class ClientEventOperation<T>(
    string name,
    PayloadCodec<T>[] codecs,
    Func<T, WebSocketCall, CancellationToken, ValueTask> handler)
  : Operation(name, OperationKind.ClientEvent)
{
  public PayloadCodec<T>[] Codecs { get; } = codecs;

  public override Type RequestType => typeof(T);

  Func<T, WebSocketCall, CancellationToken, ValueTask> Handler { get; } = handler;

  public override void Start(WebSocketConnection connection, scoped in Frame frame)
  {
    T value;
    try
    {
      value = Codecs[connection.Format].Read(connection.PayloadOf(frame.Payload), frame.Has(FrameFlags.Binary));
    }
    catch (InvalidPayloadException exception)
    {
      connection.DropEvent(Name, exception.Message);
      return;
    }

    connection.QueueEvent(new Work(this, value, connection.ReadTrace(in frame), connection.TakeLease()));
  }

  sealed class Work(ClientEventOperation<T> operation, T value, string traceParent, RateLimitLease lease) : EventWork
  {
    public override async Task RunAsync(WebSocketConnection connection)
    {
      var call = connection.BeginEvent(operation.Name, traceParent, lease);
      using var activity = Telemetry.StartActivity(operation.Name, traceParent, connection.Id);
      try
      {
        if (operation.HasPipeline)
          await RunPipelineAsync(connection, call);
        else
          await operation.Handler(value, call, call.CancellationToken);
      }
      catch (CallException exception) when (exception.Code is "unauthorized" or "forbidden")
      {
        activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, exception.Code);
        connection.EventRefused(operation.Name);
      }
      catch (Exception exception) when (!call.CancellationToken.IsCancellationRequested || exception is not OperationCanceledException)
      {
        activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, "handler_failed");
        connection.EventFailed(operation.Name, exception);
      }
      catch (OperationCanceledException)
      {
      }
      finally
      {
        await call.DisposeAsync();
      }
    }

    // Apart, so the closure over the call is created only for events with a pipeline.
    ValueTask RunPipelineAsync(WebSocketConnection connection, WebSocketCall call) =>
      operation.RunPipelineAsync(connection, call, () => operation.Handler(value, call, call.CancellationToken));
  }
}

// A stream the browser opens and the server produces. The producer is pulled only as fast as the browser grants
// credit: each item waits for credit before the enumerator moves to it.
sealed class StreamOperation<TRequest, TItem>(
    string name,
    PayloadCodec<TRequest>[] request,
    PayloadCodec<TItem>[] item,
    Func<TRequest, WebSocketCall, CancellationToken, IAsyncEnumerable<TItem>> handler)
  : Operation(name, OperationKind.Stream)
{
  public PayloadCodec<TItem>[] Item { get; } = item;

  Func<TRequest, WebSocketCall, CancellationToken, IAsyncEnumerable<TItem>> Handler { get; } = handler;

  public override Type RequestType => typeof(TRequest);

  // For the contract, a stream's result is its item type.
  public override Type ResponseType => typeof(TItem);

  public override void Start(WebSocketConnection connection, scoped in Frame frame)
  {
    TRequest value;
    try
    {
      value = request[connection.Format].Read(connection.PayloadOf(frame.Payload), frame.Has(FrameFlags.Binary));
    }
    catch (InvalidPayloadException exception)
    {
      connection.RejectStream(in frame, ErrorCode.InvalidPayload, exception.Message);
      return;
    }

    var stream = connection.BeginDownload(this, in frame);
    connection.RunHandler(new Run(this, connection, stream, value));
  }

  sealed class Run(
      StreamOperation<TRequest, TItem> operation,
      WebSocketConnection connection,
      DownloadState stream,
      TRequest value) : IThreadPoolWorkItem
  {
    public void Execute() => _ = RunAsync();

    async Task RunAsync()
    {
      stream.Call.Activity = Telemetry.StartActivity(operation.Name, stream.Call.TraceParent, connection.Id);
      Exception failure = null;
      try
      {
        if (operation.HasPipeline)
          await operation.RunPipelineAsync(connection, stream.Call, ProduceAsync);
        else
          await ProduceAsync();
      }
      catch (Exception exception)
      {
        failure = exception;
      }

      await connection.EndDownloadAsync(stream, failure);
    }

    async ValueTask ProduceAsync()
    {
      var token = stream.Call.CancellationToken;
      var codec = operation.Item[connection.Format];
      await using var items = operation.Handler(value, stream.Call, token).GetAsyncEnumerator(token);
      while (true)
      {
        await stream.WaitForCreditAsync(token);
        if (!await items.MoveNextAsync())
          break;

        if (!connection.WriteStreamItem(stream, codec, items.Current))
          break;
      }
    }
  }
}

// A stream the browser sends, read by a handler that returns one result.
sealed class UploadOperation<TRequest, TItem, TResponse>(
    string name,
    PayloadCodec<TRequest>[] request,
    PayloadCodec<TItem>[] item,
    PayloadCodec<TResponse>[] response,
    Func<TRequest, IAsyncEnumerable<TItem>, WebSocketCall, CancellationToken, ValueTask<TResponse>> handler)
  : Operation(name, OperationKind.Upload), IUploadOperation
{
  public PayloadCodec<TResponse>[] Response { get; } = response;

  Func<TRequest, IAsyncEnumerable<TItem>, WebSocketCall, CancellationToken, ValueTask<TResponse>> Handler { get; } = handler;

  public override Type RequestType => typeof(TRequest);

  public override Type ResponseType => typeof(TResponse);

  public Type ItemType => typeof(TItem);

  public override void Start(WebSocketConnection connection, scoped in Frame frame)
  {
    TRequest value;
    try
    {
      value = request[connection.Format].Read(connection.PayloadOf(frame.Payload), frame.Has(FrameFlags.Binary));
    }
    catch (InvalidPayloadException exception)
    {
      connection.RejectStream(in frame, ErrorCode.InvalidPayload, exception.Message);
      return;
    }

    var upload = connection.BeginUpload(this, in frame, item[connection.Format]);
    connection.RunHandler(new Run(this, connection, upload, value));
  }

  sealed class Run(
      UploadOperation<TRequest, TItem, TResponse> operation,
      WebSocketConnection connection,
      UploadState<TItem> upload,
      TRequest value) : IThreadPoolWorkItem
  {
    public void Execute() => _ = RunAsync();

    async Task RunAsync()
    {
      upload.Call.Activity = Telemetry.StartActivity(operation.Name, upload.Call.TraceParent, connection.Id);
      TResponse result;
      try
      {
        result = operation.HasPipeline ? await RunPipelineAsync() : await HandleAsync();
      }
      catch (Exception exception)
      {
        await connection.FailUploadAsync(upload, exception);
        return;
      }

      await connection.CompleteUploadAsync(upload, operation.Response[connection.Format], result);
    }

    // Apart, so the closure its result needs is created only for uploads with a pipeline.
    async ValueTask<TResponse> RunPipelineAsync()
    {
      TResponse result = default;
      await operation.RunPipelineAsync(connection, upload.Call, async () => result = await HandleAsync());
      return result;
    }

    // Credit starts when the handler does: an upload its authorization refuses never takes an item.
    ValueTask<TResponse> HandleAsync()
    {
      var token = upload.Call.CancellationToken;
      connection.GrantUploadCredit(upload, (uint)connection.Options.UploadWindow);
      return operation.Handler(value, upload.ReadAsync(token), upload.Call, token);
    }
  }
}

// What the contract needs to know of an upload besides its request and result.
interface IUploadOperation
{
  Type ItemType { get; }
}

// One event waiting for, or running in, its handler.
abstract class EventWork : IThreadPoolWorkItem
{
  // Set when the event starts on its own, under EventOrdering.Concurrent.
  public WebSocketConnection Connection;

  // Runs the handler. Never throws: failures go to the log.
  public abstract Task RunAsync(WebSocketConnection connection);

  void IThreadPoolWorkItem.Execute() => _ = RunAsync(Connection);
}

// What the contract needs to know of something the server sends: its name and payload types.
interface IOutgoingMember
{
  string Name { get; }

  Type RequestType { get; }

  Type ResponseType { get; }
}

// An event from another server, which the hub knows only by its name until the event's type takes over.
interface IRelayedEvent
{
  // False when its payload could not be read for some of the connections.
  bool Relay(WebSocketHub hub, in BackplaneMessage read, ReadOnlyMemory<byte> payload);
}

// The browser-facing side of a ServerEvent: its name and how its payload is written.
sealed class ServerEventInfo<T>(ServerEvent<T> descriptor, PayloadCodec<T>[] codecs) : IOutgoingMember, IRelayedEvent
{
  public ServerEvent<T> Descriptor { get; } = descriptor;

  // By the connection's format.
  public PayloadCodec<T>[] Codecs { get; } = codecs;

  public string Name => Descriptor.Name;

  public Type RequestType => typeof(T);

  public Type ResponseType => typeof(NoPayload);

  public bool Relay(WebSocketHub hub, in BackplaneMessage read, ReadOnlyMemory<byte> payload) => hub.Relay(this, in read, payload);
}

// The browser-facing side of a ClientRequest: its name and how its payloads are read and written.
sealed class ClientRequestInfo<TRequest, TResponse>(
    ClientRequest<TRequest, TResponse> descriptor,
    PayloadCodec<TRequest>[] request,
    PayloadCodec<TResponse>[] response) : IOutgoingMember
{
  public ClientRequest<TRequest, TResponse> Descriptor { get; } = descriptor;

  // By the connection's format.
  public PayloadCodec<TRequest>[] Request { get; } = request;

  public PayloadCodec<TResponse>[] Response { get; } = response;

  public string Name => Descriptor.Name;

  public Type RequestType => typeof(TRequest);

  public Type ResponseType => typeof(TResponse);
}
