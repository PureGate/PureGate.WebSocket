using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.Serialization;

namespace PureGate.WebSocket.Hosting;

enum OperationKind
{
  Unknown,
  Request,
  ClientEvent,
}

// Something a browser can call or send by name. Start runs on the receive loop: it reads the payload while the
// message is still in the buffer, then hands the handler to the thread pool.
abstract class Operation(string name, OperationKind kind)
{
  // What every name without a handler resolves to, so the name table never keeps a client's text.
  public static readonly Operation Unknown = new UnknownOperation();

  public string Name { get; } = name;

  public OperationKind Kind { get; } = kind;

  // What the browser sends and gets back, for the contract. NoPayload means nothing.
  public virtual Type RequestType => typeof(NoPayload);

  public virtual Type ResponseType => typeof(NoPayload);

  public abstract void Start(WebSocketConnection connection, scoped in Frame frame);

  sealed class UnknownOperation() : Operation("", OperationKind.Unknown)
  {
    public override void Start(WebSocketConnection connection, scoped in Frame frame)
    {
    }
  }
}

sealed class RequestOperation<TRequest, TResponse>(
    string name,
    PayloadCodec<TRequest> request,
    PayloadCodec<TResponse> response,
    Func<TRequest, WebSocketCall, CancellationToken, ValueTask<TResponse>> handler)
  : Operation(name, OperationKind.Request)
{
  public PayloadCodec<TRequest> Request { get; } = request;

  public PayloadCodec<TResponse> Response { get; } = response;

  public override Type RequestType => typeof(TRequest);

  public override Type ResponseType => typeof(TResponse);

  public override void Start(WebSocketConnection connection, scoped in Frame frame)
  {
    TRequest value;
    try
    {
      value = Request.Read(frame.Payload, frame.Has(FrameFlags.Binary));
    }
    catch (InvalidPayloadException exception)
    {
      connection.RejectCall(frame.Id, ErrorCode.InvalidPayload, exception.Message);
      return;
    }

    var call = connection.BeginCall(this, in frame);
    ThreadPool.UnsafeQueueUserWorkItem(new Run(this, connection, call, value), preferLocal: false);
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
      TResponse result;
      try
      {
        result = await operation.Invoke(value, call);
      }
      catch (Exception exception)
      {
        await connection.FailCallAsync(call, exception);
        return;
      }

      await connection.CompleteCallAsync(call, operation.Response, result);
    }
  }

  ValueTask<TResponse> Invoke(TRequest value, WebSocketCall call) => handler(value, call, call.CancellationToken);
}

sealed class ClientEventOperation<T>(
    string name,
    PayloadCodec<T> codec,
    Func<T, WebSocketCall, CancellationToken, ValueTask> handler)
  : Operation(name, OperationKind.ClientEvent)
{
  public PayloadCodec<T> Codec { get; } = codec;

  public override Type RequestType => typeof(T);

  Func<T, WebSocketCall, CancellationToken, ValueTask> Handler { get; } = handler;

  public override void Start(WebSocketConnection connection, scoped in Frame frame)
  {
    T value;
    try
    {
      value = Codec.Read(frame.Payload, frame.Has(FrameFlags.Binary));
    }
    catch (InvalidPayloadException exception)
    {
      connection.DropEvent(Name, exception.Message);
      return;
    }

    connection.QueueEvent(new Work(this, value, connection.ReadTrace(in frame)));
  }

  sealed class Work(ClientEventOperation<T> operation, T value, string traceParent) : EventWork
  {
    public override async Task RunAsync(WebSocketConnection connection)
    {
      var call = connection.BeginEvent(operation.Name, traceParent);
      try
      {
        await operation.Handler(value, call, call.CancellationToken);
      }
      catch (Exception exception) when (!call.CancellationToken.IsCancellationRequested || exception is not OperationCanceledException)
      {
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
  }
}

// One event waiting for, or running in, its handler.
abstract class EventWork
{
  // Runs the handler. Never throws: failures go to the log.
  public abstract Task RunAsync(WebSocketConnection connection);
}

// What the contract needs to know of something the server sends: its name and payload types.
interface IOutgoingMember
{
  string Name { get; }

  Type RequestType { get; }

  Type ResponseType { get; }
}

// The browser-facing side of a ServerEvent: its name and how its payload is written.
sealed class ServerEventInfo<T>(ServerEvent<T> descriptor, PayloadCodec<T> codec) : IOutgoingMember
{
  public ServerEvent<T> Descriptor { get; } = descriptor;

  public PayloadCodec<T> Codec { get; } = codec;

  public string Name => Descriptor.Name;

  public Type RequestType => typeof(T);

  public Type ResponseType => typeof(NoPayload);
}

// The browser-facing side of a ClientRequest: its name and how its payloads are read and written.
sealed class ClientRequestInfo<TRequest, TResponse>(
    ClientRequest<TRequest, TResponse> descriptor,
    PayloadCodec<TRequest> request,
    PayloadCodec<TResponse> response) : IOutgoingMember
{
  public ClientRequest<TRequest, TResponse> Descriptor { get; } = descriptor;

  public PayloadCodec<TRequest> Request { get; } = request;

  public PayloadCodec<TResponse> Response { get; } = response;

  public string Name => Descriptor.Name;

  public Type RequestType => typeof(TRequest);

  public Type ResponseType => typeof(TResponse);
}
