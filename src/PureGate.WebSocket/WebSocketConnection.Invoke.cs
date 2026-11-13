using PureGate.WebSocket.Hosting;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.Transport;

namespace PureGate.WebSocket;

// Calls to the browser. The browser said in Hello how many it handles at once; further calls wait here. A call
// that times out or is cancelled sends Cancel, and its id stays taken until the browser's Response arrives anyway.
public sealed partial class WebSocketConnection
{
  readonly Dictionary<uint, PendingInvoke> Invokes = [];
  readonly Lock InvokesGate = new();
  SemaphoreSlim ClientCallSlots;
  uint LastInvokeId;

  // Calls the browser and waits for its answer, at most PureGateWebSocketOptions.InvokeTimeout.
  public ValueTask<TResponse> InvokeAsync<TRequest, TResponse>(
      ClientRequest<TRequest, TResponse> request,
      TRequest payload,
      CancellationToken cancellationToken = default) =>
    InvokeAsync(request, payload, Options.InvokeTimeout, cancellationToken);

  // Calls the browser and waits for its answer, at most timeout (Timeout.InfiniteTimeSpan for no limit). Throws
  // CallException when the browser's handler failed or the call timed out, and OperationCanceledException when
  // cancellationToken fired.
  public async ValueTask<TResponse> InvokeAsync<TRequest, TResponse>(
      ClientRequest<TRequest, TResponse> request,
      TRequest payload,
      TimeSpan timeout,
      CancellationToken cancellationToken = default)
  {
    ArgumentNullException.ThrowIfNull(request);
    var info = Endpoint.GetClientRequest(request);
    if (ClientCallSlots is null || IsEnded)
      throw ConnectionClosed();

    using (var waiting = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, Closed))
    {
      try
      {
        await ClientCallSlots.WaitAsync(waiting.Token);
      }
      catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
      {
        throw ConnectionClosed();
      }
    }

    var pending = new PendingInvoke<TResponse>(info.Response);
    uint id;
    lock (InvokesGate)
    {
      if (IsEnded)
      {
        ClientCallSlots.Release();
        throw ConnectionClosed();
      }

      id = NextInvokeId();
      Invokes.Add(id, pending);
    }

    WriteResult written;
    try
    {
      written = WriteInvoke(info, id, payload, timeout);
    }
    catch
    {
      RemoveInvoke(id);
      throw;
    }

    if (written != WriteResult.Written)
    {
      RemoveInvoke(id);
      throw written == WriteResult.TooLarge
        ? new CallException("payload_too_large", "The request is larger than the browser accepts.")
        : ConnectionClosed();
    }

    using var answer = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    if (timeout != Timeout.InfiniteTimeSpan)
      answer.CancelAfter(timeout);

    try
    {
      return await pending.Task.WaitAsync(answer.Token);
    }
    catch (OperationCanceledException) when (answer.IsCancellationRequested)
    {
      // The answer can arrive just as the wait ends; then it is the outcome.
      if (pending.Task.IsCompleted)
        return await pending.Task;

      var cancel = new Frame { Kind = FrameKind.Cancel, Id = id };
      Write(in cancel);
      if (cancellationToken.IsCancellationRequested)
        throw;

      throw new CallException("timeout", "The browser did not answer in time.");
    }
  }

  WriteResult WriteInvoke<TRequest, TResponse>(
      ClientRequestInfo<TRequest, TResponse> info,
      uint id,
      TRequest payload,
      TimeSpan timeout)
  {
    var buffer = new PooledBuffer(256);
    try
    {
      info.Request.Write(buffer, payload);
      var frame = new Frame
      {
        Kind = FrameKind.Request,
        Flags = info.Request.IsBinary ? FrameFlags.Binary : FrameFlags.None,
        Id = id,
        Payload = buffer.WrittenSpan,
      };
      if (timeout != Timeout.InfiniteTimeSpan)
      {
        frame.Flags |= FrameFlags.HasTimeout;
        frame.Timeout = (uint)Math.Clamp(timeout.TotalMilliseconds, 0, uint.MaxValue);
      }

      var result = Outbox.Write(in frame, info.Descriptor.Name, info.Descriptor.NameUtf8);
      if (result == WriteResult.Overloaded)
        SlowConsumer();

      return result;
    }
    finally
    {
      buffer.Return();
    }
  }

  void OnResponse(scoped in Frame frame)
  {
    PendingInvoke pending;
    lock (InvokesGate)
    {
      if (!Invokes.Remove(frame.Id, out pending))
        ProtocolException.Throw(ProtocolViolation.UnknownExchange);
    }

    ClientCallSlots.Release();
    pending.Complete(in frame);
  }

  void RemoveInvoke(uint id)
  {
    lock (InvokesGate)
      Invokes.Remove(id);

    ClientCallSlots.Release();
  }

  // Ids run from 1 to 2^32 - 1 and wrap, skipping the ones still in use. Called under InvokesGate.
  uint NextInvokeId()
  {
    do
      LastInvokeId = LastInvokeId == uint.MaxValue ? 1 : LastInvokeId + 1;
    while (Invokes.ContainsKey(LastInvokeId));

    return LastInvokeId;
  }

  void FailInvokes()
  {
    PendingInvoke[] pending;
    lock (InvokesGate)
    {
      pending = [.. Invokes.Values];
      Invokes.Clear();
    }

    foreach (var invoke in pending)
      invoke.Fail(ConnectionClosed());
  }

  static CallException ConnectionClosed() => new("connection_closed", "The connection closed.");
}
