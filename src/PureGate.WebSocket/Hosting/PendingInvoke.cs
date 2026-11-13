using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.Serialization;

namespace PureGate.WebSocket.Hosting;

// A call to the browser waiting for its Response. Complete runs on the receive loop, so it reads the payload, which is
// the frame's as memory, before the buffer is reused; continuations run on the thread pool.
abstract class PendingInvoke
{
  public abstract void Complete(scoped in Frame frame, ReadOnlyMemory<byte> payload);

  public abstract void Fail(Exception exception);
}

sealed class PendingInvoke<T>(PayloadCodec<T> codec) : PendingInvoke
{
  readonly TaskCompletionSource<T> Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

  public Task<T> Task => Completion.Task;

  public override void Complete(scoped in Frame frame, ReadOnlyMemory<byte> payload)
  {
    if (frame.Has(FrameFlags.HasError))
    {
      Completion.TrySetException(CallException.FromFrame(in frame));
      return;
    }

    try
    {
      Completion.TrySetResult(codec.Read(payload, frame.Has(FrameFlags.Binary)));
    }
    catch (InvalidPayloadException exception)
    {
      Completion.TrySetException(new CallException("invalid_payload", exception.Message));
    }
  }

  public override void Fail(Exception exception) => Completion.TrySetException(exception);
}
