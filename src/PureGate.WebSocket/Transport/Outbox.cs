using System.Net.WebSockets;
using PureGate.WebSocket.Protocol;
using NetWebSocket = System.Net.WebSockets.WebSocket;

namespace PureGate.WebSocket.Transport;

enum WriteResult
{
  Written,

  // The frame alone is larger than the client accepts. Nothing was written.
  TooLarge,

  // The queue is past its limit: the client is not keeping up. Nothing was written.
  Overloaded,

  // The connection is closing or closed. Nothing was written.
  Closed,
}

// The frames waiting to go out on one connection. Writers copy their frames in under a lock and never wait.
// Frames are coalesced into messages of up to 64 KiB, never past the client's limit, and the writer that finds
// no send running starts one, which keeps sending until the outbox is empty. Names are numbered here, under the
// same lock, so definitions go out in the order they were numbered.
sealed class Outbox(NetWebSocket socket, long queueLimit)
{
  const int CoalesceBytes = 64 * 1024;

  readonly Lock Gate = new();
  readonly Queue<PooledBuffer> Ready = new();
  readonly OutgoingNameTable Names = new();
  readonly TaskCompletionSource Stopped = new(TaskCreationOptions.RunContinuationsAsynchronously);
  PooledBuffer Current;
  long Queued;
  bool Sending;
  bool Closing;
  bool Done;
  WebSocketCloseStatus CloseStatus;
  string CloseReason;

  // The largest message the client accepts. Starts at the protocol's minimum and is raised by Hello.
  public int MaxMessage { get; set; } = 16 * 1024;

  // Completes once nothing more will be sent: after the close frame, or when the socket failed.
  public Task Completion => Stopped.Task;

  public WriteResult Write(scoped in Frame frame) => Write(in frame, null, default);

  // Writes a Request or Event, naming it with name: a reference when the client already has the name, a
  // definition the first time, or the literal name once the table is full.
  public WriteResult Write(scoped in Frame frame, string name, ReadOnlySpan<byte> nameUtf8)
  {
    var start = false;
    lock (Gate)
    {
      if (Closing || Done)
        return WriteResult.Closed;

      var copy = frame;
      if (name is not null)
      {
        // Measured as the longest definition, so a frame that does not fit takes no number.
        copy.NameCode = 2 * NameRules.TableLimit + 1;
        copy.NameText = nameUtf8;
      }

      var length = FrameWriter.GetLength(in copy);
      if (length > MaxMessage)
        return WriteResult.TooLarge;
      if (Queued + length > queueLimit)
        return WriteResult.Overloaded;

      if (name is not null)
      {
        copy.NameCode = Names.Next(name);
        copy.NameText = FrameLayout.NameHasText(copy.NameCode) ? nameUtf8 : default;
        length = FrameWriter.GetLength(in copy);
      }

      var limit = Math.Min(CoalesceBytes, MaxMessage);
      if (Current is not null && Current.WrittenCount > 0 && Current.WrittenCount + length > limit)
      {
        Ready.Enqueue(Current);
        Current = null;
      }

      Current ??= new PooledBuffer(Math.Max(length, 4096));
      FrameWriter.Write(Current, in copy);
      Queued += length;

      start = !Sending;
      Sending = true;
    }

    if (start)
      _ = SendAsync();

    return WriteResult.Written;
  }

  // Sends what is queued, then a close frame. Frames written after this are refused.
  public void Close(WebSocketCloseStatus status, string reason)
  {
    var start = false;
    lock (Gate)
    {
      if (Closing || Done)
        return;

      Closing = true;
      CloseStatus = status;
      CloseReason = Truncate(reason);
      start = !Sending;
      Sending = true;
    }

    if (start)
      _ = SendAsync();
  }

  // Stops for good without a close frame, for a socket that is already gone, and gives the buffers back.
  public void Abandon()
  {
    lock (Gate)
    {
      Done = true;
      if (!Sending)
        ReturnBuffers();
    }

    Stopped.TrySetResult();
  }

  async Task SendAsync()
  {
    try
    {
      while (true)
      {
        PooledBuffer next;
        lock (Gate)
        {
          if (Done)
          {
            ReturnBuffers();
            Sending = false;
            return;
          }

          if (!Ready.TryDequeue(out next))
          {
            next = Current;
            Current = null;
          }

          if (next is null)
          {
            if (!Closing)
            {
              Sending = false;
              return;
            }

            Done = true;
          }
          else
          {
            Queued -= next.WrittenCount;
          }
        }

        if (next is null)
        {
          await socket.CloseOutputAsync(CloseStatus, CloseReason, CancellationToken.None);
          Stopped.TrySetResult();
          return;
        }

        try
        {
          await socket.SendAsync(next.WrittenMemory, WebSocketMessageType.Binary, endOfMessage: true, CancellationToken.None);
        }
        finally
        {
          next.Return();
        }
      }
    }
    catch (Exception exception) when (exception is WebSocketException or ObjectDisposedException or OperationCanceledException)
    {
      lock (Gate)
      {
        Done = true;
        Sending = false;
        ReturnBuffers();
      }

      Stopped.TrySetResult();
    }
  }

  void ReturnBuffers()
  {
    while (Ready.TryDequeue(out var buffer))
      buffer.Return();

    Current?.Return();
    Current = null;
    Queued = 0;
  }

  // A close frame's reason is at most 123 bytes of UTF-8.
  static string Truncate(string reason)
  {
    if (string.IsNullOrEmpty(reason))
      return "";

    var length = Math.Min(reason.Length, 123);
    while (System.Text.Encoding.UTF8.GetByteCount(reason.AsSpan(0, length)) > 123)
      length--;

    return reason[..length];
  }
}
