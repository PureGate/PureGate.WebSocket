using System.Net.WebSockets;
using Microsoft.AspNetCore.Http;
using PureGate.WebSocket.Protocol;
using NetWebSocket = System.Net.WebSockets.WebSocket;

namespace PureGate.WebSocket.Transport;

enum WriteResult
{
  Written,

  // The frame alone is larger than the client accepts. Nothing was written.
  TooLarge,

  // The send queue is past its limit: the client is not keeping up. Nothing was written.
  Overloaded,

  // The session ended, or has no connection and cannot keep the frame for a resume. Nothing was written.
  Closed,
}

// One accepted WebSocket of a session, with its own send queue. A session has at most one link attached at a time;
// a resume attaches a new one. The outbox's lock guards the send state.
sealed class Link(NetWebSocket socket, HttpContext context)
{
  readonly TaskCompletionSource Stopped = new(TaskCreationOptions.RunContinuationsAsynchronously);

  public NetWebSocket Socket { get; } = socket;

  public HttpContext Context { get; } = context;

  public Queue<PooledBuffer> Ready { get; } = new();

  public PooledBuffer Current;
  public long Queued;
  public bool Sending;

  // A close frame is due: what is queued goes first, and nothing new is written here.
  public bool Closing;

  // Nothing more will be sent.
  public bool Done;

  public WebSocketCloseStatus CloseStatus;
  public string CloseReason;

  // The largest message the client accepts, from its Hello.
  public int MaxMessage = 16 * 1024;

  // Whether the session ends with this link, because of why the server closed it.
  public volatile bool EndsSession;

  // Receiving.
  public byte[] ReceiveBuffer = [];
  public volatile bool HelloReceived;

  // Completes once nothing more will be sent: after the close frame, or when the socket failed.
  public Task Completion => Stopped.Task;

  public void Stop() => Stopped.TrySetResult();
}

// The frames a session sends (docs/PROTOCOL.md, Messages and frames; Acknowledgement and resume). Writers copy their
// frames in under a lock and never wait. On the attached link, frames are coalesced into messages of up to 64 KiB,
// never past the client's limit, and the writer that finds no send running starts one. Names are numbered here, so
// definitions go out in the order they were numbered.
//
// Sequenced frames are counted, and with resume on they are also kept until the client acknowledges them. Without a
// link, sequenced frames only go to that replay buffer, to be sent when the client resumes. A frame the replay buffer
// cannot hold makes the session impossible to resume.
sealed class Outbox(long queueLimit, long replayLimit)
{
  const int CoalesceBytes = 64 * 1024;

  readonly Lock Gate = new();
  readonly OutgoingNameTable Names = new();
  readonly PooledBuffer Scratch = new(256);
  ReplayBuffer Replay = replayLimit > 0 ? new ReplayBuffer(replayLimit) : null;
  Link Attached;
  long SentCount;
  long AcknowledgedCount;
  bool Ended;

  // Called, outside the lock, when a frame could not be kept while no link is attached: the session cannot resume.
  public Action ResumeLost { get; set; }

  // The largest message the client accepts. Starts at the protocol's minimum and is set by Hello.
  public int MaxMessage { get; set; } = 16 * 1024;

  public bool IsAttached => Volatile.Read(ref Attached) is not null;

  // Whether a dropped link leaves the session resumable.
  public bool CanResume
  {
    get
    {
      lock (Gate)
        return !Ended && Replay is not null;
    }
  }

  // Sequenced frames sent and not yet acknowledged: how far behind the page is.
  public long Unacknowledged => Volatile.Read(ref SentCount) - Volatile.Read(ref AcknowledgedCount);

  public WriteResult Write(scoped in Frame frame) => Write(in frame, null, default);

  // Writes a Request or Event, naming it with name: a reference when the client already has the name, a definition
  // the first time, or the literal name once the table is full.
  public WriteResult Write(scoped in Frame frame, string name, ReadOnlySpan<byte> nameUtf8)
  {
    Link start = null;
    var lost = false;
    lock (Gate)
    {
      if (Ended)
        return WriteResult.Closed;

      var link = Attached;
      var sequenced = FrameLayout.IsSequenced(frame.Kind);
      if (link is null && (!sequenced || Replay is null))
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
      if (link is not null && link.Queued + length > queueLimit)
        return WriteResult.Overloaded;

      if (sequenced && Replay is not null && !Replay.CanHold(length))
      {
        Replay.Clear();
        Replay = null;
        lost = link is null;
      }

      if (!lost)
      {
        if (name is not null)
        {
          copy.NameCode = Names.Next(name);
          copy.NameText = FrameLayout.NameHasText(copy.NameCode) ? nameUtf8 : default;
        }

        if (link is not null)
        {
          var buffer = Reserve(link, FrameWriter.GetLength(in copy));
          var offset = buffer.WrittenCount;
          FrameWriter.Write(buffer, in copy);
          if (sequenced)
            Replay?.Append(buffer.WrittenSpan[offset..]);
          if (!link.Sending)
          {
            link.Sending = true;
            start = link;
          }
        }
        else
        {
          Scratch.Clear();
          FrameWriter.Write(Scratch, in copy);
          Replay.Append(Scratch.WrittenSpan);
        }

        if (sequenced)
          Volatile.Write(ref SentCount, SentCount + 1);
      }
    }

    if (lost)
    {
      ResumeLost?.Invoke();
      return WriteResult.Closed;
    }

    if (start is not null)
      _ = SendAsync(start);

    return WriteResult.Written;
  }

  // Whether a client that processed received frames can resume: every frame after them is still kept.
  public bool CanResumeFrom(long received)
  {
    lock (Gate)
      return !Ended && Replay is not null && received <= SentCount && received >= SentCount - Replay.Count;
  }

  // Makes link the session's connection: writes welcome first, then the sequenced frames after the client's received
  // count again, which include any an earlier link had not sent. False when the session has ended.
  public bool Attach(Link link, scoped in Frame welcome, long received)
  {
    lock (Gate)
    {
      if (Ended)
        return false;

      link.MaxMessage = MaxMessage;
      Attached = link;
      var buffer = Reserve(link, FrameWriter.GetLength(in welcome));
      FrameWriter.Write(buffer, in welcome);

      if (received < SentCount && Replay is not null)
      {
        AcknowledgeUnderLock(received);
        Replay.CopyLast(SentCount - received, frame =>
        {
          var target = Reserve(link, frame.Length);
          frame.CopyTo(target.GetSpan(frame.Length));
          target.Advance(frame.Length);
        });
      }

      link.Sending = true;
    }

    _ = SendAsync(link);
    return true;
  }

  // The client's Ack: count is its processed count modulo 2^32. False for a count past what was sent.
  public bool Acknowledge(uint count)
  {
    lock (Gate)
    {
      var acknowledged = SentCount - (uint)((uint)SentCount - count);
      if (acknowledged < 0)
        return false;

      AcknowledgeUnderLock(acknowledged);
      return true;
    }
  }

  void AcknowledgeUnderLock(long acknowledged)
  {
    if (acknowledged <= AcknowledgedCount)
      return;

    Replay?.Drop(acknowledged - AcknowledgedCount);
    Volatile.Write(ref AcknowledgedCount, acknowledged);
  }

  // Sends what link has queued, unless discard says the client is too far behind for it, then a close frame. From
  // now on the session's frames no longer go to link.
  public void Close(Link link, WebSocketCloseStatus status, string reason, bool discard = false)
  {
    var start = false;
    lock (Gate)
    {
      if (Attached == link)
        Attached = null;
      if (link.Closing || link.Done)
        return;

      link.Closing = true;
      link.CloseStatus = status;
      link.CloseReason = Truncate(reason);
      if (discard)
        ReturnQueued(link);

      start = !link.Sending;
      link.Sending = true;
    }

    if (start)
      _ = SendAsync(link);
  }

  // Lets go of a link whose socket is gone: nothing more is sent on it, and its buffers go back to the pool.
  public void Detach(Link link)
  {
    lock (Gate)
    {
      if (Attached == link)
        Attached = null;

      link.Done = true;
      if (!link.Sending)
        ReturnQueued(link);
    }

    link.Stop();
  }

  // The session ended: nothing more is written, and the buffers go back to the pool.
  public void End()
  {
    lock (Gate)
    {
      Ended = true;
      Replay?.Clear();
      Replay = null;
      Scratch.Return();
    }
  }

  // The buffer the next frame of length bytes goes to: the current message, or a new one when it would grow past
  // what one message may hold.
  PooledBuffer Reserve(Link link, int length)
  {
    var limit = Math.Min(CoalesceBytes, link.MaxMessage);
    if (link.Current is not null && link.Current.WrittenCount > 0 && link.Current.WrittenCount + length > limit)
    {
      link.Ready.Enqueue(link.Current);
      link.Current = null;
    }

    link.Current ??= new PooledBuffer(Math.Max(length, 4096));
    link.Queued += length;
    return link.Current;
  }

  async Task SendAsync(Link link)
  {
    try
    {
      while (true)
      {
        PooledBuffer next;
        lock (Gate)
        {
          if (link.Done)
          {
            ReturnQueued(link);
            link.Sending = false;
            return;
          }

          if (!link.Ready.TryDequeue(out next))
          {
            next = link.Current;
            link.Current = null;
          }

          if (next is null)
          {
            if (!link.Closing)
            {
              link.Sending = false;
              return;
            }

            link.Done = true;
          }
          else
          {
            link.Queued -= next.WrittenCount;
          }
        }

        if (next is null)
        {
          await link.Socket.CloseOutputAsync(link.CloseStatus, link.CloseReason, CancellationToken.None);
          link.Stop();
          return;
        }

        try
        {
          await link.Socket.SendAsync(next.WrittenMemory, WebSocketMessageType.Binary, endOfMessage: true, CancellationToken.None);
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
        if (Attached == link)
          Attached = null;

        link.Done = true;
        link.Sending = false;
        ReturnQueued(link);
      }

      link.Stop();
    }
  }

  static void ReturnQueued(Link link)
  {
    while (link.Ready.TryDequeue(out var buffer))
      buffer.Return();

    link.Current?.Return();
    link.Current = null;
    link.Queued = 0;
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
