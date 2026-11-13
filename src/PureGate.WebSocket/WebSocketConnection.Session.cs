using System.Net.WebSockets;
using PureGate.WebSocket.Diagnostics;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.Serialization;
using PureGate.WebSocket.Transport;

namespace PureGate.WebSocket;

// The session across connections (docs/PROTOCOL.md, Acknowledgement and resume; Shutting down). Each side counts the
// sequenced frames it processes and acknowledges them. When the connection drops, the session keeps running for the
// resume window, and what it sends waits in the replay buffer. A resume attaches the browser's new connection and
// sends that buffer again from where the browser left off.
public sealed partial class WebSocketConnection
{
  const int AckEveryFrames = 64;
  static readonly TimeSpan AckDelay = TimeSpan.FromSeconds(1);

  // Guarded by LinkGate.
  long ReceivedCount;
  int UnacknowledgedReceived;
  Timer AckTimer;
  bool AckScheduled;
  Timer ExpiryTimer;
  Timer DrainTimer;

  volatile bool Draining;

  enum ResumeOutcome
  {
    Resumed,

    // Another user's session: it carries on, and the browser gets a new one.
    Refused,

    // The session can no longer resume, and ends; the browser gets a new one.
    Unavailable,
  }

  // The browser's Hello asked to resume this session on link, authenticated as identity, which must be the session's
  // user: a leaked session id alone takes nothing over. Answers with Welcome and the frames the browser missed.
  ResumeOutcome Resume(Link link, HelloMessage hello, string contractStatus, Identity identity)
  {
    var userId = Options.UserId(identity.User);
    Link old;
    lock (LinkGate)
    {
      if (IsEnded || !Started || !string.Equals(userId, UserId, StringComparison.Ordinal))
        return IsEnded ? ResumeOutcome.Unavailable : ResumeOutcome.Refused;
      if (Draining || !Outbox.CanResumeFrom(hello.Resume.Received))
        return ResumeOutcome.Unavailable;

      old = CurrentLink;
      CurrentLink = link;
      ExpiryTimer?.Change(Timeout.Infinite, Timeout.Infinite);
      HttpContext = link.Context;
      SetIdentity(identity);
      ContractStatus = contractStatus;
      Outbox.MaxMessage = hello.MaxMessage;
      if (!AttachWelcome(link, resumed: true, received: hello.Resume.Received))
      {
        CurrentLink = old;
        return ResumeOutcome.Unavailable;
      }
    }

    Log.SessionResumed(Logger, Id);
    if (old is not null)
    {
      // The browser left that connection for this one: what it still had queued went out again here.
      Outbox.Close(old, (WebSocketCloseStatus)CloseCodes.SessionReplaced, "Another connection resumed this session.", discard: true);
      _ = AbortWhenClosedAsync(old);
    }

    return ResumeOutcome.Resumed;
  }

  // A replaced connection's browser side is usually gone: give its close frame a moment, then let go of it.
  static async Task AbortWhenClosedAsync(Link link)
  {
    try
    {
      await link.Completion.WaitAsync(TimeSpan.FromSeconds(5));
    }
    catch (TimeoutException)
    {
    }

    link.Socket.Abort();
  }

  // A message's sequenced frames are processed. Called under LinkGate.
  void Processed(int frames)
  {
    ReceivedCount += frames;
    UnacknowledgedReceived += frames;
    if (UnacknowledgedReceived >= AckEveryFrames)
    {
      SendAck();
    }
    else if (!AckScheduled)
    {
      AckScheduled = true;
      AckTimer ??= new Timer(static state => ((WebSocketConnection)state).AckDue(), this, Timeout.Infinite, Timeout.Infinite);
      AckTimer.Change(AckDelay, Timeout.InfiniteTimeSpan);
    }
  }

  void AckDue()
  {
    lock (LinkGate)
    {
      AckScheduled = false;
      if (UnacknowledgedReceived > 0 && !IsEnded)
        SendAck();
    }
  }

  // Called under LinkGate. Without a connection, the next Welcome carries the count instead.
  void SendAck()
  {
    UnacknowledgedReceived = 0;
    var ack = new Frame { Kind = FrameKind.Ack, Count = (uint)ReceivedCount };
    Outbox.Write(in ack);
  }

  // A link ended. Unless it was replaced, the session either ends with it or waits for the browser to resume.
  async Task LinkEndedAsync(Link link)
  {
    bool end;
    lock (LinkGate)
    {
      var replaced = CurrentLink != link;
      if (!replaced)
        CurrentLink = null;

      end = !replaced && (link.EndsSession || !Started || Draining || !Outbox.CanResume);
      if (!replaced && !end)
      {
        ExpiryTimer ??= new Timer(static state => _ = ((WebSocketConnection)state).ExpireAsync(), this, Timeout.Infinite, Timeout.Infinite);
        ExpiryTimer.Change(Options.ResumeWindow, Timeout.InfiniteTimeSpan);
      }
    }

    var buffer = link.ReceiveBuffer;
    link.ReceiveBuffer = [];
    if (buffer.Length > 0)
      System.Buffers.ArrayPool<byte>.Shared.Return(buffer);

    if (end)
      await EndAsync();

    await FinishLinkAsync(link);
  }

  // Lets the close frame, and anything before it, go out, but not forever, and not on a socket that can no longer
  // carry it. Then lets go of the link.
  async Task FinishLinkAsync(Link link)
  {
    if (link.Socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
    {
      Outbox.Close(link, WebSocketCloseStatus.NormalClosure, "");
      try
      {
        await link.Completion.WaitAsync(TimeSpan.FromSeconds(5));
      }
      catch (TimeoutException)
      {
      }
    }

    Outbox.Detach(link);
  }

  async Task ExpireAsync()
  {
    lock (LinkGate)
    {
      if (CurrentLink is not null)
        return;
    }

    Log.SessionExpired(Logger, Id);
    await EndAsync();
  }

  // The replay buffer overflowed while the browser was away: it can no longer resume, so the session ends.
  void ResumeLost()
  {
    lock (LinkGate)
    {
      if (CurrentLink is not null)
        return;
    }

    _ = EndAsync();
  }

  // The server is stopping: GoAway, then the exchanges in progress finish, and the connection closes with 1001,
  // at the latest after DrainTimeout. Nothing new starts here meanwhile.
  internal void GoAway()
  {
    Link link;
    lock (LinkGate)
    {
      if (Draining || IsEnded)
        return;

      Draining = true;
      link = CurrentLink;
    }

    if (link is null)
    {
      _ = EndAsync();
      return;
    }

    var goAway = new Frame { Kind = FrameKind.GoAway };
    Outbox.Write(in goAway);
    DrainTimer = new Timer(static state => ((WebSocketConnection)state).Drained(), this, Options.DrainTimeout, Timeout.InfiniteTimeSpan);
    CheckDrained();
  }

  // After an exchange ends: a draining session closes once none is left.
  internal void CheckDrained()
  {
    if (!Draining)
      return;

    lock (CallsGate)
    {
      if (ActiveCalls.Count > 0 || Downloads.Count > 0 || Uploads.Count > 0)
        return;
    }

    lock (InvokesGate)
    {
      if (Invokes.Count > 0)
        return;
    }

    Drained();
  }

  void Drained() => CloseLink(WebSocketCloseStatus.EndpointUnavailable, "The server is shutting down.", endsSession: true);

  // Ends the session: its handlers' tokens fire, outstanding calls to the browser fail, and OnDisconnected runs.
  internal async Task EndAsync()
  {
    Link link;
    lock (LinkGate)
    {
      if (Interlocked.Exchange(ref Ended, 1) != 0)
        return;

      link = CurrentLink;
      CurrentLink = null;
    }

    AckTimer?.Dispose();
    ExpiryTimer?.Dispose();
    DrainTimer?.Dispose();
    ExpiryOfAuthentication?.Dispose();
    Limiter?.Dispose();
    Endpoint.Sessions.TryRemove(new KeyValuePair<string, WebSocketConnection>(SessionId, this));

    await SessionCancellation.CancelAsync();
    FailInvokes();
    FinishStreams();
    if (Connected)
    {
      Endpoint.Hub.Remove(this);
      foreach (var handler in Endpoint.Disconnected)
      {
        try
        {
          await handler(this, CancellationToken.None);
        }
        catch (Exception exception)
        {
          Log.DisconnectedHandlerFailed(Logger, Id, exception);
        }
      }
    }

    if (link is not null)
      await FinishLinkAsync(link);

    // SessionCancellation stays undisposed: late event handlers may still link to its token.
    Outbox.End();
  }
}
