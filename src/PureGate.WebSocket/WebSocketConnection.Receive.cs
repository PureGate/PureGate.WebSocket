using System.Buffers;
using System.Net.WebSockets;
using System.Text.Json;
using PureGate.WebSocket.Diagnostics;
using PureGate.WebSocket.Hosting;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.Serialization;
using PureGate.WebSocket.Transport;

namespace PureGate.WebSocket;

// Receiving: the handshake, then one message at a time. Frames are handled synchronously on the receive loop, under
// the link lock: payloads are read before the buffer is reused, handlers go to the thread pool, and a resume can
// never meet a message half processed.
public sealed partial class WebSocketConnection
{
  const int FirstReceiveBytes = 4096;

  readonly Lock LinkGate = new();
  Link CurrentLink;

  // Welcome went out: the session is the browser's, and resumable if resume is on.
  bool Started;

  // The OnConnected handlers ran: the hub knows the session, and OnDisconnected runs at its end.
  bool Connected;

  string ContractStatus;

  // Accepts one WebSocket: a new session, or the resume of one the endpoint still has.
  internal static Task AcceptAsync(EndpointState endpoint, Link link) => new WebSocketConnection(endpoint, link).RunAsync(link);

  async Task RunAsync(Link link)
  {
    link.ReceiveBuffer = ArrayPool<byte>.Shared.Rent(FirstReceiveBytes);
    HelloMessage hello;
    byte[] pipelined;
    Identity identity = default;
    try
    {
      (hello, pipelined) = await ReceiveHelloAsync(link);
      if (hello is not null)
      {
        ContractStatus = CheckContract(hello.Contract);
        identity = await AuthenticateAsync(link, hello.Token);
      }
    }
    catch (ProtocolException exception)
    {
      Log.ProtocolViolation(Logger, Id, exception.Violation, exception.Message);
      await CloseAndDrainAsync(link, (WebSocketCloseStatus)CloseCodes.ProtocolError, exception.Message);
      hello = null;
      pipelined = null;
    }
    catch (CloseException exception)
    {
      await CloseAndDrainAsync(link, exception.Status, exception.Message);
      hello = null;
      pipelined = null;
    }
    catch (Exception exception) when (IsTransportFailure(exception))
    {
      hello = null;
      pipelined = null;
    }

    if (hello is null)
    {
      link.EndsSession = true;
      await LinkEndedAsync(link);
      return;
    }

    if (hello.Resume?.Session is { } sessionId
      && Options.ResumeWindow > TimeSpan.Zero
      && Endpoint.Sessions.TryGetValue(sessionId, out var session))
    {
      switch (session.Resume(link, hello, ContractStatus, identity))
      {
        case ResumeOutcome.Resumed:
          // This session was only a stand-in until the right one was found.
          Outbox.End();
          await session.RunLinkAsync(link, null, null, default);
          return;
        case ResumeOutcome.Unavailable:
          _ = session.EndAsync();
          break;
      }
    }

    await RunLinkAsync(link, hello, pipelined, identity);
  }

  // Runs link until it ends. hello is set for a new session, which starts here; a resumed one has started already.
  async Task RunLinkAsync(Link link, HelloMessage hello, byte[] pipelined, Identity identity)
  {
    try
    {
      if (hello is null || await StartAsync(link, hello, pipelined, identity))
      {
        await ReceiveLoopAsync(link);

        // The browser closed: answer its close frame with its own code, as browsers report the answer's code. A close
        // it started ends the session, unless the page is going away (1001) or the client will resume (4006). One
        // that answers the server's close says nothing of its own.
        var status = link.Socket.CloseStatus is { } code and not WebSocketCloseStatus.Empty ? code : WebSocketCloseStatus.NormalClosure;
        if (!link.Closing)
          link.EndsSession |= status is not (WebSocketCloseStatus.EndpointUnavailable or (WebSocketCloseStatus)CloseCodes.Reconnecting);
        Outbox.Close(link, status, "");
      }
    }
    catch (ProtocolException exception)
    {
      Log.ProtocolViolation(Logger, Id, exception.Violation, exception.Message);
      link.EndsSession = true;
      await CloseAndDrainAsync(link, (WebSocketCloseStatus)CloseCodes.ProtocolError, exception.Message);
    }
    catch (CloseException exception)
    {
      link.EndsSession |= exception.EndsSession;
      await CloseAndDrainAsync(link, exception.Status, exception.Message);
    }
    catch (Exception exception) when (IsTransportFailure(exception))
    {
      // The connection dropped. The session stays, if it can resume.
    }
    finally
    {
      await LinkEndedAsync(link);
    }
  }

  static bool IsTransportFailure(Exception exception) =>
    exception is WebSocketException or OperationCanceledException or ObjectDisposedException;

  async Task<(HelloMessage Hello, byte[] Pipelined)> ReceiveHelloAsync(Link link)
  {
    int length;
    using (var timeout = new CancellationTokenSource(Options.HandshakeTimeout))
    using (timeout.Token.Register(HandshakeTimedOut, link))
    {
      length = await ReceiveMessageAsync(link);
      if (length < 0 || link.Closing)
        return (null, null);

      link.HelloReceived = true;
    }

    var hello = ReadFirstMessage(link.ReceiveBuffer.AsSpan(0, length), out var pipelined);
    return (hello, pipelined);
  }

  void HandshakeTimedOut(object state)
  {
    var link = (Link)state;
    if (!link.HelloReceived)
      Outbox.Close(link, (WebSocketCloseStatus)CloseCodes.HandshakeTimeout, "No Hello in time.");
  }

  // A new session: Welcome first, then the OnConnected handlers, then the frames that came with Hello. Those wait for
  // the handlers, which may set up groups they rely on. False when a handler failed.
  async Task<bool> StartAsync(Link link, HelloMessage hello, byte[] pipelined, Identity identity)
  {
    UserId = Options.UserId(identity.User);
    Outbox.MaxMessage = hello.MaxMessage;
    ClientCallSlots = new SemaphoreSlim(hello.Calls, hello.Calls);
    StartLimiter();

    lock (LinkGate)
    {
      SetIdentity(identity);
      CurrentLink = link;
      Started = true;
      AttachWelcome(link, resumed: false, received: 0);
    }

    Endpoint.Sessions.TryAdd(SessionId, this);
    Endpoint.Hub.Add(this);
    Connected = true;
    foreach (var handler in Endpoint.Connected)
    {
      try
      {
        await handler(this, Closed);
      }
      catch (Exception exception)
      {
        Log.ConnectedHandlerFailed(Logger, Id, exception);
        link.EndsSession = true;
        await CloseAndDrainAsync(link, WebSocketCloseStatus.InternalServerError, "The server failed to set up the connection.");
        return false;
      }
    }

    if (pipelined.Length > 0)
    {
      lock (LinkGate)
      {
        if (CurrentLink == link)
          Process(pipelined);
      }
    }

    return true;
  }

  async Task ReceiveLoopAsync(Link link)
  {
    while (true)
    {
      var length = await ReceiveMessageAsync(link);
      if (length < 0)
        return;

      lock (LinkGate)
      {
        // Replaced by a resume: the session counts nothing more from here.
        if (CurrentLink != link)
          return;
        if (!link.Closing)
          Process(link.ReceiveBuffer.AsSpan(0, length));
      }
    }
  }

  // The first message starts with Hello. Whatever follows it was pipelined, and is handled after Welcome.
  static HelloMessage ReadFirstMessage(ReadOnlySpan<byte> message, out byte[] pipelined)
  {
    var reader = new FrameReader(message, Side.Client);
    if (!reader.TryRead(out var first) || first.Kind != FrameKind.Hello)
      ProtocolException.Throw(ProtocolViolation.HandshakeExpected);

    HelloMessage hello = null;
    try
    {
      hello = JsonSerializer.Deserialize(first.Payload, HandshakeJson.Default.HelloMessage);
    }
    catch (JsonException)
    {
    }

    if (hello is null || hello.Calls < 1 || hello.MaxMessage < 16 * 1024 || hello.Resume is { Received: < 0 })
      ProtocolException.Throw(ProtocolViolation.InvalidHandshake);

    pipelined = reader.Remaining.ToArray();
    return hello;
  }

  // Welcome's contract field: whether the browser's contract.ts is the server's, or null when it sent no hash or
  // the server does not check. A stale one is logged, or refused under ContractMismatchPolicy.Reject.
  string CheckContract(string hash)
  {
    if (hash is null || Options.ContractMismatch == ContractMismatchPolicy.Ignore)
      return null;

    string current;
    try
    {
      current = Endpoint.Contract.Hash;
    }
    catch (Exception exception) when (exception is InvalidOperationException or NotSupportedException or ArgumentException)
    {
      Log.ContractFailed(Logger, exception);
      return null;
    }

    if (string.Equals(hash, current, StringComparison.Ordinal))
      return "current";

    Log.ContractStale(Logger, Id, hash, current);
    if (Options.ContractMismatch == ContractMismatchPolicy.Reject)
      throw new CloseException((WebSocketCloseStatus)CloseCodes.ContractRejected, "The browser's contract.ts is stale.");

    return "stale";
  }

  // Attaches link with its Welcome, which goes before anything else on it. Called under LinkGate.
  bool AttachWelcome(Link link, bool resumed, long received)
  {
    var welcome = new WelcomeMessage
    {
      Connection = Id,
      Session = SessionId,
      Resumed = resumed,
      Received = resumed ? ReceivedCount : 0,
      Calls = Options.MaxConcurrentCalls,
      Streams = Options.MaxStreams,
      Events = Options.EventWindow,
      MaxMessage = Options.MaxMessageBytes,
      Heartbeat = (long)Options.Heartbeat.TotalMilliseconds,
      ResumeWindow = (long)Options.ResumeWindow.TotalMilliseconds,
      Contract = ContractStatus,
      Expires = ExpiresInMilliseconds(),
      Route = Options.ResumeRoute,
    };

    var buffer = new PooledBuffer(256);
    try
    {
      using (var writer = new Utf8JsonWriter(buffer))
        JsonSerializer.Serialize(writer, welcome, HandshakeJson.Default.WelcomeMessage);

      var frame = new Frame { Kind = FrameKind.Welcome, Payload = buffer.WrittenSpan };
      return Outbox.Attach(link, in frame, received);
    }
    finally
    {
      buffer.Return();
    }
  }

  void Process(ReadOnlySpan<byte> message)
  {
    var reader = new FrameReader(message, Side.Client);
    var sequenced = 0;
    while (reader.TryRead(out var frame))
    {
      if (FrameLayout.IsSequenced(frame.Kind))
        sequenced++;

      switch (frame.Kind)
      {
        case FrameKind.Request:
          OnRequest(in frame);
          break;
        case FrameKind.Response:
          OnResponse(in frame);
          break;
        case FrameKind.Event:
          OnEvent(in frame);
          break;
        case FrameKind.StreamOpen:
          OnStreamOpen(in frame);
          break;
        case FrameKind.StreamItem:
          OnStreamItem(in frame);
          break;
        case FrameKind.StreamEnd:
          OnStreamEnd(in frame);
          break;
        case FrameKind.Credit:
          OnCredit(in frame);
          break;
        case FrameKind.Cancel:
          OnCancel(frame.Id);
          break;
        case FrameKind.Ack:
          if (!Outbox.Acknowledge(frame.Count))
            ProtocolException.Throw(ProtocolViolation.UnexpectedFrame);
          break;
        case FrameKind.Authenticate:
          OnAuthenticate(in frame);
          break;
        case FrameKind.Ping:
          var pong = new Frame { Kind = FrameKind.Pong };
          Outbox.Write(in pong);
          break;
        case FrameKind.Pong:
          break;
        default:
          ProtocolException.Throw(ProtocolViolation.UnexpectedFrame);
          break;
      }
    }

    // A message counts once all of it is processed.
    if (sequenced > 0)
      Processed(sequenced);
  }

  // Receives one whole message into the link's buffer and returns its length, or -1 when the browser closed.
  async ValueTask<int> ReceiveMessageAsync(Link link)
  {
    var limit = Options.MaxMessageBytes;
    var length = 0;
    while (true)
    {
      if (length == limit)
        throw new CloseException(WebSocketCloseStatus.MessageTooBig, $"Messages are limited to {limit} bytes.");

      if (length == link.ReceiveBuffer.Length)
      {
        var grown = ArrayPool<byte>.Shared.Rent(Math.Min(limit, length * 2));
        link.ReceiveBuffer.AsSpan(0, length).CopyTo(grown);
        ArrayPool<byte>.Shared.Return(link.ReceiveBuffer);
        link.ReceiveBuffer = grown;
      }

      var room = Math.Min(link.ReceiveBuffer.Length, limit) - length;
      var result = await link.Socket.ReceiveAsync(link.ReceiveBuffer.AsMemory(length, room), CancellationToken.None);
      if (result.MessageType == WebSocketMessageType.Close)
        return -1;
      if (result.MessageType == WebSocketMessageType.Text)
        throw new CloseException(WebSocketCloseStatus.InvalidMessageType, "Messages must be binary.");

      length += result.Count;
      if (result.EndOfMessage)
        return length;
    }
  }

  // Sends a close frame, then waits a moment for the browser's, so the close code reaches it instead of a reset.
  async Task CloseAndDrainAsync(Link link, WebSocketCloseStatus status, string reason)
  {
    Outbox.Close(link, status, reason);
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
    try
    {
      while (true)
      {
        var result = await link.Socket.ReceiveAsync(link.ReceiveBuffer.AsMemory(), timeout.Token);
        if (result.MessageType == WebSocketMessageType.Close)
          return;
      }
    }
    catch (Exception exception) when (IsTransportFailure(exception))
    {
    }
  }
}

// Ends the connection with a close code other than protocol_error.
sealed class CloseException(WebSocketCloseStatus status, string message, bool endsSession = true) : Exception(message)
{
  public WebSocketCloseStatus Status { get; } = status;

  // False for 4001 (unauthorized): a browser that comes back authenticated resumes the session.
  public bool EndsSession { get; } = endsSession;
}
