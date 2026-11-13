using System.Buffers;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text.Json;
using PureGate.WebSocket.Diagnostics;
using PureGate.WebSocket.Hosting;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.Serialization;
using PureGate.WebSocket.Transport;

namespace PureGate.WebSocket;

// Receiving: the handshake, then one message at a time. Frames are handled synchronously on the receive loop, under
// the link lock: payloads are read before the buffer is reused, and a resume can never meet a message half processed.
// The handlers a message starts run next on the receive loop, outside the lock, until their first await: one that
// completes synchronously costs no thread switch, and the answers to a message go out together in one message.
public sealed partial class WebSocketConnection
{
  const int FirstReceiveBytes = 4096;

  // The context handlers start in: an empty one, as on the thread pool, so that the upgrade request's activity, log
  // scopes and HttpContext do not flow into every call.
  static readonly ExecutionContext HandlerContext = CaptureEmptyContext();

  readonly Lock LinkGate = new();
  Link CurrentLink;

  // The link whose message Process is handling, under LinkGate.
  Link Processing;

  // Welcome went out: the session is the browser's, and resumable if resume is on.
  bool Started;

  // The OnConnected handlers ran: the hub knows the session, and OnDisconnected runs at its end.
  bool Connected;

  string ContractStatus;

  // Runs one accepted WebSocket: a new session, or the resume of one the endpoint still has. Called on a connection
  // made for it, which stands in for the session until Hello says which one it is.
  internal async Task RunAsync(Link link)
  {
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
          Endpoint.Telemetry.Resumes.Add(1, new KeyValuePair<string, object>("puregate.websocket.resumed", true));
          Outbox.End();

          // Hello need not live as long as the connection (see Take).
          hello = null;
          pipelined = null;
          await session.RunLinkAsync(link, null, null, default);
          return;
        case ResumeOutcome.Unavailable:
          _ = session.EndAsync();
          break;
      }
    }

    if (hello.Resume is not null)
      Endpoint.Telemetry.Resumes.Add(1, new KeyValuePair<string, object>("puregate.websocket.resumed", false));

    await RunLinkAsync(link, Take(ref hello), Take(ref pipelined), identity);
  }

  // Runs link until it ends. hello is set for a new session, which starts here; a resumed one has started already.
  async Task RunLinkAsync(Link link, HelloMessage hello, byte[] pipelined, Identity identity)
  {
    try
    {
      if (hello is null || await StartAsync(link, Take(ref hello), Take(ref pipelined), identity))
      {
        // The receive loop, here rather than in a method of its own: an idle connection keeps one state machine less.
        while (true)
        {
          var length = await ReceiveMessageAsync(link);
          if (length < 0 || !Handle(link, link.ReceiveBuffer.AsSpan(0, length)))
            break;
        }

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

  // Hands value on and clears the variable, so a suspended state machine stops holding it: Hello's token and contract
  // hash need not live as long as the connection.
  static T Take<T>(ref T value)
    where T : class
  {
    var taken = value;
    value = null;
    return taken;
  }

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
    Endpoint.Telemetry.Sessions.Add(1);
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
      Handle(link, pipelined);

    return true;
  }

  // Processes one message, runs the handlers it started, and sends what both wrote in one go. False when a resume
  // replaced link: the session counts nothing more from it.
  bool Handle(Link link, ReadOnlySpan<byte> message)
  {
    Outbox.Cork();
    try
    {
      lock (LinkGate)
      {
        if (CurrentLink != link)
          return false;
        if (!link.Closing)
          Process(link, message);
      }
    }
    finally
    {
      // Also when the message broke the protocol: the calls it began before that still run, and get their answer.
      RunStartedHandlers(link);
    }

    return true;
  }

  // Starts a handler from Process: the receive loop runs it once the message is processed. Anywhere else, it goes to
  // the thread pool.
  internal void RunHandler(IThreadPoolWorkItem handler)
  {
    if (Processing is { } link)
      link.StartedHandlers.Add(handler);
    else
      ThreadPool.UnsafeQueueUserWorkItem(handler, preferLocal: false);
  }

  void RunStartedHandlers(Link link)
  {
    try
    {
      if (link.StartedHandlers.Count > 0)
        ExecutionContext.Run(HandlerContext, static state => RunAll((List<IThreadPoolWorkItem>)state), link.StartedHandlers);
    }
    finally
    {
      Outbox.Uncork();
    }
  }

  static void RunAll(List<IThreadPoolWorkItem> handlers)
  {
    try
    {
      for (var i = 0; i < handlers.Count; i++)
        handlers[i].Execute();
    }
    finally
    {
      handlers.Clear();
    }
  }

  // A thread started without its starter's context has an empty one. A task would not do: the thread waiting for it
  // may run it inline, in its own context.
  static ExecutionContext CaptureEmptyContext()
  {
    ExecutionContext empty = null;
    var thread = new Thread(() => empty = ExecutionContext.Capture());
    thread.UnsafeStart();
    thread.Join();
    return empty ?? throw new InvalidOperationException("No empty execution context.");
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

  // Called under LinkGate. The handlers the message starts collect on link, for Handle to run afterwards.
  void Process(Link link, ReadOnlySpan<byte> message)
  {
    Processing = link;
    try
    {
      ProcessFrames(message);
    }
    finally
    {
      Processing = null;
    }
  }

  void ProcessFrames(ReadOnlySpan<byte> message)
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

  // Receives one whole message into the link's buffer and returns its length, or -1 when the browser closed. It
  // first waits for the message with a zero-byte receive, and gives the buffer back to the pool when it has to wait,
  // so an idle connection holds none; one whose next message is already there keeps it. Pooled, since nearly every
  // message waits for the socket.
  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
  async ValueTask<int> ReceiveMessageAsync(Link link)
  {
    var next = link.Socket.ReceiveAsync(Memory<byte>.Empty, CancellationToken.None);
    if (!next.IsCompleted)
      ReturnReceiveBuffer(link);

    var result = await next;
    var limit = Options.MaxMessageBytes;
    var length = 0;
    while (true)
    {
      if (result.MessageType == WebSocketMessageType.Close)
        return -1;
      if (result.MessageType == WebSocketMessageType.Text)
        throw new CloseException(WebSocketCloseStatus.InvalidMessageType, "Messages must be binary.");

      length += result.Count;
      if (result.EndOfMessage)
        return length;

      if (length == limit)
        throw new CloseException(WebSocketCloseStatus.MessageTooBig, $"Messages are limited to {limit} bytes.");

      if (length == link.ReceiveBuffer.Length)
      {
        var grown = ArrayPool<byte>.Shared.Rent(length == 0 ? FirstReceiveBytes : Math.Min(limit, length * 2));
        link.ReceiveBuffer.AsSpan(0, length).CopyTo(grown);
        ReturnReceiveBuffer(link);
        link.ReceiveBuffer = grown;
      }

      var room = Math.Min(link.ReceiveBuffer.Length, limit) - length;
      result = await link.Socket.ReceiveAsync(link.ReceiveBuffer.AsMemory(length, room), CancellationToken.None);
    }
  }

  static void ReturnReceiveBuffer(Link link)
  {
    var buffer = link.ReceiveBuffer;
    link.ReceiveBuffer = [];
    if (buffer.Length > 0)
      ArrayPool<byte>.Shared.Return(buffer);
  }

  // Sends a close frame, then waits a moment for the browser's, so the close code reaches it instead of a reset.
  async Task CloseAndDrainAsync(Link link, WebSocketCloseStatus status, string reason)
  {
    Outbox.Close(link, status, reason);

    // A receive into an empty buffer would only wait for data, never take it.
    if (link.ReceiveBuffer.Length == 0)
      link.ReceiveBuffer = ArrayPool<byte>.Shared.Rent(FirstReceiveBytes);

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
