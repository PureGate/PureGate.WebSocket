using System.Buffers;
using System.Net.WebSockets;
using System.Text.Json;
using PureGate.WebSocket.Diagnostics;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.Serialization;
using PureGate.WebSocket.Transport;

namespace PureGate.WebSocket;

// Receiving: the handshake, then one message at a time. Frames are handled synchronously on this loop: payloads
// are read before the buffer is reused, and handlers go to the thread pool.
public sealed partial class WebSocketConnection
{
  const int FirstReceiveBytes = 4096;

  byte[] ReceiveBuffer;
  volatile bool HelloReceived;
  volatile bool Closing;
  bool Connected;

  internal async Task RunAsync()
  {
    ReceiveBuffer = ArrayPool<byte>.Shared.Rent(FirstReceiveBytes);
    try
    {
      if (await HandshakeAsync())
      {
        while (true)
        {
          var length = await ReceiveMessageAsync();
          if (length < 0)
            break;
          if (!Closing)
            Process(ReceiveBuffer.AsSpan(0, length));
        }
      }

      // The browser closed: answer its close frame.
      Outbox.Close(WebSocketCloseStatus.NormalClosure, "");
    }
    catch (ProtocolException exception)
    {
      Log.ProtocolViolation(Logger, Id, exception.Violation, exception.Message);
      await CloseAndDrainAsync((WebSocketCloseStatus)CloseCodes.ProtocolError, exception.Message);
    }
    catch (CloseException exception)
    {
      await CloseAndDrainAsync(exception.Status, exception.Message);
    }
    catch (Exception exception) when (exception is WebSocketException or OperationCanceledException or ObjectDisposedException)
    {
      // The transport is gone.
    }
    finally
    {
      ArrayPool<byte>.Shared.Return(ReceiveBuffer);
      ReceiveBuffer = [];
      await EndAsync();
    }
  }

  async Task<bool> HandshakeAsync()
  {
    int length;
    using (var timeout = new CancellationTokenSource(Options.HandshakeTimeout))
    using (timeout.Token.Register(static state => ((WebSocketConnection)state).HandshakeTimedOut(), this))
    {
      length = await ReceiveMessageAsync();
      if (length < 0 || Closing)
        return false;

      HelloReceived = true;
    }

    var hello = ReadFirstMessage(ReceiveBuffer.AsSpan(0, length), out var pipelined);
    UserId = Options.UserId(User);
    Outbox.MaxMessage = hello.MaxMessage;
    ClientCallSlots = new SemaphoreSlim(hello.Calls, hello.Calls);

    // Welcome is the first frame the browser gets, so it goes out before anything can send events here. The
    // pipelined calls still wait for the OnConnected handlers, which may set up groups they rely on.
    SendWelcome();
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
        await CloseAndDrainAsync(WebSocketCloseStatus.InternalServerError, "The server failed to set up the connection.");
        return false;
      }
    }

    if (pipelined.Length > 0)
      Process(pipelined);

    return true;
  }

  void HandshakeTimedOut()
  {
    if (!HelloReceived)
      Close((WebSocketCloseStatus)CloseCodes.HandshakeTimeout, "No Hello in time.");
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

    if (hello is null || hello.Calls < 1 || hello.MaxMessage < 16 * 1024)
      ProtocolException.Throw(ProtocolViolation.InvalidHandshake);

    pipelined = reader.Remaining.ToArray();
    return hello;
  }

  void SendWelcome()
  {
    var welcome = new WelcomeMessage
    {
      Connection = Id,
      Session = NewId(16),
      Calls = Options.MaxConcurrentCalls,
      Streams = 1,
      Events = Options.EventWindow,
      MaxMessage = Options.MaxMessageBytes,
      Heartbeat = (long)Options.Heartbeat.TotalMilliseconds,
    };

    var buffer = new PooledBuffer(256);
    try
    {
      using (var writer = new Utf8JsonWriter(buffer))
        JsonSerializer.Serialize(writer, welcome, HandshakeJson.Default.WelcomeMessage);

      var frame = new Frame { Kind = FrameKind.Welcome, Payload = buffer.WrittenSpan };
      Write(in frame);
    }
    finally
    {
      buffer.Return();
    }
  }

  void Process(ReadOnlySpan<byte> message)
  {
    var reader = new FrameReader(message, Side.Client);
    while (reader.TryRead(out var frame))
    {
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
        case FrameKind.Cancel:
          OnCancel(frame.Id);
          break;
        case FrameKind.Ping:
          var pong = new Frame { Kind = FrameKind.Pong };
          Write(in pong);
          break;
        case FrameKind.Pong:
        case FrameKind.Ack:
          break;
        default:
          ProtocolException.Throw(ProtocolViolation.UnexpectedFrame);
          break;
      }
    }
  }

  // Receives one whole message into ReceiveBuffer and returns its length, or -1 when the browser closed.
  async ValueTask<int> ReceiveMessageAsync()
  {
    var limit = Options.MaxMessageBytes;
    var length = 0;
    while (true)
    {
      if (length == limit)
        throw new CloseException(WebSocketCloseStatus.MessageTooBig, $"Messages are limited to {limit} bytes.");

      if (length == ReceiveBuffer.Length)
      {
        var grown = ArrayPool<byte>.Shared.Rent(Math.Min(limit, length * 2));
        ReceiveBuffer.AsSpan(0, length).CopyTo(grown);
        ArrayPool<byte>.Shared.Return(ReceiveBuffer);
        ReceiveBuffer = grown;
      }

      var room = Math.Min(ReceiveBuffer.Length, limit) - length;
      var result = await Socket.ReceiveAsync(ReceiveBuffer.AsMemory(length, room), CancellationToken.None);
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
  async Task CloseAndDrainAsync(WebSocketCloseStatus status, string reason)
  {
    Close(status, reason);
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
    try
    {
      while (true)
      {
        var result = await Socket.ReceiveAsync(ReceiveBuffer.AsMemory(), timeout.Token);
        if (result.MessageType == WebSocketMessageType.Close)
          return;
      }
    }
    catch (Exception exception) when (exception is WebSocketException or OperationCanceledException or ObjectDisposedException)
    {
    }
  }

  async Task EndAsync()
  {
    if (Interlocked.Exchange(ref Ended, 1) != 0)
      return;

    await SessionCancellation.CancelAsync();
    FailInvokes();
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

    // Let the close frame and anything before it go out, but not forever, and not on a socket that can no
    // longer carry it.
    if (Socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
    {
      Outbox.Close(WebSocketCloseStatus.NormalClosure, "");
      try
      {
        await Outbox.Completion.WaitAsync(TimeSpan.FromSeconds(5));
      }
      catch (TimeoutException)
      {
      }
    }

    // SessionCancellation stays undisposed: late event handlers may still link to its token.
    Outbox.Abandon();
  }
}

// Ends the connection with a close code other than protocol_error.
sealed class CloseException(WebSocketCloseStatus status, string message) : Exception(message)
{
  public WebSocketCloseStatus Status { get; } = status;
}
