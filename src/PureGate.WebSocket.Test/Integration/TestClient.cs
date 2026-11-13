using System.Buffers;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Test.Integration;

// A frame to send, as plain data: Frame itself is a ref struct. Flags must name the optional fields that are set.
sealed class Outgoing
{
  public FrameKind Kind;
  public FrameFlags Flags;
  public uint Id;
  public uint NameCode;
  public string Name;
  public uint Timeout;
  public string Trace;
  public uint Credit;
  public uint Amount;
  public uint Count;
  public ErrorCode ErrorCode;
  public string ErrorCustom;
  public string ErrorMessage;
  public byte[] Payload = [];

  public string Json
  {
    set => Payload = Encoding.UTF8.GetBytes(value);
  }

  public static Outgoing Hello(string json) => new() { Kind = FrameKind.Hello, Json = json };

  // A Request that names its operation literally.
  public static Outgoing Request(uint id, string name, string json = null) =>
    new() { Kind = FrameKind.Request, Id = id, Name = name, Payload = Utf8(json) };

  public static Outgoing Event(string name, string json) =>
    new() { Kind = FrameKind.Event, Name = name, Payload = Utf8(json) };

  public static Outgoing Response(uint id, string json) =>
    new() { Kind = FrameKind.Response, Id = id, Payload = Utf8(json) };

  public static Outgoing Cancel(uint id) => new() { Kind = FrameKind.Cancel, Id = id };

  // A download's StreamOpen, with the client's initial credit.
  public static Outgoing Download(uint id, string name, uint credit, string json = null) =>
    new() { Kind = FrameKind.StreamOpen, Id = id, Name = name, Credit = credit, Payload = Utf8(json) };

  // An upload's StreamOpen: the server grants its credit.
  public static Outgoing Upload(uint id, string name, string json = null) =>
    new() { Kind = FrameKind.StreamOpen, Flags = FrameFlags.Upload, Id = id, Name = name, Payload = Utf8(json) };

  public static Outgoing Item(uint id, string json) => new() { Kind = FrameKind.StreamItem, Id = id, Payload = Utf8(json) };

  public static Outgoing End(uint id) => new() { Kind = FrameKind.StreamEnd, Id = id };

  public static Outgoing Grant(uint id, uint amount) => new() { Kind = FrameKind.Credit, Id = id, Amount = amount };

  public static Outgoing Ack(uint count) => new() { Kind = FrameKind.Ack, Count = count };

  public void WriteTo(IBufferWriter<byte> output)
  {
    var frame = new Frame
    {
      Kind = Kind,
      Flags = Flags,
      Id = Id,
      NameCode = NameCode,
      NameText = Name is null ? default : Encoding.ASCII.GetBytes(Name),
      Timeout = Timeout,
      Trace = Trace is null ? default : Encoding.ASCII.GetBytes(Trace),
      Credit = Credit,
      Amount = Amount,
      Count = Count,
      ErrorCode = ErrorCode,
      ErrorCustom = ErrorCustom is null ? default : Encoding.ASCII.GetBytes(ErrorCustom),
      ErrorMessage = Encoding.UTF8.GetBytes(ErrorMessage ?? ""),
      Payload = Payload,
    };
    FrameWriter.Write(output, in frame);
  }

  static byte[] Utf8(string json) => json is null ? [] : Encoding.UTF8.GetBytes(json);
}

// A frame as received, copied out of its message.
sealed class Received
{
  public FrameKind Kind;
  public FrameFlags Flags;
  public uint Id;
  public uint NameCode;
  public string Name;
  public uint Timeout;
  public string Trace;
  public uint Credit;
  public uint Amount;
  public uint Count;
  public ErrorCode ErrorCode;
  public string ErrorCustom;
  public string ErrorMessage;
  public byte[] Payload;

  public string Text => Encoding.UTF8.GetString(Payload);

  public bool Has(FrameFlags flag) => (Flags & flag) != 0;

  public JsonElement Json() => JsonDocument.Parse(Payload).RootElement.Clone();

  // The error's code as applications see it: a library name such as "timeout", or the custom code.
  public string Error => !Has(FrameFlags.HasError) ? null
    : ErrorCode == ErrorCode.Custom ? ErrorCustom
    : ErrorCodes.GetName(ErrorCode);

  public override string ToString() =>
    $"{Kind} {Flags} id={Id} name={NameCode} '{Name}' error={Error} '{ErrorMessage}' payload={Text}";

  public static Received From(scoped in Frame frame) => new()
  {
    Kind = frame.Kind,
    Flags = frame.Flags,
    Id = frame.Id,
    NameCode = frame.NameCode,
    Name = Encoding.ASCII.GetString(frame.NameText),
    Timeout = frame.Timeout,
    Trace = Encoding.ASCII.GetString(frame.Trace),
    Credit = frame.Credit,
    Amount = frame.Amount,
    Count = frame.Count,
    ErrorCode = frame.ErrorCode,
    ErrorCustom = Encoding.ASCII.GetString(frame.ErrorCustom),
    ErrorMessage = Encoding.UTF8.GetString(frame.ErrorMessage),
    Payload = frame.Payload.ToArray(),
  };
}

// A browser stand-in that speaks the protocol frame by frame, so tests control exactly what is sent.
sealed class TestClient : IAsyncDisposable
{
  public const string Subprotocol = "puregate.v1";
  public const string DefaultHello = """{"calls":16,"maxMessage":1048576}""";

  static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);

  readonly ClientWebSocket Socket;
  readonly Queue<Received> Buffered = new();
  readonly byte[] ReceiveBuffer = new byte[4 * 1024 * 1024];

  TestClient(ClientWebSocket socket) => Socket = socket;

  public JsonElement Welcome { get; private set; }

  public WebSocketState State => Socket.State;

  public WebSocketCloseStatus? CloseStatus => Socket.CloseStatus;

  public static async Task<TestClient> ConnectAsync(Uri uri, string subprotocol = Subprotocol, string origin = null)
  {
    var socket = new ClientWebSocket();
    if (subprotocol is not null)
      socket.Options.AddSubProtocol(subprotocol);
    if (origin is not null)
      socket.Options.SetRequestHeader("Origin", origin);
    socket.Options.CollectHttpResponseDetails = true;

    try
    {
      using var timeout = new CancellationTokenSource(DefaultTimeout);
      await socket.ConnectAsync(uri, timeout.Token);
    }
    catch (WebSocketException exception)
    {
      throw new UpgradeRefusedException((int)socket.HttpStatusCode, exception);
    }

    return new TestClient(socket);
  }

  // A Hello that resumes session, which this client has processed received frames of.
  public static string ResumeHello(string session, long received) =>
    "{\"calls\":16,\"maxMessage\":1048576,\"resume\":{\"session\":\"" + session + "\",\"received\":" + received + "}}";

  public string Session => Welcome.GetProperty("session").GetString();

  public async Task<Received> HelloAsync(string hello = DefaultHello)
  {
    await SendAsync(Outgoing.Hello(hello));
    var welcome = await ReceiveAsync();
    Assert.That(welcome.Kind, Is.EqualTo(FrameKind.Welcome));
    Welcome = welcome.Json();
    return welcome;
  }

  // Sends the frames as one message.
  public Task SendAsync(params Outgoing[] frames)
  {
    var output = new ArrayBufferWriter<byte>();
    foreach (var frame in frames)
      frame.WriteTo(output);

    return SendRawAsync(output.WrittenMemory.ToArray());
  }

  public async Task SendRawAsync(byte[] message, WebSocketMessageType type = WebSocketMessageType.Binary)
  {
    using var timeout = new CancellationTokenSource(DefaultTimeout);
    await Socket.SendAsync(message, type, endOfMessage: true, timeout.Token);
  }

  // The next frame, reading messages as needed. Fails when the server closes instead.
  public async Task<Received> ReceiveAsync(TimeSpan timeout = default)
  {
    while (Buffered.Count == 0)
    {
      var (type, length) = await ReceiveMessageAsync(timeout == default ? DefaultTimeout : timeout);
      if (type == WebSocketMessageType.Close)
        Assert.Fail($"The server closed with {Socket.CloseStatus}: {Socket.CloseStatusDescription}");

      var reader = new FrameReader(ReceiveBuffer.AsSpan(0, length), Side.Server);
      while (reader.TryRead(out var frame))
        Buffered.Enqueue(Received.From(in frame));
    }

    return Buffered.Dequeue();
  }

  // The next frame, or null once the server closed, for tests where a close is one of the expected outcomes.
  public async Task<Received> ReceiveOrNullAsync(TimeSpan timeout = default)
  {
    while (Buffered.Count == 0)
    {
      var (type, length) = await ReceiveMessageAsync(timeout == default ? DefaultTimeout : timeout);
      if (type == WebSocketMessageType.Close)
        return null;

      var reader = new FrameReader(ReceiveBuffer.AsSpan(0, length), Side.Server);
      while (reader.TryRead(out var frame))
        Buffered.Enqueue(Received.From(in frame));
    }

    return Buffered.Dequeue();
  }

  // The frames of the next message, for tests of how the server groups frames into messages.
  public async Task<List<Received>> ReceiveOneMessageAsync(TimeSpan timeout = default)
  {
    Assert.That(Buffered, Is.Empty, "Frames of an earlier message are still waiting.");
    var (type, length) = await ReceiveMessageAsync(timeout == default ? DefaultTimeout : timeout);
    if (type == WebSocketMessageType.Close)
      Assert.Fail($"The server closed with {Socket.CloseStatus}: {Socket.CloseStatusDescription}");

    var frames = new List<Received>();
    var reader = new FrameReader(ReceiveBuffer.AsSpan(0, length), Side.Server);
    while (reader.TryRead(out var frame))
      frames.Add(Received.From(in frame));

    return frames;
  }

  // The next frame of this kind and id, keeping the others for later.
  public async Task<Received> ReceiveAsync(FrameKind kind, uint id, TimeSpan timeout = default)
  {
    var skipped = new List<Received>();
    try
    {
      while (true)
      {
        var frame = await ReceiveAsync(timeout);
        if (frame.Kind == kind && frame.Id == id)
          return frame;

        skipped.Add(frame);
      }
    }
    finally
    {
      foreach (var frame in Buffered)
        skipped.Add(frame);
      Buffered.Clear();
      foreach (var frame in skipped)
        Buffered.Enqueue(frame);
    }
  }

  // Sends a Request and returns its Response.
  public async Task<Received> CallAsync(uint id, string name, string json = null)
  {
    await SendAsync(Outgoing.Request(id, name, json));
    return await ReceiveAsync(FrameKind.Response, id);
  }

  // Waits for the server to close, answering its close frame, and returns its close code.
  public async Task<WebSocketCloseStatus> ReceiveCloseAsync(TimeSpan timeout = default)
  {
    while (true)
    {
      var (type, _) = await ReceiveMessageAsync(timeout == default ? DefaultTimeout : timeout);
      if (type == WebSocketMessageType.Close)
        return Socket.CloseStatus ?? WebSocketCloseStatus.Empty;
    }
  }

  // Whether a frame arrives within the time: false means the server stayed quiet.
  public async Task<bool> ReceivesWithinAsync(TimeSpan time)
  {
    if (Buffered.Count > 0)
      return true;

    try
    {
      var (type, length) = await ReceiveMessageAsync(time);
      if (type == WebSocketMessageType.Close)
        return true;

      var reader = new FrameReader(ReceiveBuffer.AsSpan(0, length), Side.Server);
      while (reader.TryRead(out var frame))
        Buffered.Enqueue(Received.From(in frame));
      return true;
    }
    catch (TimeoutException)
    {
      return false;
    }
  }

  public async Task CloseAsync(WebSocketCloseStatus status = WebSocketCloseStatus.NormalClosure)
  {
    using var timeout = new CancellationTokenSource(DefaultTimeout);
    await Socket.CloseAsync(status, "", timeout.Token);
  }

  async Task<(WebSocketMessageType Type, int Length)> ReceiveMessageAsync(TimeSpan timeout)
  {
    if (PendingReceive is null)
      PendingReceive = ReceiveWholeMessageAsync();

    var completed = await Task.WhenAny(PendingReceive, Task.Delay(timeout));
    if (completed != PendingReceive)
      throw new TimeoutException("Nothing arrived in time.");

    var result = await PendingReceive;
    PendingReceive = null;
    return result;
  }

  // A receive that outlives a timed-out wait, so no message is lost to the timeout.
  Task<(WebSocketMessageType Type, int Length)> PendingReceive;

  async Task<(WebSocketMessageType Type, int Length)> ReceiveWholeMessageAsync()
  {
    var length = 0;
    while (true)
    {
      var result = await Socket.ReceiveAsync(ReceiveBuffer.AsMemory(length), CancellationToken.None);
      if (result.MessageType == WebSocketMessageType.Close)
      {
        if (Socket.State == WebSocketState.CloseReceived)
          await Socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None);
        return (WebSocketMessageType.Close, 0);
      }

      length += result.Count;
      if (result.EndOfMessage)
        return (result.MessageType, length);
    }
  }

  public ValueTask DisposeAsync()
  {
    Socket.Abort();
    Socket.Dispose();
    return ValueTask.CompletedTask;
  }
}

sealed class UpgradeRefusedException(int statusCode, Exception inner)
  : Exception($"The server refused the upgrade with HTTP {statusCode}.", inner)
{
  public int StatusCode { get; } = statusCode;
}
