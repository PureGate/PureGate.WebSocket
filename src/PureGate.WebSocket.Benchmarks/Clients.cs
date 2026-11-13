using System.Buffers;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Benchmarks;

// One connection of the system under test, doing what the scenarios need.
interface IBenchClient : IAsyncDisposable
{
  Task<string> EchoAsync(string text);

  Task BroadcastAsync(string text);

  // Called with every broadcast this connection receives.
  Action<string> OnTick { set; }
}

// A minimal PureGate.WebSocket client on the library's own frame codec: Hello, Requests, Acks, and the events it gets.
// It sends the way the browser client does: every call carries the browser's default deadline, and frames written
// while a message is going out are coalesced into the next ones, each within the server's message limit.
sealed class PureGateClient : IBenchClient
{
  const int AckEvery = 64;

  // The browser client's default deadline, which it sends with every call.
  const uint Deadline = 30_000;

  // 64 KiB, as much as the server coalesces into one message.
  static readonly byte[] Hello = """{"calls":128,"maxMessage":65536}"""u8.ToArray();

  [ThreadStatic] static ArrayBufferWriter<byte> PayloadBuffer;
  [ThreadStatic] static Utf8JsonWriter PayloadWriter;

  readonly ClientWebSocket Socket = new();
  readonly Lock Gate = new();
  readonly OutgoingNameTable Names = new();
  readonly ConcurrentDictionary<uint, TaskCompletionSource<string>> Calls = new();
  readonly TaskCompletionSource Welcomed = new(TaskCreationOptions.RunContinuationsAsynchronously);
  readonly byte[] ReceiveBuffer = new byte[64 * 1024];
  ArrayBufferWriter<byte> Pending = new(4096);
  ArrayBufferWriter<byte> Outgoing = new(4096);

  // Where each message in Pending ends, but the last.
  List<int> PendingEnds = [];
  List<int> OutgoingEnds = [];

  // The largest message the server accepts: the protocol's minimum until Welcome says.
  int MaxMessage = 16 * 1024;
  bool Sending;
  Task Receiving;
  int LastId;
  long ReceivedCount;
  int Unacknowledged;

  public Action<string> OnTick { get; set; }

  public static async Task<IBenchClient> ConnectAsync(Uri uri)
  {
    var client = new PureGateClient();
    client.Socket.Options.AddSubProtocol("puregate.v1");
    await client.Socket.ConnectAsync(uri, CancellationToken.None);
    client.Write(new Frame { Kind = FrameKind.Hello, Payload = Hello });
    client.Receiving = client.ReceiveAsync();
    await client.Welcomed.Task;
    return client;
  }

  public Task<string> EchoAsync(string text) => CallAsync("bench.echo", text);

  public Task BroadcastAsync(string text) => CallAsync("bench.broadcast", text);

  Task<string> CallAsync(string name, string text)
  {
    var id = (uint)Interlocked.Increment(ref LastId);
    var answer = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
    Calls[id] = answer;

    var payload = PayloadBuffer ??= new ArrayBufferWriter<byte>(2048);
    payload.ResetWrittenCount();
    var writer = PayloadWriter ??= new Utf8JsonWriter(payload);
    writer.Reset(payload);
    JsonSerializer.Serialize(writer, text, BenchJson.Default.String);

    Write(new Frame { Kind = FrameKind.Request, Flags = FrameFlags.HasTimeout, Id = id, Timeout = Deadline, Payload = payload.WrittenSpan }, name);
    return answer.Task;
  }

  // Copies the frame into the next message, or a new one when it would not fit; the writer that finds no send running
  // starts one.
  void Write(scoped in Frame frame, string name = null)
  {
    bool start;
    lock (Gate)
    {
      var copy = frame;
      if (name is not null)
      {
        copy.NameCode = Names.Next(name);
        copy.NameText = FrameLayout.NameHasText(copy.NameCode) ? Encoding.ASCII.GetBytes(name) : default;
      }

      var message = PendingEnds.Count > 0 ? PendingEnds[^1] : 0;
      if (Pending.WrittenCount > message && Pending.WrittenCount - message + FrameWriter.GetLength(in copy) > MaxMessage)
        PendingEnds.Add(Pending.WrittenCount);

      FrameWriter.Write(Pending, in copy);
      start = !Sending;
      Sending = true;
    }

    if (start)
      _ = SendAsync();
  }

  async Task SendAsync()
  {
    try
    {
      while (true)
      {
        ArrayBufferWriter<byte> next;
        List<int> ends;
        lock (Gate)
        {
          if (Pending.WrittenCount == 0)
          {
            Sending = false;
            return;
          }

          next = Pending;
          Pending = Outgoing;
          Outgoing = next;
          ends = PendingEnds;
          PendingEnds = OutgoingEnds;
          OutgoingEnds = ends;
        }

        var start = 0;
        foreach (var end in ends)
        {
          await Socket.SendAsync(next.WrittenMemory[start..end], WebSocketMessageType.Binary, endOfMessage: true, CancellationToken.None);
          start = end;
        }

        await Socket.SendAsync(next.WrittenMemory[start..], WebSocketMessageType.Binary, endOfMessage: true, CancellationToken.None);
        next.ResetWrittenCount();
        ends.Clear();
      }
    }
    catch (Exception exception) when (exception is WebSocketException or ObjectDisposedException)
    {
    }
  }

  async Task ReceiveAsync()
  {
    try
    {
      while (Socket.State == WebSocketState.Open)
      {
        var length = 0;
        ValueWebSocketReceiveResult result;
        do
        {
          result = await Socket.ReceiveAsync(ReceiveBuffer.AsMemory(length), CancellationToken.None);
          length += result.Count;
        }
        while (!result.EndOfMessage && result.MessageType != WebSocketMessageType.Close);

        if (result.MessageType == WebSocketMessageType.Close)
          break;

        Process(length);
      }
    }
    catch (WebSocketException)
    {
    }
    finally
    {
      foreach (var call in Calls.Values)
        call.TrySetException(new WebSocketException("The connection closed."));
    }
  }

  // Handles one message, then acknowledges what it processed when an Ack is due.
  void Process(int length)
  {
    var reader = new FrameReader(ReceiveBuffer.AsSpan(0, length), Side.Server);
    var sequenced = 0;
    while (reader.TryRead(out var frame))
    {
      if (FrameLayout.IsSequenced(frame.Kind))
        sequenced++;

      switch (frame.Kind)
      {
        case FrameKind.Welcome:
          var maxMessage = MaxMessageOf(frame.Payload);
          lock (Gate)
            MaxMessage = maxMessage;
          Welcomed.TrySetResult();
          break;
        case FrameKind.Response when Calls.TryRemove(frame.Id, out var answer):
          if (frame.Has(FrameFlags.HasError))
            answer.TrySetException(new InvalidOperationException(Encoding.UTF8.GetString(frame.ErrorMessage)));
          else
            answer.TrySetResult(frame.Payload.IsEmpty ? null : JsonSerializer.Deserialize(frame.Payload, BenchJson.Default.String));
          break;
        case FrameKind.Event:
          OnTick?.Invoke(JsonSerializer.Deserialize(frame.Payload, BenchJson.Default.String));
          break;
      }
    }

    ReceivedCount += sequenced;
    Unacknowledged += sequenced;
    if (Unacknowledged < AckEvery)
      return;

    Unacknowledged = 0;
    Write(new Frame { Kind = FrameKind.Ack, Count = (uint)ReceivedCount });
  }

  // The server's message limit, from its Welcome.
  static int MaxMessageOf(ReadOnlySpan<byte> welcome)
  {
    var reader = new Utf8JsonReader(welcome);
    while (reader.Read())
    {
      if (reader.TokenType == JsonTokenType.PropertyName && reader.CurrentDepth == 1 && reader.ValueTextEquals("maxMessage"u8))
      {
        reader.Read();
        return reader.GetInt32();
      }
    }

    throw new InvalidDataException("Welcome has no maxMessage.");
  }

  public async ValueTask DisposeAsync()
  {
    try
    {
      using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      await Socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "", timeout.Token);
    }
    catch (Exception exception) when (exception is WebSocketException or OperationCanceledException)
    {
    }

    Socket.Dispose();
    if (Receiving is not null)
      await Receiving;
  }
}

// SignalR's own .NET client, over WebSockets only, with the JSON or the MessagePack protocol.
sealed class SignalRClient : IBenchClient
{
  readonly HubConnection Connection;

  SignalRClient(HubConnection connection) => Connection = connection;

  public Action<string> OnTick { get; set; }

  public static async Task<IBenchClient> ConnectAsync(Uri server, bool messagePack)
  {
    var builder = new HubConnectionBuilder().WithUrl(new Uri(server, "/signalr"), static options =>
    {
      options.Transports = HttpTransportType.WebSockets;
      options.SkipNegotiation = true;
    });
    if (messagePack)
      builder.AddMessagePackProtocol();

    var client = new SignalRClient(builder.Build());
    client.Connection.On<string>("tick", text => client.OnTick?.Invoke(text));
    await client.Connection.StartAsync();
    return client;
  }

  public Task<string> EchoAsync(string text) => Connection.InvokeAsync<string>("Echo", text);

  public Task BroadcastAsync(string text) => Connection.InvokeAsync("Broadcast", text);

  public ValueTask DisposeAsync() => Connection.DisposeAsync();
}
