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
sealed class PureGateClient : IBenchClient
{
  const int AckEvery = 64;

  static readonly byte[] Hello = """{"calls":128,"maxMessage":1048576}"""u8.ToArray();

  readonly ClientWebSocket Socket = new();
  readonly SemaphoreSlim SendGate = new(1, 1);
  readonly OutgoingNameTable Names = new();
  readonly ConcurrentDictionary<uint, TaskCompletionSource<string>> Pending = new();
  readonly TaskCompletionSource Welcomed = new(TaskCreationOptions.RunContinuationsAsynchronously);
  readonly byte[] ReceiveBuffer = new byte[1024 * 1024];
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
    await client.SendAsync(Encode(new Frame { Kind = FrameKind.Hello, Payload = Hello }));
    client.Receiving = client.ReceiveAsync();
    await client.Welcomed.Task;
    return client;
  }

  public async Task<string> EchoAsync(string text) => await CallAsync("bench.echo", text);

  public Task BroadcastAsync(string text) => CallAsync("bench.broadcast", text);

  async Task<string> CallAsync(string name, string text)
  {
    var id = (uint)Interlocked.Increment(ref LastId);
    var answer = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
    Pending[id] = answer;
    var payload = JsonSerializer.SerializeToUtf8Bytes(text, BenchJson.Default.String);

    await SendGate.WaitAsync();
    try
    {
      var code = Names.Next(name);
      var nameText = FrameLayout.NameHasText(code) ? Encoding.ASCII.GetBytes(name) : [];
      var message = Encode(new Frame { Kind = FrameKind.Request, Id = id, NameCode = code, NameText = nameText, Payload = payload });
      await Socket.SendAsync(message, WebSocketMessageType.Binary, endOfMessage: true, CancellationToken.None);
    }
    finally
    {
      SendGate.Release();
    }

    return await answer.Task;
  }

  async Task SendAsync(ReadOnlyMemory<byte> message)
  {
    await SendGate.WaitAsync();
    try
    {
      await Socket.SendAsync(message, WebSocketMessageType.Binary, endOfMessage: true, CancellationToken.None);
    }
    finally
    {
      SendGate.Release();
    }
  }

  static byte[] Encode(scoped in Frame frame)
  {
    var output = new ArrayBufferWriter<byte>();
    FrameWriter.Write(output, in frame);
    return output.WrittenSpan.ToArray();
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

        if (Process(length) is { } ack)
          _ = SendAsync(ack);
      }
    }
    catch (WebSocketException)
    {
    }
    finally
    {
      foreach (var pending in Pending.Values)
        pending.TrySetException(new WebSocketException("The connection closed."));
    }
  }

  // Handles one message; returns an Ack to send when one is due.
  byte[] Process(int length)
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
          Welcomed.TrySetResult();
          break;
        case FrameKind.Response when Pending.TryRemove(frame.Id, out var answer):
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
      return null;

    Unacknowledged = 0;
    return Encode(new Frame { Kind = FrameKind.Ack, Count = (uint)ReceivedCount });
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
