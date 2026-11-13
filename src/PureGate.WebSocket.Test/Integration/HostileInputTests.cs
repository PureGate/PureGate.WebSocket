using System.Net.WebSockets;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Test.Integration;

// Hostile browsers against a real server: random and mutated messages, before and after the handshake. The server
// answers what it can, closes on the rest with the right code, and keeps serving everyone else.
public sealed class HostileInputTests
{
  const int Connections = 60;

  static readonly WebSocketCloseStatus[] Expected =
  [
    (WebSocketCloseStatus)CloseCodes.ProtocolError,
    WebSocketCloseStatus.MessageTooBig,
    WebSocketCloseStatus.NormalClosure,
  ];

  TestHost Host;

  [OneTimeSetUp]
  public async Task StartAsync() => Host = await TestHost.StartAsync();

  [OneTimeTearDown]
  public async Task StopAsync() => await Host.DisposeAsync();

  [Test]
  public async Task ClosesOnRandomHandshakes()
  {
    var random = new Random(42);
    for (var i = 0; i < Connections; i++)
    {
      await using var client = await Host.ConnectAsync();
      var message = new byte[random.Next(1, 200)];
      random.NextBytes(message);
      await client.SendRawAsync(message);

      Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)CloseCodes.ProtocolError));
    }

    await AssertStillServingAsync();
  }

  [Test]
  public async Task SurvivesMutatedFramesAfterTheHandshake()
  {
    var valid = Vectors.Get("messages", "valid").EnumerateArray()
      .Where(vector => Vectors.From(vector) == Side.Client)
      .Select(Vectors.Bytes)
      .ToArray();
    var random = new Random(43);
    for (var i = 0; i < Connections; i++)
    {
      await using var client = await Host.ConnectAndHelloAsync();
      for (var message = 0; message < 5 && client.State == WebSocketState.Open; message++)
      {
        try
        {
          await client.SendRawAsync(FuzzTests.Mutate(valid[random.Next(valid.Length)], random));
        }
        catch (WebSocketException)
        {
          break;
        }
      }

      // Whatever the frames were, the server either still answers or has closed with a fitting code.
      if (await ProbeAsync(client) is { } closed)
        Assert.That(closed, Is.AnyOf(Expected));
    }

    await AssertStillServingAsync();
  }

  [Test]
  public async Task ClosesOnTextMessages()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendRawAsync("{\"hello\":1}"u8.ToArray(), WebSocketMessageType.Text);

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo(WebSocketCloseStatus.InvalidMessageType));
  }

  [Test]
  public async Task ClosesOnAMessagePastTheLimit()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendRawAsync(new byte[70 * 1024]);

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo(WebSocketCloseStatus.MessageTooBig));
  }

  // Null while the server still answers a call; otherwise how it closed. A server that does neither fails the test
  // with a timeout.
  static async Task<WebSocketCloseStatus?> ProbeAsync(TestClient client)
  {
    const uint probe = 4_000_000_000;
    if (client.State != WebSocketState.Open)
      return client.CloseStatus ?? WebSocketCloseStatus.Empty;

    try
    {
      await client.SendAsync(Outgoing.Request(probe, "math.add", """{"a":1,"b":1}"""));
      while (await client.ReceiveOrNullAsync(TimeSpan.FromSeconds(5)) is { } frame)
      {
        if (frame.Kind == FrameKind.Response && frame.Id == probe)
          return frame.Text == "2" ? null : WebSocketCloseStatus.Empty;
      }
    }
    catch (WebSocketException)
    {
    }

    return client.CloseStatus ?? WebSocketCloseStatus.Empty;
  }

  async Task AssertStillServingAsync()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    Assert.That((await client.CallAsync(1, "math.add", """{"a":20,"b":22}""")).Text, Is.EqualTo("42"));
  }
}
