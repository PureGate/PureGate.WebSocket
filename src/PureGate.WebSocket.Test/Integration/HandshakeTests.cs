using System.Net.WebSockets;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Test.Integration;

public sealed class HandshakeTests
{
  TestHost Host;

  [OneTimeSetUp]
  public async Task StartAsync() => Host = await TestHost.StartAsync();

  [OneTimeTearDown]
  public async Task StopAsync() => await Host.DisposeAsync();

  [Test]
  public async Task WelcomeAnnouncesTheServersLimits()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    var welcome = client.Welcome;

    Assert.That(welcome.GetProperty("connection").GetString(), Is.Not.Empty);
    Assert.That(welcome.GetProperty("session").GetString(), Has.Length.GreaterThanOrEqualTo(22));
    Assert.That(welcome.GetProperty("resumed").GetBoolean(), Is.False);
    Assert.That(welcome.GetProperty("calls").GetInt32(), Is.EqualTo(8));
    Assert.That(welcome.GetProperty("events").GetInt32(), Is.EqualTo(16));
    Assert.That(welcome.GetProperty("maxMessage").GetInt32(), Is.EqualTo(64 * 1024));
    Assert.That(welcome.GetProperty("heartbeat").GetInt64(), Is.EqualTo(15_000));
    Assert.That(welcome.TryGetProperty("contract", out _), Is.False);
  }

  [Test]
  public async Task AnswersCallsPipelinedWithHelloAfterWelcome()
  {
    await using var client = await Host.ConnectAsync();
    await client.SendAsync(
      Outgoing.Hello(TestClient.DefaultHello),
      Outgoing.Request(1, "echo.text", "\"first\""),
      Outgoing.Request(2, "math.add", """{"a":2,"b":3}"""));

    Assert.That((await client.ReceiveAsync()).Kind, Is.EqualTo(FrameKind.Welcome));
    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Text, Is.EqualTo("\"first\""));
    Assert.That((await client.ReceiveAsync(FrameKind.Response, 2)).Text, Is.EqualTo("5"));
  }

  [Test]
  public async Task AnswersPingWithPong()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(new Outgoing { Kind = FrameKind.Ping });

    Assert.That((await client.ReceiveAsync()).Kind, Is.EqualTo(FrameKind.Pong));
  }

  [Test]
  public async Task ClosesNormallyWhenTheBrowserDoes()
  {
    var client = await Host.ConnectAndHelloAsync();
    await client.CloseAsync();

    Assert.That(client.State, Is.EqualTo(WebSocketState.Closed));
    await client.DisposeAsync();
  }

  [TestCase("""{"calls":16}""", TestName = "Hello without maxMessage")]
  [TestCase("""{"calls":0,"maxMessage":1048576}""", TestName = "Hello with calls 0")]
  [TestCase("""{"calls":4,"maxMessage":1000}""", TestName = "Hello with a maxMessage below 16 KiB")]
  [TestCase("not json", TestName = "Hello that is not JSON")]
  public async Task RefusesAnInvalidHello(string hello)
  {
    await using var client = await Host.ConnectAsync();
    await client.SendAsync(Outgoing.Hello(hello));

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  [Test]
  public async Task RefusesAFirstFrameThatIsNotHello()
  {
    await using var client = await Host.ConnectAsync();
    await client.SendAsync(Outgoing.Request(1, "echo.text", "\"x\""));

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  [Test]
  public async Task RefusesASecondHello()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Hello(TestClient.DefaultHello));

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  [Test]
  public async Task ClosesOnMalformedFrames()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendRawAsync([0x01, 0x11]);

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  [Test]
  public async Task ClosesOnTextMessages()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendRawAsync("hello"u8.ToArray(), WebSocketMessageType.Text);

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo(WebSocketCloseStatus.InvalidMessageType));
  }

  [Test]
  public async Task ClosesOnMessagesPastTheLimit()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendRawAsync(new byte[64 * 1024 + 1]);

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo(WebSocketCloseStatus.MessageTooBig));
  }

  [Test]
  public async Task ClosesWhenNoHelloComesInTime()
  {
    await using var host = await TestHost.StartAsync(options => options.HandshakeTimeout = TimeSpan.FromMilliseconds(200));
    await using var client = await host.ConnectAsync();

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4002));
  }

  [Test]
  public async Task RefusesUpgradesWithoutTheSubprotocol()
  {
    var refused = await Assert.ThrowsAsync<UpgradeRefusedException>(() => Host.ConnectAsync(subprotocol: null));

    Assert.That(refused.StatusCode, Is.EqualTo(400));
  }

  [Test]
  public async Task RefusesOtherOriginsByDefault()
  {
    await using var host = await TestHost.StartAsync(options => options.AllowedOrigins = []);

    var refused = await Assert.ThrowsAsync<UpgradeRefusedException>(() => host.ConnectAsync(origin: "https://evil.example"));
    Assert.That(refused.StatusCode, Is.EqualTo(403));

    await using var sameOrigin = await host.ConnectAsync(origin: host.Origin);
    await sameOrigin.HelloAsync();
    await using var noOrigin = await host.ConnectAsync();
    await noOrigin.HelloAsync();
  }

  [Test]
  public async Task AcceptsListedOrigins()
  {
    await using var host = await TestHost.StartAsync(options => options.AllowedOrigins = ["https://app.example"]);

    await using var listed = await host.ConnectAsync(origin: "https://app.example");
    await listed.HelloAsync();
    await Assert.ThrowsAsync<UpgradeRefusedException>(() => host.ConnectAsync(origin: host.Origin));
  }

  [Test]
  public async Task RunsConnectedAndDisconnectedHandlers()
  {
    var connected = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
    var disconnected = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var host = await TestHost.StartAsync(map: ws => ws
      .OnConnected((connection, _) =>
      {
        connected.TrySetResult(connection.Id);
        return ValueTask.CompletedTask;
      })
      .OnDisconnected((connection, _) =>
      {
        disconnected.TrySetResult(connection.Id);
        return ValueTask.CompletedTask;
      }));

    var client = await host.ConnectAndHelloAsync();
    var id = client.Welcome.GetProperty("connection").GetString();
    Assert.That(await connected.Task.WaitAsync(TimeSpan.FromSeconds(10)), Is.EqualTo(id));

    await client.CloseAsync();
    Assert.That(await disconnected.Task.WaitAsync(TimeSpan.FromSeconds(10)), Is.EqualTo(id));
    await client.DisposeAsync();
  }
}
