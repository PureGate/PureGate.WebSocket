using System.Net.WebSockets;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.TestServer;

namespace PureGate.WebSocket.Test.Integration;

public sealed class ResumeTests
{
  static readonly TimeSpan Quiet = TimeSpan.FromMilliseconds(200);

  TestHost Host;

  [OneTimeSetUp]
  public async Task StartAsync() => Host = await TestHost.StartAsync();

  [OneTimeTearDown]
  public async Task StopAsync() => await Host.DisposeAsync();

  [Test]
  public async Task AnnouncesTheSessionAndTheResumeWindow()
  {
    await using var client = await Host.ConnectAndHelloAsync();

    Assert.That(client.Session, Has.Length.GreaterThanOrEqualTo(22));
    Assert.That(client.Welcome.GetProperty("resumed").GetBoolean(), Is.False);
    Assert.That(client.Welcome.GetProperty("resumeWindow").GetInt64(), Is.EqualTo(30_000));
  }

  [Test]
  public async Task AcknowledgesWhatItProcessed()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    // Cancels for exchanges that already ended are sequenced and ignored: they count, within no window.
    await client.SendAsync([.. Enumerable.Range(1, 64).Select(id => Outgoing.Cancel((uint)id))]);
    Assert.That((await client.ReceiveAsync(FrameKind.Ack, 0)).Count, Is.EqualTo(64));

    // Fewer than 64: within a second.
    await client.SendAsync(Outgoing.Cancel(65), Outgoing.Cancel(66));
    Assert.That((await client.ReceiveAsync(FrameKind.Ack, 0, TimeSpan.FromSeconds(3))).Count, Is.EqualTo(66));
  }

  [Test]
  public async Task ResumesAndSendsWhatTheBrowserMissed()
  {
    var first = await Host.ConnectAndHelloAsync();
    var session = first.Session;
    Assert.That((await first.CallAsync(1, "echo.text", "\"one\"")).Text, Is.EqualTo("\"one\""));
    await first.DisposeAsync();

    await using var second = await Host.ConnectAsync();
    await second.HelloAsync(TestClient.ResumeHello(session, 0));

    Assert.That(second.Welcome.GetProperty("resumed").GetBoolean(), Is.True);
    Assert.That(second.Session, Is.EqualTo(session));
    Assert.That(second.Welcome.GetProperty("received").GetInt64(), Is.EqualTo(1));
    var replayed = await second.ReceiveAsync(FrameKind.Response, 1);
    Assert.That(replayed.Text, Is.EqualTo("\"one\""));
  }

  [Test]
  public async Task SendsNothingTheBrowserAlreadyProcessed()
  {
    var first = await Host.ConnectAndHelloAsync();
    var session = first.Session;
    await first.CallAsync(1, "echo.text", "\"one\"");
    await first.DisposeAsync();

    await using var second = await Host.ConnectAsync();
    await second.HelloAsync(TestClient.ResumeHello(session, 1));

    Assert.That(second.Welcome.GetProperty("resumed").GetBoolean(), Is.True);
    Assert.That(await second.ReceivesWithinAsync(Quiet), Is.False);
  }

  [Test]
  public async Task KeepsRunningWhileTheBrowserIsAway()
  {
    var first = await Host.ConnectAndHelloAsync();
    var session = first.Session;
    await first.SendAsync(Outgoing.Request(1, "calls.delay", "200"));
    await Task.Delay(50);
    await first.DisposeAsync();
    await Task.Delay(400);

    await using var second = await Host.ConnectAsync();
    await second.HelloAsync(TestClient.ResumeHello(session, 0));

    Assert.That((await second.ReceiveAsync(FrameKind.Response, 1)).Text, Is.EqualTo("\"done\""));
  }

  [Test]
  public async Task KeepsTheNameTablesAcrossAResume()
  {
    var first = await Host.ConnectAndHelloAsync();
    var session = first.Session;
    var defined = Outgoing.Request(1, "echo.text", "\"a\"");
    defined.NameCode = 3;
    await first.SendAsync(defined);
    await first.ReceiveAsync(FrameKind.Response, 1);
    await first.DisposeAsync();

    await using var second = await Host.ConnectAsync();
    await second.HelloAsync(TestClient.ResumeHello(session, 1));
    var referred = Outgoing.Request(2, null, "\"b\"");
    referred.NameCode = 2;
    await second.SendAsync(referred);

    Assert.That((await second.ReceiveAsync(FrameKind.Response, 2)).Text, Is.EqualTo("\"b\""));
  }

  [Test]
  public async Task ReplacesAConnectionThatIsStillOpen()
  {
    await using var first = await Host.ConnectAndHelloAsync();
    await using var second = await Host.ConnectAsync();
    await second.HelloAsync(TestClient.ResumeHello(first.Session, 0));

    Assert.That(second.Welcome.GetProperty("resumed").GetBoolean(), Is.True);
    Assert.That(await first.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4005));
    Assert.That((await second.CallAsync(1, "math.add", """{"a":1,"b":2}""")).Text, Is.EqualTo("3"));
  }

  [Test]
  public async Task StartsANewSessionForOneItDoesNotHave()
  {
    await using var client = await Host.ConnectAsync();
    await client.HelloAsync(TestClient.ResumeHello("not-a-session", 0));

    Assert.That(client.Welcome.GetProperty("resumed").GetBoolean(), Is.False);
    Assert.That(client.Session, Is.Not.EqualTo("not-a-session"));
    Assert.That((await client.CallAsync(1, "math.add", """{"a":1,"b":2}""")).Text, Is.EqualTo("3"));
  }

  [Test]
  public async Task EndsASessionThatCanNoLongerReplayWhatTheBrowserMissed()
  {
    var first = await Host.ConnectAndHelloAsync();
    var session = first.Session;
    await first.CallAsync(1, "echo.text", "\"one\"");
    await first.SendAsync(Outgoing.Ack(1));
    await Task.Delay(50);
    await first.DisposeAsync();

    // The Response was acknowledged and dropped, so a browser that never processed it cannot resume.
    await using var second = await Host.ConnectAsync();
    await second.HelloAsync(TestClient.ResumeHello(session, 0));
    Assert.That(second.Welcome.GetProperty("resumed").GetBoolean(), Is.False);

    await using var third = await Host.ConnectAsync();
    await third.HelloAsync(TestClient.ResumeHello(session, 1));
    Assert.That(third.Welcome.GetProperty("resumed").GetBoolean(), Is.False);
  }

  [Test]
  public async Task ClosesAConnectionThatAcknowledgesWhatWasNeverSent()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Ack(5));

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  [Test]
  public async Task EndsTheSessionWhenTheBrowserClosesIt()
  {
    var first = await Host.ConnectAndHelloAsync();
    var session = first.Session;
    await first.CloseAsync();
    await first.DisposeAsync();

    await using var second = await Host.ConnectAsync();
    await second.HelloAsync(TestClient.ResumeHello(session, 0));

    Assert.That(second.Welcome.GetProperty("resumed").GetBoolean(), Is.False);
  }

  [TestCase(WebSocketCloseStatus.EndpointUnavailable)]
  [TestCase((WebSocketCloseStatus)4006)]
  public async Task KeepsTheSessionWhenThePageGoesAwayOrWillResume(WebSocketCloseStatus status)
  {
    var first = await Host.ConnectAndHelloAsync();
    var session = first.Session;
    await first.CloseAsync(status);
    await first.DisposeAsync();

    await using var second = await Host.ConnectAsync();
    await second.HelloAsync(TestClient.ResumeHello(session, 0));

    Assert.That(second.Welcome.GetProperty("resumed").GetBoolean(), Is.True);
  }

  [Test]
  public async Task EndsASessionNobodyResumedInTime()
  {
    var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var host = await TestHost.StartAsync(
      configure: static options => options.ResumeWindow = TimeSpan.FromMilliseconds(200),
      map: ws => ws.OnDisconnected((_, _) =>
      {
        disconnected.TrySetResult();
        return ValueTask.CompletedTask;
      }));

    var first = await host.ConnectAndHelloAsync();
    var session = first.Session;
    await first.DisposeAsync();
    await disconnected.Task.WaitAsync(TimeSpan.FromSeconds(5));

    await using var second = await host.ConnectAsync();
    await second.HelloAsync(TestClient.ResumeHello(session, 0));
    Assert.That(second.Welcome.GetProperty("resumed").GetBoolean(), Is.False);
  }

  [Test]
  public async Task DoesNotResumeWithResumeOff()
  {
    await using var host = await TestHost.StartAsync(configure: static options => options.ResumeWindow = TimeSpan.Zero);
    var first = await host.ConnectAndHelloAsync();
    var session = first.Session;
    Assert.That(first.Welcome.GetProperty("resumeWindow").GetInt64(), Is.Zero);
    await first.DisposeAsync();

    await using var second = await host.ConnectAsync();
    await second.HelloAsync(TestClient.ResumeHello(session, 0));
    Assert.That(second.Welcome.GetProperty("resumed").GetBoolean(), Is.False);
  }

  [Test]
  public async Task SendsTheRouteForLoadBalancers()
  {
    await using var host = await TestHost.StartAsync(configure: static options => options.ResumeRoute = "east-2");
    await using var client = await host.ConnectAndHelloAsync();

    Assert.That(client.Welcome.GetProperty("route").GetString(), Is.EqualTo("east-2"));
  }

  // Slow consumers.

  [Test]
  public async Task ClosesAPageTooFarBehindAndLetsItResume()
  {
    await using var host = await TestHost.StartAsync(
      configure: static options => options.MaxUnacknowledgedFrames = 64,
      map: static ws => ws.MapRequest<int>("test.flood", static async (count, call, cancellationToken) =>
      {
        for (var i = 0; i < count; i++)
          await call.Connection.SendAsync(TestApi.Received, $"event {i}", cancellationToken);
      }));

    var first = await host.ConnectAndHelloAsync();
    var session = first.Session;
    await first.SendAsync(Outgoing.Request(1, "test.flood", "100"));
    Assert.That(await first.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4003));
    await first.DisposeAsync();

    await using var second = await host.ConnectAsync();
    await second.HelloAsync(TestClient.ResumeHello(session, 0));
    Assert.That(second.Welcome.GetProperty("resumed").GetBoolean(), Is.True);
    var first10 = new List<string>();
    for (var i = 0; i < 10; i++)
      first10.Add((await second.ReceiveAsync(FrameKind.Event, 0)).Text);
    Assert.That(first10, Is.EqualTo(Enumerable.Range(0, 10).Select(i => $"\"event {i}\"")));
  }

  [Test]
  public async Task DropsEventsForAPageTooFarBehindWhenAsked()
  {
    await using var host = await TestHost.StartAsync(
      configure: static options =>
      {
        options.MaxUnacknowledgedFrames = 64;
        options.SlowConsumer = SlowConsumerPolicy.DropEvents;
      },
      map: static ws => ws.MapRequest<int>("test.flood", static async (count, call, cancellationToken) =>
      {
        for (var i = 0; i < count; i++)
          await call.Connection.SendAsync(TestApi.Received, $"event {i}", cancellationToken);
      }));

    await using var client = await host.ConnectAndHelloAsync();
    var response = await client.CallAsync(1, "test.flood", "100");
    Assert.That(response.Has(FrameFlags.HasError), Is.False);

    var events = 0;
    while (await client.ReceivesWithinAsync(Quiet))
    {
      if ((await client.ReceiveAsync()).Kind == FrameKind.Event)
        events++;
    }

    Assert.That(events, Is.InRange(60, 70));
    Assert.That(client.State, Is.EqualTo(WebSocketState.Open));

    // Caught up: events flow again.
    await client.SendAsync(Outgoing.Ack((uint)(events + 1)));
    await client.CallAsync(2, "events.echo", "\"back\"");
    Assert.That((await client.ReceiveAsync(FrameKind.Event, 0)).Text, Is.EqualTo("\"back\""));
  }

  [Test]
  public async Task EndsASessionWhoseReplayBufferFillsWhileTheBrowserIsAway()
  {
    var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var host = await TestHost.StartAsync(
      configure: static options => options.ReplayBufferBytes = options.MaxMessageBytes,
      map: ws =>
      {
        ws.MapRequest("test.later", static async (call, cancellationToken) =>
        {
          await Task.Delay(300, cancellationToken);
          for (var i = 0; i < 100; i++)
            await call.Connection.SendAsync(TestApi.Received, new string('x', 2000), cancellationToken);
        });
        ws.OnDisconnected((_, _) =>
        {
          disconnected.TrySetResult();
          return ValueTask.CompletedTask;
        });
      });

    var first = await host.ConnectAndHelloAsync();
    var session = first.Session;
    await first.SendAsync(Outgoing.Request(1, "test.later"));
    await Task.Delay(50);
    await first.DisposeAsync();

    await disconnected.Task.WaitAsync(TimeSpan.FromSeconds(5));
    await using var second = await host.ConnectAsync();
    await second.HelloAsync(TestClient.ResumeHello(session, 0));
    Assert.That(second.Welcome.GetProperty("resumed").GetBoolean(), Is.False);
  }

  // Shutting down.

  [Test]
  public async Task DrainsOnShutdown()
  {
    await using var host = await TestHost.StartAsync();
    await using var client = await host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Request(1, "calls.delay", "300"));
    await Task.Delay(50);

    var stopping = host.StopAsync();
    await client.ReceiveAsync(FrameKind.GoAway, 0);

    // Still handling what arrives, without starting anything of its own: the event is not sent.
    Assert.That((await client.CallAsync(2, "events.echo", "\"late\"")).Has(FrameFlags.HasError), Is.False);
    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Text, Is.EqualTo("\"done\""));
    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo(WebSocketCloseStatus.EndpointUnavailable));
    await stopping.WaitAsync(TimeSpan.FromSeconds(10));
  }

  [Test]
  public async Task ClosesAtOnceOnShutdownWithNothingInProgress()
  {
    await using var host = await TestHost.StartAsync();
    await using var client = await host.ConnectAndHelloAsync();

    var stopping = host.StopAsync();
    await client.ReceiveAsync(FrameKind.GoAway, 0);
    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo(WebSocketCloseStatus.EndpointUnavailable));
    await stopping.WaitAsync(TimeSpan.FromSeconds(5));
  }

  [Test]
  public async Task StopsDrainingAfterTheDrainTimeout()
  {
    await using var host = await TestHost.StartAsync(configure: static options => options.DrainTimeout = TimeSpan.FromMilliseconds(200));
    await using var client = await host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Request(1, "calls.delay", "10000"));
    await Task.Delay(50);

    var stopping = host.StopAsync();
    await client.ReceiveAsync(FrameKind.GoAway, 0);
    Assert.That(await client.ReceiveCloseAsync(TimeSpan.FromSeconds(5)), Is.EqualTo(WebSocketCloseStatus.EndpointUnavailable));
    await stopping.WaitAsync(TimeSpan.FromSeconds(5));
  }
}
