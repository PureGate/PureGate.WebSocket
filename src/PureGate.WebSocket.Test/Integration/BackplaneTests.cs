using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.TestServer;

namespace PureGate.WebSocket.Test.Integration;

// Servers joined by a backplane (docs/PLAN-SERVER.md, Deploys and several instances). Two TestHosts share a TestBus,
// which stands in for the backend. Connections authenticate by token, so they have users.
public sealed class BackplaneTests
{
  static readonly ServerEvent<byte[]> Bytes = new("backplane.bytes");

  TestBus Bus;
  TestBackplane FirstBackplane;
  TestBackplane SecondBackplane;
  TestHost First;
  TestHost Second;

  [OneTimeSetUp]
  public async Task StartAsync()
  {
    Bus = new TestBus();
    (First, FirstBackplane) = await StartServerAsync(Bus);
    (Second, SecondBackplane) = await StartServerAsync(Bus);
  }

  [OneTimeTearDown]
  public async Task StopAsync()
  {
    await First.DisposeAsync();
    await Second.DisposeAsync();
    Assert.That(Bus.Violations, Is.Empty);
  }

  [Test]
  public async Task ReachesEveryoneOnEveryServer()
  {
    await using var here = await ConnectAsync(First);
    await using var there = await ConnectAsync(Second);

    await here.CallAsync(1, "events.broadcast", """{"text":"all","number":1}""");

    Assert.That(Note(await NextEventAsync(here)), Is.EqualTo("all"));
    Assert.That(Note(await NextEventAsync(there)), Is.EqualTo("all"));

    // The sender's server skips its own message when the backplane hands it back.
    Assert.That(await here.ReceivesWithinAsync(TimeSpan.FromMilliseconds(200)), Is.False);
  }

  [Test]
  public async Task ReachesAGroupOnEveryServer()
  {
    await using var here = await ConnectAsync(First);
    await using var there = await ConnectAsync(Second);
    await using var outsider = await ConnectAsync(Second);
    await here.CallAsync(1, "events.join", "\"room-a\"");
    await there.CallAsync(1, "events.join", "\"room-a\"");
    await Bus.UntilAsync(() => Bus.IsSubscribed(SecondBackplane, "group:room-a"));

    await outsider.CallAsync(1, "events.toGroup", """{"group":"room-a","note":{"text":"members","number":2}}""");

    Assert.That(Note(await NextEventAsync(here)), Is.EqualTo("members"));
    Assert.That(Note(await NextEventAsync(there)), Is.EqualTo("members"));
    Assert.That(await outsider.ReceivesWithinAsync(TimeSpan.FromMilliseconds(200)), Is.False);
  }

  [Test]
  public async Task ReachesAUserOnEveryServer()
  {
    await using var here = await ConnectAsync(First, user: "carol");
    await using var there = await ConnectAsync(Second, user: "carol");
    await using var someoneElse = await ConnectAsync(Second, user: "dave");
    await Bus.UntilAsync(() => Bus.IsSubscribed(SecondBackplane, "user:carol"));

    await Hub(First).User("carol").SendAsync(TestApi.Broadcast, new Note("carol's", 3));

    Assert.That(Note(await NextEventAsync(here)), Is.EqualTo("carol's"));
    Assert.That(Note(await NextEventAsync(there)), Is.EqualTo("carol's"));
    Assert.That(await someoneElse.ReceivesWithinAsync(TimeSpan.FromMilliseconds(200)), Is.False);
  }

  [Test]
  public async Task ReachesAConnectionOnTheServerItIsOn()
  {
    await using var there = await ConnectAsync(Second);
    var id = there.Welcome.GetProperty("connection").GetString();
    Assert.That(id, Does.StartWith(SecondBackplane.ServerId + "."));

    await Hub(First).Connection(id).SendAsync(TestApi.Broadcast, new Note("direct", 4));

    Assert.That(Note(await NextEventAsync(there)), Is.EqualTo("direct"));
    Assert.That(Bus.PublishedTo("server:" + SecondBackplane.ServerId), Is.EqualTo(1));
  }

  [Test]
  public async Task PublishesNothingForAConnectionItHas()
  {
    await using var here = await ConnectAsync(First);
    var id = here.Welcome.GetProperty("connection").GetString();
    var published = Bus.Published;

    await Hub(First).Connection(id).SendAsync(TestApi.Broadcast, new Note("local", 5));
    await Hub(First).Connection("gone").SendAsync(TestApi.Broadcast, new Note("nowhere", 5));

    Assert.That(Note(await NextEventAsync(here)), Is.EqualTo("local"));
    Assert.That(Bus.Published, Is.EqualTo(published));
  }

  [Test]
  public async Task SubscribesWhileTheServerHasConnectionsForATopic()
  {
    var member = await ConnectAsync(Second, user: "erin");
    await member.CallAsync(1, "events.join", "\"room-b\"");
    await Bus.UntilAsync(() => Bus.IsSubscribed(SecondBackplane, "group:room-b") && Bus.IsSubscribed(SecondBackplane, "user:erin"));
    Assert.That(Bus.IsSubscribed(FirstBackplane, "group:room-b"), Is.False);

    await member.CloseAsync();
    await member.DisposeAsync();

    await Bus.UntilAsync(() => !Bus.IsSubscribed(SecondBackplane, "group:room-b") && !Bus.IsSubscribed(SecondBackplane, "user:erin"));
  }

  [Test]
  public async Task WritesEachConnectionsFormat()
  {
    await using var json = await ConnectAsync(Second);
    await using var marked = await ConnectAsync(Second, format: MarkedJsonFormat.FormatName);
    await using var sender = await ConnectAsync(First);

    await sender.CallAsync(1, "events.broadcast", """{"text":"formats","number":6}""");

    Assert.That(Note(await NextEventAsync(json)), Is.EqualTo("formats"));
    var inFormat = await NextEventAsync(marked);
    Assert.That(inFormat.Payload[0], Is.EqualTo(MarkedJsonFormat.Marker));
    Assert.That(JsonDocument.Parse(inFormat.Payload.AsMemory(1)).RootElement.GetProperty("text").GetString(), Is.EqualTo("formats"));
  }

  [Test]
  public async Task SendsBytesAsTheyAreInEveryFormat()
  {
    await using var json = await ConnectAsync(Second);
    await using var marked = await ConnectAsync(Second, format: MarkedJsonFormat.FormatName);

    await Hub(First).All.SendAsync(Bytes, [1, 2, 3]);

    foreach (var client in new[] { json, marked })
    {
      var received = await NextEventAsync(client);
      Assert.That(received.Has(FrameFlags.Binary), Is.True);
      Assert.That(received.Payload, Is.EqualTo(new byte[] { 1, 2, 3 }));
    }
  }

  [Test]
  public async Task DropsMessagesItCannotRead()
  {
    await using var there = await ConnectAsync(Second);

    Bus.Publish("all", []);
    Bus.Publish("all", [99, 1, 2]);
    Bus.Publish("all", [1, 200]);
    await Hub(First).All.SendAsync(TestApi.Broadcast, new Note("after", 7));

    Assert.That(Note(await NextEventAsync(there)), Is.EqualTo("after"));
  }

  [Test]
  public async Task FailsASendTheBackplaneCouldNotPublish()
  {
    await using var here = await ConnectAsync(First);
    FirstBackplane.PublishFails = true;
    try
    {
      var failure = await Assert.ThrowsAsync<IOException>(async () =>
        await Hub(First).All.SendAsync(TestApi.Broadcast, new Note("unpublished", 8)));
      Assert.That(failure.Message, Is.EqualTo("The backend is down."));
    }
    finally
    {
      FirstBackplane.PublishFails = false;
    }

    // This server's connections got it all the same.
    Assert.That(Note(await NextEventAsync(here)), Is.EqualTo("unpublished"));
  }

  [Test]
  public async Task RetriesASubscriptionThatFailed()
  {
    await using var member = await ConnectAsync(Second, user: "frank");
    await Bus.UntilAsync(() => Bus.IsSubscribed(SecondBackplane, "user:frank"));
    SecondBackplane.FailingSubscribes = 1;

    await member.CallAsync(1, "events.join", "\"room-c\"");

    await Bus.UntilAsync(() => Bus.IsSubscribed(SecondBackplane, "group:room-c"));
    Assert.That(SecondBackplane.FailedSubscribes, Is.EqualTo(1));
  }

  // On servers of its own, so no other test's connections change what is measured.
  [Test]
  public async Task MeasuresTheBackplane()
  {
    var bus = new TestBus();
    var (first, firstBackplane) = await StartServerAsync(bus);
    var (second, secondBackplane) = await StartServerAsync(bus);
    try
    {
      var sent = new System.Collections.Concurrent.ConcurrentQueue<TelemetryTests.Measurement>();
      var received = new System.Collections.Concurrent.ConcurrentQueue<TelemetryTests.Measurement>();
      using var sending = TelemetryTests.Listen(first, sent);
      using var receiving = TelemetryTests.Listen(second, received);

      var member = await ConnectAsync(second, user: "grace");
      await bus.UntilAsync(() => bus.IsSubscribed(secondBackplane, "user:grace"));
      secondBackplane.FailingSubscribes = 1;
      await member.CallAsync(1, "events.join", "\"room-m\"");
      await bus.UntilAsync(() => bus.IsSubscribed(secondBackplane, "group:room-m"));

      await Hub(first).Group("room-m").SendAsync(TestApi.Broadcast, new Note("measured", 9));
      firstBackplane.PublishFails = true;
      await Assert.ThrowsAsync<IOException>(async () => await Hub(first).All.SendAsync(TestApi.Broadcast, new Note("lost", 9)));
      firstBackplane.PublishFails = false;

      // From a server that maps no "nowhere": version 1, server "x", All, no key, the name, no flags, then "1".
      bus.Publish("all", [1, 1, (byte)'x', 1, 0, 7, .. "nowhere"u8, 0, (byte)'1']);
      bus.Publish("all", [99]);
      Assert.That(Note(await NextEventAsync(member)), Is.EqualTo("measured"));

      await member.CloseAsync();
      await member.DisposeAsync();
      await bus.UntilAsync(() => !bus.IsSubscribed(secondBackplane, "group:room-m") && !bus.IsSubscribed(secondBackplane, "user:grace"));
      await bus.UntilAsync(() => received.Count(m => m.Instrument == "puregate.websocket.backplane.received") == 3);

      var publishes = sent.Where(m => m.Instrument == "puregate.websocket.backplane.publish.duration")
        .Select(m => $"{m.Tags["puregate.websocket.target"]} {m.Tags["puregate.websocket.outcome"]}");
      Assert.That(publishes, Is.EquivalentTo(new[] { "group ok", "all failed" }));

      var outcomes = received.Where(m => m.Instrument == "puregate.websocket.backplane.received")
        .Select(m => m.Tags["puregate.websocket.outcome"]);
      Assert.That(outcomes, Is.EquivalentTo(new[] { "delivered", "not_mapped", "unreadable" }));

      // grace's user topic, then room-m after one failed try, then both again when grace leaves.
      var subscriptions = received.Where(m => m.Instrument == "puregate.websocket.backplane.subscriptions").Select(m => m.Value);
      Assert.That(subscriptions, Is.EqualTo(new double[] { 1, 1, -1, -1 }));
      var failure = received.Single(m => m.Instrument == "puregate.websocket.backplane.subscription_failures");
      Assert.That(failure.Tags["puregate.websocket.call"], Is.EqualTo("subscribe"));
    }
    finally
    {
      await first.DisposeAsync();
      await second.DisposeAsync();
    }

    Assert.That(bus.Violations, Is.Empty);
  }

  [Test]
  public async Task RefusesTwoServerEventsOfOneName()
  {
    var builder = WebApplication.CreateSlimBuilder();
    builder.Logging.ClearProviders();
    builder.Services.AddPureGateWebSocket();
    builder.Services.AddSingleton<IWebSocketBackplane>(new TestBus().Join());
    await using var app = builder.Build();
    var ws = app.MapPureGateWebSocket("/ws");
    ws.MapEvent(new ServerEvent<int>("twice"));

    var refused = Assert.Throws<InvalidOperationException>(() => ws.MapEvent(new ServerEvent<string>("twice")));
    Assert.That(refused.Message, Does.Contain("'twice'"));
  }

  [Test]
  public async Task StopsTheHostWhenTheBackplaneStopsReceiving()
  {
    var bus = new TestBus();
    var backplane = bus.Join();
    var host = await TestHost.StartAsync(services: services => services.AddSingleton<IWebSocketBackplane>(backplane));
    await bus.UntilAsync(() => bus.IsSubscribed(backplane, "all"));
    var stopping = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.Register(() => stopping.TrySetResult());

    backplane.StopReceiving.TrySetResult();

    await stopping.Task.WaitAsync(TimeSpan.FromSeconds(10));

    // Newer hosts also rethrow the failure from StopAsync, so an app's RunAsync ends with it.
    try
    {
      await host.DisposeAsync();
    }
    catch (InvalidOperationException exception)
    {
      Assert.That(exception.Message, Does.StartWith("The backplane stopped receiving"));
    }
  }

  static async Task<(TestHost, TestBackplane)> StartServerAsync(TestBus bus)
  {
    var backplane = bus.Join();
    var host = await TestHost.StartAsync(
      configure: static options => options.Authenticate = TestApi.AuthenticateToken,
      map: static ws => ws.MapEvent(Bytes),
      services: services => services.AddSingleton<IWebSocketBackplane>(backplane));
    await bus.UntilAsync(() =>
      bus.IsSubscribed(backplane, "all") && backplane.ServerId is { } id && bus.IsSubscribed(backplane, "server:" + id));
    return (host, backplane);
  }

  static async Task<TestClient> ConnectAsync(TestHost host, string user = "anyone", string format = null)
  {
    var client = await host.ConnectAsync();
    var hello = format is null
      ? $$"""{"calls":16,"maxMessage":1048576,"token":"{{user}}"}"""
      : $$"""{"calls":16,"maxMessage":1048576,"token":"{{user}}","format":"{{format}}"}""";
    await client.HelloAsync(hello);
    return client;
  }

  static IWebSocketHub Hub(TestHost host) => host.Services.GetRequiredService<IWebSocketHub>();

  // The next event, past the responses and acks before it.
  static async Task<Received> NextEventAsync(TestClient client)
  {
    while (true)
    {
      var frame = await client.ReceiveAsync();
      if (frame.Kind == FrameKind.Event)
        return frame;
    }
  }

  static string Note(Received received) => received.Json().GetProperty("text").GetString();
}

// A backend in memory: a message published to a topic reaches, in order, every backplane subscribed to it.
sealed class TestBus
{
  readonly Lock Gate = new();
  readonly Dictionary<string, HashSet<TestBackplane>> Topics = new(StringComparer.Ordinal);
  readonly Dictionary<string, int> PublishedByTopic = new(StringComparer.Ordinal);

  // Messages published so far.
  public int Published { get; private set; }

  // Calls the hub must never make: a second subscribe to a topic, or an unsubscribe without one.
  public System.Collections.Concurrent.ConcurrentQueue<string> Violations { get; } = new();

  public TestBackplane Join() => new(this);

  public bool IsSubscribed(TestBackplane backplane, string topic)
  {
    lock (Gate)
      return Topics.TryGetValue(topic, out var subscribers) && subscribers.Contains(backplane);
  }

  public int PublishedTo(string topic)
  {
    lock (Gate)
      return PublishedByTopic.GetValueOrDefault(topic);
  }

  public void Subscribe(TestBackplane backplane, string topic)
  {
    lock (Gate)
    {
      if (!Topics.TryGetValue(topic, out var subscribers))
        Topics[topic] = subscribers = [];
      if (!subscribers.Add(backplane))
        Violations.Enqueue($"Subscribed to {topic} twice.");
    }
  }

  public void Unsubscribe(TestBackplane backplane, string topic)
  {
    lock (Gate)
    {
      if (!Topics.TryGetValue(topic, out var subscribers) || !subscribers.Remove(backplane))
        Violations.Enqueue($"Unsubscribed from {topic} without a subscription.");
    }
  }

  public void Publish(string topic, byte[] message)
  {
    lock (Gate)
    {
      Published++;
      PublishedByTopic[topic] = PublishedByTopic.GetValueOrDefault(topic) + 1;
      if (Topics.TryGetValue(topic, out var subscribers))
      {
        foreach (var subscriber in subscribers)
          subscriber.Deliver(message);
      }
    }
  }

  public async Task UntilAsync(Func<bool> condition)
  {
    var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
    while (!condition())
    {
      if (DateTime.UtcNow > deadline)
        Assert.Fail("The backplane did not get there in time.");
      await Task.Delay(10);
    }
  }
}

sealed class TestBackplane(TestBus bus) : IWebSocketBackplane
{
  readonly Channel<byte[]> Inbox = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleReader = true });
  int Failing;
  int Failed;

  // The hub's server id, from the topic of its own server.
  public string ServerId { get; private set; }

  // Subscribes that throw before one succeeds.
  public int FailingSubscribes
  {
    get => Volatile.Read(ref Failing);
    set => Volatile.Write(ref Failing, value);
  }

  public int FailedSubscribes => Volatile.Read(ref Failed);

  public volatile bool PublishFails;

  // Completed to make ReceiveAsync end on its own.
  public TaskCompletionSource StopReceiving { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

  public void Deliver(byte[] message) => Inbox.Writer.TryWrite(message);

  public async Task ReceiveAsync(Action<ReadOnlyMemory<byte>> receive, CancellationToken cancellationToken)
  {
    var reading = ReadAsync(receive, cancellationToken);
    await Task.WhenAny(reading, StopReceiving.Task);
  }

  async Task ReadAsync(Action<ReadOnlyMemory<byte>> receive, CancellationToken cancellationToken)
  {
    await foreach (var message in Inbox.Reader.ReadAllAsync(cancellationToken))
      receive(message);
  }

  public ValueTask SubscribeAsync(string topic, CancellationToken cancellationToken)
  {
    if (Interlocked.Decrement(ref Failing) >= 0)
    {
      Interlocked.Increment(ref Failed);
      return ValueTask.FromException(new IOException("The backend is down."));
    }

    Interlocked.Exchange(ref Failing, 0);
    if (topic.StartsWith("server:", StringComparison.Ordinal))
      ServerId = topic["server:".Length..];

    bus.Subscribe(this, topic);
    return default;
  }

  public ValueTask UnsubscribeAsync(string topic, CancellationToken cancellationToken)
  {
    bus.Unsubscribe(this, topic);
    return default;
  }

  // Copies the message, as a backend sending it over the network would, and completes later, as one would.
  public async ValueTask PublishAsync(string topic, ReadOnlyMemory<byte> message, CancellationToken cancellationToken)
  {
    if (PublishFails)
      throw new IOException("The backend is down.");

    var copy = message.ToArray();
    await Task.Yield();
    bus.Publish(topic, copy);
  }
}
