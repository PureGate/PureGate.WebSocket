using System.Net.WebSockets;
using Microsoft.Extensions.DependencyInjection;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.TestServer;

namespace PureGate.WebSocket.Test.Integration;

public sealed class EventTests
{
  TestHost Host;

  [OneTimeSetUp]
  public async Task StartAsync() => Host = await TestHost.StartAsync();

  [OneTimeTearDown]
  public async Task StopAsync() => await Host.DisposeAsync();

  [Test]
  public async Task HandlesEventsFromTheBrowserInOrder()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync([.. Enumerable.Range(1, 10).Select(i => Outgoing.Event("telemetry.click", $$"""{"x":{{i}},"y":0}"""))]);

    var clicks = (await client.CallAsync(1, "telemetry.waitFor", "10")).Json().EnumerateArray()
      .Select(click => click.GetProperty("x").GetInt32());
    Assert.That(clicks, Is.EqualTo(Enumerable.Range(1, 10)));
  }

  [Test]
  public async Task ReturnsEventCreditInBatches()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync([.. Enumerable.Range(1, 8).Select(i => Outgoing.Event("telemetry.click", """{"x":1,"y":1}"""))]);

    // A window of 16 returns credit every 4 events.
    var credited = 0u;
    while (credited < 8)
    {
      var credit = await client.ReceiveAsync();
      Assert.That(credit.Kind, Is.EqualTo(FrameKind.Credit));
      Assert.That(credit.Id, Is.Zero);
      credited += credit.Amount;
    }

    Assert.That(credited, Is.EqualTo(8));
  }

  [Test]
  public async Task ClosesWhenTheBrowserGoesPastTheEventWindow()
  {
    var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var host = await TestHost.StartAsync(map: ws =>
      ws.MapClientEvent<int>("test.stuck", async (_, _, _) => await gate.Task));
    await using var client = await host.ConnectAndHelloAsync();

    // The first event holds the queue, so none of the 17 is credited back.
    await client.SendAsync([.. Enumerable.Range(1, 17).Select(i => Outgoing.Event("test.stuck", "1"))]);

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
    gate.TrySetResult();
  }

  [Test]
  public async Task KeepsHandlingEventsAfterAHandlerThrows()
  {
    var handled = new System.Collections.Concurrent.ConcurrentQueue<int>();
    await using var host = await TestHost.StartAsync(map: ws =>
    {
      ws.MapClientEvent<int>("test.flaky", (value, _, _) =>
      {
        if (value == 2)
          throw new InvalidOperationException("A handler bug.");
        handled.Enqueue(value);
        return ValueTask.CompletedTask;
      });
      ws.MapRequest<int[]>("test.handled", (_, _) => ValueTask.FromResult(handled.ToArray()));
    });
    await using var client = await host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Event("test.flaky", "1"), Outgoing.Event("test.flaky", "2"), Outgoing.Event("test.flaky", "3"));

    while ((await client.CallAsync(1, "test.handled")).Text != "[1,3]")
      await Task.Delay(10);
  }

  [Test]
  public async Task LeavesGroupsItRemoves()
  {
    await using var host = await TestHost.StartAsync(map: ws => ws.MapRequest<string, bool>("test.leave", (group, call, _) =>
      ValueTask.FromResult(call.Connection.Groups.Remove(group))));
    await using var member = await host.ConnectAndHelloAsync();
    await member.CallAsync(1, "events.join", "\"room-3\"");

    Assert.That((await member.CallAsync(2, "test.leave", "\"room-3\"")).Text, Is.EqualTo("true"));
    Assert.That((await member.CallAsync(3, "test.leave", "\"room-3\"")).Text, Is.EqualTo("false"));
    await member.CallAsync(4, "events.toGroup", """{"group":"room-3","note":{"text":"gone","number":3}}""");
    Assert.That(await member.ReceivesWithinAsync(TimeSpan.FromMilliseconds(200)), Is.False);
  }

  [Test]
  public async Task DropsEventsItHasNoHandlerFor()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Event("nothing.here", "1"), Outgoing.Event("telemetry.click", "\"not a click\""));

    Assert.That((await client.CallAsync(1, "echo.text", "\"open\"")).Text, Is.EqualTo("\"open\""));
  }

  [Test]
  public async Task SendsEventsToTheBrowser()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.CallAsync(1, "events.echo", "\"first\"");
    await client.CallAsync(2, "events.echo", "\"second\"");

    var first = await client.ReceiveAsync();
    Assert.That(first.Kind, Is.EqualTo(FrameKind.Event));
    Assert.That((first.NameCode, first.Name, first.Text), Is.EqualTo((3u, "events.received", "\"first\"")));

    var second = await client.ReceiveAsync();
    Assert.That((second.NameCode, second.Text), Is.EqualTo((2u, "\"second\"")), "The second time, the name is a reference.");
  }

  [Test]
  public async Task BroadcastsToEveryConnection()
  {
    await using var first = await Host.ConnectAndHelloAsync();
    await using var second = await Host.ConnectAndHelloAsync();
    await first.CallAsync(1, "events.broadcast", """{"text":"all","number":1}""");

    foreach (var client in new[] { first, second })
    {
      Received broadcast;
      do
        broadcast = await client.ReceiveAsync();
      while (broadcast.Kind != FrameKind.Event || broadcast.Name != "events.broadcast");

      Assert.That(broadcast.Json().GetProperty("text").GetString(), Is.EqualTo("all"));
    }
  }

  [Test]
  public async Task SendsToGroupsAndUsersOnly()
  {
    await using var member = await Host.ConnectAndHelloAsync();
    await using var outsider = await Host.ConnectAndHelloAsync();
    await member.CallAsync(1, "events.join", "\"room-1\"");
    await outsider.CallAsync(1, "events.toGroup", """{"group":"room-1","note":{"text":"hi","number":2}}""");

    var note = await member.ReceiveAsync();
    Assert.That(note.Kind, Is.EqualTo(FrameKind.Event));
    Assert.That(note.Json().GetProperty("number").GetInt32(), Is.EqualTo(2));
    Assert.That(await outsider.ReceivesWithinAsync(TimeSpan.FromMilliseconds(200)), Is.False);
  }

  [Test]
  public async Task LeavesGroupsWhenTheSessionEnds()
  {
    var hub = Host.Services.GetRequiredService<IWebSocketHub>();
    var member = await Host.ConnectAndHelloAsync();
    var id = member.Welcome.GetProperty("connection").GetString();
    await member.CallAsync(1, "events.join", "\"room-2\"");
    Assert.That(hub.TryGetConnection(id, out var connection), Is.True);
    Assert.That(connection.Groups.Contains("room-2"), Is.True);

    await member.CloseAsync();
    await member.DisposeAsync();
    while (hub.TryGetConnection(id, out _))
      await Task.Delay(10);

    await hub.Group("room-2").SendAsync(TestApi.Broadcast, new Note("nobody", 0));
  }

  [Test]
  public async Task RefusesEventsNoEndpointMaps()
  {
    var hub = Host.Services.GetRequiredService<IWebSocketHub>();

    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
      await hub.All.SendAsync(new ServerEvent<int>("not.mapped"), 1));
    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
      await default(WebSocketTarget).SendAsync(TestApi.Broadcast, new Note("x", 0)));
  }
}
