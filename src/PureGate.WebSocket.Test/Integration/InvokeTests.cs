using System.Net.WebSockets;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Test.Integration;

// Calls from the server to the browser.
public sealed class InvokeTests
{
  TestHost Host;

  [OneTimeSetUp]
  public async Task StartAsync() => Host = await TestHost.StartAsync();

  [OneTimeTearDown]
  public async Task StopAsync() => await Host.DisposeAsync();

  [Test]
  public async Task CallsTheBrowserAndReturnsItsAnswer()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Request(1, "browser.confirm", "\"Delete it?\""));

    var question = await client.ReceiveAsync(FrameKind.Request, 1);
    Assert.That((question.NameCode, question.Name), Is.EqualTo((3u, "ui.confirm")));
    Assert.That(question.Json().GetProperty("text").GetString(), Is.EqualTo("Delete it?"));
    Assert.That(question.Has(FrameFlags.HasTimeout), Is.True);
    Assert.That(question.Timeout, Is.EqualTo(30_000));

    await client.SendAsync(Outgoing.Response(question.Id, "true"));
    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Text, Is.EqualTo("\"True\""));
  }

  [Test]
  public async Task PassesTheBrowsersErrors()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Request(1, "browser.confirm", "\"Delete it?\""));
    var question = await client.ReceiveAsync(FrameKind.Request, 1);

    await client.SendAsync(new Outgoing
    {
      Kind = FrameKind.Response,
      Flags = FrameFlags.HasError,
      Id = question.Id,
      ErrorCode = ErrorCode.Custom,
      ErrorCustom = "declined",
      ErrorMessage = "The user said no.",
    });

    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Text, Is.EqualTo("\"declined: The user said no.\""));
  }

  [Test]
  public async Task TimesOutAndCancelsCallsTheBrowserLeavesUnanswered()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Request(1, "browser.slow", "5000"));
    var slow = await client.ReceiveAsync(FrameKind.Request, 1);
    Assert.That(slow.Timeout, Is.EqualTo(200));

    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Text, Is.EqualTo("\"timeout\""));
    Assert.That((await client.ReceiveAsync(FrameKind.Cancel, slow.Id)).Kind, Is.EqualTo(FrameKind.Cancel));

    // The id stays taken until the browser answers, so a late answer is still welcome.
    await client.SendAsync(Outgoing.Response(slow.Id, "1"));
    Assert.That((await client.CallAsync(2, "echo.text", "\"open\"")).Text, Is.EqualTo("\"open\""));
  }

  [Test]
  public async Task KeepsToTheBrowsersCallWindow()
  {
    await using var client = await Host.ConnectAndHelloAsync("""{"calls":1,"maxMessage":1048576}""");
    await client.SendAsync(
      Outgoing.Request(1, "browser.confirm", "\"one\""),
      Outgoing.Request(2, "browser.confirm", "\"two\""));

    var first = await client.ReceiveAsync();
    Assert.That(first.Kind, Is.EqualTo(FrameKind.Request));
    Assert.That(await client.ReceivesWithinAsync(TimeSpan.FromMilliseconds(200)), Is.False, "The second call waits for the first.");

    await client.SendAsync(Outgoing.Response(first.Id, "true"));
    var second = await client.ReceiveAsync(FrameKind.Request, first.Id + 1);
    await client.SendAsync(Outgoing.Response(second.Id, "false"));

    var answers = new[] { await client.ReceiveAsync(FrameKind.Response, 1), await client.ReceiveAsync(FrameKind.Response, 2) };
    Assert.That(answers.Select(answer => answer.Text), Is.EquivalentTo(new[] { "\"True\"", "\"False\"" }));
  }

  [Test]
  public async Task StopsWaitingWhenTheCallerCancels()
  {
    await using var host = await TestHost.StartAsync(map: ws => ws.MapRequest<string>("test.askBriefly", async (call, _) =>
    {
      using var brief = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
      try
      {
        await call.Connection.InvokeAsync(TestServer.TestApi.Confirm, new TestServer.Question("?"), brief.Token);
        return "answered";
      }
      catch (OperationCanceledException)
      {
        return "cancelled";
      }
    }));
    await using var client = await host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Request(1, "test.askBriefly"));
    var question = await client.ReceiveAsync(FrameKind.Request, 1);

    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Text, Is.EqualTo("\"cancelled\""));
    Assert.That((await client.ReceiveAsync(FrameKind.Cancel, question.Id)).Kind, Is.EqualTo(FrameKind.Cancel));
  }

  [Test]
  public async Task ClosesOnAnswersToCallsItNeverMade()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Response(77, "true"));

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  [Test]
  public async Task FailsCallsWhenTheSessionEnds()
  {
    var failure = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var host = await TestHost.StartAsync(
      configure: static options => options.ResumeWindow = TimeSpan.Zero,
      map: ws => ws.MapRequest("test.ask", async (call, cancellationToken) =>
    {
      try
      {
        await call.Connection.InvokeAsync(TestServer.TestApi.Confirm, new TestServer.Question("?"), cancellationToken);
      }
      catch (CallException exception)
      {
        failure.TrySetResult(exception.Code);
      }
      catch (OperationCanceledException)
      {
        failure.TrySetResult("cancelled");
      }
    }));

    var client = await host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Request(1, "test.ask"));
    await client.ReceiveAsync(FrameKind.Request, 1);
    await client.DisposeAsync();

    Assert.That(await failure.Task.WaitAsync(TimeSpan.FromSeconds(10)), Is.AnyOf("connection_closed", "cancelled"));
  }
}
