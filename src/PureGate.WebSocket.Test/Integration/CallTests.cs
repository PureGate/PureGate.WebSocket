using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Test.Integration;

public sealed class CallTests
{
  TestHost Host;

  [OneTimeSetUp]
  public async Task StartAsync() => Host = await TestHost.StartAsync();

  [OneTimeTearDown]
  public async Task StopAsync() => await Host.DisposeAsync();

  [Test]
  public async Task CallsWithJsonPayloads()
  {
    await using var client = await Host.ConnectAndHelloAsync();

    var note = await client.CallAsync(1, "echo.note", """{"text":"héllo","number":7}""");
    Assert.That(note.Has(FrameFlags.Binary), Is.False);
    Assert.That(note.Json().GetProperty("text").GetString(), Is.EqualTo("héllo"));
    Assert.That(note.Json().GetProperty("number").GetInt32(), Is.EqualTo(7));
    Assert.That((await client.CallAsync(2, "math.add", """{"a":40,"b":2}""")).Text, Is.EqualTo("42"));
    Assert.That((await client.CallAsync(3, "echo.numbers", "[1,2,3]")).Text, Is.EqualTo("[1,2,3]"));
  }

  [Test]
  public async Task CallsWithBinaryPayloads()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    var request = Outgoing.Request(1, "echo.bytes");
    request.Flags = FrameFlags.Binary;
    request.Payload = [0, 1, 2, 255];
    await client.SendAsync(request);

    var response = await client.ReceiveAsync(FrameKind.Response, 1);
    Assert.That(response.Has(FrameFlags.Binary), Is.True);
    Assert.That(response.Payload, Is.EqualTo(new byte[] { 0, 1, 2, 255 }));
  }

  [Test]
  public async Task CallsWithReadOnlyMemoryPayloads()
  {
    await using var host = await TestHost.StartAsync(map: ws =>
      ws.MapRequest<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>("test.reverse", static (bytes, _, _) =>
        ValueTask.FromResult<ReadOnlyMemory<byte>>(bytes.ToArray().Reverse().ToArray())));
    await using var client = await host.ConnectAndHelloAsync();
    var request = Outgoing.Request(1, "test.reverse");
    request.Flags = FrameFlags.Binary;
    request.Payload = [1, 2, 3];
    await client.SendAsync(request);

    var response = await client.ReceiveAsync(FrameKind.Response, 1);
    Assert.That(response.Has(FrameFlags.Binary), Is.True);
    Assert.That(response.Payload, Is.EqualTo(new byte[] { 3, 2, 1 }));
  }

  [Test]
  public async Task CallsWithoutARequestOrAResult()
  {
    await using var client = await Host.ConnectAndHelloAsync();

    Assert.That(long.Parse((await client.CallAsync(1, "time.now")).Text), Is.GreaterThan(0));

    var noop = await client.CallAsync(2, "misc.noop");
    Assert.That(noop.Has(FrameFlags.HasError), Is.False);
    Assert.That(noop.Payload, Is.Empty);

    var consume = await client.CallAsync(3, "misc.consume", "\"ignored\"");
    Assert.That(consume.Has(FrameFlags.HasError), Is.False);
    Assert.That(consume.Payload, Is.Empty);
  }

  [Test]
  public async Task RefersToNamesItDefined()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    var define = Outgoing.Request(1, "echo.text", "\"a\"");
    define.NameCode = 3;
    var refer = Outgoing.Request(2, null, "\"b\"");
    refer.NameCode = 2;
    await client.SendAsync(define, refer);

    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Text, Is.EqualTo("\"a\""));
    Assert.That((await client.ReceiveAsync(FrameKind.Response, 2)).Text, Is.EqualTo("\"b\""));
  }

  [Test]
  public async Task PassesTheTraceParent()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    var request = Outgoing.Request(1, "echo.trace");
    request.Flags = FrameFlags.HasTrace;
    request.Trace = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    await client.SendAsync(request);

    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Text, Is.EqualTo("\"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\""));
  }

  [Test]
  public async Task AnswersUnknownOperations()
  {
    await using var client = await Host.ConnectAndHelloAsync();

    var response = await client.CallAsync(1, "nothing.here", "1");
    Assert.That(response.Error, Is.EqualTo("unknown_operation"));
    Assert.That((await client.CallAsync(2, "echo.text", "\"still open\"")).Text, Is.EqualTo("\"still open\""));
  }

  [TestCase("math.add", "\"not an object\"", TestName = "Wrong JSON type")]
  [TestCase("math.add", "{broken", TestName = "Broken JSON")]
  [TestCase("math.add", null, TestName = "No payload where one is needed")]
  [TestCase("time.now", "1", TestName = "A payload where none is taken")]
  public async Task AnswersInvalidPayloads(string name, string json)
  {
    await using var client = await Host.ConnectAndHelloAsync();

    Assert.That((await client.CallAsync(1, name, json)).Error, Is.EqualTo("invalid_payload"));
  }

  [Test]
  public async Task AnswersBinaryPayloadsForJsonOperations()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    var request = Outgoing.Request(1, "echo.text", "\"x\"");
    request.Flags = FrameFlags.Binary;
    await client.SendAsync(request);

    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Error, Is.EqualTo("invalid_payload"));
  }

  [Test]
  public async Task SendsApplicationErrorsWithTheirDetails()
  {
    await using var client = await Host.ConnectAndHelloAsync();

    var response = await client.CallAsync(1, "fail.notFound", "7");
    Assert.That(response.Error, Is.EqualTo("not_found"));
    Assert.That(response.ErrorMessage, Is.EqualTo("Item 7 does not exist."));
    Assert.That(response.Json().GetProperty("id").GetInt32(), Is.EqualTo(7));
  }

  [Test]
  public async Task HidesExceptionMessagesByDefault()
  {
    await using var client = await Host.ConnectAndHelloAsync();

    var response = await client.CallAsync(1, "fail.crash");
    Assert.That(response.Error, Is.EqualTo("handler_failed"));
    Assert.That(response.ErrorMessage, Does.Not.Contain("secret"));
  }

  [Test]
  public async Task IncludesExceptionMessagesWhenAsked()
  {
    await using var host = await TestHost.StartAsync(options => options.IncludeExceptionDetails = true);
    await using var client = await host.ConnectAndHelloAsync();

    Assert.That((await client.CallAsync(1, "fail.crash")).ErrorMessage, Does.Contain("secret"));
  }

  [Test]
  public async Task AnswersHandlerFailuresWithTheTraceTheyAreLoggedUnder()
  {
    var logs = new LogRecorder();
    await using var host = await TestHost.StartAsync(services: services => services.AddSingleton<ILoggerProvider>(logs));
    await using var client = await host.ConnectAndHelloAsync();
    var request = Outgoing.Request(1, "fail.crash");
    request.Flags = FrameFlags.HasTrace;
    request.Trace = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    await client.SendAsync(request);
    var inTheBrowsersTrace = await client.ReceiveAsync(FrameKind.Response, 1);
    var inANewTrace = await client.CallAsync(2, "fail.crash");

    // Nothing records spans here, so each trace has a span id of its own, marked as not recorded.
    Assert.That(inTheBrowsersTrace.Trace, Does.Match("^00-4bf92f3577b34da6a3ce929d0e0e4736-[0-9a-f]{16}-00$"));
    Assert.That(inTheBrowsersTrace.Trace, Does.Not.Contain("00f067aa0ba902b7"));
    Assert.That(inANewTrace.Trace, Does.Match("^00-[0-9a-f]{32}-[0-9a-f]{16}-00$"));
    Assert.That(inANewTrace.Trace, Does.Not.Contain("4bf92f3577b34da6a3ce929d0e0e4736"));
    Assert.That(
      logs.Entries.Where(entry => entry.Event == "HandlerFailed").Select(entry => entry.Fields["TraceId"]),
      Is.EqualTo(new[] { inTheBrowsersTrace.Trace[3..35], inANewTrace.Trace[3..35] }));
  }

  [Test]
  public async Task AnswersResultsThatCannotBeWrittenAsHandlerFailures()
  {
    var logs = new LogRecorder();
    await using var host = await TestHost.StartAsync(
      configure: static options => options.SerializerOptions.TypeInfoResolverChain.Insert(0, UnwritableJson.Default),
      map: static ws => ws.MapRequest<Unwritable>("fail.unwritable", static (call, cancellationToken) => ValueTask.FromResult(new Unwritable())),
      services: services => services.AddSingleton<ILoggerProvider>(logs));
    await using var client = await host.ConnectAndHelloAsync();

    var response = await client.CallAsync(1, "fail.unwritable");
    Assert.That((response.Error, response.ErrorMessage), Is.EqualTo(("handler_failed", "The handler failed.")));
    Assert.That(response.Trace, Does.Match("^00-[0-9a-f]{32}-[0-9a-f]{16}-00$"));
    Assert.That(
      logs.Entries.Where(entry => entry.Event == "ResultNotWritten").Select(entry => entry.Fields["TraceId"]),
      Is.EqualTo(new[] { response.Trace[3..35] }));
    Assert.That((await client.CallAsync(2, "echo.text", "\"still open\"")).Text, Is.EqualTo("\"still open\""));
  }

  [Test]
  public async Task CancelsHandlersAtTheirDeadline()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    var request = Outgoing.Request(1, "calls.delay", "10000");
    request.Flags = FrameFlags.HasTimeout;
    request.Timeout = 50;
    await client.SendAsync(request);

    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Error, Is.EqualTo("timeout"));
    Assert.That((await client.CallAsync(2, "calls.cancelled")).Text, Is.EqualTo("1"));
  }

  [Test]
  public async Task CancelsHandlersTheBrowserCancels()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Request(1, "calls.delay", "10000"));
    await Task.Delay(50);
    await client.SendAsync(Outgoing.Cancel(1));

    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Error, Is.EqualTo("cancelled"));
    Assert.That((await client.CallAsync(2, "calls.cancelled")).Text, Is.EqualTo("1"));
  }

  [Test]
  public async Task IgnoresCancelsForCallsThatEnded()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.CallAsync(1, "echo.text", "\"done\"");
    await client.SendAsync(Outgoing.Cancel(1), Outgoing.Cancel(99));

    Assert.That((await client.CallAsync(2, "echo.text", "\"open\"")).Text, Is.EqualTo("\"open\""));
  }

  [Test]
  public async Task RunsCallsUpToTheWindowAtOnce()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync([.. Enumerable.Range(1, 7).Select(id => Outgoing.Request((uint)id, "calls.block"))]);

    // The eighth call fills the window of 8, and needs the window's room to ask for the release.
    while ((await client.CallAsync(100, "calls.peak")).Text != "7")
      await Task.Delay(10);
    await client.CallAsync(101, "calls.release");

    for (uint id = 1; id <= 7; id++)
      Assert.That((await client.ReceiveAsync(FrameKind.Response, id)).Has(FrameFlags.HasError), Is.False);
  }

  [Test]
  public async Task ClosesWhenTheBrowserGoesPastTheWindow()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync([.. Enumerable.Range(1, 9).Select(id => Outgoing.Request((uint)id, "calls.block"))]);

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  [Test]
  public async Task ClosesWhenAnIdIsReusedTooEarly()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Request(1, "calls.block"), Outgoing.Request(1, "calls.block"));

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  [Test]
  public async Task RefusesResultsLargerThanTheBrowserAccepts()
  {
    await using var client = await Host.ConnectAndHelloAsync("""{"calls":4,"maxMessage":16384}""");

    var response = await client.CallAsync(1, "echo.repeat", "20000");
    Assert.That(response.Error, Is.EqualTo("payload_too_large"));
    Assert.That((await client.CallAsync(2, "echo.repeat", "100")).Text, Has.Length.EqualTo(102));
  }

  [Test]
  public async Task SplitsCoalescedFramesAtTheBrowsersLimit()
  {
    await using var client = await Host.ConnectAndHelloAsync("""{"calls":4,"maxMessage":16384}""");
    var json = $"\"{new string('y', 6000)}\"";
    await client.SendAsync(
      Outgoing.Request(1, "echo.text", json),
      Outgoing.Request(2, "echo.text", json),
      Outgoing.Request(3, "echo.text", json));

    for (uint id = 1; id <= 3; id++)
      Assert.That((await client.ReceiveAsync(FrameKind.Response, id)).Text, Is.EqualTo(json));
  }

  [Test]
  public async Task CancelsRunningHandlersWhenTheSessionEnds()
  {
    var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var host = await TestHost.StartAsync(
      configure: static options => options.ResumeWindow = TimeSpan.Zero,
      map: ws => ws.MapRequest("test.hang", async (call, cancellationToken) =>
    {
      try
      {
        await Task.Delay(Timeout.Infinite, cancellationToken);
      }
      catch (OperationCanceledException)
      {
        cancelled.TrySetResult();
        throw;
      }
    }));

    var client = await host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Request(1, "test.hang"));
    await Task.Delay(50);
    await client.DisposeAsync();

    await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
  }

  [Test]
  public async Task GivesHandlersAScopeOfTheirOwn()
  {
    await using var host = await TestHost.StartAsync(map: ws => ws.MapRequest<string>("test.services", (call, _) =>
      ValueTask.FromResult(call.Services is null ? "none" : call.Name)));
    await using var client = await host.ConnectAndHelloAsync();

    Assert.That((await client.CallAsync(1, "test.services")).Text, Is.EqualTo("\"test.services\""));
  }

  [Test]
  public async Task AnswersTheCallsOfOneMessageInOneMessage()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    // The whole window of 8.
    await client.SendAsync([.. Enumerable.Range(1, 8).Select(id => Outgoing.Request((uint)id, "math.add", $$"""{"a":{{id}},"b":1}"""))]);

    var answers = await client.ReceiveOneMessageAsync();
    Assert.That(
      answers.Select(frame => (frame.Kind, frame.Id, frame.Text)).ToArray(),
      Is.EqualTo(Enumerable.Range(1, 8).Select(id => (FrameKind.Response, (uint)id, $"{id + 1}")).ToArray()));
  }

  // Handlers start on the receive loop, which runs in the upgrade request's context, but see none of it.
  [Test]
  public async Task StartsHandlersOutsideTheUpgradeRequestsContext()
  {
    await using var host = await TestHost.StartAsync(
      services: static services => services.AddHttpContextAccessor(),
      map: static ws => ws.MapRequest<string>("test.context", static (call, _) =>
        ValueTask.FromResult(call.Services.GetRequiredService<IHttpContextAccessor>().HttpContext is null ? "empty" : "request")));
    await using var client = await host.ConnectAndHelloAsync();

    Assert.That((await client.CallAsync(1, "test.context")).Text, Is.EqualTo("\"empty\""));
  }

  [Test]
  public async Task RefusesNamesThatAreNotNames()
  {
    var exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
      await TestHost.StartAsync(map: ws => ws.MapRequest("bad name", (_, _) => ValueTask.CompletedTask)));
    Assert.That(exception.Message, Does.Contain("bad name"));
  }

  [Test]
  public async Task RefusesNamesMappedTwice()
  {
    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
      await TestHost.StartAsync(map: ws => ws.MapRequest("echo.text", (_, _) => ValueTask.CompletedTask)));
  }

  [Test]
  public void RefusesUnknownErrorCodes()
  {
    Assert.Throws<ArgumentException>(() => _ = new CallException("Not Found", "x"));
    Assert.That(new CallException("timeout", "x").Code, Is.EqualTo("timeout"));
    Assert.That(Encoding.ASCII.GetString("a_1"u8), Is.EqualTo(new CallException("a_1", "x").Code));
  }
}

// A result the serializer cannot write: reading its property throws.
sealed class Unwritable
{
  public int Value => throw new InvalidOperationException("A secret the browser must not see.");
}

[JsonSerializable(typeof(Unwritable))]
sealed partial class UnwritableJson : JsonSerializerContext;
