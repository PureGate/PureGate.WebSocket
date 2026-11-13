using System.Net.WebSockets;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Test.Integration;

public sealed class StreamTests
{
  static readonly TimeSpan Quiet = TimeSpan.FromMilliseconds(200);

  TestHost Host;

  [OneTimeSetUp]
  public async Task StartAsync() => Host = await TestHost.StartAsync();

  [OneTimeTearDown]
  public async Task StopAsync() => await Host.DisposeAsync();

  [Test]
  public async Task AnnouncesTheStreamsWindow()
  {
    await using var client = await Host.ConnectAndHelloAsync();

    Assert.That(client.Welcome.GetProperty("streams").GetInt32(), Is.EqualTo(4));
  }

  // Downloads.

  [Test]
  public async Task DownloadsAStream()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Download(1, "feed.count", credit: 10, "3"));

    for (var i = 1; i <= 3; i++)
    {
      var item = await client.ReceiveAsync();
      Assert.That((item.Kind, item.Id, item.Text), Is.EqualTo((FrameKind.StreamItem, 1u, i.ToString())));
    }

    var end = await client.ReceiveAsync();
    Assert.That((end.Kind, end.Id, end.Has(FrameFlags.HasError)), Is.EqualTo((FrameKind.StreamEnd, 1u, false)));
  }

  [Test]
  public async Task PullsTheProducerOnlyAsFarAsTheCredit()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Download(1, "feed.notes", credit: 3));

    for (var i = 1; i <= 3; i++)
      Assert.That((await client.ReceiveAsync(FrameKind.StreamItem, 1)).Json().GetProperty("number").GetInt32(), Is.EqualTo(i));
    Assert.That(await client.ReceivesWithinAsync(Quiet), Is.False);
    Assert.That((await client.CallAsync(2, "feed.produced")).Text, Is.EqualTo("3"));

    await client.SendAsync(Outgoing.Grant(1, 2));
    for (var i = 4; i <= 5; i++)
      Assert.That((await client.ReceiveAsync(FrameKind.StreamItem, 1)).Json().GetProperty("number").GetInt32(), Is.EqualTo(i));
    Assert.That((await client.CallAsync(3, "feed.produced")).Text, Is.EqualTo("5"));
  }

  [Test]
  public async Task EndsACancelledStreamWithCancelled()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Download(1, "feed.notes", credit: 1));
    await client.ReceiveAsync(FrameKind.StreamItem, 1);

    await client.SendAsync(Outgoing.Cancel(1));
    var end = await client.ReceiveAsync(FrameKind.StreamEnd, 1);

    Assert.That(end.Error, Is.EqualTo("cancelled"));
    Assert.That((await client.CallAsync(2, "feed.cancelled")).Text, Is.EqualTo("1"));
  }

  [Test]
  public async Task FreesTheIdWhenTheStreamEnds()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Download(1, "feed.count", credit: 1, "0"));
    await client.ReceiveAsync(FrameKind.StreamEnd, 1);

    Assert.That((await client.CallAsync(1, "math.add", """{"a":1,"b":2}""")).Text, Is.EqualTo("3"));
  }

  [Test]
  public async Task DownloadsBinaryItems()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Download(1, "feed.bytes", credit: 5, "2"));

    for (byte i = 0; i < 2; i++)
    {
      var item = await client.ReceiveAsync(FrameKind.StreamItem, 1);
      Assert.That(item.Has(FrameFlags.Binary), Is.True);
      Assert.That(item.Payload, Is.EqualTo(new[] { i, i, i }));
    }

    await client.ReceiveAsync(FrameKind.StreamEnd, 1);
  }

  [Test]
  public async Task EndsAStreamWithItsHandlersError()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Download(1, "feed.fail", credit: 5, "2"), Outgoing.Download(2, "feed.crash", credit: 5));

    var failed = await client.ReceiveAsync(FrameKind.StreamEnd, 1);
    Assert.That((failed.Error, failed.ErrorMessage), Is.EqualTo(("broken", "The feed broke.")));
    Assert.That(failed.Json().GetProperty("id").GetInt32(), Is.EqualTo(2));

    var crashed = await client.ReceiveAsync(FrameKind.StreamEnd, 2);
    Assert.That(crashed.Error, Is.EqualTo("handler_failed"));
    Assert.That(crashed.ErrorMessage, Does.Not.Contain("secret"));
  }

  [Test]
  public async Task EndsAStreamWhoseItemIsTooLarge()
  {
    await using var client = await Host.ConnectAndHelloAsync("""{"calls":16,"maxMessage":16384}""");
    await client.SendAsync(Outgoing.Download(1, "feed.huge", credit: 5, "20000"));

    Assert.That((await client.ReceiveAsync(FrameKind.StreamItem, 1)).Text, Is.EqualTo("\"first\""));
    Assert.That((await client.ReceiveAsync(FrameKind.StreamEnd, 1)).Error, Is.EqualTo("payload_too_large"));
  }

  [Test]
  public async Task RefusesStreamsItCannotStart()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(
      Outgoing.Download(1, "echo.note", credit: 1),
      Outgoing.Download(2, "store.sum", credit: 1),
      Outgoing.Download(3, "feed.count", credit: 1, "\"three\""));

    Assert.That((await client.ReceiveAsync(FrameKind.StreamEnd, 1)).Error, Is.EqualTo("unknown_operation"));
    Assert.That((await client.ReceiveAsync(FrameKind.StreamEnd, 2)).Error, Is.EqualTo("unknown_operation"));
    Assert.That((await client.ReceiveAsync(FrameKind.StreamEnd, 3)).Error, Is.EqualTo("invalid_payload"));
  }

  [Test]
  public async Task IgnoresCreditAndCancelForEndedStreams()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Grant(9, 5), Outgoing.Cancel(9));

    Assert.That((await client.CallAsync(1, "math.add", """{"a":1,"b":1}""")).Text, Is.EqualTo("2"));
  }

  // Uploads.

  [Test]
  public async Task UploadsWithinTheCreditItGrants()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Upload(1, "store.sum"));

    var sent = await UploadAsync(client, 1, Enumerable.Range(1, 20).Select(i => i.ToString()));
    await client.SendAsync(Outgoing.End(1));

    Assert.That(sent, Is.EqualTo(20));
    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Text, Is.EqualTo("210"));
  }

  [Test]
  public async Task UploadsWithARequest()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Upload(1, "store.join", "\"-\""));
    await UploadAsync(client, 1, ["\"a\"", "\"b\"", "\"c\""]);
    await client.SendAsync(Outgoing.End(1));

    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Text, Is.EqualTo("\"a-b-c\""));
  }

  [Test]
  public async Task UploadsBinaryItems()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Upload(1, "store.size"));
    await client.ReceiveAsync(FrameKind.Credit, 1);

    var item = Outgoing.Item(1, null);
    item.Flags = FrameFlags.Binary;
    item.Payload = [1, 2, 3, 4, 5];
    await client.SendAsync(item, Outgoing.End(1));

    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Text, Is.EqualTo("5"));
  }

  [Test]
  public async Task UploadsWithoutAResult()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Upload(1, "store.count", "\"notes\""));
    await UploadAsync(client, 1, ["""{"text":"a","number":1}""", """{"text":"b","number":2}"""]);
    await client.SendAsync(Outgoing.End(1));
    var counted = await client.ReceiveAsync(FrameKind.Response, 1);

    await client.SendAsync(Outgoing.Upload(2, "store.drain"));
    await UploadAsync(client, 2, ["\"last\""]);
    await client.SendAsync(Outgoing.End(2));
    var drained = await client.ReceiveAsync(FrameKind.Response, 2);

    Assert.That((counted.Has(FrameFlags.HasError), counted.Payload.Length), Is.EqualTo((false, 0)));
    Assert.That((drained.Has(FrameFlags.HasError), drained.Payload.Length), Is.EqualTo((false, 0)));
    Assert.That((await client.CallAsync(3, "store.counted")).Text, Is.EqualTo("\"last\""));
  }

  [Test]
  public async Task AnswersBeforeTheUploadEndsAndDiscardsTheRest()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Upload(1, "store.first"));
    await client.ReceiveAsync(FrameKind.Credit, 1);
    await client.SendAsync(Outgoing.Item(1, "7"));

    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Text, Is.EqualTo("7"));

    // Items already on the way when the answer went out are still within credit.
    await client.SendAsync(Outgoing.Item(1, "8"), Outgoing.Item(1, "9"), Outgoing.End(1));
    Assert.That((await client.CallAsync(1, "math.add", """{"a":2,"b":2}""")).Text, Is.EqualTo("4"));
  }

  [Test]
  public async Task KeepsAnAnsweredUploadUntilTheBrowserEndsIt()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Upload(1, "store.first"));
    await client.ReceiveAsync(FrameKind.Credit, 1);
    await client.SendAsync(Outgoing.Item(1, "7"));
    await client.ReceiveAsync(FrameKind.Response, 1);

    await client.SendAsync(Outgoing.Request(1, "math.add", """{"a":2,"b":2}"""));

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  [Test]
  public async Task AnswersWithTheHandlersError()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Upload(1, "store.fail", "2"));
    await UploadAsync(client, 1, ["5", "6"]);

    var response = await client.ReceiveAsync(FrameKind.Response, 1);
    Assert.That((response.Error, response.ErrorMessage), Is.EqualTo(("rejected", "Item 6 was refused.")));
    Assert.That(response.Json().GetProperty("id").GetInt32(), Is.EqualTo(6));
  }

  [Test]
  public async Task PassesTheBrowsersFailureToTheHandler()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Upload(1, "store.sum"));
    await UploadAsync(client, 1, ["1"]);

    var end = Outgoing.End(1);
    end.Flags = FrameFlags.HasError;
    end.ErrorCustom = "source_failed";
    end.ErrorMessage = "The file could not be read.";
    await client.SendAsync(end);

    var response = await client.ReceiveAsync(FrameKind.Response, 1);
    Assert.That((response.Error, response.ErrorMessage), Is.EqualTo(("source_failed", "The file could not be read.")));
  }

  [Test]
  public async Task AnswersInvalidItemsWithInvalidPayload()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Upload(1, "store.sum"));
    await UploadAsync(client, 1, ["\"one\""]);

    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Error, Is.EqualTo("invalid_payload"));
  }

  [Test]
  public async Task CancelsAnUpload()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Upload(1, "store.hold"));
    await UploadAsync(client, 1, ["1", "2"]);

    await client.SendAsync(Outgoing.Cancel(1));

    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Error, Is.EqualTo("cancelled"));
    Assert.That((await client.CallAsync(1, "store.cancelled")).Text, Is.EqualTo("1"));
  }

  [Test]
  public async Task CancelsAnUploadAfterItsEnd()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Upload(1, "store.hold"));
    await UploadAsync(client, 1, ["1"]);
    await client.SendAsync(Outgoing.End(1));
    Assert.That(await client.ReceivesWithinAsync(Quiet), Is.False);

    await client.SendAsync(Outgoing.Cancel(1));

    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Error, Is.EqualTo("cancelled"));
    Assert.That((await client.CallAsync(1, "store.cancelled")).Text, Is.EqualTo("1"));
  }

  [Test]
  public async Task RefusesUploadsItCannotStartAndWaitsForTheirEnd()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Upload(1, "feed.count"), Outgoing.Upload(2, "store.join", "5"));

    Assert.That((await client.ReceiveAsync(FrameKind.Response, 1)).Error, Is.EqualTo("unknown_operation"));
    Assert.That((await client.ReceiveAsync(FrameKind.Response, 2)).Error, Is.EqualTo("invalid_payload"));

    await client.SendAsync(Outgoing.End(1), Outgoing.Cancel(2));
    Assert.That((await client.CallAsync(1, "math.add", """{"a":1,"b":2}""")).Text, Is.EqualTo("3"));
    Assert.That((await client.CallAsync(2, "math.add", """{"a":2,"b":2}""")).Text, Is.EqualTo("4"));
  }

  // Protocol errors.

  [Test]
  public async Task ClosesAConnectionThatOpensTooManyStreams()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync([.. Enumerable.Range(1, 5).Select(id => Outgoing.Download((uint)id, "feed.notes", credit: 1))]);

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  [Test]
  public async Task CountsUploadsAgainstTheStreamsWindow()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(
      Outgoing.Upload(1, "store.hold"),
      Outgoing.Upload(2, "store.hold"),
      Outgoing.Download(3, "feed.notes", credit: 1),
      Outgoing.Download(4, "feed.notes", credit: 1),
      Outgoing.Upload(5, "store.hold"));

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  [Test]
  public async Task ClosesAConnectionThatReusesABusyId()
  {
    await using var first = await Host.ConnectAndHelloAsync();
    await first.SendAsync(Outgoing.Request(1, "calls.delay", "5000"), Outgoing.Download(1, "feed.notes", credit: 1));
    Assert.That(await first.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));

    await using var second = await Host.ConnectAndHelloAsync();
    await second.SendAsync(Outgoing.Download(1, "feed.notes", credit: 1), Outgoing.Request(1, "calls.delay", "5000"));
    Assert.That(await second.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  [Test]
  public async Task ClosesAConnectionThatSendsPastItsCredit()
  {
    // store.first answers after one item, so it grants nothing more: a handler that keeps reading would.
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Upload(1, "store.first"));
    var credit = await client.ReceiveAsync(FrameKind.Credit, 1);
    await client.SendAsync(Outgoing.Item(1, "1"));
    await client.ReceiveAsync(FrameKind.Response, 1);

    await client.SendAsync([.. Enumerable.Range(0, (int)credit.Amount).Select(_ => Outgoing.Item(1, "1"))]);

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  [Test]
  public async Task ClosesAConnectionThatSendsItemsWithoutCredit()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Upload(1, "feed.count"), Outgoing.Item(1, "1"));

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  [Test]
  public async Task ClosesAConnectionThatProducesADownloadsItems()
  {
    await using var first = await Host.ConnectAndHelloAsync();
    await first.SendAsync(Outgoing.Download(1, "feed.notes", credit: 1), Outgoing.Item(1, "1"));
    Assert.That(await first.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));

    await using var second = await Host.ConnectAndHelloAsync();
    await second.SendAsync(Outgoing.Download(1, "feed.notes", credit: 1), Outgoing.End(1));
    Assert.That(await second.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  [Test]
  public async Task ClosesAConnectionThatEndsAnUploadTwice()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Upload(1, "store.hold"), Outgoing.End(1), Outgoing.End(1));

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  [Test]
  public async Task ClosesAConnectionThatSendsEventCredit()
  {
    await using var client = await Host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Grant(0, 1));

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  // Sends the items as the server grants credit for them, and returns how many it sent.
  static async Task<int> UploadAsync(TestClient client, uint id, IEnumerable<string> items)
  {
    var credit = 0u;
    var sent = 0;
    foreach (var item in items)
    {
      while (credit == 0)
        credit += (await client.ReceiveAsync(FrameKind.Credit, id)).Amount;

      await client.SendAsync(Outgoing.Item(id, item));
      credit--;
      sent++;
    }

    return sent;
  }
}
