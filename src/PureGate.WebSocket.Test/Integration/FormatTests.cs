using System.Buffers;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.TestServer;

namespace PureGate.WebSocket.Test.Integration;

// Payload formats besides JSON (docs/PROTOCOL.md, Payload). The TestServer's marked-json is JSON after a marker byte,
// so every test can tell which format a payload travelled in.
public sealed class FormatTests
{
  const string MarkedHello = """{"calls":16,"maxMessage":1048576,"format":"marked-json"}""";

  TestHost Host;

  [OneTimeSetUp]
  public async Task StartAsync() => Host = await TestHost.StartAsync();

  [OneTimeTearDown]
  public async Task StopAsync() => await Host.DisposeAsync();

  [Test]
  public async Task CallsInTheFormatTheBrowserAskedFor()
  {
    await using var client = await Host.ConnectAndHelloAsync(MarkedHello);
    Assert.That(client.Welcome.GetProperty("format").GetString(), Is.EqualTo(MarkedJsonFormat.FormatName));

    await client.SendAsync(Request(1, "echo.note", """{"text":"héllo","number":7}"""));

    var response = await client.ReceiveAsync(FrameKind.Response, 1);
    Assert.That(response.Has(FrameFlags.Binary), Is.False);
    var note = Unmarked(response.Payload);
    Assert.That(note.GetProperty("text").GetString(), Is.EqualTo("héllo"));
    Assert.That(note.GetProperty("number").GetInt32(), Is.EqualTo(7));
  }

  [Test]
  public async Task UsesJsonWhenTheBrowserAsksForNoFormat()
  {
    await using var client = await Host.ConnectAndHelloAsync();

    Assert.That(client.Welcome.TryGetProperty("format", out _), Is.False);
    Assert.That((await client.CallAsync(1, "echo.text", "\"plain\"")).Text, Is.EqualTo("\"plain\""));
  }

  [Test]
  public async Task ClosesWhenTheServerHasNoFormatOfThatName()
  {
    await using var client = await Host.ConnectAsync();
    await client.SendAsync(Outgoing.Hello("""{"calls":16,"maxMessage":1048576,"format":"carrier-pigeon"}"""));

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4007));
  }

  [Test]
  public async Task RefusesPayloadsThatAreNotInTheFormat()
  {
    await using var client = await Host.ConnectAndHelloAsync(MarkedHello);

    var response = await client.CallAsync(1, "echo.note", """{"text":"plain","number":1}""");
    Assert.That(response.Error, Is.EqualTo("invalid_payload"));
  }

  [Test]
  public async Task SendsBytesAsTheyAreInEveryFormat()
  {
    await using var client = await Host.ConnectAndHelloAsync(MarkedHello);
    var request = Outgoing.Request(1, "echo.bytes");
    request.Flags = FrameFlags.Binary;
    request.Payload = [MarkedJsonFormat.Marker, 0, 1, 255];
    await client.SendAsync(request);

    var response = await client.ReceiveAsync(FrameKind.Response, 1);
    Assert.That(response.Has(FrameFlags.Binary), Is.True);
    Assert.That(response.Payload, Is.EqualTo(new byte[] { MarkedJsonFormat.Marker, 0, 1, 255 }));
  }

  // One broadcast reaches each connection in its own format.
  [Test]
  public async Task SendsEventsToEachConnectionInItsFormat()
  {
    await using var json = await Host.ConnectAndHelloAsync();
    await using var marked = await Host.ConnectAndHelloAsync(MarkedHello);

    Assert.That((await json.CallAsync(1, "events.broadcast", """{"text":"all","number":3}""")).Error, Is.Null);

    var plain = await json.ReceiveAsync(FrameKind.Event, 0);
    Assert.That(plain.Json().GetProperty("text").GetString(), Is.EqualTo("all"));
    var inFormat = await marked.ReceiveAsync(FrameKind.Event, 0);
    Assert.That(Unmarked(inFormat.Payload).GetProperty("text").GetString(), Is.EqualTo("all"));
  }

  [Test]
  public async Task TakesEventsInTheFormat()
  {
    await using var client = await Host.ConnectAndHelloAsync(MarkedHello);
    var click = Outgoing.Event("telemetry.click", "");
    click.Payload = Marked("""{"x":4,"y":5}""");
    await client.SendAsync(click, Request(1, "telemetry.waitFor", "1"));

    var clicks = Unmarked((await client.ReceiveAsync(FrameKind.Response, 1)).Payload);
    Assert.That(clicks.EnumerateArray().Single().GetProperty("x").GetInt32(), Is.EqualTo(4));
  }

  [Test]
  public async Task CallsTheBrowserInItsFormat()
  {
    await using var client = await Host.ConnectAndHelloAsync(MarkedHello);
    await client.SendAsync(Request(1, "browser.confirm", "\"sure?\""));

    var question = await client.ReceiveAsync(FrameKind.Request, 1);
    Assert.That(Unmarked(question.Payload).GetProperty("text").GetString(), Is.EqualTo("sure?"));
    var answer = Outgoing.Response(question.Id, null);
    answer.Payload = Marked("true");
    await client.SendAsync(answer);

    var response = await client.ReceiveAsync(FrameKind.Response, 1);
    Assert.That(Unmarked(response.Payload).GetString(), Is.EqualTo("True"));
  }

  [Test]
  public async Task StreamsInTheFormat()
  {
    await using var client = await Host.ConnectAndHelloAsync(MarkedHello);
    var open = Outgoing.Download(1, "feed.count", credit: 8);
    open.Payload = Marked("3");
    await client.SendAsync(open);

    for (var i = 1; i <= 3; i++)
      Assert.That(Unmarked((await client.ReceiveAsync(FrameKind.StreamItem, 1)).Payload).GetInt32(), Is.EqualTo(i));
    Assert.That((await client.ReceiveAsync(FrameKind.StreamEnd, 1)).Error, Is.Null);
  }

  [Test]
  public async Task UploadsInTheFormat()
  {
    await using var client = await Host.ConnectAndHelloAsync(MarkedHello);
    await client.SendAsync(Outgoing.Upload(1, "store.sum"));
    await client.ReceiveAsync(FrameKind.Credit, 1);

    var items = new[] { "2", "3" }.Select(json =>
    {
      var item = Outgoing.Item(1, null);
      item.Payload = Marked(json);
      return item;
    });
    await client.SendAsync([.. items, Outgoing.End(1)]);

    Assert.That(Unmarked((await client.ReceiveAsync(FrameKind.Response, 1)).Payload).GetInt32(), Is.EqualTo(5));
  }

  // What the session sent waits for the browser in the session's format, so only a Hello that asks for it resumes.
  [Test]
  public async Task ResumesInTheSessionsFormatOnly()
  {
    var first = await Host.ConnectAndHelloAsync(MarkedHello);
    var session = first.Session;
    await first.DisposeAsync();

    await using var json = await Host.ConnectAsync();
    await json.HelloAsync(TestClient.ResumeHello(session, 0));
    Assert.That(json.Welcome.GetProperty("resumed").GetBoolean(), Is.False);

    var second = await Host.ConnectAndHelloAsync(MarkedHello);
    var other = second.Session;
    await second.DisposeAsync();

    await using var marked = await Host.ConnectAsync();
    await marked.HelloAsync("{\"calls\":16,\"maxMessage\":1048576,\"format\":\"marked-json\",\"resume\":{\"session\":\"" + other + "\",\"received\":0}}");
    Assert.That(marked.Welcome.GetProperty("resumed").GetBoolean(), Is.True);
  }

  [TestCase("json")]
  [TestCase("marked-json")]
  [TestCase("MessagePack")]
  [TestCase("")]
  [TestCase("a-name-longer-than-thirty-two-chars")]
  public void RefusesFormatsWithoutAUsableName(string name)
  {
    Assert.ThrowsAsync<ArgumentException>(async () =>
      await TestHost.StartAsync(configure: options => options.PayloadFormats.Add(new NamedFormat(name))));
  }

  static Outgoing Request(uint id, string name, string json)
  {
    var request = Outgoing.Request(id, name);
    request.Payload = Marked(json);
    return request;
  }

  static byte[] Marked(string json) => [MarkedJsonFormat.Marker, .. Encoding.UTF8.GetBytes(json)];

  static JsonElement Unmarked(byte[] payload)
  {
    Assert.That(payload[0], Is.EqualTo(MarkedJsonFormat.Marker), "The payload is not in marked-json.");
    return JsonDocument.Parse(payload.AsMemory(1)).RootElement.Clone();
  }

  sealed class NamedFormat(string name) : IPayloadFormat
  {
    public string Name => name;

    public void Write<T>(IBufferWriter<byte> output, T value) => throw new NotSupportedException();

    public T Read<T>(ReadOnlyMemory<byte> payload) => throw new NotSupportedException();
  }
}
