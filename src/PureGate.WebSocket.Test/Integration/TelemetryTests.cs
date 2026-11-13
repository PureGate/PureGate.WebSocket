using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using PureGate.WebSocket.Diagnostics;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Test.Integration;

public sealed class TelemetryTests
{
  const string TraceId = "0af7651916cd43dd8448eb211c80319c";
  const string TraceParent = $"00-{TraceId}-b7ad6b7169203331-01";

  sealed record Measurement(string Instrument, double Value, Dictionary<string, object> Tags);

  // Measurements of this host's instruments only: other tests' hosts have meters of the same name.
  static MeterListener Listen(TestHost host, ConcurrentQueue<Measurement> measurements)
  {
    var meter = host.Services.GetRequiredService<Telemetry>().Meter;
    var listener = new MeterListener
    {
      InstrumentPublished = (instrument, listener) =>
      {
        if (instrument.Meter == meter)
          listener.EnableMeasurementEvents(instrument);
      },
    };
    listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => measurements.Enqueue(new(instrument.Name, value, ToDictionary(tags))));
    listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => measurements.Enqueue(new(instrument.Name, value, ToDictionary(tags))));
    listener.Start();
    return listener;
  }

  static Dictionary<string, object> ToDictionary(ReadOnlySpan<KeyValuePair<string, object>> tags)
  {
    var dictionary = new Dictionary<string, object>();
    foreach (var tag in tags)
      dictionary[tag.Key] = tag.Value;
    return dictionary;
  }

  static ActivityListener Trace(ConcurrentQueue<Activity> activities) =>
    new()
    {
      ShouldListenTo = static source => source.Name == Telemetry.Name,
      Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
      ActivityStopped = activities.Enqueue,
    };

  [Test]
  public async Task MeasuresOperationsByOutcome()
  {
    await using var host = await TestHost.StartAsync();
    var measurements = new ConcurrentQueue<Measurement>();
    using var listener = Listen(host, measurements);
    await using var client = await host.ConnectAndHelloAsync();

    await client.CallAsync(1, "math.add", """{"a":1,"b":2}""");
    await client.CallAsync(2, "fail.notFound", "7");
    await client.SendAsync(Outgoing.Download(3, "feed.count", credit: 4, "2"));
    await client.ReceiveAsync(FrameKind.StreamEnd, 3);

    var durations = measurements.Where(m => m.Instrument == "puregate.websocket.operation.duration").ToArray();
    Assert.That(
      durations.Select(m => ((string)m.Tags["puregate.websocket.operation"], (string)m.Tags["puregate.websocket.outcome"])),
      Is.EquivalentTo(new[] { ("math.add", "ok"), ("fail.notFound", "not_found"), ("feed.count", "ok") }));
    Assert.That(durations.All(m => m.Value >= 0), Is.True);
  }

  [Test]
  public async Task CountsConnectionsSessionsAndEvents()
  {
    await using var host = await TestHost.StartAsync();
    var measurements = new ConcurrentQueue<Measurement>();
    using var listener = Listen(host, measurements);

    var client = await host.ConnectAndHelloAsync();
    await client.SendAsync(Outgoing.Event("telemetry.click", """{"x":1,"y":2}"""));
    await client.CallAsync(1, "events.echo", "\"hi\"");
    await client.CloseAsync();
    await client.DisposeAsync();
    await Task.Delay(200);

    double Sum(string instrument) => measurements.Where(m => m.Instrument == instrument).Sum(m => m.Value);
    Assert.That(measurements.Count(m => m.Instrument == "puregate.websocket.connections"), Is.EqualTo(2));
    Assert.That(Sum("puregate.websocket.connections"), Is.Zero);
    Assert.That(Sum("puregate.websocket.sessions"), Is.Zero);
    Assert.That(Sum("puregate.websocket.events.received"), Is.EqualTo(1));
    Assert.That(Sum("puregate.websocket.events.sent"), Is.EqualTo(1));
  }

  [Test]
  public async Task CountsResumes()
  {
    await using var host = await TestHost.StartAsync();
    var measurements = new ConcurrentQueue<Measurement>();
    using var listener = Listen(host, measurements);

    var first = await host.ConnectAndHelloAsync();
    await first.DisposeAsync();
    await using var second = await host.ConnectAsync();
    await second.HelloAsync(TestClient.ResumeHello(first.Session, 0));
    await using var third = await host.ConnectAsync();
    await third.HelloAsync(TestClient.ResumeHello("unknown", 0));

    var resumes = measurements.Where(m => m.Instrument == "puregate.websocket.resumes")
      .Select(m => (bool)m.Tags["puregate.websocket.resumed"]);
    Assert.That(resumes, Is.EquivalentTo(new[] { true, false }));
  }

  [Test]
  public async Task TracesOperationsInTheBrowsersTrace()
  {
    var activities = new ConcurrentQueue<Activity>();
    using var listener = Trace(activities);
    ActivitySource.AddActivityListener(listener);
    await using var host = await TestHost.StartAsync();
    await using var client = await host.ConnectAndHelloAsync();

    var add = Outgoing.Request(1, "math.add", """{"a":1,"b":2}""");
    add.Flags = FrameFlags.HasTrace;
    add.Trace = TraceParent;
    var fail = Outgoing.Request(2, "fail.notFound", "7");
    fail.Flags = FrameFlags.HasTrace;
    fail.Trace = TraceParent;
    await client.SendAsync(add, fail);
    await client.ReceiveAsync(FrameKind.Response, 1);
    await client.ReceiveAsync(FrameKind.Response, 2);
    await Task.Delay(100);

    var mine = activities.Where(activity => activity.TraceId.ToHexString() == TraceId).ToDictionary(activity => activity.OperationName);
    Assert.That(mine.Keys, Is.EquivalentTo(new[] { "math.add", "fail.notFound" }));
    Assert.That(mine["math.add"].ParentSpanId.ToHexString(), Is.EqualTo("b7ad6b7169203331"));
    Assert.That(mine["math.add"].Kind, Is.EqualTo(ActivityKind.Server));
    Assert.That(mine["math.add"].GetTagItem("rpc.method"), Is.EqualTo("math.add"));
    Assert.That(mine["math.add"].Status, Is.EqualTo(ActivityStatusCode.Unset));
    Assert.That((mine["fail.notFound"].Status, mine["fail.notFound"].StatusDescription), Is.EqualTo((ActivityStatusCode.Error, "not_found")));
  }

  [Test]
  public async Task CarriesTheTraceToTheBrowsersHandler()
  {
    var activities = new ConcurrentQueue<Activity>();
    using var listener = Trace(activities);
    ActivitySource.AddActivityListener(listener);
    await using var host = await TestHost.StartAsync();
    await using var client = await host.ConnectAndHelloAsync();

    var ask = Outgoing.Request(1, "browser.confirm", "\"sure?\"");
    ask.Flags = FrameFlags.HasTrace;
    ask.Trace = TraceParent;
    await client.SendAsync(ask);
    var invoke = await client.ReceiveAsync();

    Assert.That((invoke.Kind, invoke.Has(FrameFlags.HasTrace)), Is.EqualTo((FrameKind.Request, true)));
    Assert.That(invoke.Trace, Does.StartWith($"00-{TraceId}-"));
  }
}
