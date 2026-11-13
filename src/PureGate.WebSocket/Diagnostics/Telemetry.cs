using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace PureGate.WebSocket.Diagnostics;

// Metrics and traces, both named "PureGate.WebSocket": add them to OpenTelemetry with AddMeter and AddSource. Nothing
// is measured or allocated for them until something listens.
sealed class Telemetry : IDisposable
{
  public const string Name = "PureGate.WebSocket";

  // One for the process, as ActivitySources are.
  public static readonly ActivitySource Source = new(Name);

  // Created by the app's IMeterFactory, so each app has its own.
  public Meter Meter { get; }

  public Telemetry(IMeterFactory factory)
  {
    Meter = factory?.Create(Name) ?? new Meter(Name);
    Connections = Meter.CreateUpDownCounter<long>(
      "puregate.websocket.connections", "{connection}", "Open WebSocket connections.");
    Sessions = Meter.CreateUpDownCounter<long>(
      "puregate.websocket.sessions", "{session}", "Sessions, including those waiting for the browser to resume.");
    Resumes = Meter.CreateCounter<long>(
      "puregate.websocket.resumes", "{resume}", "Resume attempts, by whether the session carried on.");
    OperationDuration = Meter.CreateHistogram<double>(
      "puregate.websocket.operation.duration", "s", "How long calls, streams and uploads took, by operation and outcome.",
      advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10] });
    EventsReceived = Meter.CreateCounter<long>(
      "puregate.websocket.events.received", "{event}", "Events from browsers, by event.");
    EventsSent = Meter.CreateCounter<long>(
      "puregate.websocket.events.sent", "{event}", "Events sent to browsers.");
    SlowConsumers = Meter.CreateCounter<long>(
      "puregate.websocket.slow_consumers", "{connection}", "Connections that fell too far behind.");
  }

  public UpDownCounter<long> Connections { get; }

  public UpDownCounter<long> Sessions { get; }

  public Counter<long> Resumes { get; }

  public Histogram<double> OperationDuration { get; }

  public Counter<long> EventsReceived { get; }

  public Counter<long> EventsSent { get; }

  public Counter<long> SlowConsumers { get; }

  // A server span for an operation, continuing the browser's trace when it sent a traceparent. Null when nothing
  // listens.
  public static Activity StartActivity(string operation, string traceParent, string connectionId)
  {
    if (!Source.HasListeners())
      return null;

    ActivityContext.TryParse(traceParent, null, out var parent);
    var activity = Source.StartActivity(operation, ActivityKind.Server, parent);
    if (activity is { IsAllDataRequested: true })
    {
      activity.SetTag("rpc.system", "puregate.websocket");
      activity.SetTag("rpc.method", operation);
      activity.SetTag("puregate.websocket.connection", connectionId);
    }

    return activity;
  }

  // Ends an operation's span and records its duration. outcome is "ok" or the error code it ended with.
  public void Complete(Activity activity, string operation, long started, string outcome)
  {
    if (activity is not null)
    {
      if (outcome != "ok")
        activity.SetStatus(ActivityStatusCode.Error, outcome);
      activity.SetTag("puregate.websocket.outcome", outcome);
      activity.Dispose();
    }

    if (OperationDuration.Enabled)
    {
      OperationDuration.Record(
        Stopwatch.GetElapsedTime(started).TotalSeconds,
        new KeyValuePair<string, object>("puregate.websocket.operation", operation),
        new KeyValuePair<string, object>("puregate.websocket.outcome", outcome));
    }
  }

  public void Dispose() => Meter.Dispose();
}
