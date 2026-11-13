using PureGate.WebSocket.Diagnostics;
using PureGate.WebSocket.Hosting;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.Transport;

namespace PureGate.WebSocket;

// Events in both directions. The browser may send EventWindow events the server has not credited back; handlers
// picking them up return the credit in batches. Events to the browser have no window: a browser that falls behind
// meets the slow-consumer policy instead.
public sealed partial class WebSocketConnection
{
  readonly EventQueue Events;
  int UncreditedEvents;
  int PickedUpEvents;

  // Queues the event for this connection. Returns once it is queued, not once it arrived.
  public ValueTask SendAsync<T>(ServerEvent<T> serverEvent, T payload, CancellationToken cancellationToken = default)
  {
    ArgumentNullException.ThrowIfNull(serverEvent);
    if (!Endpoint.TryGetEvent(serverEvent, out var info))
      throw new InvalidOperationException($"The event '{serverEvent.Name}' is not mapped on this endpoint: call MapEvent first.");

    var buffer = PooledBuffer.Scratch();
    try
    {
      var codec = info.Codecs[Format];
      codec.Write(buffer, payload);
      WriteEvent(serverEvent.Name, serverEvent.NameUtf8, codec.IsBinary, buffer.WrittenSpan);
    }
    finally
    {
      buffer.ReturnScratch();
    }

    return ValueTask.CompletedTask;
  }

  // Sends an event, unless the connection is draining for a shutdown, or the page is too far behind and the policy
  // drops events. A broadcast dispatches the send to the thread pool (Outbox.Write).
  internal void WriteEvent(string name, ReadOnlySpan<byte> nameUtf8, bool binary, ReadOnlySpan<byte> payload, bool dispatch = false)
  {
    if (Draining)
      return;

    if (Options.SlowConsumer == SlowConsumerPolicy.DropEvents && IsFarBehind)
    {
      Log.EventNotSent(Logger, Id, name);
      return;
    }

    var frame = new Frame
    {
      Kind = FrameKind.Event,
      Flags = binary ? FrameFlags.Binary : FrameFlags.None,
      Payload = payload,
    };

    switch (Outbox.Write(in frame, name, nameUtf8, dispatch))
    {
      case WriteResult.TooLarge:
        Log.EventTooLarge(Logger, Id, name);
        break;
      case WriteResult.Overloaded when Options.SlowConsumer == SlowConsumerPolicy.DropEvents:
        Log.EventNotSent(Logger, Id, name);
        break;
      case WriteResult.Written:
        Endpoint.Telemetry.EventsSent.Add(1);
        CheckBacklog(WriteResult.Written);
        break;
      case var result:
        CheckBacklog(result);
        break;
    }
  }

  void OnEvent(scoped in Frame frame)
  {
    if (Interlocked.Increment(ref UncreditedEvents) > Options.EventWindow)
      ProtocolException.Throw(ProtocolViolation.WindowExceeded);

    var operation = Names.Resolve(frame.NameCode, frame.NameText);
    if (operation.Kind != OperationKind.ClientEvent)
    {
      DropEvent(null, "No event handler has this name.");
      return;
    }

    if (Endpoint.Telemetry.EventsReceived.Enabled)
      Endpoint.Telemetry.EventsReceived.Add(1, new KeyValuePair<string, object>("puregate.websocket.event", operation.Name));

    if (!TryAcquire())
    {
      DropEvent(operation.Name, "The connection is over its rate limit.");
      return;
    }

    operation.Start(this, in frame);
    ReleaseUnusedLease();
  }

  internal void QueueEvent(EventWork work)
  {
    if (Events is not null)
    {
      Events.Enqueue(work);
      return;
    }

    EventPickedUp();
    work.Connection = this;
    RunHandler(work);
  }

  internal WebSocketCall BeginEvent(string name, string traceParent, System.Threading.RateLimiting.RateLimitLease lease) =>
    new(this, name, 0, traceParent, Endpoint.Scopes) { Lease = lease };

  internal void DropEvent(string name, string reason)
  {
    Log.EventDropped(Logger, Id, name, reason);
    EventPickedUp();
  }

  internal void EventFailed(string name, Exception exception) => Log.EventHandlerFailed(Logger, Id, name, exception);

  // A handler took an event: its credit goes back to the browser, in batches of a quarter of the window.
  internal void EventPickedUp()
  {
    if (Interlocked.Increment(ref PickedUpEvents) < Math.Max(1, Options.EventWindow / 4))
      return;

    var amount = Interlocked.Exchange(ref PickedUpEvents, 0);
    if (amount == 0)
      return;

    Interlocked.Add(ref UncreditedEvents, -amount);
    var credit = new Frame { Kind = FrameKind.Credit, Id = 0, Amount = (uint)amount };
    Write(in credit);
  }
}
