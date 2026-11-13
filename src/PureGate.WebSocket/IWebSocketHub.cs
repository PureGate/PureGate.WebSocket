using PureGate.WebSocket.Hosting;

namespace PureGate.WebSocket;

// The connections on this server, for sending events to them from anywhere in the app. Covers every
// PureGate.WebSocket endpoint; a connection only receives the events its endpoint maps.
public interface IWebSocketHub
{
  WebSocketTarget All { get; }

  // Every connection of the user with this id (see PureGateWebSocketOptions.UserId), such as all their tabs.
  WebSocketTarget User(string userId);

  WebSocketTarget Group(string name);

  WebSocketTarget Connection(string connectionId);

  bool TryGetConnection(string connectionId, out WebSocketConnection connection);
}

// Some of the hub's connections, to send an event to.
public readonly struct WebSocketTarget
{
  internal WebSocketTarget(WebSocketHub hub, TargetKind kind, string key, WebSocketConnection except)
  {
    Hub = hub;
    Kind = kind;
    Key = key;
    ExceptConnection = except;
  }

  internal WebSocketHub Hub { get; }

  internal TargetKind Kind { get; }

  internal string Key { get; }

  internal WebSocketConnection ExceptConnection { get; }

  // The same connections without this one, typically the sender's.
  public WebSocketTarget Except(WebSocketConnection connection) => new(Hub, Kind, Key, connection);

  // Queues the event for every connection. Returns once it is queued, not once it arrived.
  public ValueTask SendAsync<T>(ServerEvent<T> serverEvent, T payload, CancellationToken cancellationToken = default)
  {
    if (Hub is null)
      throw new InvalidOperationException("This target was not made by a hub.");

    Hub.Send(in this, serverEvent, payload);
    return ValueTask.CompletedTask;
  }
}
