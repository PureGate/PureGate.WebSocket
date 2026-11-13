using PureGate.WebSocket.Hosting;

namespace PureGate.WebSocket;

// The app's connections, for sending events to them from anywhere in the app. Covers every PureGate.WebSocket
// endpoint; a connection only receives the events its endpoint maps. Sends reach this server's connections, and with
// an IWebSocketBackplane those on the app's other servers too.
public interface IWebSocketHub
{
  WebSocketTarget All { get; }

  // Every connection of the user with this id (see PureGateWebSocketOptions.UserId), such as all their tabs.
  WebSocketTarget User(string userId);

  WebSocketTarget Group(string name);

  WebSocketTarget Connection(string connectionId);

  // Finds a connection on this server only.
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

  // Queues the event for every connection on this server, and with a backplane publishes it for the other servers.
  // Returns once it is queued and published, not once it arrived. A backplane that fails to publish fails the send,
  // after this server's connections got the event.
  public ValueTask SendAsync<T>(ServerEvent<T> serverEvent, T payload, CancellationToken cancellationToken = default)
  {
    if (Hub is null)
      throw new InvalidOperationException("This target was not made by a hub.");

    return Hub.SendAsync(in this, serverEvent, payload, cancellationToken);
  }
}
