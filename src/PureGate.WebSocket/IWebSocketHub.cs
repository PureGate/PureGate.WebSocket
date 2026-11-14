using PureGate.WebSocket.Hosting;

namespace PureGate.WebSocket;

/// <summary>
/// The app's connections, for sending events to them from anywhere in the app. Covers every PureGate.WebSocket
/// endpoint; a connection only receives the events its endpoint maps. Sends reach this server's connections, and with
/// an IWebSocketBackplane those on the app's other servers too.
/// </summary>
public interface IWebSocketHub
{
  /// <summary>
  /// Every connection.
  /// </summary>
  WebSocketTarget All { get; }

  /// <summary>
  /// Every connection of the user with this id (see PureGateWebSocketOptions.UserId), such as all their tabs.
  /// </summary>
  WebSocketTarget User(string userId);

  /// <summary>
  /// Every connection in the group with this name (see WebSocketConnection.Groups).
  /// </summary>
  WebSocketTarget Group(string name);

  /// <summary>
  /// The connection with this id.
  /// </summary>
  WebSocketTarget Connection(string connectionId);

  /// <summary>
  /// Finds a connection on this server only.
  /// </summary>
  bool TryGetConnection(string connectionId, out WebSocketConnection connection);
}

/// <summary>
/// Some of the hub's connections, to send an event to.
/// </summary>
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

  /// <summary>
  /// The same connections without this one, typically the sender's.
  /// </summary>
  public WebSocketTarget Except(WebSocketConnection connection) => new(Hub, Kind, Key, connection);

  /// <summary>
  /// Queues the event for every connection on this server, and with a backplane publishes it for the other servers.
  /// Returns once it is queued and published, not once it arrived. A backplane that fails to publish fails the send,
  /// after this server's connections got the event.
  /// </summary>
  public ValueTask SendAsync<T>(ServerEvent<T> serverEvent, T payload, CancellationToken cancellationToken = default)
  {
    if (Hub is null)
      throw new InvalidOperationException("This target was not made by a hub.");

    return Hub.SendAsync(in this, serverEvent, payload, cancellationToken);
  }
}
