using System.Collections.Concurrent;
using PureGate.WebSocket.Transport;

namespace PureGate.WebSocket.Hosting;

enum TargetKind
{
  None,
  All,
  User,
  Group,
  Connection,
}

// The connections on this server, by id, user and group. Reads are lock-free; adding and removing take a lock,
// so a group or user is never dropped while someone joins it.
sealed class WebSocketHub : IWebSocketHub
{
  readonly ConcurrentDictionary<string, WebSocketConnection> Connections = new(StringComparer.Ordinal);
  readonly ConcurrentDictionary<string, ConcurrentDictionary<WebSocketConnection, byte>> Users = new(StringComparer.Ordinal);
  readonly ConcurrentDictionary<string, ConcurrentDictionary<WebSocketConnection, byte>> Groups = new(StringComparer.Ordinal);
  readonly Lock Gate = new();
  EndpointState[] Endpoints = [];

  public WebSocketTarget All => new(this, TargetKind.All, null, null);

  public WebSocketTarget User(string userId) => new(this, TargetKind.User, Required(userId), null);

  public WebSocketTarget Group(string name) => new(this, TargetKind.Group, Required(name), null);

  public WebSocketTarget Connection(string connectionId) => new(this, TargetKind.Connection, Required(connectionId), null);

  public bool TryGetConnection(string connectionId, out WebSocketConnection connection) =>
    Connections.TryGetValue(connectionId, out connection);

  internal void AddEndpoint(EndpointState endpoint)
  {
    lock (Gate)
      Endpoints = [.. Endpoints, endpoint];
  }

  internal void Add(WebSocketConnection connection)
  {
    lock (Gate)
    {
      Connections[connection.Id] = connection;
      if (connection.UserId is not null)
        Join(Users, connection.UserId, connection);
    }
  }

  internal void Remove(WebSocketConnection connection)
  {
    lock (Gate)
    {
      Connections.TryRemove(connection.Id, out _);
      if (connection.UserId is not null)
        Leave(Users, connection.UserId, connection);
      foreach (var group in connection.Groups)
        Leave(Groups, group, connection);
    }
  }

  internal void AddToGroup(WebSocketConnection connection, string group)
  {
    lock (Gate)
    {
      if (Connections.ContainsKey(connection.Id))
        Join(Groups, group, connection);
    }
  }

  internal void RemoveFromGroup(WebSocketConnection connection, string group)
  {
    lock (Gate)
      Leave(Groups, group, connection);
  }

  // Sends one event to every connection the target names. The payload is serialized once per endpoint, and
  // every connection copies the same bytes.
  internal void Send<T>(in WebSocketTarget target, ServerEvent<T> descriptor, T payload)
  {
    ArgumentNullException.ThrowIfNull(descriptor);
    var mapped = false;
    foreach (var endpoint in Endpoints)
      mapped |= endpoint.TryGetEvent(descriptor, out _);
    if (!mapped)
      throw new InvalidOperationException($"The event '{descriptor.Name}' is not mapped on any endpoint: call MapEvent first.");

    EndpointState serializedFor = null;
    ServerEventInfo<T> info = null;
    PooledBuffer buffer = null;
    try
    {
      foreach (var connection in Resolve(target))
      {
        if (connection == target.ExceptConnection)
          continue;

        if (connection.Endpoint != serializedFor)
        {
          if (!connection.Endpoint.TryGetEvent(descriptor, out info))
            continue;

          buffer?.Return();
          buffer = new PooledBuffer(256);
          info.Codec.Write(buffer, payload);
          serializedFor = connection.Endpoint;
        }

        connection.WriteEvent(info.Descriptor.Name, info.Descriptor.NameUtf8, info.Codec.IsBinary, buffer.WrittenSpan);
      }
    }
    finally
    {
      buffer?.Return();
    }
  }

  IEnumerable<WebSocketConnection> Resolve(WebSocketTarget target) => target.Kind switch
  {
    TargetKind.All => Connections.Values,
    TargetKind.User => Users.TryGetValue(target.Key, out var user) ? user.Keys : [],
    TargetKind.Group => Groups.TryGetValue(target.Key, out var group) ? group.Keys : [],
    TargetKind.Connection => Connections.TryGetValue(target.Key, out var connection) ? [connection] : [],
    _ => throw new InvalidOperationException("This target was not made by a hub."),
  };

  static void Join(
      ConcurrentDictionary<string, ConcurrentDictionary<WebSocketConnection, byte>> index,
      string key,
      WebSocketConnection connection) =>
    index.GetOrAdd(key, static _ => new ConcurrentDictionary<WebSocketConnection, byte>())[connection] = 0;

  static void Leave(
      ConcurrentDictionary<string, ConcurrentDictionary<WebSocketConnection, byte>> index,
      string key,
      WebSocketConnection connection)
  {
    if (index.TryGetValue(key, out var members) && members.TryRemove(connection, out _) && members.IsEmpty)
      index.TryRemove(key, out _);
  }

  static string Required(string value)
  {
    ArgumentException.ThrowIfNullOrEmpty(value);
    return value;
  }
}
