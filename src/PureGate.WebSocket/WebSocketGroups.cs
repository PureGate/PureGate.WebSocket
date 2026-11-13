using System.Collections;
using System.Collections.Concurrent;

namespace PureGate.WebSocket;

// The groups a connection is in, for hub.Group(name). Left automatically when the session ends.
public sealed class WebSocketGroups : IReadOnlyCollection<string>
{
  readonly WebSocketConnection Connection;
  readonly ConcurrentDictionary<string, byte> Names = new(StringComparer.Ordinal);

  internal WebSocketGroups(WebSocketConnection connection) => Connection = connection;

  public int Count => Names.Count;

  public void Add(string group)
  {
    ArgumentException.ThrowIfNullOrEmpty(group);
    if (Names.TryAdd(group, 0) && !Connection.IsEnded)
      Connection.Endpoint.Hub.AddToGroup(Connection, group);
  }

  public bool Remove(string group)
  {
    ArgumentException.ThrowIfNullOrEmpty(group);
    if (!Names.TryRemove(group, out _))
      return false;

    Connection.Endpoint.Hub.RemoveFromGroup(Connection, group);
    return true;
  }

  public bool Contains(string group) => Names.ContainsKey(group);

  public IEnumerator<string> GetEnumerator() => Names.Keys.GetEnumerator();

  IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
