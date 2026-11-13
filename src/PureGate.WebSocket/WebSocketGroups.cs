using System.Collections;
using System.Collections.Concurrent;

namespace PureGate.WebSocket;

// The groups a connection is in, for hub.Group(name). Left automatically when the session ends.
public sealed class WebSocketGroups : IReadOnlyCollection<string>
{
  readonly WebSocketConnection Connection;

  // Created when the connection joins its first group, with one lock: only the connection's handlers contend for it.
  ConcurrentDictionary<string, byte> Names;

  internal WebSocketGroups(WebSocketConnection connection) => Connection = connection;

  public int Count => Volatile.Read(ref Names)?.Count ?? 0;

  public void Add(string group)
  {
    ArgumentException.ThrowIfNullOrEmpty(group);
    var names = Volatile.Read(ref Names);
    if (names is null)
    {
      Interlocked.CompareExchange(ref Names, new ConcurrentDictionary<string, byte>(1, 4, StringComparer.Ordinal), null);
      names = Names;
    }

    if (names.TryAdd(group, 0) && !Connection.IsEnded)
      Connection.Endpoint.Hub.AddToGroup(Connection, group);
  }

  public bool Remove(string group)
  {
    ArgumentException.ThrowIfNullOrEmpty(group);
    if (Volatile.Read(ref Names) is not { } names || !names.TryRemove(group, out _))
      return false;

    Connection.Endpoint.Hub.RemoveFromGroup(Connection, group);
    return true;
  }

  public bool Contains(string group) => Volatile.Read(ref Names)?.ContainsKey(group) == true;

  public IEnumerator<string> GetEnumerator() =>
    Volatile.Read(ref Names) is { } names ? names.Keys.GetEnumerator() : Enumerable.Empty<string>().GetEnumerator();

  IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
