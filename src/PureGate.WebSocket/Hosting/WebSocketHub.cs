using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PureGate.WebSocket.Diagnostics;
using PureGate.WebSocket.Serialization;
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
// so a group or user is never dropped while someone joins it. With a backplane, sends reach the other servers too,
// and each delivers to its own connections.
sealed class WebSocketHub : IWebSocketHub
{
  readonly ConcurrentDictionary<string, WebSocketConnection> Connections = new(StringComparer.Ordinal);
  readonly ConcurrentDictionary<string, ConcurrentDictionary<WebSocketConnection, byte>> Users = new(StringComparer.Ordinal);
  readonly ConcurrentDictionary<string, ConcurrentDictionary<WebSocketConnection, byte>> Groups = new(StringComparer.Ordinal);
  readonly Lock Gate = new();
  EndpointState[] Endpoints = [];

  // With a backplane, each event's first mapping by its name: the backplane's messages carry the payload as it
  // writes it, and those from other servers find their event here.
  readonly ConcurrentDictionary<string, IRelayedEvent> FirstMappings = new(StringComparer.Ordinal);

  public WebSocketHub(IWebSocketBackplane backplane, ILoggerFactory loggers, Telemetry telemetry)
  {
    Logger = (loggers ?? NullLoggerFactory.Instance).CreateLogger("PureGate.WebSocket");
    if (backplane is not null)
      Backplane = new HubBackplane(backplane, Logger, telemetry);
  }

  internal HubBackplane Backplane { get; }

  ILogger Logger { get; }

  public WebSocketTarget All => new(this, TargetKind.All, null, null);

  public WebSocketTarget User(string userId) => new(this, TargetKind.User, Required(userId), null);

  public WebSocketTarget Group(string name) => new(this, TargetKind.Group, Required(name), null);

  public WebSocketTarget Connection(string connectionId) => new(this, TargetKind.Connection, Required(connectionId), null);

  public bool TryGetConnection(string connectionId, out WebSocketConnection connection) =>
    Connections.TryGetValue(connectionId, out connection);

  // With a backplane, an id starts with this server's, so a send to it from another server comes here.
  internal string NewConnectionId() =>
    Backplane is null ? WebSocketConnection.NewId(9) : string.Concat(Backplane.ServerId, ".", WebSocketConnection.NewId(9));

  internal void AddEndpoint(EndpointState endpoint)
  {
    lock (Gate)
      Endpoints = [.. Endpoints, endpoint];
  }

  // An endpoint maps an event. With a backplane, its name stands for one ServerEvent across endpoints, which the
  // other servers find it by.
  internal void EventMapped<T>(ServerEventInfo<T> info)
  {
    if (Backplane is null)
      return;

    if (FirstMappings.GetOrAdd(info.Name, info) is ServerEventInfo<T> first && first.Descriptor == info.Descriptor)
      return;

    throw new InvalidOperationException($"Another ServerEvent is mapped as '{info.Name}'. With a backplane, an event's name stands for one ServerEvent on every endpoint.");
  }

  internal void Add(WebSocketConnection connection)
  {
    lock (Gate)
    {
      Connections[connection.Id] = connection;
      if (connection.UserId is not null && Join(Users, connection.UserId, connection))
        Backplane?.Want(HubBackplane.UserTopic(connection.UserId), true);
    }
  }

  internal void Remove(WebSocketConnection connection)
  {
    lock (Gate)
    {
      Connections.TryRemove(connection.Id, out _);
      if (connection.UserId is not null && Leave(Users, connection.UserId, connection))
        Backplane?.Want(HubBackplane.UserTopic(connection.UserId), false);
      foreach (var group in connection.Groups)
      {
        if (Leave(Groups, group, connection))
          Backplane?.Want(HubBackplane.GroupTopic(group), false);
      }
    }
  }

  internal void AddToGroup(WebSocketConnection connection, string group)
  {
    lock (Gate)
    {
      if (Connections.ContainsKey(connection.Id) && Join(Groups, group, connection))
        Backplane?.Want(HubBackplane.GroupTopic(group), true);
    }
  }

  internal void RemoveFromGroup(WebSocketConnection connection, string group)
  {
    lock (Gate)
    {
      if (Leave(Groups, group, connection))
        Backplane?.Want(HubBackplane.GroupTopic(group), false);
    }
  }

  // Sends one event to every connection the target names: this server's, then with a backplane the other servers'.
  // The payload is serialized once per endpoint and format, and every connection copies the same bytes. The indexes
  // are enumerated as they are, without a snapshot: a connection that joins or leaves meanwhile may or may not get
  // the event.
  internal ValueTask SendAsync<T>(in WebSocketTarget target, ServerEvent<T> descriptor, T payload, CancellationToken cancellationToken)
  {
    ArgumentNullException.ThrowIfNull(descriptor);
    var mapped = false;
    foreach (var endpoint in Endpoints)
      mapped |= endpoint.TryGetEvent(descriptor, out _);
    if (!mapped)
      throw new InvalidOperationException($"The event '{descriptor.Name}' is not mapped on any endpoint: call MapEvent first.");

    WebSocketConnection only = null;
    if (target.Kind == TargetKind.Connection)
      Connections.TryGetValue(target.Key, out only);

    var topic = Backplane?.TopicOf(in target, foundHere: only is not null);
    PooledBuffer message = null;

    // One connection sends at once; for more, each send runs on the thread pool, so they go out in parallel.
    var delivery = new Delivery<T>(descriptor, payload, target.ExceptConnection, dispatch: target.Kind != TargetKind.Connection);
    try
    {
      if (topic is not null)
      {
        var first = (ServerEventInfo<T>)FirstMappings[descriptor.Name];
        message = Backplane.Write(in target, first, payload, out var shared);
        delivery.Share(first, shared);
      }

      Deliver(ref delivery, target.Kind, target.Key, only);
    }
    catch
    {
      message?.ReturnScratch();
      throw;
    }
    finally
    {
      delivery.Dispose();
    }

    return message is null ? default : Backplane.PublishAsync(topic, target.Kind, message, cancellationToken);
  }

  // A message from another server, through the backplane, for this server's connections of its target. One that
  // cannot be read is logged and dropped: nothing here throws back into the backplane. This server's own messages
  // are skipped without a count.
  internal void Receive(ReadOnlyMemory<byte> message)
  {
    try
    {
      if (!Backplane.TryRead(message.Span, out var read))
        return;

      if (!FirstMappings.TryGetValue(read.Event, out var relayed))
      {
        Log.BackplaneEventNotMapped(Logger, read.Event);
        Backplane.Received("not_mapped");
        return;
      }

      Backplane.Received(relayed.Relay(this, in read, message[read.PayloadOffset..]) ? "delivered" : "unreadable");
    }
    catch (Exception exception)
    {
      Log.BackplaneMessageDropped(Logger, exception);
      Backplane.Received("unreadable");
    }
  }

  // Delivers a message from another server. False when its payload could not be read for some of the connections.
  internal bool Relay<T>(ServerEventInfo<T> first, in BackplaneMessage read, ReadOnlyMemory<byte> payload)
  {
    if (read.Binary != first.Codecs[0].IsBinary)
      throw new InvalidDataException($"The event '{first.Name}' came as {(read.Binary ? "bytes" : "JSON")}, which its type here does not take.");

    WebSocketConnection only = null;
    if (read.Kind == TargetKind.Connection && !Connections.TryGetValue(read.Key, out only))
      return true;

    var delivery = new Delivery<T>(first, payload, dispatch: read.Kind != TargetKind.Connection);
    try
    {
      Deliver(ref delivery, read.Kind, read.Key, only);
    }
    finally
    {
      delivery.Dispose();
    }

    if (delivery.Unreadable is not { } exception)
      return true;

    Log.BackplanePayloadUnreadable(Logger, first.Name, exception);
    return false;
  }

  void Deliver<T>(ref Delivery<T> delivery, TargetKind kind, string key, WebSocketConnection only)
  {
    switch (kind)
    {
      case TargetKind.All:
        foreach (var pair in Connections)
          delivery.To(pair.Value);
        break;
      case TargetKind.User:
        if (Users.TryGetValue(key, out var user))
        {
          foreach (var pair in user)
            delivery.To(pair.Key);
        }

        break;
      case TargetKind.Group:
        if (Groups.TryGetValue(key, out var group))
        {
          foreach (var pair in group)
            delivery.To(pair.Key);
        }

        break;
      case TargetKind.Connection:
        if (only is not null)
          delivery.To(only);
        break;
      default:
        throw new InvalidOperationException("This target was not made by a hub.");
    }
  }

  // One event on its way to this server's connections of a target. Its payload is serialized for the endpoint of the
  // last connection, once for each format a connection there uses. With a backplane, the connections of the event's
  // first mapping that take JSON, and all that take raw bytes, get the backplane's payload as it is; a message from
  // another server is read only when a connection needs it written another way.
  struct Delivery<T> : IDisposable
  {
    readonly ServerEvent<T> Descriptor;
    readonly WebSocketConnection Except;
    readonly bool Dispatch;
    T Payload;
    bool HasPayload;

    // The payload as First writes it in JSON, or its raw bytes: what the backplane carries.
    ServerEventInfo<T> First;
    ReadOnlyMemory<byte> Shared;

    EndpointState SerializedFor;
    ServerEventInfo<T> Info;

    // By format. JSON's is apart, so an endpoint without other formats needs no array.
    PooledBuffer Json;
    PooledBuffer[] Others;

    // A send from this server.
    public Delivery(ServerEvent<T> descriptor, T payload, WebSocketConnection except, bool dispatch)
    {
      Descriptor = descriptor;
      Payload = payload;
      HasPayload = true;
      Except = except;
      Dispatch = dispatch;
    }

    // A send from another server, whose payload first wrote.
    public Delivery(ServerEventInfo<T> first, ReadOnlyMemory<byte> payload, bool dispatch)
    {
      Descriptor = first.Descriptor;
      First = first;
      Shared = payload;
      Dispatch = dispatch;
    }

    // Why the payload from another server could not be read, for the connections that needed it written another way.
    public Exception Unreadable { get; private set; }

    public void Share(ServerEventInfo<T> first, ReadOnlyMemory<byte> payload)
    {
      First = first;
      Shared = payload;
    }

    public void To(WebSocketConnection connection)
    {
      if (connection == Except)
        return;

      if (connection.Endpoint != SerializedFor)
      {
        // An endpoint that does not map the event leaves the payload serialized for the last one that does.
        if (!connection.Endpoint.TryGetEvent(Descriptor, out var found))
          return;

        Dispose();
        Info = found;
        SerializedFor = connection.Endpoint;
      }

      var codec = Info.Codecs[connection.Format];
      if (TrySerialize(connection.Format, codec, out var bytes))
        connection.WriteEvent(Info.Descriptor.Name, Info.Descriptor.NameUtf8, codec.IsBinary, bytes, Dispatch);
    }

    bool TrySerialize(int format, PayloadCodec<T> codec, out ReadOnlySpan<byte> bytes)
    {
      // Raw bytes travel the same in every format.
      if (format > 0 && codec == Info.Codecs[0])
        format = 0;

      if (format == 0 && Info == First)
      {
        bytes = Shared.Span;
        return true;
      }

      if (format > 0)
        Others ??= new PooledBuffer[Info.Codecs.Length];

      ref var buffer = ref format == 0 ? ref Json : ref Others[format];
      if (buffer is null)
      {
        if (!HasPayload && !TryReadShared())
        {
          bytes = default;
          return false;
        }

        buffer = new PooledBuffer(256);
        codec.Write(buffer, Payload);
      }

      bytes = buffer.WrittenSpan;
      return true;
    }

    bool TryReadShared()
    {
      if (Unreadable is not null)
        return false;

      try
      {
        Payload = First.Codecs[0].Read(Shared, First.Codecs[0].IsBinary);
        HasPayload = true;
        return true;
      }
      catch (Exception exception)
      {
        Unreadable = exception;
        return false;
      }
    }

    public void Dispose()
    {
      Json?.Return();
      Json = null;
      if (Others is null)
        return;

      foreach (var buffer in Others)
        buffer?.Return();
      Others = null;
    }
  }

  // Adds connection to the key's members, and returns whether it is the first. Called under Gate.
  static bool Join(
      ConcurrentDictionary<string, ConcurrentDictionary<WebSocketConnection, byte>> index,
      string key,
      WebSocketConnection connection)
  {
    if (index.TryGetValue(key, out var members))
    {
      members[connection] = 0;
      return false;
    }

    index[key] = new ConcurrentDictionary<WebSocketConnection, byte> { [connection] = 0 };
    return true;
  }

  // Removes connection from the key's members, and returns whether it was the last. Called under Gate.
  static bool Leave(
      ConcurrentDictionary<string, ConcurrentDictionary<WebSocketConnection, byte>> index,
      string key,
      WebSocketConnection connection) =>
    index.TryGetValue(key, out var members) && members.TryRemove(connection, out _) && members.IsEmpty && index.TryRemove(key, out _);

  static string Required(string value)
  {
    ArgumentException.ThrowIfNullOrEmpty(value);
    return value;
  }
}
