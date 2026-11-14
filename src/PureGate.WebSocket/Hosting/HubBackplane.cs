using System.Buffers.Text;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PureGate.WebSocket.Diagnostics;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.Transport;

namespace PureGate.WebSocket.Hosting;

// A message from another server, read up to its payload.
readonly record struct BackplaneMessage(TargetKind Kind, string Key, string Event, bool Binary, int PayloadOffset);

// The hub's side of its IWebSocketBackplane (docs/PLAN-SERVER.md, Deploys and several instances): this server's id,
// the topics it subscribes to, and the messages it publishes and reads.
//
// A message is a version byte, the sending server's id, the target's kind and key, the event's name, a flags byte,
// and the payload: JSON as the event's first mapping writes it, or its raw bytes. Each string is a varint length and
// UTF-8.
sealed class HubBackplane
{
  const byte Version = 1;
  const byte BinaryFlag = 1;
  const string AllTopic = "all";

  static readonly TimeSpan FirstRetry = TimeSpan.FromSeconds(1);
  static readonly TimeSpan LastRetry = TimeSpan.FromSeconds(30);

  readonly Lock Gate = new();
  readonly Dictionary<string, Subscription> Subscriptions = new(StringComparer.Ordinal);
  readonly byte[] ServerIdUtf8;
  readonly ILogger Logger;
  readonly Telemetry Telemetry;

  // Guarded by Gate.
  bool Started;
  CancellationToken Stopping;

  public HubBackplane(IWebSocketBackplane backend, ILogger logger, Telemetry telemetry)
  {
    Backend = backend;
    Logger = logger;
    Telemetry = telemetry;
    ServerId = Base64Url.EncodeToString(RandomNumberGenerator.GetBytes(9));
    ServerIdUtf8 = Encoding.ASCII.GetBytes(ServerId);
  }

  public IWebSocketBackplane Backend { get; }

  // New each time the server starts. This server's connection ids start with it and a dot, so a send to a connection
  // elsewhere goes to its server's topic.
  public string ServerId { get; }

  public static string UserTopic(string userId) => "user:" + userId;

  public static string GroupTopic(string group) => "group:" + group;

  static string ServerTopic(ReadOnlySpan<char> serverId) => string.Concat("server:", serverId);

  // The topic a send to target goes out on, or null when no other server has its connections: a connection found
  // here, or one whose id names no other server.
  public string TopicOf(in WebSocketTarget target, bool foundHere)
  {
    switch (target.Kind)
    {
      case TargetKind.All:
        return AllTopic;
      case TargetKind.User:
        return UserTopic(target.Key);
      case TargetKind.Group:
        return GroupTopic(target.Key);
      case TargetKind.Connection when !foundHere:
        var dot = target.Key.IndexOf('.');
        var server = dot > 0 ? target.Key.AsSpan(0, dot) : default;
        return server.IsEmpty || server.SequenceEqual(ServerId.AsSpan()) ? null : ServerTopic(server);
      default:
        return null;
    }
  }

  // The message for an event to target: its header, then the payload as first writes it, which shared is. A scratch
  // buffer, for PublishAsync to give back.
  public PooledBuffer Write<T>(in WebSocketTarget target, ServerEventInfo<T> first, T payload, out ReadOnlyMemory<byte> shared)
  {
    var codec = first.Codecs[0];
    var buffer = PooledBuffer.Scratch();
    try
    {
      WriteByte(buffer, Version);
      WriteText(buffer, ServerIdUtf8);
      WriteByte(buffer, (byte)target.Kind);
      WriteText(buffer, target.Key ?? "");
      WriteText(buffer, first.Descriptor.NameUtf8);
      WriteByte(buffer, codec.IsBinary ? BinaryFlag : (byte)0);
      var start = buffer.WrittenCount;
      codec.Write(buffer, payload);
      shared = buffer.WrittenMemory[start..];
      return buffer;
    }
    catch
    {
      buffer.ReturnScratch();
      throw;
    }
  }

  // Publishes a message from Write for a send to a target of kind, and gives its buffer back once the backplane is
  // done with it.
  public ValueTask PublishAsync(string topic, TargetKind kind, PooledBuffer message, CancellationToken cancellationToken)
  {
    var started = Telemetry.BackplanePublishDuration.Enabled ? Stopwatch.GetTimestamp() : 0;
    ValueTask publish;
    try
    {
      publish = Backend.PublishAsync(topic, message.WrittenMemory, cancellationToken);
    }
    catch (Exception exception)
    {
      message.ReturnScratch();
      Published(started, kind, succeeded: false);
      return ValueTask.FromException(exception);
    }

    if (!publish.IsCompleted)
      return AwaitPublishAsync(publish, message, started, kind);

    try
    {
      publish.GetAwaiter().GetResult();
      Published(started, kind, succeeded: true);
      return default;
    }
    catch (Exception exception)
    {
      Published(started, kind, succeeded: false);
      return ValueTask.FromException(exception);
    }
    finally
    {
      message.ReturnScratch();
    }
  }

  // Finished on another thread, so the buffer goes back to the pool rather than to this thread's scratch. Pooled, as
  // a backend that sends over the network rarely completes at once.
  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
  async ValueTask AwaitPublishAsync(ValueTask publish, PooledBuffer message, long started, TargetKind kind)
  {
    try
    {
      await publish;
      Published(started, kind, succeeded: true);
    }
    catch
    {
      Published(started, kind, succeeded: false);
      throw;
    }
    finally
    {
      message.Return();
    }
  }

  // Records a publish that started measuring, when something listened as it began.
  void Published(long started, TargetKind kind, bool succeeded)
  {
    if (started == 0)
      return;

    Telemetry.BackplanePublishDuration.Record(
      Stopwatch.GetElapsedTime(started).TotalSeconds,
      new KeyValuePair<string, object>("puregate.websocket.target", NameOf(kind)),
      new KeyValuePair<string, object>("puregate.websocket.outcome", succeeded ? "ok" : "failed"));
  }

  // What became of a message from another server: "delivered", "not_mapped" or "unreadable".
  public void Received(string outcome)
  {
    if (Telemetry.BackplaneReceived.Enabled)
      Telemetry.BackplaneReceived.Add(1, new KeyValuePair<string, object>("puregate.websocket.outcome", outcome));
  }

  static string NameOf(TargetKind kind) => kind switch
  {
    TargetKind.All => "all",
    TargetKind.User => "user",
    TargetKind.Group => "group",
    _ => "connection",
  };

  // Reads a message up to its payload. False for one this server sent. Throws for a message it cannot read.
  public bool TryRead(ReadOnlySpan<byte> message, out BackplaneMessage read)
  {
    read = default;
    if (message.IsEmpty)
      throw new InvalidDataException("The message is empty.");
    if (message[0] != Version)
      throw new InvalidDataException($"The message has version {message[0]}; this server reads version {Version}.");

    var offset = 1;
    if (ReadText(message, ref offset).SequenceEqual(ServerIdUtf8))
      return false;

    var kind = (TargetKind)message[offset++];
    if (kind is not (TargetKind.All or TargetKind.User or TargetKind.Group or TargetKind.Connection))
      throw new InvalidDataException($"The message has an unknown target kind, {(byte)kind}.");

    var key = Encoding.UTF8.GetString(ReadText(message, ref offset));
    var name = Encoding.UTF8.GetString(ReadText(message, ref offset));
    var flags = message[offset++];
    read = new BackplaneMessage(kind, key, name, (flags & BinaryFlag) != 0, offset);
    return true;
  }

  // Whether this server wants the topic's messages. The hub calls it under its own lock as connections come, go, join
  // and leave, so the last call for a topic always says what is true now.
  public void Want(string topic, bool wanted)
  {
    Subscription start;
    lock (Gate)
    {
      if (!Subscriptions.TryGetValue(topic, out var subscription))
      {
        if (!wanted)
          return;

        subscription = new Subscription(this, topic);
        Subscriptions[topic] = subscription;
      }

      subscription.Wanted = wanted;
      start = Started ? Claim(subscription) : null;
    }

    // The backplane is called neither under a lock nor on the caller's thread.
    if (start is not null)
      ThreadPool.UnsafeQueueUserWorkItem(start, preferLocal: false);
  }

  // The backplane is receiving: from now on, the subscriptions follow what this server wants.
  public void Start(CancellationToken stopping)
  {
    List<Subscription> start = [];
    lock (Gate)
    {
      Stopping = stopping;
      Started = true;
      foreach (var subscription in Subscriptions.Values)
      {
        if (Claim(subscription) is { } claimed)
          start.Add(claimed);
      }
    }

    foreach (var subscription in start)
      ThreadPool.UnsafeQueueUserWorkItem(subscription, preferLocal: false);

    Want(AllTopic, true);
    Want(ServerTopic(ServerId), true);
  }

  // The subscription, when it needs a change and no reconcile is running for it. Called under Gate.
  static Subscription Claim(Subscription subscription)
  {
    if (subscription.Busy || subscription.Wanted == subscription.Subscribed)
      return null;

    subscription.Busy = true;
    return subscription;
  }

  // Brings the backplane to what this server wants for one topic, until both agree. One runs per topic at a time, so
  // the backplane's calls for a topic never overlap. A failed call is tried again, waiting longer each time.
  async Task ReconcileAsync(Subscription subscription)
  {
    var retry = FirstRetry;
    while (true)
    {
      bool subscribe;
      CancellationToken stopping;
      lock (Gate)
      {
        stopping = Stopping;
        if (subscription.Wanted == subscription.Subscribed || stopping.IsCancellationRequested)
        {
          subscription.Busy = false;
          if (!subscription.Wanted && !subscription.Subscribed)
            Subscriptions.Remove(subscription.Topic);
          return;
        }

        subscribe = subscription.Wanted;
      }

      try
      {
        if (subscribe)
          await Backend.SubscribeAsync(subscription.Topic, stopping);
        else
          await Backend.UnsubscribeAsync(subscription.Topic, stopping);

        lock (Gate)
          subscription.Subscribed = subscribe;
        Telemetry.BackplaneSubscriptions.Add(subscribe ? 1 : -1);
        retry = FirstRetry;
      }
      catch (Exception exception) when (!stopping.IsCancellationRequested)
      {
        Telemetry.BackplaneSubscriptionFailures.Add(1, new KeyValuePair<string, object>("puregate.websocket.call", subscribe ? "subscribe" : "unsubscribe"));
        Log.BackplaneSubscriptionFailed(Logger, subscribe ? "subscribe to" : "unsubscribe from", subscription.Topic, retry, exception);
        try
        {
          await Task.Delay(retry, stopping);
        }
        catch (OperationCanceledException)
        {
        }

        retry = TimeSpan.FromTicks(Math.Min(retry.Ticks * 2, LastRetry.Ticks));
      }
      catch (Exception)
      {
        // The app is stopping: the next turn ends.
      }
    }
  }

  static void WriteByte(PooledBuffer buffer, byte value)
  {
    buffer.GetSpan(1)[0] = value;
    buffer.Advance(1);
  }

  static void WriteText(PooledBuffer buffer, ReadOnlySpan<byte> text)
  {
    var span = buffer.GetSpan(Varint.MaxLength + text.Length);
    var length = Varint.Write(span, (uint)text.Length);
    text.CopyTo(span[length..]);
    buffer.Advance(length + text.Length);
  }

  static void WriteText(PooledBuffer buffer, string text)
  {
    var count = Encoding.UTF8.GetByteCount(text);
    var span = buffer.GetSpan(Varint.MaxLength + count);
    var length = Varint.Write(span, (uint)count);
    Encoding.UTF8.GetBytes(text, span[length..]);
    buffer.Advance(length + count);
  }

  static ReadOnlySpan<byte> ReadText(ReadOnlySpan<byte> message, ref int offset)
  {
    var length = (int)Varint.Read(message, ref offset);
    if ((uint)length > (uint)(message.Length - offset))
      throw new InvalidDataException("The message is truncated.");

    var text = message.Slice(offset, length);
    offset += length;
    return text;
  }

  // One topic's subscription. Its fields are guarded by the owner's Gate.
  sealed class Subscription(HubBackplane owner, string topic) : IThreadPoolWorkItem
  {
    public string Topic { get; } = topic;

    // This server has connections for the topic.
    public bool Wanted;

    // What the backplane last agreed to.
    public bool Subscribed;

    // A reconcile is running for the topic.
    public bool Busy;

    void IThreadPoolWorkItem.Execute() => _ = owner.ReconcileAsync(this);
  }
}

// Runs the hub's backplane for the app's lifetime. Without one, it does nothing.
sealed class BackplaneService(WebSocketHub hub) : BackgroundService
{
  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    if (hub.Backplane is not { } backplane)
      return;

    var receiving = backplane.Backend.ReceiveAsync(hub.Receive, stoppingToken);
    backplane.Start(stoppingToken);
    try
    {
      await receiving;
    }
    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
    {
      return;
    }

    if (!stoppingToken.IsCancellationRequested)
      throw new InvalidOperationException("The backplane stopped receiving: events from the other servers no longer reach this one.");
  }
}
