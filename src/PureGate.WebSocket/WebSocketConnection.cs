using System.Buffers.Text;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Security.Claims;
using System.Security.Cryptography;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using PureGate.WebSocket.Hosting;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.Transport;

namespace PureGate.WebSocket;

// One browser's session with an endpoint: what handlers and the hub talk to, and everything the session keeps. A
// session outlives its WebSocket: when the connection drops, it keeps running and waits for the browser to resume.
// The other parts of this class: receiving (Receive), resume, acknowledgement and shutdown (Session), calls from the
// browser (Calls), streams (Streams), events in both directions (Events), and calls to the browser (Invoke).
public sealed partial class WebSocketConnection
{
  readonly CancellationTokenSource SessionCancellation = new();
  readonly IncomingNameTable<Operation> Names;
  readonly Outbox Outbox;
  int Ended;

  internal WebSocketConnection(EndpointState endpoint, Link link)
  {
    Endpoint = endpoint;
    HttpContext = link.Context;
    User = link.Context.User;
    Id = NewId(9);
    SessionId = NewId(16);
    Groups = new WebSocketGroups(this);
    Names = new IncomingNameTable<Operation>(endpoint.ResolveName);
    var options = endpoint.Options;
    Outbox = new Outbox(options.SendQueueLimitBytes, options.ResumeWindow > TimeSpan.Zero ? options.ReplayBufferBytes : 0)
    {
      ResumeLost = ResumeLost,
    };
    Closed = SessionCancellation.Token;
    Events = options.EventOrdering == EventOrdering.Sequential ? new EventQueue(this) : null;
  }

  // The connection's id: unique on this server, safe to log, and sent to the browser in Welcome. It stays the same
  // when the browser resumes.
  public string Id { get; }

  // The user of the latest upgrade request: a resume must be by the same user.
  public ClaimsPrincipal User { get; private set; }

  // The id hub.User(id) finds this connection by, from PureGateWebSocketOptions.UserId. Null without one.
  public string UserId { get; private set; }

  // Whatever the app wants to keep with the connection. Safe to use from several handlers at once. Created the first
  // time it is read, with one lock: only the connection's handlers contend for it.
  public IDictionary<object, object> Items
  {
    get
    {
      if (Volatile.Read(ref ItemsStore) is { } items)
        return items;

      Interlocked.CompareExchange(ref ItemsStore, new ConcurrentDictionary<object, object>(1, 4), null);
      return ItemsStore;
    }
  }

  ConcurrentDictionary<object, object> ItemsStore;

  public WebSocketGroups Groups { get; }

  // The latest upgrade request's context.
  public HttpContext HttpContext { get; private set; }

  // Fires when the session ends: not when its connection drops, if the browser can still resume.
  public CancellationToken Closed { get; }

  internal string SessionId { get; }

  internal EndpointState Endpoint { get; }

  internal PureGateWebSocketOptions Options => Endpoint.Options;

  internal ILogger Logger => Endpoint.Logger;

  internal bool IsEnded => Volatile.Read(ref Ended) != 0;

  // Closes the connection for good: what is queued goes out first, then the close frame, and the session ends.
  public ValueTask CloseAsync(WebSocketCloseStatus status = WebSocketCloseStatus.NormalClosure, string reason = "")
  {
    CloseLink(status, reason, endsSession: true);
    return ValueTask.CompletedTask;
  }

  // Closes the current connection. Unless endsSession, the session stays for the browser to resume when it can;
  // discard drops what was still queued for the connection, which a resume sends again.
  internal void CloseLink(WebSocketCloseStatus status, string reason, bool endsSession, bool discard = false)
  {
    Link link;
    lock (LinkGate)
    {
      link = CurrentLink;
      if (link is not null)
        link.EndsSession |= endsSession;
    }

    if (link is not null)
      Outbox.Close(link, status, reason, discard);
    else if (endsSession)
      _ = EndAsync();
  }

  internal WriteResult Write(scoped in Frame frame)
  {
    var result = Outbox.Write(in frame);
    CheckBacklog(result);
    return result;
  }

  // After a write that cannot be dropped: a full send queue, or a page too far behind under SlowConsumerPolicy.Close,
  // closes the connection.
  void CheckBacklog(WriteResult result)
  {
    if (result == WriteResult.Overloaded || (Options.SlowConsumer == SlowConsumerPolicy.Close && IsFarBehind))
      SlowConsumer();
  }

  bool IsFarBehind => Outbox.Unacknowledged > Options.MaxUnacknowledgedFrames && Outbox.IsAttached;

  void SlowConsumer()
  {
    Diagnostics.Log.SlowConsumer(Logger, Id);
    Endpoint.Telemetry.SlowConsumers.Add(1);
    CloseLink((WebSocketCloseStatus)CloseCodes.SlowConsumer, "The client fell too far behind.", endsSession: false, discard: true);
  }

  static string NewId(int bytes) => Base64Url.EncodeToString(RandomNumberGenerator.GetBytes(bytes));
}

// The protocol's own close codes (docs/PROTOCOL.md, Closing).
static class CloseCodes
{
  public const int ProtocolError = 4000;
  public const int Unauthorized = 4001;
  public const int HandshakeTimeout = 4002;
  public const int SlowConsumer = 4003;
  public const int ContractRejected = 4004;
  public const int SessionReplaced = 4005;
  public const int Reconnecting = 4006;
  public const int FormatUnsupported = 4007;
}
