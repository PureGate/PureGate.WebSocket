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
using NetWebSocket = System.Net.WebSockets.WebSocket;

namespace PureGate.WebSocket;

// One browser's session with an endpoint: what handlers and the hub talk to, and everything the session keeps.
// The other parts of this class: receiving (Receive), calls from the browser (Calls), events in both directions
// (Events), and calls to the browser (Invoke).
public sealed partial class WebSocketConnection
{
  readonly NetWebSocket Socket;
  readonly CancellationTokenSource SessionCancellation = new();
  readonly IncomingNameTable<Operation> Names;
  readonly Outbox Outbox;
  int Ended;

  internal WebSocketConnection(EndpointState endpoint, HttpContext httpContext, NetWebSocket socket)
  {
    Endpoint = endpoint;
    HttpContext = httpContext;
    Socket = socket;
    Id = NewId(9);
    User = httpContext.User;
    Groups = new WebSocketGroups(this);
    Names = new IncomingNameTable<Operation>(endpoint.ResolveName);
    Outbox = new Outbox(socket, endpoint.Options.SendQueueLimitBytes);
    Closed = SessionCancellation.Token;
    Events = endpoint.Options.EventOrdering == EventOrdering.Sequential ? new EventQueue(this) : null;
  }

  // The connection's id: unique on this server, safe to log, and sent to the browser in Welcome.
  public string Id { get; }

  public ClaimsPrincipal User { get; private set; }

  // The id hub.User(id) finds this connection by, from PureGateWebSocketOptions.UserId. Null without one.
  public string UserId { get; private set; }

  // Whatever the app wants to keep with the connection. Safe to use from several handlers at once.
  public IDictionary<object, object> Items { get; } = new ConcurrentDictionary<object, object>();

  public WebSocketGroups Groups { get; }

  // The upgrade request's context.
  public HttpContext HttpContext { get; }

  // Fires when the session ends.
  public CancellationToken Closed { get; }

  internal EndpointState Endpoint { get; }

  internal PureGateWebSocketOptions Options => Endpoint.Options;

  internal ILogger Logger => Endpoint.Logger;

  internal bool IsEnded => Volatile.Read(ref Ended) != 0;

  // Closes the connection: what is queued goes out first, then the close frame.
  public ValueTask CloseAsync(WebSocketCloseStatus status = WebSocketCloseStatus.NormalClosure, string reason = "")
  {
    Close(status, reason);
    return ValueTask.CompletedTask;
  }

  internal void Close(WebSocketCloseStatus status, string reason)
  {
    Closing = true;
    Outbox.Close(status, reason);
  }

  internal WriteResult Write(scoped in Frame frame)
  {
    var result = Outbox.Write(in frame);
    if (result == WriteResult.Overloaded)
      SlowConsumer();

    return result;
  }

  void SlowConsumer()
  {
    Diagnostics.Log.SlowConsumer(Logger, Id);
    Close((WebSocketCloseStatus)CloseCodes.SlowConsumer, "The client fell too far behind.");
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
}
