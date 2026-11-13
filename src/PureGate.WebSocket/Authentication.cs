using System.Security.Claims;

namespace PureGate.WebSocket;

// What PureGateWebSocketOptions.Authenticate decided about a browser's token.
public sealed class WebSocketAuthenticationResult
{
  static readonly WebSocketAuthenticationResult Failed = new(null, null);

  WebSocketAuthenticationResult(ClaimsPrincipal user, DateTimeOffset? expires)
  {
    User = user;
    Expires = expires;
  }

  // The token is valid: the connection is user's, until expires when it is set. Before then, the browser refreshes it
  // with a new token for the same user.
  public static WebSocketAuthenticationResult Success(ClaimsPrincipal user, DateTimeOffset? expires = null)
  {
    ArgumentNullException.ThrowIfNull(user);
    return new WebSocketAuthenticationResult(user, expires);
  }

  // The token is not valid. The connection closes with 4001 (unauthorized).
  public static WebSocketAuthenticationResult Fail() => Failed;

  public bool Succeeded => User is not null;

  public ClaimsPrincipal User { get; }

  public DateTimeOffset? Expires { get; }
}

// Runs around an operation's handler, from OperationBuilder.AddFilter: before it, after it, or instead of it. Calling
// next runs the rest: the later filters, then the handler. A filter that throws CallException fails the call with it.
public delegate ValueTask WebSocketFilter(WebSocketCall call, Func<ValueTask> next);
