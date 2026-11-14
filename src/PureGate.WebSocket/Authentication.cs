using System.Security.Claims;

namespace PureGate.WebSocket;

/// <summary>
/// What PureGateWebSocketOptions.Authenticate decided about a browser's token.
/// </summary>
public sealed class WebSocketAuthenticationResult
{
  static readonly WebSocketAuthenticationResult Failed = new(null, null);

  WebSocketAuthenticationResult(ClaimsPrincipal user, DateTimeOffset? expires)
  {
    User = user;
    Expires = expires;
  }

  /// <summary>
  /// The token is valid: the connection is user's, until expires when it is set. Before then, the browser refreshes it
  /// with a new token for the same user.
  /// </summary>
  public static WebSocketAuthenticationResult Success(ClaimsPrincipal user, DateTimeOffset? expires = null)
  {
    ArgumentNullException.ThrowIfNull(user);
    return new WebSocketAuthenticationResult(user, expires);
  }

  /// <summary>
  /// The token is not valid. The connection closes with 4001 (unauthorized).
  /// </summary>
  public static WebSocketAuthenticationResult Fail() => Failed;

  /// <summary>
  /// Whether the token is valid.
  /// </summary>
  public bool Succeeded => User is not null;

  /// <summary>
  /// The connection's user when the token is valid, otherwise null.
  /// </summary>
  public ClaimsPrincipal User { get; }

  /// <summary>
  /// When the token expires, if it does. Without a refresh before then, the connection closes with 4001
  /// (unauthorized) and stays resumable.
  /// </summary>
  public DateTimeOffset? Expires { get; }
}

/// <summary>
/// Runs around an operation's handler, from OperationBuilder.AddFilter: before it, after it, or instead of it. Calling
/// next runs the rest: the later filters, then the handler. A filter that throws CallException fails the call with it.
/// </summary>
public delegate ValueTask WebSocketFilter(WebSocketCall call, Func<ValueTask> next);
