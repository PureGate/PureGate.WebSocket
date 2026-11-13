using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Security.Claims;
using System.Text.Json;
using System.Threading.RateLimiting;
using Microsoft.AspNetCore.Authentication;
using PureGate.WebSocket.Diagnostics;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.Serialization;
using PureGate.WebSocket.Transport;

namespace PureGate.WebSocket;

// Who the connection is, and what it may do (docs/PLAN-SERVER.md, Security). The user comes from the upgrade request,
// or from a token that PureGateWebSocketOptions.Authenticate validates, and can expire: a browser with a token
// refreshes it in place, and one without is closed with 4001 at expiry and comes back with fresh cookies. Operation
// policies are evaluated for the connection's user and cached until the user changes.
public sealed partial class WebSocketConnection
{
  // Timers past this are set again when they fire: Timer cannot wait much longer.
  static readonly TimeSpan LongestWait = TimeSpan.FromDays(30);

  // Each policy's outcome for one user, made when a policy is first evaluated for them. It names its user, so an
  // evaluation that started for an earlier user never lands in the current one's, and a new user starts afresh.
  AuthorizationCache Authorizations;

  // When the authentication expires; null when it doesn't. Guarded by LinkGate.
  DateTimeOffset? AuthenticationExpires;
  Timer ExpiryOfAuthentication;

  RateLimiter Limiter;

  // A permit the receive loop took for the exchange it is starting, until the exchange's call takes it.
  RateLimitLease NextLease;

  readonly record struct Identity(ClaimsPrincipal User, DateTimeOffset? Expires);

  // The user of a new connection: the token's, when the endpoint authenticates by token, or the upgrade's. Throws
  // CloseException (4001) when the token is missing, refused or already expired.
  async Task<Identity> AuthenticateAsync(Link link, string token)
  {
    var upgradeExpires = UpgradeExpires(link);
    if (Options.Authenticate is null)
      return new Identity(link.Context.User, upgradeExpires);

    if (string.IsNullOrEmpty(token))
      throw Unauthorized("A token is required.");

    var result = await ValidateAsync(link, token, Closed);
    if (result is null)
      throw Unauthorized("The token was refused.");

    return new Identity(result.User, Earlier(result.Expires, upgradeExpires));
  }

  // The token's result, or null when it is refused, fails to validate, or has expired.
  async Task<WebSocketAuthenticationResult> ValidateAsync(Link link, string token, CancellationToken cancellationToken)
  {
    WebSocketAuthenticationResult result;
    try
    {
      using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
      timeout.CancelAfter(Options.HandshakeTimeout);
      result = await Options.Authenticate(token, link.Context, timeout.Token);
    }
    catch (Exception exception)
    {
      Log.AuthenticationFailed(Logger, Id, exception);
      return null;
    }

    if (result is not { Succeeded: true } || result.Expires <= DateTimeOffset.UtcNow)
    {
      Log.TokenRefused(Logger, Id);
      return null;
    }

    return result;
  }

  // When the upgrade's own authentication expires, if its scheme says: a cookie's, for instance.
  static DateTimeOffset? UpgradeExpires(Link link) =>
    link.Context.Features.Get<IAuthenticateResultFeature>()?.AuthenticateResult?.Properties?.ExpiresUtc;

  static DateTimeOffset? Earlier(DateTimeOffset? first, DateTimeOffset? second) =>
    first is null ? second : second is null ? first : first < second ? first : second;

  static CloseException Unauthorized(string reason) => new((WebSocketCloseStatus)CloseCodes.Unauthorized, reason, endsSession: false);

  // Takes identity as the connection's. Called under LinkGate.
  void SetIdentity(Identity identity)
  {
    User = identity.User;
    AuthenticationExpires = identity.Expires;
    ScheduleAuthenticationExpiry();
  }

  void ScheduleAuthenticationExpiry()
  {
    if (AuthenticationExpires is not { } expires)
    {
      ExpiryOfAuthentication?.Change(Timeout.Infinite, Timeout.Infinite);
      return;
    }

    var due = expires - DateTimeOffset.UtcNow;
    due = due < TimeSpan.Zero ? TimeSpan.Zero : due > LongestWait ? LongestWait : due;
    ExpiryOfAuthentication ??= new Timer(static state => ((WebSocketConnection)state).AuthenticationDue(), this, Timeout.Infinite, Timeout.Infinite);
    ExpiryOfAuthentication.Change(due, Timeout.InfiniteTimeSpan);
  }

  // Milliseconds until the authentication expires, for Welcome and Authenticated; 0 when it doesn't.
  long ExpiresInMilliseconds() =>
    AuthenticationExpires is { } expires ? Math.Max(1, (long)(expires - DateTimeOffset.UtcNow).TotalMilliseconds) : 0;

  // At expiry without a refresh: 4001, and the session stays for a browser that comes back authenticated.
  void AuthenticationDue()
  {
    lock (LinkGate)
    {
      if (IsEnded || AuthenticationExpires is not { } expires)
        return;

      if (expires > DateTimeOffset.UtcNow)
      {
        ScheduleAuthenticationExpiry();
        return;
      }
    }

    Log.AuthenticationExpired(Logger, Id);
    CloseLink((WebSocketCloseStatus)CloseCodes.Unauthorized, "The authentication expired.", endsSession: false);
  }

  // The browser's Authenticate: a fresh token for the same user. Frames keep flowing with the current identity until
  // it is validated; then Authenticated answers with the new expiry. Any failure closes with 4001.
  void OnAuthenticate(scoped in Frame frame)
  {
    if (Options.Authenticate is null)
      ProtocolException.Throw(ProtocolViolation.UnexpectedFrame);

    AuthenticateMessage message = null;
    try
    {
      message = JsonSerializer.Deserialize(frame.Payload, HandshakeJson.Default.AuthenticateMessage);
    }
    catch (JsonException)
    {
    }

    if (string.IsNullOrEmpty(message?.Token))
      ProtocolException.Throw(ProtocolViolation.InvalidHandshake);

    _ = RefreshAsync(CurrentLink, message.Token);
  }

  async Task RefreshAsync(Link link, string token)
  {
    var result = await ValidateAsync(link, token, Closed);
    lock (LinkGate)
    {
      if (IsEnded)
        return;

      if (result is not null && string.Equals(Options.UserId(result.User), UserId, StringComparison.Ordinal))
      {
        SetIdentity(new Identity(result.User, Earlier(result.Expires, UpgradeExpires(link))));
        var buffer = new PooledBuffer(64);
        try
        {
          using (var writer = new Utf8JsonWriter(buffer))
            JsonSerializer.Serialize(writer, new AuthenticatedMessage { Expires = ExpiresInMilliseconds() }, HandshakeJson.Default.AuthenticatedMessage);

          var frame = new Frame { Kind = FrameKind.Authenticated, Payload = buffer.WrittenSpan };
          Outbox.Write(in frame);
        }
        finally
        {
          buffer.Return();
        }

        return;
      }
    }

    CloseLink((WebSocketCloseStatus)CloseCodes.Unauthorized, "The refreshed token was refused.", endsSession: false);
  }

  // Null when the connection's user passes every policy; otherwise the reason it does not, as the call's error.
  internal async ValueTask<CallException> AuthorizeAsync(string[] policies)
  {
    var user = User;
    var cache = Volatile.Read(ref Authorizations);
    if (cache?.User != user)
    {
      // Two calls that race here each make one, and one of them is kept: outcomes are only ever evaluated again.
      cache = new AuthorizationCache(user);
      Volatile.Write(ref Authorizations, cache);
    }

    foreach (var policy in policies)
    {
      if (!cache.Outcomes.TryGetValue(policy, out var allowed))
      {
        allowed = await Endpoint.AuthorizeAsync(user, policy);
        cache.Outcomes[policy] = allowed;
      }

      if (!allowed)
      {
        return user?.Identity?.IsAuthenticated == true
          ? new CallException("forbidden", "The operation's authorization policy refused the call.")
          : new CallException("unauthorized", "The operation needs an authenticated user.");
      }
    }

    return null;
  }

  internal void EventRefused(string name) => Log.EventDropped(Logger, Id, name, "Its authorization policy refused it.");

  // Rate limiting: a permit for every exchange the browser starts, taken on the receive loop.

  void StartLimiter() => Limiter = Options.RateLimiter?.Invoke(this);

  // False when the connection is over its limit. Otherwise the permit waits in NextLease for the exchange's call.
  bool TryAcquire()
  {
    if (Limiter is null)
      return true;

    var lease = Limiter.AttemptAcquire();
    if (!lease.IsAcquired)
    {
      lease.Dispose();
      Log.RateLimited(Logger, Id);
      return false;
    }

    NextLease = lease;
    return true;
  }

  // The permit for the exchange being started, if the receive loop took one. Called on the receive loop.
  internal RateLimitLease TakeLease()
  {
    var lease = NextLease;
    NextLease = null;
    return lease;
  }

  // An exchange that did not start lets its permit go.
  void ReleaseUnusedLease() => TakeLease()?.Dispose();
}

sealed class AuthorizationCache(ClaimsPrincipal user)
{
  public ClaimsPrincipal User { get; } = user;

  // One lock: only the connection's handlers contend for it.
  public ConcurrentDictionary<string, bool> Outcomes { get; } = new(1, 4, StringComparer.Ordinal);
}
