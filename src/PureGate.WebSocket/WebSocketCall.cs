using System.Security.Claims;
using Microsoft.Extensions.DependencyInjection;

namespace PureGate.WebSocket;

/// <summary>
/// One call or event a handler is answering. Valid only until the handler finishes, like HttpContext.
/// </summary>
public sealed class WebSocketCall : IAsyncDisposable
{
  readonly IServiceScopeFactory ScopeFactory;
  AsyncServiceScope Scope;
  bool HasScope;

  internal WebSocketCall(
      WebSocketConnection connection,
      string name,
      uint id,
      string traceParent,
      IServiceScopeFactory scopeFactory)
  {
    Connection = connection;
    Name = name;
    Id = id;
    TraceParent = traceParent;
    ScopeFactory = scopeFactory;
    Cancellation = connection.RentCancellation();
    CancellationToken = Cancellation.Token;
  }

  /// <summary>
  /// The connection the call came on.
  /// </summary>
  public WebSocketConnection Connection { get; }

  /// <summary>
  /// The connection's user.
  /// </summary>
  public ClaimsPrincipal User => Connection.User;

  /// <summary>
  /// The operation's or event's full name, such as "orders.get".
  /// </summary>
  public string Name { get; }

  /// <summary>
  /// The browser's W3C traceparent, when it sent one.
  /// </summary>
  public string TraceParent { get; }

  /// <summary>
  /// Services for this call: a DI scope created the first time it is read, and disposed when the handler finishes.
  /// </summary>
  public IServiceProvider Services
  {
    get
    {
      if (!HasScope)
      {
        Scope = ScopeFactory.CreateAsyncScope();
        HasScope = true;
      }

      return Scope.ServiceProvider;
    }
  }

  /// <summary>
  /// Fires when the browser cancels the call, its deadline passes, or the session ends. Valid only until the handler
  /// finishes: the server reuses it for a later call, as ASP.NET Core reuses RequestAborted.
  /// </summary>
  public CancellationToken CancellationToken { get; }

  internal uint Id { get; }

  // The connection's, until the call is disposed and gives it back.
  internal CancellationTokenSource Cancellation { get; private set; }

  internal bool CancelledByClient { get; private set; }

  // When the call began, for its duration, and its trace span when something listens.
  internal long Started { get; } = System.Diagnostics.Stopwatch.GetTimestamp();

  internal System.Diagnostics.Activity Activity { get; set; }

  // The rate limiter's permit, held until the handler finishes.
  internal System.Threading.RateLimiting.RateLimitLease Lease { get; set; }

  // Lets the permit go before the exchange's last frame is written, as the window does, so the browser's next
  // call never meets a limit the server still counts as taken.
  internal void ReleaseLease()
  {
    Lease?.Dispose();
    Lease = null;
  }

  // Environment.TickCount64 when the deadline passes; 0 without one.
  internal long Deadline { get; private set; }

  // Asked after ruling out the browser's Cancel and the session's end, which leaves the deadline as the only
  // thing that cancels the token. The timer can fire before TickCount64, which moves in steps of up to 16 ms,
  // reaches the deadline, so the token is checked as well as the clock.
  internal bool DeadlinePassed =>
    Deadline != 0 && (Cancellation is { IsCancellationRequested: true } || Environment.TickCount64 >= Deadline);

  internal void SetDeadline(uint milliseconds)
  {
    Deadline = Environment.TickCount64 + milliseconds;
    Cancellation.CancelAfter(TimeSpan.FromMilliseconds(milliseconds));
  }

  // Called under the connection's CallsGate while the call is still active, so its source is not yet given back
  // for another call to reuse. The callbacks run on the thread pool, not under the lock. A handler that disposed
  // its own call already gave the source back.
  internal void CancelByClient()
  {
    CancelledByClient = true;
    try
    {
      _ = Cancellation?.CancelAsync();
    }
    catch (ObjectDisposedException)
    {
    }
  }

  /// <summary>
  /// Ends the call: disposes its service scope and gives back what it holds. The server calls it when the handler
  /// finishes.
  /// </summary>
  public async ValueTask DisposeAsync()
  {
    if (HasScope)
    {
      HasScope = false;
      await Scope.DisposeAsync();
    }

    ReleaseLease();
    if (Cancellation is { } cancellation)
    {
      Cancellation = null;
      Connection.ReturnCancellation(cancellation);
    }
  }
}
