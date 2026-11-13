using System.Security.Claims;
using Microsoft.Extensions.DependencyInjection;

namespace PureGate.WebSocket;

// One call or event a handler is answering. Valid only until the handler finishes, like HttpContext.
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
      IServiceScopeFactory scopeFactory,
      CancellationToken sessionToken)
  {
    Connection = connection;
    Name = name;
    Id = id;
    TraceParent = traceParent;
    ScopeFactory = scopeFactory;
    Cancellation = CancellationTokenSource.CreateLinkedTokenSource(sessionToken);
  }

  public WebSocketConnection Connection { get; }

  public ClaimsPrincipal User => Connection.User;

  // The operation's or event's full name, such as "orders.get".
  public string Name { get; }

  // The browser's W3C traceparent, when it sent one.
  public string TraceParent { get; }

  // Services for this call: a DI scope created the first time it is read, and disposed when the handler finishes.
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

  // Fires when the browser cancels the call, its deadline passes, or the session ends.
  public CancellationToken CancellationToken => Cancellation.Token;

  internal uint Id { get; }

  internal CancellationTokenSource Cancellation { get; }

  internal bool CancelledByClient { get; private set; }

  // Environment.TickCount64 when the deadline passes; 0 without one.
  internal long Deadline { get; private set; }

  // Asked after ruling out the browser's Cancel and the session's end, which leaves the deadline as the only
  // thing that cancels the token. The timer can fire before TickCount64, which moves in steps of up to 16 ms,
  // reaches the deadline, so the token is checked as well as the clock.
  internal bool DeadlinePassed =>
    Deadline != 0 && (Cancellation.IsCancellationRequested || Environment.TickCount64 >= Deadline);

  internal void SetDeadline(uint milliseconds)
  {
    Deadline = Environment.TickCount64 + milliseconds;
    Cancellation.CancelAfter(TimeSpan.FromMilliseconds(milliseconds));
  }

  internal void CancelByClient()
  {
    CancelledByClient = true;
    _ = Cancellation.CancelAsync();
  }

  public async ValueTask DisposeAsync()
  {
    if (HasScope)
    {
      HasScope = false;
      await Scope.DisposeAsync();
    }

    Cancellation.Dispose();
  }
}
