using System.Net.WebSockets;
using System.Security.Claims;
using System.Threading.RateLimiting;
using Microsoft.Extensions.DependencyInjection;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Test.Integration;

public sealed class SecurityTests
{
  static readonly TimeSpan Quiet = TimeSpan.FromMilliseconds(300);

  // Tokens are "user" or "user:role", with "+500" for one that expires in 500 ms; "bad" fails and "throw" throws.
  static ValueTask<WebSocketAuthenticationResult> Authenticate(string token, Microsoft.AspNetCore.Http.HttpContext context, CancellationToken cancellationToken)
  {
    if (token == "bad")
      return ValueTask.FromResult(WebSocketAuthenticationResult.Fail());
    if (token == "throw")
      throw new InvalidOperationException("The token service is down.");

    var expiresIn = token.Contains('+') ? int.Parse(token[(token.IndexOf('+') + 1)..]) : (int?)null;
    var name = token.Split('+')[0];
    var parts = name.Split(':');
    var claims = new List<Claim> { new(ClaimTypes.NameIdentifier, parts[0]) };
    if (parts.Length > 1)
      claims.Add(new Claim(ClaimTypes.Role, parts[1]));

    var user = new ClaimsPrincipal(new ClaimsIdentity(claims, "token"));
    DateTimeOffset? expires = expiresIn is { } milliseconds ? DateTimeOffset.UtcNow.AddMilliseconds(milliseconds) : null;
    return ValueTask.FromResult(WebSocketAuthenticationResult.Success(user, expires));
  }

  static string Hello(string token, string resume = null) =>
    resume is null
      ? $$"""{"calls":16,"maxMessage":1048576,"token":"{{token}}"}"""
      : $$"""{"calls":16,"maxMessage":1048576,"token":"{{token}}","resume":{"session":"{{resume}}","received":0} }""";

  static Outgoing Refresh(string json) => new() { Kind = FrameKind.Authenticate, Json = json };

  static Task<TestHost> TokenHostAsync(Action<WebSocketEndpointBuilder> map = null) =>
    TestHost.StartAsync(
      configure: static options => options.Authenticate = Authenticate,
      map: ws =>
      {
        ws.MapRequest<string>("whoami", static (call, _) => ValueTask.FromResult(call.Connection.UserId ?? ""));
        MapSecured(ws);
        map?.Invoke(ws);
      },
      services: static services => services.AddAuthorization(static options =>
        options.AddPolicy("admin", static policy => policy.RequireRole("admin"))));

  static void MapSecured(WebSocketEndpointBuilder ws)
  {
    var secure = ws.MapGroup("secure");
    secure.MapRequest<string>("member", static (call, _) => ValueTask.FromResult("member")).RequireAuthorization();
    secure.MapRequest<string>("admin", static (call, _) => ValueTask.FromResult("admin")).RequireAuthorization("admin");
  }

  // Authentication by token.

  [Test]
  public async Task AuthenticatesByToken()
  {
    await using var host = await TokenHostAsync();
    await using var client = await host.ConnectAsync();
    await client.HelloAsync(Hello("alice"));

    Assert.That((await client.CallAsync(1, "whoami")).Text, Is.EqualTo("\"alice\""));
    Assert.That(client.Welcome.TryGetProperty("expires", out _), Is.False);
  }

  [TestCase("""{"calls":16,"maxMessage":1048576}""")]
  [TestCase("""{"calls":16,"maxMessage":1048576,"token":"bad"}""")]
  [TestCase("""{"calls":16,"maxMessage":1048576,"token":"throw"}""")]
  [TestCase("""{"calls":16,"maxMessage":1048576,"token":"alice+-10"}""")]
  public async Task RefusesAHelloWithoutAValidToken(string hello)
  {
    await using var host = await TokenHostAsync();
    await using var client = await host.ConnectAsync();
    await client.SendAsync(Outgoing.Hello(hello));

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4001));
  }

  [Test]
  public async Task ClosesWithUnauthorizedAtExpiryAndLetsTheBrowserResume()
  {
    await using var host = await TokenHostAsync();
    var first = await host.ConnectAsync();
    await first.HelloAsync(Hello("alice+300"));
    Assert.That(first.Welcome.GetProperty("expires").GetInt64(), Is.InRange(1, 300));

    Assert.That(await first.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4001));
    await first.DisposeAsync();

    await using var second = await host.ConnectAsync();
    await second.HelloAsync(Hello("alice", first.Session));
    Assert.That(second.Welcome.GetProperty("resumed").GetBoolean(), Is.True);
  }

  [Test]
  public async Task RefreshesInPlace()
  {
    await using var host = await TokenHostAsync();
    await using var client = await host.ConnectAsync();
    await client.HelloAsync(Hello("alice+500"));

    await client.SendAsync(Refresh("""{"token":"alice+60000"}"""));
    var authenticated = await client.ReceiveAsync(FrameKind.Authenticated, 0);
    Assert.That(authenticated.Json().GetProperty("expires").GetInt64(), Is.InRange(50_000, 60_000));

    await Task.Delay(700);
    Assert.That((await client.CallAsync(1, "whoami")).Text, Is.EqualTo("\"alice\""));
  }

  [TestCase("""{"token":"bob"}""")]
  [TestCase("""{"token":"bad"}""")]
  public async Task ClosesWhenARefreshIsRefused(string refresh)
  {
    await using var host = await TokenHostAsync();
    await using var client = await host.ConnectAsync();
    await client.HelloAsync(Hello("alice"));

    await client.SendAsync(Refresh(refresh));
    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4001));
  }

  [TestCase("""{"token":"alice"}""", false)]
  [TestCase("""{"token":""}""", true)]
  [TestCase("not json", true)]
  public async Task ClosesWithAProtocolErrorForAnAuthenticateItCannotTake(string refresh, bool tokenMode)
  {
    await using var host = tokenMode ? await TokenHostAsync() : await TestHost.StartAsync();
    await using var client = await host.ConnectAsync();
    await client.HelloAsync(tokenMode ? Hello("alice") : TestClient.DefaultHello);

    await client.SendAsync(Refresh(refresh));
    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4000));
  }

  [Test]
  public async Task ResumesOnlyForTheSessionsUser()
  {
    await using var host = await TokenHostAsync();
    var first = await host.ConnectAsync();
    await first.HelloAsync(Hello("alice"));
    var session = first.Session;
    await first.DisposeAsync();

    await using var other = await host.ConnectAsync();
    await other.HelloAsync(Hello("bob", session));
    Assert.That(other.Welcome.GetProperty("resumed").GetBoolean(), Is.False);
    Assert.That((await other.CallAsync(1, "whoami")).Text, Is.EqualTo("\"bob\""));

    // Bob's attempt did not end Alice's session.
    await using var owner = await host.ConnectAsync();
    await owner.HelloAsync(Hello("alice", session));
    Assert.That(owner.Welcome.GetProperty("resumed").GetBoolean(), Is.True);
  }

  // Authorization per operation.

  [Test]
  public async Task AuthorizesEachOperation()
  {
    await using var host = await TokenHostAsync();
    await using var member = await host.ConnectAsync();
    await member.HelloAsync(Hello("alice"));
    await using var admin = await host.ConnectAsync();
    await admin.HelloAsync(Hello("root:admin"));

    Assert.That((await member.CallAsync(1, "secure.member")).Text, Is.EqualTo("\"member\""));
    Assert.That((await member.CallAsync(2, "secure.admin")).Error, Is.EqualTo("forbidden"));
    Assert.That((await admin.CallAsync(1, "secure.admin")).Text, Is.EqualTo("\"admin\""));
  }

  [Test]
  public async Task AnswersUnauthorizedForAnAnonymousUser()
  {
    await using var host = await TestHost.StartAsync(map: MapSecured, services: static services => services.AddAuthorization());
    await using var client = await host.ConnectAndHelloAsync();

    Assert.That((await client.CallAsync(1, "secure.member")).Error, Is.EqualTo("unauthorized"));
  }

  [Test]
  public async Task AuthorizesTheRefreshedUserAgain()
  {
    await using var host = await TokenHostAsync();
    await using var client = await host.ConnectAsync();
    await client.HelloAsync(Hello("alice"));
    Assert.That((await client.CallAsync(1, "secure.admin")).Error, Is.EqualTo("forbidden"));

    await client.SendAsync(Refresh("""{"token":"alice:admin"}"""));
    await client.ReceiveAsync(FrameKind.Authenticated, 0);

    Assert.That((await client.CallAsync(2, "secure.admin")).Text, Is.EqualTo("\"admin\""));
  }

  [Test]
  public async Task RefusesStreamsUploadsAndEventsTheirPoliciesRefuse()
  {
    var handled = 0;
    await using var host = await TokenHostAsync(ws =>
    {
      ws.MapStream<int>("secure.feed", static (_, _) => Numbers()).RequireAuthorization("admin");
      ws.MapUpload<int, int>("secure.put", static async (items, _, _) =>
      {
        await foreach (var item in items)
          return item;
        return 0;
      }).RequireAuthorization("admin");
      ws.MapClientEvent<int>("secure.ping", (_, _, _) =>
      {
        Interlocked.Increment(ref handled);
        return ValueTask.CompletedTask;
      }).RequireAuthorization("admin");
    });

    await using var client = await host.ConnectAsync();
    await client.HelloAsync(Hello("alice"));
    await client.SendAsync(
      Outgoing.Download(1, "secure.feed", credit: 4),
      Outgoing.Upload(2, "secure.put"),
      Outgoing.Event("secure.ping", "1"));

    Assert.That((await client.ReceiveAsync(FrameKind.StreamEnd, 1)).Error, Is.EqualTo("forbidden"));
    Assert.That((await client.ReceiveAsync(FrameKind.Response, 2)).Error, Is.EqualTo("forbidden"));
    await client.SendAsync(Outgoing.End(2));
    Assert.That(await client.ReceivesWithinAsync(Quiet), Is.False);
    Assert.That(handled, Is.Zero);
  }

  static async IAsyncEnumerable<int> Numbers()
  {
    await Task.Yield();
    yield return 1;
  }

  // Filters.

  [Test]
  public async Task RunsFiltersAroundTheHandlerInOrder()
  {
    var order = new List<string>();
    await using var host = await TestHost.StartAsync(map: ws =>
      ws.MapRequest<string, string>("test.filtered", (text, _, _) =>
        {
          lock (order)
            order.Add("handler");
          return ValueTask.FromResult(text);
        })
        .AddFilter(async (call, next) =>
        {
          lock (order)
            order.Add($"outer {call.Name}");
          await next();
          lock (order)
            order.Add("outer after");
        })
        .AddFilter(async (_, next) =>
        {
          lock (order)
            order.Add("inner");
          await next();
        }));

    await using var client = await host.ConnectAndHelloAsync();
    Assert.That((await client.CallAsync(1, "test.filtered", "\"x\"")).Text, Is.EqualTo("\"x\""));
    Assert.That(order, Is.EqualTo(new[] { "outer test.filtered", "inner", "handler", "outer after" }));
  }

  [Test]
  public async Task LetsAFilterRefuseACall()
  {
    var ran = false;
    await using var host = await TestHost.StartAsync(map: ws =>
      ws.MapRequest("test.blocked", (_, _) =>
        {
          ran = true;
          return ValueTask.CompletedTask;
        })
        .AddFilter(static (_, _) => throw new CallException("blocked", "Not today.")));

    await using var client = await host.ConnectAndHelloAsync();
    var response = await client.CallAsync(1, "test.blocked");

    Assert.That((response.Error, response.ErrorMessage), Is.EqualTo(("blocked", "Not today.")));
    Assert.That(ran, Is.False);
  }

  // Rate limiting.

  [Test]
  public async Task AnswersOverloadedPastTheRateLimit()
  {
    await using var host = await TestHost.StartAsync(configure: static options => options.RateLimiter = static _ =>
      new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions { PermitLimit = 3, Window = TimeSpan.FromHours(1) }));
    await using var client = await host.ConnectAndHelloAsync();

    for (uint id = 1; id <= 3; id++)
      Assert.That((await client.CallAsync(id, "misc.noop")).Has(FrameFlags.HasError), Is.False);

    Assert.That((await client.CallAsync(4, "misc.noop")).Error, Is.EqualTo("overloaded"));
    await client.SendAsync(Outgoing.Download(5, "feed.count", credit: 1, "1"));
    Assert.That((await client.ReceiveAsync(FrameKind.StreamEnd, 5)).Error, Is.EqualTo("overloaded"));
  }

  [Test]
  public async Task HoldsTheRateLimitersPermitUntilTheHandlerFinishes()
  {
    await using var host = await TestHost.StartAsync(configure: static options => options.RateLimiter = static _ =>
      new ConcurrencyLimiter(new ConcurrencyLimiterOptions { PermitLimit = 1 }));
    await using var client = await host.ConnectAndHelloAsync();

    await client.SendAsync(Outgoing.Request(1, "calls.delay", "200"));
    Assert.That((await client.CallAsync(2, "misc.noop")).Error, Is.EqualTo("overloaded"));
    await client.ReceiveAsync(FrameKind.Response, 1);
    Assert.That((await client.CallAsync(3, "misc.noop")).Has(FrameFlags.HasError), Is.False);
  }

  [Test]
  public async Task DropsEventsPastTheRateLimit()
  {
    var handled = 0;
    await using var host = await TestHost.StartAsync(
      configure: static options => options.RateLimiter = static _ =>
        new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions { PermitLimit = 2, Window = TimeSpan.FromHours(1) }),
      map: ws => ws.MapClientEvent<int>("test.count", (_, _, _) =>
      {
        Interlocked.Increment(ref handled);
        return ValueTask.CompletedTask;
      }));
    await using var client = await host.ConnectAndHelloAsync();

    await client.SendAsync(Outgoing.Event("test.count", "1"), Outgoing.Event("test.count", "2"), Outgoing.Event("test.count", "3"));
    await Task.Delay(200);

    Assert.That(Volatile.Read(ref handled), Is.EqualTo(2));
  }
}
