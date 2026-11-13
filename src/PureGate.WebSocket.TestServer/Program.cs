using PureGate.WebSocket;
using PureGate.WebSocket.TestServer;

// Started by the TypeScript tests with --urls http://127.0.0.1:0. It prints its address once it listens.
var builder = WebApplication.CreateSlimBuilder(args);
builder.Services.AddPureGateWebSocket(TestApi.Configure);
builder.Services.AddAuthorization(static options => options.AddPolicy("admin", static policy => policy.RequireRole("admin")));

var app = builder.Build();
app.UseWebSockets();
TestApi.Map(app.MapPureGateWebSocket("/ws"));

// The same server for apps that authenticate by token (TestApi.AuthenticateToken).
TestApi.MapSecure(app.MapPureGateWebSocket("/ws-token", static options => options.Authenticate = TestApi.AuthenticateToken));

app.Lifetime.ApplicationStarted.Register(() => Console.WriteLine($"PUREGATE_TEST_SERVER {string.Join(' ', app.Urls)}"));
app.Run();
