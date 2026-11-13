using PureGate.WebSocket;
using PureGate.WebSocket.TestServer;

// Started by the TypeScript tests with --urls http://127.0.0.1:0. It prints its address once it listens.
var builder = WebApplication.CreateSlimBuilder(args);
builder.Services.AddPureGateWebSocket(TestApi.Configure);

var app = builder.Build();
app.UseWebSockets();
TestApi.Map(app.MapPureGateWebSocket("/ws"));
app.Lifetime.ApplicationStarted.Register(() => Console.WriteLine($"PUREGATE_TEST_SERVER {string.Join(' ', app.Urls)}"));
app.Run();
