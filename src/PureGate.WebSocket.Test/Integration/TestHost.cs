using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PureGate.WebSocket.TestServer;

namespace PureGate.WebSocket.Test.Integration;

// An in-process server with the TestServer's operations, on a free loopback port. configure and map adjust it for
// one test.
sealed class TestHost : IAsyncDisposable
{
  readonly WebApplication App;

  TestHost(WebApplication app, WebSocketEndpointBuilder endpoint, Uri uri)
  {
    App = app;
    Endpoint = endpoint;
    Uri = uri;
  }

  public WebSocketEndpointBuilder Endpoint { get; }

  public Uri Uri { get; }

  // The address without the path, for Origin headers.
  public string Origin => $"http://{Uri.Authority}";

  public IServiceProvider Services => App.Services;

  public static async Task<TestHost> StartAsync(
      Action<PureGateWebSocketOptions> configure = null,
      Action<WebSocketEndpointBuilder> map = null)
  {
    var builder = WebApplication.CreateSlimBuilder();
    builder.WebHost.UseUrls("http://127.0.0.1:0");
    builder.Logging.ClearProviders();
    builder.Services.AddPureGateWebSocket(options =>
    {
      TestApi.Configure(options);
      configure?.Invoke(options);
    });

    var app = builder.Build();
    app.UseWebSockets();
    var ws = app.MapPureGateWebSocket("/ws");
    TestApi.Map(ws);
    map?.Invoke(ws);
    await app.StartAsync();

    var address = app.Urls.First();
    return new TestHost(app, ws, new Uri($"ws{address["http".Length..]}/ws"));
  }

  public Task<TestClient> ConnectAsync(string origin = null, string subprotocol = TestClient.Subprotocol) =>
    TestClient.ConnectAsync(Uri, subprotocol, origin);

  // A client that has sent Hello and received Welcome.
  public async Task<TestClient> ConnectAndHelloAsync(string hello = TestClient.DefaultHello)
  {
    var client = await ConnectAsync();
    await client.HelloAsync(hello);
    return client;
  }

  // Stops the host as a deploy would: GoAway on every connection, then the drain.
  public Task StopAsync() => App.StopAsync();

  public async ValueTask DisposeAsync()
  {
    await App.StopAsync();
    await App.DisposeAsync();
  }
}
