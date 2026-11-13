using System.Diagnostics.CodeAnalysis;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PureGate.WebSocket.Hosting;

namespace PureGate.WebSocket;

public static class PureGateWebSocketExtensions
{
  // Registers the hub and the options every endpoint starts from.
  public static IServiceCollection AddPureGateWebSocket(
      this IServiceCollection services,
      Action<PureGateWebSocketOptions> configure = null)
  {
    ArgumentNullException.ThrowIfNull(services);
    services.AddOptions<PureGateWebSocketOptions>();
    if (configure is not null)
      services.Configure(configure);

    services.TryAddSingleton<WebSocketHub>();
    services.TryAddSingleton<IWebSocketHub>(static provider => provider.GetRequiredService<WebSocketHub>());
    return services;
  }

  // Maps a PureGate.WebSocket endpoint. Requires app.UseWebSockets(). The route accepts any HTTP method, so
  // WebSockets over HTTP/2 (CONNECT) reach it too. configure overrides the shared options for this endpoint.
  public static WebSocketEndpointBuilder MapPureGateWebSocket(
      this IEndpointRouteBuilder endpoints,
      [StringSyntax("Route")] string pattern,
      Action<PureGateWebSocketOptions> configure = null)
  {
    ArgumentNullException.ThrowIfNull(endpoints);
    var services = endpoints.ServiceProvider;
    var hub = services.GetService<WebSocketHub>()
      ?? throw new InvalidOperationException("Call services.AddPureGateWebSocket() before mapping an endpoint.");

    var options = services.GetRequiredService<IOptions<PureGateWebSocketOptions>>().Value.Clone();
    configure?.Invoke(options);
    var endpoint = new EndpointState(
      pattern,
      options,
      hub,
      services.GetRequiredService<ILoggerFactory>(),
      services.GetRequiredService<IServiceScopeFactory>());

    // A stopping host sends GoAway on every connection, so browsers move to another instance before this one goes.
    services.GetService<IHostApplicationLifetime>()?.ApplicationStopping.Register(endpoint.GoAway);

    var inner = endpoints.Map(pattern, endpoint.HandleAsync);
    return new WebSocketEndpointBuilder(endpoint, inner, endpoints);
  }
}
