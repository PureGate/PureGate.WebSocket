using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Net.WebSockets;
using System.Security.Claims;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PureGate.WebSocket.Diagnostics;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Hosting;

// One mapped endpoint: its options, everything mapped on it, and the request handler that accepts its
// connections. Mapping happens at startup; the first connection freezes it.
sealed class EndpointState
{
  public const string Subprotocol = "puregate.v1";

  readonly Dictionary<string, Operation> Operations = new(StringComparer.Ordinal);
  readonly Dictionary<object, object> EventInfos = new(ReferenceEqualityComparer.Instance);
  readonly Dictionary<object, object> ClientRequestInfos = new(ReferenceEqualityComparer.Instance);
  readonly Lock Gate = new();
  FrozenDictionary<string, Operation>.AlternateLookup<ReadOnlySpan<char>> Lookup;
  volatile bool Started;

  readonly Lazy<WebSocketContract> LazyContract;

  public EndpointState(
      string route,
      PureGateWebSocketOptions options,
      WebSocketHub hub,
      ILoggerFactory loggers,
      IServiceProvider services)
  {
    options.Validate();
    UseResolver(options.SerializerOptions);
    options.SerializerOptions.MakeReadOnly();

    Route = route;
    Options = options;
    Hub = hub;
    Logger = loggers.CreateLogger("PureGate.WebSocket");
    Services = services;
    Scopes = services.GetRequiredService<IServiceScopeFactory>();
    ResolveName = Resolve;
    LazyContract = new Lazy<WebSocketContract>(BuildContract);
    hub.AddEndpoint(this);
  }

  public string Route { get; }

  // Built the first time it is needed. Building it freezes the mapping, like the first connection does.
  public WebSocketContract Contract => LazyContract.Value;

  public PureGateWebSocketOptions Options { get; }

  public WebSocketHub Hub { get; }

  public ILogger Logger { get; }

  public IServiceScopeFactory Scopes { get; }

  // The app's root services.
  public IServiceProvider Services { get; }

  // Whether user passes policy, or the default policy for "". Operation policies need services.AddAuthorization().
  public async ValueTask<bool> AuthorizeAsync(ClaimsPrincipal user, string policy)
  {
    var policies = Services.GetService<IAuthorizationPolicyProvider>()
      ?? throw new InvalidOperationException("Operations that RequireAuthorization need services.AddAuthorization().");
    var resolved = policy.Length == 0 ? await policies.GetDefaultPolicyAsync() : await policies.GetPolicyAsync(policy);
    if (resolved is null)
      throw new InvalidOperationException($"No authorization policy is named '{policy}'.");

    var result = await Services.GetRequiredService<IAuthorizationService>().AuthorizeAsync(user ?? new ClaimsPrincipal(), resource: null, resolved);
    return result.Succeeded;
  }

  // The incoming name tables' resolver: one delegate for every connection.
  public NameResolver<Operation> ResolveName { get; }

  public List<Func<WebSocketConnection, CancellationToken, ValueTask>> Connected { get; } = [];

  public List<Func<WebSocketConnection, CancellationToken, ValueTask>> Disconnected { get; } = [];

  public void Add(Operation operation)
  {
    lock (Gate)
    {
      EnsureNotStarted();
      if (!Operations.TryAdd(operation.Name, operation))
        throw new InvalidOperationException($"'{operation.Name}' is mapped twice on this endpoint.");
    }
  }

  public void AddEvent<T>(ServerEvent<T> descriptor)
  {
    var info = new ServerEventInfo<T>(descriptor, Serialization.PayloadCodecs.Create<T>(Options.SerializerOptions));
    lock (Gate)
    {
      EnsureNotStarted();
      if (!EventInfos.TryAdd(descriptor, info))
        throw new InvalidOperationException($"The event '{descriptor.Name}' is mapped twice on this endpoint.");
    }
  }

  public void AddClientRequest<TRequest, TResponse>(ClientRequest<TRequest, TResponse> descriptor)
  {
    var info = new ClientRequestInfo<TRequest, TResponse>(
      descriptor,
      Serialization.PayloadCodecs.Create<TRequest>(Options.SerializerOptions),
      Serialization.PayloadCodecs.Create<TResponse>(Options.SerializerOptions));
    lock (Gate)
    {
      EnsureNotStarted();
      if (!ClientRequestInfos.TryAdd(descriptor, info))
        throw new InvalidOperationException($"The browser call '{descriptor.Name}' is mapped twice on this endpoint.");
    }
  }

  public bool TryGetEvent<T>(ServerEvent<T> descriptor, out ServerEventInfo<T> info)
  {
    var found = EventInfos.TryGetValue(descriptor, out var value);
    info = found ? (ServerEventInfo<T>)value : null;
    return found;
  }

  public ClientRequestInfo<TRequest, TResponse> GetClientRequest<TRequest, TResponse>(
      ClientRequest<TRequest, TResponse> descriptor) =>
    ClientRequestInfos.TryGetValue(descriptor, out var info)
      ? (ClientRequestInfo<TRequest, TResponse>)info
      : throw new InvalidOperationException($"The browser call '{descriptor.Name}' is not mapped on this endpoint: call MapClientRequest first.");

  // The upgrade request: checks it, accepts the WebSocket and runs the connection until it ends.
  public async Task HandleAsync(HttpContext context)
  {
    if (context.Features.Get<IHttpWebSocketFeature>() is null)
    {
      Log.WebSocketsNotEnabled(Logger);
      context.Response.StatusCode = StatusCodes.Status500InternalServerError;
      return;
    }

    if (!context.WebSockets.IsWebSocketRequest)
    {
      context.Response.StatusCode = StatusCodes.Status400BadRequest;
      await context.Response.WriteAsync("This endpoint only accepts WebSocket connections.");
      return;
    }

    if (!context.WebSockets.WebSocketRequestedProtocols.Contains(Subprotocol))
    {
      context.Response.StatusCode = StatusCodes.Status400BadRequest;
      await context.Response.WriteAsync($"Offer the WebSocket subprotocol {Subprotocol}.");
      return;
    }

    if (!IsOriginAllowed(context))
    {
      Log.OriginRefused(Logger, context.Request.Headers.Origin.ToString());
      context.Response.StatusCode = StatusCodes.Status403Forbidden;
      return;
    }

    Start();
    using var socket = await context.WebSockets.AcceptWebSocketAsync(new WebSocketAcceptContext
    {
      SubProtocol = Subprotocol,
      KeepAliveInterval = Options.KeepAliveInterval,
      KeepAliveTimeout = Options.KeepAliveTimeout,
      DangerousEnableCompression = Options.EnableCompression,
    });

    await WebSocketConnection.AcceptAsync(this, new Transport.Link(socket, context));
  }

  // Every session of this endpoint by its id, connected or waiting for a resume.
  public ConcurrentDictionary<string, WebSocketConnection> Sessions { get; } = new(StringComparer.Ordinal);

  // The host is stopping: every session gets GoAway and drains.
  public void GoAway()
  {
    foreach (var session in Sessions.Values)
      session.GoAway();
  }

  // The contract over HTTP, for MapContract. The hash is the ETag, so an unchanged contract costs a 304.
  public async Task WriteContractAsync(HttpContext context)
  {
    var contract = Contract;
    var etag = $"\"{contract.Hash}\"";
    context.Response.Headers.ETag = etag;
    if (context.Request.Headers.IfNoneMatch.Contains(etag))
    {
      context.Response.StatusCode = StatusCodes.Status304NotModified;
      return;
    }

    context.Response.ContentType = "text/plain; charset=utf-8";
    await context.Response.WriteAsync(contract.TypeScript);
  }

  WebSocketContract BuildContract()
  {
    Start();
    var members = new List<Contracts.ContractMember>();
    foreach (var operation in Operations.Values)
    {
      if (operation.Kind == OperationKind.Request)
        members.Add(new(Section: "api", operation.Name, operation.RequestType, operation.ResponseType));
      else if (operation.Kind == OperationKind.Stream)
        members.Add(new(Section: "streams", operation.Name, operation.RequestType, operation.ResponseType));
      else if (operation is IUploadOperation upload)
        members.Add(new(Section: "uploads", operation.Name, operation.RequestType, operation.ResponseType, upload.ItemType));
      else if (operation.Kind == OperationKind.ClientEvent)
        members.Add(new(Section: "clientEvents", operation.Name, operation.RequestType, operation.ResponseType));
    }

    foreach (var info in EventInfos.Values.Cast<IOutgoingMember>())
      members.Add(new(Section: "events", info.Name, info.RequestType, info.ResponseType));
    foreach (var info in ClientRequestInfos.Values.Cast<IOutgoingMember>())
      members.Add(new(Section: "clientApi", info.Name, info.RequestType, info.ResponseType));

    return Contracts.ContractGenerator.Generate(Route, members, Options.SerializerOptions, Options.Contract);
  }

  bool IsOriginAllowed(HttpContext context)
  {
    var origin = context.Request.Headers.Origin.ToString();
    if (origin.Length == 0)
      return true;

    if (Options.AllowedOrigins.Count > 0)
    {
      foreach (var allowed in Options.AllowedOrigins)
      {
        if (allowed == "*" || string.Equals(allowed.TrimEnd('/'), origin, StringComparison.OrdinalIgnoreCase))
          return true;
      }

      return false;
    }

    // Same origin: the page was served by the host it connects to. The scheme is not compared, since a proxy
    // that ends TLS makes https pages arrive as http requests.
    return Uri.TryCreate(origin, UriKind.Absolute, out var uri)
      && string.Equals(uri.Authority, context.Request.Host.Value, StringComparison.OrdinalIgnoreCase);
  }

  void Start()
  {
    if (Started)
      return;

    lock (Gate)
    {
      if (Started)
        return;

      Lookup = Operations.ToFrozenDictionary(StringComparer.Ordinal).GetAlternateLookup<ReadOnlySpan<char>>();
      Started = true;
    }
  }

  public void EnsureNotStarted()
  {
    if (Started)
      throw new InvalidOperationException("The endpoint already accepts connections: map everything at startup.");
  }

  Operation Resolve(ReadOnlySpan<byte> name)
  {
    // Names are ASCII and at most 256 bytes, so they decode on the stack.
    Span<char> chars = stackalloc char[NameRules.MaxLength];
    var length = Encoding.ASCII.GetChars(name, chars);
    return Lookup.TryGetValue(chars[..length], out var operation) ? operation : Operation.Unknown;
  }

  // Without a resolver, payload types are read by reflection where the app allows it. Under Native AOT the app
  // has to add a source-generated context, and GetTypeInfo then says which type is missing.
  [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Guarded by JsonSerializer.IsReflectionEnabledByDefault.")]
  [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Guarded by JsonSerializer.IsReflectionEnabledByDefault.")]
  static void UseResolver(JsonSerializerOptions options)
  {
    if (options.TypeInfoResolver is null && JsonSerializer.IsReflectionEnabledByDefault)
      options.TypeInfoResolver = new DefaultJsonTypeInfoResolver();
  }
}
