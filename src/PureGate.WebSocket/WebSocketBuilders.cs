using System.Diagnostics.CodeAnalysis;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using PureGate.WebSocket.Hosting;
using PureGate.WebSocket.Serialization;

namespace PureGate.WebSocket;

/// <summary>
/// Maps what browsers can call and send: on the endpoint itself, or on a group whose names share a prefix.
/// </summary>
public abstract class WebSocketBuilder
{
  private protected WebSocketBuilder(EndpointState endpoint, string prefix)
  {
    Endpoint = endpoint;
    Prefix = prefix;
  }

  internal EndpointState Endpoint { get; }

  /// <summary>
  /// Names mapped here start with this and a dot. Empty on the endpoint.
  /// </summary>
  public string Prefix { get; }

  /// <summary>
  /// A group whose names start with this one's prefix, prefix and a dot.
  /// </summary>
  public WebSocketGroup MapGroup(string prefix) => new(Endpoint, FullName(prefix));

  /// <summary>
  /// An operation the browser calls with a request and gets a result from.
  /// </summary>
  public OperationBuilder MapRequest<TRequest, TResponse>(
      string name,
      Func<TRequest, WebSocketCall, CancellationToken, ValueTask<TResponse>> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    return Add(new RequestOperation<TRequest, TResponse>(FullName(name), Codecs<TRequest>(), Codecs<TResponse>(), handler));
  }

  /// <summary>
  /// An operation without a request, such as "orders.latest".
  /// </summary>
  public OperationBuilder MapRequest<TResponse>(
      string name,
      Func<WebSocketCall, CancellationToken, ValueTask<TResponse>> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    return Add(new RequestOperation<NoPayload, TResponse>(
      FullName(name),
      Codecs<NoPayload>(),
      Codecs<TResponse>(),
      (_, call, cancellationToken) => handler(call, cancellationToken)));
  }

  /// <summary>
  /// An operation without a result: the browser's call completes when the handler does.
  /// </summary>
  public OperationBuilder MapRequest<TRequest>(
      string name,
      Func<TRequest, WebSocketCall, CancellationToken, ValueTask> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    return Add(new RequestOperation<TRequest, NoPayload>(
      FullName(name),
      Codecs<TRequest>(),
      Codecs<NoPayload>(),
      async (request, call, cancellationToken) =>
      {
        await handler(request, call, cancellationToken);
        return default;
      }));
  }

  /// <summary>
  /// An operation with neither a request nor a result.
  /// </summary>
  public OperationBuilder MapRequest(string name, Func<WebSocketCall, CancellationToken, ValueTask> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    return Add(new RequestOperation<NoPayload, NoPayload>(
      FullName(name),
      Codecs<NoPayload>(),
      Codecs<NoPayload>(),
      async (_, call, cancellationToken) =>
      {
        await handler(call, cancellationToken);
        return default;
      }));
  }

  /// <summary>
  /// A stream the browser opens and reads. The handler's items are pulled only as fast as the browser takes them, and
  /// its token fires when the browser stops reading.
  /// </summary>
  public OperationBuilder MapStream<TRequest, TItem>(
      string name,
      Func<TRequest, WebSocketCall, CancellationToken, IAsyncEnumerable<TItem>> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    return Add(new StreamOperation<TRequest, TItem>(FullName(name), Codecs<TRequest>(), Codecs<TItem>(), handler));
  }

  /// <summary>
  /// A stream without a request, such as "prices.all".
  /// </summary>
  public OperationBuilder MapStream<TItem>(
      string name,
      Func<WebSocketCall, CancellationToken, IAsyncEnumerable<TItem>> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    return Add(new StreamOperation<NoPayload, TItem>(
      FullName(name),
      Codecs<NoPayload>(),
      Codecs<TItem>(),
      (_, call, cancellationToken) => handler(call, cancellationToken)));
  }

  /// <summary>
  /// A stream the browser sends. The handler reads its items, which arrive only as fast as it takes them, and returns
  /// the browser's result.
  /// </summary>
  public OperationBuilder MapUpload<TRequest, TItem, TResponse>(
      string name,
      Func<TRequest, IAsyncEnumerable<TItem>, WebSocketCall, CancellationToken, ValueTask<TResponse>> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    return Add(new UploadOperation<TRequest, TItem, TResponse>(
      FullName(name),
      Codecs<TRequest>(),
      Codecs<TItem>(),
      Codecs<TResponse>(),
      handler));
  }

  /// <summary>
  /// An upload without a request.
  /// </summary>
  public OperationBuilder MapUpload<TItem, TResponse>(
      string name,
      Func<IAsyncEnumerable<TItem>, WebSocketCall, CancellationToken, ValueTask<TResponse>> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    return Add(new UploadOperation<NoPayload, TItem, TResponse>(
      FullName(name),
      Codecs<NoPayload>(),
      Codecs<TItem>(),
      Codecs<TResponse>(),
      (_, items, call, cancellationToken) => handler(items, call, cancellationToken)));
  }

  /// <summary>
  /// An upload without a result: the browser's upload completes when the handler does.
  /// </summary>
  public OperationBuilder MapUpload<TRequest, TItem>(
      string name,
      Func<TRequest, IAsyncEnumerable<TItem>, WebSocketCall, CancellationToken, ValueTask> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    return Add(new UploadOperation<TRequest, TItem, NoPayload>(
      FullName(name),
      Codecs<TRequest>(),
      Codecs<TItem>(),
      Codecs<NoPayload>(),
      async (request, items, call, cancellationToken) =>
      {
        await handler(request, items, call, cancellationToken);
        return default;
      }));
  }

  /// <summary>
  /// An upload with neither a request nor a result.
  /// </summary>
  public OperationBuilder MapUpload<TItem>(
      string name,
      Func<IAsyncEnumerable<TItem>, WebSocketCall, CancellationToken, ValueTask> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    return Add(new UploadOperation<NoPayload, TItem, NoPayload>(
      FullName(name),
      Codecs<NoPayload>(),
      Codecs<TItem>(),
      Codecs<NoPayload>(),
      async (_, items, call, cancellationToken) =>
      {
        await handler(items, call, cancellationToken);
        return default;
      }));
  }

  /// <summary>
  /// An event the browser sends. Events from one connection are handled in order (PureGateWebSocketOptions.EventOrdering).
  /// </summary>
  public OperationBuilder MapClientEvent<T>(string name, Func<T, WebSocketCall, CancellationToken, ValueTask> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    return Add(new ClientEventOperation<T>(FullName(name), Codecs<T>(), handler));
  }

  PayloadCodec<T>[] Codecs<T>() => Endpoint.Codecs<T>();

  OperationBuilder Add(Operation operation)
  {
    Endpoint.Add(operation);
    return new OperationBuilder(Endpoint, operation);
  }

  string FullName(string name)
  {
    ArgumentNullException.ThrowIfNull(name);
    return NameCheck.Validate(Prefix.Length == 0 ? name : $"{Prefix}.{name}");
  }
}

/// <summary>
/// A group of names under one prefix, from MapGroup.
/// </summary>
public sealed class WebSocketGroup : WebSocketBuilder
{
  internal WebSocketGroup(EndpointState endpoint, string prefix)
    : base(endpoint, prefix)
  {
  }
}

/// <summary>
/// The endpoint from MapPureGateWebSocket. Conventions such as RequireAuthorization apply to its upgrade request.
/// </summary>
public sealed class WebSocketEndpointBuilder : WebSocketBuilder, IEndpointConventionBuilder
{
  readonly IEndpointConventionBuilder Inner;
  readonly IEndpointRouteBuilder Routes;

  internal WebSocketEndpointBuilder(EndpointState endpoint, IEndpointConventionBuilder inner, IEndpointRouteBuilder routes)
    : base(endpoint, "")
  {
    Inner = inner;
    Routes = routes;
  }

  /// <summary>
  /// The endpoint's API as TypeScript, as MapContract serves it. Reading it freezes the mapping.
  /// </summary>
  public WebSocketContract Contract => Endpoint.Contract;

  /// <summary>
  /// Serves contract.ts over HTTP GET, for developers to pull into the browser app. It shows the whole API, so map it
  /// in development only, or put it behind authorization with the conventions this returns.
  /// </summary>
  public IEndpointConventionBuilder MapContract([StringSyntax("Route")] string pattern)
  {
    ArgumentNullException.ThrowIfNull(pattern);
    return Routes.MapGet(pattern, Endpoint.WriteContractAsync);
  }

  /// <summary>
  /// An event this endpoint's connections can be sent. Its name is the descriptor's, whatever the prefix.
  /// </summary>
  public WebSocketEndpointBuilder MapEvent<T>(ServerEvent<T> serverEvent)
  {
    ArgumentNullException.ThrowIfNull(serverEvent);
    Endpoint.AddEvent(serverEvent);
    return this;
  }

  /// <summary>
  /// A call this endpoint's browsers answer. Its name is the descriptor's, whatever the prefix.
  /// </summary>
  public WebSocketEndpointBuilder MapClientRequest<TRequest, TResponse>(ClientRequest<TRequest, TResponse> request)
  {
    ArgumentNullException.ThrowIfNull(request);
    Endpoint.AddClientRequest(request);
    return this;
  }

  /// <summary>
  /// Runs when a session starts, after Welcome and before the browser's first calls are handled.
  /// </summary>
  public WebSocketEndpointBuilder OnConnected(Func<WebSocketConnection, CancellationToken, ValueTask> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    Endpoint.Connected.Add(handler);
    return this;
  }

  /// <summary>
  /// Runs when a session ends.
  /// </summary>
  public WebSocketEndpointBuilder OnDisconnected(Func<WebSocketConnection, CancellationToken, ValueTask> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    Endpoint.Disconnected.Add(handler);
    return this;
  }

  /// <inheritdoc/>
  public void Add(Action<EndpointBuilder> convention) => Inner.Add(convention);

  /// <inheritdoc/>
  public void Finally(Action<EndpointBuilder> finallyConvention) => Inner.Finally(finallyConvention);
}

/// <summary>
/// One mapped operation, stream or event: its authorization and filters.
/// </summary>
public sealed class OperationBuilder
{
  readonly EndpointState Endpoint;

  internal OperationBuilder(EndpointState endpoint, Operation operation)
  {
    Endpoint = endpoint;
    Operation = operation;
  }

  internal Operation Operation { get; }

  /// <summary>
  /// The full name browsers use, such as "orders.get".
  /// </summary>
  public string Name => Operation.Name;

  /// <summary>
  /// Every call must pass these policies, evaluated with IAuthorizationService for the connection's user, or the
  /// default policy when none is named. The outcome is cached per connection until its user changes. A refused call
  /// fails with unauthorized, or forbidden for an authenticated user, before its handler runs; a refused event is
  /// dropped. Needs services.AddAuthorization().
  /// </summary>
  public OperationBuilder RequireAuthorization(params string[] policies)
  {
    ArgumentNullException.ThrowIfNull(policies);
    Endpoint.EnsureNotStarted();
    var added = policies.Length == 0 ? [""] : policies;
    Operation.Policies = Operation.Policies is null ? added : [.. Operation.Policies, .. added];
    return this;
  }

  /// <summary>
  /// Runs filter around every call's handler, after authorization and the filters added before it.
  /// </summary>
  public OperationBuilder AddFilter(WebSocketFilter filter)
  {
    ArgumentNullException.ThrowIfNull(filter);
    Endpoint.EnsureNotStarted();
    (Operation.Filters ??= []).Add(filter);
    return this;
  }
}
