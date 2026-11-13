using System.Diagnostics.CodeAnalysis;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using PureGate.WebSocket.Hosting;
using PureGate.WebSocket.Serialization;

namespace PureGate.WebSocket;

// Maps what browsers can call and send: on the endpoint itself, or on a group whose names share a prefix.
public abstract class WebSocketBuilder
{
  private protected WebSocketBuilder(EndpointState endpoint, string prefix)
  {
    Endpoint = endpoint;
    Prefix = prefix;
  }

  internal EndpointState Endpoint { get; }

  // Names mapped here start with this and a dot. Empty on the endpoint.
  public string Prefix { get; }

  // A group whose names start with this one's prefix, prefix and a dot.
  public WebSocketGroup MapGroup(string prefix) => new(Endpoint, FullName(prefix));

  // An operation the browser calls with a request and gets a result from.
  public OperationBuilder MapRequest<TRequest, TResponse>(
      string name,
      Func<TRequest, WebSocketCall, CancellationToken, ValueTask<TResponse>> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    return Add(new RequestOperation<TRequest, TResponse>(FullName(name), Codecs<TRequest>(), Codecs<TResponse>(), handler));
  }

  // An operation without a request, such as "orders.latest".
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

  // An operation without a result: the browser's call completes when the handler does.
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

  // An operation with neither a request nor a result.
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

  // A stream the browser opens and reads. The handler's items are pulled only as fast as the browser takes them, and
  // its token fires when the browser stops reading.
  public OperationBuilder MapStream<TRequest, TItem>(
      string name,
      Func<TRequest, WebSocketCall, CancellationToken, IAsyncEnumerable<TItem>> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    return Add(new StreamOperation<TRequest, TItem>(FullName(name), Codecs<TRequest>(), Codecs<TItem>(), handler));
  }

  // A stream without a request, such as "prices.all".
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

  // A stream the browser sends. The handler reads its items, which arrive only as fast as it takes them, and returns
  // the browser's result.
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

  // An upload without a request.
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

  // An upload without a result: the browser's upload completes when the handler does.
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

  // An upload with neither a request nor a result.
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

  // An event the browser sends. Events from one connection are handled in order (PureGateWebSocketOptions.EventOrdering).
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

// A group of names under one prefix, from MapGroup.
public sealed class WebSocketGroup : WebSocketBuilder
{
  internal WebSocketGroup(EndpointState endpoint, string prefix)
    : base(endpoint, prefix)
  {
  }
}

// The endpoint from MapPureGateWebSocket. Conventions such as RequireAuthorization apply to its upgrade request.
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

  // The endpoint's API as TypeScript, as MapContract serves it. Reading it freezes the mapping.
  public WebSocketContract Contract => Endpoint.Contract;

  // Serves contract.ts over HTTP GET, for developers to pull into the browser app. It shows the whole API, so map it
  // in development only, or put it behind authorization with the conventions this returns.
  public IEndpointConventionBuilder MapContract([StringSyntax("Route")] string pattern)
  {
    ArgumentNullException.ThrowIfNull(pattern);
    return Routes.MapGet(pattern, Endpoint.WriteContractAsync);
  }

  // An event this endpoint's connections can be sent. Its name is the descriptor's, whatever the prefix.
  public WebSocketEndpointBuilder MapEvent<T>(ServerEvent<T> serverEvent)
  {
    ArgumentNullException.ThrowIfNull(serverEvent);
    Endpoint.AddEvent(serverEvent);
    return this;
  }

  // A call this endpoint's browsers answer. Its name is the descriptor's, whatever the prefix.
  public WebSocketEndpointBuilder MapClientRequest<TRequest, TResponse>(ClientRequest<TRequest, TResponse> request)
  {
    ArgumentNullException.ThrowIfNull(request);
    Endpoint.AddClientRequest(request);
    return this;
  }

  // Runs when a session starts, after Welcome and before the browser's first calls are handled.
  public WebSocketEndpointBuilder OnConnected(Func<WebSocketConnection, CancellationToken, ValueTask> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    Endpoint.Connected.Add(handler);
    return this;
  }

  // Runs when a session ends.
  public WebSocketEndpointBuilder OnDisconnected(Func<WebSocketConnection, CancellationToken, ValueTask> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    Endpoint.Disconnected.Add(handler);
    return this;
  }

  public void Add(Action<EndpointBuilder> convention) => Inner.Add(convention);

  public void Finally(Action<EndpointBuilder> finallyConvention) => Inner.Finally(finallyConvention);
}

// One mapped operation, stream or event: its authorization and filters.
public sealed class OperationBuilder
{
  readonly EndpointState Endpoint;

  internal OperationBuilder(EndpointState endpoint, Operation operation)
  {
    Endpoint = endpoint;
    Operation = operation;
  }

  internal Operation Operation { get; }

  // The full name browsers use, such as "orders.get".
  public string Name => Operation.Name;

  // Every call must pass these policies, evaluated with IAuthorizationService for the connection's user, or the
  // default policy when none is named. The outcome is cached per connection until its user changes. A refused call
  // fails with unauthorized, or forbidden for an authenticated user, before its handler runs; a refused event is
  // dropped. Needs services.AddAuthorization().
  public OperationBuilder RequireAuthorization(params string[] policies)
  {
    ArgumentNullException.ThrowIfNull(policies);
    Endpoint.EnsureNotStarted();
    var added = policies.Length == 0 ? [""] : policies;
    Operation.Policies = Operation.Policies is null ? added : [.. Operation.Policies, .. added];
    return this;
  }

  // Runs filter around every call's handler, after authorization and the filters added before it.
  public OperationBuilder AddFilter(WebSocketFilter filter)
  {
    ArgumentNullException.ThrowIfNull(filter);
    Endpoint.EnsureNotStarted();
    (Operation.Filters ??= []).Add(filter);
    return this;
  }
}
