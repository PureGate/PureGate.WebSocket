using Microsoft.AspNetCore.Builder;
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
    return Add(new RequestOperation<TRequest, TResponse>(FullName(name), Codec<TRequest>(), Codec<TResponse>(), handler));
  }

  // An operation without a request, such as "orders.latest".
  public OperationBuilder MapRequest<TResponse>(
      string name,
      Func<WebSocketCall, CancellationToken, ValueTask<TResponse>> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    return Add(new RequestOperation<NoPayload, TResponse>(
      FullName(name),
      Codec<NoPayload>(),
      Codec<TResponse>(),
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
      Codec<TRequest>(),
      Codec<NoPayload>(),
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
      Codec<NoPayload>(),
      Codec<NoPayload>(),
      async (_, call, cancellationToken) =>
      {
        await handler(call, cancellationToken);
        return default;
      }));
  }

  // An event the browser sends. Events from one connection are handled in order (PureGateWebSocketOptions.EventOrdering).
  public OperationBuilder MapClientEvent<T>(string name, Func<T, WebSocketCall, CancellationToken, ValueTask> handler)
  {
    ArgumentNullException.ThrowIfNull(handler);
    return Add(new ClientEventOperation<T>(FullName(name), Codec<T>(), handler));
  }

  PayloadCodec<T> Codec<T>() => PayloadCodecs.Create<T>(Endpoint.Options.SerializerOptions);

  OperationBuilder Add(Operation operation)
  {
    Endpoint.Add(operation);
    return new OperationBuilder(operation);
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

  internal WebSocketEndpointBuilder(EndpointState endpoint, IEndpointConventionBuilder inner)
    : base(endpoint, "")
  {
    Inner = inner;
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

// One mapped operation or event. Authorization and filters will be set here.
public sealed class OperationBuilder
{
  internal OperationBuilder(Operation operation) => Operation = operation;

  internal Operation Operation { get; }

  // The full name browsers use, such as "orders.get".
  public string Name => Operation.Name;
}
