# PureGate.WebSocket

Typed calls, streams and events between ASP.NET Core and browsers over one WebSocket. The browser side is [`@puregate/web-socket`](https://www.npmjs.com/package/@puregate/web-socket), a TypeScript client with no runtime dependencies. Its types come from your server: map your operations in C#, pull the generated `contract.ts`, and every call in the browser is checked by the compiler.

```csharp
var builder = WebApplication.CreateBuilder(args);
builder.Services.AddPureGateWebSocket();

var app = builder.Build();
app.UseWebSockets();

var ws = app.MapPureGateWebSocket("/ws");
var orders = ws.MapGroup("orders");
orders.MapRequest<int, Order>("get", static async (id, call, cancellationToken) =>
  await call.Services.GetRequiredService<OrderStore>().FindAsync(id, cancellationToken)
    ?? throw new CallException("not_found", $"Order {id} does not exist."));
orders.MapStream<string, Order>("watch", static (customer, call, cancellationToken) =>
  call.Services.GetRequiredService<OrderStore>().WatchAsync(customer, cancellationToken));
ws.MapEvent(Shop.OrderShipped);

if (app.Environment.IsDevelopment())
  ws.MapContract("/ws/contract.ts");

app.Run();

static class Shop
{
  public static readonly ServerEvent<Order> OrderShipped = new("orders.shipped");
}
```

In the browser, with the `contract.ts` pulled from `/ws/contract.ts`:

```ts
import { connect } from "@puregate/web-socket";
import { type Contract, contractHash } from "./contract.ts";

const conn = connect<Contract>("/ws", { contractHash });

const order = await conn.api.orders.get(42);
for await (const update of conn.streams.orders.watch("acme")) render(update);
conn.on.orders.shipped((shipped) => toast(`Order ${shipped.id} shipped`));
```

## What you get

- **A contract from your own types.** `MapContract` serves TypeScript generated from the server's JSON metadata, so names, casing and optional properties come out exactly as they go over the wire. Its hash tells a development server when a browser's copy is stale.
- **Calls both ways.** `MapRequest` for the browser's calls, `InvokeAsync` for the server's calls into the browser, with deadlines and cancellation that reach the other side.
- **Streams and uploads with flow control.** `MapStream` pulls an `IAsyncEnumerable<T>` only as fast as the browser reads. `MapUpload` hands the handler the browser's items as an `IAsyncEnumerable<T>`, read only as fast as it takes them: a file upload never fills the server's memory.
- **Events,** to one connection, a user, a group or everyone through `IWebSocketHub`, and from the browser with `MapClientEvent`.
- **Connections that survive the network.** A dropped connection resumes its session: calls, streams and events in progress carry on, and nothing is lost or repeated. A stopping server sends GoAway, finishes the calls in progress, and browsers move to another instance.
- **Security.** Cookie or token authentication, token refresh without reconnecting, authorization policies per operation, filters, a rate limiter per connection, an origin check, and limits on every window and queue.
- **Observability.** Metrics and traces named `PureGate.WebSocket`: add them with `AddMeter("PureGate.WebSocket")` and `AddSource("PureGate.WebSocket")`. A browser's `traceparent` continues into the handler's span.
- **Native AOT.** No reflection on the hot path; JSON goes through source-generated metadata when you provide it.

## Handlers

```csharp
ws.MapRequest<TRequest, TResponse>(name, (request, call, cancellationToken) => ValueTask<TResponse>);
ws.MapStream<TRequest, TItem>(name, (request, call, cancellationToken) => IAsyncEnumerable<TItem>);
ws.MapUpload<TRequest, TItem, TResponse>(name, (request, items, call, cancellationToken) => ValueTask<TResponse>);
ws.MapClientEvent<T>(name, (payload, call, cancellationToken) => ValueTask);
```

Overloads drop the request or the result. The `cancellationToken` fires when the browser cancels, the deadline passes, or the session ends; like `call`, it is valid only until the handler finishes. `call.Services` is a DI scope created only when you use it, and `call.Connection` reaches the connection: its user, its groups, `SendAsync` and `InvokeAsync`.

Handlers start on the connection's receive loop as soon as their message is read, so one that finishes without waiting costs no thread switch. Until its first `await`, though, a handler holds up its connection's next messages. Move blocking or CPU-heavy work off the loop with `await Task.Yield()` first, and never wait synchronously for the connection's own `InvokeAsync`: its answer arrives on the loop you would be holding.

A handler that throws `CallException("code", "message", details)` fails the call with that code, which the browser sees as `CallError`. Any other exception becomes `handler_failed`, with its message kept on the server unless `IncludeExceptionDetails` is on.

```csharp
ws.MapRequest<Order>("orders.place", PlaceAsync)
  .RequireAuthorization("customers")
  .AddFilter(static async (call, next) =>
  {
    using var scope = call.Services.GetRequiredService<ILogger<Program>>().BeginScope(call.Name);
    await next();
  });
```

## Payload formats

Payloads are JSON unless the browser asks for another format, such as MessagePack. Bring the library you like: wrap it in an `IPayloadFormat`, and add it to the options. Browsers that ask for no format keep using JSON.

```csharp
sealed class MessagePackFormat(MessagePackSerializerOptions options) : IPayloadFormat
{
  public string Name => "msgpack";

  public void Write<T>(IBufferWriter<byte> output, T value) => MessagePackSerializer.Serialize(output, value, options);

  public T Read<T>(ReadOnlyMemory<byte> payload) => MessagePackSerializer.Deserialize<T>(payload, options);
}

builder.Services.AddPureGateWebSocket(options => options.PayloadFormats.Add(new MessagePackFormat(messagePackOptions)));
```

The browser asks for it by name, with a JavaScript MessagePack library:

```ts
import { decode, encode } from "@msgpack/msgpack";

const conn = connect<Contract>("/ws", { contractHash, format: { name: "msgpack", encode, decode } });
```

`contract.ts` describes each value as its JSON, so a format must read and write the same shapes: objects as maps keyed by their JSON property names, not as arrays. `byte[]` and `ReadOnlyMemory<byte>` stay raw bytes in every format, and error details stay JSON. `Read` gets a slice of the received message, which is reused once it returns, so the value it reads must not keep that memory.

## Options

`AddPureGateWebSocket(options => …)` sets them for every endpoint, and `MapPureGateWebSocket(pattern, options => …)` for one. The defaults suit untrusted clients on the public internet; among them:

| Option | Default | |
|---|---|---|
| `MaxConcurrentCalls` | 32 | Calls one browser may have running at once. More wait in the browser. |
| `MaxStreams` | 8 | Streams and uploads open at once. |
| `MaxMessageBytes` | 64 KiB | The largest message accepted. Larger data goes through streams. |
| `ResumeWindow` | 30 s | How long a dropped session waits for the browser. `TimeSpan.Zero` turns resume off. |
| `Authenticate` | none | Validates a token from the browser, for apps that cannot use cookies. |
| `PayloadFormats` | none | [Payload formats](#payload-formats) besides JSON that browsers may ask for. |
| `RateLimiter` | none | A `RateLimiter` per connection. |
| `AllowedOrigins` | the endpoint's own | Origins allowed to connect, or `"*"`. |
| `SlowConsumer` | `Close` | What happens to a browser that falls too far behind: close it, and let it resume, or drop events for it. |
| `DrainTimeout` | 10 s | How long a stopping server lets calls in progress finish. |

## More

- [The protocol](https://github.com/PureGate/PureGate.WebSocket/blob/main/docs/PROTOCOL.md)
- [The server's design](https://github.com/PureGate/PureGate.WebSocket/blob/main/docs/PLAN-SERVER.md) and [the client's](https://github.com/PureGate/PureGate.WebSocket/blob/main/docs/PLAN-CLIENT.md)
- [The chat sample](https://github.com/PureGate/PureGate.WebSocket/tree/main/samples/chat)

MIT licensed.
