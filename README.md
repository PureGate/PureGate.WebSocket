# PureGate.WebSocket

[![CI](https://github.com/PureGate/PureGate.WebSocket/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/PureGate/PureGate.WebSocket/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/PureGate.WebSocket?logo=nuget&label=NuGet)](https://www.nuget.org/packages/PureGate.WebSocket)
[![npm](https://img.shields.io/npm/v/@puregate/web-socket?logo=npm&label=npm)](https://www.npmjs.com/package/@puregate/web-socket)
[![Client size](https://img.shields.io/bundlejs/size/@puregate/web-socket?label=client%20min%2Bgzip)](https://bundlejs.com/?q=@puregate/web-socket)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

Typed calls, streams and events between ASP.NET Core and browsers over one WebSocket.

| Package | |
|---|---|
| [`PureGate.WebSocket`](src/PureGate.WebSocket/README.md) on NuGet | The server, for ASP.NET Core. |
| [`@puregate/web-socket`](packages/web-socket/README.md) on npm | The browser client, in TypeScript, with no runtime dependencies. |

## A contract from your own types

Map operations in C#, and the server generates `contract.ts` from them. In the browser, every name is a typed path and every call is checked by the TypeScript compiler.

```csharp
builder.Services.AddPureGateWebSocket();
app.UseWebSockets();

var ws = app.MapPureGateWebSocket("/ws");
if (app.Environment.IsDevelopment())
  ws.MapContract("/ws/contract.ts");
```

```ts
import { connect } from "@puregate/web-socket";
import { type Contract, contractHash } from "./contract.ts"; // pulled from /ws/contract.ts

const conn = connect<Contract>("/ws", { contractHash });
```

## Calls

Deadlines and cancellation reach the server. A `CallException` becomes a `CallError` with the same code.

```csharp
ws.MapRequest<int, Order>("orders.get", static async (id, call, cancellationToken) =>
  await call.Services.GetRequiredService<OrderStore>().FindAsync(id, cancellationToken)
    ?? throw new CallException("not_found", $"Order {id} does not exist."));
```

```ts
try {
  render(await conn.api.orders.get(42, { timeout: 5_000, signal }));
} catch (error) {
  if (error instanceof CallError && error.code === "not_found") showMissing();
}
```

## Streams

The server pulls the `IAsyncEnumerable<T>` only as fast as the browser reads it. Breaking out of the loop cancels it.

```csharp
ws.MapStream<string, Tick>("prices.watch", static (symbol, call, cancellationToken) =>
  call.Services.GetRequiredService<Prices>().WatchAsync(symbol, cancellationToken));
```

```ts
for await (const tick of conn.streams.prices.watch("MSFT")) render(tick);
```

## Uploads

The browser sends items only as fast as the handler takes them, so a large file never fills the server's memory.

```csharp
ws.MapUpload<string, byte[], long>("files.put", static async (name, chunks, call, cancellationToken) =>
{
  long size = 0;
  await foreach (var chunk in chunks)
    size += chunk.Length;
  return size;
});
```

```ts
const size = await conn.uploads.files.put(file.name, file.stream());
```

## Events

To one connection, a user's tabs, a group or everyone, from anywhere in the app, and from the browser to the server.

```csharp
static class Shop
{
  public static readonly ServerEvent<Order> OrderShipped = new("orders.shipped");
}

ws.MapEvent(Shop.OrderShipped);
ws.MapClientEvent<Click>("telemetry.click", static (click, call, cancellationToken) => Analytics.TrackAsync(click, cancellationToken));

// Anywhere, through IWebSocketHub: All, User(id), Group(name), Connection(id).
await hub.User(order.CustomerId).SendAsync(Shop.OrderShipped, order);
```

```ts
const stop = conn.on.orders.shipped((order) => toast(`Order ${order.id} shipped`));
conn.emit.telemetry.click({ x: 10, y: 20 });
```

## Groups

A connection joins and leaves groups; it leaves them all when its session ends.

```csharp
ws.MapRequest<string>("rooms.join", static (room, call, cancellationToken) =>
{
  call.Connection.Groups.Add(room);
  return ValueTask.CompletedTask;
});

// In a handler: everyone in the lobby but the sender.
await hub.Group("lobby").Except(call.Connection).SendAsync(Chat.Posted, message);
```

## Calls into the browser

The server asks a browser and waits for its answer, with a deadline.

```csharp
static class Ui
{
  public static readonly ClientRequest<Question, bool> Confirm = new("ui.confirm");
}

ws.MapClientRequest(Ui.Confirm);

// In a handler: ask the browser that made the call.
bool sure = await call.Connection.InvokeAsync(Ui.Confirm, new Question("Cancel order 42?"), cancellationToken);
```

```ts
conn.handle.ui.confirm((question) => window.confirm(question.text));
```

## Connections that survive the network

A dropped connection reconnects with backoff and resumes its session: calls, streams and events in progress carry on, and nothing is lost or repeated. A stopping server sends GoAway and finishes the calls in progress while browsers move to another instance. Only when the session is gone does the page need to reload:

```ts
conn.onStateChange((state, change) => {
  if (state === "open" && change.resumed === false) reload();
});
```

## Security

Cookie or token authentication with refresh before it expires, authorization policies and filters per operation, a rate limiter per connection, and an origin check that is on by default.

```csharp
builder.Services.AddPureGateWebSocket(options =>
{
  options.Authenticate = static (token, context, cancellationToken) => Tokens.ValidateAsync(token, cancellationToken);
  options.RateLimiter = static connection => new TokenBucketRateLimiter(new()
  {
    TokenLimit = 100,
    TokensPerPeriod = 50,
    ReplenishmentPeriod = TimeSpan.FromSeconds(1),
  });
});

ws.MapRequest<Order>("orders.place", PlaceAsync)
  .RequireAuthorization("customers")
  .AddFilter(static async (call, next) =>
  {
    using var scope = call.Services.GetRequiredService<ILogger<Program>>().BeginScope(call.Name);
    await next();
  });
```

```ts
const conn = connect<Contract>("/ws", { contractHash, token: () => auth.getAccessToken() });
```

## Payload formats

JSON by default. Any other format, such as MessagePack, over the library you like: a small `IPayloadFormat` on the server, and `encode` and `decode` in the browser. [The full example.](src/PureGate.WebSocket/README.md#payload-formats)

```csharp
options.PayloadFormats.Add(new MessagePackFormat(MessagePackSerializerOptions.Standard));
```

```ts
import { decode, encode } from "@msgpack/msgpack";

const conn = connect<Contract>("/ws", { contractHash, format: { name: "msgpack", encode, decode } });
```

## Several servers

A backplane carries events between servers, so groups, users and broadcasts reach every one. It only moves bytes on topics, so one for Redis, NATS, Kafka or a database is short. [A Redis backplane.](src/PureGate.WebSocket/README.md#several-servers)

```csharp
builder.Services.AddSingleton<IWebSocketBackplane>(new RedisBackplane(ConnectionMultiplexer.Connect("localhost")));
```

## Observability

Metrics and traces named `PureGate.WebSocket`, and a browser's `traceparent` continues into the handler's span.

```csharp
builder.Services.AddOpenTelemetry()
  .WithMetrics(metrics => metrics.AddMeter("PureGate.WebSocket"))
  .WithTracing(tracing => tracing.AddSource("PureGate.WebSocket"));
```

```ts
const conn = connect<Contract>("/ws", { contractHash, traceparent: () => currentTraceparent() });
```

## Native AOT

No reflection on the hot path. Give the serializer source-generated metadata, and the server publishes with Native AOT.

```csharp
[JsonSerializable(typeof(Order))]
partial class AppJson : JsonSerializerContext;

builder.Services.AddPureGateWebSocket(options => options.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJson.Default));
```

## Documentation

- [The server](src/PureGate.WebSocket/README.md) and [the client](packages/web-socket/README.md): every option, and what happens when things go wrong.
- [The protocol](docs/PROTOCOL.md): the wire format, which both packages implement and test against the shared [test vectors](test-vectors/frames.json).
- [The plan](docs/PLAN.md), and the designs of [the server](docs/PLAN-SERVER.md) and [the client](docs/PLAN-CLIENT.md).
- [The chat sample](samples/chat/README.md).
- [The benchmarks](docs/BENCHMARKS.md) against SignalR, and of the browser client.

## Repository

```
src/PureGate.WebSocket              the server library
src/PureGate.WebSocket.Test         its unit and integration tests
src/PureGate.WebSocket.TestServer   the server the TypeScript integration tests run against
src/PureGate.WebSocket.Benchmarks   the load generator against SignalR, run on purpose only
packages/web-socket                 the browser client and its tests
samples/chat                        a chat: an ASP.NET Core server and a Vite page
test-vectors                        byte-level vectors both implementations check
```

Building and testing:

```sh
dotnet build PureGate.WebSocket.slnx
dotnet test src/PureGate.WebSocket.Test

yarn install
yarn check
yarn typecheck
yarn test             # unit and integration tests in Node, against the TestServer
yarn test:browsers    # the same in Chromium, Firefox and WebKit
yarn workspace @puregate/web-socket size
```

The browser tests need Playwright's browsers once: `yarn workspace @puregate/web-socket playwright install`.

MIT licensed.
