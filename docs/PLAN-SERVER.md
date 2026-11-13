# PureGate.WebSocket server plan

The .NET package `PureGate.WebSocket` is the ASP.NET Core side of [PROTOCOL.md](PROTOCOL.md). It targets net10.0 and net11.0, references only the ASP.NET Core shared framework, has no NuGet dependencies, and is Native AOT compatible. The phases are in [PLAN.md](PLAN.md); the browser side is in [PLAN-CLIENT.md](PLAN-CLIENT.md).

## Target usage

```csharp
using PureGate.WebSocket;

var builder = WebApplication.CreateSlimBuilder(args);
builder.Services.AddPureGateWebSocket(options =>
{
  options.SerializerOptions.TypeInfoResolverChain.Insert(0, ShopJsonContext.Default);
});

var app = builder.Build();
app.UseWebSockets();

var ws = app.MapPureGateWebSocket("/ws").RequireAuthorization();

var orders = ws.MapGroup("orders");
orders.MapRequest<int, Order>("get", static async (id, call, cancellationToken) =>
  await call.Services.GetRequiredService<OrderStore>().GetAsync(id, cancellationToken)
    ?? throw new CallException("not_found", $"Order {id} does not exist."));

ws.MapStream<string, Tick>("prices.watch", static (symbol, call, cancellationToken) =>
  call.Services.GetRequiredService<PriceFeed>().WatchAsync(symbol, cancellationToken));

ws.MapUpload<NewFile, byte[], SavedFile>("files.put", static (file, chunks, call, cancellationToken) =>
  call.Services.GetRequiredService<FileStore>().SaveAsync(file, chunks, cancellationToken));

ws.MapEvent(Shop.OrderShipped);
ws.MapClientRequest(Shop.Confirm);
ws.MapClientEvent<Click>("telemetry.click", static (click, call, cancellationToken) => ValueTask.CompletedTask);

ws.OnConnected(static (connection, cancellationToken) =>
{
  connection.Groups.Add($"tenant:{connection.User.FindFirstValue("tenant")}");
  return ValueTask.CompletedTask;
});

if (app.Environment.IsDevelopment())
  ws.MapContract("/ws/contract.ts");

app.Run();

static class Shop
{
  public static readonly ServerEvent<OrderShipped> OrderShipped = new("orders.shipped");
  public static readonly ClientRequest<Question, bool> Confirm = new("ui.confirm");
}
```

Sending from anywhere in the app, and calling the browser from a handler:

```csharp
sealed class Shipping(IWebSocketHub hub)
{
  public ValueTask ShippedAsync(string userId, OrderShipped shipped) =>
    hub.User(userId).SendAsync(Shop.OrderShipped, shipped);
}

// In a handler: ask the browser that made the call.
bool confirmed = await call.Connection.InvokeAsync(Shop.Confirm, new Question("Cancel order 42?"), cancellationToken);
```

Events the server sends and requests the browser handles are declared once as static descriptors. Code that sends them uses the descriptor, so a send site never repeats a name or a type.

## Public API

### Registration

| API | Purpose |
|---|---|
| `services.AddPureGateWebSocket(configure)` | Registers the hub and the options shared by every endpoint. |
| `app.MapPureGateWebSocket(pattern, configure?)` | Maps a WebSocket endpoint and returns a `WebSocketEndpointBuilder`. `configure` overrides options for this endpoint. The route accepts any HTTP method, so WebSockets over HTTP/2 (CONNECT) match too. |
| `WebSocketEndpointBuilder` | Implements `IEndpointConventionBuilder`, so `RequireAuthorization`, `RequireHost` and the other conventions apply to the upgrade request. |
| `MapGroup(prefix)` | A builder whose names start with `prefix.`. Groups nest. |
| `MapRequest<TRequest, TResponse>(name, handler)` | An operation the browser calls. Overloads drop the request or the response: `(call, ct) => ValueTask<TResponse>`, `(request, call, ct) => ValueTask`, `(call, ct) => ValueTask`. They differ in the handler's parameters. |
| `MapStream<TRequest, TItem>(name, handler)` | A stream the browser opens. The handler returns `IAsyncEnumerable<TItem>`, and an overload drops the request. |
| `MapUpload<TRequest, TItem, TResponse>(name, handler)` | A stream the browser sends. The handler `(request, items, call, ct) => ValueTask<TResponse>` reads `items`, an `IAsyncEnumerable<TItem>`, and returns the result. Overloads drop the request or the response. |
| `MapEvent<T>(ServerEvent<T>)` | Declares an event the server sends. |
| `MapClientRequest<TRequest, TResponse>(ClientRequest<TRequest, TResponse>)` | Declares an operation the browser handles. |
| `MapClientEvent<T>(name, handler)` | An event the browser sends, handled by `(payload, call, ct) => ValueTask`. |
| `OnConnected`, `OnDisconnected` | Session lifecycle. A session that resumes does not disconnect, and `OnDisconnected` runs when the session ends. |
| `MapContract(pattern)` | Serves `contract.ts` over HTTP GET (see [Contract generation](#contract-generation)). |
| `Contract` | The same contract as an object, for tests. |

Each `Map*` call returns an `OperationBuilder` with `RequireAuthorization(policies)` and `AddFilter(filter)`. Names are validated when they are mapped. A duplicate name, an invalid name, or a type missing from the configured JSON metadata throws at startup, not on the first call.

### Handlers and calls

```csharp
Func<TRequest, WebSocketCall, CancellationToken, ValueTask<TResponse>> handler
```

Handlers are plain `Func`s, so the `MapRequest` overloads tell themselves apart by the handler's parameters, and a lambda's types are inferred. The `cancellationToken` fires when the caller cancels, the call's deadline passes, or the session ends.

| Type | Members |
|---|---|
| `WebSocketCall` | `Connection`, `User`, `Name`, `TraceParent`, and `Services`, a DI scope created only if the handler touches it. Like `HttpContext`, it is pooled and valid only until the handler finishes. |
| `WebSocketConnection` | `Id`, `User`, `Items`, `Groups` (`Add`, `Remove`, `Contains`), `SendAsync(ServerEvent<T>, T)`, `InvokeAsync(ClientRequest<TRequest, TResponse>, TRequest, ct, timeout?)`, `CloseAsync(code?)`, and `Closed`, a token that fires when the session ends. `HttpContext` is the current transport's, which changes after a resume. |
| `IWebSocketHub` | `All`, `User(id)`, `Group(name)`, `Connection(id)`, each a `WebSocketTarget` with `SendAsync(ServerEvent<T>, T)` and `Except(connection)`. It covers every endpoint in the app; a connection only receives the events its endpoint maps. |
| `ServerEvent<T>`, `ClientRequest<TRequest, TResponse>` | Descriptors: a name and types, created once, usually as static fields. Sending an event that no endpoint maps throws. |
| `CallException(code, message, details?)` | Fails a call with an application error code and optional JSON details. |

How a handler's outcome reaches the caller:

| Handler outcome | Error sent |
|---|---|
| Returns | none |
| Throws `CallException` | its code, message and details |
| Cancelled by the caller | `cancelled`, which the caller has already stopped waiting for |
| Deadline passed | `timeout` |
| Request payload unreadable | `invalid_payload`, before the handler runs |
| Throws anything else | `handler_failed`, with the exception message only if `IncludeExceptionDetails` is on |
| Returns a result larger than the client's `maxMessage` | `payload_too_large` |

### Options

Defaults are sized for untrusted clients on the public internet.

| Option | Default | Notes |
|---|---|---|
| `SerializerOptions` | `JsonSerializerDefaults.Web` | camelCase, which matches TypeScript. For AOT, add a source-generated context. |
| `MaxMessageBytes` | 64 KiB | The largest message accepted from a client. Larger data goes through streams. |
| `MaxConcurrentCalls` | 32 | Welcome's `calls`. SignalR runs one call at a time per client by default. |
| `MaxStreams` | 8 | Welcome's `streams`, downloads and uploads together. |
| `UploadWindow` | 16 | Items an upload may send ahead of its handler. |
| `EventWindow` | 256 | Welcome's `events`. |
| `HandshakeTimeout` | 10 s | Time allowed for the Hello. |
| `Heartbeat` | 15 s | Welcome's `heartbeat`. |
| `KeepAliveInterval`, `KeepAliveTimeout` | 15 s, 30 s | WebSocket ping frames. A browser that stops answering is closed. |
| `SendQueueLimitBytes` | 1 MiB | Bytes waiting to be written to one connection. |
| `MaxUnacknowledgedFrames` | 4,096 | How far the page may fall behind before `SlowConsumer` applies. |
| `SlowConsumer` | `Close` | `Close` (4003, and the client resumes) or `DropEvents`. |
| `ResumeWindow` | 30 s | How long a dropped session stays resumable. 0 turns resume off. |
| `ReplayBufferBytes` | 1 MiB | Unacknowledged frames kept for resume. Past this, the session can no longer resume. |
| `ResumeRoute` | none | This instance's routing id, sent as Welcome's `route` (see [Deploys and several instances](#deploys-and-several-instances)). |
| `DrainTimeout` | 10 s | How long a stopping server lets exchanges in progress finish after GoAway. |
| `AllowedOrigins` | none | When empty, only same-origin upgrades and requests without an `Origin` header are accepted. |
| `Authenticate` | none | Validates the token from Hello, or from an Authenticate frame on a refresh. Returns the user and, optionally, when the authentication expires. |
| `RateLimiter` | none | A factory for one `RateLimiter` per connection. Every call, stream and event takes a permit until its handler finishes; past the limit, calls and streams fail with `overloaded` and events are dropped. |
| `UserId` | the `NameIdentifier` claim | Maps a user to an id, for `hub.User(id)`. |
| `InvokeTimeout` | 30 s | Default deadline for server → browser calls. |
| `IncludeExceptionDetails` | false | |
| `ContractMismatch` | `Warn` | `Ignore`, `Warn` (log it and tell the client) or `Reject` (close with 4004). |
| `EventOrdering` | `Sequential` | Events from one connection are handled one at a time, in order. `Concurrent` lifts that. |
| `EnableCompression` | false | permessage-deflate (see [Security](#security)). |

The memory one connection can hold is bounded by these limits: one message being received, the send queue, the replay buffer, and the request payloads within the calls window.

## Internals

```
src/PureGate.WebSocket/
  Protocol/     VarInt, FrameReader, FrameWriter, FrameKind, FrameFlags, NameTable, ErrorCode, handshake JSON
  Transport/    upgrade (subprotocol, origin, keep-alive, compression), receive loop, send path, replay buffer
  Sessions/     Session (survives reconnects), SessionStore (resume lookup, expiry), windows, pending exchanges
  Endpoints/    builders, Map* registration, operation registry, descriptors
  Dispatch/     pooled call work items, error mapping, event ordering, stream pumping
  Hub/          connection registry, user index, groups, broadcast
  Contract/     schema export, TypeScript emitter, canonical hash, MapContract
  Security/     origin policy, token authentication, authorization cache
  Diagnostics/  Meter, ActivitySource, source-generated log messages
  Timing/       one shared timer for every deadline
```

The library's root namespace is `PureGate.WebSocket`. Inside the library, the name `WebSocket` resolves to that namespace, not to `System.Net.WebSockets.WebSocket`. So library code refers to the BCL type through an alias, which keeps the collision out of public signatures.

A **Session** holds everything that must survive a reconnect: id counters, both name tables, sequence counts, windows, credits, pending exchanges, groups and `Items`. A transport (one accepted WebSocket) attaches to it. A resume swaps the transport and replays. A session without a transport keeps running: its handlers finish, and everything it sends goes to the replay buffer until the client resumes or the buffer fills.

## Performance design

The goal is less CPU, memory and allocation per message and per connection than SignalR, measured, not assumed.

1. **Receiving.** `WebSocket.ReceiveAsync(Memory<byte>)` returns a `ValueTask`, so receiving allocates nothing. Each message is first waited for with a zero-byte receive, which ASP.NET Core's managed WebSocket supports: when it has to wait, the buffer goes back to the pool, so an idle connection holds none. The buffer is rented again for the message and grown to fit it.
2. **Parsing in place.** `FrameReader` is a `ref struct` over the message bytes. Payloads are deserialized straight from the span, using a `JsonTypeInfo<T>` resolved once at mapping time. There are no per-call type lookups and no intermediate strings.
3. **Names.** A name reference is an index into an array of resolved operations. A literal name is looked up through a frozen dictionary's span-based alternate lookup, after decoding its ASCII bytes on the stack. The table never keeps a client's text: a name without a handler resolves to one shared "unknown" entry. The protocol's 1,024 definitions per connection therefore cost at most one reference each, however long the names are.
4. **Sending.** Frames are written with `Utf8JsonWriter` into pooled buffers. A sender writes directly when no send is in progress and queues otherwise. The sender that finishes drains the queue, coalescing queued frames into one message of up to 64 KiB. There is no send task per connection.
5. **Broadcast.** The payload is serialized once into a reference-counted pooled buffer. Each connection writes its own small header (its name code differs) and copies the payload. A slow connection never holds up the others: its frames queue, and its slow-consumer policy applies.
6. **Dispatch.** Handlers start on the receive loop once their message is processed, outside the link lock and in an empty execution context, as on the thread pool. One that completes synchronously costs no thread switch, and the answers to one message go out together in one message. Until its first `await`, a handler holds up its connection's next messages, as SignalR's do by default. Calls' `CancellationTokenSource`s are reused with `TryReset`, and the built-in delegates are static.
7. **Timers.** One shared timer keeps a priority queue of deadlines: handshakes, call deadlines, heartbeats, Ack delays and session expiry. No connection or call owns a timer.
8. **DI scopes** are created only when a handler reads `call.Services`.
9. **Native AOT.** No reflection. JSON goes through source-generated metadata, the AOT and trimming analyzers run on every build, and CI publishes the test server with `PublishAot`.

## Contract generation

The running server describes its own API as `contract.ts`, the file the browser client is typed against (see [PLAN-CLIENT.md](PLAN-CLIENT.md#the-contract)). No build-time tool is involved.

**Inputs.** Every `Map*` registration contributes its name and types to one section:

| Registration | Contract section |
|---|---|
| `MapRequest` | `api` |
| `MapStream` | `streams` |
| `MapUpload` | `uploads` |
| `MapEvent` | `events` |
| `MapClientRequest` | `clientApi` |
| `MapClientEvent` | `clientEvents` |

**Schemas.** `JsonSchemaExporter` describes each type from the same `JsonTypeInfo` the serializer uses. Naming policy, `[JsonPropertyName]`, ignore conditions, enum converters and number handling therefore come out exactly as they go over the wire.

- `TreatNullObliviousAsNonNullable` is on, so reference-type properties map to plain TypeScript types rather than `T | null`. An option emits `| null` instead.
- `TransformSchemaNode` records the .NET type of each schema node, which turns repeated types into named interfaces.

**Type mapping:**

| .NET | TypeScript |
|---|---|
| `bool` | `boolean` |
| `int`, `double`, `decimal` and the other numeric types | `number` |
| `long`, `ulong` | `number`, with a doc comment: exact only up to 2^53 |
| `string`, `char`, `Guid`, `Uri`, `DateTime`, `DateTimeOffset`, `DateOnly`, `TimeOnly`, `TimeSpan` | `string`, with the format in a doc comment |
| `T[]`, `List<T>`, other collections | `T[]` |
| `Dictionary<string, T>` | `Record<string, T>` |
| enum | a union of its numbers, or of its names with `JsonStringEnumConverter` |
| `Nullable<T>` | `T \| null` |
| class, struct, record | `interface` |
| `[JsonPolymorphic]` hierarchy | a union discriminated by `$type` |
| `JsonElement`, `JsonNode`, `object` | `unknown` |
| top-level `byte[]` or `ReadOnlyMemory<byte>` payload | `Uint8Array`, sent as a binary payload |
| `byte[]` inside an object | `string` (base64, as System.Text.Json writes it) |
| no payload | `void` |

A type the exporter cannot describe, such as one with a custom converter, becomes `unknown` with a comment. `options.Contract.MapType<Money>("string")` overrides it.

**Naming.** An interface takes the .NET type's name. Generic types are spelled out (`Page<Order>` becomes `PageOfOrder`). Clashing names gain namespace segments until they're unique. Sections, names and types are emitted in a fixed order, so the same API always produces the same file.

**Output:**

```ts
// Generated by PureGate.WebSocket from /ws. Contract 9f3a1c0b7d2e4a65.
export interface Order { id: number; total: number }
export interface OrderShipped { orderId: number; carrier: string }

export interface Contract {
  api: { orders: { get(request: number): Order } };
  streams: { prices: { watch(request: string): Tick } };
  uploads: { files: { put(request: NewFile, item: Uint8Array): SavedFile } };
  events: { orders: { shipped(event: OrderShipped): void } };
  clientApi: { ui: { confirm(request: Question): boolean } };
  clientEvents: { telemetry: { click(event: Click): void } };
}

export const contractHash = "9f3a1c0b7d2e4a65";
```

**Hash.** The first 16 hex digits of a SHA-256 over a canonical JSON form of the names and their schemas, with sorted keys. Named types appear by their TypeScript names, and only the keywords that shape the TypeScript count: types, properties, items, enums, unions and formats. The hash describes the API, not the TypeScript text, so an improved emitter in a new library version doesn't mark every client stale. Keywords such as `pattern` and `required`, which vary between .NET versions, stay out, so upgrading .NET doesn't either.

**Endpoint.** `MapContract` serves the file as `text/plain; charset=utf-8`, with the hash as its ETag (so a conditional request gets 304). It exposes the API's whole surface, so map it in development only, or put it behind authorization.

**In tests.** `endpoint.Contract` returns the TypeScript and the hash. An app's test can compare it with the committed `contract.ts` and fail in CI when someone forgot to pull it.

**Checking.** The server compares Hello's `contract` with its own hash, answers with `current` or `stale` in Welcome, and applies `ContractMismatch`.

## Security

- **Origin.** By default, upgrades are accepted only when the `Origin` header is absent (not a browser) or matches the request's host. That blocks cross-site WebSocket hijacking with cookies, and `AllowedOrigins` widens it. A refused upgrade gets 403.
- **Authentication.** Either at the upgrade, with the usual ASP.NET Core schemes and `RequireAuthorization()` on the endpoint (cookies work this way), or through Hello's `token` and `options.Authenticate` for apps that cannot use cookies. Tokens never go into the URL, where proxies and logs would keep them. An endpoint uses one way or the other.
- **Expiry and refresh.** A connection's authentication expires at the earlier of two times: the `ExpiresUtc` of the upgrade's authentication result, when the scheme sets one, and the expiry `Authenticate` returns. Welcome tells the client when.
  - A client with a token refreshes in place with an Authenticate frame. The new token must be for the same user, and the connection's authorization cache is cleared.
  - Without a refresh, the server closes with 4001 at expiry and keeps the session resumable. A cookie-based client that reconnects with renewed cookies loses nothing.
- **Resume** requires the same user id as the session's, so a leaked session id alone cannot take a session over.
- **Authorization per operation.** `RequireAuthorization(policies)` is evaluated with `IAuthorizationService` against the connection's user, and the result is cached per connection and policy. A failure answers `forbidden`.
- **Limits.** The defaults above bound memory, concurrency and queues for every connection, including ones still in the handshake.
- **Rate limiting.** An optional `RateLimiter` per connection, from `System.Threading.RateLimiting` in the shared framework. A request over the limit gets `overloaded`.
- **Errors.** Exception messages stay on the server unless `IncludeExceptionDetails` is on.
- **Compression** is off by default. When a secret and attacker-controlled text share a compressed message, the compressed size can leak the secret (the CRIME/BREACH class of attacks). Enable it only for endpoints where that cannot happen.
- **Hostile input.** Fuzz tests feed random and mutated frames, broken JSON, window and name-table abuse, oversized messages, and handshakes that never finish. The server must close with the right code and leak nothing.

## Deploys and several instances

- **Shutdown.** When the host starts stopping (`IHostApplicationLifetime.ApplicationStopping`), every connection gets GoAway.
  - The server stops sending new events and browser calls, keeps handling what arrives, and lets the exchanges in progress finish within `DrainTimeout`, bounded by the host's own shutdown timeout.
  - Then it closes with 1001, and `OnDisconnected` runs for every session.
- **Restarts end sessions.** Sessions are held in memory. After a deploy or restart, every client reconnects with a new session and reports `resumed: false`, so apps reload their state.
- **Resume across instances.** A resume has to reach the instance that holds the session. There are two ways:
  - the load balancer's cookie affinity, which works because browsers send cookies with the upgrade;
  - setting `ResumeRoute` to the instance's id, which the client adds to the reconnect URL as the `route` query parameter for the load balancer to route on.

  Without either, a resume that reaches another instance fails safely with `resumed: false`.
- **Groups and broadcasts** reach the connections on one instance. Reaching all instances needs a backplane, planned after the first release.

## Testing

| Project | Contents |
|---|---|
| `src/PureGate.WebSocket.Test` | NUnit tests. Not shipped. |
| `src/PureGate.WebSocket.TestServer` | An ASP.NET Core app with test operations: echo, delays, failures, streams, server → browser calls and events. The TypeScript integration tests run against it, and CI also publishes it with Native AOT. |

The tests talk to the server through an internal C# implementation of the protocol over `ClientWebSocket`, which lives only in the test project.

- **Unit:** varints, frames, name tables, error mapping, the TypeScript emitter's type mapping and naming.
- **Test vectors:** the shared `test-vectors/frames.json`. Every valid vector must round-trip, and every invalid one must be rejected.
- **Integration:** in-process Kestrel on `127.0.0.1:0`, covering every frame kind, window, error path, deadline, cancellation, stream and upload credit, the handshake with pipelined calls, token refresh and expiry, and GoAway during calls.
- **Contract snapshots:** expected `.ts` files for a set of test types, checked into the test project.
- **Chaos:** sockets aborted at random during calls, events and streams. With resume on, nothing may be lost or duplicated.
- **Fuzzing:** seeded, so every failure can be reproduced, and each one found becomes a regression test.
- **AOT:** CI publishes the TestServer with `PublishAot` and runs the TypeScript integration suite against the native binary.
- **Coverage:** a line coverage gate of 85%, as in PureGate.Socket.

The suite runs once per change. Repeated runs and benchmarks happen at the end of a phase.

## Benchmarks

`src/PureGate.WebSocket.Benchmarks` is a console load generator that drives the server with the internal C# client, and drives SignalR with its .NET client using the JSON and MessagePack protocols.

| Scenario | Measures |
|---|---|
| 10,000 idle connections | memory per connection |
| 1 KB echo, sequential, one connection | calls/s, p50, p99 |
| 1 KB echo, 64 callers on one connection | calls/s, p99 |
| 1,000 connections making steady calls | p50, p99, CPU |
| One event broadcast to 10,000 connections | time until the last connection has it |
| Any call | server allocations per call |

Benchmarks run only when you ask, with tiered compilation off (`TC=0`), no warmup, each run under 3 minutes, reporting the median of several runs. The results are in [BENCHMARKS.md](BENCHMARKS.md).

## Work by phase

| Phase | Server work |
|---|---|
| 1. Frames | `Protocol/`: varints, frame reader and writer, name tables, error codes; the test vector file and its tests. |
| 2. Calls and events | Upgrade, Hello and Welcome with pipelining, windows, Request/Response, deadlines, Cancel, events both ways, server → browser calls, Ping/Pong, the hub with groups and users, options, TestServer. |
| 3. Contract | Schema export, the TypeScript emitter, the hash, `MapContract`, `endpoint.Contract`, the mismatch policy, snapshot tests. |
| 4. Streams and uploads | `MapStream`, with credit that pulls the producer's `IAsyncEnumerable` only as fast as the browser reads; `MapUpload`, with credit granted as the handler reads; binary items, Cancel. |
| 5. Resume and deploys | Ack, slow-consumer policies, replay buffer, session store and expiry, resume, the user check, `ResumeRoute`, GoAway and draining, chaos tests. |
| 6. Security | Origin policy, token authentication, expiry and refresh, operation policies, rate limiting, filters, fuzzing. |
| 7. Performance | Meter and ActivitySource, `traceparent`, logging, the benchmarks against SignalR, allocation work. |
| 8. Release | NuGet README, samples, CI and the publish workflow. |
