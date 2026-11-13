# PureGate.WebSocket plan

PureGate.WebSocket connects browsers to ASP.NET Core over one WebSocket: typed calls and events in both directions, and streams both ways. Its TypeScript client has no dependencies, and the server needs nothing beyond ASP.NET Core. This plan covers both halves and the order to build them in.

| Document | Covers |
|---|---|
| [PROTOCOL.md](PROTOCOL.md) | The wire format both sides implement. |
| [PLAN-SERVER.md](PLAN-SERVER.md) | The .NET package `PureGate.WebSocket`. |
| [PLAN-CLIENT.md](PLAN-CLIENT.md) | The npm package `@puregate/web-socket`. |

## Goals

1. **Typed end to end, with no code generator.** The browser client is generic over one contract type. The running server can write that type out from its own registrations, and the type can also be written by hand.
2. **Calls that behave like function calls.** Deadlines travel to the server, cancelling a call cancels its handler, and failures carry error codes and details.
3. **Fast where it counts on the web.** The first call travels with the handshake. Frames are small and coalesced into one message when ready together, and every name crosses the wire once per connection. Broadcasts are serialized once, and the server spends less memory and CPU per connection and per message.
4. **Built for real networks.** Reconnect with backoff, resume without losing or repeating messages, deploys that let calls in progress finish, flow control for calls and streams, slow-client handling, and liveness checks that survive background tabs.
5. **Safe on the public internet.** Origin checks on by default, limits sized for untrusted clients, authentication at the upgrade or in the handshake that is refreshed in place before it expires, and authorization per operation.
6. **Native AOT on the server.** No reflection; JSON through source-generated metadata.

## Non-goals

- **Fallback transports** such as Server-Sent Events or long polling. WebSocket only.
- **Clients other than browsers.** The client also runs on Node 22 and later, which the tests use, but there is no .NET or mobile client.
- **Scale-out across servers** in the first release. Groups and broadcasts reach the connections on one server. A backplane comes after the first release, as a separate package; a Redis one would bring its own dependency. Several instances still work: a resume reaches the right one through cookie affinity or the `route` parameter.
- **Built-in codecs other than JSON.** Raw bytes work everywhere; MessagePack and similar are out.
- **Request batching.** Pipelining and coalescing already make separate calls cheap.

## How it fits together

```
  Browser                                          ASP.NET Core
  ┌───────────────────────────────┐                ┌──────────────────────────────────────┐
  │ app code                      │                │ app.MapPureGateWebSocket("/ws")      │
  │   conn.api.orders.get(42)     │                │   MapRequest, MapStream, MapUpload,  │
  │   conn.on.orders.shipped(fn)  │                │   MapEvent, MapClientRequest, …      │
  │                               │                │ IWebSocketHub: all, user, group      │
  │ @puregate/web-socket          │     wss://     │ PureGate.WebSocket                   │
  │   frames ⇄ WebSocket          │ ◄────────────► │   frames ⇄ WebSocket (Kestrel)       │
  └───────────────────────────────┘  puregate.v1   └──────────────────────────────────────┘
        ▲ contract.ts, committed                       │ GET /ws/contract.ts (development)
        └──────────────────────────────────────────────┘
```

## Key decisions

| Decision | Why |
|---|---|
| A binary frame header with a JSON payload | Browsers parse JSON natively and fast, while a JavaScript MessagePack decoder is often no faster. The header costs a few bytes. |
| One WebSocket message carries one or more frames | Frames ready together go in one `send()`, and the browser dispatches one `message` event for all of them. |
| Names defined on first use, numbered after that | A name crosses the wire once per connection. |
| The version is the subprotocol `puregate.v1` | A mismatch fails at the upgrade, without an extra message. |
| The first calls ride with Hello | Connecting and the first call share one round trip. |
| Responses return call credit | Flow control for calls costs no extra frames. |
| Frames counted, acknowledged in batches | One mechanism serves both resume and slow-consumer detection. The browser API cannot slow the network, so acknowledgements are the only reliable signal of how far behind a page is. |
| WebSocket close codes instead of a Close frame | WebSocket already has them. |
| Contract leaves are always method signatures, events included | A group can never be mistaken for a payload object, so every section can be walked as a typed path. |
| Every section is a typed path: `conn.api.orders.get(42)`, `conn.on.orders.shipped(fn)`, `conn.emit…`, `conn.handle…` | One style for the whole client, no names written as strings, and the editor links each call site to the contract. |
| The contract is described by the running server, using `JsonSchemaExporter` | The types match exactly what the serializer sends, with no build tool to maintain. |
| The contract hash covers the canonical schemas, not the TypeScript text | Improving the emitter doesn't mark every client stale. |
| Events and browser calls are declared as static descriptors | Send sites are typed and never repeat a name. |
| Tokens go in Hello, never in the URL | URLs end up in proxy and server logs. |
| Tokens are refreshed in place with an Authenticate frame | Short-lived tokens don't force a reconnect. |
| The client opens every stream; an Upload flag makes it the producer | Ids stay in one space, and the same frames carry downloads and uploads. |
| GoAway before a server shuts down | During a deploy, calls in progress finish while new calls already go to another instance. |
| 32 concurrent calls per connection by default | SignalR runs one call at a time per client by default. |
| Compression off by default | Compressing secrets together with attacker-controlled text can leak them through message sizes. |
| Resume on by default, with a 30-second window | Short drops are common on mobile networks and sleeping laptops, and resuming hides them from the app. |

## Repository layout

```
docs/                                  this plan and the protocol
src/PureGate.WebSocket/                the .NET package
src/PureGate.WebSocket.Test/           .NET tests (phase 1)
src/PureGate.WebSocket.TestServer/     test app for the TypeScript integration suite (phase 2)
src/PureGate.WebSocket.Benchmarks/     load generator against SignalR (phase 7)
packages/web-socket/                   @puregate/web-socket
test-vectors/frames.json               byte-level vectors both test suites check (phase 1)
.github/workflows/                     CI from phase 1, publishing in phase 8
```

## Phases

Each phase delivers both halves together, with tests on both sides, and ends when its exit criteria pass in CI.

| Phase | Delivers | Done when |
|---|---|---|
| 1. Frames | Varints, frame reader and writer, name tables and error codes on both sides; the shared test vectors; CI for both halves. | Both suites pass every vector, including rejecting every malformed one. |
| 2. Calls and events | Handshake with pipelined calls, windows, requests and responses, deadlines, cancellation, events both ways, server → browser calls, heartbeat, the hub with users and groups; the TestServer and the integration suite. | The integration suite passes on Ubuntu and Windows, in Node and in Chromium, Firefox and WebKit, and against the Native AOT build of the TestServer. |
| 3. Contract | Schema export, the TypeScript emitter, the hash and its check on both sides; the contract types and mapped types. | The TestServer's generated `contract.ts` compiles, the type tests pass against it, and its hash is the same on every run and machine. |
| 4. Streams and uploads | Download streams and uploads with item credit, binary items, cancellation. | In both directions, a producer stops within one buffer of a consumer that stops reading, and cancelling ends the handler. |
| 5. Reconnect, resume and deploys | Acknowledgements, slow-consumer policies, replay buffers, resume, the `route` for load balancers, GoAway and draining, reconnect backoff, page lifecycle hooks. | A chaos suite that aborts sockets during calls, events and streams loses and repeats nothing, and a server stopped mid-traffic finishes its calls in progress. |
| 6. Security and limits | Origin policy, token authentication with expiry and refresh, policies per operation, rate limiting, filters, fuzzing on both decoders. | Seeded fuzzing finds nothing, and every limit and refusal path has a test. |
| 7. Observability and performance | Metrics, tracing with `traceparent`, logging; the benchmarks against SignalR; the client size budget. | The benchmark results are recorded, and the size budget is enforced in CI. |
| 8. Release | READMEs for NuGet and npm, samples, publish workflows. | You publish the first version. |

## Quality bar

- **Zero warnings.** .NET builds treat warnings as errors and run the AOT analyzers. TypeScript compiles under `strict` plus the extra checks in `tsconfig.json`. Biome is clean.
- **Tests.** Every feature is tested on both sides, and over a real socket, not just against mocks.
- **Coverage.** Line coverage stays at 85% or more, on both sides.
- **Docs.** Every public API has XML docs (.NET) or TSDoc (TypeScript).
- **Performance.** Hot paths are designed not to allocate, and no performance claim goes in without a benchmark behind it.
- **Cadence.** The suite runs once per change. Repeated runs and benchmarks wait for the end of a phase.
- **Naming.** The product is always "PureGate.WebSocket" in prose.

## Dependencies

Neither package has runtime dependencies beyond ASP.NET Core on the server and the browser on the client. These development dependencies are approved; anything else needs your approval first.

| Where | Packages | For |
|---|---|---|
| Repository root | `@biomejs/biome` | Formatting and linting |
| `packages/web-socket` | `typescript`, `vitest`, `vite`, `@types/node` | Building, unit tests, integration tests |
| `packages/web-socket` | `@vitest/browser-playwright`, `playwright` | Tests in real Chromium, Firefox and WebKit |
| `packages/web-socket` | `@vitest/coverage-v8` | TypeScript coverage |
| `src/PureGate.WebSocket.Test` | NUnit, NUnit3TestAdapter, Microsoft.NET.Test.Sdk, coverlet.collector | .NET tests and coverage |
| `src/PureGate.WebSocket.Benchmarks` | Microsoft.AspNetCore.SignalR.Client, Microsoft.AspNetCore.SignalR.Protocols.MessagePack | Benchmarks against SignalR |

Each package is added in the phase that first needs it, always at its latest stable release: npm's `latest` tag, NuGet's newest non-preview version. Versions are looked up at the time, never taken from memory, and updated to the latest when they fall behind. Packages that must match each other move together, such as Vitest and its browser and coverage plugins.
