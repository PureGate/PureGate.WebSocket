# PureGate.WebSocket

Typed calls, streams and events between ASP.NET Core and browsers over one WebSocket.

| Package | |
|---|---|
| [`PureGate.WebSocket`](src/PureGate.WebSocket/README.md) on NuGet | The server, for ASP.NET Core. |
| [`@puregate/web-socket`](packages/web-socket/README.md) on npm | The browser client, in TypeScript, with no runtime dependencies. |

You map operations on the server; the server generates `contract.ts` from your own types; the browser uses it, and every call is checked by the TypeScript compiler:

```csharp
var ws = app.MapPureGateWebSocket("/ws");
ws.MapRequest<int, Order>("orders.get", static (id, call, cancellationToken) => FindOrderAsync(id, cancellationToken));
ws.MapContract("/ws/contract.ts");
```

```ts
const conn = connect<Contract>("/ws", { contractHash });
const order = await conn.api.orders.get(42);
```

Beyond calls: server → browser calls, events to users and groups, streams and uploads with flow control, sessions that resume after a dropped connection without losing or repeating anything, GoAway for deploys, token authentication with refresh, authorization per operation, rate limiting, metrics and tracing, and Native AOT.

## Documentation

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
