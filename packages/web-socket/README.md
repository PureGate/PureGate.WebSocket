# @puregate/web-socket

The browser client of [PureGate.WebSocket](https://www.nuget.org/packages/PureGate.WebSocket): typed calls, streams and events with an ASP.NET Core server over one WebSocket. No runtime dependencies, about 12 KB minified and gzipped.

```sh
npm install @puregate/web-socket
```

The types come from the server. Its `MapContract` endpoint serves `contract.ts`, generated from the server's own operations and types; commit it next to your code and pull it again when the server changes.

```ts
import { CallError, connect } from "@puregate/web-socket";
import { type Contract, contractHash } from "./contract.ts";

const conn = connect<Contract>("/ws", { contractHash });

// Calls, with the server's types for the request and the result.
const order = await conn.api.orders.get(42, { timeout: 5_000 });

// Streams from the server, read as fast as you take them.
for await (const tick of conn.streams.prices.watch("MSFT")) render(tick);

// Uploads, read from the source only as fast as the server takes them.
const saved = await conn.uploads.files.put({ name: file.name }, file.stream());

// Events both ways, and the server's calls into the browser.
const stop = conn.on.orders.shipped((shipped) => toast(`Order ${shipped.id} shipped`));
conn.emit.telemetry.click({ x: 10, y: 20 });
conn.handle.ui.confirm((question) => window.confirm(question.text));

try {
  await conn.api.orders.get(7);
} catch (error) {
  if (error instanceof CallError && error.code === "not_found") showMissing();
}
```

Every name is a typed path, never a string: a typo, a wrong argument or a wrong use of a result is a compile error, and go to definition leads from a call to the contract.

## Connecting

`connect` returns at once. Calls made before the connection opens wait, and the first ones go out with the handshake, so the first call costs no extra round trip. `conn.opened` resolves once it is open.

| Option | Default | |
|---|---|---|
| `contractHash` | none | From `contract.ts`. A development server warns when the browser's copy is stale. |
| `token` | none | `() => string \| Promise<string>`, for servers that authenticate by token. Called for every connect, and to refresh before the authentication expires. |
| `timeout` | 30 s | A call's default deadline, which the server gets too. |
| `streamBuffer` | 16 | Items a stream may have waiting in the browser. |
| `reconnect` | `{ delays: [0, 1000, 2000, 5000, 10000, 30000], jitter: 0.2 }` | The last delay repeats; `false` closes for good instead. |
| `maxQueued` | 256 | Calls, streams and events that may wait for the connection. |
| `traceparent` | none | `() => string \| undefined`, a W3C traceparent for each call, stream and event. |
| `onError` | `console.error` | Receives exceptions from listeners and handlers. |

`conn.with({ signal, timeout })` gives `api` and `streams` with those defaults, for example one `AbortSignal` for everything a component starts.

## When the connection drops

The state becomes `"reconnecting"`, and the client comes back with the backoff above, at once when the browser goes back online or the tab becomes visible. When the server still has the session, it resumes: calls, streams and events in progress carry on, and nothing is lost or repeated. When it doesn't, `onStateChange` reports `resumed: false`, what was in progress fails with `connection_closed`, and the page should reload what it shows:

```ts
conn.onStateChange((state, change) => {
  if (state === "open" && change.resumed === false) reload();
});
```

Calls made while reconnecting wait and go out once the connection is back. A server that is stopping sends GoAway: the client moves to a new connection at once, while the old one finishes its calls. Pages entering the back/forward cache close their connection and resume it when they come back.

## Errors

A failed call rejects with `CallError`, whose `code` is the server's own, such as `not_found`, or one of the protocol's: `timeout`, `cancelled`, `unauthorized`, `forbidden`, `unknown_operation`, `invalid_payload`, `payload_too_large`, `overloaded`, `handler_failed`, `connection_closed`. `details` holds the server's JSON details, if it sent any.

## Environments

Evergreen browsers and web workers. It needs only a global `WebSocket`, so Node 22 and later run it too.

## More

- [The protocol](https://github.com/PureGate/PureGate.WebSocket/blob/main/docs/PROTOCOL.md)
- [The client's design](https://github.com/PureGate/PureGate.WebSocket/blob/main/docs/PLAN-CLIENT.md)
- [The chat sample](https://github.com/PureGate/PureGate.WebSocket/tree/main/samples/chat)

MIT licensed.
