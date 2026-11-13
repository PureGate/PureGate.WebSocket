# @puregate/web-socket client plan

The npm package `@puregate/web-socket` is the browser side of [PROTOCOL.md](PROTOCOL.md). It is written in TypeScript, ships as ES modules with type declarations, and has no runtime dependencies. The phases are in [PLAN.md](PLAN.md); the server side is in [PLAN-SERVER.md](PLAN-SERVER.md).

## Target usage

```ts
import { CallError, connect } from "@puregate/web-socket";
import { type Contract, contractHash } from "./api/contract";

const conn = connect<Contract>("/ws", { contractHash });

const order = await conn.api.orders.get(42);
const quick = await conn.api.orders.get(42, { timeout: 2_000, signal: controller.signal });

for await (const tick of conn.streams.prices.watch("MSFT")) {
  render(tick);
}

const saved = await conn.uploads.files.put({ name: file.name }, file.stream());

const off = conn.on.orders.shipped((shipped) => toast(`Order ${shipped.orderId} shipped`));
conn.emit.telemetry.click({ x: 10, y: 20 });
conn.handle.ui.confirm((question) => window.confirm(question.text));

conn.onStateChange((state, change) => {
  if (state === "open" && !change.resumed) reloadPage();
});

try {
  await conn.api.orders.get(7);
} catch (error) {
  if (error instanceof CallError && error.code === "not_found") showMissing();
}

await conn.close();
```

Every section is reached the same way, a typed path and then a call, with no names written as strings. A typo in a path, a wrong argument or a wrong use of the result is a compile error. Paths autocomplete, and go to definition and find all references lead from a call site to the contract and back.

## The contract

The client is generic over one contract type. It comes from the server's `contract.ts` endpoint ([PLAN-SERVER.md](PLAN-SERVER.md#contract-generation)), pulled and committed:

```
curl http://localhost:5000/ws/contract.ts -o src/api/contract.ts
```

Or it is written by hand; it's the same type either way:

```ts
interface Order { id: number; total: number }
interface OrderShipped { orderId: number; carrier: string }

export interface Contract {
  api: { orders: { get(id: number): Order } };
  events: { orders: { shipped(event: OrderShipped): void } };
}
```

Rules:

- There are six sections, each optional: `api` (the browser calls), `streams` (the browser reads from the server), `uploads` (the browser streams to the server), `events` (the server sends), `clientApi` (the server calls), and `clientEvents` (the browser sends).
- Every leaf is a method signature. Usually it has zero or one parameter, the payload. An upload's last parameter is its item type, after an optional request: `put(request: NewFile, item: Uint8Array): SavedFile`. The return type is the response for `api`, `uploads` and `clientApi`, the item type for `streams`, and `void` for events. Writing events as methods too is what keeps a group apart from a payload object.
- Objects group leaves, to any depth, and the path is the name: `api.orders.get` is `orders.get` on the wire.
- Name segments follow the protocol: `[A-Za-z_][A-Za-z0-9_]*`.

## Public API

### `connect<C>(url, options?)`

Returns a `Connection<C>` at once and starts connecting. A relative URL resolves against the page's location, with `http` becoming `ws` and `https` becoming `wss`.

| Member | Purpose |
|---|---|
| `api` | `conn.api.orders.get(request, options?)` returns `Promise<Response>`. |
| `streams` | `conn.streams.prices.watch(request, options?)` returns a `CallStream<Item>`: an `AsyncIterable` with `cancel()`. |
| `uploads` | `conn.uploads.files.put(request, source, options?)` sends a stream and returns `Promise<Response>`. `source` is an `AsyncIterable`, an `Iterable` or a `ReadableStream`. Without a request it's `put(source, options?)`: when the second argument is a source, the first is the request. |
| `on` | `conn.on.orders.shipped(listener)` listens to a server event. Returns a function that stops listening. |
| `emit` | `conn.emit.telemetry.click(payload)` sends an event to the server. |
| `handle` | `conn.handle.ui.confirm(handler)` answers a server → browser call. The handler is `(payload, { signal }) => Response \| Promise<Response>`. Returns a function that unregisters it. A name has one handler: registering a second throws, so unregister the first to replace it. |
| `with(options)` | A view whose `api` and `streams` use these call options, for example one `AbortSignal` for everything a component starts. |
| `state` | `"connecting"`, `"open"`, `"reconnecting"` or `"closed"`. |
| `opened` | A promise that resolves when the connection first opens, and rejects if it closes before that. |
| `onStateChange(listener)` | `(state, change) => void`, where `change` holds `resumed`, `code` and `reason` when they apply. Returns a function that stops listening. |
| `connectionId` | The server's id for the connection, for logs. Empty until open. |
| `close(code?, reason?)` | Closes for good: no reconnect, and outstanding calls fail with `connection_closed`. |

### Options

| Option | Default | Notes |
|---|---|---|
| `contractHash` | none | Sent in Hello, so a development server can say the contract is stale. |
| `token` | none | `() => string \| Promise<string>`, called for every connect and reconnect, and to refresh before the authentication expires. For apps that can't use cookies. |
| `format` | JSON | A payload format the server also has, such as MessagePack: `{ name, encode, decode }`. |
| `timeout` | 30 s | Default deadline for calls. |
| `streamBuffer` | 16 | Items a stream may have waiting before the server must stop. |
| `reconnect` | delays `[0, 1_000, 2_000, 5_000, 10_000, 30_000]` ms, jitter 20% | The last delay repeats. `false` turns reconnecting off. |
| `maxQueued` | 256 | Calls and events waiting for the connection or for a window. Past it, a call rejects with `overloaded` and `emit` throws. |
| `maxMessage` | 16 MiB | The largest message accepted, sent in Hello. |
| `calls` | 16 | Server → browser calls handled at once, sent in Hello. |
| `traceparent` | none | `() => string \| undefined`, attached to calls, streams and events. |
| `onError` | `console.error` | Receives exceptions thrown by listeners and handlers. |
| `WebSocket` | `globalThis.WebSocket` | Another implementation, for tests or unusual environments. |

Call options are `{ timeout?, signal? }`. Stream options are `{ signal?, buffer? }`.

### Errors

`CallError extends Error` has `code`, `message` and `details`. `code` is one of the protocol's error names (`timeout`, `cancelled`, `unauthorized`, `forbidden`, `unknown_operation`, `invalid_payload`, `payload_too_large`, `overloaded`, `handler_failed`, `connection_closed`, `protocol_error`) or an application code such as `not_found`. Its type is the union of the known names plus `string`, so the known names still autocomplete. `details` holds the server's JSON details, or `undefined`.

## Typing

The library turns the contract into its API with mapped types. The sketch:

```ts
// conn.api.orders.get(request, options?), from the api section.
type ApiOf<G> = {
  readonly [K in keyof G]: G[K] extends () => infer R
    ? NoRequest<Awaited<R>>
    : G[K] extends (request: infer Q) => infer R
      ? (request: Q, options?: CallOptions) => Promise<Awaited<R>>
      : ApiOf<G[K]>;
};

// Without a request, options still need a slot: api.ping() or api.ping(undefined, { timeout }).
interface NoRequest<R> {
  (): Promise<R>;
  (request: undefined, options: CallOptions): Promise<R>;
}

// conn.uploads.files.put(request, source, options?), from the uploads section.
// The one-parameter form is checked first: a two-parameter leaf doesn't match it.
type UploadOf<G> = {
  readonly [K in keyof G]: G[K] extends (item: infer T) => infer R
    ? (source: Source<T>, options?: CallOptions) => Promise<Awaited<R>>
    : G[K] extends (request: infer Q, item: infer T) => infer R
      ? (request: Q, source: Source<T>, options?: CallOptions) => Promise<Awaited<R>>
      : UploadOf<G[K]>;
};
type Source<T> = AsyncIterable<T> | Iterable<T> | ReadableStream<T>;

// conn.on.orders.shipped(listener), from the events section.
type OnOf<G> = {
  readonly [K in keyof G]: G[K] extends (event: infer E) => void
    ? (listener: (event: E) => void) => () => void
    : OnOf<G[K]>;
};

// conn.emit.telemetry.click(event), from the clientEvents section.
type EmitOf<G> = {
  readonly [K in keyof G]: G[K] extends (event: infer E) => void ? (event: E) => void : EmitOf<G[K]>;
};

// conn.handle.ui.confirm(handler), from the clientApi section.
type HandleOf<G> = {
  readonly [K in keyof G]: G[K] extends (request: infer Q) => infer R
    ? (handler: (request: Q, context: { signal: AbortSignal }) => R | Promise<R>) => () => void
    : HandleOf<G[K]>;
};
```

At runtime one `Proxy` implementation serves all six sections. It collects the property path, and invoking the path makes the call, opens the stream, starts the upload, subscribes, emits or registers, depending on the property it started from. The library never needs to see the contract, and proxies are cached per path.

The mapped types are homomorphic (`[K in keyof G]`), so the editor links each path to its declaration: go to definition on `conn.on.orders.shipped` lands on `shipped` in the contract, and find all references on that member finds every listener.

## Behaviour

**Connecting.** The first message carries Hello and every call made so far, within the protocol's initial windows, so the first call costs no extra round trip. Until the connection opens, calls wait in order, each still bound by its own timeout.

**Calls.**
- **Payloads.** A `Uint8Array` or `ArrayBuffer` goes as a binary payload, `undefined` as an empty one, and anything else through `JSON.stringify`. Replies decode the same way in reverse: binary to `Uint8Array`, empty to `undefined`, JSON through `JSON.parse`.
- **Timeouts.** A call's timeout travels to the server as its deadline. Locally, the call rejects with `timeout` once it passes.
- **Aborting.** Aborting the signal rejects the call at once with `cancelled` and sends Cancel. The id stays reserved until the server's Response arrives.
- **Windows.** Calls beyond the server's window wait locally, in order, and never reach the server's memory.

**Streams.** A stream opens on the first `next()`, so a stream that is never read costs nothing. Its credit is the buffer size, and half the buffer is granted again each time the application takes that many items. `break`, `return`, `cancel()` and an aborted signal send Cancel. A failed StreamEnd is thrown from the iterator as `CallError`.

**Uploads.** The client reads from the source only as the server grants credit, so a large file is never read ahead of the server's handler. When the source ends, the client sends StreamEnd; when the source throws, a failed StreamEnd. Aborting the signal sends Cancel. The promise settles with the server's Response, and a Response that arrives before the source ends stops the reading. An upload has no deadline unless its options set one, since the default call timeout would cut large files off. A `ReadableStream`'s byte chunks are split to fit the server's message limit, since the browser chooses those chunks and a `Blob`'s can be the whole file. Items from an iterable are the application's own and go as they are.

**Events.** Listeners run synchronously, in arrival order. A listener that throws is reported to `onError` and does not stop the others. Events nobody listens to are dropped, and still acknowledged. `emit` queues while the connection isn't open or the event window is full.

**Server → browser calls.** A call for a name without a handler is answered with `unknown_operation`. The handler's `signal` aborts on Cancel or when the session ends. A thrown `CallError` keeps its code; anything else becomes `handler_failed` with the error's message.

**Reconnect and resume.**
- **When dropped,** the state becomes `reconnecting`, and the client retries with the backoff delays. It also tries at once on the `online` event and when the page becomes visible. The close code decides whether to reconnect at all (see the protocol's [Closing](PROTOCOL.md#closing) table). After 4001 it retries once and resumes: with a fresh token if it has a token provider, otherwise with the browser's current cookies.
- **While disconnected,** new calls and events wait in the queue. The server has never seen them, so they go out after the next Welcome, whether or not the session resumed.
- **Behind a load balancer,** the reconnect URL carries Welcome's `route`, when there was one, as the `route` query parameter.
- **Resumed:** outstanding calls, streams and events carry on, and nothing is lost or repeated.
- **Not resumed:** outstanding calls fail with `connection_closed`, streams end with it, and events sent but not yet acknowledged may be lost. `onStateChange` reports `resumed: false`, so the app can reload its state. Listeners and handlers stay registered.

**Refreshing authentication.** When Welcome gives an `expires` and the client has a token provider, it fetches a fresh token and sends Authenticate one minute before expiry, or halfway through when less than two minutes remain. Calls keep flowing during the refresh. Without a token provider (cookies), there's nothing to refresh: the server closes with 4001 at expiry, and the client reconnects with the browser's current cookies and resumes.

**Server shutdown.** On GoAway, the client opens a new connection at once and sends new calls and events there, while the old connection finishes the exchanges in progress. The new connection has a new session, so `onStateChange` reports `resumed: false`.

**Page lifecycle.** On `pagehide` into the back/forward cache, the client closes with 4006 (`reconnecting`; browsers cannot send 1001 themselves) and keeps the session; on `pageshow` it resumes. That keeps the page eligible for the cache. Background tabs run timers late. Liveness compares timestamps rather than counting timer ticks, so a late timer never looks like a dead connection.

**Environments.** Evergreen browsers. The client uses only features that are Baseline 2024 or older, such as `Promise.withResolvers` and `AbortSignal.any`. It also works in web workers, where the page lifecycle hooks are skipped. Node 22 and later, which have a global `WebSocket`, run it too; that's what the tests use, and not a goal beyond that.

## Internals

```
packages/web-socket/
  src/
    index.ts          public exports only
    connection.ts     connect(), state machine, handshake, pipelining
    contract.ts       contract shape and the mapped types
    errors.ts         CallError, error codes
    protocol/         varint, frame reader, frame writer, kinds and flags, name tables
    session/          windows, pending calls, Acks, replay buffer, resume
    calls/            the api proxy, streams, uploads, events, server → browser handlers
    runtime/          reconnect backoff, the deadline timer, page lifecycle
  test/               Vitest suites, with their own tsconfig.json adding Node types
```

Library code sees only DOM types. The `test/tsconfig.json` extends the package's config and adds Node types for the suites and their setup.

## Performance design

1. **Receiving.** `binaryType` is `"arraybuffer"`. Frames are parsed with a `Uint8Array` and a hand-written varint decoder, with no `DataView` per frame.
2. **Payloads without copies.** JSON is decoded with one shared `TextDecoder` over a `subarray`, then `JSON.parse`. Binary payloads are handed out as views of the message's buffer, which is never reused.
3. **Names.** A name is decoded once, when it's defined. A reference is an array lookup.
4. **Sending.** One growable `Uint8Array` per connection, and JSON written into it with `TextEncoder.encodeInto`. Frames queued in the same task are flushed in one microtask as one message, with one `send()`. `send()` copies the bytes, so the buffer is reused at once; a test confirms that in Chromium, Firefox and WebKit.
5. **Deadlines.** Pending calls sit in a `Map` by id. One timer serves every deadline, set for the nearest one.
6. **Size.** The target is at most 6 KB minified and gzipped for the whole package. CI measures it with a Vite library build, since Vite is already a dev dependency.

## Packaging

- ES modules only, `"type": "module"`, an `exports` map with `types`, and `"sideEffects": false`.
- No runtime dependencies. The approved dev dependencies are listed in [PLAN.md](PLAN.md#dependencies).
- Ships `dist` and `src`, with declaration maps and source maps, so "go to definition" lands on the TypeScript source.
- `prepack` builds, so `yarn pack` and `yarn npm publish` always ship fresh output.
- Every public export has a TSDoc comment.
- You set the version by hand.

## Testing

- **Unit:** varints, frame reading and writing, name tables, payload encoding, proxy paths, windows, Acks. Backoff and deadlines run on Vitest's fake timers.
- **Test vectors:** `test-vectors/frames.json`, the same file the .NET tests read.
- **Types:** `expectTypeOf` assertions in the test files, so `yarn typecheck` fails if the mapped types drift: wrong names, payloads, results or event types.
- **Integration:** a Vitest global setup, written in TypeScript, builds and starts the .NET TestServer and passes its URL to the suites. The suites connect with Node's built-in `WebSocket` and cover every feature against the real server, including uploads, token refresh and GoAway. In CI they also run against the Native AOT build of the TestServer.
- **Real browsers:** Vitest's browser mode runs the unit and integration suites in Chromium, Firefox and WebKit through Playwright, against the same TestServer, which allows the test page's origin. That's where engine differences show up, such as `send()` copying and how background timers behave.
- **Coverage:** `@vitest/coverage-v8` measures the Node runs, with the same 85% line gate as the .NET side.
- **Fuzzing:** seeded random and mutated messages. The client must close with 4000 and never let an exception escape the socket handlers.
- **Benchmarks:** `vitest bench` for encode and decode throughput; Vitest ships the benchmark runner.

## Work by phase

| Phase | Client work |
|---|---|
| 1. Frames | `protocol/`: varints, frame reader and writer, name tables, error codes; the test vector suite. |
| 2. Calls and events | `connect`, Hello and Welcome with pipelining, windows, the `api`, `on`, `emit` and `handle` proxies, deadlines, Cancel, Ping/Pong, states, the integration setup against the TestServer in Node and in real browsers. |
| 3. Contract | The contract types and mapped types, type tests against the TestServer's generated `contract.ts`, the stale-contract warning. |
| 4. Streams and uploads | `streams` proxy, `CallStream` with credit, the `uploads` proxy reading its source as credit arrives, binary items, Cancel. |
| 5. Resume and deploys | Acks, replay buffer, resume, backoff, the `route` parameter, the GoAway handover, the `online`, visibility and back/forward cache hooks, chaos tests against the TestServer. |
| 6. Security | Token provider, refresh before expiry, the 4001 retry, the fuzzing suite. |
| 7. Performance | Size budget in CI, encode and decode benchmarks, profiling of the hot paths. |
| 8. Release | npm README, an example, CI and the publish workflow. |
