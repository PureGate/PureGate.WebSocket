/**
 * The shape of every contract: up to six sections of method signatures, grouped in objects to any depth. A path is a
 * name: api.orders.get is the operation "orders.get". See docs/PLAN-CLIENT.md, The contract.
 */
export interface ContractShape {
  /** Operations the browser calls. */
  readonly api?: object;
  /** Streams the browser reads from the server. */
  readonly streams?: object;
  /** Streams the browser sends to the server. */
  readonly uploads?: object;
  /** Events the server sends. */
  readonly events?: object;
  /** Operations the server calls, which the browser answers. */
  readonly clientApi?: object;
  /** Events the browser sends. */
  readonly clientEvents?: object;
}

/** Options for one call. */
export interface CallOptions {
  /** Milliseconds to wait for the result. The server gets the same deadline. */
  readonly timeout?: number;
  /** Aborting it cancels the call, on the server too. */
  readonly signal?: AbortSignal;
}

/** Options for one stream. */
export interface StreamOptions {
  /** Aborting it cancels the stream, on the server too, and the iterator throws cancelled. */
  readonly signal?: AbortSignal;
  /** Items that may wait in the browser before the server must stop. ConnectOptions.streamBuffer by default. */
  readonly buffer?: number;
}

/** Options for one upload. Uploads have no deadline unless they set one. */
export interface UploadOptions {
  /** Milliseconds until the upload is cancelled. */
  readonly timeout?: number;
  /** Aborting it cancels the upload, on the server too. */
  readonly signal?: AbortSignal;
}

/**
 * Items from the server, read with for await. The stream opens on the first read. Leaving the loop early, return()
 * and cancel() stop it on the server too; a failed stream throws its CallError once the items before it are read.
 */
export interface CallStream<T> extends AsyncIterableIterator<T, undefined> {
  /** Stops the stream, as leaving a for await loop does. */
  return(): Promise<IteratorResult<T, undefined>>;
  /** Stops the stream. The next read finishes the iteration. */
  cancel(): void;
}

/**
 * What an upload reads its items from, only as fast as the server takes them. A ReadableStream's byte chunks are split
 * to fit the server's messages, as file.stream() needs; an iterable's items go as they are.
 */
export type UploadSource<T> = AsyncIterable<T> | Iterable<T> | ReadableStream<T>;

/** What a browser-side handler gets besides the payload. */
export interface HandlerContext {
  /** Aborts when the server cancels the call, its deadline passes, or the connection closes. */
  readonly signal: AbortSignal;
}

type Empty = Record<never, never>;

type Section<C, K extends keyof ContractShape> = K extends keyof C ? NonNullable<C[K]> : Empty;

/** Without a request, the options still need a place: api.ping() or api.ping(undefined, { timeout }). */
export interface NoRequest<R> {
  (): Promise<R>;
  (request: undefined, options: CallOptions): Promise<R>;
}

/** conn.api.orders.get(request, options?), from the api section. */
export type ApiOf<G> = {
  readonly [K in keyof G]: G[K] extends () => infer R
    ? NoRequest<Awaited<R>>
    : G[K] extends (request: infer Q) => infer R
      ? (request: Q, options?: CallOptions) => Promise<Awaited<R>>
      : ApiOf<G[K]>;
};

/** Without a request, the options still need a place: streams.prices.all() or streams.prices.all(undefined, { buffer }). */
export interface NoRequestStream<T> {
  (): CallStream<T>;
  (request: undefined, options: StreamOptions): CallStream<T>;
}

/** conn.streams.prices.watch(request, options?), from the streams section. The leaf's return type is the item type. */
export type StreamOf<G> = {
  readonly [K in keyof G]: G[K] extends () => infer T
    ? NoRequestStream<Awaited<T>>
    : G[K] extends (request: infer Q) => infer T
      ? (request: Q, options?: StreamOptions) => CallStream<Awaited<T>>
      : StreamOf<G[K]>;
};

/**
 * conn.uploads.files.put(request, source, options?), or put(source, options?) without a request, from the uploads
 * section. The leaf's last parameter is the item type. The one-parameter form is checked first: a two-parameter leaf
 * doesn't match it.
 */
export type UploadOf<G> = {
  readonly [K in keyof G]: G[K] extends (item: infer T) => infer R
    ? (source: UploadSource<T>, options?: UploadOptions) => Promise<Awaited<R>>
    : G[K] extends (request: infer Q, item: infer T) => infer R
      ? (request: Q, source: UploadSource<T>, options?: UploadOptions) => Promise<Awaited<R>>
      : UploadOf<G[K]>;
};

/** conn.on.orders.shipped(listener), from the events section. Returns a function that stops listening. */
export type OnOf<G> = {
  readonly [K in keyof G]: G[K] extends (event: infer E) => void
    ? (listener: (event: E) => void) => () => void
    : OnOf<G[K]>;
};

/** conn.emit.telemetry.click(event), from the clientEvents section. */
export type EmitOf<G> = {
  readonly [K in keyof G]: G[K] extends (event: infer E) => void ? (event: E) => void : EmitOf<G[K]>;
};

/** conn.handle.ui.confirm(handler), from the clientApi section. Returns a function that unregisters the handler. */
export type HandleOf<G> = {
  readonly [K in keyof G]: G[K] extends (request: infer Q) => infer R
    ? (handler: (request: Q, context: HandlerContext) => Awaited<R> | Promise<Awaited<R>>) => () => void
    : HandleOf<G[K]>;
};

/**
 * "connecting" until the first Welcome, "open" while connected, "reconnecting" after the connection dropped or the
 * server went away, and "closed" for good.
 */
export type ConnectionState = "connecting" | "open" | "reconnecting" | "closed";

/** What changed with a state, when it applies. */
export interface StateChange {
  /**
   * Set when the state becomes "open": whether the session carried on. After false, calls, streams and events in
   * progress failed or may be lost, so reload what the page shows.
   */
  readonly resumed?: boolean;
  /** Set when the state becomes "reconnecting" or "closed": the WebSocket close code. */
  readonly code?: number;
  readonly reason?: string;
}

/** How the connection comes back after a drop. */
export interface ReconnectOptions {
  /** Milliseconds before each attempt; the last one repeats. [0, 1000, 2000, 5000, 10000, 30000]. */
  readonly delays?: readonly number[];
  /** How much each delay varies at random, as a fraction of it, so clients don't all come back at once. 0.2. */
  readonly jitter?: number;
}

/** The calls and streams of a connection with default options, from conn.with(options). */
export interface CallView<C extends ContractShape> {
  readonly api: ApiOf<Section<C, "api">>;
  /** Streams take the view's signal. */
  readonly streams: StreamOf<Section<C, "streams">>;
}

/** A connection to a PureGate.WebSocket endpoint, typed by its contract. */
export interface Connection<C extends ContractShape = ContractShape> {
  /** Operations on the server: conn.api.orders.get(42, { timeout }). */
  readonly api: ApiOf<Section<C, "api">>;
  /** Streams from the server: for await (const tick of conn.streams.prices.watch("MSFT")). */
  readonly streams: StreamOf<Section<C, "streams">>;
  /** Streams to the server: await conn.uploads.files.put(request, file.stream()). */
  readonly uploads: UploadOf<Section<C, "uploads">>;
  /** Listens to the server's events: conn.on.orders.shipped(listener). */
  readonly on: OnOf<Section<C, "events">>;
  /** Sends events to the server: conn.emit.telemetry.click(event). */
  readonly emit: EmitOf<Section<C, "clientEvents">>;
  /** Answers the server's calls: conn.handle.ui.confirm(handler). One handler per name. */
  readonly handle: HandleOf<Section<C, "clientApi">>;
  readonly state: ConnectionState;
  /** Resolves when the connection first opens; rejects if it closes for good before that. */
  readonly opened: Promise<void>;
  /** The server's id for the connection, for logs. Empty until open. */
  readonly connectionId: string;
  /** The api with these options as defaults, such as one signal for everything a component starts. */
  with(options: CallOptions): CallView<C>;
  /** Returns a function that stops listening. */
  onStateChange(listener: (state: ConnectionState, change: StateChange) => void): () => void;
  /** Closes for good, without reconnecting. Outstanding calls fail with connection_closed. */
  close(code?: number, reason?: string): Promise<void>;
}

export type WebSocketConstructor = new (url: string | URL, protocols?: string | string[]) => WebSocket;

export interface ConnectOptions {
  /** The contract's hash, from the generated contract.ts. A development server warns when it is stale. */
  readonly contractHash?: string;
  /** A credential for servers that authenticate by token, called for every connect and reconnect. */
  readonly token?: () => string | Promise<string>;
  /** Default milliseconds a call waits. 30,000. */
  readonly timeout?: number;
  /** Items a stream may have waiting in the browser before the server must stop. 16. */
  readonly streamBuffer?: number;
  /** Calls and events that may wait for the connection or a window. 256. */
  readonly maxQueued?: number;
  /** The largest message the browser accepts, in bytes, at least 16 KiB. 16 MiB. */
  readonly maxMessage?: number;
  /** How many server calls the browser handles at once. 16. */
  readonly calls?: number;
  /** A W3C traceparent for each call and event. */
  readonly traceparent?: () => string | undefined;
  /** Receives exceptions from listeners and handlers. console.error by default. */
  readonly onError?: (error: unknown) => void;
  /** How the connection comes back after a drop, or false to close for good instead. */
  readonly reconnect?: false | ReconnectOptions;
  /** Another WebSocket implementation, for tests or unusual environments. */
  readonly WebSocket?: WebSocketConstructor;
}
