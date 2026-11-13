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

export type ConnectionState = "connecting" | "open" | "closed";

/** What changed with a state, when it applies. */
export interface StateChange {
  /** Set when the state becomes "open": whether an earlier session carried on. */
  readonly resumed?: boolean;
  /** Set when the state becomes "closed": the WebSocket close code. */
  readonly code?: number;
  readonly reason?: string;
}

/** The calls of a connection with default options, from conn.with(options). */
export interface CallView<C extends ContractShape> {
  readonly api: ApiOf<Section<C, "api">>;
}

/** A connection to a PureGate.WebSocket endpoint, typed by its contract. */
export interface Connection<C extends ContractShape = ContractShape> {
  /** Operations on the server: conn.api.orders.get(42, { timeout }). */
  readonly api: ApiOf<Section<C, "api">>;
  /** Listens to the server's events: conn.on.orders.shipped(listener). */
  readonly on: OnOf<Section<C, "events">>;
  /** Sends events to the server: conn.emit.telemetry.click(event). */
  readonly emit: EmitOf<Section<C, "clientEvents">>;
  /** Answers the server's calls: conn.handle.ui.confirm(handler). One handler per name. */
  readonly handle: HandleOf<Section<C, "clientApi">>;
  readonly state: ConnectionState;
  /** Resolves when the connection first opens; rejects if it closes before that. */
  readonly opened: Promise<void>;
  /** The server's id for the connection, for logs. Empty until open. */
  readonly connectionId: string;
  /** The api with these options as defaults, such as one signal for everything a component starts. */
  with(options: CallOptions): CallView<C>;
  /** Returns a function that stops listening. */
  onStateChange(listener: (state: ConnectionState, change: StateChange) => void): () => void;
  /** Closes for good. Outstanding calls fail with connection_closed. */
  close(code?: number, reason?: string): Promise<void>;
}

export type WebSocketConstructor = new (url: string | URL, protocols?: string | string[]) => WebSocket;

export interface ConnectOptions {
  /** The contract's hash, from the generated contract.ts. A development server warns when it is stale. */
  readonly contractHash?: string;
  /** A credential for servers that authenticate by token, called for every connect. */
  readonly token?: () => string | Promise<string>;
  /** Default milliseconds a call waits. 30,000. */
  readonly timeout?: number;
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
  /** Another WebSocket implementation, for tests or unusual environments. */
  readonly WebSocket?: WebSocketConstructor;
}
