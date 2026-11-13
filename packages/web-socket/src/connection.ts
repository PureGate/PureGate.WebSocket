import type {
  CallOptions,
  CallView,
  Connection,
  ConnectionState,
  ConnectOptions,
  ContractShape,
  StateChange,
  StreamOptions,
  UploadOptions,
  WebSocketConstructor,
} from "./contract.ts";
import { Deadlines } from "./deadlines.ts";
import { CallError, connectionClosed } from "./errors.ts";
import { encodePayload, type Payload } from "./payload.ts";
import { pathProxy } from "./proxy.ts";
import {
  type Handler,
  type PendingCall,
  type QueuedEvent,
  reconnectingCode,
  Session,
  type SessionOwner,
} from "./session.ts";
import { Download, type Exchange, isSource, type StreamQueue, Upload } from "./streams.ts";

const defaultDelays: readonly number[] = [0, 1_000, 2_000, 5_000, 10_000, 30_000];

// Close codes after which the client does not reconnect (docs/PROTOCOL.md, Closing).
const finalCodes = new Set([1000, 1003, 1009, 4000, 4004, 4005, 4007]);
const unauthorizedCode = 4001;

/**
 * Connects to a PureGate.WebSocket endpoint and returns the connection at once; it opens in the background, and calls
 * made before then wait. A relative url resolves against the page's location. A dropped connection comes back on its
 * own, resuming its session when the server still has it.
 */
export function connect<C extends ContractShape>(url: string | URL, options: ConnectOptions = {}): Connection<C> {
  return new ConnectionCore(url, options) as unknown as Connection<C>;
}

// The connection the application holds. It queues what the application starts, hands it to the current session
// when there is room, and brings the connection back when it drops. After GoAway, the old session finishes what it
// has in progress next to the new one.
class ConnectionCore {
  readonly api: unknown;
  readonly streams: unknown;
  readonly uploads: unknown;
  readonly on: unknown;
  readonly emit: unknown;
  readonly handle: unknown;
  readonly opened: Promise<void>;

  readonly #url: string;
  readonly #options: ConnectOptions;
  readonly #owner: SessionOwner;
  readonly #streamQueue: StreamQueue;
  readonly #callQueue: PendingCall[] = [];
  readonly #eventQueue: QueuedEvent[] = [];
  readonly #streamsWaiting: Exchange[] = [];
  readonly #listeners = new Map<string, Set<(event: unknown) => void>>();
  readonly #handlers = new Map<string, Handler>();
  readonly #stateListeners = new Set<(state: ConnectionState, change: StateChange) => void>();
  readonly #deadlines = new Deadlines<PendingCall>((call) => this.#expire(call));
  readonly #draining = new Set<Session>();
  readonly #resolveOpened: () => void;
  readonly #rejectOpened: (error: unknown) => void;
  readonly #closed: Promise<void>;
  readonly #resolveClosed: () => void;

  #session: Session;
  #state: ConnectionState = "connecting";
  #closing = false;
  #attempt = 0;
  #reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  /** In the back/forward cache: reconnecting waits for pageshow. */
  #suspended = false;
  #authRetried = false;
  #stopWatching: (() => void) | undefined;

  constructor(url: string | URL, options: ConnectOptions) {
    this.#options = options;
    this.api = pathProxy((path, args) => this.#call(path, args[0], args[1] as CallOptions | undefined));
    this.streams = pathProxy((path, args) => this.#stream(path, args[0], args[1] as StreamOptions | undefined));
    this.uploads = pathProxy((path, args) => this.#upload(path, args));
    this.on = pathProxy((path, args) => this.#listen(path, args[0] as (event: unknown) => void));
    this.emit = pathProxy((path, args) => this.#emit(path, args[0]));
    this.handle = pathProxy((path, args) => this.#register(path, args[0] as Handler));

    const opened = Promise.withResolvers<void>();
    this.opened = opened.promise;
    this.#resolveOpened = opened.resolve;
    this.#rejectOpened = opened.reject;
    // Nobody has to wait for opened: a rejection nobody sees is not an error.
    this.opened.catch(() => undefined);

    const closed = Promise.withResolvers<void>();
    this.#closed = closed.promise;
    this.#resolveClosed = closed.resolve;

    const WebSocketType: WebSocketConstructor | undefined = options.WebSocket ?? globalThis.WebSocket;
    if (WebSocketType === undefined) throw new TypeError("This environment has no WebSocket: pass options.WebSocket.");

    this.#url = resolveUrl(url);
    this.#owner = {
      options,
      WebSocket: WebSocketType,
      handler: (name) => this.#handlers.get(name),
      hasListeners: (name) => (this.#listeners.get(name)?.size ?? 0) > 0,
      dispatch: (name, event) => this.#dispatch(name, event),
      settle: (call, error, value) => this.#settle(call, error, value),
      reportError: (error) => this.#reportError(error),
      drain: () => this.#drain(),
      welcomed: (session, resumed, contract) => this.#welcomed(session, resumed, contract),
      dropped: (session, code, reason) => this.#dropped(session, code, reason),
      goAway: (session) => this.#goAway(session),
    };
    this.#streamQueue = {
      format: options.format,
      open: (stream) => this.#openStream(stream),
      dequeue: (stream) => removeFrom(this.#streamsWaiting, stream),
    };

    this.#session = new Session(this.#owner);
    this.#session.connect(this.#url);
    this.#watchPage();
  }

  get state(): ConnectionState {
    return this.#state;
  }

  get connectionId(): string {
    return this.#session.connectionId;
  }

  with(defaults: CallOptions): CallView<ContractShape> {
    return {
      api: pathProxy((path, args) => this.#call(path, args[0], merge(defaults, args[1] as CallOptions | undefined))),
      streams: pathProxy((path, args) =>
        this.#stream(path, args[0], mergeStream(defaults, args[1] as StreamOptions | undefined)),
      ),
    } as CallView<ContractShape>;
  }

  onStateChange(listener: (state: ConnectionState, change: StateChange) => void): () => void {
    this.#stateListeners.add(listener);
    return () => this.#stateListeners.delete(listener);
  }

  close(code = 1000, reason = ""): Promise<void> {
    if (this.#state === "closed") return this.#closed;

    this.#closing = true;
    clearTimeout(this.#reconnectTimer);
    for (const session of this.#draining) session.close(code, reason);
    if (this.#session.connected) this.#session.close(code, reason);
    else this.#finish(code, reason);
    return this.#closed;
  }

  // The sessions' side.

  #welcomed(session: Session, resumed: boolean, contract: string | undefined): void {
    if (session !== this.#session) return;

    this.#attempt = 0;
    this.#authRetried = false;
    if (contract === "stale") {
      console.warn("PureGate.WebSocket: the server's contract has changed. Pull contract.ts again.");
    }

    const first = this.#state === "connecting";
    this.#state = "open";
    if (first) this.#resolveOpened();
    this.#notify("open", { resumed });
    this.#drain();
  }

  // A session's WebSocket closed. The close code decides whether the connection comes back (docs/PROTOCOL.md,
  // Closing). After 4001 it tries once more, resuming with a fresh token or the browser's current cookies.
  #dropped(session: Session, code: number, reason: string): void {
    if (session !== this.#session) {
      // A session that drained after GoAway: what it still had in progress is lost.
      this.#draining.delete(session);
      session.fail(connectionClosed());
      return;
    }

    if (this.#state === "closed") return;
    const unauthorized = code === unauthorizedCode;
    if (
      this.#closing ||
      this.#options.reconnect === false ||
      finalCodes.has(code) ||
      (unauthorized && this.#authRetried)
    ) {
      this.#finish(code, reason);
      return;
    }

    if (unauthorized) this.#authRetried = true;
    if (!session.resumable) {
      session.fail(connectionClosed());
      this.#session = new Session(this.#owner);
    }

    if (this.#state === "open") {
      this.#state = "reconnecting";
      this.#notify("reconnecting", { code, reason });
    }

    this.#schedule(unauthorized ? 0 : undefined);
  }

  // The server is stopping: a new session starts at once, on a new connection, while the old one finishes.
  #goAway(session: Session): void {
    if (session !== this.#session || this.#state === "closed") return;

    this.#draining.add(session);
    this.#session = new Session(this.#owner);
    this.#state = "reconnecting";
    this.#notify("reconnecting", {});
    this.#session.connect(this.#url);
  }

  // Reconnecting, with the backoff delays and jitter of ConnectOptions.reconnect.

  #schedule(delay: number | undefined): void {
    if (this.#suspended) return;

    const reconnect = this.#options.reconnect === false ? {} : (this.#options.reconnect ?? {});
    const delays = reconnect.delays !== undefined && reconnect.delays.length > 0 ? reconnect.delays : defaultDelays;
    const jitter = reconnect.jitter ?? 0.2;
    const base = delay ?? delays[Math.min(this.#attempt, delays.length - 1)] ?? 0;
    this.#attempt++;
    const wait = Math.max(0, base * (1 + (Math.random() * 2 - 1) * jitter));
    clearTimeout(this.#reconnectTimer);
    this.#reconnectTimer = setTimeout(() => this.#reconnectNow(), wait);
  }

  #reconnectNow(): void {
    clearTimeout(this.#reconnectTimer);
    this.#reconnectTimer = undefined;
    if (this.#state === "closed" || this.#state === "open" || this.#suspended || this.#session.connected) return;
    this.#session.connect(this.#endpoint());
  }

  // Behind a load balancer, the session's route brings a resume back to the instance that has it.
  #endpoint(): string {
    const route = this.#session.route;
    if (route === undefined || !this.#session.resumable) return this.#url;
    const url = new URL(this.#url);
    url.searchParams.set("route", route);
    return url.href;
  }

  // The page: try at once when the network or the tab comes back, and leave the back/forward cache usable.
  #watchPage(): void {
    const target = globalThis as Partial<EventTarget>;
    if (typeof target.addEventListener !== "function" || typeof target.removeEventListener !== "function") return;

    const add = target.addEventListener.bind(target);
    const remove = target.removeEventListener.bind(target);
    const now = () => this.#reconnectNow();
    add("online", now);

    // Workers have no page lifecycle.
    const page = (globalThis as { document?: Document }).document;
    if (page === undefined) {
      this.#stopWatching = () => remove("online", now);
      return;
    }

    const visible = () => {
      if (page.visibilityState === "visible") this.#reconnectNow();
    };
    const hide = (event: Event) => {
      if ((event as PageTransitionEvent).persisted) this.#suspend();
    };
    const show = (event: Event) => {
      if (!(event as PageTransitionEvent).persisted) return;
      this.#suspended = false;
      this.#reconnectNow();
    };

    page.addEventListener("visibilitychange", visible);
    add("pagehide", hide);
    add("pageshow", show);
    this.#stopWatching = () => {
      remove("online", now);
      page.removeEventListener("visibilitychange", visible);
      remove("pagehide", hide);
      remove("pageshow", show);
    };
  }

  // Into the back/forward cache: an open WebSocket would keep the page out of it. The session stays for pageshow.
  #suspend(): void {
    if (this.#state === "closed") return;
    this.#suspended = true;
    clearTimeout(this.#reconnectTimer);
    this.#session.close(reconnectingCode, "The page is in the back/forward cache.");
  }

  // Ending for good.

  #finish(code: number, reason: string): void {
    if (this.#state === "closed") return;
    this.#state = "closed";
    clearTimeout(this.#reconnectTimer);
    this.#stopWatching?.();

    const error = connectionClosed();
    this.#session.fail(error);
    for (const session of this.#draining) session.fail(error);
    this.#draining.clear();
    for (const call of this.#callQueue.splice(0)) this.#settle(call, error);
    this.#eventQueue.length = 0;
    for (const stream of this.#streamsWaiting.splice(0)) stream.closed(error);
    this.#deadlines.clear();

    this.#rejectOpened(error);
    this.#notify("closed", { code, reason });
    this.#resolveClosed();
  }

  // What waits goes to the current session as its windows allow.
  #drain(): void {
    const session = this.#session;
    while (this.#callQueue.length > 0 && session.canSendCall()) {
      const call = this.#callQueue[0] as PendingCall;
      if (!call.settled && !session.sendCall(call)) break;
      this.#callQueue.shift();
    }

    while (this.#eventQueue.length > 0 && session.canSendEvent()) {
      if (!session.sendEvent(this.#eventQueue[0] as QueuedEvent, false)) break;
      this.#eventQueue.shift();
    }

    while (this.#streamsWaiting.length > 0 && session.canOpenStream()) {
      if (!session.openStream(this.#streamsWaiting[0] as Exchange)) break;
      this.#streamsWaiting.shift();
    }
  }

  // Calls to the server.

  #call(name: string, request: unknown, options: CallOptions | undefined): Promise<unknown> {
    if (this.#state === "closed") return Promise.reject(connectionClosed());

    let payload: Payload;
    try {
      payload = encodePayload(request, this.#options.format);
    } catch (error) {
      return Promise.reject(error);
    }

    const signal = options?.signal;
    if (signal?.aborted) return Promise.reject(new CallError("cancelled", "The call was cancelled."));

    const { promise, resolve, reject } = Promise.withResolvers<unknown>();
    const call: PendingCall = {
      name,
      payload,
      deadline: performance.now() + (options?.timeout ?? this.#options.timeout ?? 30_000),
      resolve,
      reject,
      signal,
      id: 0,
      session: undefined,
      settled: false,
      onAbort: undefined,
    };

    if (signal !== undefined) {
      call.onAbort = () => this.#abort(call);
      signal.addEventListener("abort", call.onAbort, { once: true });
    }

    this.#deadlines.add(call);
    if (this.#callQueue.length === 0 && this.#session.canSendCall() && this.#session.sendCall(call)) {
      // Sent.
    } else if (this.#callQueue.length >= (this.#options.maxQueued ?? 256)) {
      this.#settle(call, new CallError("overloaded", "Too many calls are waiting for the connection."));
    } else {
      this.#callQueue.push(call);
    }

    return promise;
  }

  #expire(call: PendingCall): void {
    // The server has the same deadline and stops the handler itself; the id stays taken until its Response.
    if (call.settled) return;
    if (call.id === 0) removeFrom(this.#callQueue, call);
    this.#settle(call, new CallError("timeout", "The call's deadline passed."));
  }

  #abort(call: PendingCall): void {
    if (call.settled) return;
    if (call.id === 0) removeFrom(this.#callQueue, call);
    else call.session?.cancel(call.id);
    this.#settle(call, new CallError("cancelled", "The call was cancelled."));
  }

  #settle(call: PendingCall, error: unknown, value?: unknown): void {
    if (call.settled) return;
    call.settled = true;
    this.#deadlines.delete(call);
    if (call.onAbort !== undefined) call.signal?.removeEventListener("abort", call.onAbort);
    if (error === undefined) call.resolve(value);
    else call.reject(error);
  }

  // Streams in both directions.

  #stream(name: string, request: unknown, options: StreamOptions | undefined): Download<unknown> {
    const buffer = options?.buffer ?? this.#options.streamBuffer ?? 16;
    if (!(Number.isInteger(buffer) && buffer >= 1))
      throw new RangeError("A stream's buffer is a whole number, at least 1.");
    return new Download(this.#streamQueue, name, encodePayload(request, this.#options.format), buffer, options?.signal);
  }

  // put(request, source, options?) or put(source, options?): a second argument that is a source makes the first one
  // the request.
  #upload(name: string, args: unknown[]): Promise<unknown> {
    const hasRequest = args.length >= 2 && isSource(args[1]);
    const source = hasRequest ? args[1] : args[0];
    const options = (hasRequest ? args[2] : args[1]) as UploadOptions | undefined;
    if (!isSource(source)) {
      return Promise.reject(new TypeError(`Upload ${name} from an AsyncIterable, an Iterable or a ReadableStream.`));
    }

    let payload: Payload;
    try {
      payload = encodePayload(hasRequest ? args[0] : undefined, this.#options.format);
    } catch (error) {
      return Promise.reject(error);
    }

    return new Upload(this.#streamQueue, name, payload, source, options?.signal, options?.timeout).promise;
  }

  #openStream(stream: Exchange): void {
    if (this.#state === "closed") {
      stream.closed(connectionClosed());
    } else if (this.#streamsWaiting.length === 0 && this.#session.canOpenStream() && this.#session.openStream(stream)) {
      // Opened.
    } else if (this.#streamsWaiting.length >= (this.#options.maxQueued ?? 256)) {
      stream.refused(new CallError("overloaded", "Too many streams are waiting for the connection."));
    } else {
      this.#streamsWaiting.push(stream);
    }
  }

  // Events in both directions.

  #listen(name: string, listener: (event: unknown) => void): () => void {
    if (typeof listener !== "function") throw new TypeError(`Listen to ${name} with a function.`);
    let listeners = this.#listeners.get(name);
    if (listeners === undefined) {
      listeners = new Set();
      this.#listeners.set(name, listeners);
    }

    listeners.add(listener);
    return () => {
      listeners.delete(listener);
      if (listeners.size === 0 && this.#listeners.get(name) === listeners) this.#listeners.delete(name);
    };
  }

  #dispatch(name: string, event: unknown): void {
    const listeners = this.#listeners.get(name);
    if (listeners === undefined) return;
    for (const listener of Array.from(listeners)) {
      try {
        listener(event);
      } catch (error) {
        this.#reportError(error);
      }
    }
  }

  #emit(name: string, event: unknown): void {
    if (this.#state === "closed") throw connectionClosed();

    const payload = encodePayload(event, this.#options.format);
    if (
      this.#eventQueue.length === 0 &&
      this.#session.canSendEvent() &&
      this.#session.sendEvent({ name, payload }, true)
    ) {
      // Sent.
    } else if (this.#eventQueue.length >= (this.#options.maxQueued ?? 256)) {
      throw new CallError("overloaded", "Too many events are waiting for the connection.");
    } else {
      this.#eventQueue.push({ name, payload });
    }
  }

  // Calls from the server, answered by the handlers registered with handle.

  #register(name: string, handler: Handler): () => void {
    if (typeof handler !== "function") throw new TypeError(`Handle ${name} with a function.`);
    if (this.#handlers.has(name)) throw new Error(`${name} already has a handler. Unregister it first.`);

    this.#handlers.set(name, handler);
    return () => {
      if (this.#handlers.get(name) === handler) this.#handlers.delete(name);
    };
  }

  #notify(state: ConnectionState, change: StateChange): void {
    for (const listener of Array.from(this.#stateListeners)) {
      try {
        listener(state, change);
      } catch (error) {
        this.#reportError(error);
      }
    }
  }

  #reportError(error: unknown): void {
    if (this.#options.onError !== undefined) this.#options.onError(error);
    else console.error(error);
  }
}

function merge(defaults: CallOptions, options: CallOptions | undefined): CallOptions {
  if (options === undefined) return defaults;
  const timeout = options.timeout ?? defaults.timeout;
  const signal = either(defaults.signal, options.signal);
  return {
    ...(timeout === undefined ? {} : { timeout }),
    ...(signal === undefined ? {} : { signal }),
  };
}

// A stream takes the view's signal; it has no deadline.
function mergeStream(defaults: CallOptions, options: StreamOptions | undefined): StreamOptions {
  const signal = either(defaults.signal, options?.signal);
  return { ...options, ...(signal === undefined ? {} : { signal }) };
}

/** A signal that aborts when either does. */
function either(first: AbortSignal | undefined, second: AbortSignal | undefined): AbortSignal | undefined {
  if (first === undefined) return second;
  if (second === undefined) return first;
  return AbortSignal.any([first, second]);
}

function removeFrom<T>(items: T[], item: T): void {
  const index = items.indexOf(item);
  if (index >= 0) items.splice(index, 1);
}

function resolveUrl(url: string | URL): string {
  const base = (globalThis as { location?: { href: string } }).location?.href;
  const resolved = new URL(url, base);
  if (resolved.protocol === "http:") resolved.protocol = "ws:";
  else if (resolved.protocol === "https:") resolved.protocol = "wss:";
  return resolved.href;
}
