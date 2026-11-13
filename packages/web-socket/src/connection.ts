import type {
  CallOptions,
  CallView,
  Connection,
  ConnectionState,
  ConnectOptions,
  ContractShape,
  HandlerContext,
  StateChange,
  StreamOptions,
  UploadOptions,
  WebSocketConstructor,
} from "./contract.ts";
import { Deadlines } from "./deadlines.ts";
import { CallError, connectionClosed } from "./errors.ts";
import { decodePayload, encodePayload, type Payload } from "./payload.ts";
import { errorCodeNames, errorCodeOf, isValidCustomCode } from "./protocol/errors.ts";
import { createFrame, type Frame, FrameFlags, FrameKind, nameHasText, resetFrame } from "./protocol/frames.ts";
import { IncomingNames, nameTableLimit, OutgoingNames } from "./protocol/names.ts";
import { readFrames } from "./protocol/reader.ts";
import { ProtocolError } from "./protocol/violation.ts";
import { FrameWriter, frameLength } from "./protocol/writer.ts";
import { pathProxy } from "./proxy.ts";
import { Download, type Exchange, isSource, type StreamHost, Upload } from "./streams.ts";

const subprotocol = "puregate.v1";
const protocolError = 4000;

// Before Welcome, the minimums every server advertises (docs/PROTOCOL.md, Handshake).
const initialCalls = 4;
const initialEvents = 16;
const initialStreams = 1;
const minimumMessage = 16 * 1024;

// The longest name code, to measure a frame before its name is numbered.
const longestNameCode = 2 * nameTableLimit + 1;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

interface PendingCall {
  readonly name: string;
  readonly payload: Payload;
  readonly deadline: number;
  readonly resolve: (value: unknown) => void;
  readonly reject: (error: unknown) => void;
  readonly signal: AbortSignal | undefined;
  /** 0 until the Request is sent. */
  id: number;
  settled: boolean;
  onAbort: (() => void) | undefined;
}

interface QueuedEvent {
  readonly name: string;
  readonly payload: Payload;
}

interface ServerCall {
  readonly controller: AbortController;
  /** Why the browser stopped waiting for its own handler, if it did. */
  reason: "" | "cancelled" | "timeout";
}

type Handler = (request: unknown, context: HandlerContext) => unknown;

interface Welcome {
  connection: string;
  calls: number;
  streams: number;
  events: number;
  maxMessage: number;
  heartbeat: number;
  contract?: string;
}

/**
 * Connects to a PureGate.WebSocket endpoint and returns the connection at once; it opens in the background, and calls
 * made before then wait. A relative url resolves against the page's location.
 */
export function connect<C extends ContractShape>(url: string | URL, options: ConnectOptions = {}): Connection<C> {
  return new ConnectionCore(url, options) as unknown as Connection<C>;
}

class ConnectionCore {
  readonly api: unknown;
  readonly streams: unknown;
  readonly uploads: unknown;
  readonly on: unknown;
  readonly emit: unknown;
  readonly handle: unknown;
  readonly opened: Promise<void>;

  readonly #options: ConnectOptions;
  readonly #socket: WebSocket;
  readonly #writer = new FrameWriter();
  readonly #scratch = createFrame(FrameKind.Ping);
  readonly #incoming = new IncomingNames();
  readonly #outgoing = new OutgoingNames();
  readonly #pending = new Map<number, PendingCall>();
  readonly #callQueue: PendingCall[] = [];
  readonly #eventQueue: QueuedEvent[] = [];
  readonly #listeners = new Map<string, Set<(event: unknown) => void>>();
  readonly #handlers = new Map<string, Handler>();
  readonly #serverCalls = new Map<number, ServerCall>();
  readonly #downloads = new Map<number, Download<unknown>>();
  readonly #uploads = new Map<number, Upload>();
  readonly #streamQueue: Exchange[] = [];
  readonly #streamHost: StreamHost;
  readonly #stateListeners = new Set<(state: ConnectionState, change: StateChange) => void>();
  readonly #deadlines = new Deadlines<PendingCall>((call) => this.#expire(call));
  readonly #resolveOpened: () => void;
  readonly #rejectOpened: (error: unknown) => void;
  readonly #closed: Promise<void>;
  readonly #resolveClosed: () => void;

  #state: ConnectionState = "connecting";
  #connectionId = "";
  #helloSent = false;
  #welcomed = false;
  #callWindow = initialCalls;
  #eventWindow = initialEvents;
  #streamWindow = initialStreams;
  #openStreams = 0;
  #serverMaxMessage = minimumMessage;
  #inFlight = 0;
  #uncreditedEvents = 0;
  #lastId = 0;
  #flushQueued = false;
  #heartbeat = 0;
  #heartbeatTimer: ReturnType<typeof setTimeout> | undefined;
  #lastReceived = performance.now();
  #pingSent = false;

  constructor(url: string | URL, options: ConnectOptions) {
    this.#options = options;
    this.api = pathProxy((path, args) => this.#call(path, args[0], args[1] as CallOptions | undefined));
    this.streams = pathProxy((path, args) => this.#stream(path, args[0], args[1] as StreamOptions | undefined));
    this.uploads = pathProxy((path, args) => this.#upload(path, args));
    this.on = pathProxy((path, args) => this.#listen(path, args[0] as (event: unknown) => void));
    this.emit = pathProxy((path, args) => this.#emit(path, args[0]));
    this.handle = pathProxy((path, args) => this.#register(path, args[0] as Handler));
    this.#streamHost = {
      open: (stream) => this.#openStream(stream),
      dequeue: (stream) => removeFrom(this.#streamQueue, stream),
      credit: (id, amount) => this.#writeCredit(id, amount),
      cancel: (id) => this.#writeCancel(id),
      item: (id, payload) => this.#writeItem(id, payload),
      end: (id, error) => this.#writeEnd(id, error),
      release: (id) => this.#releaseUpload(id),
    };

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

    this.#socket = new WebSocketType(resolveUrl(url), subprotocol);
    this.#socket.binaryType = "arraybuffer";
    this.#socket.onopen = () => void this.#onOpen();
    this.#socket.onmessage = (event: MessageEvent) => this.#onMessage(event);
    this.#socket.onclose = (event: CloseEvent) => this.#onClose(event.code, event.reason);
  }

  get state(): ConnectionState {
    return this.#state;
  }

  get connectionId(): string {
    return this.#connectionId;
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
    if (this.#state !== "closed" && this.#socket.readyState < 2) this.#socket.close(code, reason);
    return this.#closed;
  }

  // Opening: Hello first, then everything made while connecting, within the initial windows.

  async #onOpen(): Promise<void> {
    if (this.#socket.protocol !== subprotocol) {
      this.#socket.close(1002, "The server did not select puregate.v1.");
      return;
    }

    let token: string | undefined;
    try {
      token = await this.#options.token?.();
    } catch (error) {
      this.#reportError(error);
      this.#socket.close(1000, "No token.");
      return;
    }

    if (this.#state === "closed") return;
    const hello: Record<string, unknown> = {
      calls: this.#options.calls ?? 16,
      maxMessage: Math.max(minimumMessage, this.#options.maxMessage ?? 16 * 1024 * 1024),
    };
    if (token !== undefined) hello.token = token;
    if (this.#options.contractHash !== undefined) hello.contract = this.#options.contractHash;

    const frame = this.#frame(FrameKind.Hello);
    frame.payload = encoder.encode(JSON.stringify(hello));
    this.#write(frame);
    this.#helloSent = true;
    this.#drainCalls();
    this.#drainEvents();
    this.#drainStreams();
  }

  #onMessage(event: MessageEvent): void {
    this.#lastReceived = performance.now();
    this.#pingSent = false;
    try {
      readFrames(new Uint8Array(event.data as ArrayBuffer), "server", (frame) => this.#onFrame(frame));
    } catch (error) {
      this.#fail(error);
    }
  }

  #onFrame(frame: Frame): void {
    if (!this.#welcomed && frame.kind !== FrameKind.Welcome) throw new ProtocolError("unexpected_frame");

    switch (frame.kind) {
      case FrameKind.Welcome:
        this.#onWelcome(frame);
        break;
      case FrameKind.Response:
        this.#onResponse(frame);
        break;
      case FrameKind.Event:
        this.#onEvent(frame);
        break;
      case FrameKind.Request:
        this.#onServerCall(frame);
        break;
      case FrameKind.Cancel:
        this.#onServerCancel(frame.id);
        break;
      case FrameKind.StreamItem:
        this.#onStreamItem(frame);
        break;
      case FrameKind.StreamEnd:
        this.#onStreamEnd(frame);
        break;
      case FrameKind.Credit:
        this.#onCredit(frame);
        break;
      case FrameKind.Ping:
        this.#write(this.#frame(FrameKind.Pong));
        break;
      case FrameKind.Pong:
      case FrameKind.Ack:
      case FrameKind.GoAway:
        break;
      default:
        throw new ProtocolError("unexpected_frame");
    }
  }

  #onWelcome(frame: Frame): void {
    if (this.#welcomed) throw new ProtocolError("unexpected_frame");

    let welcome: Welcome;
    try {
      welcome = JSON.parse(decoder.decode(frame.payload)) as Welcome;
    } catch {
      throw new ProtocolError("invalid_handshake");
    }

    if (
      !(
        welcome.calls >= initialCalls &&
        welcome.streams >= initialStreams &&
        welcome.events >= initialEvents &&
        welcome.maxMessage >= minimumMessage
      )
    ) {
      throw new ProtocolError("invalid_handshake");
    }

    this.#welcomed = true;
    this.#connectionId = String(welcome.connection ?? "");
    this.#callWindow = welcome.calls;
    this.#streamWindow = welcome.streams;
    this.#eventWindow = welcome.events;
    this.#serverMaxMessage = welcome.maxMessage;
    if (welcome.contract === "stale") {
      console.warn("PureGate.WebSocket: the server's contract has changed. Pull contract.ts again.");
    }

    this.#state = "open";
    this.#resolveOpened();
    this.#notify("open", { resumed: false });
    this.#startHeartbeat(welcome.heartbeat > 0 ? welcome.heartbeat : 15_000);
    this.#drainCalls();
    this.#drainEvents();
    this.#drainStreams();
  }

  // Calls to the server.

  #call(name: string, request: unknown, options: CallOptions | undefined): Promise<unknown> {
    if (this.#state === "closed") return Promise.reject(connectionClosed());

    let payload: Payload;
    try {
      payload = encodePayload(request);
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
      settled: false,
      onAbort: undefined,
    };

    if (signal !== undefined) {
      call.onAbort = () => this.#abort(call);
      signal.addEventListener("abort", call.onAbort, { once: true });
    }

    this.#deadlines.add(call);
    if (this.#callQueue.length === 0 && this.#canSendCall() && this.#sendCall(call)) {
      // Sent.
    } else if (this.#callQueue.length >= (this.#options.maxQueued ?? 256)) {
      this.#settle(call, new CallError("overloaded", "Too many calls are waiting for the connection."));
    } else {
      this.#callQueue.push(call);
    }

    return promise;
  }

  #canSendCall(): boolean {
    return this.#helloSent && this.#state !== "closed" && this.#inFlight < this.#callWindow;
  }

  // Sends the call, or settles it when it can never fit. Returns false when it has to wait: before Welcome, only the
  // protocol's minimum message size is known, and a larger call waits to learn the server's.
  #sendCall(call: PendingCall): boolean {
    const id = this.#nextId();
    const frame = this.#frame(FrameKind.Request);
    frame.flags = FrameFlags.HasTimeout | (call.payload.binary ? FrameFlags.Binary : 0);
    frame.id = id;
    frame.timeout = Math.min(0xffffffff, Math.max(0, Math.ceil(call.deadline - performance.now())));
    frame.payload = call.payload.bytes;
    const trace = this.#options.traceparent?.();
    if (trace !== undefined) {
      frame.flags |= FrameFlags.HasTrace;
      frame.trace = trace;
    }

    if (!this.#writeNamed(frame, call.name)) {
      if (!this.#welcomed) return false;
      this.#settle(call, new CallError("payload_too_large", "The request is larger than the server accepts."));
      return true;
    }

    call.id = id;
    this.#pending.set(id, call);
    this.#inFlight++;
    return true;
  }

  #drainCalls(): void {
    while (this.#callQueue.length > 0 && this.#canSendCall()) {
      const call = this.#callQueue[0] as PendingCall;
      if (!call.settled && !this.#sendCall(call)) return;
      this.#callQueue.shift();
    }
  }

  #onResponse(frame: Frame): void {
    const call = this.#pending.get(frame.id);
    if (call === undefined) {
      this.#onUploadResponse(frame);
      return;
    }

    this.#pending.delete(frame.id);
    this.#inFlight--;
    if (!call.settled) {
      const [error, result] = resultOf(frame);
      this.#settle(call, error, result);
    }

    this.#drainCalls();
  }

  #expire(call: PendingCall): void {
    // The server has the same deadline and stops the handler itself; the id stays taken until its Response.
    if (call.settled) return;
    if (call.id === 0) removeFrom(this.#callQueue, call);
    this.#settle(call, new CallError("timeout", "The call's deadline passed."));
  }

  #abort(call: PendingCall): void {
    if (call.settled) return;
    if (call.id === 0) {
      removeFrom(this.#callQueue, call);
    } else {
      this.#writeCancel(call.id);
    }

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

  // Ids run from 1 to 2^32 - 1 and wrap, skipping the ones still in use.
  #nextId(): number {
    do this.#lastId = this.#lastId === 0xffffffff ? 1 : this.#lastId + 1;
    while (this.#pending.has(this.#lastId) || this.#downloads.has(this.#lastId) || this.#uploads.has(this.#lastId));
    return this.#lastId;
  }

  // Streams in both directions. Their StreamOpens share the ids of calls and wait for the streams window, which
  // downloads and uploads take together.

  #stream(name: string, request: unknown, options: StreamOptions | undefined): Download<unknown> {
    const buffer = options?.buffer ?? this.#options.streamBuffer ?? 16;
    if (!(Number.isInteger(buffer) && buffer >= 1))
      throw new RangeError("A stream's buffer is a whole number, at least 1.");
    return new Download(this.#streamHost, name, encodePayload(request), buffer, options?.signal);
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
      payload = encodePayload(hasRequest ? args[0] : undefined);
    } catch (error) {
      return Promise.reject(error);
    }

    return new Upload(this.#streamHost, name, payload, source, options?.signal, options?.timeout).promise;
  }

  #openStream(stream: Exchange): void {
    if (this.#state === "closed") {
      stream.closed(connectionClosed());
    } else if (this.#streamQueue.length === 0 && this.#canOpenStream() && this.#sendStreamOpen(stream)) {
      // Opened.
    } else if (this.#streamQueue.length >= (this.#options.maxQueued ?? 256)) {
      stream.refused(new CallError("overloaded", "Too many streams are waiting for the connection."));
    } else {
      this.#streamQueue.push(stream);
    }
  }

  #canOpenStream(): boolean {
    return this.#helloSent && this.#state !== "closed" && this.#openStreams < this.#streamWindow;
  }

  // Opens the stream, or refuses it when it can never fit. Returns false when it has to wait for Welcome, as calls do.
  #sendStreamOpen(stream: Exchange): boolean {
    const id = this.#nextId();
    const frame = this.#frame(FrameKind.StreamOpen);
    frame.flags = (stream.payload.binary ? FrameFlags.Binary : 0) | (stream.upload ? FrameFlags.Upload : 0);
    frame.id = id;
    frame.credit = stream.initialCredit;
    frame.payload = stream.payload.bytes;
    const trace = this.#options.traceparent?.();
    if (trace !== undefined) {
      frame.flags |= FrameFlags.HasTrace;
      frame.trace = trace;
    }

    if (!this.#writeNamed(frame, stream.name)) {
      if (!this.#welcomed) return false;
      stream.refused(new CallError("payload_too_large", "The request is larger than the server accepts."));
      return true;
    }

    if (stream instanceof Upload) this.#uploads.set(id, stream);
    else this.#downloads.set(id, stream as Download<unknown>);
    this.#openStreams++;
    stream.opened(id);
    return true;
  }

  #drainStreams(): void {
    while (this.#streamQueue.length > 0 && this.#canOpenStream()) {
      if (!this.#sendStreamOpen(this.#streamQueue[0] as Exchange)) return;
      this.#streamQueue.shift();
    }
  }

  #onStreamItem(frame: Frame): void {
    const download = this.#downloads.get(frame.id);
    if (download === undefined) throw new ProtocolError("unknown_exchange");
    download.onItem((frame.flags & FrameFlags.Binary) !== 0, frame.payload);
  }

  #onStreamEnd(frame: Frame): void {
    const download = this.#downloads.get(frame.id);
    if (download === undefined) throw new ProtocolError("unknown_exchange");

    this.#downloads.delete(frame.id);
    this.#openStreams--;
    download.onEnd(frame.flags & FrameFlags.HasError ? errorOf(frame) : undefined);
    this.#drainStreams();
  }

  // Credit with id 0 returns event credit; any other is for an upload. Credit for an upload that ended crossed its
  // end on the wire. The server produces the downloads, so it never grants credit for one.
  #onCredit(frame: Frame): void {
    if (frame.id === 0) {
      this.#uncreditedEvents = Math.max(0, this.#uncreditedEvents - frame.amount);
      this.#drainEvents();
      return;
    }

    if (this.#downloads.has(frame.id)) throw new ProtocolError("unknown_exchange");
    this.#uploads.get(frame.id)?.onCredit(frame.amount);
  }

  #onUploadResponse(frame: Frame): void {
    const upload = this.#uploads.get(frame.id);
    if (upload === undefined || upload.answered) throw new ProtocolError("unknown_exchange");
    const [error, result] = resultOf(frame);
    upload.onResponse(error, result);
  }

  #releaseUpload(id: number): void {
    if (!this.#uploads.delete(id)) return;
    this.#openStreams--;
    this.#drainStreams();
  }

  #writeCredit(id: number, amount: number): void {
    const frame = this.#frame(FrameKind.Credit);
    frame.id = id;
    frame.amount = amount;
    this.#write(frame);
  }

  #writeCancel(id: number): void {
    const frame = this.#frame(FrameKind.Cancel);
    frame.id = id;
    this.#write(frame);
  }

  #writeItem(id: number, payload: Payload): boolean {
    const frame = this.#frame(FrameKind.StreamItem);
    frame.flags = payload.binary ? FrameFlags.Binary : 0;
    frame.id = id;
    frame.payload = payload.bytes;
    return this.#write(frame);
  }

  #writeEnd(id: number, error: CallError | undefined): void {
    const frame = this.#frame(FrameKind.StreamEnd);
    frame.id = id;
    if (error !== undefined) {
      frame.flags = FrameFlags.HasError;
      [frame.errorCode, frame.errorCustom, frame.errorMessage] = errorFields(error);
      if (!this.#write(frame)) {
        frame.errorMessage = "The error was too large to send.";
        this.#write(frame);
      }
      return;
    }

    this.#write(frame);
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

  #onEvent(frame: Frame): void {
    const name = this.#incoming.resolve(frame.nameCode, frame.nameText);
    const listeners = this.#listeners.get(name);
    if (listeners === undefined || listeners.size === 0) return;

    let event: unknown;
    try {
      event = decodePayload((frame.flags & FrameFlags.Binary) !== 0, frame.payload);
    } catch (error) {
      this.#reportError(error);
      return;
    }

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

    const payload = encodePayload(event);
    if (this.#eventQueue.length === 0 && this.#canSendEvent() && this.#sendEvent({ name, payload }, true)) {
      // Sent.
    } else if (this.#eventQueue.length >= (this.#options.maxQueued ?? 256)) {
      throw new CallError("overloaded", "Too many events are waiting for the connection.");
    } else {
      this.#eventQueue.push({ name, payload });
    }
  }

  #canSendEvent(): boolean {
    return this.#helloSent && this.#state !== "closed" && this.#uncreditedEvents < this.#eventWindow;
  }

  // Sends the event, or gives it up when it can never fit. Returns false when it has to wait for Welcome, as calls do.
  #sendEvent(event: QueuedEvent, throwIfTooLarge: boolean): boolean {
    const frame = this.#frame(FrameKind.Event);
    frame.flags = event.payload.binary ? FrameFlags.Binary : 0;
    frame.payload = event.payload.bytes;
    const trace = this.#options.traceparent?.();
    if (trace !== undefined) {
      frame.flags |= FrameFlags.HasTrace;
      frame.trace = trace;
    }

    if (!this.#writeNamed(frame, event.name)) {
      if (!this.#welcomed) return false;
      const error = new CallError("payload_too_large", `The event ${event.name} is larger than the server accepts.`);
      if (throwIfTooLarge) throw error;
      this.#reportError(error);
      return true;
    }

    this.#uncreditedEvents++;
    return true;
  }

  #drainEvents(): void {
    while (this.#eventQueue.length > 0 && this.#canSendEvent()) {
      if (!this.#sendEvent(this.#eventQueue[0] as QueuedEvent, false)) return;
      this.#eventQueue.shift();
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

  #onServerCall(frame: Frame): void {
    const name = this.#incoming.resolve(frame.nameCode, frame.nameText);
    if (this.#serverCalls.size >= (this.#options.calls ?? 16)) throw new ProtocolError("window_exceeded");
    if (this.#serverCalls.has(frame.id)) throw new ProtocolError("duplicate_id");

    const id = frame.id;
    const handler = this.#handlers.get(name);
    if (handler === undefined) {
      this.#answerError(id, errorCodeOf("unknown_operation"), "", `The browser has no handler for ${name}.`);
      return;
    }

    let request: unknown;
    try {
      request = decodePayload((frame.flags & FrameFlags.Binary) !== 0, frame.payload);
    } catch (error) {
      this.#answerError(id, errorCodeOf("invalid_payload"), "", `The request is not valid JSON: ${String(error)}`);
      return;
    }

    const call: ServerCall = { controller: new AbortController(), reason: "" };
    this.#serverCalls.set(id, call);
    const timer =
      frame.flags & FrameFlags.HasTimeout
        ? setTimeout(() => {
            call.reason ||= "timeout";
            call.controller.abort();
          }, frame.timeout)
        : undefined;

    Promise.resolve()
      .then(() => handler(request, { signal: call.controller.signal }))
      .then(
        (result) => this.#answer(id, call, undefined, result),
        (error: unknown) => this.#answer(id, call, error ?? new Error("The handler failed."), undefined),
      )
      .finally(() => clearTimeout(timer));
  }

  #onServerCancel(id: number): void {
    // A Cancel for a call that already ended crossed its Response on the wire.
    const call = this.#serverCalls.get(id);
    if (call === undefined) return;
    call.reason ||= "cancelled";
    call.controller.abort();
  }

  #answer(id: number, call: ServerCall, error: unknown, result: unknown): void {
    this.#serverCalls.delete(id);
    if (this.#state === "closed") return;

    if (call.reason === "cancelled") {
      this.#answerError(id, errorCodeOf("cancelled"), "", "The call was cancelled.");
    } else if (call.reason === "timeout") {
      this.#answerError(id, errorCodeOf("timeout"), "", "The call's deadline passed.");
    } else if (error !== undefined) {
      this.#answerFailure(id, error);
    } else {
      let payload: Payload;
      try {
        payload = encodePayload(result);
      } catch (encodeError) {
        this.#answerFailure(id, encodeError);
        return;
      }

      const frame = this.#frame(FrameKind.Response);
      frame.flags = payload.binary ? FrameFlags.Binary : 0;
      frame.id = id;
      frame.payload = payload.bytes;
      if (!this.#write(frame)) {
        this.#answerError(id, errorCodeOf("payload_too_large"), "", "The result is larger than the server accepts.");
      }
    }
  }

  #answerFailure(id: number, error: unknown): void {
    this.#answerError(id, ...errorFields(error));
  }

  #answerError(id: number, code: number, custom: string, message: string): void {
    const frame = this.#frame(FrameKind.Response);
    frame.flags = FrameFlags.HasError;
    frame.id = id;
    frame.errorCode = code;
    frame.errorCustom = custom;
    frame.errorMessage = message;
    if (!this.#write(frame)) {
      frame.errorMessage = "The error was too large to send.";
      this.#write(frame);
    }
  }

  // Writing: frames queued in the same task go out together, in one message, at the end of it.

  #frame(kind: FrameKind): Frame {
    resetFrame(this.#scratch, kind);
    return this.#scratch;
  }

  // Names the frame's operation and writes it. A frame too large to send takes no name number.
  #writeNamed(frame: Frame, name: string): boolean {
    frame.nameCode = longestNameCode;
    frame.nameText = name;
    if (frameLength(frame) > this.#serverMaxMessage) return false;

    frame.nameCode = this.#outgoing.next(name);
    frame.nameText = nameHasText(frame.nameCode) ? name : "";
    return this.#write(frame);
  }

  #write(frame: Frame): boolean {
    const length = frameLength(frame);
    if (length > this.#serverMaxMessage) return false;
    if (this.#writer.length + length > this.#serverMaxMessage) this.#flush();

    this.#writer.write(frame);
    if (!this.#flushQueued) {
      this.#flushQueued = true;
      queueMicrotask(() => {
        this.#flushQueued = false;
        this.#flush();
      });
    }

    return true;
  }

  #flush(): void {
    if (this.#writer.length === 0) return;
    if (this.#socket.readyState === 1) this.#socket.send(this.#writer.view());
    this.#writer.clear();
  }

  // Liveness: a Ping after a heartbeat of silence, and closing after two.

  #startHeartbeat(interval: number): void {
    this.#heartbeat = interval;
    this.#lastReceived = performance.now();
    this.#scheduleHeartbeat();
  }

  #scheduleHeartbeat(): void {
    this.#heartbeatTimer = setTimeout(() => this.#checkHeartbeat(), this.#heartbeat / 2);
  }

  #checkHeartbeat(): void {
    if (this.#state === "closed") return;
    const silence = performance.now() - this.#lastReceived;
    if (silence >= 2 * this.#heartbeat) {
      this.#socket.close(1000, "The server stopped answering.");
      this.#onClose(1006, "The server stopped answering.");
      return;
    }

    if (silence >= this.#heartbeat && !this.#pingSent) {
      this.#pingSent = true;
      this.#write(this.#frame(FrameKind.Ping));
    }

    this.#scheduleHeartbeat();
  }

  // Ending.

  #fail(error: unknown): void {
    if (error instanceof ProtocolError) {
      this.#reportError(error);
      if (this.#socket.readyState < 2) this.#socket.close(protocolError, error.violation);
      this.#onClose(protocolError, error.violation);
      return;
    }

    this.#reportError(error);
  }

  #onClose(code: number, reason: string): void {
    if (this.#state === "closed") return;
    this.#state = "closed";
    if (this.#heartbeatTimer !== undefined) clearTimeout(this.#heartbeatTimer);

    for (const call of this.#pending.values()) this.#settle(call, connectionClosed());
    for (const call of this.#callQueue) this.#settle(call, connectionClosed());
    for (const call of this.#serverCalls.values()) call.controller.abort();
    const streams = [...this.#streamQueue, ...this.#downloads.values(), ...this.#uploads.values()];
    this.#pending.clear();
    this.#callQueue.length = 0;
    this.#eventQueue.length = 0;
    this.#serverCalls.clear();
    this.#deadlines.clear();
    this.#streamQueue.length = 0;
    this.#downloads.clear();
    this.#uploads.clear();
    this.#openStreams = 0;
    for (const stream of streams) stream.closed(connectionClosed());

    this.#rejectOpened(connectionClosed());
    this.#notify("closed", { code, reason });
    this.#resolveClosed();
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

function errorOf(frame: Frame): CallError {
  const code = frame.errorCode === 0 ? frame.errorCustom : (errorCodeNames[frame.errorCode] ?? "protocol_error");
  let details: unknown;
  if (frame.payload.length > 0) {
    try {
      details = JSON.parse(decoder.decode(frame.payload));
    } catch {
      details = undefined;
    }
  }

  return new CallError(code, frame.errorMessage, details);
}

/** A Response's outcome: its error, or its result. */
function resultOf(frame: Frame): [CallError | undefined, unknown] {
  if (frame.flags & FrameFlags.HasError) return [errorOf(frame), undefined];
  try {
    return [undefined, decodePayload((frame.flags & FrameFlags.Binary) !== 0, frame.payload)];
  } catch (error) {
    return [new CallError("invalid_payload", `The result is not valid JSON: ${String(error)}`), undefined];
  }
}

/** How an error goes on the wire: a CallError's own code when the protocol allows it, otherwise handler_failed. */
function errorFields(error: unknown): [code: number, custom: string, message: string] {
  const message = error instanceof Error ? error.message : String(error);
  if (error instanceof CallError) {
    const code = errorCodeOf(error.code);
    if (code !== 0) return [code, "", message];
    if (isValidCustomCode(encoder.encode(error.code))) return [0, error.code, message];
  }

  return [errorCodeOf("handler_failed"), "", message];
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
