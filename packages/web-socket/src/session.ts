import type { ConnectOptions, HandlerContext, WebSocketConstructor } from "./contract.ts";
import { CallError, connectionClosed } from "./errors.ts";
import { decodePayload, encodePayload, type Payload } from "./payload.ts";
import { errorCodeNames, errorCodeOf, isValidCustomCode } from "./protocol/errors.ts";
import {
  createFrame,
  type Frame,
  FrameFlags,
  FrameKind,
  isSequenced,
  nameHasText,
  resetFrame,
} from "./protocol/frames.ts";
import { IncomingNames, nameTableLimit, OutgoingNames } from "./protocol/names.ts";
import { readFrames } from "./protocol/reader.ts";
import { ProtocolError } from "./protocol/violation.ts";
import { FrameWriter, frameLength } from "./protocol/writer.ts";
import { type Download, type Exchange, type StreamLink, Upload } from "./streams.ts";

export const subprotocol = "puregate.v1";

// Close codes (docs/PROTOCOL.md, Closing).
export const protocolErrorCode = 4000;
export const reconnectingCode = 4006;

// Before Welcome, the minimums every server advertises (docs/PROTOCOL.md, Handshake).
const initialCalls = 4;
const initialEvents = 16;
const initialStreams = 1;
export const minimumMessage = 16 * 1024;

// The longest name code, to measure a frame before its name is numbered.
const longestNameCode = 2 * nameTableLimit + 1;

// The most a StreamItem takes besides its payload: the length prefix, the kind, the flags and the id, at their longest.
const streamItemOverhead = 5 + 1 + 1 + 5;

// Acknowledgement (docs/PROTOCOL.md, Acknowledgement and resume).
const ackEveryFrames = 64;
const ackDelay = 1_000;

// Unacknowledged frames kept for a resume. Past this the session can no longer resume, as on the server.
const replayLimit = 4 * 1024 * 1024;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** A call to the server: waiting for the connection or a window, or for its Response. */
export interface PendingCall {
  readonly name: string;
  readonly payload: Payload;
  readonly deadline: number;
  readonly resolve: (value: unknown) => void;
  readonly reject: (error: unknown) => void;
  readonly signal: AbortSignal | undefined;
  /** 0 until the Request is sent. */
  id: number;
  /** The session the Request went out on. */
  session: Session | undefined;
  settled: boolean;
  onAbort: (() => void) | undefined;
}

export interface QueuedEvent {
  readonly name: string;
  readonly payload: Payload;
}

export type Handler = (request: unknown, context: HandlerContext) => unknown;

interface ServerCall {
  readonly controller: AbortController;
  /** Why the browser stopped waiting for its own handler, if it did. */
  reason: "" | "cancelled" | "timeout";
}

interface Welcome {
  connection: string;
  session: string;
  resumed?: boolean;
  received?: number;
  calls: number;
  streams: number;
  events: number;
  maxMessage: number;
  heartbeat: number;
  resumeWindow?: number;
  contract?: string;
  route?: string;
}

/** What a session needs from the connection that owns it. */
export interface SessionOwner {
  readonly options: ConnectOptions;
  readonly WebSocket: WebSocketConstructor;
  handler(name: string): Handler | undefined;
  hasListeners(name: string): boolean;
  dispatch(name: string, event: unknown): void;
  settle(call: PendingCall, error: unknown, value?: unknown): void;
  reportError(error: unknown): void;
  /** A window has room: send what waits. */
  drain(): void;
  welcomed(session: Session, resumed: boolean, contract: string | undefined): void;
  /** The session's WebSocket closed. */
  dropped(session: Session, code: number, reason: string): void;
  /** The server is stopping: the session takes nothing new. */
  goAway(session: Session): void;
}

/**
 * One session with the server (docs/PROTOCOL.md): ids, name tables, windows, the exchanges in progress, and the
 * sequenced frames each side processed. A session outlives its WebSocket. When the connection drops, the session keeps
 * what it sent and the server has not acknowledged, and a resume sends it again from where the server left off.
 */
export class Session {
  readonly #owner: SessionOwner;
  readonly #writer = new FrameWriter();
  readonly #spare = new FrameWriter(256);
  readonly #scratch = createFrame(FrameKind.Ping);
  readonly #pending = new Map<number, PendingCall>();
  readonly #downloads = new Map<number, Download<unknown>>();
  readonly #uploads = new Map<number, Upload>();
  readonly #serverCalls = new Map<number, ServerCall>();
  readonly #link: StreamLink;
  #incoming = new IncomingNames();
  #outgoing = new OutgoingNames();

  #socket: WebSocket | undefined;
  /** The current WebSocket's Hello went out. */
  #helloSent = false;
  /** The current WebSocket's Welcome arrived. */
  #welcomed = false;
  /** The current WebSocket's Hello asked to resume. */
  #resuming = false;
  /** GoAway arrived: the session takes nothing new. */
  #draining = false;
  #ended = false;

  // From Welcome.
  #id = "";
  #connectionId = "";
  #route: string | undefined;
  #resumeWindow = 0;
  #callWindow = initialCalls;
  #eventWindow = initialEvents;
  #streamWindow = initialStreams;
  #serverMaxMessage = minimumMessage;

  #inFlight = 0;
  #uncreditedEvents = 0;
  #openStreams = 0;
  #lastId = 0;

  // Sequenced frames sent, and those the server acknowledged; the ones between are kept for a resume.
  #sent = 0;
  #acknowledged = 0;
  #replay: Uint8Array[] = [];
  #replayBytes = 0;
  #keepReplay = true;

  // Sequenced frames processed, and those not yet acknowledged.
  #received = 0;
  #unacknowledged = 0;
  #ackTimer: ReturnType<typeof setTimeout> | undefined;

  #flushQueued = false;
  /** Why the client closed the current WebSocket itself, if it did. */
  #closedWith: { code: number; reason: string } | undefined;

  #heartbeat = 0;
  #heartbeatTimer: ReturnType<typeof setTimeout> | undefined;
  #lastReceived = 0;
  #pingSent = false;

  constructor(owner: SessionOwner) {
    this.#owner = owner;
    this.#link = {
      credit: (id, amount) => this.#writeCredit(id, amount),
      cancel: (id) => this.cancel(id),
      item: (id, payload) => this.#writeItem(id, payload),
      maxItemBytes: () => this.#serverMaxMessage - streamItemOverhead,
      end: (id, error) => this.#writeEnd(id, error),
      release: (id) => this.#releaseUpload(id),
    };
  }

  /** The server's id for the current connection. Empty until Welcome. */
  get connectionId(): string {
    return this.#connectionId;
  }

  /** The routing id from Welcome, for the reconnect URL. */
  get route(): string | undefined {
    return this.#route;
  }

  /** Whether a dropped connection can come back to this session. */
  get resumable(): boolean {
    return this.#id !== "" && this.#resumeWindow > 0 && this.#keepReplay && !this.#draining && !this.#ended;
  }

  /** Whether the session has a WebSocket that is opening or open. */
  get connected(): boolean {
    return this.#socket !== undefined;
  }

  /** Whether new calls, events and streams can go out now: Hello is out, and a resume waits for its Welcome. */
  get ready(): boolean {
    return this.#helloSent && !this.#ended && !this.#draining && (this.#welcomed || !this.#resuming);
  }

  /** Opens a WebSocket for the session, resuming it if it can. */
  connect(url: string): void {
    this.#resuming = this.resumable;
    this.#helloSent = false;
    this.#welcomed = false;
    this.#closedWith = undefined;

    const socket = new this.#owner.WebSocket(url, subprotocol);
    socket.binaryType = "arraybuffer";
    socket.onopen = () => void this.#onOpen(socket);
    socket.onmessage = (event: MessageEvent) => {
      if (socket === this.#socket) this.#onMessage(event);
    };
    socket.onclose = (event: CloseEvent) => this.#onClose(socket, event.code, event.reason);
    this.#socket = socket;
  }

  /**
   * Closes the session's WebSocket. Its onclose reports the drop with this code: a close event carries the code of
   * the server's answer, and the client's own reason for closing is what decides what happens next.
   */
  close(code: number, reason: string): void {
    if (this.#socket === undefined || this.#socket.readyState >= 2) return;
    this.#closedWith ??= { code, reason };
    this.#socket.close(code, reason);
  }

  // Opening: Hello first, then, unless resuming, everything that waited, within the initial windows.

  async #onOpen(socket: WebSocket): Promise<void> {
    if (socket.protocol !== subprotocol) {
      // Browsers only send 1000 and 3000–4999.
      this.close(protocolErrorCode, "The server did not select puregate.v1.");
      return;
    }

    let token: string | undefined;
    try {
      token = await this.#owner.options.token?.();
    } catch (error) {
      this.#owner.reportError(error);
      this.close(1000, "No token.");
      return;
    }

    if (socket !== this.#socket || socket.readyState !== 1) return;
    const options = this.#owner.options;
    const hello: Record<string, unknown> = {
      calls: options.calls ?? 16,
      maxMessage: Math.max(minimumMessage, options.maxMessage ?? 16 * 1024 * 1024),
    };
    if (token !== undefined) hello.token = token;
    if (options.contractHash !== undefined) hello.contract = options.contractHash;
    if (this.#resuming) hello.resume = { session: this.#id, received: this.#received };

    const frame = this.#frame(FrameKind.Hello);
    frame.payload = encoder.encode(JSON.stringify(hello));
    this.#helloSent = true;
    this.#writeNow(frame);
    if (!this.#resuming) this.#owner.drain();
  }

  #onMessage(event: MessageEvent): void {
    this.#lastReceived = performance.now();
    this.#pingSent = false;
    let sequenced = 0;
    try {
      readFrames(new Uint8Array(event.data as ArrayBuffer), "server", (frame) => {
        if (isSequenced(frame.kind)) sequenced++;
        this.#onFrame(frame);
      });
    } catch (error) {
      this.#fail(error);
      return;
    }

    // A message counts once all of it is dispatched.
    if (sequenced > 0) this.#processed(sequenced);
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
      case FrameKind.Ack:
        this.#onAck(frame.count);
        break;
      case FrameKind.Ping:
        this.#write(this.#frame(FrameKind.Pong));
        break;
      case FrameKind.Pong:
        break;
      case FrameKind.GoAway:
        if (!this.#draining) {
          this.#draining = true;
          this.#owner.goAway(this);
        }
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

    const resumed = welcome.resumed === true;
    if (
      !(
        welcome.calls >= initialCalls &&
        welcome.streams >= initialStreams &&
        welcome.events >= initialEvents &&
        welcome.maxMessage >= minimumMessage
      ) ||
      (resumed && !this.#resuming)
    ) {
      throw new ProtocolError("invalid_handshake");
    }

    // The session was gone on the server: everything in progress fails, and this is a new one.
    if (this.#resuming && !resumed) this.#reset(connectionClosed());

    this.#welcomed = true;
    this.#id = String(welcome.session ?? "");
    this.#connectionId = String(welcome.connection ?? "");
    this.#route = typeof welcome.route === "string" ? welcome.route : undefined;
    this.#resumeWindow = welcome.resumeWindow ?? 0;
    this.#callWindow = welcome.calls;
    this.#eventWindow = welcome.events;
    this.#streamWindow = welcome.streams;
    this.#serverMaxMessage = welcome.maxMessage;
    if (this.#resumeWindow <= 0) this.#dropReplay();

    if (resumed) {
      const received = welcome.received ?? -1;
      if (!(received >= this.#acknowledged && received <= this.#sent)) throw new ProtocolError("invalid_handshake");
      this.#acknowledge(received);
      for (const frames of this.#replay) {
        if (this.#writer.length + frames.length > this.#serverMaxMessage) this.#flush();
        this.#writer.append(frames);
      }
      this.#scheduleFlush();
    }

    this.#startHeartbeat(welcome.heartbeat > 0 ? welcome.heartbeat : 15_000);
    this.#owner.welcomed(this, resumed, welcome.contract);
  }

  // Calls to the server.

  canSendCall(): boolean {
    return this.ready && this.#inFlight < this.#callWindow;
  }

  // Sends the call, or settles it when it can never fit. Returns false when it has to wait: before Welcome, only the
  // protocol's minimum message size is known, and a larger call waits to learn the server's.
  sendCall(call: PendingCall): boolean {
    const id = this.#nextId();
    const frame = this.#frame(FrameKind.Request);
    frame.flags = FrameFlags.HasTimeout | (call.payload.binary ? FrameFlags.Binary : 0);
    frame.id = id;
    frame.timeout = Math.min(0xffffffff, Math.max(0, Math.ceil(call.deadline - performance.now())));
    frame.payload = call.payload.bytes;
    this.#trace(frame);

    if (!this.#writeNamed(frame, call.name)) {
      if (!this.#welcomed) return false;
      this.#owner.settle(call, new CallError("payload_too_large", "The request is larger than the server accepts."));
      return true;
    }

    call.id = id;
    call.session = this;
    this.#pending.set(id, call);
    this.#inFlight++;
    return true;
  }

  /** Cancels an exchange this session started: a call, a download or an upload. */
  cancel(id: number): void {
    const frame = this.#frame(FrameKind.Cancel);
    frame.id = id;
    this.#write(frame);
  }

  #onResponse(frame: Frame): void {
    const call = this.#pending.get(frame.id);
    if (call === undefined) {
      const upload = this.#uploads.get(frame.id);
      if (upload === undefined || upload.answered) throw new ProtocolError("unknown_exchange");
      const [error, result] = resultOf(frame);
      upload.onResponse(error, result);
      return;
    }

    this.#pending.delete(frame.id);
    this.#inFlight--;
    if (!call.settled) {
      const [error, result] = resultOf(frame);
      this.#owner.settle(call, error, result);
    }

    this.#owner.drain();
  }

  // Ids run from 1 to 2^32 - 1 and wrap, skipping the ones still in use.
  #nextId(): number {
    do this.#lastId = this.#lastId === 0xffffffff ? 1 : this.#lastId + 1;
    while (this.#pending.has(this.#lastId) || this.#downloads.has(this.#lastId) || this.#uploads.has(this.#lastId));
    return this.#lastId;
  }

  // Events in both directions.

  canSendEvent(): boolean {
    return this.ready && this.#uncreditedEvents < this.#eventWindow;
  }

  // Sends the event, or gives it up when it can never fit. Returns false when it has to wait for Welcome, as calls do.
  sendEvent(event: QueuedEvent, throwIfTooLarge: boolean): boolean {
    const frame = this.#frame(FrameKind.Event);
    frame.flags = event.payload.binary ? FrameFlags.Binary : 0;
    frame.payload = event.payload.bytes;
    this.#trace(frame);

    if (!this.#writeNamed(frame, event.name)) {
      if (!this.#welcomed) return false;
      const error = new CallError("payload_too_large", `The event ${event.name} is larger than the server accepts.`);
      if (throwIfTooLarge) throw error;
      this.#owner.reportError(error);
      return true;
    }

    this.#uncreditedEvents++;
    return true;
  }

  #onEvent(frame: Frame): void {
    const name = this.#incoming.resolve(frame.nameCode, frame.nameText);
    if (!this.#owner.hasListeners(name)) return;

    let event: unknown;
    try {
      event = decodePayload((frame.flags & FrameFlags.Binary) !== 0, frame.payload);
    } catch (error) {
      this.#owner.reportError(error);
      return;
    }

    this.#owner.dispatch(name, event);
  }

  // Streams in both directions. Their StreamOpens share the ids of calls and wait for the streams window, which
  // downloads and uploads take together.

  canOpenStream(): boolean {
    return this.ready && this.#openStreams < this.#streamWindow;
  }

  // Opens the stream, or refuses it when it can never fit. Returns false when it has to wait for Welcome, as calls do.
  openStream(stream: Exchange): boolean {
    const id = this.#nextId();
    const frame = this.#frame(FrameKind.StreamOpen);
    frame.flags = (stream.payload.binary ? FrameFlags.Binary : 0) | (stream.upload ? FrameFlags.Upload : 0);
    frame.id = id;
    frame.credit = stream.initialCredit;
    frame.payload = stream.payload.bytes;
    this.#trace(frame);

    if (!this.#writeNamed(frame, stream.name)) {
      if (!this.#welcomed) return false;
      stream.refused(new CallError("payload_too_large", "The request is larger than the server accepts."));
      return true;
    }

    if (stream instanceof Upload) this.#uploads.set(id, stream);
    else this.#downloads.set(id, stream as Download<unknown>);
    this.#openStreams++;
    stream.opened(id, this.#link);
    return true;
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
    this.#owner.drain();
  }

  // Credit with id 0 returns event credit; any other is for an upload. Credit for an upload that ended crossed its
  // end on the wire. The server produces the downloads, so it never grants credit for one.
  #onCredit(frame: Frame): void {
    if (frame.id === 0) {
      this.#uncreditedEvents = Math.max(0, this.#uncreditedEvents - frame.amount);
      this.#owner.drain();
      return;
    }

    if (this.#downloads.has(frame.id)) throw new ProtocolError("unknown_exchange");
    this.#uploads.get(frame.id)?.onCredit(frame.amount);
  }

  #releaseUpload(id: number): void {
    if (!this.#uploads.delete(id)) return;
    this.#openStreams--;
    this.#owner.drain();
  }

  #writeCredit(id: number, amount: number): void {
    const frame = this.#frame(FrameKind.Credit);
    frame.id = id;
    frame.amount = amount;
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
    if (error === undefined) {
      this.#write(frame);
      return;
    }

    frame.flags = FrameFlags.HasError;
    [frame.errorCode, frame.errorCustom, frame.errorMessage] = errorFields(error);
    if (!this.#write(frame)) {
      frame.errorMessage = "The error was too large to send.";
      this.#write(frame);
    }
  }

  // Calls from the server, answered by the handlers registered with handle.

  #onServerCall(frame: Frame): void {
    const name = this.#incoming.resolve(frame.nameCode, frame.nameText);
    if (this.#serverCalls.size >= (this.#owner.options.calls ?? 16)) throw new ProtocolError("window_exceeded");
    if (this.#serverCalls.has(frame.id)) throw new ProtocolError("duplicate_id");

    const id = frame.id;
    const handler = this.#owner.handler(name);
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
    // A call of a session that ended, or was replaced by a new one, has nobody to answer.
    if (this.#serverCalls.get(id) !== call) return;
    this.#serverCalls.delete(id);

    if (call.reason === "cancelled") {
      this.#answerError(id, errorCodeOf("cancelled"), "", "The call was cancelled.");
    } else if (call.reason === "timeout") {
      this.#answerError(id, errorCodeOf("timeout"), "", "The call's deadline passed.");
    } else if (error !== undefined) {
      this.#answerError(id, ...errorFields(error));
    } else {
      let payload: Payload;
      try {
        payload = encodePayload(result);
      } catch (encodeError) {
        this.#answerError(id, ...errorFields(encodeError));
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

  // Acknowledgement: no later than 64 frames, or a second, after the first one not acknowledged.

  #processed(frames: number): void {
    this.#received += frames;
    this.#unacknowledged += frames;
    if (this.#unacknowledged >= ackEveryFrames) {
      this.#sendAck();
    } else if (this.#ackTimer === undefined) {
      this.#ackTimer = setTimeout(() => {
        this.#ackTimer = undefined;
        if (this.#unacknowledged > 0) this.#sendAck();
      }, ackDelay);
    }
  }

  // Without a connection, the next Hello carries the count instead.
  #sendAck(): void {
    clearTimeout(this.#ackTimer);
    this.#ackTimer = undefined;
    this.#unacknowledged = 0;
    const frame = this.#frame(FrameKind.Ack);
    frame.count = this.#received % 0x1_0000_0000;
    this.#write(frame);
  }

  // The server's Ack counts modulo 2^32: it means the largest count up to the frames sent that has those low bits.
  #onAck(count: number): void {
    const acknowledged = this.#sent - ((((this.#sent - count) % 0x1_0000_0000) + 0x1_0000_0000) % 0x1_0000_0000);
    if (acknowledged < 0) throw new ProtocolError("unexpected_frame");
    this.#acknowledge(acknowledged);
  }

  #acknowledge(through: number): void {
    if (through <= this.#acknowledged) return;
    const dropped = this.#replay.splice(0, through - this.#acknowledged);
    for (const frames of dropped) this.#replayBytes -= frames.length;
    this.#acknowledged = through;
  }

  #keep(frames: Uint8Array): void {
    if (!this.#keepReplay) return;
    this.#replay.push(frames);
    this.#replayBytes += frames.length;
    if (this.#replayBytes > replayLimit) this.#dropReplay();
  }

  #dropReplay(): void {
    this.#keepReplay = false;
    this.#replay = [];
    this.#replayBytes = 0;
  }

  // Writing: frames queued in the same task go out together, in one message, at the end of it. Sequenced frames are
  // kept for a resume, and without a connection they only go there.

  #frame(kind: FrameKind): Frame {
    resetFrame(this.#scratch, kind);
    return this.#scratch;
  }

  #trace(frame: Frame): void {
    const trace = this.#owner.options.traceparent?.();
    if (trace !== undefined) {
      frame.flags |= FrameFlags.HasTrace;
      frame.trace = trace;
    }
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

  // False when the frame is larger than the server accepts.
  #write(frame: Frame): boolean {
    const length = frameLength(frame);
    if (length > this.#serverMaxMessage) return false;

    const sequenced = isSequenced(frame.kind);
    if (sequenced) this.#sent++;

    // While a resume waits for its Welcome, new frames go after the ones it sends again.
    if (!this.#helloSent || this.#socket?.readyState !== 1 || (this.#resuming && !this.#welcomed)) {
      if (sequenced && this.#keepReplay) {
        this.#spare.clear();
        this.#spare.write(frame);
        this.#keep(this.#spare.view().slice());
      }

      return true;
    }

    this.#writeNow(frame, length, sequenced);
    return true;
  }

  #writeNow(frame: Frame, length = frameLength(frame), sequenced = false): void {
    if (this.#writer.length + length > this.#serverMaxMessage) this.#flush();
    const start = this.#writer.length;
    this.#writer.write(frame);
    if (sequenced) this.#keep(this.#writer.view().slice(start));
    this.#scheduleFlush();
  }

  #scheduleFlush(): void {
    if (this.#flushQueued) return;
    this.#flushQueued = true;
    queueMicrotask(() => {
      this.#flushQueued = false;
      this.#flush();
    });
  }

  #flush(): void {
    if (this.#writer.length === 0) return;
    if (this.#socket?.readyState === 1) this.#socket.send(this.#writer.view());
    this.#writer.clear();
  }

  // Liveness: a Ping after a heartbeat of silence, and a reconnect after two.

  #startHeartbeat(interval: number): void {
    this.#heartbeat = interval;
    this.#lastReceived = performance.now();
    clearTimeout(this.#heartbeatTimer);
    this.#heartbeatTimer = setTimeout(() => this.#checkHeartbeat(), this.#heartbeat / 2);
  }

  #checkHeartbeat(): void {
    const socket = this.#socket;
    if (socket === undefined || this.#ended) return;

    // Timestamps, not timer ticks: a background tab runs timers late, and that is not a dead connection.
    const silence = performance.now() - this.#lastReceived;
    if (silence >= 2 * this.#heartbeat) {
      socket.close(reconnectingCode, "The server stopped answering.");
      this.#onClose(socket, 1006, "The server stopped answering.");
      return;
    }

    if (silence >= this.#heartbeat && !this.#pingSent) {
      this.#pingSent = true;
      this.#write(this.#frame(FrameKind.Ping));
    }

    this.#heartbeatTimer = setTimeout(() => this.#checkHeartbeat(), this.#heartbeat / 2);
  }

  // Ending.

  #fail(error: unknown): void {
    this.#owner.reportError(error);
    if (!(error instanceof ProtocolError)) return;

    const socket = this.#socket;
    if (socket === undefined) return;
    if (socket.readyState < 2) socket.close(protocolErrorCode, error.violation);
    this.#onClose(socket, protocolErrorCode, error.violation);
  }

  #onClose(socket: WebSocket, code: number, reason: string): void {
    if (socket !== this.#socket) return;
    this.#socket = undefined;
    this.#helloSent = false;
    this.#welcomed = false;
    clearTimeout(this.#heartbeatTimer);
    clearTimeout(this.#ackTimer);
    this.#ackTimer = undefined;

    // What the socket had not sent yet is kept for the resume, or lost with the session.
    this.#writer.clear();
    const closedWith = this.#closedWith ?? { code, reason };
    this.#closedWith = undefined;
    if (!this.#ended) this.#owner.dropped(this, closedWith.code, closedWith.reason);
  }

  /** Ends the session for good: everything in progress fails with error. */
  fail(error: CallError): void {
    if (this.#ended) return;
    this.#ended = true;
    this.#failExchanges(error);
    this.#dropReplay();
    clearTimeout(this.#heartbeatTimer);
    clearTimeout(this.#ackTimer);
    this.close(1000, "");
  }

  // The server no longer has the session: what was in progress fails, and the session starts over as a new one.
  #reset(error: CallError): void {
    this.#failExchanges(error);
    this.#incoming = new IncomingNames();
    this.#outgoing = new OutgoingNames();
    this.#lastId = 0;
    this.#sent = 0;
    this.#acknowledged = 0;
    this.#replay = [];
    this.#replayBytes = 0;
    this.#keepReplay = true;
    this.#received = 0;
    this.#unacknowledged = 0;
    this.#resuming = false;
  }

  #failExchanges(error: CallError): void {
    for (const call of this.#pending.values()) this.#owner.settle(call, error);
    const streams = [...this.#downloads.values(), ...this.#uploads.values()];
    for (const call of this.#serverCalls.values()) call.controller.abort();
    this.#pending.clear();
    this.#downloads.clear();
    this.#uploads.clear();
    this.#serverCalls.clear();
    this.#inFlight = 0;
    this.#uncreditedEvents = 0;
    this.#openStreams = 0;
    for (const stream of streams) stream.closed(error);
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
