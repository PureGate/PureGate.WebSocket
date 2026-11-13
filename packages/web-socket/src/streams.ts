import type { CallStream, UploadSource } from "./contract.ts";
import { CallError } from "./errors.ts";
import { decodePayload, encodePayload, type Payload } from "./payload.ts";
import { ProtocolError } from "./protocol/violation.ts";

// Streams in both directions (docs/PROTOCOL.md, Streams). The connection queues them, a session opens them within its
// streams window and routes their frames here; these classes keep each stream's credit and its side of the exchange.

/** Where a stream waits for a session to open it. */
export interface StreamQueue {
  /** Opens the stream when a window has room. It may refuse it, or report the connection closed, at once. */
  open(stream: Exchange): void;
  /** Takes back a stream that has not opened yet. */
  dequeue(stream: Exchange): void;
}

/** What an open stream sends through the session that opened it. */
export interface StreamLink {
  credit(id: number, amount: number): void;
  cancel(id: number): void;
  /** False when the item is larger than the server accepts. */
  item(id: number, payload: Payload): boolean;
  /** The largest binary item that always fits in one of the server's messages. */
  maxItemBytes(): number;
  end(id: number, error: CallError | undefined): void;
  /** Both sides are done with an upload: its id and its place in the window are free. */
  release(id: number): void;
}

/** A stream waiting for, or holding, a place in the streams window. */
export interface Exchange {
  readonly name: string;
  readonly payload: Payload;
  readonly upload: boolean;
  /** The credit its StreamOpen carries. */
  readonly initialCredit: number;
  /** Its StreamOpen went out with this id. */
  opened(id: number, link: StreamLink): void;
  /** Its StreamOpen can never go out: too large, or too many streams waiting. */
  refused(error: CallError): void;
  /** The connection closed. */
  closed(error: CallError): void;
}

function cancelled(): CallError {
  return new CallError("cancelled", "The stream was cancelled.");
}

/**
 * A stream the server produces. It opens on the first next(), holds credit for its buffer, and grants half the buffer
 * again each time the application has taken that many items. break, return(), cancel() and the signal send Cancel; the
 * id stays taken until the server's StreamEnd, which the connection routes here.
 */
export class Download<T> implements CallStream<T>, Exchange {
  readonly upload = false;
  readonly name: string;
  readonly payload: Payload;
  readonly initialCredit: number;

  readonly #host: StreamQueue;
  readonly #batch: number;
  readonly #signal: AbortSignal | undefined;
  readonly #items: unknown[] = [];
  readonly #waiters: PromiseWithResolvers<IteratorResult<T, undefined>>[] = [];
  readonly #onAbort = () => this.#stop(cancelled());

  #id = 0;
  #link: StreamLink | undefined;
  #state: "idle" | "queued" | "open" | "done" = "idle";
  /** Items the server may still send. */
  #credit: number;
  /** Items taken since credit was last granted. */
  #taken = 0;
  /** The application stopped reading. */
  #stopped = false;
  /** Thrown once the buffered items are taken. */
  #error: CallError | undefined;

  constructor(host: StreamQueue, name: string, payload: Payload, buffer: number, signal: AbortSignal | undefined) {
    this.#host = host;
    this.name = name;
    this.payload = payload;
    this.initialCredit = buffer;
    this.#credit = buffer;
    this.#batch = Math.max(1, buffer >> 1);
    this.#signal = signal;
  }

  [Symbol.asyncIterator](): this {
    return this;
  }

  next(): Promise<IteratorResult<T, undefined>> {
    if (this.#state === "idle") this.#start();
    if (this.#items.length > 0) return Promise.resolve({ value: this.#take(), done: false });
    if (this.#state === "done" || this.#stopped) return this.#finish();

    const waiter = Promise.withResolvers<IteratorResult<T, undefined>>();
    this.#waiters.push(waiter);
    return waiter.promise;
  }

  return(): Promise<IteratorResult<T, undefined>> {
    this.cancel();
    return Promise.resolve({ value: undefined, done: true });
  }

  cancel(): void {
    this.#stop(undefined);
  }

  // The connection's side.

  opened(id: number, link: StreamLink): void {
    this.#link = link;
    this.#id = id;
    this.#state = "open";
  }

  refused(error: CallError): void {
    this.#done(error);
  }

  closed(error: CallError): void {
    this.#done(error);
  }

  /** A StreamItem. Throws when the server sent more than its credit. */
  onItem(binary: boolean, bytes: Uint8Array): void {
    if (--this.#credit < 0) throw new ProtocolError("window_exceeded");
    if (this.#stopped) return;

    let value: unknown;
    try {
      value = decodePayload(binary, bytes);
    } catch (error) {
      this.#stop(new CallError("invalid_payload", `An item is not valid JSON: ${String(error)}`));
      return;
    }

    const waiter = this.#waiters.shift();
    if (waiter === undefined) {
      this.#items.push(value);
    } else {
      this.#counted();
      waiter.resolve({ value: value as T, done: false });
    }
  }

  /** The server's StreamEnd: error when it failed. */
  onEnd(error: CallError | undefined): void {
    this.#done(error);
  }

  #start(): void {
    if (this.#signal?.aborted) {
      this.#state = "done";
      this.#stopped = true;
      this.#error = cancelled();
      return;
    }

    this.#state = "queued";
    this.#signal?.addEventListener("abort", this.#onAbort, { once: true });
    this.#host.open(this);
  }

  #take(): T {
    this.#counted();
    return this.#items.shift() as T;
  }

  // The application took an item: credit goes back in batches of half the buffer.
  #counted(): void {
    if (this.#state !== "open" || this.#stopped || ++this.#taken < this.#batch) return;
    this.#credit += this.#taken;
    this.#link?.credit(this.#id, this.#taken);
    this.#taken = 0;
  }

  // The application stops reading, with an error for the reader when the signal aborted or an item was unreadable.
  #stop(error: CallError | undefined): void {
    if (this.#stopped) return;
    this.#stopped = true;
    this.#items.length = 0;
    this.#error ??= error;
    this.#signal?.removeEventListener("abort", this.#onAbort);

    if (this.#state === "queued") {
      this.#host.dequeue(this);
      this.#state = "done";
    } else if (this.#state === "open") {
      this.#link?.cancel(this.#id);
    } else if (this.#state === "idle") {
      this.#state = "done";
    }

    this.#wake();
  }

  // The stream is over on the wire. Buffered items stay readable, then the error is thrown.
  #done(error: CallError | undefined): void {
    if (this.#state === "done") return;
    this.#state = "done";
    this.#signal?.removeEventListener("abort", this.#onAbort);
    if (!this.#stopped) this.#error ??= error;
    this.#wake();
  }

  #wake(): void {
    for (const waiter of this.#waiters.splice(0)) {
      const error = this.#error;
      this.#error = undefined;
      if (error === undefined) waiter.resolve({ value: undefined, done: true });
      else waiter.reject(error);
    }
  }

  #finish(): Promise<IteratorResult<T, undefined>> {
    const error = this.#error;
    this.#error = undefined;
    return error === undefined ? Promise.resolve({ value: undefined, done: true }) : Promise.reject(error);
  }
}

/** Reads a source one item at a time, whatever kind of source it is. */
interface SourceReader {
  next(): Promise<IteratorResult<unknown>>;
  /** Lets go of the source early. Never throws. */
  release(): void;
}

/** Whether a value can be an upload's source: an AsyncIterable, an Iterable or a ReadableStream. */
export function isSource(value: unknown): value is UploadSource<unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    (typeof (value as ReadableStream).getReader === "function" ||
      Symbol.asyncIterator in value ||
      Symbol.iterator in value)
  );
}

// A ReadableStream's byte chunks are split into pieces of at most maxPiece() bytes: browsers choose those chunks, a
// Blob's can be the whole file, so their boundaries mean nothing. An iterable's items are the application's own, and
// go as they are.
function readerOf(source: UploadSource<unknown>, maxPiece: () => number): SourceReader {
  // A ReadableStream first: not every browser makes one async-iterable.
  if (typeof (source as ReadableStream).getReader === "function") {
    const reader = (source as ReadableStream<unknown>).getReader();
    let rest: Uint8Array | undefined;
    return {
      next: async () => {
        if (rest === undefined) {
          const result = await reader.read();
          if (result.done || !(result.value instanceof Uint8Array)) return result as IteratorResult<unknown>;
          rest = result.value;
        }

        const limit = maxPiece();
        const piece = rest.length <= limit ? rest : rest.subarray(0, limit);
        rest = piece === rest ? undefined : rest.subarray(limit);
        return { value: piece, done: false };
      },
      release: () => void reader.cancel().catch(() => undefined),
    };
  }

  const iterator =
    Symbol.asyncIterator in source
      ? (source as AsyncIterable<unknown>)[Symbol.asyncIterator]()
      : (source as Iterable<unknown>)[Symbol.iterator]();
  return {
    next: async () => iterator.next(),
    release: () => {
      try {
        void Promise.resolve(iterator.return?.()).catch(() => undefined);
      } catch {
        // A source that fails to stop has nothing more to give.
      }
    },
  };
}

/**
 * A stream the browser sends. The source is read only as the server grants credit. The promise settles with the
 * server's Response, or earlier with the reason the upload stopped: its signal, its timeout, or its source's failure.
 * The id stays taken until the server has answered and the browser has sent StreamEnd or Cancel.
 */
export class Upload implements Exchange {
  readonly upload = true;
  readonly initialCredit = 0;
  readonly name: string;
  readonly payload: Payload;
  readonly promise: Promise<unknown>;

  readonly #host: StreamQueue;
  readonly #source: SourceReader;
  readonly #signal: AbortSignal | undefined;
  readonly #resolve: (value: unknown) => void;
  readonly #reject: (error: unknown) => void;
  readonly #onAbort = () => this.#stop(cancelled());

  #id = 0;
  #link: StreamLink | undefined;
  #state: "queued" | "open" | "done" = "queued";
  #credit = 0;
  #reading = false;
  /** StreamEnd or Cancel went out. */
  #ended = false;
  /** The server's Response arrived. */
  #answered = false;
  #settled = false;
  #timer: ReturnType<typeof setTimeout> | undefined;

  constructor(
    host: StreamQueue,
    name: string,
    payload: Payload,
    source: UploadSource<unknown>,
    signal: AbortSignal | undefined,
    timeout: number | undefined,
  ) {
    this.#host = host;
    this.name = name;
    this.payload = payload;
    this.#signal = signal;
    const { promise, resolve, reject } = Promise.withResolvers<unknown>();
    this.promise = promise;
    this.#resolve = resolve;
    this.#reject = reject;
    this.#source = readerOf(source, () => (this.#link as StreamLink).maxItemBytes());

    if (signal?.aborted) {
      this.#state = "done";
      this.#settle(cancelled());
      this.#source.release();
      return;
    }

    signal?.addEventListener("abort", this.#onAbort, { once: true });
    if (timeout !== undefined) {
      this.#timer = setTimeout(() => this.#stop(new CallError("timeout", "The upload's deadline passed.")), timeout);
    }

    host.open(this);
  }

  // The connection's side.

  opened(id: number, link: StreamLink): void {
    this.#link = link;
    this.#id = id;
    this.#state = "open";
  }

  refused(error: CallError): void {
    this.#state = "done";
    this.#settle(error);
    this.#source.release();
  }

  closed(error: CallError): void {
    if (this.#state === "done") return;
    this.#state = "done";
    this.#settle(error);
    this.#source.release();
  }

  onCredit(amount: number): void {
    this.#credit += amount;
    void this.#pump();
  }

  /** The server's Response: error when it failed, otherwise the result. */
  onResponse(error: CallError | undefined, result: unknown): void {
    this.#answered = true;
    if (error === undefined) this.#settle(undefined, result);
    else this.#settle(error);

    // An answer before the source ended: stop reading and end the stream.
    if (!this.#ended) {
      this.#ended = true;
      this.#link?.end(this.#id, undefined);
      this.#source.release();
    }

    this.#release();
  }

  get answered(): boolean {
    return this.#answered;
  }

  async #pump(): Promise<void> {
    if (this.#reading) return;
    this.#reading = true;
    try {
      while (this.#credit > 0 && this.#sending) {
        let next: IteratorResult<unknown>;
        try {
          next = await this.#source.next();
        } catch (error) {
          this.#fail(error);
          return;
        }

        // The upload stopped while the source was read: the item has nowhere to go.
        if (!this.#sending) return;
        if (next.done) {
          this.#ended = true;
          this.#link?.end(this.#id, undefined);
          this.#release();
          return;
        }

        let payload: Payload;
        try {
          payload = encodePayload(next.value);
        } catch (error) {
          this.#fail(error);
          return;
        }

        this.#credit--;
        if (!this.#link?.item(this.#id, payload)) {
          this.#fail(new CallError("payload_too_large", `An item of ${this.name} is larger than the server accepts.`));
          return;
        }
      }
    } finally {
      this.#reading = false;
    }
  }

  get #sending(): boolean {
    return this.#state === "open" && !this.#ended && !this.#answered;
  }

  // The source failed, or gave an item that cannot be sent: a failed StreamEnd tells the server's handler, and the
  // promise rejects with the reason.
  #fail(error: unknown): void {
    this.#ended = true;
    this.#link?.end(this.#id, sourceError(error));
    this.#settle(error);
    this.#source.release();
    this.#release();
  }

  // The signal or the deadline: Cancel, which stops the server's handler even after StreamEnd, until it answers.
  #stop(error: CallError): void {
    if (this.#settled) return;
    this.#settle(error);
    if (this.#state === "queued") {
      this.#state = "done";
      this.#host.dequeue(this);
    } else if (this.#state === "open" && !this.#answered) {
      this.#ended = true;
      this.#link?.cancel(this.#id);
    }

    this.#source.release();
  }

  #release(): void {
    if (this.#state !== "open" || !this.#ended || !this.#answered) return;
    this.#state = "done";
    this.#link?.release(this.#id);
  }

  #settle(error: unknown, value?: unknown): void {
    if (this.#settled) return;
    this.#settled = true;
    clearTimeout(this.#timer);
    this.#signal?.removeEventListener("abort", this.#onAbort);
    if (error === undefined) this.#resolve(value);
    else this.#reject(error);
  }
}

// What the server's handler sees of a source's failure: the CallError's own code, or handler_failed.
function sourceError(error: unknown): CallError {
  if (error instanceof CallError) return error;
  return new CallError("handler_failed", error instanceof Error ? error.message : String(error));
}
