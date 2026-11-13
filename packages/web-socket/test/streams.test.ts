import { describe, expect, it } from "vitest";
import { CallError, type ConnectOptions, connect, type WebSocketConstructor } from "../src/index.ts";
import { errorCodeOf } from "../src/protocol/errors.ts";
import { type Frame, FrameFlags, FrameKind } from "../src/protocol/frames.ts";
import { FakeSocket, frame, settle } from "./fake-socket.ts";

interface Contract {
  streams: { feed: { ticks(): number; watch(symbol: string): number } };
  uploads: { store: { sum(item: number): number; put(name: string, item: Uint8Array): number } };
}

const encoder = new TextEncoder();

function start(options: ConnectOptions = {}) {
  const errors: unknown[] = [];
  const conn = connect<Contract>("ws://test/ws", {
    WebSocket: FakeSocket as unknown as WebSocketConstructor,
    onError: (error) => errors.push(error),
    ...options,
  });
  const socket = FakeSocket.last as FakeSocket;
  return { conn, socket, errors };
}

async function opened(options: ConnectOptions = {}, welcome: Record<string, unknown> = {}) {
  const started = start(options);
  started.socket.open();
  await settle();
  started.socket.welcome(welcome);
  await started.conn.opened;
  return started;
}

function sent(socket: FakeSocket, kind: FrameKind): Frame[] {
  return socket.sent.flat().filter((each) => each.kind === kind);
}

function item(id: number, value: unknown): Frame {
  return frame(FrameKind.StreamItem, { id, payload: encoder.encode(JSON.stringify(value)) });
}

describe("downloads", () => {
  it("opens with the buffer as credit and grants half of it as items are taken", async () => {
    const { conn, socket } = await opened({ streamBuffer: 4 });
    const stream = conn.streams.feed.watch("MSFT");
    const first = stream.next();
    await settle();

    const [open] = sent(socket, FrameKind.StreamOpen);
    expect(open).toMatchObject({ id: 1, nameText: "feed.watch", credit: 4, flags: 0 });
    expect(new TextDecoder().decode(open?.payload)).toBe('"MSFT"');

    socket.receive(item(1, 10), item(1, 11), item(1, 12));
    expect(await first).toEqual({ value: 10, done: false });
    await settle();
    expect(sent(socket, FrameKind.Credit)).toHaveLength(0);

    expect(await stream.next()).toEqual({ value: 11, done: false });
    await settle();
    expect(sent(socket, FrameKind.Credit)).toMatchObject([{ id: 1, amount: 2 }]);

    socket.receive(frame(FrameKind.StreamEnd, { id: 1 }));
    expect(await stream.next()).toEqual({ value: 12, done: false });
    expect(await stream.next()).toEqual({ value: undefined, done: true });
  });

  it("closes with a protocol error when the server sends past the credit", async () => {
    // The waiting read takes the first item, which leaves less than half the buffer taken: no credit goes back.
    const { conn, socket } = await opened({ streamBuffer: 4 });
    conn.streams.feed
      .ticks()
      .next()
      .catch(() => undefined);
    await settle();

    socket.receive(item(1, 1), item(1, 2), item(1, 3), item(1, 4), item(1, 5));
    expect(socket.closedWith).toEqual({ code: 4000, reason: "window_exceeded" });
  });

  it("closes with a protocol error for frames of streams it doesn't have", async () => {
    for (const unknown of [item(9, 1), frame(FrameKind.StreamEnd, { id: 9 }), frame(FrameKind.Response, { id: 9 })]) {
      const { socket } = await opened();
      socket.receive(unknown);
      expect(socket.closedWith).toEqual({ code: 4000, reason: "unknown_exchange" });
    }
  });

  it("closes with a protocol error when the server grants credit for a download", async () => {
    const { conn, socket } = await opened();
    conn.streams.feed
      .ticks()
      .next()
      .catch(() => undefined);
    await settle();

    socket.receive(frame(FrameKind.Credit, { id: 1, amount: 1 }));
    expect(socket.closedWith).toEqual({ code: 4000, reason: "unknown_exchange" });
  });

  it("cancels a stream whose item is not JSON, and keeps its id until StreamEnd", async () => {
    const { conn, socket } = await opened();
    const stream = conn.streams.feed.ticks();
    const next = stream.next();
    await settle();

    socket.receive(frame(FrameKind.StreamItem, { id: 1, payload: encoder.encode("{") }));
    await expect(next).rejects.toMatchObject({ code: "invalid_payload" });
    await settle();
    expect(sent(socket, FrameKind.Cancel)).toMatchObject([{ id: 1 }]);

    // Items still on the way are dropped; the id is busy until the server ends the stream.
    socket.receive(item(1, 2));
    conn.streams.feed
      .ticks()
      .next()
      .catch(() => undefined);
    await settle();
    expect(sent(socket, FrameKind.StreamOpen).at(-1)?.id).toBe(2);
    socket.receive(
      frame(FrameKind.StreamEnd, { id: 1, flags: FrameFlags.HasError, errorCode: errorCodeOf("cancelled") }),
    );
    expect(await stream.next()).toEqual({ value: undefined, done: true });
  });

  it("waits for the streams window, and takes back a stream cancelled while it waits", async () => {
    const { conn, socket } = await opened({}, { streams: 1 });
    const first = conn.streams.feed.ticks();
    const second = conn.streams.feed.ticks();
    const third = conn.streams.feed.ticks();
    first.next().catch(() => undefined);
    const secondNext = second.next();
    third.next().catch(() => undefined);
    await settle();
    expect(sent(socket, FrameKind.StreamOpen)).toHaveLength(1);

    second.cancel();
    expect(await secondNext).toEqual({ value: undefined, done: true });
    socket.receive(frame(FrameKind.StreamEnd, { id: 1 }));
    await settle();

    expect(sent(socket, FrameKind.StreamOpen)).toMatchObject([{ id: 1 }, { id: 2 }]);
  });

  it("opens one stream before Welcome", async () => {
    const { conn, socket } = start();
    conn.streams.feed
      .ticks()
      .next()
      .catch(() => undefined);
    conn.streams.feed
      .ticks()
      .next()
      .catch(() => undefined);
    socket.open();
    await settle();
    expect(sent(socket, FrameKind.StreamOpen)).toHaveLength(1);

    socket.welcome({ streams: 4 });
    await settle();
    expect(sent(socket, FrameKind.StreamOpen)).toHaveLength(2);
  });

  it("refuses streams past maxQueued, and requests the server cannot take", async () => {
    const { conn } = start({ maxQueued: 1 });
    conn.streams.feed
      .ticks()
      .next()
      .catch(() => undefined);
    await expect(conn.streams.feed.ticks().next()).rejects.toMatchObject({ code: "overloaded" });

    const { conn: small } = await opened({}, { maxMessage: 16 * 1024 });
    await expect(small.streams.feed.watch("x".repeat(20_000)).next()).rejects.toMatchObject({
      code: "payload_too_large",
    });
  });

  it("finishes at once for a signal that already aborted, sending nothing", async () => {
    const { conn, socket } = await opened();
    const stream = conn.streams.feed.ticks(undefined, { signal: AbortSignal.abort() });

    await expect(stream.next()).rejects.toMatchObject({ code: "cancelled" });
    expect(await stream.return()).toEqual({ value: undefined, done: true });
    await settle();
    expect(sent(socket, FrameKind.StreamOpen)).toHaveLength(0);
  });

  it("rejects a read that waits when the signal aborts", async () => {
    const { conn, socket } = await opened();
    const controller = new AbortController();
    const stream = conn.streams.feed.ticks(undefined, { signal: controller.signal });
    const next = stream.next();
    await settle();

    controller.abort();
    await expect(next).rejects.toMatchObject({ code: "cancelled" });
    await settle();
    expect(sent(socket, FrameKind.Cancel)).toMatchObject([{ id: 1 }]);
  });

  it("ends streams with connection_closed when the connection closes for good, including those still waiting", async () => {
    const { conn, socket } = await opened({ reconnect: false }, { streams: 1 });
    const open = conn.streams.feed.ticks().next();
    const waiting = conn.streams.feed.ticks().next();
    await settle();

    socket.closeFromServer(1001);
    await expect(open).rejects.toMatchObject({ code: "connection_closed" });
    await expect(waiting).rejects.toMatchObject({ code: "connection_closed" });
    await expect(conn.streams.feed.ticks().next()).rejects.toMatchObject({ code: "connection_closed" });
  });

  it("refuses a buffer that is not a whole number", async () => {
    const { conn } = await opened();

    expect(() => conn.streams.feed.ticks(undefined, { buffer: 0 })).toThrow(RangeError);
    expect(() => conn.streams.feed.ticks(undefined, { buffer: 1.5 })).toThrow(RangeError);
  });

  it("attaches a traceparent to StreamOpen", async () => {
    const { conn, socket } = await opened({ traceparent: () => "00-trace" });
    conn.streams.feed
      .ticks()
      .next()
      .catch(() => undefined);
    await settle();

    expect(sent(socket, FrameKind.StreamOpen)[0]).toMatchObject({ flags: FrameFlags.HasTrace, trace: "00-trace" });
  });
});

describe("uploads", () => {
  it("reads the source only as credit arrives, then ends the stream and waits for the answer", async () => {
    const { conn, socket } = await opened({}, { streams: 1 });
    let pulled = 0;
    function* numbers() {
      for (let i = 1; i <= 3; i++) {
        pulled++;
        yield i;
      }
    }

    const upload = conn.uploads.store.sum(numbers());
    const next = conn.uploads.store.sum([1]);
    await settle();
    expect(sent(socket, FrameKind.StreamOpen)).toMatchObject([{ id: 1, flags: FrameFlags.Upload, credit: 0 }]);
    expect(pulled).toBe(0);

    socket.receive(frame(FrameKind.Credit, { id: 1, amount: 2 }));
    await settle();
    expect(pulled).toBe(2);
    expect(sent(socket, FrameKind.StreamItem)).toHaveLength(2);

    socket.receive(frame(FrameKind.Credit, { id: 1, amount: 2 }));
    await settle();
    expect(sent(socket, FrameKind.StreamItem)).toHaveLength(3);
    expect(sent(socket, FrameKind.StreamEnd)).toMatchObject([{ id: 1, flags: 0 }]);

    socket.receive(frame(FrameKind.Response, { id: 1, payload: encoder.encode("6") }));
    expect(await upload).toBe(6);

    // Both sides are done: the upload left the window, and the next one opens.
    await settle();
    expect(sent(socket, FrameKind.StreamOpen)).toMatchObject([{ id: 1 }, { id: 2 }]);
    void next.catch(() => undefined);
  });

  it("stops reading at an early answer, and ends the stream", async () => {
    const { conn, socket } = await opened();
    let released = false;
    function* endless() {
      try {
        for (;;) yield 1;
      } finally {
        released = true;
      }
    }

    const upload = conn.uploads.store.sum(endless());
    await settle();
    socket.receive(frame(FrameKind.Credit, { id: 1, amount: 1 }));
    await settle();
    socket.receive(frame(FrameKind.Response, { id: 1, payload: encoder.encode("1") }));

    expect(await upload).toBe(1);
    await settle();
    expect(released).toBe(true);
    expect(sent(socket, FrameKind.StreamEnd)).toMatchObject([{ id: 1 }]);

    // Credit that crossed the answer is ignored.
    socket.receive(frame(FrameKind.Credit, { id: 1, amount: 5 }));
    expect(socket.closedWith).toBeUndefined();
  });

  it("sends a failed StreamEnd when the source throws, with the CallError's own code", async () => {
    const { conn, socket } = await opened();
    async function* failing(): AsyncGenerator<number> {
      yield 1;
      throw new Error("The disk is gone.");
    }
    async function* refusing(): AsyncGenerator<number> {
      yield* [];
      throw new CallError("not_ready", "Not yet.");
    }

    const first = conn.uploads.store.sum(failing());
    const second = conn.uploads.store.sum(refusing());
    await settle();
    socket.receive(frame(FrameKind.Credit, { id: 1, amount: 4 }), frame(FrameKind.Credit, { id: 2, amount: 4 }));

    await expect(first).rejects.toThrow("The disk is gone.");
    await expect(second).rejects.toMatchObject({ code: "not_ready" });
    await settle();
    expect(sent(socket, FrameKind.StreamEnd).sort((a, b) => a.id - b.id)).toMatchObject([
      {
        id: 1,
        flags: FrameFlags.HasError,
        errorCode: errorCodeOf("handler_failed"),
        errorMessage: "The disk is gone.",
      },
      { id: 2, flags: FrameFlags.HasError, errorCode: 0, errorCustom: "not_ready", errorMessage: "Not yet." },
    ]);
  });

  it("fails an upload whose item is larger than the server accepts, or cannot be JSON", async () => {
    const { conn, socket } = await opened({}, { maxMessage: 16 * 1024 });
    const large = conn.uploads.store.put("big", [new Uint8Array(20_000)]);
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    const unwritable = conn.uploads.store.sum([cyclic as unknown as number]);
    await settle();
    socket.receive(frame(FrameKind.Credit, { id: 1, amount: 1 }), frame(FrameKind.Credit, { id: 2, amount: 1 }));

    await expect(large).rejects.toMatchObject({ code: "payload_too_large" });
    await expect(unwritable).rejects.toBeInstanceOf(TypeError);
  });

  it("cancels on abort, even after the source ended, and drops the answer", async () => {
    const { conn, socket } = await opened();
    const controller = new AbortController();
    const upload = conn.uploads.store.sum([1], { signal: controller.signal });
    await settle();
    socket.receive(frame(FrameKind.Credit, { id: 1, amount: 4 }));
    await settle();
    expect(sent(socket, FrameKind.StreamEnd)).toHaveLength(1);

    controller.abort();
    await expect(upload).rejects.toMatchObject({ code: "cancelled" });
    await settle();
    expect(sent(socket, FrameKind.Cancel)).toMatchObject([{ id: 1 }]);

    socket.receive(
      frame(FrameKind.Response, { id: 1, flags: FrameFlags.HasError, errorCode: errorCodeOf("cancelled") }),
    );
    expect(socket.closedWith).toBeUndefined();
  });

  it("takes back an upload aborted before it opened", async () => {
    const { conn, socket } = await opened({}, { streams: 1 });
    conn.uploads.store.sum([1]).catch(() => undefined);
    const controller = new AbortController();
    const waiting = conn.uploads.store.sum([2], { signal: controller.signal });

    controller.abort();
    await expect(waiting).rejects.toMatchObject({ code: "cancelled" });
    await expect(conn.uploads.store.sum([3], { signal: AbortSignal.abort() })).rejects.toMatchObject({
      code: "cancelled",
    });
    await settle();
    expect(sent(socket, FrameKind.StreamOpen)).toHaveLength(1);
  });

  it("times out an upload that set a deadline", async () => {
    const { conn, socket } = await opened();
    const upload = conn.uploads.store.sum([1], { timeout: 10 });

    await expect(upload).rejects.toMatchObject({ code: "timeout" });
    await settle();
    expect(sent(socket, FrameKind.Cancel)).toMatchObject([{ id: 1 }]);
  });

  it("splits a ReadableStream's chunks to fit the server's messages, and sends an iterable's items as they are", async () => {
    const { conn, socket } = await opened({}, { maxMessage: 16 * 1024 });
    const chunk = new Uint8Array(40_000).map((_, i) => i % 7);
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(chunk);
        controller.close();
      },
    });

    void conn.uploads.store.put("file", stream).catch(() => undefined);
    await settle();
    socket.receive(frame(FrameKind.Credit, { id: 1, amount: 10 }));
    await settle();

    const pieces = sent(socket, FrameKind.StreamItem).map((each) => each.payload);
    expect(pieces.map((piece) => piece.length)).toEqual([16_372, 16_372, 7256]);
    expect(pieces.every((piece) => piece.length + 12 <= 16 * 1024)).toBe(true);
    expect(new Uint8Array(pieces.flatMap((piece) => [...piece]))).toEqual(chunk);

    const large = conn.uploads.store.put("file", [new Uint8Array(20_000)]);
    await settle();
    socket.receive(frame(FrameKind.Credit, { id: 2, amount: 1 }));
    await expect(large).rejects.toMatchObject({ code: "payload_too_large" });
  });

  it("tells a request from a source", async () => {
    const { conn, socket } = await opened();
    conn.uploads.store.put("name.bin", [Uint8Array.of(1)]).catch(() => undefined);
    conn.uploads.store.sum([1], {}).catch(() => undefined);
    await settle();

    const [put, sum] = sent(socket, FrameKind.StreamOpen);
    expect(new TextDecoder().decode(put?.payload)).toBe('"name.bin"');
    expect(sum?.payload).toHaveLength(0);
  });

  it("closes with a protocol error for a second answer", async () => {
    const { conn, socket } = await opened();
    void conn.uploads.store.sum([]).catch(() => undefined);
    await settle();

    socket.receive(frame(FrameKind.Response, { id: 1, payload: encoder.encode("0") }));
    socket.receive(frame(FrameKind.Response, { id: 1, payload: encoder.encode("0") }));
    expect(socket.closedWith).toEqual({ code: 4000, reason: "unknown_exchange" });
  });

  it("rejects uploads when the connection closes, and releases their sources", async () => {
    const { conn, socket } = await opened();
    let released = false;
    const source = new ReadableStream<number>({
      cancel() {
        released = true;
      },
    });

    const upload = conn.uploads.store.sum(source);
    await settle();
    socket.closeFromServer(1001);

    await expect(upload).rejects.toMatchObject({ code: "connection_closed" });
    await settle();
    expect(released).toBe(true);
  });
});
