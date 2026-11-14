import { describe, expect, it } from "vitest";
import { CallError } from "../../src/index.ts";
import { open, until } from "./contract.ts";

const pause = (milliseconds: number) => new Promise((resolve) => setTimeout(resolve, milliseconds));

async function collect<T>(items: AsyncIterable<T>): Promise<T[]> {
  const collected: T[] = [];
  for await (const item of items) collected.push(item);
  return collected;
}

describe("streams", () => {
  it("reads a stream to its end", async () => {
    const conn = open();

    expect(await collect(conn.streams.feed.count(5))).toEqual([1, 2, 3, 4, 5]);
  });

  it("reads binary items", async () => {
    const conn = open();

    expect(await collect(conn.streams.feed.bytes(2))).toEqual([Uint8Array.of(0, 0, 0), Uint8Array.of(1, 1, 1)]);
  });

  it("pulls the server's producer only as far as the buffer", async () => {
    const conn = open();
    const stream = conn.streams.feed.notes(undefined, { buffer: 4 });

    expect((await stream.next()).value).toEqual({ text: "tick", number: 1 });
    await until(async () => (await conn.api.feed.produced()) === 4);
    await pause(100);
    expect(await conn.api.feed.produced()).toBe(4);

    // Half the buffer taken: the server may produce two more.
    expect((await stream.next()).value).toEqual({ text: "tick", number: 2 });
    await until(async () => (await conn.api.feed.produced()) === 6);
    stream.cancel();
  });

  it("stops the server's producer when the loop breaks", async () => {
    const conn = open();
    const numbers: number[] = [];
    for await (const note of conn.streams.feed.notes()) {
      numbers.push(note.number);
      if (numbers.length === 3) break;
    }

    expect(numbers).toEqual([1, 2, 3]);
    await until(async () => (await conn.api.feed.cancelled()) === 1);
  });

  it("opens a stream only when it is read", async () => {
    const conn = open();
    const stream = conn.streams.feed.notes();
    await conn.opened;
    await pause(50);

    expect(await conn.api.feed.produced()).toBe(0);
    stream.cancel();
    expect(await stream.next()).toEqual({ value: undefined, done: true });
  });

  it("throws the server's error once the items before it are read", async () => {
    const conn = open();
    const numbers: number[] = [];
    let failure: unknown;
    try {
      for await (const number of conn.streams.feed.fail(2)) numbers.push(number);
    } catch (error) {
      failure = error;
    }

    expect(numbers).toEqual([1, 2]);
    expect(failure).toBeInstanceOf(CallError);
    expect(failure).toMatchObject({ code: "broken", message: "The feed broke.", details: { id: 2 } });
    await expect(collect(conn.streams.feed.crash())).rejects.toMatchObject({
      code: "handler_failed",
      traceId: expect.stringMatching(/^[\da-f]{32}$/),
    });
  });

  it("throws cancelled when the signal aborts, dropping what was buffered", async () => {
    const conn = open();
    const controller = new AbortController();
    const stream = conn.streams.feed.notes(undefined, { signal: controller.signal });
    expect((await stream.next()).value).toEqual({ text: "tick", number: 1 });

    controller.abort();
    await expect(stream.next()).rejects.toMatchObject({ code: "cancelled" });
    expect(await stream.next()).toEqual({ value: undefined, done: true });
    await until(async () => (await conn.api.feed.cancelled()) === 1);
  });

  it("aborts streams with the view's signal", async () => {
    const conn = open();
    const controller = new AbortController();
    const stream = conn.with({ signal: controller.signal }).streams.feed.notes();
    await stream.next();

    controller.abort();
    await until(async () => (await conn.api.feed.cancelled()) === 1);
  });

  it("runs more streams than the window holds, in turn", async () => {
    const conn = open();
    const streams = Array.from({ length: 10 }, (_, i) => collect(conn.streams.feed.count(i)));

    expect(await Promise.all(streams)).toEqual(
      Array.from({ length: 10 }, (_, i) => Array.from({ length: i }, (__, j) => j + 1)),
    );
  });

  it("fails a stream whose item is larger than the browser accepts", async () => {
    const conn = open({ maxMessage: 16 * 1024 });
    const items: string[] = [];
    let failure: unknown;
    try {
      for await (const item of conn.streams.feed.huge(20_000)) items.push(item);
    } catch (error) {
      failure = error;
    }

    expect(items).toEqual(["first"]);
    expect(failure).toMatchObject({ code: "payload_too_large" });
  });

  it("ends streams with connection_closed when the connection closes", async () => {
    const conn = open();
    const stream = conn.streams.feed.notes();
    await stream.next();

    // Items that arrived before the close are still read, then the stream fails.
    await conn.close();
    await expect(collect(stream)).rejects.toMatchObject({ code: "connection_closed" });
  });
});

describe("uploads", () => {
  it("uploads an array", async () => {
    const conn = open();

    expect(await conn.uploads.store.sum(Array.from({ length: 20 }, (_, i) => i + 1))).toBe(210);
  });

  it("uploads with a request", async () => {
    const conn = open();

    expect(await conn.uploads.store.join("-", ["a", "b", "c"])).toBe("a-b-c");
  });

  it("uploads from an async generator", async () => {
    const conn = open();
    async function* numbers() {
      for (let i = 1; i <= 10; i++) {
        await pause(1);
        yield i;
      }
    }

    expect(await conn.uploads.store.sum(numbers())).toBe(55);
  });

  it("uploads bytes from a ReadableStream", async () => {
    const conn = open();
    const chunks = [Uint8Array.of(1, 2, 3), Uint8Array.of(4, 5), new Uint8Array(1000)];
    const source = new ReadableStream<Uint8Array>({
      pull(controller) {
        const chunk = chunks.shift();
        if (chunk === undefined) controller.close();
        else controller.enqueue(chunk);
      },
    });

    expect(await conn.uploads.store.size(source)).toBe(1005);
  });

  it("uploads a Blob's stream, whose chunks are larger than a message", async () => {
    const conn = open();
    const bytes = new Uint8Array(1_000_000).map((_, i) => i % 251);

    expect(await conn.uploads.store.size(new Blob([bytes]).stream())).toBe(1_000_000);
  });

  it("uploads without a result", async () => {
    const conn = open();

    expect(
      await conn.uploads.store.count("notes", [
        { text: "a", number: 1 },
        { text: "b", number: 2 },
      ]),
    ).toBeUndefined();
    expect(await conn.api.store.counted()).toBe("notes:2");
    expect(await conn.uploads.store.drain(["last"])).toBeUndefined();
    expect(await conn.api.store.counted()).toBe("last");
  });

  it("reads the source only as the server grants credit, and stops at the answer", async () => {
    const conn = open();
    let pulled = 0;
    let released = false;
    async function* endless() {
      try {
        for (;;) {
          pulled++;
          yield pulled;
        }
      } finally {
        released = true;
      }
    }

    expect(await conn.uploads.store.first(endless())).toBe(1);
    await until(() => released);
    // The server's upload window is 4, and it answered before granting more.
    expect(pulled).toBeLessThanOrEqual(4);
  });

  it("rejects with the handler's error", async () => {
    const conn = open();

    await expect(conn.uploads.store.fail(2, [5, 6, 7])).rejects.toMatchObject({
      code: "rejected",
      message: "Item 6 was refused.",
      details: { id: 6 },
    });
  });

  it("rejects with the source's own error, and tells the server", async () => {
    const conn = open();
    const failure = new Error("The disk is gone.");
    async function* failing() {
      yield 1;
      throw failure;
    }

    await expect(conn.uploads.store.sum(failing())).rejects.toBe(failure);
    expect(await conn.api.math.add({ a: 1, b: 2 })).toBe(3);
  });

  it("cancels an upload with its signal", async () => {
    const conn = open();
    const controller = new AbortController();
    let pulled = 0;
    async function* slow() {
      for (;;) {
        await pause(5);
        yield ++pulled;
      }
    }

    // Pulled items mean the server granted credit: the upload is open there, not still waiting in the browser.
    const upload = conn.uploads.store.hold(slow(), { signal: controller.signal });
    await until(() => pulled >= 2);
    controller.abort();

    await expect(upload).rejects.toMatchObject({ code: "cancelled" });
    await until(async () => (await conn.api.store.cancelled()) === 1);
  });

  it("cancels an upload whose source has ended, while its handler runs", async () => {
    const conn = open();
    // Open first, so the deadline does not pass while the upload waits for the connection.
    await conn.opened;

    await expect(conn.uploads.store.hold([1, 2], { timeout: 200 })).rejects.toMatchObject({ code: "timeout" });
    await until(async () => (await conn.api.store.cancelled()) === 1);
  });

  it("refuses something that is not a source", async () => {
    const conn = open();

    await expect(conn.uploads.store.sum(42 as unknown as number[])).rejects.toBeInstanceOf(TypeError);
  });

  it("runs more uploads than the window holds, in turn", async () => {
    const conn = open();
    const uploads = Array.from({ length: 10 }, (_, i) => conn.uploads.store.sum(Array.from({ length: i }, () => 1)));

    expect(await Promise.all(uploads)).toEqual(Array.from({ length: 10 }, (_, i) => i));
  });

  it("shares the window and the ids with calls and downloads", async () => {
    const conn = open();
    const work = await Promise.all([
      conn.uploads.store.sum([1, 2, 3]),
      collect(conn.streams.feed.count(3)),
      conn.api.math.add({ a: 2, b: 3 }),
      conn.uploads.store.join("+", ["x", "y"]),
      collect(conn.streams.feed.bytes(1)),
    ]);

    expect(work).toEqual([6, [1, 2, 3], 5, "x+y", [Uint8Array.of(0, 0, 0)]]);
  });
});
