import { describe, expect, it } from "vitest";
import type { ConnectionState, StateChange } from "../../src/index.ts";
import { open, until } from "./contract.ts";

const pause = (milliseconds: number) => new Promise((resolve) => setTimeout(resolve, milliseconds));

// The server cuts the connection with misc.drop. The session stays, and the client resumes it.
describe("resume", () => {
  function watched() {
    const conn = open({ reconnect: { delays: [0], jitter: 0 } });
    const states: [ConnectionState, StateChange][] = [];
    conn.onStateChange((state, change) => states.push([state, change]));
    return { conn, states };
  }

  it("resumes after the connection drops, losing nothing", async () => {
    const { conn, states } = watched();
    const delayed = conn.api.calls.delay(300);
    await conn.opened;

    await conn.api.misc.drop();
    expect(await delayed).toBe("done");
    expect(states.map(([state]) => state)).toEqual(["open", "reconnecting", "open"]);
    expect(states.at(-1)).toEqual(["open", { resumed: true }]);
  });

  it("delivers events the server sent while the connection was down", async () => {
    const { conn } = watched();
    const received: string[] = [];
    conn.on.events.received((text) => received.push(text));

    await conn.api.events.later("while away");
    await conn.api.misc.drop();

    await until(() => received.length > 0);
    expect(received).toEqual(["while away"]);
  });

  it("sends calls made while reconnecting once it resumed", async () => {
    const { conn } = watched();
    await conn.opened;
    const { promise: made, resolve } = Promise.withResolvers<Promise<string>>();
    conn.onStateChange((state) => {
      if (state === "reconnecting") resolve(conn.api.echo.text("after"));
    });

    const drop = conn.api.misc.drop();
    expect(await await made).toBe("after");
    await drop;
  });

  it("carries a stream across a resume", async () => {
    const { conn } = watched();
    const stream = conn.streams.feed.count(50, { buffer: 4 });
    const numbers: number[] = [];
    for (let i = 0; i < 5; i++) numbers.push((await stream.next()).value as number);

    await conn.api.misc.drop();
    for await (const number of stream) numbers.push(number);
    expect(numbers).toEqual(Array.from({ length: 50 }, (_, i) => i + 1));
  });

  it("carries an upload across a resume", async () => {
    const { conn } = watched();
    let pulled = 0;
    async function* numbers() {
      for (let i = 1; i <= 50; i++) {
        pulled = i;
        await pause(2);
        yield i;
      }
    }

    const sum = conn.uploads.store.sum(numbers());
    await until(() => pulled >= 10);
    await conn.api.misc.drop();
    expect(await sum).toBe(1275);
  });

  it("keeps the server's handlers to the browser across a resume", async () => {
    const { conn } = watched();
    conn.handle.ui.confirm(async (question) => {
      await pause(100);
      return question.text === "sure?";
    });

    // Opened first: a reset right after connecting can lose the Welcome, and a session without one cannot resume.
    await conn.opened;
    const answer = conn.api.browser.confirm("sure?");
    await pause(20);
    await conn.api.misc.drop();
    expect(await answer).toBe("True");
  });
});

describe("chaos", () => {
  it("loses and repeats nothing, either way, while the connection keeps dropping", async () => {
    const conn = open({ reconnect: { delays: [0], jitter: 0 } });
    const received: string[] = [];
    conn.on.events.received((text) => received.push(text));
    await conn.opened;

    const work: Promise<unknown>[] = [];
    for (let round = 0; round < 5; round++) {
      for (let i = 0; i < 20; i++) {
        const n = round * 20 + i;
        work.push(conn.api.echo.text(`call ${n}`).then((text) => expect(text).toBe(`call ${n}`)));
        work.push(conn.api.events.echo(`event ${n}`));
        conn.emit.telemetry.click({ x: n, y: round });
      }

      work.push(conn.api.misc.drop());
      await pause(30);
    }

    await Promise.all(work);
    const clicks = await conn.api.telemetry.waitFor(100);
    await until(() => received.length >= 100);
    await pause(100);

    const expected = Array.from({ length: 100 }, (_, n) => `event ${n}`);
    expect([...received].sort()).toEqual(expected.sort());
    expect(clicks.map((click) => click.x).sort((a, b) => a - b)).toEqual(Array.from({ length: 100 }, (_, n) => n));
  });
});

// The page lifecycle exists only in browsers.
describe.skipIf(typeof document === "undefined")("page lifecycle", () => {
  it("leaves the back/forward cache usable, and resumes on pageshow", async () => {
    const conn = open();
    const states: [ConnectionState, StateChange][] = [];
    conn.onStateChange((state, change) => states.push([state, change]));
    await conn.opened;
    const delayed = conn.api.calls.delay(200);

    window.dispatchEvent(new PageTransitionEvent("pagehide", { persisted: true }));
    await until(() => conn.state === "reconnecting");
    await pause(300);
    expect(conn.state).toBe("reconnecting");

    window.dispatchEvent(new PageTransitionEvent("pageshow", { persisted: true }));
    expect(await delayed).toBe("done");
    expect(states.at(-1)).toEqual(["open", { resumed: true }]);
  });

  it("tries at once when the browser is back online", async () => {
    const conn = open({ reconnect: { delays: [60_000], jitter: 0 } });
    await conn.opened;
    const drop = conn.api.misc.drop();
    await until(() => conn.state === "reconnecting");

    window.dispatchEvent(new Event("online"));
    await drop;
    expect(conn.state).toBe("open");
  });
});
