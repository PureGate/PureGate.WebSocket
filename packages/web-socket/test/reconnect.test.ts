import { afterEach, describe, expect, it, vi } from "vitest";
import {
  type ConnectionState,
  type ConnectOptions,
  connect,
  type StateChange,
  type WebSocketConstructor,
} from "../src/index.ts";
import { type Frame, FrameKind } from "../src/protocol/frames.ts";
import { FakeSocket, frame, settle } from "./fake-socket.ts";

interface Contract {
  api: { echo: { text(text: string): string } };
  events: { news: { posted(text: string): void } };
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const resumable = { session: "s1", resumeWindow: 30_000 };

function start(options: ConnectOptions = {}) {
  const errors: unknown[] = [];
  const states: [ConnectionState, StateChange][] = [];
  const conn = connect<Contract>("ws://test/ws", {
    WebSocket: FakeSocket as unknown as WebSocketConstructor,
    onError: (error) => errors.push(error),
    reconnect: { delays: [0], jitter: 0 },
    ...options,
  });
  conn.onStateChange((state, change) => states.push([state, change]));
  return { conn, errors, states, socket: FakeSocket.last as FakeSocket };
}

/** Opens the socket and answers with Welcome. */
async function welcome(socket: FakeSocket, fields: Record<string, unknown> = {}): Promise<void> {
  socket.open();
  await settle();
  socket.welcome(fields);
  await settle();
}

/** The socket the client opened next, once its reconnect timer ran. */
async function next(previous: FakeSocket): Promise<FakeSocket> {
  await settle();
  const socket = FakeSocket.last as FakeSocket;
  expect(socket).not.toBe(previous);
  return socket;
}

function hello(socket: FakeSocket): Record<string, unknown> {
  const first = socket.sent[0]?.[0] as Frame;
  expect(first.kind).toBe(FrameKind.Hello);
  return JSON.parse(decoder.decode(first.payload)) as Record<string, unknown>;
}

function sent(socket: FakeSocket, kind: FrameKind): Frame[] {
  return socket.sent.flat().filter((each) => each.kind === kind);
}

function response(id: number, text: string): Frame {
  return frame(FrameKind.Response, { id, payload: encoder.encode(JSON.stringify(text)) });
}

function event(text: string): Frame {
  return frame(FrameKind.Event, {
    nameCode: 0,
    nameText: "news.posted",
    payload: encoder.encode(JSON.stringify(text)),
  });
}

afterEach(() => {
  vi.useRealTimers();
});

describe("acknowledgement", () => {
  it("acknowledges every 64 frames, and the rest within a second", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const { conn, socket } = start();
    conn.on.news.posted(() => undefined);
    socket.open();
    await vi.advanceTimersByTimeAsync(0);
    socket.welcome();

    socket.receive(...Array.from({ length: 64 }, (_, i) => event(`${i}`)));
    await vi.advanceTimersByTimeAsync(0);
    expect(sent(socket, FrameKind.Ack)).toMatchObject([{ count: 64 }]);

    socket.receive(event("a"), event("b"));
    await vi.advanceTimersByTimeAsync(999);
    expect(sent(socket, FrameKind.Ack)).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(sent(socket, FrameKind.Ack)).toMatchObject([{ count: 64 }, { count: 66 }]);
  });

  it("counts events nobody listens to", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const { socket } = start();
    socket.open();
    await vi.advanceTimersByTimeAsync(0);
    socket.welcome();

    socket.receive(event("ignored"));
    await vi.advanceTimersByTimeAsync(1000);
    expect(sent(socket, FrameKind.Ack)).toMatchObject([{ count: 1 }]);
  });

  it("closes with a protocol error for an Ack past what was sent", async () => {
    const { socket } = start({ reconnect: false });
    await welcome(socket);

    socket.receive(frame(FrameKind.Ack, { count: 3 }));
    expect(socket.closedWith).toEqual({ code: 4000, reason: "unexpected_frame" });
  });
});

describe("reconnect", () => {
  it("resumes the session, sending again what the server did not process", async () => {
    const { conn, socket, states } = start();
    await welcome(socket, resumable);
    const first = conn.api.echo.text("one");
    const second = conn.api.echo.text("two");
    await settle();
    socket.receive(frame(FrameKind.Ack, { count: 1 }));

    socket.closeFromServer(1006);
    expect(conn.state).toBe("reconnecting");
    const waiting = conn.api.echo.text("three");

    const again = await next(socket);
    again.open();
    await settle();
    expect(hello(again).resume).toEqual({ session: "s1", received: 0 });
    // Nothing goes out before the Welcome of a resume.
    expect(again.sent.flat()).toHaveLength(1);

    again.welcome({ ...resumable, resumed: true, received: 1 });
    await settle();
    const requests = sent(again, FrameKind.Request);
    expect(requests.map((request) => request.id)).toEqual([2, 3]);
    expect(decoder.decode(requests[0]?.payload)).toBe('"two"');

    again.receive(response(1, "one"), response(2, "two"), response(3, "three"));
    expect(await Promise.all([first, second, waiting])).toEqual(["one", "two", "three"]);
    expect(states).toEqual([
      ["open", { resumed: false }],
      ["reconnecting", { code: 1006, reason: "" }],
      ["open", { resumed: true }],
    ]);
  });

  it("tells the server how much it processed, and keeps the name tables", async () => {
    const { conn, socket } = start();
    conn.on.news.posted(() => undefined);
    await welcome(socket, resumable);
    void conn.api.echo.text("a");
    socket.receive(event("x"), event("y"));
    await settle();

    socket.closeFromServer(1006);
    const again = await next(socket);
    again.open();
    await settle();
    expect(hello(again).resume).toEqual({ session: "s1", received: 2 });

    again.welcome({ ...resumable, resumed: true, received: 1 });
    void conn.api.echo.text("b");
    await settle();
    // echo.text was defined as name 1 on the first connection, so the resumed session refers to it.
    expect(sent(again, FrameKind.Request)).toMatchObject([{ id: 2, nameCode: 2 }]);
  });

  it("starts over when the server no longer has the session", async () => {
    const { conn, socket, states } = start();
    await welcome(socket, resumable);
    const lost = conn.api.echo.text("lost").catch((error: unknown) => error);
    await settle();

    socket.closeFromServer(1006);
    const waiting = conn.api.echo.text("kept");
    const again = await next(socket);
    await welcome(again, { session: "s2", resumeWindow: 30_000, resumed: false });

    expect(await lost).toMatchObject({ code: "connection_closed" });
    expect(sent(again, FrameKind.Request)).toMatchObject([{ id: 1, nameCode: 3, nameText: "echo.text" }]);
    again.receive(response(1, "kept"));
    expect(await waiting).toBe("kept");
    expect(states.at(-1)).toEqual(["open", { resumed: false }]);
  });

  it("fails what was in progress at once when the session cannot resume", async () => {
    const { conn, socket } = start();
    await welcome(socket);
    const lost = conn.api.echo.text("lost");
    await settle();

    socket.closeFromServer(1006);
    await expect(lost).rejects.toMatchObject({ code: "connection_closed" });

    const again = await next(socket);
    again.open();
    await settle();
    expect(hello(again).resume).toBeUndefined();
  });

  it("waits longer after each failed attempt, and again from the start once open", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const { socket } = start({ reconnect: { delays: [100, 500], jitter: 0 } });
    socket.closeFromServer(1006);
    await vi.advanceTimersByTimeAsync(99);
    expect(FakeSocket.last).toBe(socket);
    await vi.advanceTimersByTimeAsync(1);
    const second = FakeSocket.last as FakeSocket;
    expect(second).not.toBe(socket);

    second.closeFromServer(1006);
    await vi.advanceTimersByTimeAsync(499);
    expect(FakeSocket.last).toBe(second);
    await vi.advanceTimersByTimeAsync(1);
    const third = FakeSocket.last as FakeSocket;
    expect(third).not.toBe(second);

    third.open();
    await vi.advanceTimersByTimeAsync(0);
    third.welcome();
    third.closeFromServer(1006);
    await vi.advanceTimersByTimeAsync(100);
    expect(FakeSocket.last).not.toBe(third);
  });

  it("stays connecting until the first Welcome, and opens then", async () => {
    const { conn, socket, states } = start();
    socket.closeFromServer(1006);
    expect(conn.state).toBe("connecting");

    const again = await next(socket);
    await welcome(again);
    await conn.opened;
    expect(states).toEqual([["open", { resumed: false }]]);
  });

  it.each([1000, 1003, 1009, 4000, 4004, 4005])("closes for good after %i", async (code) => {
    const { conn, socket } = start();
    await welcome(socket, resumable);

    socket.closeFromServer(code);
    expect(conn.state).toBe("closed");
    await settle();
    expect(FakeSocket.last).toBe(socket);
  });

  it("closes for good when reconnecting is off", async () => {
    const { conn, socket } = start({ reconnect: false });
    await welcome(socket, resumable);

    socket.closeFromServer(1006);
    expect(conn.state).toBe("closed");
    await expect(conn.opened).resolves.toBeUndefined();
  });

  it("rejects opened when it closes for good before opening", async () => {
    const { conn, socket } = start();
    socket.closeFromServer(4004);

    await expect(conn.opened).rejects.toMatchObject({ code: "connection_closed" });
  });

  it("tries once more after unauthorized, at once, then gives up", async () => {
    const tokens: string[] = [];
    let issued = 0;
    const { conn, socket } = start({
      reconnect: { delays: [60_000], jitter: 0 },
      token: () => {
        const token = `t${++issued}`;
        tokens.push(token);
        return token;
      },
    });
    await welcome(socket, resumable);

    socket.closeFromServer(4001);
    const again = await next(socket);
    again.open();
    await settle();
    expect(hello(again)).toMatchObject({ token: "t2", resume: { session: "s1" } });

    again.closeFromServer(4001);
    expect(conn.state).toBe("closed");
  });

  it("adds the route to the URL of a resume", async () => {
    const { socket } = start();
    await welcome(socket, { ...resumable, route: "east-2" });

    socket.closeFromServer(1006);
    const again = await next(socket);
    expect(again.url).toBe("ws://test/ws?route=east-2");
  });

  it("closes at once while reconnecting", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const { conn, socket } = start({ reconnect: { delays: [1000], jitter: 0 } });
    await vi.advanceTimersByTimeAsync(0);
    socket.open();
    await vi.advanceTimersByTimeAsync(0);
    socket.welcome(resumable);
    const call = conn.api.echo.text("lost");
    await vi.advanceTimersByTimeAsync(0);
    socket.closeFromServer(1006);

    await conn.close();
    expect(conn.state).toBe("closed");
    await expect(call).rejects.toMatchObject({ code: "connection_closed" });
    await vi.advanceTimersByTimeAsync(1000);
    expect(FakeSocket.last).toBe(socket);
  });

  it("closes with a protocol error for a resumed Welcome it did not ask for", async () => {
    const { socket } = start();
    socket.open();
    await settle();

    socket.welcome({ resumed: true, received: 0 });
    expect(socket.closedWith).toEqual({ code: 4000, reason: "invalid_handshake" });
  });

  it("closes with a protocol error for a resume that counts more than was sent", async () => {
    const { socket } = start();
    await welcome(socket, resumable);
    socket.closeFromServer(1006);
    const again = await next(socket);
    again.open();
    await settle();

    again.welcome({ ...resumable, resumed: true, received: 5 });
    expect(again.closedWith).toEqual({ code: 4000, reason: "invalid_handshake" });
  });
});

describe("going away", () => {
  it("moves new work to a new connection while the old one finishes", async () => {
    const { conn, socket, states } = start();
    await welcome(socket, resumable);
    const old = conn.api.echo.text("old");
    const lost = conn.api.echo.text("lost");
    await settle();

    socket.receive(frame(FrameKind.GoAway));
    expect(conn.state).toBe("reconnecting");
    const fresh = FakeSocket.last as FakeSocket;
    expect(fresh).not.toBe(socket);
    const newer = conn.api.echo.text("new");
    await welcome(fresh, { session: "s2", resumeWindow: 30_000 });
    expect(sent(fresh, FrameKind.Request)).toMatchObject([{ id: 1 }]);
    expect(sent(socket, FrameKind.Request)).toHaveLength(2);

    socket.receive(response(1, "old"));
    fresh.receive(response(1, "new"));
    expect(await old).toBe("old");
    expect(await newer).toBe("new");

    // The old connection closes when the server has drained: what it still had fails.
    socket.closeFromServer(1001);
    await expect(lost).rejects.toMatchObject({ code: "connection_closed" });
    expect(conn.state).toBe("open");
    expect(states.slice(1)).toEqual([
      ["reconnecting", {}],
      ["open", { resumed: false }],
    ]);
  });
});
