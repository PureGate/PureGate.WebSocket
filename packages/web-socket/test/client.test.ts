import { afterEach, describe, expect, it, vi } from "vitest";
import { type ConnectOptions, connect, type WebSocketConstructor } from "../src/index.ts";
import { FrameFlags, FrameKind } from "../src/protocol/frames.ts";
import { FakeSocket, frame, settle } from "./fake-socket.ts";

interface Contract {
  api: { echo: { text(text: string): string }; slow: { call(): void } };
  events: { news: { posted(text: string): void } };
  clientApi: { ui: { confirm(text: string): boolean } };
  clientEvents: { telemetry: { click(x: number): void } };
}

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

const encoder = new TextEncoder();

afterEach(() => {
  vi.useRealTimers();
});

describe("client", () => {
  it("offers the subprotocol and sends Hello first, with the calls made before opening", async () => {
    const { conn, socket } = start({ calls: 3, maxMessage: 20_000, contractHash: "abc" });
    const call = conn.api.echo.text("early");
    expect(socket.protocols).toBe("puregate.v1");
    expect(socket.binaryType).toBe("arraybuffer");

    socket.open();
    await settle();
    const [hello, request] = socket.sent[0] ?? [];
    expect(hello?.kind).toBe(FrameKind.Hello);
    expect(JSON.parse(new TextDecoder().decode(hello?.payload))).toEqual({
      calls: 3,
      maxMessage: 20_000,
      contract: "abc",
    });
    expect(request).toMatchObject({ kind: FrameKind.Request, id: 1, nameCode: 3, nameText: "echo.text" });

    socket.welcome();
    socket.receive(frame(FrameKind.Response, { id: 1, payload: encoder.encode('"early"') }));
    expect(await call).toBe("early");
  });

  it("sends the token in Hello", async () => {
    const { socket } = start({ token: async () => "secret" });
    socket.open();
    await settle();

    expect(JSON.parse(new TextDecoder().decode(socket.sent[0]?.[0]?.payload)).token).toBe("secret");
  });

  it("keeps to four calls before Welcome, then to the server's window", async () => {
    const { conn, socket } = start();
    for (let i = 0; i < 10; i++) void conn.api.echo.text(`${i}`).catch(() => undefined);
    socket.open();
    await settle();
    expect(socket.sent.flat().filter((sent) => sent.kind === FrameKind.Request)).toHaveLength(4);

    socket.welcome({ calls: 6 });
    await settle();
    expect(socket.sent.flat().filter((sent) => sent.kind === FrameKind.Request)).toHaveLength(6);
  });

  it("refuses more queued calls than maxQueued", async () => {
    const { conn } = start({ maxQueued: 1 });
    void conn.api.echo.text("queued").catch(() => undefined);

    await expect(conn.api.echo.text("one too many")).rejects.toMatchObject({ code: "overloaded" });
  });

  it("closes with a protocol error when the server breaks the protocol", async () => {
    const { conn, socket, errors } = start();
    socket.open();
    await settle();
    socket.welcome();
    socket.receive(frame(FrameKind.Response, { id: 99 }));

    expect(socket.closedWith?.code).toBe(4000);
    expect(conn.state).toBe("closed");
    expect(errors[0]).toMatchObject({ violation: "unknown_exchange" });
  });

  it.each([
    ["a frame before Welcome", (socket: FakeSocket) => socket.receive(frame(FrameKind.Ping)), "unexpected_frame"],
    ["a Welcome below the minimums", (socket: FakeSocket) => socket.welcome({ calls: 1 }), "invalid_handshake"],
    [
      "a Welcome that is not JSON",
      (socket: FakeSocket) => socket.receive(frame(FrameKind.Welcome, { payload: encoder.encode("{") })),
      "invalid_handshake",
    ],
  ])("refuses %s", async (_, act, violation) => {
    const { conn, socket, errors } = start();
    socket.open();
    await settle();
    act(socket);

    await expect(conn.opened).rejects.toMatchObject({ code: "connection_closed" });
    expect(errors[0]).toMatchObject({ violation });
  });

  it("closes when the server does not select the subprotocol", async () => {
    const { socket } = start();
    socket.protocol = "";
    socket.open();

    // Browsers only send 1000 and 3000–4999 themselves.
    expect(socket.closedWith?.code).toBe(4000);
  });

  it("closes when the server answers more calls at once than the browser takes", async () => {
    const { conn, socket, errors } = start({ calls: 1 });
    conn.handle.ui.confirm(() => new Promise<boolean>(() => undefined));
    socket.open();
    await settle();
    socket.welcome();
    const ask = (id: number) =>
      frame(FrameKind.Request, { id, nameCode: 0, nameText: "ui.confirm", payload: encoder.encode('"?"') });
    socket.receive(ask(1), ask(2));

    expect(errors[0]).toMatchObject({ violation: "window_exceeded" });
  });

  it("pings after a heartbeat of silence, and reconnects after two", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "performance"] });
    const { conn, socket } = start({ reconnect: { delays: [100], jitter: 0 } });
    socket.open();
    await vi.advanceTimersByTimeAsync(0);
    socket.welcome({ heartbeat: 1000 });

    await vi.advanceTimersByTimeAsync(1000);
    expect(socket.frames()).toContain("Ping");
    socket.receive(frame(FrameKind.Pong));
    await vi.advanceTimersByTimeAsync(1500);
    expect(conn.state).toBe("open");

    await vi.advanceTimersByTimeAsync(2500);
    expect(socket.closedWith?.code).toBe(4006);
    expect(conn.state).toBe("reconnecting");
    await vi.advanceTimersByTimeAsync(100);
    expect(FakeSocket.last).not.toBe(socket);
  });

  it("returns event credit to the window", async () => {
    const { conn, socket } = start();
    socket.open();
    await settle();
    socket.welcome({ events: 16 });
    for (let x = 0; x < 20; x++) conn.emit.telemetry.click(x);
    await settle();
    expect(socket.sent.flat().filter((sent) => sent.kind === FrameKind.Event)).toHaveLength(16);

    socket.receive(frame(FrameKind.Credit, { id: 0, amount: 4 }));
    await settle();
    expect(socket.sent.flat().filter((sent) => sent.kind === FrameKind.Event)).toHaveLength(20);
  });

  it("answers the server's Ping and binary calls", async () => {
    const { conn, socket } = start();
    conn.handle.ui.confirm((request) => request as unknown as boolean);
    socket.open();
    await settle();
    socket.welcome();
    socket.receive(
      frame(FrameKind.Ping),
      frame(FrameKind.Request, {
        id: 5,
        flags: FrameFlags.Binary,
        nameCode: 0,
        nameText: "ui.confirm",
        payload: Uint8Array.of(9),
      }),
    );
    await settle();

    const frames = socket.frames();
    expect(frames).toContain("Pong");
    expect(frames).toContain("Response binary id=5 payload=09");
  });

  it("names a section's path only when it is called", () => {
    const { conn } = start();

    expect(() => (conn.api as unknown as () => unknown)()).toThrow(TypeError);
    expect((conn.api as unknown as { then?: unknown }).then).toBeUndefined();
    expect(conn.api.echo).toBe(conn.api.echo);
  });
});
