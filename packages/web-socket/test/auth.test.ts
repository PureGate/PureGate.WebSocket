import { afterEach, describe, expect, it, vi } from "vitest";
import { type ConnectOptions, connect, type WebSocketConstructor } from "../src/index.ts";
import { type Frame, FrameKind } from "../src/protocol/frames.ts";
import { FakeSocket, frame } from "./fake-socket.ts";

interface Contract {
  api: { echo: { text(text: string): string } };
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function start(options: ConnectOptions = {}) {
  const conn = connect<Contract>("ws://test/ws", {
    WebSocket: FakeSocket as unknown as WebSocketConstructor,
    onError: () => undefined,
    ...options,
  });
  return { conn, socket: FakeSocket.last as FakeSocket };
}

function authenticates(socket: FakeSocket): string[] {
  return socket.sent
    .flat()
    .filter((each: Frame) => each.kind === FrameKind.Authenticate)
    .map((each) => (JSON.parse(decoder.decode(each.payload)) as { token: string }).token);
}

afterEach(() => {
  vi.useRealTimers();
});

describe("refreshing authentication", () => {
  it("refreshes a minute before expiry, then halfway when less than two minutes are left", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    let issued = 0;
    const { socket } = start({ token: () => `t${++issued}` });
    socket.open();
    await vi.advanceTimersByTimeAsync(0);
    socket.welcome({ expires: 600_000, heartbeat: 3_600_000 });

    await vi.advanceTimersByTimeAsync(539_999);
    expect(authenticates(socket)).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(authenticates(socket)).toEqual(["t2"]);

    socket.receive(frame(FrameKind.Authenticated, { payload: encoder.encode('{"expires":100000}') }));
    await vi.advanceTimersByTimeAsync(50_000);
    expect(authenticates(socket)).toEqual(["t2", "t3"]);

    // An answer without expires means the authentication no longer expires.
    socket.receive(frame(FrameKind.Authenticated, { payload: encoder.encode("{}") }));
    await vi.advanceTimersByTimeAsync(10_000_000);
    expect(authenticates(socket)).toEqual(["t2", "t3"]);
  });

  it("does not refresh without a token provider, or without an expiry", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const cookies = start();
    cookies.socket.open();
    await vi.advanceTimersByTimeAsync(0);
    cookies.socket.welcome({ expires: 60_000, heartbeat: 3_600_000 });

    const tokens = start({ token: () => "t" });
    tokens.socket.open();
    await vi.advanceTimersByTimeAsync(0);
    tokens.socket.welcome({ heartbeat: 3_600_000 });

    await vi.advanceTimersByTimeAsync(120_000);
    expect(authenticates(cookies.socket)).toEqual([]);
    expect(authenticates(tokens.socket)).toEqual([]);
  });

  it("reports a token provider that fails, and keeps the connection", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const errors: unknown[] = [];
    let calls = 0;
    const { conn, socket } = start({
      onError: (error) => errors.push(error),
      token: () => {
        if (++calls > 1) throw new Error("No token now.");
        return "t1";
      },
    });
    socket.open();
    await vi.advanceTimersByTimeAsync(0);
    socket.welcome({ expires: 1_000, heartbeat: 3_600_000 });

    await vi.advanceTimersByTimeAsync(500);
    expect(errors).toMatchObject([{ message: "No token now." }]);
    expect(conn.state).toBe("open");
  });

  it("closes with a protocol error for an Authenticated that is not JSON", () => {
    const { socket } = start({ reconnect: false });
    socket.open();
    return Promise.resolve().then(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
      socket.welcome();
      socket.receive(frame(FrameKind.Authenticated, { payload: encoder.encode("{") }));
      expect(socket.closedWith).toEqual({ code: 4000, reason: "invalid_handshake" });
    });
  });
});
