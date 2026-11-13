import { describe, expect, it } from "vitest";
import { type ConnectOptions, connect, type WebSocketConstructor } from "../src/index.ts";
import { decodePayload, encodePayload } from "../src/payload.ts";
import { FrameFlags, FrameKind } from "../src/protocol/frames.ts";
import { FakeSocket, frame, settle } from "./fake-socket.ts";
import { markedJson, marker } from "./formats.ts";

interface Contract {
  api: { echo: { note(note: { text: string }): { text: string }; bytes(bytes: Uint8Array): Uint8Array } };
}

// In marked-json, unless the options say otherwise.
function start(options: ConnectOptions = { format: markedJson }) {
  const conn = connect<Contract>("ws://test/ws", {
    WebSocket: FakeSocket as unknown as WebSocketConstructor,
    onError: () => undefined,
    ...options,
  });
  return { conn, socket: FakeSocket.last as FakeSocket };
}

describe("payload formats", () => {
  it("writes and reads values in the format, and bytes as they are", () => {
    const written = encodePayload({ a: 1 }, markedJson);
    expect(written.binary).toBe(false);
    expect(written.bytes[0]).toBe(marker);
    expect(decodePayload(false, written.bytes, markedJson)).toEqual({ a: 1 });

    const bytes = Uint8Array.of(1, 2);
    expect(encodePayload(bytes, markedJson)).toEqual({ binary: true, bytes });
    expect(decodePayload(true, bytes, markedJson)).toBe(bytes);
    expect(encodePayload(undefined, markedJson).bytes).toHaveLength(0);
  });

  it("asks for the format in Hello, and calls in it", async () => {
    const { conn, socket } = start();
    const call = conn.api.echo.note({ text: "hi" });
    socket.open();
    await settle();

    const [hello, request] = socket.sent[0] ?? [];
    expect(JSON.parse(new TextDecoder().decode(hello?.payload))).toMatchObject({ format: "marked-json" });
    expect(request?.payload[0]).toBe(marker);
    expect(markedJson.decode(request?.payload ?? new Uint8Array())).toEqual({ text: "hi" });

    socket.welcome({ format: "marked-json" });
    socket.receive(frame(FrameKind.Response, { id: 1, payload: markedJson.encode({ text: "back" }) }));
    expect(await call).toEqual({ text: "back" });
  });

  it("fails a result that is not in the format with invalid_payload", async () => {
    const { conn, socket } = start();
    const call = conn.api.echo.note({ text: "hi" });
    socket.open();
    await settle();
    socket.welcome({ format: "marked-json" });

    socket.receive(frame(FrameKind.Response, { id: 1, payload: new TextEncoder().encode('{"text":"plain"}') }));
    await expect(call).rejects.toMatchObject({ code: "invalid_payload" });
  });

  it("sends bytes as they are", async () => {
    const { conn, socket } = start();
    const call = conn.api.echo.bytes(Uint8Array.of(9));
    socket.open();
    await settle();
    socket.welcome({ format: "marked-json" });

    const request = socket.sent[0]?.[1];
    expect((request?.flags ?? 0) & FrameFlags.Binary).toBe(FrameFlags.Binary);
    expect(request?.payload).toEqual(Uint8Array.of(9));
    socket.receive(frame(FrameKind.Response, { id: 1, flags: FrameFlags.Binary, payload: Uint8Array.of(8) }));
    expect(await call).toEqual(Uint8Array.of(8));
  });

  // A server that does not name the format in Welcome ignored it, and would read the payloads as JSON.
  it("closes for good when Welcome does not name the format", async () => {
    const { conn, socket } = start();
    socket.open();
    await settle();
    socket.welcome();

    expect(socket.closedWith?.code).toBe(4000);
    expect(conn.state).toBe("closed");
  });

  it("closes for good when Welcome names a format it did not ask for", async () => {
    const { conn, socket } = start({});
    socket.open();
    await settle();
    socket.welcome({ format: "marked-json" });

    expect(socket.closedWith?.code).toBe(4000);
    expect(conn.state).toBe("closed");
  });

  it("does not reconnect when the server has no format of that name", async () => {
    const { conn, socket } = start();
    socket.open();
    await settle();
    socket.closeFromServer(4007);

    expect(conn.state).toBe("closed");
    expect(FakeSocket.last).toBe(socket);
  });
});
