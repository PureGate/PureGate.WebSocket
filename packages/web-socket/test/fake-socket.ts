import { createFrame, type Frame, FrameKind } from "../src/protocol/frames.ts";
import { readFrames } from "../src/protocol/reader.ts";
import { FrameWriter } from "../src/protocol/writer.ts";
import { render } from "./vectors.ts";

/**
 * A WebSocket stand-in the tests drive by hand: it records what the client sends, and delivers frames as the server
 * would. The client only uses the parts of the WebSocket API implemented here.
 */
export class FakeSocket {
  static last: FakeSocket | undefined;

  readonly url: string;
  readonly protocols: string | string[] | undefined;
  readonly sent: Frame[][] = [];
  protocol = "puregate.v1";
  readyState = 0;
  binaryType = "blob";
  closedWith: { code: number; reason: string } | undefined;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: ArrayBuffer }) => void) | null = null;
  onclose: ((event: { code: number; reason: string }) => void) | null = null;

  constructor(url: string | URL, protocols?: string | string[]) {
    this.url = String(url);
    this.protocols = protocols;
    FakeSocket.last = this;
  }

  send(data: Uint8Array): void {
    const frames: Frame[] = [];
    readFrames(data, "client", (frame) => frames.push({ ...frame, payload: frame.payload.slice() }));
    this.sent.push(frames);
  }

  close(code = 1000, reason = ""): void {
    this.closedWith = { code, reason };
    this.readyState = 3;
  }

  /** Every frame sent so far, rendered. */
  frames(): string[] {
    return this.sent.flat().map(render);
  }

  open(): void {
    this.readyState = 1;
    this.onopen?.();
  }

  /** Delivers frames from the server in one message. */
  receive(...frames: Frame[]): void {
    const writer = new FrameWriter();
    for (const frame of frames) writer.write(frame);
    this.onmessage?.({ data: writer.view().slice().buffer });
  }

  /** The server's Welcome, with overrides. */
  welcome(fields: Record<string, unknown> = {}): void {
    const frame = createFrame(FrameKind.Welcome);
    frame.payload = new TextEncoder().encode(
      JSON.stringify({
        connection: "c1",
        session: "s",
        calls: 8,
        streams: 4,
        events: 16,
        maxMessage: 65536,
        heartbeat: 15000,
        ...fields,
      }),
    );
    this.receive(frame);
  }

  /** The server closes. */
  closeFromServer(code: number, reason = ""): void {
    this.readyState = 3;
    this.onclose?.({ code, reason });
  }
}

export function frame(kind: FrameKind, fields: Partial<Frame> = {}): Frame {
  return { ...createFrame(kind), ...fields };
}

/** Lets queued microtasks, such as the client's flush, run. */
export function settle(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}
