import { describe, expect, it } from "vitest";
import { isValidCustomCode } from "../src/protocol/errors.ts";
import { createFrame, type Frame, FrameFlags, FrameKind, type Side } from "../src/protocol/frames.ts";
import { isValidName } from "../src/protocol/names.ts";
import { readFrames } from "../src/protocol/reader.ts";
import { ProtocolError } from "../src/protocol/violation.ts";
import { FrameWriter } from "../src/protocol/writer.ts";
import { hex, parseBytes, render, toFrame, vectors, violationOf } from "./vectors.ts";

const ascii = new TextEncoder();

function read(bytes: Uint8Array, from: Side): string[] {
  const frames: string[] = [];
  readFrames(bytes, from, (frame) => frames.push(render(frame)));
  return frames;
}

function write(...frames: Frame[]): Uint8Array {
  const writer = new FrameWriter();
  for (const frame of frames) writer.write(frame);
  return writer.view();
}

describe("frames", () => {
  it.each(vectors.messages.valid)("reads: $description", ({ bytes, from, frames }) => {
    expect(read(parseBytes(bytes), from)).toEqual(frames.map((frame) => render(toFrame(frame))));
  });

  it.each(vectors.messages.valid)("writes: $description", ({ bytes, frames }) => {
    // A small first buffer makes the writer grow.
    const writer = new FrameWriter(4);
    for (const frame of frames) writer.write(toFrame(frame));

    expect(hex(writer.view())).toBe(hex(parseBytes(bytes)));
  });

  it.each(vectors.messages.invalid)("rejects: $description", ({ bytes, from, violation }) => {
    expect(violationOf(() => read(parseBytes(bytes), from))).toBe(violation);
  });

  it("accepts names up to 256 bytes", () => {
    const dotted = `${Array(128).fill("a").join(".")}b`;
    expect(dotted).toHaveLength(256);

    expect(isValidName(ascii.encode(dotted))).toBe(true);
    expect(isValidName(ascii.encode("a".repeat(256)))).toBe(true);
    expect(isValidName(ascii.encode("a".repeat(257)))).toBe(false);
    expect(isValidName(ascii.encode("Orders_2.get_All"))).toBe(true);
    expect(isValidName(ascii.encode("_.__"))).toBe(true);
  });

  it("accepts traces up to 256 bytes", () => {
    const event = (trace: string) => {
      const frame = createFrame(FrameKind.Event);
      frame.flags = FrameFlags.HasTrace;
      frame.nameText = "a";
      frame.trace = trace;
      return write(frame);
    };

    expect(read(event("~".repeat(256)), "client")).toHaveLength(1);
    expect(violationOf(() => read(event("~".repeat(257)), "client"))).toBe("invalid_trace");
  });

  it("accepts custom codes up to 64 bytes", () => {
    expect(isValidCustomCode(ascii.encode("z".repeat(64)))).toBe(true);
    expect(isValidCustomCode(ascii.encode("z".repeat(65)))).toBe(false);
    expect(isValidCustomCode(ascii.encode("not_found_2"))).toBe(true);
    expect(isValidCustomCode(ascii.encode("not-found"))).toBe(false);
  });

  it("round-trips error messages beyond ASCII", () => {
    const response = createFrame(FrameKind.Response);
    response.flags = FrameFlags.HasError;
    response.id = 1;
    response.errorCode = 9;
    response.errorMessage = "Hata: ğüş, 😀, and a lone \ud800 surrogate";

    const messages: string[] = [];
    readFrames(write(response), "server", (frame) => messages.push(frame.errorMessage));

    expect(messages).toEqual(["Hata: ğüş, 😀, and a lone � surrogate"]);
  });

  it("round-trips large payloads", () => {
    const item = createFrame(FrameKind.StreamItem);
    item.flags = FrameFlags.Binary;
    item.id = 12;
    item.payload = Uint8Array.from({ length: 300_000 }, (_, i) => (i * 31) % 256);

    const payloads: Uint8Array[] = [];
    readFrames(write(item), "server", (frame) => payloads.push(frame.payload.slice()));

    expect(payloads).toHaveLength(1);
    expect(payloads[0]).toEqual(item.payload);
  });

  it("clears the reused frame between frames", () => {
    const request = createFrame(FrameKind.Request);
    request.flags = FrameFlags.HasTimeout | FrameFlags.HasTrace;
    request.id = 1;
    request.nameText = "a";
    request.timeout = 9;
    request.trace = "t";
    const event = createFrame(FrameKind.Event);
    event.nameCode = 2;

    const seen: { trace: string; timeout: number; id: number }[] = [];
    readFrames(write(request, event), "client", ({ trace, timeout, id }) => seen.push({ trace, timeout, id }));

    expect(seen).toEqual([
      { trace: "t", timeout: 9, id: 1 },
      { trace: "", timeout: 0, id: 0 },
    ]);
  });

  // Corrupted messages must be rejected with ProtocolError, never with another error. The seed makes every failure
  // reproducible.
  it("rejects corrupted messages only with ProtocolError", () => {
    const messages = vectors.messages.valid.map(({ bytes, from }) => ({ bytes: parseBytes(bytes), from }));
    const random = mulberry32(20261113);

    for (let i = 0; i < 20_000; i++) {
      const original = messages[Math.floor(random() * messages.length)];
      if (original === undefined) continue;
      const bytes = corrupt(original.bytes, random);
      try {
        readFrames(bytes, original.from, () => undefined);
      } catch (error) {
        if (!(error instanceof ProtocolError)) expect.fail(`${String(error)} for [${hex(bytes)}]`);
      }
    }
  });
});

function corrupt(original: Uint8Array, random: () => number): Uint8Array {
  const bytes = Array.from(original);
  for (let edits = 1 + Math.floor(random() * 3); edits > 0; edits--) {
    const position = Math.floor(random() * (bytes.length + 1));
    const action = Math.floor(random() * 4);
    if (action === 0 && position < bytes.length) bytes[position] = Math.floor(random() * 256);
    else if (action === 1 && position < bytes.length) bytes.splice(position, 1);
    else if (action === 2) bytes.splice(position, 0, Math.floor(random() * 256));
    else bytes.length = position;
  }

  return Uint8Array.from(bytes);
}

// A small seeded generator, so the corruption test is the same on every run.
function mulberry32(seed: number): () => number {
  let state = seed;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
