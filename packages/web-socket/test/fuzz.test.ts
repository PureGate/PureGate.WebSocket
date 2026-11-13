import { describe, expect, it } from "vitest";
import type { Side } from "../src/protocol/frames.ts";
import { readFrames } from "../src/protocol/reader.ts";
import { ProtocolError } from "../src/protocol/violation.ts";
import { parseBytes, vectors } from "./vectors.ts";

// Hostile input for the frame reader, as the .NET FuzzTests: random bytes and mutated valid messages. Every outcome is
// frames or a ProtocolError, never another error or a read past the message. Seeded, so a failure repeats.
const rounds = 5_000;

// A small seeded generator (mulberry32), so every run sees the same input.
function seeded(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296;
  };
}

function readAll(message: Uint8Array, from: Side): void {
  try {
    let frames = 0;
    readFrames(message, from, (frame) => {
      frames++;
      expect(frame.payload.byteOffset + frame.payload.length).toBeLessThanOrEqual(message.byteOffset + message.length);
    });
    expect(frames).toBeLessThanOrEqual(message.length);
  } catch (error) {
    if (!(error instanceof ProtocolError)) throw error;
  }
}

function mutate(bytes: Uint8Array, random: () => number): Uint8Array {
  const message = Array.from(bytes);
  const mutations = 1 + Math.floor(random() * 3);
  for (let i = 0; i < mutations; i++) {
    const at = message.length === 0 ? 0 : Math.floor(random() * message.length);
    const byte = Math.floor(random() * 256);
    switch (Math.floor(random() * 5)) {
      case 0:
        if (message.length > 0) message[at] = (message[at] as number) ^ (1 << Math.floor(random() * 8));
        break;
      case 1:
        if (message.length > 0) message[at] = byte;
        break;
      case 2:
        message.splice(at, 0, byte);
        break;
      case 3:
        message.splice(at, 1);
        break;
      default:
        message.length = at;
    }
  }

  return Uint8Array.from(message);
}

describe("fuzzing the frame reader", () => {
  it.each<Side>(["client", "server"])("reads random bytes from the %s safely", (from) => {
    const random = seeded(1234);
    for (let round = 0; round < rounds; round++) {
      const message = new Uint8Array(Math.floor(random() * 64));
      for (let i = 0; i < message.length; i++) message[i] = Math.floor(random() * 256);
      readAll(message, from);
    }
  });

  it("reads mutated messages safely", () => {
    const valid = vectors.messages.valid.map((vector) => ({ bytes: parseBytes(vector.bytes), from: vector.from }));
    const random = seeded(5678);
    for (let round = 0; round < rounds; round++) {
      const { bytes, from } = valid[Math.floor(random() * valid.length)] as (typeof valid)[number];
      readAll(mutate(bytes, random), from);
    }
  });
});
