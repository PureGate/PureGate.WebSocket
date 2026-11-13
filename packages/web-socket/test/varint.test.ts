import { describe, expect, it } from "vitest";
import { Cursor } from "../src/protocol/cursor.ts";
import { maxVarintLength, varintLength, writeVarint } from "../src/protocol/varint.ts";
import { hex, parseBytes, vectors, violationOf } from "./vectors.ts";

describe("varints", () => {
  it.each(vectors.varints.valid)("reads [$bytes]", ({ bytes, value }) => {
    const input = parseBytes(bytes);
    const cursor = new Cursor(input);

    expect(cursor.varint()).toBe(value);
    expect(cursor.offset).toBe(input.length);
  });

  it.each(vectors.varints.valid.filter((vector) => vector.shortest !== false))(
    "writes $value as [$bytes]",
    ({ bytes, value }) => {
      const output = new Uint8Array(maxVarintLength);
      const length = writeVarint(output, 0, value);

      expect(hex(output.subarray(0, length))).toBe(hex(parseBytes(bytes)));
      expect(varintLength(value)).toBe(length);
    },
  );

  it.each(vectors.varints.invalid)("rejects [$bytes] with $violation", ({ bytes, violation }) => {
    expect(violationOf(() => new Cursor(parseBytes(bytes)).varint())).toBe(violation);
  });

  it.each([0, 127, 128, 2 ** 14 - 1, 2 ** 14, 2 ** 21 - 1, 2 ** 21, 2 ** 28 - 1, 2 ** 28, 2 ** 32 - 1])(
    "round-trips %i at a length boundary",
    (value) => {
      const output = new Uint8Array(maxVarintLength);
      const length = writeVarint(output, 0, value);
      const cursor = new Cursor(output, 0, length);

      expect(cursor.varint()).toBe(value);
      expect(cursor.offset).toBe(length);
      expect(varintLength(value)).toBe(length);
    },
  );

  it("reads from the given offset and stops at the given end", () => {
    const cursor = new Cursor(Uint8Array.of(0xff, 0xac, 0x02, 0x07), 1, 3);

    expect(cursor.varint()).toBe(300);
    expect(violationOf(() => cursor.byte())).toBe("truncated");
  });
});
