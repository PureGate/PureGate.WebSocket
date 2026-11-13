import { describe, expect, it, vi } from "vitest";
import { Deadlines } from "../src/deadlines.ts";
import { decodePayload, encodePayload } from "../src/payload.ts";

describe("payloads", () => {
  it("sends bytes as they are", () => {
    const bytes = Uint8Array.of(1, 2, 3);

    expect(encodePayload(bytes)).toEqual({ binary: true, bytes });
    expect(encodePayload(bytes.buffer)).toEqual({ binary: true, bytes });
    expect(encodePayload(new DataView(bytes.buffer, 1, 2)).bytes).toEqual(Uint8Array.of(2, 3));
  });

  it("sends undefined as nothing and everything else as JSON", () => {
    expect(encodePayload(undefined).bytes).toHaveLength(0);
    expect(new TextDecoder().decode(encodePayload({ a: [1, "b"] }).bytes)).toBe('{"a":[1,"b"]}');
    expect(new TextDecoder().decode(encodePayload(null).bytes)).toBe("null");
    expect(() => encodePayload(() => undefined)).toThrow(TypeError);
  });

  it("reads payloads back", () => {
    expect(decodePayload(false, new Uint8Array(0))).toBeUndefined();
    expect(decodePayload(false, new TextEncoder().encode('{"a":1}'))).toEqual({ a: 1 });
    expect(decodePayload(true, Uint8Array.of(7))).toEqual(Uint8Array.of(7));
  });
});

describe("deadlines", () => {
  it("expires items at their deadline, with one timer", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "performance"] });
    try {
      const expired: string[] = [];
      const deadlines = new Deadlines<{ deadline: number; name: string }>((item) => expired.push(item.name));
      const now = performance.now();
      const late = { deadline: now + 300, name: "late" };
      deadlines.add(late);
      deadlines.add({ deadline: now + 100, name: "early" });
      deadlines.add({ deadline: now + 200, name: "removed" });
      deadlines.delete({ deadline: now + 200, name: "other" });

      await vi.advanceTimersByTimeAsync(150);
      expect(expired).toEqual(["early"]);
      await vi.advanceTimersByTimeAsync(200);
      expect(expired).toEqual(["early", "removed", "late"]);

      deadlines.add({ deadline: performance.now() + 50, name: "cleared" });
      deadlines.clear();
      await vi.advanceTimersByTimeAsync(100);
      expect(expired).toHaveLength(3);
    } finally {
      vi.useRealTimers();
    }
  });
});
