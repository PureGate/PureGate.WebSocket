import { describe, expect, it } from "vitest";
import type { ConnectionState, StateChange } from "../../src/index.ts";
import { open } from "./contract.ts";

describe("connection", () => {
  it("reports its states", async () => {
    const conn = open();
    const states: [ConnectionState, StateChange][] = [];
    conn.onStateChange((state, change) => states.push([state, change]));

    await conn.opened;
    await conn.close();
    expect(states).toEqual([
      ["open", { resumed: false }],
      ["closed", { code: 1000, reason: "" }],
    ]);
    expect(conn.state).toBe("closed");
  });

  it("fails outstanding and later calls when it closes", async () => {
    const conn = open();
    await conn.opened;
    const outstanding = conn.api.calls.delay(10_000);

    await conn.close();
    await expect(outstanding).rejects.toMatchObject({ code: "connection_closed" });
    await expect(conn.api.echo.text("after")).rejects.toMatchObject({ code: "connection_closed" });
    expect(() => conn.emit.telemetry.click({ x: 1, y: 1 })).toThrow();
  });

  it("rejects opened when it closes before opening", async () => {
    const conn = open();
    const closing = conn.close();

    await expect(conn.opened).rejects.toMatchObject({ code: "connection_closed" });
    await closing;
  });
});
