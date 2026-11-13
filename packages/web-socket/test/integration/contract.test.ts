import { afterEach, describe, expect, expectTypeOf, it, vi } from "vitest";
import { type Contract, contractHash, type Note } from "./contract.generated.ts";
import { open } from "./contract.ts";

describe("contract", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("connects quietly with the server's own contract", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const conn = open({ contractHash });

    await conn.opened;
    expect(contractHash).toMatch(/^[0-9a-f]{16}$/);
    expect(warn).not.toHaveBeenCalled();
  });

  it("warns when the browser's contract is stale", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const conn = open({ contractHash: "0000000000000000" });

    await conn.opened;
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("Pull contract.ts again"));
  });

  it("types the connection from the generated contract", () => {
    expectTypeOf<Contract["api"]["echo"]["note"]>().parameter(0).toEqualTypeOf<Note>();
    expectTypeOf<ReturnType<Contract["api"]["telemetry"]["waitFor"]>>().toEqualTypeOf<{ x: number; y: number }[]>();
  });
});
