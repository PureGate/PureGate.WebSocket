import { describe, expect, it } from "vitest";
import { CallError } from "../../src/index.ts";
import { open, until } from "./contract.ts";

describe("calls", () => {
  it("calls with JSON and binary payloads", async () => {
    const conn = open();

    expect(await conn.api.echo.note({ text: "héllo", number: 7 })).toEqual({ text: "héllo", number: 7 });
    expect(await conn.api.math.add({ a: 40, b: 2 })).toBe(42);
    expect(await conn.api.echo.numbers([1, 2, 3])).toEqual([1, 2, 3]);
    expect(await conn.api.echo.bytes(Uint8Array.of(0, 1, 255))).toEqual(Uint8Array.of(0, 1, 255));
  });

  it("calls without a request or a result", async () => {
    const conn = open();

    expect(await conn.api.time.now()).toBeGreaterThan(0);
    expect(await conn.api.misc.noop()).toBeUndefined();
    expect(await conn.api.misc.consume("ignored")).toBeUndefined();
  });

  it("sends calls made before the connection opened", async () => {
    const conn = open();
    const calls = [conn.api.echo.text("a"), conn.api.echo.text("b"), conn.api.math.add({ a: 1, b: 1 })];

    expect(conn.state).toBe("connecting");
    expect(await Promise.all(calls)).toEqual(["a", "b", 2]);
    expect(conn.state).toBe("open");
    expect(conn.connectionId).not.toBe("");
  });

  it("runs more calls than the window holds, in turn", async () => {
    const conn = open();
    const texts = Array.from({ length: 50 }, (_, i) => `call ${i}`);

    expect(await Promise.all(texts.map((text) => conn.api.echo.text(text)))).toEqual(texts);
  });

  it("keeps calls beyond the server's window in the browser", async () => {
    const conn = open();
    // The TestServer's window is 8. Eight calls block until their deadline; the ninth waits in the browser for one.
    const blocked = Array.from({ length: 8 }, () => conn.api.calls.block(undefined, { timeout: 300 }));
    const queued = conn.api.echo.text("after the window frees up");

    const outcomes = await Promise.allSettled(blocked);
    expect(outcomes.map((outcome) => outcome.status)).toEqual(Array(8).fill("rejected"));
    expect(await queued).toBe("after the window frees up");
    expect(await conn.api.calls.peak()).toBe(8);
  });

  it("passes the server's error codes and details", async () => {
    const conn = open();

    const error = await conn.api.fail.notFound(7).catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(CallError);
    expect(error).toMatchObject({
      code: "not_found",
      message: "Item 7 does not exist.",
      details: { id: 7 },
      traceId: undefined,
    });

    await expect(conn.api.fail.crash()).rejects.toMatchObject({
      code: "handler_failed",
      traceId: expect.stringMatching(/^[\da-f]{32}$/),
    });
    const unknown = (conn.api as unknown as { nothing: { here(): Promise<unknown> } }).nothing.here();
    await expect(unknown).rejects.toMatchObject({ code: "unknown_operation" });
  });

  it("times out calls, and the server stops their handlers", async () => {
    const conn = open();
    // Opened first: a call whose deadline passes while it waits for the connection is never sent at all.
    await conn.opened;

    await expect(conn.api.calls.delay(10_000, { timeout: 50 })).rejects.toMatchObject({ code: "timeout" });
    await until(async () => (await conn.api.calls.cancelled()) === 1);
  });

  it("cancels calls when their signal aborts", async () => {
    const conn = open();
    const controller = new AbortController();
    const call = conn.api.calls.delay(10_000, { signal: controller.signal });
    await conn.api.echo.text("the delay has started");

    controller.abort();
    await expect(call).rejects.toMatchObject({ code: "cancelled" });
    await until(async () => (await conn.api.calls.cancelled()) === 1);
  });

  it("refuses calls whose signal already aborted", async () => {
    const conn = open();

    await expect(conn.api.echo.text("x", { signal: AbortSignal.abort() })).rejects.toMatchObject({ code: "cancelled" });
  });

  it("drops queued calls that are cancelled before they are sent", async () => {
    const conn = open();
    const blocked = Array.from({ length: 8 }, () => conn.api.calls.block());
    const controller = new AbortController();
    const queued = conn.api.echo.text("never sent", { signal: controller.signal });

    controller.abort();
    await expect(queued).rejects.toMatchObject({ code: "cancelled" });
    await expect(conn.api.echo.text("too late", { timeout: 100 })).rejects.toMatchObject({ code: "timeout" });
    await conn.close();
    await Promise.allSettled(blocked);
  });

  it("applies default options from with()", async () => {
    const conn = open();
    const controller = new AbortController();
    const scoped = conn.with({ signal: controller.signal, timeout: 5_000 });

    expect(await scoped.api.echo.text("scoped")).toBe("scoped");
    const call = scoped.api.calls.delay(10_000);
    controller.abort();
    await expect(call).rejects.toMatchObject({ code: "cancelled" });
  });

  it("passes the traceparent", async () => {
    const trace = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    const conn = open({ traceparent: () => trace });

    expect(await conn.api.echo.trace()).toBe(trace);
  });

  it("refuses requests larger than the server accepts", async () => {
    const conn = open();

    await expect(conn.api.echo.text("x".repeat(70_000))).rejects.toMatchObject({ code: "payload_too_large" });
    expect(await conn.api.echo.text("small")).toBe("small");
  });

  it("refuses results larger than the browser accepts", async () => {
    const conn = open({ maxMessage: 16_384 });

    await expect(conn.api.echo.repeat(20_000)).rejects.toMatchObject({ code: "payload_too_large" });
    expect(await conn.api.echo.repeat(100)).toHaveLength(100);
  });

  it("coalesces frames ready together within the server's limit", async () => {
    const conn = open();
    const text = "y".repeat(20_000);

    expect(await Promise.all([1, 2, 3, 4].map(() => conn.api.echo.text(text)))).toEqual([text, text, text, text]);
  });
});
