import { describe, expect, it } from "vitest";
import { CallError } from "../../src/index.ts";
import { open, until } from "./contract.ts";

// Calls from the server, answered by handlers registered with handle.
describe("handlers", () => {
  it("answers the server's calls", async () => {
    const conn = open();
    const questions: string[] = [];
    conn.handle.ui.confirm((question) => {
      questions.push(question.text);
      return question.text === "Delete it?";
    });

    expect(await conn.api.browser.confirm("Delete it?")).toBe("True");
    expect(await conn.api.browser.confirm("Keep it?")).toBe("False");
    expect(questions).toEqual(["Delete it?", "Keep it?"]);
  });

  it("answers with async handlers", async () => {
    const conn = open();
    conn.handle.ui.confirm(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
      return true;
    });

    expect(await conn.api.browser.confirm("Later?")).toBe("True");
  });

  it("passes the handler's errors to the server", async () => {
    const conn = open();
    conn.handle.ui.confirm(() => {
      throw new CallError("declined", "The user said no.");
    });

    expect(await conn.api.browser.confirm("Delete it?")).toBe("declined: The user said no.");
  });

  it("answers unknown_operation without a handler", async () => {
    const conn = open();

    expect(await conn.api.browser.confirm("Anyone?")).toMatch(/^unknown_operation: /);
  });

  it("aborts handlers when the server stops waiting", async () => {
    const conn = open();
    let aborted = false;
    conn.handle.ui.slow(
      (_milliseconds, { signal }) =>
        new Promise<number>((_, reject) => {
          signal.addEventListener("abort", () => {
            aborted = true;
            reject(new Error("aborted"));
          });
        }),
    );

    expect(await conn.api.browser.slow(5_000)).toBe("timeout");
    await until(() => aborted);
  });

  it("allows one handler per name", async () => {
    const conn = open();
    const unregister = conn.handle.ui.confirm(() => true);

    expect(() => conn.handle.ui.confirm(() => false)).toThrow(/already has a handler/);
    unregister();
    conn.handle.ui.confirm(() => false);
    expect(await conn.api.browser.confirm("Replaced?")).toBe("False");
  });
});
