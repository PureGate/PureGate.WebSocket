import { afterEach, inject } from "vitest";
import { type Connection, type ConnectOptions, connect } from "../../src/index.ts";
import type { Contract } from "./contract.generated.ts";

// The tests are typed by the TestServer's own contract, which the .NET tests regenerate and compare
// (ContractTests.DescribesTheTestServer).
export type { Click, Note, Question } from "./contract.generated.ts";
export type TestContract = Contract;

const opened: Connection<TestContract>[] = [];

/** A connection to the TestServer, closed after the test. */
export function open(options: ConnectOptions = {}): Connection<TestContract> {
  const connection = connect<TestContract>(inject("serverUrl"), options);
  opened.push(connection);
  return connection;
}

afterEach(async () => {
  await Promise.all(opened.splice(0).map((connection) => connection.close()));
});

/** Polls until check returns true, failing after a few seconds. */
export async function until(check: () => boolean | Promise<boolean>): Promise<void> {
  const deadline = performance.now() + 5_000;
  while (!(await check())) {
    if (performance.now() > deadline) throw new Error("The condition never held.");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}
