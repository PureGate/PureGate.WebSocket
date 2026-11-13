import { afterEach, inject } from "vitest";
import { type Connection, type ConnectOptions, connect } from "../../src/index.ts";

// The TestServer's contract, written by hand (src/PureGate.WebSocket.TestServer/TestApi.cs).

export interface Note {
  text: string;
  number: number;
}

export interface Click {
  x: number;
  y: number;
}

export interface Question {
  text: string;
}

export interface TestContract {
  api: {
    echo: {
      note(note: Note): Note;
      text(text: string): string;
      bytes(bytes: Uint8Array): Uint8Array;
      numbers(numbers: number[]): number[];
      trace(): string;
      repeat(length: number): string;
    };
    math: { add(request: { a: number; b: number }): number };
    time: { now(): number };
    misc: { noop(): void; consume(text: string): void };
    calls: {
      delay(milliseconds: number): string;
      cancelled(): number;
      block(): number;
      release(): void;
      peak(): number;
    };
    fail: { notFound(id: number): string; crash(): string };
    events: {
      echo(text: string): void;
      broadcast(note: Note): void;
      join(group: string): void;
      toGroup(request: { group: string; note: Note }): void;
    };
    telemetry: { waitFor(count: number): Click[] };
    browser: { confirm(text: string): string; slow(milliseconds: number): string };
  };
  events: { events: { received(text: string): void; broadcast(note: Note): void } };
  clientApi: { ui: { confirm(question: Question): boolean; slow(milliseconds: number): number } };
  clientEvents: { telemetry: { click(click: Click): void } };
}

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
