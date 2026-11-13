import { describe, expectTypeOf, it } from "vitest";
import type { CallOptions, Connection, HandlerContext } from "../src/index.ts";

interface Order {
  id: number;
}

interface Contract {
  api: { orders: { get(id: number): Order; latest(): Order; nested: { deep(text: string): string } }; ping(): void };
  events: { orders: { shipped(event: Order): void } };
  clientApi: { ui: { confirm(text: string): boolean } };
  clientEvents: { telemetry: { click(x: number): void } };
}

// Compile-time checks: yarn typecheck fails when the mapped types drift. The function is never called.
function check(conn: Connection<Contract>, empty: Connection<{ api: { only(): string } }>): void {
  expectTypeOf(conn.api.orders.get).parameter(0).toEqualTypeOf<number>();
  expectTypeOf(conn.api.orders.get).parameter(1).toEqualTypeOf<CallOptions | undefined>();
  expectTypeOf(conn.api.orders.get(1)).resolves.toEqualTypeOf<Order>();
  expectTypeOf(conn.api.orders.latest()).resolves.toEqualTypeOf<Order>();
  expectTypeOf(conn.api.orders.latest(undefined, { timeout: 1 })).resolves.toEqualTypeOf<Order>();
  expectTypeOf(conn.api.orders.nested.deep("x")).resolves.toEqualTypeOf<string>();
  expectTypeOf(conn.api.ping()).resolves.toEqualTypeOf<void>();
  expectTypeOf(conn.with({ timeout: 1 }).api.orders.get(1)).resolves.toEqualTypeOf<Order>();

  expectTypeOf(conn.on.orders.shipped).parameter(0).toEqualTypeOf<(event: Order) => void>();
  expectTypeOf(conn.on.orders.shipped(() => undefined)).toEqualTypeOf<() => void>();
  expectTypeOf(conn.emit.telemetry.click).toEqualTypeOf<(event: number) => void>();
  expectTypeOf(conn.handle.ui.confirm)
    .parameter(0)
    .toEqualTypeOf<(request: string, context: HandlerContext) => boolean | Promise<boolean>>();

  // A contract without a section has an empty one.
  expectTypeOf(empty.on).toEqualTypeOf<Record<never, never>>();

  // @ts-expect-error: a wrong argument type
  void conn.api.orders.get("1");
  // @ts-expect-error: a name that is not in the contract
  void conn.api.orders.remove(1);
  // @ts-expect-error: a wrong event payload
  conn.emit.telemetry.click("x");
  // @ts-expect-error: a handler with the wrong result
  conn.handle.ui.confirm(() => "yes");
}

describe("types", () => {
  it("are checked by the compiler", () => {
    expectTypeOf(check).toBeFunction();
  });
});
