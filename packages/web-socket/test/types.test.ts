import { describe, expectTypeOf, it } from "vitest";
import type {
  CallOptions,
  CallStream,
  Connection,
  HandlerContext,
  StreamOptions,
  UploadOptions,
  UploadSource,
} from "../src/index.ts";

interface Order {
  id: number;
}

interface Contract {
  api: { orders: { get(id: number): Order; latest(): Order; nested: { deep(text: string): string } }; ping(): void };
  streams: { orders: { watch(id: number): Order; all(): Order } };
  uploads: { files: { put(name: string, item: Uint8Array): number; append(item: Uint8Array): void } };
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

  expectTypeOf(conn.streams.orders.watch(1)).toEqualTypeOf<CallStream<Order>>();
  expectTypeOf(conn.streams.orders.watch).parameter(1).toEqualTypeOf<StreamOptions | undefined>();
  expectTypeOf(conn.streams.orders.all()).toEqualTypeOf<CallStream<Order>>();
  expectTypeOf(conn.streams.orders.all(undefined, { buffer: 4 })).toEqualTypeOf<CallStream<Order>>();
  expectTypeOf(conn.with({ timeout: 1 }).streams.orders.watch(1)).toEqualTypeOf<CallStream<Order>>();
  expectTypeOf(conn.uploads.files.put).parameter(0).toEqualTypeOf<string>();
  expectTypeOf(conn.uploads.files.put).parameter(1).toEqualTypeOf<UploadSource<Uint8Array>>();
  expectTypeOf(conn.uploads.files.put).parameter(2).toEqualTypeOf<UploadOptions | undefined>();
  expectTypeOf(conn.uploads.files.put("a", [])).resolves.toEqualTypeOf<number>();
  expectTypeOf(conn.uploads.files.append).parameter(0).toEqualTypeOf<UploadSource<Uint8Array>>();
  expectTypeOf(conn.uploads.files.append([])).resolves.toEqualTypeOf<void>();

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
  // @ts-expect-error: a stream item of the wrong type
  void conn.uploads.files.append(["text"]);
  // @ts-expect-error: an upload without its request
  void conn.uploads.files.put([Uint8Array.of(1)]);
}

describe("types", () => {
  it("are checked by the compiler", () => {
    expectTypeOf(check).toBeFunction();
  });
});
