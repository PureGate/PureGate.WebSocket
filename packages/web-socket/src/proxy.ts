/**
 * An object whose property paths are names: reading conn.api.orders.get builds the path "orders.get", and calling it
 * passes the path and the arguments to invoke. Proxies are cached per path, so a path never allocates twice.
 *
 * "then" is never a path: awaiting or returning a proxy from an async function must not look like a call.
 */
export function pathProxy(invoke: (path: string, args: unknown[]) => unknown, path = ""): unknown {
  const children = new Map<string, unknown>();
  // A function target, so the proxy can be called.
  const target = (() => undefined) as object;

  return new Proxy(target, {
    get(_, key) {
      if (typeof key !== "string" || key === "then") return undefined;
      let child = children.get(key);
      if (child === undefined) {
        child = pathProxy(invoke, path.length === 0 ? key : `${path}.${key}`);
        children.set(key, child);
      }
      return child;
    },
    apply(_, __, args: unknown[]) {
      if (path.length === 0)
        throw new TypeError("Call an operation, such as conn.api.orders.get(...), not the section.");
      return invoke(path, args);
    },
  });
}
