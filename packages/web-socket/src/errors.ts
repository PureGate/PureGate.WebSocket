import type { ErrorCodeName } from "./protocol/errors.ts";

/**
 * A call that failed. code is one of the protocol's error names, such as "timeout" or "unknown_operation", or the
 * server's own code, such as "not_found". details holds the server's JSON details, or undefined.
 */
export class CallError extends Error {
  readonly code: ErrorCodeName | (string & {});
  readonly details: unknown;

  constructor(code: ErrorCodeName | (string & {}), message: string, details?: unknown) {
    super(message || code);
    this.name = "CallError";
    this.code = code;
    this.details = details;
  }
}

export function connectionClosed(): CallError {
  return new CallError("connection_closed", "The connection closed.");
}
