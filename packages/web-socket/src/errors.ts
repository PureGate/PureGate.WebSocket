import type { ErrorCodeName } from "./protocol/errors.ts";

/**
 * A call that failed. code is one of the protocol's error names, such as "timeout" or "unknown_operation", or the
 * server's own code, such as "not_found". details holds the server's JSON details, or undefined. traceId is the W3C
 * trace id the server logged the failure under, sent with handler_failed, so a report of the error leads to the
 * server's log entry.
 */
export class CallError extends Error {
  readonly code: ErrorCodeName | (string & {});
  readonly details: unknown;
  readonly traceId: string | undefined;

  constructor(code: ErrorCodeName | (string & {}), message: string, details?: unknown, traceId?: string) {
    super(message || code);
    this.name = "CallError";
    this.code = code;
    this.details = details;
    this.traceId = traceId;
  }
}

export function connectionClosed(): CallError {
  return new CallError("connection_closed", "The connection closed.");
}
