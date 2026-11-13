/** The protocol's error codes, by number. Code 0 means an application's own code follows as text. */
export const errorCodeNames = [
  "",
  "timeout",
  "cancelled",
  "unauthorized",
  "forbidden",
  "unknown_operation",
  "invalid_payload",
  "payload_too_large",
  "overloaded",
  "handler_failed",
  "connection_closed",
  "protocol_error",
] as const;

/** The name of one of the library's error codes, such as "timeout". */
export type ErrorCodeName = Exclude<(typeof errorCodeNames)[number], "">;

export const lastErrorCode = 11;
export const maxCustomCodeLength = 64;

/** The library code with this name, or 0 when the name is an application's own code. */
export function errorCodeOf(name: string): number {
  return Math.max(0, (errorCodeNames as readonly string[]).indexOf(name));
}

/** Whether bytes are an application code: 1 to 64 bytes of [a-z0-9_]. */
export function isValidCustomCode(bytes: Uint8Array): boolean {
  if (bytes.length === 0 || bytes.length > maxCustomCodeLength) return false;

  for (const byte of bytes) {
    const valid = (byte >= 0x61 && byte <= 0x7a) || (byte >= 0x30 && byte <= 0x39) || byte === 0x5f;
    if (!valid) return false;
  }

  return true;
}
