import type { PayloadFormat } from "./contract.ts";

// Payloads (docs/PROTOCOL.md, Payload): bytes travel as they are, undefined as nothing, anything else in the
// connection's format, JSON unless the options name another.

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const noBytes = new Uint8Array(0);

export interface Payload {
  readonly binary: boolean;
  readonly bytes: Uint8Array;
}

const empty: Payload = { binary: false, bytes: noBytes };

export function encodePayload(value: unknown, format?: PayloadFormat): Payload {
  if (value === undefined) return empty;
  if (value instanceof Uint8Array) return { binary: true, bytes: value };
  if (value instanceof ArrayBuffer) return { binary: true, bytes: new Uint8Array(value) };
  if (ArrayBuffer.isView(value)) {
    return { binary: true, bytes: new Uint8Array(value.buffer, value.byteOffset, value.byteLength) };
  }

  if (format !== undefined) return { binary: false, bytes: format.encode(value) };
  const json = JSON.stringify(value);
  if (json === undefined) throw new TypeError("The payload cannot be written as JSON.");
  return { binary: false, bytes: encoder.encode(json) };
}

/** The value of a payload. Bytes are a view into the message, which is never reused. */
export function decodePayload(binary: boolean, bytes: Uint8Array, format?: PayloadFormat): unknown {
  if (binary) return bytes;
  if (bytes.length === 0) return undefined;
  return format === undefined ? JSON.parse(decoder.decode(bytes)) : format.decode(bytes);
}
