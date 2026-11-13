/**
 * Why a peer's bytes broke the protocol. test-vectors/frames.json uses the same names, and so does the .NET server, so
 * both implementations reject malformed input the same way.
 */
export type Violation =
  | "truncated"
  | "varint_too_long"
  | "varint_overflow"
  | "empty_message"
  | "empty_frame"
  | "frame_overrun"
  | "reserved_kind"
  | "wrong_direction"
  | "reserved_flag"
  | "invalid_flag"
  | "invalid_id"
  | "invalid_name"
  | "invalid_string"
  | "invalid_trace"
  | "invalid_error_code"
  | "invalid_custom_code"
  | "invalid_credit"
  | "trailing_bytes"
  | "name_out_of_order"
  | "name_undefined"
  | "name_table_full";

/** Thrown when a peer breaks the protocol. The connection then closes with 4000 (protocol_error). */
export class ProtocolError extends Error {
  readonly violation: Violation;

  constructor(violation: Violation) {
    super(`The peer broke the protocol: ${violation}.`);
    this.name = "ProtocolError";
    this.violation = violation;
  }
}
