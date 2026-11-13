import { ProtocolError } from "./violation.ts";

/** Reads a message's bytes from offset up to end. Reading past end fails with "truncated". */
export class Cursor {
  readonly bytes: Uint8Array;
  offset: number;
  end: number;

  constructor(bytes: Uint8Array, offset = 0, end = bytes.length) {
    this.bytes = bytes;
    this.offset = offset;
    this.end = end;
  }

  byte(): number {
    if (this.offset >= this.end) throw new ProtocolError("truncated");
    return this.bytes[this.offset++] as number;
  }

  /** An unsigned LEB128 varint below 2^32 (docs/PROTOCOL.md, Encoding primitives). */
  varint(): number {
    let value = 0;
    for (let shift = 0; shift < 28; shift += 7) {
      const byte = this.byte();
      value |= (byte & 0x7f) << shift;
      if (byte < 0x80) return value;
    }

    // The fifth byte holds bits 28 to 31: only its low 4 bits can be set, and it must be the last.
    const last = this.byte();
    if (last >= 0x80) throw new ProtocolError("varint_too_long");
    if (last > 0x0f) throw new ProtocolError("varint_overflow");
    return value + last * 0x10000000;
  }

  /** The next length bytes, as a view into the message. */
  take(length: number): Uint8Array {
    if (length > this.end - this.offset) throw new ProtocolError("truncated");
    const view = this.bytes.subarray(this.offset, this.offset + length);
    this.offset += length;
    return view;
  }

  /** A str: its byte length as a varint, then that many bytes. */
  string(): Uint8Array {
    return this.take(this.varint());
  }
}
