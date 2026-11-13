/** The longest varint: 5 bytes hold 32 bits. Reading is Cursor.varint. */
export const maxVarintLength = 5;

/** Bytes the shortest encoding of value takes, for 0 ≤ value < 2^32. */
export function varintLength(value: number): number {
  if (value < 0x80) return 1;
  if (value < 0x4000) return 2;
  if (value < 0x200000) return 3;
  if (value < 0x10000000) return 4;
  return 5;
}

/** Writes value (0 ≤ value < 2^32) at offset in its shortest form, and returns the offset after it. */
export function writeVarint(bytes: Uint8Array, offset: number, value: number): number {
  let at = offset;
  let rest = value;
  while (rest >= 0x80) {
    bytes[at++] = (rest & 0x7f) | 0x80;
    rest >>>= 7;
  }

  bytes[at++] = rest;
  return at;
}
