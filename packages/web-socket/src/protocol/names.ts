import { ProtocolError } from "./violation.ts";

export const maxNameLength = 256;

/** Definitions per direction. A sender names further operations literally. */
export const nameTableLimit = 1024;

/** Whether bytes are a name: dot-separated segments of [A-Za-z_][A-Za-z0-9_]*, 1 to 256 bytes in all. */
export function isValidName(bytes: Uint8Array): boolean {
  if (bytes.length === 0 || bytes.length > maxNameLength) return false;

  let segmentStart = true;
  for (const byte of bytes) {
    if (byte === 0x2e) {
      if (segmentStart) return false;
      segmentStart = true;
    } else if (isLetterOrUnderscore(byte) || (!segmentStart && isDigit(byte))) {
      segmentStart = false;
    } else {
      return false;
    }
  }

  return !segmentStart;
}

function isLetterOrUnderscore(byte: number): boolean {
  // byte | 0x20 maps A–Z onto a–z and nothing else onto them.
  const lower = byte | 0x20;
  return (lower >= 0x61 && lower <= 0x7a) || byte === 0x5f;
}

function isDigit(byte: number): boolean {
  return byte >= 0x30 && byte <= 0x39;
}

/** The receive side of a name table (docs/PROTOCOL.md, Names). */
export class IncomingNames {
  readonly #names: string[] = [];
  readonly #limit: number;

  constructor(limit = nameTableLimit) {
    this.#limit = limit;
  }

  /** The name a frame's name field stands for. text is the field's text, when its code carries one. */
  resolve(code: number, text: string): string {
    if (code === 0) return text;

    if (code % 2 === 1) {
      if ((code - 1) / 2 !== this.#names.length + 1) throw new ProtocolError("name_out_of_order");
      if (this.#names.length >= this.#limit) throw new ProtocolError("name_table_full");
      this.#names.push(text);
      return text;
    }

    const name = this.#names[code / 2 - 1];
    if (name === undefined) throw new ProtocolError("name_undefined");
    return name;
  }
}

/** The send side of a name table: decides how each frame names its operation or event. */
export class OutgoingNames {
  readonly #numbers = new Map<string, number>();
  readonly #limit: number;

  constructor(limit = nameTableLimit) {
    this.#limit = limit;
  }

  /**
   * The code for the next frame that carries name. Codes 0 and 2n + 1 need the name's text after them (nameHasText).
   * Frames must go out in the order their codes were taken, so definitions arrive numbered 1, 2, 3, ….
   */
  next(name: string): number {
    const number = this.#numbers.get(name);
    if (number !== undefined) return number * 2;
    if (this.#numbers.size >= this.#limit) return 0;

    const next = this.#numbers.size + 1;
    this.#numbers.set(name, next);
    return next * 2 + 1;
  }
}
