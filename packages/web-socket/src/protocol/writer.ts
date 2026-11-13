import { type Frame, FrameFlags, FrameKind, hasFlags, hasPayload, nameHasText } from "./frames.ts";
import { varintLength, writeVarint } from "./varint.ts";

const utf8 = new TextEncoder();

/**
 * Writes frames, with their length prefixes, into one message (docs/PROTOCOL.md, Messages and frames). Each frame's
 * length is computed first, so nothing is moved afterwards. The caller makes every frame well-formed: flags say which
 * optional fields are set, and names, traces and codes follow the protocol's rules. Only the reader validates.
 */
export class FrameWriter {
  #bytes: Uint8Array;
  #length = 0;

  constructor(capacity = 1024) {
    this.#bytes = new Uint8Array(capacity);
  }

  /** Bytes written so far. */
  get length(): number {
    return this.#length;
  }

  /** The message written so far, as a view that the next write or clear may overwrite. */
  view(): Uint8Array {
    return this.#bytes.subarray(0, this.#length);
  }

  /** Starts the next message, reusing the buffer. */
  clear(): void {
    this.#length = 0;
  }

  write(frame: Frame): void {
    const messageLength = frame.flags & FrameFlags.HasError ? utf8Length(frame.errorMessage) : 0;
    const bodyLength = getBodyLength(frame, messageLength);
    const bytes = this.#reserve(varintLength(bodyLength) + bodyLength);

    let offset = writeVarint(bytes, this.#length, bodyLength);
    bytes[offset++] = frame.kind;
    if (hasFlags(frame.kind)) bytes[offset++] = frame.flags;

    switch (frame.kind) {
      case FrameKind.Request:
        offset = writeVarint(bytes, offset, frame.id);
        offset = writeName(bytes, offset, frame);
        if (frame.flags & FrameFlags.HasTimeout) offset = writeVarint(bytes, offset, frame.timeout);
        if (frame.flags & FrameFlags.HasTrace) offset = writeAscii(bytes, offset, frame.trace);
        break;

      case FrameKind.Response:
      case FrameKind.StreamEnd:
        offset = writeVarint(bytes, offset, frame.id);
        if (frame.flags & FrameFlags.HasError) offset = writeError(bytes, offset, frame, messageLength);
        break;

      case FrameKind.Event:
        offset = writeName(bytes, offset, frame);
        if (frame.flags & FrameFlags.HasTrace) offset = writeAscii(bytes, offset, frame.trace);
        break;

      case FrameKind.StreamOpen:
        offset = writeVarint(bytes, offset, frame.id);
        offset = writeName(bytes, offset, frame);
        offset = writeVarint(bytes, offset, frame.credit);
        if (frame.flags & FrameFlags.HasTrace) offset = writeAscii(bytes, offset, frame.trace);
        break;

      case FrameKind.StreamItem:
      case FrameKind.Cancel:
        offset = writeVarint(bytes, offset, frame.id);
        break;

      case FrameKind.Credit:
        offset = writeVarint(bytes, offset, frame.id);
        offset = writeVarint(bytes, offset, frame.amount);
        break;

      case FrameKind.Ack:
        offset = writeVarint(bytes, offset, frame.count);
        break;
    }

    if (hasPayload(frame.kind)) {
      bytes.set(frame.payload, offset);
      offset += frame.payload.length;
    }

    this.#length = offset;
  }

  // Makes room for length more bytes and returns the buffer to write them into.
  #reserve(length: number): Uint8Array {
    const needed = this.#length + length;
    if (needed > this.#bytes.length) {
      const grown = new Uint8Array(Math.max(needed, this.#bytes.length * 2));
      grown.set(this.view());
      this.#bytes = grown;
    }

    return this.#bytes;
  }
}

// Bytes after the length prefix: the kind, the fields and the payload.
function getBodyLength(frame: Frame, messageLength: number): number {
  let length = hasFlags(frame.kind) ? 2 : 1;
  switch (frame.kind) {
    case FrameKind.Request:
      length += varintLength(frame.id) + getNameLength(frame);
      if (frame.flags & FrameFlags.HasTimeout) length += varintLength(frame.timeout);
      if (frame.flags & FrameFlags.HasTrace) length += getAsciiLength(frame.trace);
      break;

    case FrameKind.Response:
    case FrameKind.StreamEnd:
      length += varintLength(frame.id);
      if (frame.flags & FrameFlags.HasError) {
        length += varintLength(frame.errorCode) + varintLength(messageLength) + messageLength;
        if (frame.errorCode === 0) length += getAsciiLength(frame.errorCustom);
      }
      break;

    case FrameKind.Event:
      length += getNameLength(frame);
      if (frame.flags & FrameFlags.HasTrace) length += getAsciiLength(frame.trace);
      break;

    case FrameKind.StreamOpen:
      length += varintLength(frame.id) + getNameLength(frame) + varintLength(frame.credit);
      if (frame.flags & FrameFlags.HasTrace) length += getAsciiLength(frame.trace);
      break;

    case FrameKind.StreamItem:
    case FrameKind.Cancel:
      length += varintLength(frame.id);
      break;

    case FrameKind.Credit:
      length += varintLength(frame.id) + varintLength(frame.amount);
      break;

    case FrameKind.Ack:
      length += varintLength(frame.count);
      break;
  }

  return hasPayload(frame.kind) ? length + frame.payload.length : length;
}

// Names, traces and custom codes are ASCII, so their byte length is their string length.
function getAsciiLength(text: string): number {
  return varintLength(text.length) + text.length;
}

function getNameLength(frame: Frame): number {
  return varintLength(frame.nameCode) + (nameHasText(frame.nameCode) ? getAsciiLength(frame.nameText) : 0);
}

function writeAscii(bytes: Uint8Array, offset: number, text: string): number {
  let at = writeVarint(bytes, offset, text.length);
  for (let i = 0; i < text.length; i++) bytes[at++] = text.charCodeAt(i);
  return at;
}

function writeName(bytes: Uint8Array, offset: number, frame: Frame): number {
  const at = writeVarint(bytes, offset, frame.nameCode);
  return nameHasText(frame.nameCode) ? writeAscii(bytes, at, frame.nameText) : at;
}

function writeError(bytes: Uint8Array, offset: number, frame: Frame, messageLength: number): number {
  let at = writeVarint(bytes, offset, frame.errorCode);
  if (frame.errorCode === 0) at = writeAscii(bytes, at, frame.errorCustom);
  at = writeVarint(bytes, at, messageLength);
  return at + utf8.encodeInto(frame.errorMessage, bytes.subarray(at, at + messageLength)).written;
}

/** The UTF-8 length of text, as TextEncoder encodes it: a lone surrogate becomes U+FFFD, 3 bytes. */
function utf8Length(text: string): number {
  let length = 0;
  for (let i = 0; i < text.length; i++) {
    const unit = text.charCodeAt(i);
    if (unit < 0x80) {
      length += 1;
    } else if (unit < 0x800) {
      length += 2;
    } else if (unit >= 0xd800 && unit <= 0xdbff && (text.charCodeAt(i + 1) & 0xfc00) === 0xdc00) {
      length += 4;
      i++;
    } else {
      length += 3;
    }
  }

  return length;
}
