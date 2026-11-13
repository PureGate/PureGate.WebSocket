import { Cursor } from "./cursor.ts";
import { isValidCustomCode, lastErrorCode } from "./errors.ts";
import {
  allowedFlags,
  canSend,
  createFrame,
  type Frame,
  FrameFlags,
  FrameKind,
  hasFlags,
  hasPayload,
  lastKind,
  nameHasText,
  reservedFlags,
  resetFrame,
  type Side,
} from "./frames.ts";
import { isValidName } from "./names.ts";
import { ProtocolError } from "./violation.ts";

const maxTraceLength = 256;
const utf8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

/**
 * Reads the frames of one message (docs/PROTOCOL.md, Messages and frames), validates every field, and passes each frame
 * to visit. from is the side that sent the message. Malformed input throws ProtocolError, after the frames before it
 * were visited. One frame object is reused for every frame, so visit must copy what it keeps; payloads are views into
 * the message.
 */
export function readFrames(message: Uint8Array, from: Side, visit: (frame: Frame) => void): void {
  if (message.length === 0) throw new ProtocolError("empty_message");

  const cursor = new Cursor(message);
  const frame = createFrame(FrameKind.Ping);
  while (cursor.offset < message.length) {
    cursor.end = message.length;
    const length = cursor.varint();
    if (length === 0) throw new ProtocolError("empty_frame");
    if (length > message.length - cursor.offset) throw new ProtocolError("frame_overrun");

    cursor.end = cursor.offset + length;
    decode(cursor, from, frame);
    cursor.offset = cursor.end;
    visit(frame);
  }
}

function decode(cursor: Cursor, from: Side, frame: Frame): void {
  const kind = cursor.byte();
  if (kind === 0 || kind > lastKind) throw new ProtocolError("reserved_kind");
  if (!canSend(kind, from)) throw new ProtocolError("wrong_direction");

  resetFrame(frame, kind as FrameKind);
  if (hasFlags(kind)) frame.flags = readFlags(cursor, kind);

  switch (kind) {
    case FrameKind.Request:
      frame.id = readId(cursor);
      readName(cursor, frame);
      if (frame.flags & FrameFlags.HasTimeout) frame.timeout = cursor.varint();
      if (frame.flags & FrameFlags.HasTrace) frame.trace = readTrace(cursor);
      break;

    case FrameKind.Response:
    case FrameKind.StreamEnd:
      frame.id = readId(cursor);
      if (frame.flags & FrameFlags.HasError) readError(cursor, frame);
      break;

    case FrameKind.Event:
      readName(cursor, frame);
      if (frame.flags & FrameFlags.HasTrace) frame.trace = readTrace(cursor);
      break;

    case FrameKind.StreamOpen:
      frame.id = readId(cursor);
      readName(cursor, frame);
      frame.credit = cursor.varint();
      // A download needs credit to start with; an upload starts with none, and the server grants it.
      if (frame.flags & FrameFlags.Upload ? frame.credit !== 0 : frame.credit === 0) {
        throw new ProtocolError("invalid_credit");
      }
      if (frame.flags & FrameFlags.HasTrace) frame.trace = readTrace(cursor);
      break;

    case FrameKind.StreamItem:
    case FrameKind.Cancel:
      frame.id = readId(cursor);
      break;

    case FrameKind.Credit:
      // Id 0 is the event window, not a stream.
      frame.id = cursor.varint();
      frame.amount = cursor.varint();
      if (frame.amount === 0) throw new ProtocolError("invalid_credit");
      break;

    case FrameKind.Ack:
      frame.count = cursor.varint();
      break;
  }

  if (hasPayload(kind)) frame.payload = cursor.take(cursor.end - cursor.offset);
  else if (cursor.offset !== cursor.end) throw new ProtocolError("trailing_bytes");
}

function readFlags(cursor: Cursor, kind: number): number {
  const flags = cursor.byte();
  if (flags & reservedFlags) throw new ProtocolError("reserved_flag");
  if (flags & ~allowedFlags(kind)) throw new ProtocolError("invalid_flag");
  return flags;
}

function readId(cursor: Cursor): number {
  const id = cursor.varint();
  if (id === 0) throw new ProtocolError("invalid_id");
  return id;
}

function readName(cursor: Cursor, frame: Frame): void {
  frame.nameCode = cursor.varint();
  if (!nameHasText(frame.nameCode)) return;

  const text = cursor.string();
  if (!isValidName(text)) throw new ProtocolError("invalid_name");
  frame.nameText = ascii(text);
}

function readTrace(cursor: Cursor): string {
  const trace = cursor.string();
  if (trace.length === 0 || trace.length > maxTraceLength) throw new ProtocolError("invalid_trace");
  for (const byte of trace) {
    if (byte < 0x21 || byte > 0x7e) throw new ProtocolError("invalid_trace");
  }

  return ascii(trace);
}

function readError(cursor: Cursor, frame: Frame): void {
  const code = cursor.varint();
  if (code > lastErrorCode) throw new ProtocolError("invalid_error_code");

  frame.errorCode = code;
  if (code === 0) {
    const custom = cursor.string();
    if (!isValidCustomCode(custom)) throw new ProtocolError("invalid_custom_code");
    frame.errorCustom = ascii(custom);
  }

  const message = cursor.string();
  try {
    frame.errorMessage = utf8.decode(message);
  } catch {
    throw new ProtocolError("invalid_string");
  }
}

/** Text already checked to be ASCII. */
function ascii(bytes: Uint8Array): string {
  return String.fromCharCode(...bytes);
}
