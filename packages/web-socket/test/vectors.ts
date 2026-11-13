import data from "../../../test-vectors/frames.json" with { type: "json" };
import {
  createFrame,
  type Frame,
  FrameFlags,
  FrameKind,
  hasId,
  hasName,
  hasPayload,
  nameHasText,
  type Side,
} from "../src/protocol/frames.ts";
import { ProtocolError } from "../src/protocol/violation.ts";

// test-vectors/frames.json, which the .NET tests check too, and the helpers to read it.

export interface NameField {
  code: number;
  text?: string;
}

export interface FrameVector {
  kind: keyof typeof FrameKind;
  binary?: boolean;
  upload?: boolean;
  id?: number;
  name?: NameField;
  timeout?: number;
  trace?: string;
  credit?: number;
  amount?: number;
  count?: number;
  error?: { code: number; custom?: string; message: string };
  json?: string;
  bytes?: string;
}

export interface Vectors {
  errorCodes: string[];
  varints: {
    valid: { bytes: string; value: number; shortest?: boolean }[];
    invalid: { bytes: string; violation: string }[];
  };
  messages: {
    valid: { description: string; from: Side; bytes: string; frames: FrameVector[] }[];
    invalid: { description: string; from: Side; bytes: string; violation: string }[];
  };
  names: {
    incoming: {
      description: string;
      limit: number;
      fields: NameField[];
      names?: string[];
      violation?: string;
      at?: number;
    }[];
    outgoing: { description: string; limit: number; names: string[]; fields: NameField[] }[];
  };
}

export const vectors = data as Vectors;

const encoder = new TextEncoder();
const token = /'([^']*)'|([0-9a-fA-F]{2})(?=\s|$)|\S+/g;

/** The vectors' byte notation: two-digit hex bytes and 'quoted' UTF-8 text, separated by spaces. */
export function parseBytes(notation: string): Uint8Array {
  const bytes: number[] = [];
  for (const [whole, text, hex] of notation.matchAll(token)) {
    if (text !== undefined) bytes.push(...encoder.encode(text));
    else if (hex !== undefined) bytes.push(Number.parseInt(hex, 16));
    else throw new Error(`Not a hex byte or quoted text: ${whole}`);
  }

  return Uint8Array.from(bytes);
}

export function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

/** The violation run throws, or a description of what it did instead, so a wrong outcome fails the comparison. */
export function violationOf(run: () => unknown): string {
  try {
    run();
    return "no error";
  } catch (error) {
    return error instanceof ProtocolError ? error.violation : `${String(error)}`;
  }
}

/** The frame a vector describes. Flags follow from the fields it lists. */
export function toFrame(vector: FrameVector): Frame {
  const frame = createFrame(FrameKind[vector.kind]);
  if (vector.binary) frame.flags |= FrameFlags.Binary;
  if (vector.upload) frame.flags |= FrameFlags.Upload;
  if (vector.id !== undefined) frame.id = vector.id;
  if (vector.name !== undefined) {
    frame.nameCode = vector.name.code;
    frame.nameText = vector.name.text ?? "";
  }
  if (vector.timeout !== undefined) {
    frame.flags |= FrameFlags.HasTimeout;
    frame.timeout = vector.timeout;
  }
  if (vector.trace !== undefined) {
    frame.flags |= FrameFlags.HasTrace;
    frame.trace = vector.trace;
  }
  if (vector.credit !== undefined) frame.credit = vector.credit;
  if (vector.amount !== undefined) frame.amount = vector.amount;
  if (vector.count !== undefined) frame.count = vector.count;
  if (vector.error !== undefined) {
    frame.flags |= FrameFlags.HasError;
    frame.errorCode = vector.error.code;
    frame.errorCustom = vector.error.custom ?? "";
    frame.errorMessage = vector.error.message;
  }
  if (vector.json !== undefined) frame.payload = encoder.encode(vector.json);
  else if (vector.bytes !== undefined) frame.payload = parseBytes(vector.bytes);
  return frame;
}

const kindNames = new Map(Object.entries(FrameKind).map(([name, kind]) => [kind as number, name]));

/** A frame as text, showing only the fields its kind and flags carry, so frames compare by what they mean. */
export function render(frame: Frame): string {
  const parts = [kindNames.get(frame.kind) ?? `kind ${frame.kind}`];
  if (frame.flags & FrameFlags.Binary) parts.push("binary");
  if (frame.flags & FrameFlags.Upload) parts.push("upload");
  if (hasId(frame.kind)) parts.push(`id=${frame.id}`);
  if (hasName(frame.kind)) {
    parts.push(`name=${frame.nameCode}`);
    if (nameHasText(frame.nameCode)) parts.push(`'${frame.nameText}'`);
  }
  if (frame.flags & FrameFlags.HasTimeout) parts.push(`timeout=${frame.timeout}`);
  if (frame.flags & FrameFlags.HasTrace) parts.push(`trace='${frame.trace}'`);
  if (frame.kind === FrameKind.StreamOpen) parts.push(`credit=${frame.credit}`);
  if (frame.kind === FrameKind.Credit) parts.push(`amount=${frame.amount}`);
  if (frame.kind === FrameKind.Ack) parts.push(`count=${frame.count}`);
  if (frame.flags & FrameFlags.HasError) {
    parts.push(`error=${frame.errorCode}`);
    if (frame.errorCode === 0) parts.push(`'${frame.errorCustom}'`);
    parts.push(`message='${frame.errorMessage}'`);
  }
  if (hasPayload(frame.kind)) parts.push(`payload=${hex(frame.payload)}`);
  return parts.join(" ");
}
