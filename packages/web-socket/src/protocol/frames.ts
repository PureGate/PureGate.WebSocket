/** The frame kinds of docs/PROTOCOL.md. Kinds 0 and 17–255 are reserved. */
export const FrameKind = {
  Hello: 1,
  Welcome: 2,
  Request: 3,
  Response: 4,
  Event: 5,
  StreamOpen: 6,
  StreamItem: 7,
  StreamEnd: 8,
  Credit: 9,
  Cancel: 10,
  Ack: 11,
  Ping: 12,
  Pong: 13,
  Authenticate: 14,
  Authenticated: 15,
  GoAway: 16,
} as const;

export type FrameKind = (typeof FrameKind)[keyof typeof FrameKind];

/** The flags byte. Bits 5–7 are reserved. */
export const FrameFlags = {
  /** The payload is raw bytes, not JSON. */
  Binary: 0x01,
  /** A timeout field follows the name. Request only. */
  HasTimeout: 0x02,
  /** An error field follows the id: the exchange failed. Response and StreamEnd only. */
  HasError: 0x04,
  /**
   * A trace field (a W3C traceparent) follows: the caller's trace on a Request, Event or StreamOpen, and the trace of
   * the work that answered on a Response or StreamEnd.
   */
  HasTrace: 0x08,
  /** The client produces this stream's items. StreamOpen only. */
  Upload: 0x10,
} as const;

export const lastKind = FrameKind.GoAway;

/** Flag bits no version 1 frame may set. */
export const reservedFlags = 0xe0;

/** The two ends of a connection. Some frame kinds may only be sent by one of them. */
export type Side = "client" | "server";

/**
 * One frame, as read from a message or about to be written to one. Fields its kind and flags don't carry stay at 0,
 * "" or an empty payload. A name's text is set only when its code carries one (see nameHasText).
 */
export interface Frame {
  kind: FrameKind;
  flags: number;
  id: number;
  nameCode: number;
  nameText: string;
  /** Milliseconds the caller still waits. Set with the HasTimeout flag. */
  timeout: number;
  /** A W3C traceparent. Set with the HasTrace flag. */
  trace: string;
  /** StreamOpen: the client's initial credit, in items. 0 for an upload. */
  credit: number;
  /** Credit: how many items, or events when id is 0, the receiver grants. */
  amount: number;
  /** Ack: how many sequenced frames the sender has processed. */
  count: number;
  /** Set with the HasError flag. errorCustom only when errorCode is 0. */
  errorCode: number;
  errorCustom: string;
  errorMessage: string;
  /** Everything after the fields, for the kinds that have a payload. */
  payload: Uint8Array;
}

const noBytes = new Uint8Array(0);

export function createFrame(kind: FrameKind): Frame {
  return {
    kind,
    flags: 0,
    id: 0,
    nameCode: 0,
    nameText: "",
    timeout: 0,
    trace: "",
    credit: 0,
    amount: 0,
    count: 0,
    errorCode: 0,
    errorCustom: "",
    errorMessage: "",
    payload: noBytes,
  };
}

/** Clears a frame for reuse, keeping its shape. */
export function resetFrame(frame: Frame, kind: FrameKind): void {
  frame.kind = kind;
  frame.flags = 0;
  frame.id = 0;
  frame.nameCode = 0;
  frame.nameText = "";
  frame.timeout = 0;
  frame.trace = "";
  frame.credit = 0;
  frame.amount = 0;
  frame.count = 0;
  frame.errorCode = 0;
  frame.errorCustom = "";
  frame.errorMessage = "";
  frame.payload = noBytes;
}

// Which fields each kind's layout has, and who may send it, indexed by kind.
const flagsByte = 1;
const idField = 2;
const nameField = 4;
const payloadField = 8;
const clientOnly = 16;
const serverOnly = 32;

const layouts: readonly number[] = [
  0, // reserved
  payloadField | clientOnly, // Hello
  payloadField | serverOnly, // Welcome
  flagsByte | idField | nameField | payloadField, // Request
  flagsByte | idField | payloadField, // Response
  flagsByte | nameField | payloadField, // Event
  flagsByte | idField | nameField | payloadField | clientOnly, // StreamOpen
  flagsByte | idField | payloadField, // StreamItem
  flagsByte | idField | payloadField, // StreamEnd
  idField, // Credit
  idField, // Cancel
  0, // Ack
  0, // Ping
  0, // Pong
  payloadField | clientOnly, // Authenticate
  payloadField | serverOnly, // Authenticated
  serverOnly, // GoAway
];

const { Binary, HasTimeout, HasError, HasTrace, Upload } = FrameFlags;

// The flags each kind allows, indexed by kind.
const allowed: readonly number[] = [
  0,
  0,
  0,
  Binary | HasTimeout | HasTrace, // Request
  Binary | HasError | HasTrace, // Response
  Binary | HasTrace, // Event
  Binary | HasTrace | Upload, // StreamOpen
  Binary, // StreamItem
  Binary | HasError | HasTrace, // StreamEnd
];

function layout(kind: number): number {
  return layouts[kind] ?? 0;
}

export function hasFlags(kind: number): boolean {
  return (layout(kind) & flagsByte) !== 0;
}

export function hasId(kind: number): boolean {
  return (layout(kind) & idField) !== 0;
}

export function hasName(kind: number): boolean {
  return (layout(kind) & nameField) !== 0;
}

/** Whether the kind's layout ends in a payload. The others end at their last field. */
export function hasPayload(kind: number): boolean {
  return (layout(kind) & payloadField) !== 0;
}

export function allowedFlags(kind: number): number {
  return allowed[kind] ?? 0;
}

export function canSend(kind: number, from: Side): boolean {
  return (layout(kind) & (from === "client" ? serverOnly : clientOnly)) === 0;
}

/** Name codes 0 (literal) and 2n + 1 (definition) are followed by the name's text; 2n refers to it. */
export function nameHasText(code: number): boolean {
  return code === 0 || code % 2 === 1;
}

/** Whether frames of this kind are counted for acknowledgement and resume: Request through Cancel. */
export function isSequenced(kind: FrameKind): boolean {
  return kind >= FrameKind.Request && kind <= FrameKind.Cancel;
}
