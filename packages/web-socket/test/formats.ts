import type { PayloadFormat } from "../src/index.ts";

/** Never the first byte of UTF-8 JSON. */
export const marker = 0xfe;

/**
 * The TestServer's marked-json (MarkedJsonFormat.cs): JSON after a marker byte, so a test can tell which format a
 * payload travelled in.
 */
export const markedJson: PayloadFormat = {
  name: "marked-json",
  encode(value) {
    const json = new TextEncoder().encode(JSON.stringify(value));
    const bytes = new Uint8Array(json.length + 1);
    bytes[0] = marker;
    bytes.set(json, 1);
    return bytes;
  },
  decode(bytes) {
    if (bytes[0] !== marker) throw new TypeError("The payload has no marker.");
    return JSON.parse(new TextDecoder().decode(bytes.subarray(1)));
  },
};
