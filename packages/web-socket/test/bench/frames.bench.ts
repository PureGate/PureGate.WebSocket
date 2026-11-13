import { test } from "vitest";
import { decodePayload, encodePayload } from "../../src/payload.ts";
import { createFrame, FrameFlags, FrameKind } from "../../src/protocol/frames.ts";
import { readFrames } from "../../src/protocol/reader.ts";
import { FrameWriter } from "../../src/protocol/writer.ts";

// The client's hot paths: writing a call, reading its answer, and the payloads either way. Run on purpose only:
// yarn workspace @puregate/web-socket bench.

const note = { text: "x".repeat(1024), number: 42 };
const json = encodePayload(note).bytes;

const writer = new FrameWriter();
const request = createFrame(FrameKind.Request);
request.flags = FrameFlags.HasTimeout;
request.id = 7;
request.nameCode = 2;
request.timeout = 30_000;
request.payload = json;

const response = createFrame(FrameKind.Response);
response.id = 7;
response.payload = json;
const responses = new FrameWriter();
for (let i = 0; i < 16; i++) responses.write(response);
const message = responses.view().slice();

test("frames", async ({ bench }) => {
  await bench.compare(
    bench("write a 1 KB Request", () => {
      writer.clear();
      writer.write(request);
    }),
    bench("read 16 Responses of 1 KB", () => {
      readFrames(message, "server", () => undefined);
    }),
  );
});

test("payloads", async ({ bench }) => {
  await bench.compare(
    bench("encode 1 KB of JSON", () => {
      encodePayload(note);
    }),
    bench("decode 1 KB of JSON", () => {
      decodePayload(false, json);
    }),
  );
});
