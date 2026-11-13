import { describe, expect, it } from "vitest";
import { markedJson } from "../formats.ts";
import { type Note, open, until } from "./contract.ts";

// The TestServer has marked-json, JSON after a marker byte: a payload in the wrong format fails to read on either side.
describe("payload formats", () => {
  it("calls, streams and uploads in the format, and sends bytes as they are", async () => {
    const conn = open({ format: markedJson });

    expect(await conn.api.echo.note({ text: "héllo", number: 7 })).toEqual({ text: "héllo", number: 7 });
    expect(await conn.api.echo.bytes(Uint8Array.of(0xfe, 1))).toEqual(Uint8Array.of(0xfe, 1));

    const counted: number[] = [];
    for await (const count of conn.streams.feed.count(3)) counted.push(count);
    expect(counted).toEqual([1, 2, 3]);
    expect(await conn.uploads.store.sum([2, 3])).toBe(5);
  });

  it("takes events and calls from the server in the format", async () => {
    const marked = open({ format: markedJson });
    const json = open();
    const notes: Note[] = [];
    marked.on.events.broadcast((note) => notes.push(note));
    json.on.events.broadcast((note) => notes.push(note));
    marked.handle.ui.confirm((question) => question.text === "sure?");

    expect(await marked.api.browser.confirm("sure?")).toBe("True");

    // One event to a group with a connection in each format. A group of its own: the other tests share the server.
    await marked.api.events.join("formats-ts");
    await json.api.events.join("formats-ts");
    await json.api.events.toGroup({ group: "formats-ts", note: { text: "both", number: 1 } });
    await until(() => notes.length === 2);
    expect(notes).toEqual([
      { text: "both", number: 1 },
      { text: "both", number: 1 },
    ]);
  });

  it("sends events in the format", async () => {
    const conn = open({ format: markedJson });

    conn.emit.telemetry.click({ x: 4, y: 5 });
    expect(await conn.api.telemetry.waitFor(1)).toEqual([{ x: 4, y: 5 }]);
  });

  it("closes for good when the server has no format of that name", async () => {
    const conn = open({ format: { ...markedJson, name: "carrier-pigeon" } });

    await expect(conn.opened).rejects.toMatchObject({ code: "connection_closed" });
    expect(conn.state).toBe("closed");
  });
});
