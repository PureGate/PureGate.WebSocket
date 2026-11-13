import { describe, expect, it } from "vitest";
import { type Note, open, until } from "./contract.ts";

describe("events", () => {
  it("listens to the server's events", async () => {
    const conn = open();
    const received: string[] = [];
    const stop = conn.on.events.received((text) => received.push(text));

    await conn.api.events.echo("first");
    await conn.api.events.echo("second");
    await until(() => received.length === 2);
    expect(received).toEqual(["first", "second"]);

    stop();
    await conn.api.events.echo("third");
    await conn.api.echo.text("the third event came before this");
    expect(received).toEqual(["first", "second"]);
  });

  it("broadcasts to every connection", async () => {
    const first = open();
    const second = open();
    const notes: Note[] = [];
    first.on.events.broadcast((note) => notes.push(note));
    second.on.events.broadcast((note) => notes.push(note));
    await second.opened;

    await first.api.events.broadcast({ text: "all", number: 1 });
    await until(() => notes.length >= 2);
    expect(notes.filter((note) => note.text === "all")).toHaveLength(2);
  });

  it("sends to a group's members only", async () => {
    const member = open();
    const outsider = open();
    const memberNotes: Note[] = [];
    const outsiderNotes: Note[] = [];
    member.on.events.broadcast((note) => memberNotes.push(note));
    outsider.on.events.broadcast((note) => outsiderNotes.push(note));

    await member.api.events.join("room-ts");
    await outsider.api.events.toGroup({ group: "room-ts", note: { text: "hi", number: 2 } });
    await until(() => memberNotes.length === 1);
    await outsider.api.echo.text("anything for the outsider came before this");
    expect(memberNotes).toEqual([{ text: "hi", number: 2 }]);
    expect(outsiderNotes).toEqual([]);
  });

  it("keeps listening when a listener throws", async () => {
    const errors: unknown[] = [];
    const conn = open({ onError: (error) => errors.push(error) });
    const received: string[] = [];
    conn.on.events.received(() => {
      throw new Error("listener bug");
    });
    conn.on.events.received((text) => received.push(text));

    await conn.api.events.echo("still delivered");
    await until(() => received.length === 1);
    expect(errors).toHaveLength(1);
  });

  it("sends events to the server in order, past the event window", async () => {
    const conn = open();
    // The TestServer's window is 16, so these wait for credit along the way.
    for (let x = 1; x <= 40; x++) conn.emit.telemetry.click({ x, y: 0 });

    const clicks = await conn.api.telemetry.waitFor(40);
    expect(clicks.map((click) => click.x)).toEqual(Array.from({ length: 40 }, (_, i) => i + 1));
  });
});
