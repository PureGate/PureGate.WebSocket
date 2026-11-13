import { describe, expect, it } from "vitest";
import { nameHasText } from "../src/protocol/frames.ts";
import { IncomingNames, nameTableLimit, OutgoingNames } from "../src/protocol/names.ts";
import { type NameField, vectors, violationOf } from "./vectors.ts";

function describeField(field: NameField): string {
  return field.text === undefined ? `${field.code}` : `${field.code} '${field.text}'`;
}

describe("name tables", () => {
  it.each(vectors.names.incoming)("resolves incoming names: $description", (vector) => {
    const table = new IncomingNames(vector.limit);
    const failsAt = vector.at ?? vector.fields.length;

    const names = vector.fields.slice(0, failsAt).map((field) => table.resolve(field.code, field.text ?? ""));
    if (vector.violation === undefined) {
      expect(names).toEqual(vector.names);
    } else {
      const field = vector.fields[failsAt];
      expect(field).toBeDefined();
      if (field !== undefined) {
        expect(violationOf(() => table.resolve(field.code, field.text ?? ""))).toBe(vector.violation);
      }
    }
  });

  it.each(vectors.names.outgoing)("numbers outgoing names: $description", (vector) => {
    const table = new OutgoingNames(vector.limit);

    const fields = vector.names.map((name) => {
      const code = table.next(name);
      return describeField(nameHasText(code) ? { code, text: name } : { code });
    });

    expect(fields).toEqual(vector.fields.map(describeField));
  });

  it("holds the protocol's limit of 1024 definitions", () => {
    const incoming = new IncomingNames();
    const outgoing = new OutgoingNames();
    for (let number = 1; number <= nameTableLimit; number++) {
      expect(outgoing.next(`n${number}`)).toBe(number * 2 + 1);
      incoming.resolve(number * 2 + 1, `n${number}`);
    }

    expect(outgoing.next("one.more")).toBe(0);
    expect(incoming.resolve(nameTableLimit * 2, "")).toBe(`n${nameTableLimit}`);
    expect(violationOf(() => incoming.resolve(nameTableLimit * 2 + 3, "x"))).toBe("name_table_full");
  });
});
