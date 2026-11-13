import { describe, expect, it } from "vitest";
import { errorCodeNames, errorCodeOf, lastErrorCode } from "../src/protocol/errors.ts";
import { vectors } from "./vectors.ts";

describe("error codes", () => {
  it("names every code as the vectors do", () => {
    expect(errorCodeNames.slice(1)).toEqual(vectors.errorCodes);
    expect(errorCodeNames).toHaveLength(lastErrorCode + 1);
    vectors.errorCodes.forEach((name, index) => {
      expect(errorCodeOf(name)).toBe(index + 1);
    });
  });

  it("treats other names as custom codes", () => {
    expect(errorCodeOf("not_found")).toBe(0);
    expect(errorCodeOf("")).toBe(0);
  });
});
