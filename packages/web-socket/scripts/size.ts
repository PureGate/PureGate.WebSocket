// What the client costs an app: the library bundled and minified as an app's build would, then gzipped. Fails when it
// grows past the budget, so growth is a decision rather than an accident. yarn size; CI runs it on every push.
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";
import { build, type Rolldown } from "vite";

const budget = 13 * 1024;

const result = (await build({
  configFile: false,
  logLevel: "silent",
  build: {
    write: false,
    minify: true,
    lib: { entry: fileURLToPath(new URL("../src/index.ts", import.meta.url)), formats: ["es"] },
  },
})) as Rolldown.RolldownOutput | Rolldown.RolldownOutput[];

const outputs = Array.isArray(result) ? result : [result];
const code = outputs
  .flatMap((output) => output.output)
  .filter((chunk) => chunk.type === "chunk")
  .map((chunk) => chunk.code)
  .join("");
const gzipped = gzipSync(code, { level: 9 }).length;

console.log(`@puregate/web-socket: ${code.length} bytes minified, ${gzipped} bytes gzipped, budget ${budget}.`);
if (gzipped > budget) {
  console.error(`Over the size budget by ${gzipped - budget} bytes.`);
  process.exitCode = 1;
}
