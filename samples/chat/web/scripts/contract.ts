// Pulls the server's contract into src/contract.generated.ts. Start the server in Development first.
import { writeFile } from "node:fs/promises";

const url = process.env.CHAT_CONTRACT ?? "http://localhost:5080/chat/contract.ts";
const response = await fetch(url);
if (!response.ok) throw new Error(`${url} answered ${response.status}. Is the server running in Development?`);

await writeFile(new URL("../src/contract.generated.ts", import.meta.url), await response.text());
console.log(`Pulled ${url}.`);
