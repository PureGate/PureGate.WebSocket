import { type ChildProcess, spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import type { TestProject } from "vitest/node";

declare module "vitest" {
  export interface ProvidedContext {
    /** The TestServer's WebSocket endpoint. */
    serverUrl: string;
  }
}

const repository = fileURLToPath(new URL("../../../../", import.meta.url));
const project = "src/PureGate.WebSocket.TestServer";

/**
 * Starts the .NET TestServer for the integration tests: the executable in PUREGATE_TEST_SERVER when it is set, as CI
 * does with the Native AOT build, or else a fresh build of the project.
 */
export default async function setup(context: TestProject): Promise<() => Promise<void>> {
  const command = process.env.PUREGATE_TEST_SERVER ?? build();
  const [executable, args] = command.endsWith(".dll") ? ["dotnet", [command]] : [command, []];
  const server = spawn(executable, [...args, "--urls", "http://127.0.0.1:0"], {
    cwd: repository,
    stdio: ["ignore", "pipe", "inherit"],
  });

  const address = await listening(server);
  context.provide("serverUrl", `${address.replace("http://", "ws://")}/ws`);

  return async () => {
    const exited = new Promise((resolve) => server.once("exit", resolve));
    server.kill();
    await exited;
  };
}

function build(): string {
  const result = spawnSync("dotnet", ["build", project, "-c", "Release", "-f", "net11.0", "--nologo", "-v", "quiet"], {
    cwd: repository,
    stdio: "inherit",
  });
  if (result.status !== 0) throw new Error("The TestServer did not build.");
  return `${project}/bin/Release/net11.0/PureGate.WebSocket.TestServer.dll`;
}

// Resolves with the address the server prints once it listens.
function listening(server: ChildProcess): Promise<string> {
  return new Promise((resolve, reject) => {
    let output = "";
    const timer = setTimeout(() => reject(new Error(`The TestServer did not start in time:\n${output}`)), 60_000);
    const onData = (chunk: Buffer) => {
      output += chunk.toString();
      const address = /PUREGATE_TEST_SERVER (\S+)/.exec(output)?.[1];
      if (address === undefined) return;

      clearTimeout(timer);
      server.off("exit", onExit);
      // Keep reading, so the server never blocks on a full pipe.
      server.stdout?.off("data", onData);
      server.stdout?.on("data", () => undefined);
      resolve(address);
    };
    const onExit = (code: number | null) => {
      clearTimeout(timer);
      reject(new Error(`The TestServer exited with ${code}:\n${output}`));
    };

    server.stdout?.on("data", onData);
    server.once("exit", onExit);
  });
}
