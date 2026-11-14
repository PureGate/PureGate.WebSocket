import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";

// yarn test:ts runs the unit and integration projects in Node, with coverage. yarn test:ts:browsers runs both suites
// again in Chromium, Firefox and WebKit, where the engines differ.
export default defineConfig({
  test: {
    coverage: {
      provider: "v8",
      include: ["src/**/*.ts"],
      thresholds: { lines: 85 },
    },
    projects: [
      {
        test: {
          name: "unit",
          include: ["test/*.test.ts"],
        },
      },
      {
        // The client's hot paths, which `yarn bench` runs as "hot-paths (bench)", on purpose only. Node runs the
        // sources as they are, without Vite's module runner, whose export getters the loops would measure too.
        test: {
          name: "hot-paths",
          include: [],
          benchmark: { include: ["test/bench/*.bench.ts"] },
          experimental: { viteModuleRunner: false },
        },
      },
      {
        // Against the .NET TestServer, which the global setup starts.
        test: {
          name: "integration",
          include: ["test/integration/**/*.test.ts"],
          globalSetup: ["test/integration/server.ts"],
          testTimeout: 20_000,
        },
      },
      {
        test: {
          name: "browser",
          include: ["test/*.test.ts", "test/integration/**/*.test.ts"],
          globalSetup: ["test/integration/server.ts"],
          testTimeout: 20_000,
          browser: {
            enabled: true,
            headless: true,
            provider: playwright(),
            instances: [{ browser: "chromium" }, { browser: "firefox" }, { browser: "webkit" }],
          },
        },
      },
    ],
  },
});
