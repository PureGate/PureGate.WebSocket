import { defineConfig } from "vite";

// yarn dev serves the page and passes /chat on to the server (../Chat.Server, http://localhost:5080). yarn build
// writes the page into the server's wwwroot, which serves it in production.
export default defineConfig({
  server: {
    proxy: { "/chat": { target: "http://localhost:5080", ws: true } },
  },
  build: {
    outDir: "../Chat.Server/wwwroot",
    emptyOutDir: true,
  },
});
