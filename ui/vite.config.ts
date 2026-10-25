import { fileURLToPath } from "node:url";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";
import solid from "vite-plugin-solid";

// The node's UI address for the dev server's `/api` proxy (`just ui-dev <addr>`).
const nodeAddr = process.env.SYNC_NODE_UI_ADDR ?? "127.0.0.1:7777";

export default defineConfig({
  plugins: [solid(), tailwindcss()],
  // `@/` is `src/`, as in the shadcn-solid components.
  resolve: { alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) } },
  server: {
    proxy: {
      "/api": {
        target: `http://${nodeAddr}`,
        changeOrigin: true,
        ws: true,
        // The node takes a WebSocket only from its own UI address (ADR 0005).
        configure: (proxy) => {
          proxy.on("proxyReqWs", (proxyReq) => proxyReq.setHeader("origin", `http://${nodeAddr}`));
        },
      },
    },
  },
});
