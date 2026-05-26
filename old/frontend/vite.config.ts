import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { TanStackRouterVite } from "@tanstack/router-plugin/vite";
import path from "node:path";

const BACKEND = process.env.BACKEND_URL || "http://localhost:8081";

export default defineConfig({
  plugins: [
    TanStackRouterVite({ target: "react", autoCodeSplitting: true }),
    react(),
    tailwindcss(),
  ],
  resolve: {
    alias: { "@": path.resolve(__dirname, "./src") },
  },
  server: {
    host: "0.0.0.0",
    port: 5173,
    // Docker bind mounts on Linux silently lose inotify events for some
    // files; polling is reliable at the cost of a tiny CPU baseline.
    watch: { usePolling: true, interval: 300 },
    proxy: {
      "/api": { target: BACKEND, changeOrigin: true },
      "/v1": { target: BACKEND, changeOrigin: true },
      "/healthz": { target: BACKEND, changeOrigin: true },
    },
  },
});
