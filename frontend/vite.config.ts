import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";

const serverPort = Number(process.env.VITE_DEV_SERVER_PORT ?? "5173");
const hmrClientPort = process.env.VITE_HMR_CLIENT_PORT
  ? Number(process.env.VITE_HMR_CLIENT_PORT)
  : undefined;

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    host: "0.0.0.0",
    port: serverPort,
    strictPort: true,
    hmr: hmrClientPort ? { clientPort: hmrClientPort } : undefined
  }
});
