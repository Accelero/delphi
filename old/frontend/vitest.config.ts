/// <reference types="vitest" />
import { defineConfig, mergeConfig } from "vitest/config";
import path from "node:path";

import viteConfig from "./vite.config";

// Vitest reuses Vite's config (plugins, aliases, …) so component tests see
// JSX/TSX, the `@/` alias, etc. exactly the way the dev server does. Only
// test-specific knobs live in the override.
export default mergeConfig(
  viteConfig,
  defineConfig({
    test: {
      globals: true,
      environment: "jsdom",
      setupFiles: ["./test-utils/setup.ts"],
      // Colocated unit + component tests live next to source.
      include: ["src/**/*.test.{ts,tsx}"],
      // Don't pick up generated TS types or build output.
      exclude: ["node_modules", "dist", "src/routeTree.gen.*"],
      css: false,
      // Run via Node (not Bun): Vitest's default tinypool worker trips
      // over Bun's `child_process.spawnSync` shim at the moment, so the
      // canonical invocation is `npx vitest run` (or via the Makefile
      // target `make frontend-test`, which uses a Node container).
    },
    resolve: {
      alias: { "@": path.resolve(__dirname, "./src") },
    },
  }),
);
