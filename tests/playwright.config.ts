import { defineConfig, devices } from "@playwright/test";

/**
 * E2E tests target the full T2 Docker stack:
 *   Traefik -> oauth2-proxy -> Keycloak -> frontend/api/realtime/worker -> NATS/SurrealDB.
 *
 * Tests assume the stack is already running. Locally:
 *   make rebuild-up
 *   cd tests && bun run test
 */
export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? "github" : "list",
  timeout: 45_000,
  expect: {
    timeout: 10_000
  },
  use: {
    ...devices["Desktop Chrome"],
    baseURL: process.env.E2E_BASE_URL ?? "http://localhost:8080",
    trace: "on-first-retry",
    screenshot: "only-on-failure",
    video: "retain-on-failure"
  }
});
