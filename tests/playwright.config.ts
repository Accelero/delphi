import { defineConfig, devices } from "@playwright/test";

/**
 * Two projects, two stacks:
 *
 *   `tier1` — runs against `docker-compose.yml` (dev-auth, no proxy).
 *             Fast smoke tests on the SPA + backend + DB. Auth bypassed.
 *
 *   `tier2` — runs against `docker-compose.full.yml` (Traefik + Dex +
 *             oauth2-proxy + Redis + backend + frontend). Tests the full
 *             auth perimeter. Slower; tagged with `@tier2` so we can
 *             cherry-pick which specs run here.
 *
 * Stack lifecycle is managed in `helpers/compose.ts`. CI brings the
 * relevant stack up before running tests; locally use `make up` /
 * `make full-up` first.
 */
export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false, // single stack; parallel browsers fight for DB rows
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? "github" : "list",
  use: {
    trace: "on-first-retry",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
  projects: [
    {
      name: "tier1",
      use: {
        ...devices["Desktop Chrome"],
        baseURL: process.env.TIER1_URL ?? "http://localhost:5173",
      },
      grepInvert: /@tier2/,
    },
    {
      name: "tier2",
      use: {
        ...devices["Desktop Chrome"],
        baseURL: process.env.TIER2_URL ?? "http://localhost",
      },
    },
  ],
});
