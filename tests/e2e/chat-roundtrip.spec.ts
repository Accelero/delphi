/**
 * Smoke test: load the SPA, verify the chat surface mounts and the user
 * is authenticated. Runs in both tiers — the difference is who issued
 * identity (the dev injector vs Dex via oauth2-proxy).
 *
 * Untagged: tier1 (grepInvert `/@tier2/`) and tier2 (no filter) both run it.
 * Add `@tier2` to a test title to scope it to the full-stack project only.
 *
 * Sending a real message and asserting on the streamed response requires
 * a fake LLM provider (the ANTHROPIC/OPENAI key env vars). That's a
 * follow-up — this smoke test just confirms wiring.
 */

import { test, expect } from "@playwright/test";

import { loginViaDex } from "../helpers/login";

test("chat surface is reachable and user is signed in", async ({
  page,
  baseURL,
}) => {
  // Tier 2 needs a login dance; Tier 1 auto-authenticates via dev-auth.
  if (baseURL && new URL(baseURL).port !== "5173") {
    await loginViaDex(page);
  }

  await page.goto("/");

  // Sidebar mounts → app didn't 401-loop.
  await expect(page.getByText(/delphi/i).first()).toBeVisible();

  // The user-menu button is rendered when /api/auth/me succeeded.
  await expect(page.getByRole("button", { name: /signed in|@/i }).first())
    .toBeVisible({ timeout: 10_000 });
});
