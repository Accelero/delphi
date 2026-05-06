/**
 * OIDC login helper for Tier 2. Drives Dex's login form once at the start
 * of a test, then Playwright's storage-state takes over and subsequent
 * navigations are pre-authenticated.
 *
 * The credentials match the static-password user defined in
 * `ops/dex/config.yaml`. If you change one, update the other.
 */

import type { Page } from "@playwright/test";

export const DEX_USER = {
  email: "alice@delphi.test",
  password: "alice",
} as const;

export async function loginViaDex(page: Page): Promise<void> {
  // Hitting any protected URL kicks off the OIDC chain → Dex login form.
  await page.goto("/api/auth/me");

  // Dex's static-password connector form fields.
  await page.getByLabel(/email/i).fill(DEX_USER.email);
  await page.getByLabel(/password/i).fill(DEX_USER.password);
  await page.getByRole("button", { name: /login/i }).click();

  // After callback we should land on the SPA. `waitForURL` covers the
  // brief redirect chain (Dex → oauth2-proxy callback → original URL).
  await page.waitForURL(/^http:\/\/localhost\/(api\/)?/);
}
