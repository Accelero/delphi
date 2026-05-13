/**
 * OIDC login helper for Tier 2. Drives Keycloak's login form once at the
 * start of a test, then Playwright's storage-state takes over and
 * subsequent navigations are pre-authenticated.
 *
 * The credentials match users defined in `ops/keycloak/realm-export.json`.
 * Two seeded users:
 *   - alice@delphi.test  (tenant-a, roles: member, owner)
 *   - bob@delphi.test    (tenant-b, roles: member)
 */

import type { Page } from "@playwright/test";

export const KEYCLOAK_USERS = {
  alice: { username: "alice", password: "alice", tenant: "tenant-a" },
  bob:   { username: "bob",   password: "bob",   tenant: "tenant-b" },
} as const;

export type KeycloakUser = keyof typeof KEYCLOAK_USERS;

export async function loginViaKeycloak(
  page: Page,
  who: KeycloakUser = "alice",
): Promise<void> {
  const user = KEYCLOAK_USERS[who];

  // /oauth2/sign_in (with skip_provider_button=true) redirects straight
  // to the Keycloak login form. Going through /api/auth/me would just
  // 401 with no redirect — the SPA navigates on 401, but Playwright
  // page.goto doesn't.
  await page.goto("/oauth2/sign_in");

  // Keycloak's default login form has `username` and `password` inputs.
  await page.locator("#username").fill(user.username);
  await page.locator("#password").fill(user.password);
  await page.locator("#kc-login").click();

  // After callback we should land on the SPA. `waitForURL` covers the
  // redirect chain (Keycloak → oauth2-proxy callback → original URL).
  await page.waitForURL(/^http:\/\/localhost\/(api\/)?/);
}

/**
 * Drive the full sign-out chain including the Keycloak confirmation
 * step. The chain:
 *   /signout
 *     ─► Traefik signout-chain middleware → /oauth2/sign_out?rd=…
 *     ─► oauth2-proxy clears _oauth2_proxy + Redis session
 *     ─► Keycloak end-session endpoint
 *        ── shows a "Are you sure?" confirmation page because we
 *           don't pass id_token_hint (Keycloak ≥ 18 requirement)
 *     ─► click "Logout" button
 *     ─► Keycloak invalidates SSO session, drops cookies
 *     ─► /  (clean SPA boot)
 */
export async function signOutViaKeycloak(page: Page): Promise<void> {
  await page.goto("http://localhost/signout");
  // Land on Keycloak's logout-confirm page; the form's submit button
  // is rendered as `input[type=submit]` with the localised label
  // "Logout" (msg key: doLogout). Match either the role-based name or
  // the raw input.
  const confirm = page
    .getByRole("button", { name: /^log[ -]?out$/i })
    .or(page.locator("input[type=submit]"));
  await confirm.first().click();
  // Keycloak's post_logout_redirect_uri sends us back to "/".
  await page.waitForURL(/^http:\/\/localhost\/?$/);
}
