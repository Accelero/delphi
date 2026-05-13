/**
 * Tier-2 only: verifies the /signout chain actually terminates *both*
 * sessions (BFF + IdP SSO), so the user is truly logged out and the
 * next /api call requires a fresh Keycloak login form — not a silent
 * SSO re-auth.
 *
 * The chain under test:
 *   /signout
 *     ─► Traefik signout-chain middleware
 *     ─► /oauth2/sign_out?rd=<keycloak end-session URL>
 *     ─► oauth2-proxy clears _oauth2_proxy cookie + Redis session
 *     ─► Keycloak end-session endpoint kills SSO session
 *     ─► / (clean SPA boot)
 *
 * Bug this test guards against: dropping the `?rd=` chain, or losing
 * the Keycloak whitelist entry, lets the BFF cookie clear but leaves
 * the IdP SSO session alive — `/api/auth/me` then silently
 * re-authenticates and "logout" is a no-op the user can't see.
 */
import { test, expect } from "@playwright/test";

import { loginViaKeycloak, signOutViaKeycloak } from "../helpers/login";

const ORIGIN = "http://localhost";
const KEYCLOAK_ORIGIN = "http://localhost:8088";

test("/signout terminates BFF + IdP sessions, no silent re-auth @tier2", async ({
  browser,
}) => {
  const ctx = await browser.newContext();
  const page = await ctx.newPage();

  // 1. Log in. After this, both BFF and Keycloak SSO cookies exist.
  await loginViaKeycloak(page, "alice");
  let me = await page.request.get(`${ORIGIN}/api/auth/me`);
  expect(me.status()).toBe(200);

  const before = await ctx.cookies();
  expect(before.find((c) => c.name === "_oauth2_proxy")).toBeTruthy();
  // Keycloak's SSO marker is `KEYCLOAK_IDENTITY` (long-lived, holds
  // the auth state). `KEYCLOAK_SESSION` is the matching session-id
  // cookie. `AUTH_SESSION_ID` is a per-tab CSRF token for the auth
  // *flow* and is intentionally rotated rather than cleared on
  // logout — don't assert on it.
  expect(
    before.find((c) => c.name === "KEYCLOAK_IDENTITY"),
  ).toBeTruthy();

  // 2. Drive the full sign-out chain. Playwright follows redirects by
  // default, so a single goto walks the whole sequence and lands on
  // wherever Keycloak's end-session endpoint sends us (= "/").
  await signOutViaKeycloak(page);

  // 3. Both cookies must be gone. The BFF cookie is set with a past
  // Expires by oauth2-proxy; Keycloak drops its session cookies on
  // end-session.
  const after = await ctx.cookies();
  expect(after.find((c) => c.name === "_oauth2_proxy")).toBeFalsy();
  expect(after.find((c) => c.name === "KEYCLOAK_IDENTITY")).toBeFalsy();

  // 4. /api/auth/me must now 401 (forward-auth refuses without cookie).
  me = await page.request.get(`${ORIGIN}/api/auth/me`, {
    maxRedirects: 0,
    failOnStatusCode: false,
  });
  // oauth2-proxy returns 302 to /oauth2/sign_in for HTML clients but
  // 401 when accept !~ html. Playwright's APIRequestContext sends
  // accept: */* so we can see either; both prove we're unauthenticated.
  expect([401, 302]).toContain(me.status());

  // 5. Visiting /oauth2/sign_in must surface the Keycloak login form,
  // not silently re-authenticate. After redirect chain we should be
  // sitting on a Keycloak page (8088) with the login form visible.
  await page.goto(`${ORIGIN}/oauth2/sign_in`);
  await expect(page).toHaveURL(new RegExp(`^${KEYCLOAK_ORIGIN}/`));
  await expect(page.locator("#username")).toBeVisible();
  await expect(page.locator("#password")).toBeVisible();

  await ctx.close();
});

test("re-login after signout works and yields a fresh BFF session @tier2", async ({
  browser,
}) => {
  const ctx = await browser.newContext();
  const page = await ctx.newPage();

  await loginViaKeycloak(page, "alice");
  const firstCookie = (await ctx.cookies()).find(
    (c) => c.name === "_oauth2_proxy",
  )!;
  expect(firstCookie).toBeTruthy();

  await signOutViaKeycloak(page);

  // Re-login as the same user. New BFF session id, not a replay.
  await loginViaKeycloak(page, "alice");
  const secondCookie = (await ctx.cookies()).find(
    (c) => c.name === "_oauth2_proxy",
  )!;
  expect(secondCookie).toBeTruthy();
  expect(secondCookie.value).not.toBe(firstCookie.value);

  await ctx.close();
});

test("signout by alice does not log out a separate bob context @tier2", async ({
  browser,
}) => {
  // Two independent browsers — alice's logout must not affect bob.
  const aliceCtx = await browser.newContext();
  const alicePage = await aliceCtx.newPage();
  await loginViaKeycloak(alicePage, "alice");

  const bobCtx = await browser.newContext();
  const bobPage = await bobCtx.newPage();
  await loginViaKeycloak(bobPage, "bob");

  await signOutViaKeycloak(alicePage);

  // Bob is still authenticated.
  const bobMe = await bobPage.request.get(`${ORIGIN}/api/auth/me`);
  expect(bobMe.status()).toBe(200);

  await aliceCtx.close();
  await bobCtx.close();
});
