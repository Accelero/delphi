/**
 * Tier-2 only: verifies the full chain Keycloak → oauth2-proxy → backend
 * delivers the correct tenant claim per user. Two seeded users in the
 * realm export, alice in tenant-a and bob in tenant-b; they should land
 * in different tenants on `/api/auth/me`.
 *
 * Tagged `@tier2` so tier-1 doesn't try to drive a Keycloak that isn't
 * running.
 */
import { test, expect } from "@playwright/test";

import { loginViaKeycloak, KEYCLOAK_USERS } from "../helpers/login";

test("alice and bob land in different tenants @tier2", async ({ browser }) => {
  // Each user gets a fresh browser context (no shared cookies).
  const aliceCtx = await browser.newContext();
  const alicePage = await aliceCtx.newPage();
  await loginViaKeycloak(alicePage, "alice");
  const aliceMe = await alicePage.request.get("/api/auth/me");
  expect(aliceMe.status()).toBe(200);
  const aliceJson = await aliceMe.json();

  const bobCtx = await browser.newContext();
  const bobPage = await bobCtx.newPage();
  await loginViaKeycloak(bobPage, "bob");
  const bobMe = await bobPage.request.get("/api/auth/me");
  expect(bobMe.status()).toBe(200);
  const bobJson = await bobMe.json();

  // Tenant ids are SurrealDB record refs in the form `tenant:<key>`.
  // The actual UUID-shaped keys are generated, so we can only assert
  // that they differ — but they MUST differ end-to-end.
  expect(aliceJson.tenant.id).not.toBe(bobJson.tenant.id);
  expect(aliceJson.user.email).toContain(KEYCLOAK_USERS.alice.username);
  expect(bobJson.user.email).toContain(KEYCLOAK_USERS.bob.username);

  await aliceCtx.close();
  await bobCtx.close();
});
