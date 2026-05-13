/**
 * Tier-2 only: end-to-end proof that data ingested in one tenant
 * cannot be read by a user in another tenant. Relies on:
 *   - JWT `tenant_id` claim emitted by Keycloak per user-attribute.
 *   - Backend stamping `tenant_id` on ingest from `auth.tenant_id`
 *     (never from request body — the wire shape doesn't accept it).
 *   - SurrealDB record-level access rules refusing cross-tenant reads.
 *
 * Bug class this test catches: any future "convenience" path that
 * reads tenant_id from a query/body, drops the WHERE clause from a
 * storage method, or weakens the SurrealDB access rule, would let
 * bob see alice's docs in this assertion.
 *
 * Why a separate spec from `tenant-isolation.spec.ts`: that one only
 * proves the tenant_id claim differs end-to-end. This one writes
 * data on one side and asserts it is *not visible* on the other —
 * the actual leakage check.
 */
import { test, expect } from "@playwright/test";

import { loginViaKeycloak } from "../helpers/login";

const ORIGIN = "http://localhost";

type FeedItem = { id: string; canonical_id: string; title?: string };
type FeedPage = { items: FeedItem[]; next_cursor?: string | null };

async function readAllFeed(
  request: import("@playwright/test").APIRequestContext,
): Promise<FeedItem[]> {
  // Walk the cursor pagination once. The seeded feed is small; one or
  // two pages covers it.
  const out: FeedItem[] = [];
  let cursor: string | null | undefined = undefined;
  for (let i = 0; i < 10; i++) {
    const url =
      `${ORIGIN}/api/discovery/feed` +
      (cursor ? `?cursor=${encodeURIComponent(cursor)}` : "");
    const res = await request.get(url);
    expect(res.status()).toBe(200);
    const page = (await res.json()) as FeedPage;
    out.push(...page.items);
    if (!page.next_cursor) break;
    cursor = page.next_cursor;
  }
  return out;
}

test("ingested doc is visible to its tenant only @tier2", async ({
  browser,
}) => {
  // ── alice: logs in, ingests a unique doc ──────────────────────────
  const aliceCtx = await browser.newContext();
  const alicePage = await aliceCtx.newPage();
  await loginViaKeycloak(alicePage, "alice");

  const tag = `leak-test-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const doc = {
    canonical_id: tag,
    source_type: "manual",
    source_uri: `https://example.test/${tag}`,
    title: `Leakage probe ${tag}`,
    summary: "alice ingested this; bob must not see it.",
  };

  const ingest = await alicePage.request.post(
    `${ORIGIN}/api/ingestion/documents`,
    { data: doc },
  );
  expect(ingest.status()).toBe(200);
  const ingestBody = (await ingest.json()) as { outcome: string; id: unknown };
  expect(ingestBody.outcome).toBe("created");

  // ── alice: sees her own doc in her feed ──────────────────────────
  const aliceFeed = await readAllFeed(alicePage.request);
  expect(
    aliceFeed.some((it) => it.canonical_id === tag),
    `alice's own ingest should appear in her feed (looking for canonical_id=${tag})`,
  ).toBe(true);

  // ── bob: logs in fresh in a separate context, must NOT see it ────
  const bobCtx = await browser.newContext();
  const bobPage = await bobCtx.newPage();
  await loginViaKeycloak(bobPage, "bob");

  // Sanity: bob and alice are actually in different tenants.
  const aliceMe = await alicePage.request.get(`${ORIGIN}/api/auth/me`);
  const bobMe = await bobPage.request.get(`${ORIGIN}/api/auth/me`);
  expect(aliceMe.status()).toBe(200);
  expect(bobMe.status()).toBe(200);
  expect((await aliceMe.json()).tenant.id).not.toBe(
    (await bobMe.json()).tenant.id,
  );

  const bobFeed = await readAllFeed(bobPage.request);
  expect(
    bobFeed.some((it) => it.canonical_id === tag),
    `bob (other tenant) must NOT see alice's doc — leakage detected`,
  ).toBe(false);

  await aliceCtx.close();
  await bobCtx.close();
});

test("bob without ingester role cannot ingest @tier2", async ({ browser }) => {
  const ctx = await browser.newContext();
  const page = await ctx.newPage();
  await loginViaKeycloak(page, "bob");

  const res = await page.request.post(`${ORIGIN}/api/ingestion/documents`, {
    data: {
      canonical_id: `bob-attempt-${Date.now()}`,
      source_type: "manual",
      source_uri: "https://example.test/bob-attempt",
      title: "bob shouldn't be allowed to ingest",
    },
    failOnStatusCode: false,
  });
  // INGESTER_ROLES = ["ingester", "owner"]; bob has only "member".
  expect(res.status()).toBe(403);

  await ctx.close();
});

// NOTE: a third test for "alice cannot mark-read a doc in another
// tenant" was prototyped here and uncovered that `POST
// /api/discovery/items/:id/read` returns 204 even for a non-existent
// or out-of-tenant document id (silent upsert into per-user read
// state). Tracked in AUDIT.md as M13. The two tests above already
// prove the read-direction leakage seal — the mark-read wart is
// orthogonal and gets its own ticket rather than a fragile e2e.
