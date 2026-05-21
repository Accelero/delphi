/**
 * In-browser PDF viewer round-trip.
 *
 *   seed a doc + PDF → open feed → click title → assert the viewer
 *   minted a download URL via GET /api/documents/:key/view-url (200),
 *   that the browser then fetched the PDF bytes directly from object
 *   storage (MinIO :9000, not the backend), and that react-pdf produced
 *   a page canvas → click Back → assert we're back on the feed with the
 *   row still present.
 *
 * Both directions of object access now bypass the backend: the byte
 * fetch is browser↔MinIO direct. See docs/architecture/object-access.md.
 *
 * Runs in both tiers — Tier 2 needs a Keycloak login first; tier 1's
 * dev-auth bypass auto-authenticates.
 */
import { test, expect } from "@playwright/test";

import { loginViaKeycloak } from "../helpers/login";
import { seedPdf, tierFromBaseUrl } from "../helpers/pdf-fixture";

test("clicking a document opens the PDF viewer and Back returns to the feed", async ({
  page,
  baseURL,
}) => {
  const tier = tierFromBaseUrl(baseURL);
  if (tier === "tier2") {
    // alice has the `owner` role required by /api/ingestion/documents.
    await loginViaKeycloak(page, "alice");
  }

  const seeded = await seedPdf(page.request, tier);

  // ── open the feed ──────────────────────────────────────────────────
  await page.goto("/feed");
  const titleButton = page.getByRole("button", { name: seeded.title });
  await expect(titleButton).toBeVisible({ timeout: 10_000 });

  // ── click → viewer mints a download URL, then PDF.js fetches the ───
  //    bytes directly from object storage (browser↔MinIO, not backend).
  const viewUrlPattern = /\/api\/documents\/[^/]+\/view-url$/;
  const viewUrlResponsePromise = page.waitForResponse(
    (res) => viewUrlPattern.test(res.url()) && res.status() === 200,
    { timeout: 10_000 },
  );
  // The direct object-storage fetch is a presigned GET carrying SigV4
  // query params; it does NOT go through /api/. Asserting its 200/206
  // proves the byte path is browser↔store direct.
  const objectFetchPromise = page.waitForResponse(
    (res) =>
      res.url().includes("X-Amz-Signature=") &&
      (res.status() === 200 || res.status() === 206),
    { timeout: 15_000 },
  );
  await titleButton.click();

  const viewUrlResponse = await viewUrlResponsePromise;
  const viewUrlBody = await viewUrlResponse.json();
  expect(viewUrlBody.url).toContain("X-Amz-Signature=");
  expect(typeof viewUrlBody.expires_at).toBe("string");

  await objectFetchPromise;

  // Back button proves the viewer chrome rendered.
  const backButton = page.getByRole("button", { name: /back to feed/i });
  await expect(backButton).toBeVisible();
  await expect(
    page.getByRole("heading", { name: seeded.title }),
  ).toBeVisible();

  // react-pdf only paints `.react-pdf__Page__canvas` after pdf.js has
  // parsed the document and committed a page render — its presence is
  // the load-success signal.
  await expect(page.locator("canvas.react-pdf__Page__canvas").first())
    .toBeVisible({ timeout: 15_000 });

  // ── back to feed ───────────────────────────────────────────────────
  await backButton.click();
  await expect(backButton).toBeHidden();
  await expect(titleButton).toBeVisible();
});
