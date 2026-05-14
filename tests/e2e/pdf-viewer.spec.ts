/**
 * In-browser PDF viewer round-trip.
 *
 *   seed a doc + PDF → open feed → click title → assert the viewer
 *   overlays, that GET /api/documents/:key/file returned 200, and that
 *   react-pdf produced a page canvas → click Back → assert we're back
 *   on the feed with the row still present.
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

  // ── click → viewer mounts and the file endpoint serves the PDF ─────
  const filePattern = /\/api\/documents\/[^/]+\/file$/;
  const fileResponsePromise = page.waitForResponse(
    (res) => filePattern.test(res.url()) && res.status() === 200,
    { timeout: 10_000 },
  );
  await titleButton.click();

  const fileResponse = await fileResponsePromise;
  expect(fileResponse.headers()["content-type"]).toContain("application/pdf");

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
