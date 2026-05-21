/**
 * Document ingestion — end-to-end through the real (ingestion v2) arch:
 * the browser uploads a file via Uppy multipart **directly to the object
 * store** (presigned PUTs the backend mints), the backend validates +
 * commits, the doc appears in the feed, and opening it mints a `view-url`
 * the browser fetches **directly from the store** (PDF.js range GETs).
 * The backend is never in the byte path in either direction.
 *
 * Untagged → runs in both tier projects; tier 2 logs in via Keycloak
 * (alice has `owner` ⊇ `ingester`), tier 1's dev-auth injector supplies
 * the role.
 */
import { readFileSync } from "node:fs";
import { randomBytes } from "node:crypto";

import { expect, test } from "@playwright/test";

import { loginViaKeycloak } from "../helpers/login";
import { FIXTURE_PDF, tierFromBaseUrl } from "../helpers/pdf-fixture";

const PDF = readFileSync(FIXTURE_PDF);

const COMPLETE_OK = (r: { url(): string; status(): number }) =>
  /\/api\/ingestion\/uploads\/[^/]+\/complete$/.test(r.url()) && r.status() === 200;

/** A presigned object-store request (carries SigV4 query params) — proves
 *  the byte path is browser↔store direct, not via the backend/`/api`. */
const presigned = (method: string) => (r: {
  url(): string;
  status(): number;
  request(): { method(): string };
}) =>
  r.url().includes("X-Amz-Signature=") &&
  r.request().method() === method &&
  (r.status() === 200 || r.status() === 206);

async function loginIfTier2(page: Parameters<typeof loginViaKeycloak>[0], baseURL?: string) {
  if (tierFromBaseUrl(baseURL) === "tier2") await loginViaKeycloak(page, "alice");
}

test("single-file upload: browser→store direct, doc lands in feed, viewer opens", async ({
  page,
  baseURL,
}) => {
  await loginIfTier2(page, baseURL);

  const tag = randomBytes(4).toString("hex");
  const title = `E2E upload ${tag}`;

  await page.goto("/upload");

  // Pick the file via the (hidden) input; the selection list confirms it.
  await page.locator('input[aria-label="Choose files"]').setInputFiles({
    name: `${tag}.pdf`,
    mimeType: "application/pdf",
    buffer: PDF,
  });
  await expect(
    page.locator('[aria-label="Selected files"]').getByText(`${tag}.pdf`),
  ).toBeVisible();

  // Single file → metadata form is active; set a title so the doc is
  // locatable in the feed (autofill is a no-op today, so title = prefill).
  await page.locator('fieldset[aria-label="Metadata"] input').first().fill(title);

  // Arm the proofs before submitting: a direct presigned PUT to the store
  // and the backend's /complete 200.
  const directPut = page.waitForResponse(presigned("PUT"), { timeout: 30_000 });
  const complete = page.waitForResponse(COMPLETE_OK, { timeout: 30_000 });

  await page.getByRole("button", { name: "Upload", exact: true }).click();

  // Tracker (always-mounted) shows the in-flight task.
  await expect(page.getByRole("status")).toBeVisible();

  await directPut; // bytes went browser→store directly
  await complete; // backend validated + committed

  // The committed doc shows up in the feed.
  await page.goto("/feed");
  const card = page.getByRole("button", { name: title });
  await expect(card).toBeVisible({ timeout: 10_000 });

  // Opening it mints a view-url and PDF.js fetches bytes directly.
  const viewUrl = page.waitForResponse(
    (r) => /\/api\/documents\/[^/]+\/view-url$/.test(r.url()) && r.status() === 200,
    { timeout: 10_000 },
  );
  const directGet = page.waitForResponse(presigned("GET"), { timeout: 15_000 });
  await card.click();
  await viewUrl;
  await directGet;
});

test("multi-file upload: metadata form disabled, every file ingests", async ({
  page,
  baseURL,
}) => {
  await loginIfTier2(page, baseURL);

  const tag = randomBytes(4).toString("hex");
  const files = [`a-${tag}.pdf`, `b-${tag}.pdf`].map((name) => ({
    name,
    mimeType: "application/pdf",
    buffer: PDF,
  }));

  await page.goto("/upload");

  let completes = 0;
  page.on("response", (r) => {
    if (COMPLETE_OK(r)) completes += 1;
  });

  await page.locator('input[aria-label="Choose files"]').setInputFiles(files);

  // >1 file → the metadata form is disabled (a shared form can't title N).
  // Assert on a control inside the fieldset: Playwright's toBeDisabled
  // reports native controls (not the <fieldset> element itself), and a
  // disabled fieldset disables its descendants.
  await expect(
    page.locator('fieldset[aria-label="Metadata"] input').first(),
  ).toBeDisabled();
  await expect(page.getByText(/auto-filled for batch uploads/i)).toBeVisible();

  await page.getByRole("button", { name: "Upload", exact: true }).click();

  // Both files complete (each its own create→sign→PUT→complete cycle).
  await expect.poll(() => completes, { timeout: 45_000 }).toBeGreaterThanOrEqual(2);
});
