/**
 * Seed a PDF into the running backend's object store and ingest a
 * document row that points at it.
 *
 * The backend's `LocalFsObjectStore` is mounted inside the container
 * at `/var/lib/delphi/originals`. Tier 1 also bind-mounts that to a
 * host directory, but Tier 2 doesn't — `docker cp` works either way,
 * so this helper uses cp uniformly rather than reaching into the host
 * filesystem (which would also be root-owned in tier 1).
 *
 * Callers receive `{ canonicalId, title }` so the test can find the row
 * in the feed; the storage key is randomised per-call to keep tests
 * independent.
 */
import { execSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { fileURLToPath } from "node:url";

import type { APIRequestContext } from "@playwright/test";

const REPO_ROOT = fileURLToPath(new URL("../../", import.meta.url));
const FIXTURE_PDF = `${REPO_ROOT}tests/fixtures/minimal.pdf`;

const BACKEND_CONTAINER = {
  tier1: "delphi-backend",
  tier2: "delphi-full-backend",
} as const;

export type Tier = keyof typeof BACKEND_CONTAINER;

/** Map Playwright's baseURL to a stack tier. Tier 1 dev server runs on
 *  :5173; Tier 2 sits behind Traefik on :80 (no explicit port). */
export function tierFromBaseUrl(baseURL: string | undefined): Tier {
  return baseURL && new URL(baseURL).port === "5173" ? "tier1" : "tier2";
}

export type SeededPdf = {
  canonicalId: string;
  title: string;
  /** In-container path under the object store root. Useful to assert
   *  what the backend will resolve via `storage_uri`. */
  containerPath: string;
};

/** Drop `tests/fixtures/minimal.pdf` into the backend container and
 *  POST an ingestion request pointing at it. Returns identifiers the
 *  test can use to locate the row in the feed. */
export async function seedPdf(
  request: APIRequestContext,
  tier: Tier,
): Promise<SeededPdf> {
  const tag = randomBytes(4).toString("hex");
  const canonicalId = `e2e-viewer/${tag}`;
  const title = `Viewer e2e ${tag}`;
  const containerPath = `/var/lib/delphi/originals/e2e/${tag}.pdf`;
  const container = BACKEND_CONTAINER[tier];

  // `docker cp` requires the destination directory to exist.
  execSync(`docker exec ${container} mkdir -p /var/lib/delphi/originals/e2e`);
  execSync(`docker cp ${FIXTURE_PDF} ${container}:${containerPath}`);

  const res = await request.post("/api/ingestion/documents", {
    data: {
      canonical_id: canonicalId,
      source_type: "manual",
      source_uri: `https://example.test/e2e/${tag}`,
      title,
      summary: "e2e viewer fixture",
      storage_uri: `file://${containerPath}`,
    },
  });
  if (res.status() !== 200) {
    throw new Error(
      `seed ingest failed: ${res.status()} ${await res.text()}`,
    );
  }
  return { canonicalId, title, containerPath };
}
