/**
 * Seed a real document by driving the **ingestion v2 upload flow** over
 * the API: create-upload → presigned `PUT` of the fixture bytes straight
 * to the object store → complete. This produces exactly what a browser
 * upload produces — a committed `document` with an `s3://` `storage_uri`
 * and its bytes actually in MinIO — so the viewer's `view-url` + direct
 * presigned `GET` path works against it.
 *
 * (Replaces the old LocalFs `docker cp` + `file://` seed, which the
 * S3-only object store can no longer resolve.)
 *
 * Callers receive `{ canonicalId, title, docId }` to locate the row in
 * the feed and deep-link to it; ids are randomised per call.
 */
import { readFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { fileURLToPath } from "node:url";

import type { APIRequestContext } from "@playwright/test";

const REPO_ROOT = fileURLToPath(new URL("../../", import.meta.url));
export const FIXTURE_PDF = `${REPO_ROOT}tests/fixtures/minimal.pdf`;

export type Tier = "tier1" | "tier2";

/** Map Playwright's baseURL to a stack tier. Tier 1 dev server runs on
 *  :5173; Tier 2 sits behind Traefik on :80 (no explicit port). */
export function tierFromBaseUrl(baseURL: string | undefined): Tier {
  return baseURL && new URL(baseURL).port === "5173" ? "tier1" : "tier2";
}

export type SeededPdf = {
  /** Natural id (`e2e:viewer-<tag>`) — manual uploads normally omit this,
   *  but the seed sets one so tests can find the row by `canonical_id`. */
  canonicalId: string;
  title: string;
  /** SurrealDB record id of the committed document (`document:<doc_id>`),
   *  i.e. what `?doc=` deep links and `view-url` are keyed on. */
  docId: string;
};

/** Drive create → sign-part → direct PUT → complete. The fixture is a
 *  single small part. Works for both tiers: in tier 2 `request` carries
 *  alice's session (forward-auth → JWT with the `ingester` role); in
 *  tier 1 the dev-auth injector supplies the role. */
export async function seedPdf(
  request: APIRequestContext,
  _tier: Tier,
): Promise<SeededPdf> {
  const tag = randomBytes(4).toString("hex");
  const canonicalId = `e2e:viewer-${tag}`;
  const title = `Viewer e2e ${tag}`;
  const bytes = readFileSync(FIXTURE_PDF);

  // 1. Open the upload session.
  const create = await request.post("/api/ingestion/uploads", {
    data: {
      canonical_id: canonicalId,
      source_type: "manual",
      title,
      filename: `${tag}.pdf`,
      content_type: "application/pdf",
      size: bytes.byteLength,
    },
  });
  if (!create.ok()) {
    throw new Error(`create-upload ${create.status()}: ${await create.text()}`);
  }
  const { doc_id } = (await create.json()) as { doc_id: string };

  // 2. Presign part 1.
  const sign = await request.post(
    `/api/ingestion/uploads/${encodeURIComponent(doc_id)}/sign-part`,
    { data: { part_number: 1 } },
  );
  if (!sign.ok()) {
    throw new Error(`sign-part ${sign.status()}: ${await sign.text()}`);
  }
  const { url } = (await sign.json()) as { url: string };

  // 3. PUT the bytes DIRECTLY to the object store (browser↔store path).
  const put = await request.put(url, { data: bytes });
  if (!put.ok()) {
    throw new Error(`presigned PUT ${put.status()}: ${await put.text()}`);
  }
  const etag = put.headers()["etag"];
  if (!etag) throw new Error("presigned PUT returned no ETag");

  // 4. Complete → validate + commit the document row.
  const complete = await request.post(
    `/api/ingestion/uploads/${encodeURIComponent(doc_id)}/complete`,
    { data: { parts: [{ part_number: 1, etag }] } },
  );
  if (!complete.ok()) {
    throw new Error(`complete ${complete.status()}: ${await complete.text()}`);
  }
  const result = (await complete.json()) as { result: string; doc_id?: string };
  if (result.result !== "ready") {
    throw new Error(`complete not ready: ${JSON.stringify(result)}`);
  }

  return { canonicalId, title, docId: result.doc_id ?? doc_id };
}
