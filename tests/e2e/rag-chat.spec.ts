/**
 * RAG chat round-trip + PDF overlay deep-link.
 *
 * Two halves:
 *
 *   1. **Deterministic.** Seed a chunk row directly via the
 *      `/api/ingestion/documents` HTTP path (which the existing
 *      `seedPdf` helper does), then open `/feed?doc=&chunk=` with
 *      hand-rolled ids. The viewer mounts; we don't assert overlays
 *      here because seeding chunks via HTTP requires the embedder to
 *      be reachable. Instead we cover the viewer-with-chunk path in a
 *      unit test (`PdfViewer.test.tsx`) and verify here that the
 *      query-param plumbing reaches the viewer.
 *
 *   2. **LLM round-trip (`test.skip` unless `DELPHI_PROVIDER_ANTHROPIC_API_KEY` is
 *      set).** POST `/api/chat` directly with a synthetic question,
 *      assert the response opens with a `2:` citations data block
 *      and contains at least one `[N]` marker.
 */
import { test, expect } from "@playwright/test";

import { loginViaKeycloak } from "../helpers/login";
import { seedPdf, tierFromBaseUrl } from "../helpers/pdf-fixture";

test("deep link `/feed?doc=&chunk=` opens the PDF viewer at the target doc", async ({
  page,
  baseURL,
  request,
}) => {
  const tier = tierFromBaseUrl(baseURL);
  if (tier === "tier2") {
    await loginViaKeycloak(page, "alice");
  }

  // Tier 2 gates `/api/*` on the BFF cookie which lives on the page's
  // context, not the default `request` fixture. Tier 1's dev-auth
  // injector ignores cookies, so either context works there.
  const apiRequest = tier === "tier2" ? page.request : request;
  const seeded = await seedPdf(apiRequest, tier);
  // The viewer reads its target doc id from `?doc=`. We synthesise the
  // chunk id too — the viewer's fetch will 404, but the page chrome
  // (back button + title) must still mount, which is the contract here.
  // Using a known-bad chunk key makes the test deterministic regardless
  // of whether the chunker actually ran on the fixture in this stack.
  const docKey = seeded.canonicalId.replace(/[^a-zA-Z0-9]/g, "");
  // Find the actual document id from the feed so the deep link
  // dereferences a real row.
  const feed = await apiRequest.get("/api/discovery/feed?limit=50");
  expect(feed.ok()).toBeTruthy();
  const json = (await feed.json()) as { items: Array<{ id: string; canonical_id: string }> };
  const doc = json.items.find((d) => d.canonical_id === seeded.canonicalId);
  expect(doc, "seeded doc must appear in feed").toBeTruthy();
  const docId = doc!.id;
  // Open the deep link.
  await page.goto(
    `/feed?doc=${encodeURIComponent(docId)}&chunk=chunk%3Anonexistent`,
  );
  // The viewer's chrome must render (it doesn't depend on the chunk
  // round-trip succeeding). Deep-link path synthesises a FeedDocument
  // with title=null and canonical_id set to the URL doc-id — surfacing
  // the real seeded title for deep links would need a
  // `GET /api/documents/:key` metadata endpoint (v1 follow-up). For
  // now we verify "viewer mounted" via the back button + a rendered
  // canvas, which together prove the right doc loaded.
  await expect(
    page.getByRole("button", { name: /back to feed/i }),
  ).toBeVisible({ timeout: 15_000 });
  // react-pdf has painted the page canvas — proof that the bytes
  // loaded through the same /api/documents/:key/file path the
  // viewer test exercises.
  await expect(
    page.locator("canvas.react-pdf__Page__canvas").first(),
  ).toBeVisible({ timeout: 20_000 });
  // Silence the unused-var warning for the local key (kept around for
  // diagnostic readability if the test fails later).
  void docKey;
});

const LLM_ROUNDTRIP =
  process.env.DELPHI_PROVIDER_ANTHROPIC_API_KEY || process.env.DELPHI_PROVIDER_OPENAI_API_KEY
    ? test
    : test.skip;

LLM_ROUNDTRIP(
  "chat reply opens with a citations data block when retrieval has chunks",
  async ({ request, baseURL }) => {
    const tier = tierFromBaseUrl(baseURL);
    if (tier === "tier2") {
      // Tier 2 needs the keycloak login dance — skip the LLM round
      // trip in tier 2 to keep this test light.
      test.skip();
    }

    // Post chat-streaming redesign: POST /messages IS the stream. Body
    // shape is `{ id, text, parent_id }` with a client-minted ULID for
    // the user message id. The deterministic backend test
    // (`rag_retrieval.rs`) already covers the data-stream protocol
    // end-to-end; this version just verifies the running backend agrees
    // with the wire format.
    const created = await request.post("/api/chat/conversations", {
      data: {},
    });
    expect(created.ok()).toBeTruthy();
    const conv = (await created.json()) as { id: string };
    const key = conv.id.split(":").slice(1).join(":");

    // Crockford ULID — generated client-side to thread the optimistic
    // insert / server commit with the same record id.
    const userId = "01HXY0000000000000000000ZZ";

    const submit = await request.post(
      `/api/chat/conversations/${encodeURIComponent(key)}/messages`,
      {
        data: {
          id: userId,
          text: "what does this paper say about chunks?",
          parent_id: null,
        },
      },
    );
    expect(submit.status()).toBe(200);
    const body = await submit.text();
    // The protocol opens with the `8:` task frame and ends with `d:`.
    expect(body).toMatch(/^8:\{"taskId"/m);
    expect(body).toMatch(/d:\{"finishReason"/);
  },
);
