# Delphi — RAG v1 (Retrieval-Augmented Chat with Grounded PDF Citations)

Design doc for the first slice of the Exploration pillar's "RAG chat
with the corpus" feature. Authoritative companion to
[`ARCH.md`](../ARCH.md); the chunk schema lives in
`backend/schema.surql` (the `chunk` table is already scaffolded — see
"Schema deltas" below for the small diff).

## Goal

A user chatting with their corpus sees the LLM cite specific passages
inline (`[1]`, `[2]`, …). Clicking a citation opens the source PDF in
the existing in-app viewer, scrolled to the relevant page(s) with the
cited passage **highlighted by drawing CSS rectangles at PDF
coordinates** that were captured at ingest. The citation link contains
only opaque identifiers — no quote text in URLs.

## Scope

### In scope (v1)

- Per-document text extraction with positions (`Word { page, bbox, text }`).
- Chunking the word stream into ~paragraph-sized chunks (size + overlap
  configurable).
- Per-chunk **line-level bbox list** computed at ingest.
- Two embedders, hard-coded in a registry, always both run on every
  ingest: **BGE-small-en-v1.5** (chunk-level, 384-dim) and **SPECTER2**
  (document-level, 768-dim, fed `title + [SEP] + abstract`).
- TEI sidecars in both compose stacks serving the two models.
- KNN retrieval over chunks with configurable neighbor expansion.
- LLM prompt that enumerates retrieved chunks with `[N]` markers and
  instructs the model to cite them.
- New `GET /api/chunks/:id` endpoint (tenant-scoped) serving the
  metadata + bboxes the viewer needs to draw the highlight.
- Frontend: `[N]` markers in the chat surface render as deep links;
  PdfViewer accepts a `chunk=…` query param and draws per-line CSS
  rectangles over the right page(s).
- Tests across all four tiers (unit / integration / e2e).

### Deferred

- Per-tenant embedder enable/disable.
- Multiple chunk embedders side-by-side.
- Anthropic Citations API or a verification pass for citation honesty.
- LLM tools (`fetch_section`, `fetch_page`).
- OCR for scanned PDFs (Option B fallback for image-only docs).
- Swap to a better extractor (GROBID / Marker / Nougat / Mistral OCR) —
  drop-in via the `TextExtractor` trait when we want it.
- Per-paper "similar papers" UI driven by SPECTER2 vectors (the column
  is populated in v1; surfacing it is later).

## End-to-end flow

```
ingest                                                            chat
─────                                                            ─────
PDF bytes                                                User asks Q
   │                                                            │
   ▼                                                            ▼
TextExtractor::extract                                  Embedder::query(Q)
   ↳ Vec<Word{page, bbox, text}>                            ↳ vec<f32>
   │                                                            │
   ▼                                                            ▼
Chunker::chunk(words, cfg)                              Storage::knn(vec, top_k)
   ↳ Vec<Chunk{ordinal, text, line_bboxes, …}>              ↳ [{chunk_id, …}]
   │                                                            │
   ▼                                                            ▼
for each enabled chunk-embedder:                        expand neighbors (±r)
    Embedder::passages(texts) → vec<vec<f32>>                   │
for the one doc-embedder:                                       ▼
    Embedder::document(title + [SEP] + abstract)        compose prompt
                                                        with [N] labels +
   │                                                    chunk_ids returned
   ▼                                                    to client out-of-band
Storage::upsert_chunks(chunks_with_vectors)                     │
Storage::upsert_document(doc_with_paper_embedding)              ▼
                                                        stream LLM response
                                                                │
                                                                ▼
                                                Frontend parses [N] markers,
                                                renders <a href="/feed?
                                                doc=…&chunk=…">[N]</a>
                                                                │
                                                  click ──────────
                                                                ▼
                                                PdfViewer fetches
                                                GET /api/chunks/:id,
                                                scrolls to page,
                                                overlays bbox rectangles.
```

## Architectural decisions

### Why per-word bboxes at ingest (Option A), not "search in pdf.js text layer at view time" (Option B)

One ground truth for text-to-position mapping. The chunker, the chunk
row, and the viewer all consume the same `Word` stream from a single
`TextExtractor` impl. Swapping that impl later for SOTA (GROBID, Marker,
Nougat, Mistral OCR) re-ingests the corpus and updates highlights
uniformly — no viewer code touches its text layer for highlighting.

Trade we accept: we own the coordinate math (PDF origin is bottom-left;
CSS origin is top-left; pages can be rotated; zoom scales points to
pixels). It's about 50 lines once, then forgotten.

### Why a `TextExtractor` trait

Same reason `ObjectStore` and `LlmClient` are traits: today's impl
(`pdftotext -bbox-layout`) is good enough; tomorrow's will be much
better. The output type is extractor-agnostic
(`Vec<Word { page, bbox, text }>`), so chunking / storage / viewer don't
change when we swap.

### Why a hard-coded embedder registry, not pure-config

Each model has model-specific input preparation that can't be expressed
in config:

- BGE wants optional `passage:` / `query:` prefixes.
- Nomic wants `search_document:` / `search_query:`.
- E5 wants `passage:` / `query:`.
- SPECTER2 wants `title + [SEP] + abstract` joining.
- Instructor wants a task instruction string.

Getting these wrong silently degrades retrieval 5–15%. So the registry
encodes `prepare_passage`, `prepare_document`, `prepare_query`
transforms in code, and config just enables / disables entries and
points at endpoints. Adding a model = one registry entry, a 5-line PR.

### Why citation URLs use `chunk_id`, not quote text

TLS encrypts URLs **in transit**; URLs are not private end-to-end. They
land in browser history, autocomplete, server access logs, reverse-proxy
logs, crash/telemetry reports, browser extensions with `tabs`
permission. Putting quote text in URLs leaks document content through
all those channels. The fix: opaque ids only. The viewer fetches the
chunk's text + bboxes from the tenant-scoped
`GET /api/chunks/:id`. Same auth posture as everything else.

### Why no per-tenant embedder config in v1

Schema doesn't force the choice now (every ingest populates every
enabled column; per-tenant scoping happens at retrieval time, where
filters already live). Single-user / private deployments — SPEC's first
target — have no tenant conflict. Per-tenant becomes a drop-in
`tenant.disabled_embedders: [...]` later when SaaS has a concrete need.

## Module layout

New (each is a module per `.claude/CLAUDE.md` rules — interface file
re-exports the public surface, internals are private):

- `backend/src/text_extractor/` — `TextExtractor` trait + `Word`
  struct + `pdftotext -bbox-layout` impl + tests.
- `backend/src/embedder/` — `Embedder` trait + `TeiEmbedder` HTTP impl
  + `registry` mapping known model names → trait objects with their
  per-model `prepare_*` transforms + `embedder_from_env` constructor.
- `backend/src/chunker/` — chunking algorithm + line-bbox grouping.

Existing modules touched:

- `backend/src/ingestion/` — `Pipeline::ingest` calls the new modules
  in order (extract → chunk → embed → upsert).
- `backend/src/storage/` — `AuthedDb` / `SystemStorage` get a `knn`
  method + a `get_chunk(tenant, chunk_id)` method; `Document` model
  gains `paper_embedding: Option<Vec<f32>>`.
- `backend/src/api/` — new `chunks.rs` module for `GET /api/chunks/:id`;
  `chat.rs` integrates retrieval into the request flow.
- `frontend/src/components/discovery/PdfViewer.tsx` — overlay layer
  drawn on top of `react-pdf`'s page canvas.
- `frontend/src/components/chat/MessageBody.tsx` — parses `[N]`
  markers in streamed assistant text and renders them as links.
- `frontend/src/lib/api.ts` — `documentFileUrl` already exists; add
  `chunkUrl(chunk_id)` and a typed `getChunk(chunk_id)` helper.
- `frontend/src/routes/feed.tsx` — accept `?doc=&chunk=` query
  params, open the viewer with the chunk preselected.

## Schema deltas (against `backend/schema.surql`)

The `chunk` table already exists. Only field changes needed:

1. **Rename `bbox` → `bboxes`** and change shape to `option<array<object>>`
   so one chunk carries N line rectangles (`[{page, x, y, w, h}, ...]`).
   The existing field is FLEXIBLE TYPE option<object> — empty in all
   rows currently (no chunker runs), so this is a `REMOVE FIELD bbox …;
   DEFINE FIELD bboxes …;` migration. No data to convert.
2. **Drop `page`** (single-int) from `chunk`. Multi-page is the new
   reality; the array of bboxes carries page indices. Existing rows: none.

Add to `document`:

- `paper_embedding: option<array<float>>` (768-dim, SPECTER2).
- `paper_embedding_model: option<string>`.
- HNSW index `document_paper_embedding ON document FIELDS
  paper_embedding HNSW DIMENSION 768 DIST COSINE TYPE F32 EFC 200 M 16`.

The existing HNSW index on `chunk.embedding` (384-dim) stays as-is.

`schema.surql` is `IF NOT EXISTS`-only with `REMOVE … IF EXISTS` for
superseded fields — follow that idiom for the `bbox → bboxes` /
`page` removals.

## Config

All env-var (consistent with existing patterns).

| Var | Default | Effect |
|---|---|---|
| `RAG_CHUNK_SIZE_TOKENS` | `400` | Target chunk size. |
| `RAG_CHUNK_OVERLAP_TOKENS` | `50` | Overlap between adjacent chunks. |
| `RAG_RETRIEVAL_TOP_K` | `5` | KNN page size. |
| `RAG_RETRIEVAL_NEIGHBOR_RADIUS` | `1` | 0 = chunk only, 1 = ±1 neighbor, … |
| `EMBEDDER_CHUNK_ENDPOINT` | `http://tei-chunk:80` | TEI URL for BGE-small. |
| `EMBEDDER_CHUNK_ENABLED` | `true` | Master switch (off ⇒ ingest skips chunking + embedding entirely). |
| `EMBEDDER_CHUNK_MODEL_NAME` | `bge-small-en-v1.5` | Passed to TEI; also written to `chunk.embedding_model`. |
| `EMBEDDER_DOCUMENT_ENDPOINT` | `http://tei-paper:80` | TEI URL for SPECTER2. |
| `EMBEDDER_DOCUMENT_ENABLED` | `true` | Master switch. |
| `EMBEDDER_DOCUMENT_MODEL_NAME` | `specter2` | Passed to TEI; also written to `document.paper_embedding_model`. |

Tokeniser for chunk-size accounting: a cheap approximation (split on
whitespace and punctuation) is fine for v1 — chunks are paragraph-
aligned anyway, so the token count is a soft target not a hard limit.
Don't pull in `tiktoken`-style deps yet.

## TEI sidecars

Add to **both** `docker-compose.yml` (tier 1) and
`docker-compose.full.yml` (tier 2):

```yaml
tei-chunk:
  image: ghcr.io/huggingface/text-embeddings-inference:cpu-1.5
  command: ["--model-id", "BAAI/bge-small-en-v1.5"]
  # No port mapping in tier 2; tier 1 can expose for curl debugging.

tei-paper:
  image: ghcr.io/huggingface/text-embeddings-inference:cpu-1.5
  command: ["--model-id", "allenai/specter2_base"]
```

Backend `depends_on` both. Tier-1 + tier-2 hostnames are `tei-chunk` /
`tei-paper` from inside the docker network.

Models cache to a named volume so dev restarts are fast.

## Public API surface

### `GET /api/chunks/:id`

Auth: standard tenant-scoped (any authenticated user). Path param is the
key portion of `chunk:<key>` (same pattern as `/api/documents/:key/file`
from the viewer).

Response:

```json
{
  "id": "chunk:abc123",
  "doc_id": "document:xyz789",
  "ordinal": 7,
  "text": "...the full chunk text...",
  "bboxes": [
    { "page": 3, "x": 72.0, "y": 540.0, "w": 460.0, "h": 12.0 },
    { "page": 3, "x": 72.0, "y": 525.0, "w": 460.0, "h": 12.0 },
    { "page": 4, "x": 72.0, "y": 730.0, "w": 460.0, "h": 12.0 }
  ]
}
```

Errors: 404 if not found in caller's tenant; tenancy is enforced by
the `AuthedDb` SELECT.

### `POST /api/chat` (existing) — retrieval integration

Before calling the LLM:

1. Embed the user's latest message via `Embedder::query`.
2. KNN over `chunk.embedding` (tenant-scoped, top-`k`).
3. Expand each hit by `±radius` neighbors (same `doc`, adjacent
   `ordinal`).
4. Compose a `Role::System` message that enumerates each chunk:

   ```
   You have access to the following excerpts from the user's corpus.
   When you make a claim drawn from one of them, append the corresponding
   [N] marker. Cite only chunks you actually used.

   [1] "Title of paper X" (page 3)
   <chunk text>

   [2] "Title of paper Y" (page 7)
   <chunk text>

   ...
   ```

5. Stream the assistant response as today.

The chunk-id list must reach the client so it can resolve `[N]` →
`chunk_id` when rendering. Two ways: stream them as a custom event in
the existing Vercel AI SDK Data Stream Protocol (preferred — fits
existing `api/stream.rs` writer), or send a `cite` block at the start
of the stream. Pick whichever is cleanest in the existing protocol;
shape:

```json
{ "type": "citations", "chunks": [
    { "n": 1, "chunk_id": "chunk:abc", "doc_id": "document:xyz",
      "doc_title": "...", "page": 3 },
    { "n": 2, ... }
] }
```

## Coordinate math (viewer overlay)

`react-pdf` renders each page at some CSS pixel width
(`Page.width` prop). pdf.js scales the underlying PDF (PDF points,
1/72 inch, origin **bottom-left**) to that pixel width by a factor
`scale = pixelWidth / pageViewportWidth`. Each stored bbox is in PDF
points:

```
css_x = bbox.x * scale
css_y = (pageHeight - bbox.y - bbox.h) * scale   // origin flip
css_w = bbox.w * scale
css_h = bbox.h * scale
```

`pageHeight` and `pageViewportWidth` come from the
`onLoadSuccess(pageProxy)` callback (`pageProxy.view`). Page rotation:
PDFs can declare `/Rotate 90|180|270` on a page; `pageProxy.rotate`
exposes it. The standard transform is:

```
function transform(bbox, pageWidth, pageHeight, rotate) {
  switch (rotate) {
    case 0:   return { x: bbox.x, y: pageHeight - bbox.y - bbox.h, w: bbox.w, h: bbox.h };
    case 90:  return { x: bbox.y, y: bbox.x, w: bbox.h, h: bbox.w };
    case 180: return { x: pageWidth - bbox.x - bbox.w, y: bbox.y, w: bbox.w, h: bbox.h };
    case 270: return { x: pageHeight - bbox.y - bbox.h, y: pageWidth - bbox.x - bbox.w, w: bbox.h, h: bbox.w };
  }
}
```

Then scale by the render scale. v1 may ship rotate=0 only and skip
the other branches if no rotated test fixture exists yet — flag in
code as a TODO.

## Citation rendering (frontend)

The chat surface streams Markdown. The new rendering rule: the regex
`/\[(\d+)\]/g` over assistant message text, with each match resolved
against the per-message citation table (the `citations` event payload
above). Resolved match → `<a class="citation" href="/feed?doc=DOC_ID&chunk=CHUNK_ID">[N]</a>`.
Unresolved match → plain text `[N]` (don't crash on a model hallucinating
a number).

In `feed.tsx`, when `?chunk=…` is in the URL on mount, the `Feed`
auto-sets `selected` to a placeholder document so the `PdfViewer`
mounts; the viewer then fetches `/api/chunks/:id` and uses the returned
`doc_id` to also fetch `/api/documents/:doc_key/file`. (Alternative: the
link encodes both — `?doc=&chunk=` — to skip the chunk-first round trip.
Prefer encoding both since we already know `doc_id` at render time.)

## Test plan

Follows `docs/architecture/testing.md`'s placement rules.

### Backend unit (colocated `#[cfg(test)]`)

- `text_extractor/pdftotext_bbox.rs::tests` — drives the impl against
  `tests/fixtures/minimal.pdf` (already in repo) and a multi-page paper;
  asserts word count, page numbers, monotone reading order, bbox
  ranges in plausible PDF coordinate space.
- `chunker/mod.rs::tests` — word-stream in, chunks out: target size
  respected (±tokens), overlap honoured, line-bbox union correct
  (same baseline grouped), multi-page chunks carry bboxes from both
  pages.
- `embedder/tei.rs::tests` — `wiremock`-based fake TEI; asserts the
  POST body matches the per-model prepare transform (e.g. SPECTER2
  body is `title [SEP] abstract`, BGE body has `passage:` prefix).
- `embedder/registry.rs::tests` — known-model lookup, unknown-model
  returns a registry error not a panic.
- `api/chunks.rs::tests` — 404 on cross-tenant id (via `TestApp` rig).

### Backend integration (`backend/tests/*.rs`)

- `rag_ingest.rs` — ingest a request whose `storage_uri` points at the
  fixture PDF in `MemObjectStore`; assert chunks are written with
  `bboxes` populated, both `chunk.embedding` and `document.paper_embedding`
  are populated (use a fake `Embedder` that returns deterministic
  vectors so the test is hermetic).
- `rag_retrieval.rs` — seed N chunks with controlled embeddings; query
  with a vector close to chunk 7; assert KNN returns 7 first, neighbor
  expansion adds 6 and 8 when radius=1, doesn't bleed into other docs.
- `chunks_endpoint.rs` — `GET /api/chunks/:id` round-trips a chunk for
  the owning tenant; returns 404 for another tenant.

### Frontend unit (Vitest, colocated)

- `MessageBody.test.tsx` — input markdown with `[1] [2] [99]` and a
  citation table for 1 + 2; assert anchors for resolved markers and
  plain text for unresolved.
- `PdfViewer.test.tsx` — given a fake chunk fixture
  (mock `/api/chunks/:id` via MSW), assert overlay `<div>`s render
  with expected `left/top/width/height` for a known scale + page
  height (no actual pdf.js needed for the math; mock the page proxy
  to return fixed `view`).

### E2E (`tests/e2e/rag-chat.spec.ts` — new)

- Seed a paper via the existing `seedPdf` helper (the fixture
  `tests/fixtures/minimal.pdf` is fine for this).
- POST `/api/chat` directly with `messages: [{role: 'user', content:
  'what does this paper say?'}]` — assert the streamed protocol
  includes a `citations` block and the text contains at least one
  `[1]`-style marker. (Uses a fake LLM in CI: tier-1 backend can be
  wired to `MINIMAX_BASE_URL` pointing at a small local stub, OR
  this test runs only when `ANTHROPIC_API_KEY` is set — guard
  with `test.skip` accordingly. Mark non-blocking.)
- Open `/feed?doc=<id>&chunk=<id>` directly; assert the viewer mounts,
  scrolls to the right page, and at least one
  `.delphi-chunk-overlay` element is visible. (This part is
  deterministic and should always run.)

Tier coverage: deterministic backend + viewer parts run in both tiers;
LLM round-trip skipped without a key.

## Suggested build order

1. `TextExtractor` trait + pdftotext-bbox impl + unit tests.
2. `Chunker` trait/free function + unit tests (line-bbox grouping is
   the tricky part — get it right here).
3. `Embedder` trait + TEI HTTP impl + registry + unit tests (use
   `wiremock`).
4. TEI sidecars in `docker-compose.yml` + `docker-compose.full.yml`.
5. Schema deltas: `bbox → bboxes`, drop `page`, add `paper_embedding*`
   on document, add HNSW index for paper_embedding.
6. `Storage::knn` + `get_chunk` impls; update `Chunk` model.
7. Integrate into `Pipeline::ingest`.
8. `GET /api/chunks/:id` endpoint + integration test.
9. Chat retrieval integration in `api/chat.rs` + streaming the
   `citations` block.
10. Frontend: `MessageBody` parser + link rendering.
11. Frontend: `PdfViewer` accepts `chunk` prop, fetches, draws overlays.
12. Frontend: `feed.tsx` reads `?doc=&chunk=` query params and opens
    the viewer.
13. E2E spec.

Each step ends with `cargo test` / `bun test` / `bunx playwright`
passing before moving on — these are the natural commit boundaries.

## Done criteria

- A user ingesting a PDF gets chunks with bboxes and embeddings, plus
  a populated `paper_embedding` on the document.
- A chat reply contains `[N]` markers; clicking one opens the PDF in
  the viewer with the chunk highlighted on the right page(s).
- All three test tiers pass; e2e (deterministic part) passes in both
  tier 1 and tier 2.
- No quote or passage text appears anywhere in URLs.
- The `TextExtractor` and `Embedder` traits are public; their concrete
  impls are not (re-exports only). The chunker is internal to its
  module. Module imports satisfy `.claude/CLAUDE.md`.
