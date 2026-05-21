# Document Ingestion — Roadmap (the delta to the end-goal)

What is **not yet built** between the current implementation
([`ingestion.md`](./ingestion.md)) and the end-goal spec
([`../specs/ingestion.md`](../specs/ingestion.md)). Ordered by priority.

## Where we are

The upload path is complete and verified: presigned multipart upload →
object validation → **bounded text extraction (real, via `pdftotext`)** →
metadata merge → transactional commit, with a background task tracker on
the frontend. Identity/dedup, the validation surface, the
`MetadataExtractor` seam, and both compose tiers (MinIO) are in place.

The **automated metadata extraction is a no-op** (`NoopExtractor`), and
several lifecycle/cleanup pieces from the original design are not wired.
Those are the delta below.

---

## 1. LLM metadata extractor (the headline gap)

**State:** `MetadataExtractor` is wired through `AppState` as
`NoopExtractor` — automated metadata returns empty. Today a file-only
upload commits with no title/authors/summary (see §2).

**Goal:** an `LlmExtractor` behind the same trait that reads the
extracted text + the user prefill and returns `ExtractedMetadata`
(title, authors, summary, language, published_at, source-specific
`extra`), via the existing `rig`-based `llm` module with a
JSON-schema-constrained response. Bound it (input-char cap, timeout,
graceful failure → empty so the upload still commits). Wire it into
`AppState` when an LLM provider is configured; keep `NoopExtractor` as
the fallback.

**Slots into:** completion pipeline stage 3 (`ingestion/autofill.rs`) —
**no caller change**, the text already arrives in
`ExtractionContext.text`.

**Tests:** the existing fake LLM client (`tests/common/fake_llm.rs`)
scripted to return canned metadata; assert it lands on the row and that
**prefill overrides extractor output field-by-field**.

## 2. Filename → title fallback (interim UX fix)

**State:** a file-only upload (empty form + `NoopExtractor`) commits with
`title = NULL`; the feed renders `title ?? canonical_id` → blank. The SPA
already sends `filename`, but `CreateUploadRequest` has no such field, so
it is silently dropped.

**Goal:** until §1 lands, fall back to the filename so uploads are
labelled. Either add `filename` to `CreateUploadRequest` and use it as
the title default when no prefill/extraction supplies one, or set
`title = file.name` client-side when the form is empty. Small, high-value
while autofill is a no-op.

## 3. Document delete → object-store delete

**State:** `delete_document` (`storage/surreal.rs`) cascades the DB child
rows but **does not delete the S3 object** — every delete orphans its
artefact.

**Goal:** best-effort `ObjectStore::delete(storage_uri)` after the row
delete (log-and-continue on failure; the cleaner in §4 is the backstop).

## 4. Nightly orphan cleaner

**State:** the trait/storage methods exist (`list_multipart_uploads`,
`abort_multipart_upload`, `list_objects`, `list_stale_upload_sessions`,
`delete_upload_sessions_before`, `delete_old_rejections`,
`list_documents_storage_uris`) but **nothing invokes them on a
schedule** — orphaned multipart uploads, stale `upload_session` rows, old
`ingestion_rejection` rows, and delete-orphaned objects accumulate.

**Goal:** a scheduled task (the in-process scheduler is the natural host)
running the orphan sweep with a single age threshold
(`INGEST_CLEANUP_MIN_AGE_HOURS`, asserted `> 2 ×` session TTL) plus the
rejection TTL: abort stale multiparts, delete stale sessions, delete
unreferenced aged objects, reap old rejection rows. Cross-tenant, so it
runs under `SystemDb`.

## 5. E2E upload-flow test (tier-2)

**State:** none (`tests/e2e/upload-flow.spec.ts` doesn't exist).

**Goal:** a `@tier2` Playwright spec — log in, open `/upload`,
`setInputFiles` two small fixtures, submit, poll `/feed` until both
appear; a second case asserting a prefilled title wins over a (fake-LLM)
extractor result. Requires the e2e user to carry the `ingester` role in
the realm export.

## 6. `canonical_id` promotion from extracted identifiers

**State:** `canonical_id` is fixed at `/uploads` time (absent for manual
uploads); manual uploads are never deduped.

**Goal:** when §1's extractor recognises a natural identifier (a DOI,
etc.) in the document, optionally **promote** it to `canonical_id` (and
`dedup_key`) at commit, so an uploaded paper deduplicates against an
adapter-ingested copy. Needs a decision on conflict semantics when the
promoted id collides with an existing document.

## 7. Post-commit enrichment (embeddings + knowledge layer)

**State:** the upload path commits `document` + `document_content` only.
The end-goal corpus document also carries vector embeddings and
knowledge-layer links.

**Goal:** connect a committed upload to the existing RAG/embedding
pipeline ([`rag.md`](./rag.md)) and (later) the knowledge-extraction
pass, so an uploaded document becomes searchable/RAG-able without a
manual re-index. Likely an async post-commit stage rather than inline in
`/complete`.

## 8. Antivirus scanning (deployment-time, deferred)

Per [`../SECURITY.md`](../SECURITY.md): a ClamAV sidecar in the object
validation stage for deployments with untrusted uploaders. Layers into
the existing validator; not in the dev/tier-2 stacks. No code today.

## 9. `required_fields` policy, once §1 lands

`MetadataPolicy.required_fields` is intentionally **empty** today
(requiring e.g. `title` would 400 every file-only upload while autofill
is a no-op). Once the LLM extractor reliably produces a title, decide
which descriptive fields become app-mandatory and set the env knob.

---

## Not ingestion work (noted to avoid confusion)

These surfaced during ingestion build-out but are pre-existing and
separate:

- `Feed.test.tsx` fails to load under Vitest (`DOMMatrix is not defined`
  from `pdfjs-dist` / `PdfViewer.tsx`) — a pdf.js/jsdom polyfill gap.
- `bun run lint` reports pre-existing type errors in `ai-elements/*` and
  `discovery/*` (e.g. `toBeInTheDocument` matcher types not wired into
  tsconfig).
