# Plan — unify ingestion on the upload path; remove the legacy pipeline

**Status:** proposed (2026-05-24). Sister to
[`ingestion.md`](./ingestion.md) and the NATS-backed ingest plan in
[`scaling-nats.md`](./scaling-nats.md).

## Goal

One ingestion route for everything. Users and automated ingesters (source
adapters / bots) all go through the **upload path**
(`/api/ingestion/uploads*` → S3 → `/complete` → `commit_upload`). The
legacy in-process **pipeline** (`POST /api/ingestion/documents` +
`Pipeline` + the source-adapter loopback client) is removed. Bots
authenticate as a service account via **OAuth client-credentials** (a JWT
through the normal BFF/OIDC chain, carrying the `ingester` role the upload
endpoints already gate on).

## Background: the two paths today

| | Upload path (keep) | Pipeline path (remove) |
|---|---|---|
| Entry | `POST /api/ingestion/uploads*` | `POST /api/ingestion/documents` |
| Used by | users (SPA) | source adapters (loopback), legacy callers |
| Content | binary file in S3 (PDF) | inline `raw_text` (abstract/metadata) |
| DB write | `commit_upload` — **one Surreal transaction** (document + content + dedup_key + session delete) | `Pipeline::ingest` — `upsert_document` then `upsert_content`, **two non-transactional calls** (audit M7) |
| Status | live | wired but dormant (`DELPHI_SOURCES_ENABLED != true`) |

### What `RagSink` and `NotifyingSink` are

Both are `IngestSink` **middleware decorators** that wrap the `Pipeline`
(assembled in `ingestion/http.rs` as `NotifyingSink(RagSink(Pipeline))`):

- **`RagSink`** (`ingestion/rag.rs`) — after the inner sink persists the
  document, reads the stored file via `storage_uri` → extracts text →
  chunks → embeds → `upsert_chunks`, and optionally embeds
  `title + [SEP] + abstract` into `document.paper_embedding`. It is the
  **only caller of `upsert_chunks`** — i.e. the only thing that makes a
  document retrievable by RAG. Failures are warn-and-continue.
- **`NotifyingSink`** (`ingestion/notifier.rs`) — on a `Created` outcome,
  reads the row back and broadcasts a `FeedItemEvent` on the process-global
  channel that the discovery-feed SSE stream fans out. Best-effort.

### The catch

**Neither is on the upload path.** `completion.rs::commit` builds the
`Document` (with `storage_uri` already set to the S3 object) and calls
`commit_upload` — document + content only. So uploaded documents today are
**not chunked/embedded** (not RAG-retrievable) and **not broadcast** to the
feed. Removing the pipeline therefore can't be a straight delete: the
upload path must first gain these two behaviours.

## Target architecture

Chunk/embed and feed-broadcast are **not** upload-specific or
pipeline-specific — they're the **general post-persist stages** that must
run however a document arrived. Model them as **one shared post-ingest
pipeline** that every entry point runs after it commits a document:

```
acquire (upload | adapter)
  → RagIndexer.compute   (extract → chunk → embed; pure, no DB)
  → persist              (commit_upload: document + content + chunks, one txn)
  → FeedNotifier         (broadcast FeedItemEvent; best-effort)
```

The chunk/embed *compute* runs before persistence so the commit can be
atomic (see Reliability); the feed broadcast runs after. Same two stages
either way — `RagIndexer` (the compute that produces chunks +
paper_embedding) and `FeedNotifier` — shared by every entry point.

- **`RagIndexer`** — `index(doc_id, storage_uri, title, summary)`; owns
  storage + object_store + extractor + embedders + chunk config. Body =
  today's `RagSink::run_chunk_pipeline` + `run_paper_embedding`.
- **`FeedNotifier`** — `notify_created(doc_id)`; reads the row back and
  sends `FeedItemEvent`. Body = today's `NotifyingSink` `Created` branch.

These are the *same* stages that exist today — just lifted out of the
`IngestSink` decorator chain into a first-class pipeline that doesn't care
who persisted the document. A small `PostIngest` struct holds the two
stages and exposes `run(doc_id, storage_uri, title, summary)`; the upload
`/complete` path calls it after `commit_upload`, and (Phase 3) so does
anything else that commits.

### On the "sink" naming

`IngestSink` / `RagSink` / `NotifyingSink` are named for the *source → sink*
dataflow metaphor (adapters are sources; the consumer at the end of the
flow is the sink) and the decorator-middleware pattern that wraps them.
Once persistence is unified on `commit_upload` and the post-ingest stages
are a plain pipeline, that pattern is gone — so retire the "sink"
vocabulary: `RagSink → RagIndexer`, `NotifyingSink → FeedNotifier`, and
drop the `IngestSink` trait.

`commit_upload` already guarantees the doc+content atomicity that M7 was
about, so the orphan problem never existed on this path.

## Reliability — post-ingest failure & crash recovery

Post-ingest runs *after* the durable commit and is failure-prone (PDF
extraction subprocess, TEI embedding over the network) — it cannot be in
the commit transaction. Today's `RagSink` is best-effort "warn and
continue", so a failure/crash leaves a document **permanently
un-searchable** (no chunks) with no recovery. That's not acceptable as the
single ingestion path.

**Key facts that shape the fix:**
- The `document` row is ground truth and the source bytes in S3 are
  durable, so chunks/embeddings are **rebuildable** at any time.
- `upsert_chunks` is idempotent (keyed on doc + ordinal + embedding_model +
  chunk_strategy), so re-running indexing is safe and converges.
- **Lazy/on-demand indexing does not work for chunks**: RAG retrieval is a
  corpus-wide vector search over `chunk`; an un-indexed doc has no chunks,
  so it's invisible to search and nothing ever triggers its lazy index.
  Lazy only fits per-document triggers, not search-discovery.

### MVP: synchronous, atomic, fail-and-retry

Index *before* the commit and make the commit atomic, so there is never a
committed-but-unindexed document and nothing to reconcile:

```
/complete →  validate object
          →  extract → chunk → embed        (slow; no DB. failure here = upload fails)
          →  commit (one txn): document + content + chunks [+ paper_embedding]
          →  delete upload_session
          →  notify (best-effort)
```

- The slow/failure-prone work (PDF extract subprocess, TEI embedding) runs
  **before** any DB write — we don't hold a transaction across it.
- The commit writes document + content + chunks in **one** transaction and
  only then deletes the session. So on any failure the session survives and
  the client just re-POSTs `/complete` (the S3 object is unchanged →
  deterministic re-derive; `upsert_chunks` is idempotent).
- A genuinely un-indexable file (corrupt PDF, TEI down) **fails the upload**
  with a clear error — better than admitting an un-searchable doc. Note:
  extraction yielding *zero words* is **not** a failure (commit with no
  chunks, like an image-only PDF) — only extractor/embedder *errors* fail.
- **Horizontally safe by construction:** each `/complete` is self-contained
  on whichever replica handles it; no background work, no shared mutable
  state beyond the (transactional) DB. Scaling to N replicas just adds
  `/complete` throughput.
- **Feed notify stays best-effort** (post-commit, fire-and-forget): a missed
  `FeedItemEvent` self-heals because the feed is cursor-paginated from the
  DB.

Cost: `/complete` latency now includes extract+embed (seconds for a large
PDF), and a transient sidecar blip fails an upload the user must retry.
Acceptable for interactive uploads (user is present) and bot uploads (the
bot retries).

**Transaction-size ceiling.** Writing document + content + *all* chunks in
one transaction is fine for the MVP target (a scholarly paper is ~40-60
chunks, a few hundred KB). It does **not** scale to a whole book: ~800-1000
chunks + embeddings + the full transcript is several MB, and SurrealDB
buffers the entire transaction in memory (risking timeouts / write-batch
limits — verify the exact ceiling before relying on it).

**Guaranteeing atomicity for large files — two-phase publish.** You don't
get atomicity from one giant transaction; you get it by keeping partial
state *invisible* until one tiny atomic flip:

```
1. CREATE document  status = 'indexing'        ← invisible to readers
2. UPSERT document_content (full text; one row, multi-MB is one KV value)
3. INSERT chunks in idempotent batches          ← still invisible
4. UPDATE document SET status='ready', paper_embedding=…   ← atomic publish (1 small txn)
5. DELETE upload_session ; 6. notify (best-effort)
```

The flip at step 4 is a single-row transaction — the DB guarantees *that*
is atomic. The heavy writes (multi-MB content row, thousands of chunk rows)
happen before the flip while the doc is invisible, so they needn't be
atomic *with each other* — only the flip is. The guarantee rests on three
things:
1. the flip is one transaction (DB-atomic, free);
2. **every read path filters `status='ready'`** — feed, vector search,
   keyword search, `get_document`. This is code discipline; miss one query
   and a half-indexed doc leaks. This is the part we own;
3. abandoned `indexing` docs (crash before the flip) are reconciled
   (idempotent re-run finishes + flips) or GC'd — readers never saw them.

This `status`/visibility flag **is** the `index_state` column from the
async path below — one mechanism gives atomic visibility *and* the
work-queue state. Deferred with the async work; the paper MVP collapses
steps 1-4 into a single transaction (always `ready` on commit) and needs
none of it.

### Later: async indexing for scale (when synchronous `/complete` is too slow)

When indexing latency hurts or bulk/bot ingestion wants throughput, move
indexing off the request: commit the document first with
`document.index_state = pending`, and drain the work asynchronously. The
hard part under **horizontal scaling** is not double-processing across
replicas — solve it with a DB-level atomic claim rather than a separate
process:

- **Shared work, no extra deployment:** every backend also runs the drain
  loop; each claims work atomically —
  `UPDATE document SET index_state='claimed' WHERE index_state='pending' … RETURN id`
  — so replicas self-balance and never double-claim. `index_state`
  (`pending → claimed → indexed | failed` + attempt count) is the queue.
- **Or a dedicated worker deployment** — simpler to reason about, one more
  unit to run.
- Either way: idempotent re-runs, backoff + attempt cap → terminal
  `failed`, startup sweep of crash-orphaned `claimed`/`pending`.

Deferred until needed; the MVP's atomic synchronous path is correct and
needs none of it.

## Phases

### Phase 1 — make the upload path fully capable

Synchronous, atomic, fail-and-retry (see Reliability → MVP). No
`index_state`, no worker — those are the deferred scale-out path.

1. Extract a `RagIndexer` from `RagSink`: a **pure compute** step
   `compute(storage_uri, title, summary) -> { chunks, paper_embedding }`
   (extract → chunk → embed via object_store/extractor/embedders + chunk
   config). No DB writes. Errors propagate (they fail the upload);
   extraction yielding zero words returns empty chunks, not an error. Pull
   `FeedNotifier::notify_created(doc_id)` out of `NotifyingSink`. Keep
   `RagSink`/`NotifyingSink` as thin shims over these until Phase 2.
2. Extend the commit to write chunks atomically: add chunks (and
   `paper_embedding`) to the `commit_upload` transaction (CREATE document +
   UPSERT content + INSERT chunks + DELETE session), so a successful
   `/complete` lands a fully-indexed doc or nothing.
3. Reorder `completion.rs`: run `RagIndexer::compute` **before** `commit`.
   On compute error, return the completion error with the session intact
   (client re-POSTs `/complete`). After commit, fire `FeedNotifier`
   (best-effort). The completion context already has `storage_uri`, `auth`,
   and the broadcast `tx` via `AppState`.

*Exit:* a user upload produces chunks + a feed event. New
integration test: upload → `/complete` → assert chunks exist + a
`FeedItemEvent` fires (mirrors `rag_ingest` / `discovery_feed` tests).

### Phase 2 — remove the legacy pipeline (moots M7, M8)

Delete, once Phase 1 is green:
- `ingestion/pipeline.rs` (`Pipeline`, `IngestRequest`, `IngestOutcome`,
  `IngestSink`), `ingestion/http.rs` (`ingest_documents`,
  `IngestRequestBody`), the `RagSink`/`NotifyingSink` decorator wrappers
  (the service bodies live on in `RagIndexer`/`FeedNotifier`).
- The `POST /api/ingestion/documents` route in `api/mod.rs`.
- `sources/ingest_client.rs` and its loopback wiring (the scheduler is
  reworked in Phase 3; until then it has no sink to call).
- Trim `ingestion/mod.rs` re-exports accordingly.

This removes the M7 (non-transactional pipeline) and M8 (unbounded
`/ingestion/documents` metadata) surfaces entirely — mark both
**won't-fix / removed** in [`../audit.md`](../audit.md).

### Phase 3 — bot ingestion via the upload API (separate, larger)

Out of scope for the removal itself; the follow-on that makes adapters work
again:
- Source adapters download the source file and drive the upload API
  (create session → presigned PUT → `/complete`) instead of POSTing inline
  JSON.
- Auth: a Keycloak **service account** + client-credentials grant; the
  backend trusts the resulting JWT exactly like a user's (header path
  unchanged). No `SystemStorage` ingestion path.
- **Abstract-only documents** (Semantic Scholar gives metadata + abstract,
  often no PDF): decide whether to (a) store the abstract as a text object
  in S3 and run it through the same flow, or (b) add an inline-content mode
  to the upload API. Leaning (a) — keeps one path.

## Open decisions

1. **`/complete` latency budget.** Synchronous extract+embed makes
   `/complete` take seconds for a large PDF. Fine for MVP; the trigger to
   build the async path (Reliability → Later) is when that latency or bulk
   ingestion volume becomes painful. (With the synchronous MVP,
   `GET /uploads/:id` "ready" already means "indexed" — no divergence.)
2. **Abstract-only ingestion** (Phase 3) — text-as-S3-object vs inline mode.
3. Whether `IngestSink` survives at all — likely not; nothing wraps once
   the pipeline is gone.

*(Deferred to the async scale-out path: `index_state` column, worker
cadence, and the cross-replica atomic-claim mechanism.)*

## Test plan

- Phase 1: new integration test for upload → chunks + feed event; existing
  `ingestion_uploads`, `rag_ingest`, `discovery_feed` stay green.
- Phase 2: compile + full suite after deletion; confirm no dangling refs to
  `Pipeline`/`IngestSink`/`/api/ingestion/documents`.
- Both feature configs, then Tier-1 smoke (upload a PDF, confirm it appears
  in the feed and is retrievable in chat).
