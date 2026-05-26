# Ingestion Microservice Migration Plan

Status: planned. This is the second rebuild slice, after chat is validated.
It turns ingestion into a durable NATS/JetStream saga with idempotent stages
and explicit document visibility.

Reference old implementation for behavior and constraints:

- `old/docs/architecture/ingestion.md`
- `old/docs/architecture/object-access.md`
- `old/docs/architecture/object-validator.md`
- `old/docs/architecture/rag.md`
- `old/docs/architecture/scaling-nats.md`
- `old/backend/src/ingestion/`
- `old/backend/src/object_store/`

## 1. Target Architecture

```text
Browser
  +- create upload/sign parts/complete/status --> api-service
  `- direct PUT/GET --------------------------> object store

api-service
  +- owns upload sessions and presigned URLs
  +- writes document(state=staging)
  `- publishes ingest.validate.requested

ingest-validator -----> object metadata + object safety validation
ingest-extractor -----> bounded text extraction + metadata autofill
ingest-chunker -------> chunk rows and expected embedding work
embedding-worker -----> chunk/document embeddings
ingest-publisher ----> final readiness barrier and state=ready flip
ingest-reconciler ---> stuck-job retry, failure, and cleanup
```

NATS distributes work; SurrealDB remains the source of truth and visibility
gate. Object bytes continue to move directly between browser and object
storage.

## 2. Storage Model

Use a new clean schema, keeping storage boundaries portable enough for a
future Postgres/Qdrant split.

Core records:

- `upload_session`: user-visible upload state and object-store multipart
  details.
- `document`: corpus document, initially `state=staging`, visible only when
  `state=ready`.
- `document_content`: extracted text and parser metadata.
- `chunk`: deterministic chunk rows keyed by document and ordinal.
- `embedding`: deterministic rows keyed by chunk/document, model, and
  pipeline version.
- `ingestion_job`: durable saga state, expected model set, pipeline version,
  attempts, current stage, and terminal error.
- `ingestion_event_outbox`: optional safety net for DB-write plus NATS-publish
  boundaries if the first implementation needs stronger recovery.

Document states:

- `staging`: upload accepted but not yet fully processed.
- `validating`: object and metadata validation running.
- `indexing`: extraction, chunking, or embedding running.
- `ready`: document is visible to corpus/search/feed/chat.
- `failed`: terminal failure; visible only in upload/job status.

Rules:

- Corpus reads, search, RAG, and feed only read `state=ready`.
- Every stage write is idempotent by deterministic keys.
- Completion is derived from durable state, not from decrementing counters.
- Pipeline version and embedding model set are captured at job start.
- The final publish step is a single durable visibility flip.

## 3. NATS Design

Use one `INGEST` JetStream stream with stage subjects:

- `ingest.validate.requested`
- `ingest.extract.requested`
- `ingest.chunk.requested`
- `ingest.embed.requested`
- `ingest.publish.requested`
- `ingest.failed`
- `ingest.compensate.requested`
- `ingest.reconcile.requested`

Durable pull consumers:

- `ingest-validator`
- `ingest-extractor`
- `ingest-chunker`
- `embedding-worker`
- `ingest-publisher`
- `ingest-reconciler`

Message rules:

- Every message includes `v`, `tenant_id`, `job_id`, `document_id`,
  `pipeline_version`, `attempt`, and `causation_id`.
- Use deterministic `Nats-Msg-Id` for stage transitions.
- Each handler writes durable state, publishes the next event, waits for
  PubAck, then acks the current message.
- Redelivery is normal and must be safe.
- Terminal failures publish failure state and stop normal progression.

## 4. API Surface

Initial endpoints:

- `POST /api/ingestion/uploads`
  - validates metadata request.
  - creates `upload_session`.
  - opens multipart upload.
  - returns object-store upload details.
- `POST /api/ingestion/uploads/:id/sign-part`
  - returns a short-lived presigned PUT URL.
- `POST /api/ingestion/uploads/:id/complete`
  - completes multipart upload.
  - creates `document(state=staging)` and `ingestion_job`.
  - publishes `ingest.validate.requested`.
  - returns `{ result: "accepted", document_id, job_id }`.
- `GET /api/ingestion/uploads/:id`
  - returns upload/job status for frontend recovery.
- `GET /api/ingestion/jobs/:id`
  - returns detailed stage status for the upload tracker.

The HTTP complete endpoint should not block on extraction, chunking, or
embedding.

## 5. Stage Behavior

Validation:

- HEAD object for actual size and metadata.
- Range-read only bounded sniff windows.
- Validate object type and reject dangerous PDF/text payloads.
- Validate user metadata and final required fields policy.
- On success, publish extraction.
- On terminal validation failure, mark job/document failed and write a
  user-visible rejection reason.

Extraction:

- Bounded object read.
- PDF extraction through a sandboxed/timeout-controlled extractor.
- Text/markdown extraction through bounded UTF-8 handling.
- Metadata autofill may fail non-fatally.
- Persist `document_content` and metadata candidates.
- Publish chunking.

Chunking:

- Deterministically chunk content by document and ordinal.
- Upsert chunk rows.
- Record expected embedding tasks from captured model set.
- Publish one `ingest.embed.requested` per chunk/model, plus document-level
  embedding tasks if configured.

Embedding:

- Dedicated worker pool.
- Calls TEI/provider with bounded concurrency.
- Upserts embedding by deterministic key.
- After each successful embed, checks the state-derived barrier.
- If all expected embeddings exist, publish `ingest.publish.requested`.

Publish:

- Re-read required job/document state.
- Verify expected chunks/embeddings exist.
- Flip `document.state` to `ready` atomically.
- Mark job `ready`.
- Emit future feed-ready event once feed rework exists.

Reconcile:

- Periodically finds jobs stuck in non-terminal states.
- Requeues retryable stages.
- Marks terminal failures after retry budget.
- Deletes or expires abandoned staging objects through lifecycle policy and
  cleanup backstops.

## 6. Frontend

Use the existing direct-upload UX pattern, rebuilt cleanly:

- Upload page with shadcn components.
- Multipart upload manager.
- Persistent upload tracker.
- Status polling by upload/job id.
- User-visible states: queued, uploading, accepted, validating,
  extracting, chunking, embedding, ready, failed.
- Ready state links to the document/corpus view once those surfaces exist.

The frontend should treat `/complete` as acceptance into async processing,
not as final readiness.

## 7. Test Plan

Unit tests:

- metadata validation.
- object validation decisions.
- deterministic chunk ids.
- deterministic embedding ids.
- idempotent stage writes.
- barrier logic under duplicate embed completion.

Integration tests:

- upload complete publishes validation event.
- each stage handles duplicate delivery safely.
- worker crash before ack causes redelivery without duplicates.
- failure marks job failed and hides document from corpus.
- publish flips document to ready only when all expected outputs exist.

T2 e2e tests:

- authenticated upload to accepted job.
- status progresses through stages.
- ready document appears in corpus read model.
- failed validation shows rejection reason.
- kill/restart worker during processing and verify recovery.

Manual gate:

- Upload a PDF.
- Watch status progress.
- Verify ready visibility.
- Verify failed object rejection.
- Verify duplicate/retry behavior with worker restarts.

## 8. Assumptions

- Embedding remains on the critical path for initial ingestion.
- A future mode may make embeddings degradable, but not in this phase.
- Object storage remains S3-compatible.
- SurrealDB stores initial vectors, but embedding records are shaped so
  Qdrant can later become the vector store.
- Feed notification from ingestion is deferred until feed rework.
