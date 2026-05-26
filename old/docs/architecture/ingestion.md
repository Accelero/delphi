# Document Ingestion — Architecture

How ingestion is built. Sister doc to [`ARCH.md`](./ARCH.md);
implements [`../specs/ingestion.md`](../specs/ingestion.md). The forward
plan for removing the remaining legacy JSON path is
[`ingestion-unify-on-upload.md`](./ingestion-unify-on-upload.md), with
the async work-queue version in [`scaling-nats.md`](./scaling-nats.md).
This document describes what is actually in the tree.

## Topology

```
Browser (Uppy AwsS3 multipart)
   │  1. POST /api/ingestion/uploads            → create session, open S3 multipart
   │  2. POST /api/ingestion/uploads/:id/sign-part (×N) → presigned PUT per part
   │  ──direct PUT──▶ S3 (MinIO)                ← bytes never traverse the backend
   │  3. POST /api/ingestion/uploads/:id/complete → run completion pipeline, commit
   └  (4. GET /api/ingestion/uploads/:id)        → recovery-poll status

Backend (axum)
   ├─ ingestion/ : upload endpoints + completion pipeline + autofill seam + validation
   ├─ object_store/ : ObjectStore trait → S3ObjectStore (aws-sdk-s3)
   ├─ text_extractor/ : sandboxed pdftotext
   └─ storage/ : SurrealDB (document, document_content, upload_session, ingestion_rejection)

Reference-only path: POST /api/ingestion/documents (JSON, no bytes) → shared upsert.
```

All four upload endpoints require the `ingester` role in the JWT
(Keycloak composite `owner` includes it). Engine PERMISSIONS scope
`upload_session` to `(tenant_id, user_id)`; handlers re-check the loaded
row's identity (belt-and-suspenders).

## Object storage

A single `ObjectStore` trait (`object_store/mod.rs`) with the production
impl `S3ObjectStore` (`aws-sdk-s3`, `object_store/s3.rs`). It serves
MinIO / Hetzner / R2 / B2 / AWS through one client; selected at runtime
from `DELPHI_INGEST_OBJECT_STORE_URL=s3://<bucket>/`.

- **Dual endpoint.** `DELPHI_INGEST_S3_ENDPOINT_INTERNAL` is used for backend→S3
  calls (HEAD/GET/complete/abort); `DELPHI_INGEST_S3_ENDPOINT_PUBLIC` is baked
  into the presigned upload/download URLs the browser hits directly. Both
  tiers now publish MinIO on `:9000` and the browser reaches it
  directly — Traefik is no longer in the byte path. See
  [`object-access.md`](./object-access.md).
- **In-process test shim.** `MemObjectStore` (`object_store/mem.rs`)
  implements the full multipart surface in memory (`mem-multipart://`
  URLs), so integration tests drive create→sign→complete with no Docker.
  (The former `LocalFsObjectStore` was removed; the shim moved here.)
- **CORS.** Relies on MinIO's built-in default CORS, which echoes the
  request `Origin`, allows `PUT`/`GET`/`HEAD`, and exposes `ETag` +
  `Accept-Ranges`/`Content-Range` (which Uppy reads from each part's PUT
  response, and which PDF.js needs for ranged downloads). The per-bucket
  `mc cors set` S3 API is not implemented by MinIO, so `minio-init` does
  not configure CORS; production restricts origins via the server-level
  `MINIO_API_CORS_ALLOW_ORIGIN` env on the `minio` service.

## Delivery (download)

Both directions of object access are minted behind the swappable
`AccessMinter` seam — the backend hands the client a short-lived,
scoped URL and stays out of the byte path. Upload parts are presigned
`PUT`s; **download** is a presigned `GET`: `GET /api/documents/:key/view-url`
runs the tenant-scoped `get_document` authz check, then mints a presigned
`GET` (`INGEST_DOWNLOAD_URL_TTL_SECS`, default 120s) and returns
`{ url, expires_at }`. The SPA's `PdfViewer` hands that URL to PDF.js,
which fetches bytes (with range requests) directly from the store. The
old byte-streaming `GET /api/documents/:key/file` proxy is gone. Full
design + the deferred CDN/STS/proxy minters:
[`object-access.md`](./object-access.md). (`ObjectStore::get_by_url`
remains for the backend's *own* RAG chunk-extraction read, not for
client delivery.)

## The upload endpoints

`ingestion/uploads.rs`. Thin handlers; the heavy logic lives in the
completion pipeline and the validators.

| Endpoint | Body limit | Does |
|---|---|---|
| `POST /uploads` | small | validate prefill metadata, mint `doc_id` (ULID), open S3 multipart, INSERT `upload_session(state="uploading")`, return `{ doc_id, key, upload_id, part_size_bytes, part_url_ttl_secs }` |
| `POST /uploads/:id/sign-part` | tiny | presign one part PUT against the public S3 endpoint |
| `POST /uploads/:id/complete` | bounded | CAS `uploading→validating`, complete the S3 multipart, run the completion pipeline, return `{ result: "ready", doc_id }` or `{ result: "rejected", reason }` |
| `GET /uploads/:id` | 0 | status: session state, or `ready` (record-id lookup), or rejection reason |

The client never supplies `tenant_id`, `key`, `upload_id`, or
`storage_uri`; all are server-derived. `source_type` defaults to
`"manual"`; `source_uri` defaults to a placeholder URN when absent;
`canonical_id` is omitted by the SPA (see Identity below). Unknown body
fields (e.g. the `filename` the SPA currently sends) are ignored.

## The completion pipeline — workflow as code

`ingestion/completion.rs::run_completion` is one function whose body is
the ordered workflow (the `/complete` handler just CASes the state and
calls it). Stages, in fixed order:

1. **Validate uploaded object** (`validation/object.rs`) — HEAD for real
   size, ranged-GET sniff window for magic bytes, format-parse, polyglot
   rejection. Reject ⇒ wipe S3 + record rejection (fatal).
2. **Extract text** (`ingestion/text_extract.rs`) — one **bounded**
   ranged GET (capped at `pdf_max_input_bytes`), PDFs through the
   sandboxed `pdftotext` discipline (timeout + `kill_on_drop` + capped
   stdout), text/markdown a bounded UTF-8 read. Failure ⇒ empty text,
   non-fatal. This is a deliberate, capped exception to "upload bytes
   never stream through the backend": validation and text extraction read
   bounded ranges from the already-committed object after `/complete`.
3. **Autofill** (`ingestion/autofill.rs`) — feed the text + prefill to a
   `MetadataExtractor`. Failure ⇒ empty, non-fatal.
4. **Validate autofill output** (untrusted) — drop if invalid, non-fatal.
5. **Merge** — `merge_metadata(prefill, autofill)`: prefill wins,
   unset optional fields stay unset.
6. **Validate merged metadata** — final gate against the policy's
   `required_fields`. Fail ⇒ reject (fatal).
7. **Commit** — one SurrealDB transaction: `CREATE document:<doc_id>`
   (deterministic record id), UPSERT `document_content`, DELETE the
   `upload_session`.

Fatal stages (1, 6) and the canonical-id conflict path route through the
shared reject flow (wipe S3 object, delete session, write an
`ingestion_rejection` row) via `SystemDb`. Non-fatal stages (2–4) degrade
because the bytes are already committed.

## The metadata-extractor seam

`MetadataExtractor` (`ingestion/autofill.rs`) is the dependency-inversion
boundary for automated metadata derivation:

```rust
#[async_trait]
trait MetadataExtractor: Send + Sync {
    async fn extract(&self, ctx: &ExtractionContext<'_>) -> Result<ExtractedMetadata>;
}
```

`ExtractionContext` carries the extracted `text` + the user `prefill`;
`ExtractedMetadata` maps onto the `Document` descriptive fields plus a
free-form `extra` object. It is wired into `AppState` as
`Arc<dyn MetadataExtractor>`. **Today it is `NoopExtractor`** (returns
empty), so automated metadata is a no-op; the LLM-backed implementation
is the main remaining work (see roadmap). Swapping it in requires no
caller change.

## Validation surface

Two small, audit-focused functions in `ingestion/validation/`:

- `validate_ingestion_metadata(req, policy)` — the wire request at
  `/uploads` (content-type allowlist, size cap, metadata depth/size,
  `canonical_id`/`source_uri` shape *when present*).
- `validate_descriptive_metadata(view, policy)` — the descriptive
  metadata at pipeline stages 4 and 6 (title length, `published_at`
  sanity, `extra` depth/size, `required_fields`). Shares helpers with the
  wire validator but is a distinct entry point (different input type).

`MetadataPolicy.required_fields` (a `MetadataField` set) is configured
from env and defaults **empty** — nothing is app-mandatory while autofill
is a no-op.

## Identity, dedup, and the schema

`backend/schema.surql`:

- **`doc_id` is the identity.** A ULID minted at `/uploads`; the document
  is `CREATE`d as `document:<doc_id>`, so the SurrealDB record id *is*
  the doc_id. This lets `GET /uploads/:id` resolve `ready` by record-id
  lookup after the session row is gone.
- **`canonical_id` is optional** (`option<string>`), unset for manual
  uploads. The conflict pre-check in `commit_upload` / `upsert_document`
  is **skipped when it is `None`** (otherwise every manual upload would
  false-match the previous `NONE` row).
- **Dedup is "unique-when-set"** via a computed `dedup_key` field
  (`NONE` when `canonical_id` is `NONE`, else `"<tenant>|<canonical_id>"`)
  with a single-field UNIQUE index — because SurrealDB does not support
  filtered/partial unique indexes and composite-unique does not exclude
  `NONE`. The app computes `dedup_key` (the engine can't: a `VALUE`
  clause runs before the `tenant_id` `DEFAULT`).
- **Hard-required, server-derived fields:** record id, `tenant_id`
  (`DEFAULT $auth.tenant_id`), `source_type`, `source_uri`,
  `content_hash` (the S3 ETag — opaque, not a stable content hash).
- **Tables:** `document` (committed corpus), `document_content`
  (extracted text, 1:1), `upload_session` (in-flight only, deleted on
  commit), `ingestion_rejection` (short-TTL reject reasons for status
  polling, system-written only).

## Frontend

- **Route + tab.** `routes/upload.tsx` — a thin enqueue surface
  (drag-drop / picker, optional metadata form active for single-file
  only). Submit hands off and immediately frees the UI.
- **`UploadManager`** (`lib/uploadManager.ts`) — a framework-agnostic
  observable (consumed via `useSyncExternalStore`) that owns the upload
  task state machine (`queued → creating → uploading → validating →
  ready | failed`) and lives **above the router** (mounted once in
  `UploadProvider` in `__root.tsx`), so uploads survive navigation.
- **`uppyDriver.ts`** — `@uppy/aws-s3` v4 in multipart mode; its
  callbacks delegate to the manager. The `/complete` 200 is the terminal
  `ready` signal on the happy path; `GET /uploads/:id` is a recovery poll
  for a dropped/timed-out `/complete`.
- **`UploadTracker`** (`components/upload/`) — a persistent,
  always-mounted widget rendering per-task progress/state; `ready`
  auto-dismisses, `failed` is dismissable and self-expires.

## Deployment

Both compose tiers run MinIO + a `minio-init` one-shot (bucket create +
lock anonymous access off). Both tiers expose MinIO on `:9000` for the
browser; Traefik is not in the object byte path in either tier (matching
prod, where a managed store is reached directly). Backend env:
`DELPHI_INGEST_OBJECT_STORE_URL`,
`DELPHI_INGEST_S3_ENDPOINT_INTERNAL/PUBLIC`, `DELPHI_INGEST_S3_BUCKET`,
`DELPHI_INGEST_S3_ACCESS_KEY_ID/SECRET_ACCESS_KEY`, `DELPHI_INGEST_S3_FORCE_PATH_STYLE`
(all in `.env.example`). The backend image builds on `rust:1.95`
(`aws-sdk-s3` requires ≥ 1.91.1).

## Testing

- Backend: `cargo test` (both feature configs) drives the full
  create→sign→complete flow in-process against `MemObjectStore`
  (`tests/ingestion_uploads.rs`, `tests/upload_session_cross_tenant.rs`).
  A MinIO-testcontainer test gated by `MINIO_TEST_ENDPOINT` covers the
  real S3 client.
- Frontend: `make frontend-test` (Vitest under Node); `uploadManager.test.ts`
  is the high-value state-machine test.
