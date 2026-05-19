# Ingestion v2 — Direct-to-S3 upload via Uppy + presigned multipart

Status: planned. Sister doc to [`ARCH.md`](../ARCH.md) and
[`SECURITY.md`](../SECURITY.md). The ingestion pipeline described here
supersedes the in-process single-shot `POST /api/ingestion/documents`
path for any flow that carries bytes; the JSON-only endpoint is kept
for URL-reference-only ingestion.

## Goals

- A first-class **document upload API** the SPA, OAuth clients (e.g.
  the future containerised arxiv adapter), and custom adapters all use
  through the same endpoints. JWT-only auth, no special trust for any
  client class.
- Bytes go **direct from client to S3-compatible storage** via Uppy's
  `AwsS3Multipart`, with the backend minting every presigned URL.
- Storage is **provider-agnostic** behind one `ObjectStore` interface.
  MinIO, Hetzner, R2, B2, AWS — same code.
- Tenant scoping is **enforced engine-side** by SurrealDB PERMISSIONS
  on every table, with a redundant app-level check at every handler
  boundary. The handler is the belt, the engine is the suspenders.
- The `document` table contains **only validated content**. Anything
  in-progress lives in a separate `upload_session` table. "Make
  illegal states unrepresentable" at the schema level.
- The **single invariant** that guards the system: *undocumented S3 is
  dangerous — the backend never reads it, the cleaner eventually
  deletes it*.

## Non-goals (this milestone)

- Adapter extraction into its own container — next milestone. The
  arxiv adapter keeps calling the API and may need a small call-site
  update.
- The text-extraction / chunking / embedding pipeline. Bytes land, a
  `document` row is written; downstream processing is a separate
  piece of work.
- AV scanning. ClamAV sidecar is documented in
  [`SECURITY.md`](../SECURITY.md) as a production-deployment add-on,
  not a dev concern.
- Resumable-across-page-refresh upload state. Uppy resumes within a
  session; deep resume is later.
- Per-tenant STS credentials. One service credential; tenancy via
  signed prefix. STS layering (MinIO/AWS) is defence-in-depth and
  comes later if we want it.
- A cancel/abort endpoint on in-progress uploads. To abandon an
  upload the client simply stops driving it; the nightly cleaner
  reaps the session and the S3 multipart upload. The only DELETE in
  the API surface is on committed documents, via the existing
  document-delete path.

## Architecture overview

```
SPA / OAuth client / custom adapter (Uppy AwsS3Multipart-shape)
   │
   ├── POST   /api/ingestion/uploads                 → validate metadata, open multipart, create session
   ├── POST   /api/ingestion/uploads/:id/sign-part   → presigned PUT for one part
   ├── POST   /api/ingestion/uploads/:id/complete    → complete multipart, run validator, write document or wipe
   └── GET    /api/ingestion/uploads/:id             → status polling (state, ready+doc_id, or rejection reason)

Backend
   │
   └── ObjectStore (trait)
        ├── S3ObjectStore     (aws-sdk-s3, configurable endpoint)
        └── LocalFsObjectStore (already exists, tests only)

S3-compatible store (MinIO / Hetzner / R2 / B2 / AWS)
   bucket/
     tenants/<tenant_slug>/<doc_uuid>     ← path enforced by backend signature
```

The existing JSON-only ingestion endpoint
(`POST /api/ingestion/documents`) is retained for the *no-file-body*
case (URL-reference-only ingestion). The two paths share
`Storage::upsert_document` once the bytes are validated.

## Canonical storage_uri form

One single canonical shape across the system. Backend writes it,
cleaner compares against it, and a shared helper renders it from a
key:

```text
storage_uri = "s3://<bucket>/<key>"
key         = "tenants/<tenant_slug>/<doc_id>"
```

Rules: no querystring, no leading slash, exactly that prefix. The
`tenant_slug` is the `tenant.slug` field (lowercase `[a-z0-9-]`, per
the existing schema ASSERT) — never the raw SurrealDB record id, which
contains a colon. A `storage_uri_for_key()` helper in
`storage/object_store.rs` is the single place that constructs this
string; both the `/complete` handler and the cleaner call it. A unit
test round-trips through that helper.

## State diagram

```
(no row)                                 -- before POST /uploads
   │
   ▼   create
upload_session.state = "uploading"        -- S3 multipart upload open
   │
   ▼   complete (S3 CompleteMultipartUpload OK; CAS state transition)
upload_session.state = "validating"       -- bytes committed, validator running
   │
   ├── pass (transaction: INSERT document; DELETE session)
   │      ▼
   │   document row exists, no session   -- terminal: ready to serve
   │
   └── fail (transaction: DeleteObject; DELETE session; log rejection)
          ▼
       no row anywhere                   -- terminal: rejected
```

No persistent `failed` or `quarantined` state on either table.
Rejection metadata (reason, sniffed_type, tenant, doc_id, timestamp)
lives in a small `ingestion_rejection` table that the SPA's status
poll consults; the cleaner reaps it after a short TTL.

## DB schema

Three tables. The `document` table is unchanged in shape from today —
no `state` column added. `upload_session` and `ingestion_rejection`
are new.

```sql
-- in-progress uploads only; deleted on terminal transition
DEFINE TABLE upload_session SCHEMAFULL
    PERMISSIONS
        FOR select WHERE tenant_id = $auth.tenant_id
                     AND user_id   = $auth.id
        FOR create WHERE tenant_id = $auth.tenant_id
                     AND user_id   = $auth.id
        FOR update WHERE tenant_id = $auth.tenant_id
                     AND user_id   = $auth.id
        FOR delete WHERE tenant_id = $auth.tenant_id
                     AND user_id   = $auth.id;

DEFINE FIELD tenant_id ON upload_session TYPE record<tenant>
    DEFAULT $auth.tenant_id
    ASSERT  $value != NONE;
DEFINE FIELD user_id   ON upload_session TYPE record<app_user>
    DEFAULT $auth.id
    ASSERT  $value != NONE;

DEFINE FIELD doc_id                ON upload_session TYPE string;
DEFINE FIELD s3_key                ON upload_session TYPE string;
DEFINE FIELD s3_upload_id          ON upload_session TYPE string;
DEFINE FIELD state                 ON upload_session TYPE string
    ASSERT $value IN ["uploading", "validating"];
DEFINE FIELD canonical_id          ON upload_session TYPE string;
DEFINE FIELD declared_size         ON upload_session TYPE int;
DEFINE FIELD declared_content_type ON upload_session TYPE string;
DEFINE FIELD declared_metadata     ON upload_session FLEXIBLE TYPE object;
DEFINE FIELD started_at            ON upload_session TYPE datetime DEFAULT time::now();

DEFINE INDEX upload_session_canonical
    ON upload_session FIELDS tenant_id, canonical_id UNIQUE;
DEFINE INDEX upload_session_started_at
    ON upload_session FIELDS started_at;  -- cleaner scans by age

-- rejection reasons for status polling; reaped by cleaner
DEFINE TABLE ingestion_rejection SCHEMAFULL
    PERMISSIONS
        FOR select WHERE tenant_id = $auth.tenant_id
                     AND user_id   = $auth.id
        FOR create, update, delete WHERE FALSE;  -- system writes only

DEFINE FIELD tenant_id    ON ingestion_rejection TYPE record<tenant>;
DEFINE FIELD user_id      ON ingestion_rejection TYPE record<app_user>;
DEFINE FIELD doc_id       ON ingestion_rejection TYPE string;
DEFINE FIELD reason       ON ingestion_rejection TYPE string;
DEFINE FIELD sniffed_type ON ingestion_rejection TYPE option<string>;
DEFINE FIELD rejected_at  ON ingestion_rejection TYPE datetime DEFAULT time::now();

DEFINE INDEX ingestion_rejection_doc
    ON ingestion_rejection FIELDS tenant_id, doc_id;
```

The `user_id = $auth.id` rule in `upload_session` means only the
originating identity can sign-part / complete / GET-status — whether
that identity is a human user or an OAuth client.

The existing `UNIQUE (tenant_id, canonical_id)` index on `document`
stays as-is. The matching index on `upload_session` prevents two
simultaneous uploads of the same canonical id from burning bandwidth;
the second attempt at `/create` gets a 409.

## DB access pattern

Which connection is used for which operation, made explicit so
implementation isn't ambiguous:

| Operation | Connection | Reason |
|---|---|---|
| `POST /uploads` | AuthedDb | per-request; engine PERMISSIONS scope |
| `POST /uploads/:id/sign-part` | AuthedDb | per-request; engine PERMISSIONS scope |
| `POST /uploads/:id/complete` | AuthedDb | per-request; engine PERMISSIONS scope |
| `GET /uploads/:id` | AuthedDb | per-request; engine PERMISSIONS scope |
| Validator (inside `/complete`) | AuthedDb | same request scope |
| Nightly cleaner | SystemDb | cross-tenant orphan sweep |

The cleaner is the only system-DB caller in this design. Every
user-facing endpoint goes through AuthedDb, so engine PERMISSIONS are
load-bearing on every read and write.

## Storage trait additions

`Storage` (and its `AuthedDb` impl) gain typed methods for upload
sessions. No raw SurrealQL escapes the storage module:

```rust
async fn create_upload_session(&self, req: &CreateSessionParams)
    -> Result<UploadSession>;

async fn get_upload_session(&self, doc_id: &str)
    -> Result<Option<UploadSession>>;

/// Compare-and-swap state transition. Returns true if the row was
/// updated (caller proceeds), false if it wasn't (another caller has
/// the session or it doesn't exist). Implemented as
/// `UPDATE upload_session WHERE doc_id = $d AND state = $from
///  SET state = $to RETURN BEFORE`.
async fn cas_upload_session_state(&self, doc_id: &str, from: &str, to: &str)
    -> Result<bool>;

/// Commit transaction: INSERT document + DELETE upload_session in
/// one Surreal transaction. On UNIQUE conflict on
/// `document(tenant_id, canonical_id)` returns
/// `Err(StorageError::CanonicalIdConflict { existing_doc_id })`.
async fn commit_upload(&self, doc_id: &str, doc: &Document)
    -> Result<DocId>;

/// Used by the abort path and the validator-reject path.
async fn delete_upload_session(&self, doc_id: &str) -> Result<()>;

/// Validator-reject side-channel; one row, one short-TTL.
async fn record_ingestion_rejection(&self, rec: &IngestionRejection)
    -> Result<()>;

async fn get_ingestion_rejection(&self, doc_id: &str)
    -> Result<Option<IngestionRejection>>;
```

The `SystemDb` impl additionally exposes:

```rust
async fn list_stale_upload_sessions(&self, older_than: DateTime<Utc>)
    -> Result<Vec<UploadSession>>;

async fn delete_upload_sessions_before(&self, cutoff: DateTime<Utc>)
    -> Result<usize>;

async fn list_documents_storage_uris(&self) -> Result<HashSet<String>>;

async fn delete_old_rejections(&self, cutoff: DateTime<Utc>)
    -> Result<usize>;
```

## Endpoint contracts

All four new endpoints require the **`ingester` role** in the JWT's
`roles` claim. Role hierarchy lives in the IdP, not in the
backend — `owner` is configured as a Keycloak composite role that
includes `ingester`, so owners ingest without a separate assignment.
The handler check is therefore a flat `auth.roles.iter().any(|r| r ==
"ingester")`, not a list; adding higher-tier roles in the future is a
Keycloak realm config change, not a code change.

Per-route body limits set explicitly via `axum::extract::DefaultBodyLimit`:

| Route | Body limit |
|---|---|
| `POST /uploads` | 8 KB |
| `POST /uploads/:id/sign-part` | 256 B |
| `POST /uploads/:id/complete` | 640 KB (10,000 parts × ~64 B) |
| `GET /uploads/:id` | 0 |

Bytes never traverse the backend; the actual upload size is bounded
at S3 only, not at the backend.

### 1. `POST /api/ingestion/uploads`

Request:
```json
{
  "canonical_id": "arxiv:2401.12345" | "manual:6c4a..." | ...,
  "source_type": "arxiv" | "manual" | "...",
  "source_uri": "https://arxiv.org/abs/2401.12345",
  "title": "...",
  "content_type": "application/pdf",
  "size": 8421376,
  "metadata": { ... }
}
```

For `source_type = "manual"`, the SPA generates a UUID and
constructs `canonical_id = "manual:<uuid>"`. Other source types
use their natural canonical id (`arxiv:<id>`, `doi:<id>`, etc.).
The `MetadataPolicy.canonical_id_pattern` regex enforces shape.

Handler:
1. JWT → tenant + auth.id. Reject any client-supplied `tenant_id`,
   `storage_uri`, `key`, or `upload_id`.
2. Role check: `auth.roles` contains `"ingester"`. 403 otherwise.
3. **`validate_ingestion_metadata(req, policy)`** — see security
   section. 400 on failure.
4. Generate `doc_id = uuid()`. Key = `tenants/<slug>/<doc_id>`.
5. `ObjectStore::create_multipart_upload(key, content_type)`.
   `content_type` is recorded on object metadata; **actual
   enforcement of content-type happens at `/complete`** in the
   validator, not in S3.
6. INSERT `upload_session(state="uploading", ...)`. UNIQUE conflict
   on `(tenant_id, canonical_id)` → 409.
7. Return `{ doc_id, key, upload_id, part_size, part_url_ttl_secs }`.

### 2. `POST /api/ingestion/uploads/:doc_id/sign-part`

Request: `{ "part_number": 3 }`. 1-indexed, S3 convention; max 10,000.

Handler: load session (AuthedDb — engine PERMISSIONS filter by
tenant + user). Belt: check `state="uploading"` and
`started_at > now() - session_ttl`. Mint presigned PUT with
`INGEST_UPLOAD_PART_URL_TTL_SECS`. Return `{ url }`.

The presigned URL does **not** include a per-part `Content-Length`
signed header — see "What S3 enforces" below.

### 3. `POST /api/ingestion/uploads/:doc_id/complete`

Request:
```json
{ "parts": [ { "part_number": 1, "etag": "\"abc\"" }, ... ] }
```

Handler:
1. CAS state transition: `cas_upload_session_state(doc_id,
   "uploading", "validating")`. If false (another caller has it,
   or it doesn't exist, or wrong state): return 409 with the
   current state if known.
2. `ObjectStore::complete_multipart_upload(...)`. On error: CAS
   state back to `"uploading"` (so the SPA can retry), return
   502. If S3 reports the upload already completed, treat as
   idempotent success.
3. **`validate_uploaded_object(session, object_store, policy)`**
   — see security section.
4. On pass: `commit_upload(doc_id, doc)` (atomic INSERT + DELETE
   session in one Surreal transaction).
   - On `CanonicalIdConflict { existing_doc_id }`:
     `ObjectStore::delete(key)`, `delete_upload_session(doc_id)`,
     `record_ingestion_rejection(reason="canonical_id_conflict")`,
     return 422 with `{ reason: "canonical_id_conflict",
     existing_doc_id }`.
   - On other DB errors: log, leave session in `"validating"`
     state (cleaner picks it up via age), return 500. The S3
     object is the orphan in this case; cleaner reaps it.
   - On success: return 200 `{ doc_id, state: "ready" }`.
5. On reject (validator returned `ObjectReject`):
   `ObjectStore::delete(key)`, `delete_upload_session(doc_id)`,
   `record_ingestion_rejection(reason=...)`, return 422 with
   `{ reason }`.

**Retried `/complete` while state="validating":** the CAS in step 1
fails (state is already "validating"), so the handler returns 409
with `state="validating"`. The SPA polls `GET /uploads/:doc_id`
until it sees the terminal state.

### 4. `GET /api/ingestion/uploads/:doc_id`

Status polling. Resolution order:

1. If `upload_session` exists → `{ state: session.state }`.
2. If `document` exists with `doc_id` (via a tenant-scoped lookup)
   → `{ state: "ready", doc_id }`.
3. If `ingestion_rejection` exists → `{ state: "rejected", reason }`.
4. Else → 404 (the session is old enough to have been swept and
   the rejection log expired, or the doc_id never existed for this
   identity).

All three lookups go through AuthedDb; engine PERMISSIONS scope to
the originating user, so one user cannot probe another's doc ids.

## Security-relevant code: two encapsulated functions

These two functions are the entire security surface for ingestion.
They are flagged as audit-critical and live in their own module so
reviewers, refactors, and future hardening (AV scanner, deeper
parsers, format-specific checks) target one place.

### `validate_ingestion_metadata`

```rust
// backend/src/ingestion/validation/metadata.rs

pub struct MetadataPolicy {
    pub allowed_content_types: HashSet<String>,
    pub max_size_bytes: u64,
    pub max_title_chars: usize,
    pub max_metadata_depth: usize,
    pub max_metadata_bytes: usize,
    pub canonical_id_pattern: Regex,
}

pub enum MetadataReject {
    DisallowedContentType,
    SizeExceedsLimit,
    TitleTooLong,
    MetadataTooDeep,
    MetadataTooLarge,
    InvalidCanonicalId,
    MalformedRequest(String),
}

pub fn validate_ingestion_metadata(
    req: &CreateUploadRequest,
    policy: &MetadataPolicy,
) -> Result<(), MetadataReject>;
```

Called synchronously at the top of `POST /uploads`, before any S3
operation. Pure function — same input + same policy → same answer.
Auditable as a unit; testable with property tests.

Layer-1 of the validation stack. Closes audit item **M8** (unbounded
metadata) by construction.

### `validate_uploaded_object`

```rust
// backend/src/ingestion/validation/object.rs

pub struct ObjectPolicy {
    pub allowed_content_types: HashSet<String>,
    pub size_tolerance_bytes: u64,        // accept declared - actual within this band
    pub sniff_window_bytes: usize,        // ranged GET window for magic-byte detection
    pub pdf_parse_timeout: Duration,
    pub pdf_max_pages: usize,
    pub pdf_max_input_bytes: u64,         // hard cap on bytes piped into the parser
    pub reject_polyglots: bool,           // if sniffer matches >1 allowed type → reject
}

pub enum ObjectReject {
    SizeMismatch { declared: u64, actual: u64 },
    ContentTypeMismatch { declared: String, sniffed: String },
    NotInAllowlist,
    Polyglot { matched: Vec<String> },
    PdfParseFailed,
    PdfParseTimeout,
    PdfTooManyPages,
    Utf8DecodeFailed,
    HeadFailed(String),
}

pub async fn validate_uploaded_object(
    key: &str,
    declared_size: u64,
    declared_content_type: &str,
    object_store: &dyn ObjectStore,
    policy: &ObjectPolicy,
) -> Result<ValidatedAttrs, ObjectReject>;

pub struct ValidatedAttrs {
    pub size: u64,
    pub etag: String,
    pub sniffed_content_type: String,
}
```

Validator memory and download budget:

- **HEAD first.** Verifies `actual_size` against `declared_size`
  (within `size_tolerance_bytes`) and captures the committed ETag.
  If `actual_size > policy.pdf_max_input_bytes` and the declared
  type is PDF → reject without downloading.
- **Ranged GET for sniff.** Only `policy.sniff_window_bytes` are
  fetched for the magic-byte check (`infer` / `tree_magic`). Never
  the full file.
- **Bounded download for parse.** For PDF: stream into a tempfile
  capped at `policy.pdf_max_input_bytes` (abort-on-exceed), then
  spawn the parser as a sandboxed child process (timeout +
  `kill_on_drop` + capped stdout, same discipline as H4 hardening
  on `pdftotext`). For text: stream into memory under the same
  cap and run UTF-8 validation.
- **Polyglot rejection.** If `reject_polyglots = true` and the
  sniffer matches more than one allowed type (e.g. a PDF that's
  also a valid ZIP), reject as
  `Polyglot { matched: [...] }`.

This is where future security upgrades land:
- ClamAV scan (production deployments only — see
  [`SECURITY.md`](../SECURITY.md)).
- Better PDF parsers (page-count limits, JS-content detection).
- New content types: extend `ObjectPolicy` and the dispatch table
  inside this function.

Both functions are explicitly out of the request handler bodies.
Handlers compose them; they don't open-code validation logic. Anyone
reviewing security only needs to read these two files plus the few
lines of handler that call them.

## What S3 actually enforces

The original draft of this plan over-claimed; corrected here so the
implementing agent doesn't burn time on signatures that don't exist:

- **Content-Type is recorded, not enforced.**
  `CreateMultipartUpload` accepts a `ContentType` parameter that's
  stored as object metadata. The individual `UploadPart` requests
  do **not** carry `Content-Type`, and S3 does not verify the
  bytes match. **Actual content-type enforcement happens in the
  validator at `/complete`**, by sniffing the first N bytes and
  comparing against the declared type.
- **Per-part Content-Length is not a "ceiling".** SigV4 can include
  `content-length` in `SignedHeaders`, but in that case S3 requires
  the client to send **exactly** that value — not a maximum. A
  uniform signed part-size would reject the natural smaller final
  part. So the presigned URLs for parts do **not** include a
  signed `Content-Length`. The overall-size guarantee comes from
  the validator's HEAD at `/complete`, not from any S3-side
  ceiling.

[`SECURITY.md`](../SECURITY.md) is updated to reflect this.

## Storage abstraction

Extend the existing `ObjectStore` trait:

```rust
trait ObjectStore: Send + Sync {
    // existing
    async fn put(&self, key: &str, content_type: &str, body: Bytes) -> Result<PutOutcome>;
    async fn get(&self, key: &str) -> Result<Bytes>;
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes>;  // new — for validator sniff
    async fn delete(&self, key: &str) -> Result<()>;
    async fn head(&self, key: &str) -> Result<ObjectMeta>;

    // new — multipart
    async fn create_multipart_upload(&self, key: &str, content_type: &str) -> Result<String>;
    async fn presign_upload_part(
        &self, key: &str, upload_id: &str, part_number: u16,
        ttl: Duration,
    ) -> Result<Url>;
    async fn complete_multipart_upload(
        &self, key: &str, upload_id: &str, parts: &[PartRef],
    ) -> Result<CompleteOutcome>;
    async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> Result<()>;

    // new — listing for the cleaner
    async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectEntry>>;
    async fn list_multipart_uploads(&self) -> Result<Vec<MultipartEntry>>;
}
```

`S3ObjectStore` wraps `aws-sdk-s3`. Configured with `endpoint_url`,
`region`, `force_path_style`, credentials. Same impl serves MinIO,
Hetzner, R2, B2, AWS. `LocalFsObjectStore` gains a tiny shim for
multipart in tests.

## Document delete → S3 delete

`Storage::delete_document` is extended to call
`ObjectStore::delete(storage_uri)` best-effort after the row delete.
Failure to delete the S3 object does **not** roll back the row
deletion (the user's intent is "this document should no longer
exist"), but is logged at warn level. The nightly cleaner is the
backstop for any object the eager delete failed to remove — it now
sees that `storage_uri` referenced by no row and reaps the object.

Multi-tenant SaaS users get prompt-delete semantics by default; the
cleaner's age threshold is purely the cleanup floor for orphans, not
the typical-case latency.

## Configuration

All env, all defaulted, all documented in `.env.example`:

| Variable | Default | Notes |
|---|---|---|
| `INGEST_S3_ENDPOINT` | (none) | e.g. `https://nbg1.your-objectstorage.com` for Hetzner; unset for AWS. |
| `INGEST_S3_REGION` | `us-east-1` | Required by SDK; ignored by some providers. |
| `INGEST_S3_BUCKET` | (required) | One bucket; tenant scoping by prefix. |
| `INGEST_S3_ACCESS_KEY_ID` / `INGEST_S3_SECRET_ACCESS_KEY` | (required) | Service credential. |
| `INGEST_S3_FORCE_PATH_STYLE` | `true` | True for MinIO/Hetzner/B2; false for AWS / R2. |
| `INGEST_UPLOAD_PART_SIZE_BYTES` | `8388608` (8 MB) | Returned in `create`; Uppy uses for chunking. S3 minimum 5 MB except last part. |
| `INGEST_UPLOAD_PART_URL_TTL_SECS` | `900` (15 min) | Per-part presigned URL lifetime. |
| `INGEST_UPLOAD_SESSION_TTL_SECS` | `3600` (1 h) | Session bounded lifetime. `sign-part` rejects older. |
| `INGEST_UPLOAD_MAX_FILE_SIZE_BYTES` | `209715200` (200 MB) | Layer-1 cap. |
| `INGEST_ALLOWED_CONTENT_TYPES` | `application/pdf,text/plain,text/markdown` | Comma-separated allowlist. |
| `INGEST_VALIDATOR_SNIFF_WINDOW_BYTES` | `4096` | Ranged GET size for magic-byte check. |
| `INGEST_VALIDATOR_PDF_MAX_INPUT_BYTES` | `52428800` (50 MB) | Hard cap on bytes piped into the PDF parser. |
| `INGEST_VALIDATOR_PDF_MAX_PAGES` | `2000` | Page-count cap. |
| `INGEST_VALIDATOR_PDF_TIMEOUT_SECS` | `30` | Wall-clock timeout on PDF parse. |
| `INGEST_VALIDATOR_REJECT_POLYGLOTS` | `true` | Reject files matching >1 allowed type. |
| `INGEST_REJECTION_TTL_SECS` | `3600` (1 h) | How long `ingestion_rejection` rows are kept for status polling. |
| `INGEST_CLEANUP_MIN_AGE_HOURS` | `4` | Cleaner ignores anything younger. Asserted at startup to be `> 2 × INGEST_UPLOAD_SESSION_TTL_SECS`. |
| `INGEST_CLEANUP_CRON` | `0 3 * * *` | Nightly. See "Cleaner cadence" below. |

## Frontend (Uppy)

`@uppy/core` + `@uppy/aws-s3-multipart`. A thin component wires the
endpoints:

```ts
const uppy = new Uppy().use(AwsS3Multipart, {
  createMultipartUpload: (file) =>
    api.post('/api/ingestion/uploads', { ...metadata, content_type: file.type, size: file.size }),
  signPart: (file, p) =>
    api.post(`/api/ingestion/uploads/${file.meta.doc_id}/sign-part`, { part_number: p.partNumber }),
  completeMultipartUpload: (file, parts) =>
    api.post(`/api/ingestion/uploads/${file.meta.doc_id}/complete`, { parts }),
  // No abortMultipartUpload callback: we deliberately don't expose
  // cancel-mid-upload. If the user closes the tab, the cleaner reaps.
  getChunkSize: (file) => file.meta.part_size,
});
```

The SPA polls `GET /uploads/:id` after Uppy declares complete to
learn whether validation passed or rejected. Rejected uploads show
the reason from the `ingestion_rejection` row; no document appears.

## Nightly cleaner

```
Once per night (INGEST_CLEANUP_CRON):
  1. SystemDb: list_documents_storage_uris() → set of valid keys
  2. ObjectStore::list_multipart_uploads()
     for each m where m.initiated < now - INGEST_CLEANUP_MIN_AGE_HOURS:
        abort_multipart_upload(m.key, m.upload_id)
  3. SystemDb: list_stale_upload_sessions(now - INGEST_CLEANUP_MIN_AGE_HOURS)
                → these are sessions past TTL; delete each.
  4. ObjectStore::list_objects("tenants/")
     for each o where o.last_modified < now - INGEST_CLEANUP_MIN_AGE_HOURS
                  and storage_uri_for_key(o.key) not in valid_keys:
        delete(o.key)
  5. SystemDb: delete_old_rejections(now - INGEST_REJECTION_TTL_SECS)
```

Five operations, one shared age threshold (except for rejection
reaping, which has its own TTL). The age check on step 4 closes the
validator-vs-cleaner race: no object younger than
`INGEST_CLEANUP_MIN_AGE_HOURS` is ever a deletion candidate, and the
session TTL bounds in-flight session age to less than that.

**Cleaner cadence trade-off.** Nightly is the operational simplest
shape and was the explicit design choice. With a 4 h age threshold
and a 24 h cron, an attacker (or a buggy adapter) leaking orphaned
uploads sees them accumulate for up to ~24 h before reaping. That's
an S3-bill amplification, not a security issue — undocumented S3 is
already "never accessed by the backend" by invariant. If the bill
becomes meaningful, switch to `0 */4 * * *` (every 4 h) without
changing the age threshold.

The orphan matrix the cleaner handles:

| Case | Picked up by |
|---|---|
| S3 multipart older than threshold, no `s3_upload_id` in any session | Step 2 (abort) |
| Session row older than threshold, S3 multipart already gone | Step 3 (delete row) |
| Session row older than threshold, S3 multipart still listed | Step 2 + Step 3 |
| Committed S3 object older than threshold, no `document.storage_uri` referencing it | Step 4 (delete object) |
| Document row deleted via API, S3 delete failed | Step 4 (delete object) |
| Old `ingestion_rejection` row | Step 5 |

Runs from the existing in-process scheduler. Moves out when the rest
does.

## Tenant enforcement invariants

The plan rests on three rules, in order of primacy:

1. **Engine-side PERMISSIONS are the load-bearing defence.** Every
   table reachable by user requests carries a PERMISSIONS clause
   keyed on `$auth.tenant_id` (and `$auth.id` for upload session
   ownership). A handler that forgets its filter still cannot read
   another tenant's row, because the engine rejects it. This is
   the guarantee `ARCH.md` promises ("the database itself refuses
   cross-tenant queries").
2. **The backend never accepts `storage_uri`, `key`, `upload_id`,
   `tenant_id`, or `user_id` from request bodies.** Every one is
   server-derived from the JWT. Layer-1 metadata validation
   rejects requests that include these fields.
3. **Handlers check `session.tenant_id == auth.tenant_id` and
   `session.user_id == auth.id` redundantly** as belt-and-
   suspenders defence. If a regression slips engine-side PERMISSIONS
   off, the app-level checks catch it on the next test run.

## Tests

- **Schema-side cross-tenant test** (new, blocks step 0).
  `backend/tests/upload_session_cross_tenant.rs` — mirrors
  `cross_tenant_isolation.rs`: authenticated as tenant A, attempts
  to SELECT / UPDATE / DELETE an `upload_session` row owned by
  tenant B. Engine refuses every call. Same for
  `ingestion_rejection`.
- **Unit tests on the two validator functions.** Property tests on
  `validate_ingestion_metadata` (any input ⇒ no panic, decision
  matches policy). Table tests on `validate_uploaded_object` against
  crafted PDFs, malformed PDFs, text-claiming-PDF, oversized,
  polyglot PDF-and-ZIP, oversize-exceeds-pdf_max_input_bytes.
- **Integration tests in `backend/tests/`** (against a MinIO
  testcontainer, joining tier-2 setup):
  - `ingestion_upload_happy_path.rs` — create → sign N → complete →
    assert `document` row exists, no `upload_session`.
  - `ingestion_upload_cross_user_blocked.rs` — alice creates, bob
    (same tenant, same role) cannot sign-part / complete /
    GET-status.
  - `ingestion_upload_concurrent_complete.rs` — two `/complete`
    POSTs in flight; one returns 200, the other returns 409 with
    `state="validating"`.
  - `ingestion_upload_canonical_conflict.rs` — alice's upload
    completes; bob's upload of the same `canonical_id` finishes
    after; bob's `/complete` returns 422 with `existing_doc_id =
    alice's`.
  - `ingestion_upload_metadata_rejected.rs` — bad content-type /
    oversized declared / malformed metadata → 400 from
    `validate_ingestion_metadata`.
  - `ingestion_upload_object_rejected.rs` — declared PDF, actual
    text → 422, no row, S3 wiped, rejection logged.
  - `ingestion_upload_polyglot_rejected.rs` — PDF-ZIP polyglot →
    422, no row, S3 wiped.
  - `ingestion_cleanup_orphans.rs` — orphan multipart older than
    threshold gets aborted; orphan object older than threshold
    gets deleted; in-flight session within threshold is untouched;
    document-referenced object stays.
- **E2E in `tests/e2e/`**: full Uppy upload of a small PDF in
  tier-1, assert document appears.

## Implementation order

0. **Schema migration** — define `upload_session` and
   `ingestion_rejection` tables with full PERMISSIONS / DEFAULT /
   ASSERT clauses. Land the cross-tenant schema test
   (`upload_session_cross_tenant.rs`) first; it guards against
   future schema edits.
1. **`S3ObjectStore`** + multipart trait methods. Land it behind a
   feature flag, exercised by unit tests against a MinIO
   testcontainer.
2. **`validate_ingestion_metadata`** module + property tests.
3. **`validate_uploaded_object`** module + table tests.
4. **Storage trait additions** (`create_upload_session`,
   `cas_upload_session_state`, `commit_upload`, etc.) — typed
   surface, no raw SurrealQL leaks past this layer.
5. The four endpoints, in order:
   - **5a.** `POST /uploads` — wires to
     `validate_ingestion_metadata` + `create_upload_session`.
   - **5b.** `POST /uploads/:id/sign-part`.
   - **5c.** `POST /uploads/:id/complete` — including CAS state
     transition (§1.2), `commit_upload` saga with
     canonical_id-conflict handling, validator dispatch.
   - **5d.** `GET /uploads/:id`.
6. Document-delete extension to call `ObjectStore::delete`.
7. Nightly cleaner.
8. SPA Uppy integration.
9. E2E test.

## Things to verify before coding

- **Hetzner multipart**: confirm `CreateMultipartUpload` /
  `UploadPart` / `CompleteMultipartUpload` all work against their S3
  API. 10-minute `aws-cli --endpoint-url` probe.
- **`force_path_style` matrix**: Hetzner/B2/MinIO want path-style;
  R2 and AWS want virtual-hosted-style. The flag handles it;
  document the matrix in `.env.example`.
- **Uppy `getChunkSize`**: confirm it reads from `file.meta.part_size`
  cleanly; fallback is a global config the SPA fetches at boot.
- **SurrealDB CAS semantics on `UPDATE ... WHERE state = $from`**:
  confirm the empty result set (no rows updated) is reliably
  distinguishable from "row exists but state was something else."

## Audit items this targets

(To be ticked on AUDIT.md after merge, not pre-ticked here.)

- **M7** (non-transactional ingest) — `commit_upload` is one
  Surreal transaction; failure paths are enumerated above and each
  has a documented rollback / cleanup actor.
- **M8** (unbounded metadata) — `validate_ingestion_metadata`
  enforces depth + size + shape + content-type.
- **H3 / I1** (body-size limits) — per-route `DefaultBodyLimit`
  values are spelled out above; the actual file upload is bounded
  at S3, not at the backend (bytes never traverse the backend at
  all).

Items the plan does **not** target despite earlier draft claims:

- **M9** (frontend `source_uri` rendering) — orthogonal; lives in
  `DocumentCard.tsx`.
- **M11** (`ARXIV_QUERY` validator) — adapter-internal.
