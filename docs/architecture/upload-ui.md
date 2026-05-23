# Upload UI — manual + autofilled document ingestion

Status: planned. Companion to [`ingestion-v2.md`](./ingestion-v2.md), which
specifies the backend ingestion endpoints this plan consumes.

The user opens an **Upload** tab, drops one or many files (or picks
them), optionally fills a metadata form, and hits Upload. Each file is
streamed direct browser → S3 (MinIO) via Uppy multipart. The metadata
form is a **prefill**: whatever the user supplied is kept; everything
else the backend deduces server-side — eventually with LLM help. The
same interface serves precise single-file ingestion and bulk
"just-take-these" multi-file ingestion.

## Two modes, one interface

- **Single file → manual prefill available.** User may fill `title` /
  `summary` / `authors` / `language`. Those values win; autofill only
  fills the blanks they left.
- **Multiple files → form disabled, autofill only.** When more than
  one file is selected the metadata form is greyed out — a single
  shared form can't sensibly title N different documents. Backend
  extracts each file's metadata from its bytes (LLM agent, deferred).

Both go through the identical server pipeline; the only difference is
whether a prefill is present.

## Required vs optional fields

Two tiers of "required":

- **DB schema (hard).** `backend/schema.surql` enforces this. The
  record id (`document:<doc_id>`) is the identity — unique by
  construction. `source_type` and `content_hash` are `TYPE string`
  (non-option) ⇒ `CREATE` fails if absent; both are server-derived
  (`source_type="manual"`, `content_hash` from the validated object).
  `tenant_id` is guarded by `ASSERT $value != NONE`. **`canonical_id`
  moves to `option<string>`** (was required) — see §1.0 schema change.
- **App metadata policy (soft, configurable).** Which *descriptive*
  fields (`title`, …) must be present **after merge**. A
  `required_fields` knob on the metadata policy. Starts **empty**
  (nothing app-required) because autofill is a noop today — making
  `title` app-required now would fail every multi-file upload until
  the LLM extractor lands.

Optional (`option<>`) fields — `title`, `summary`, `language`,
`published_at` — are left **unset** (`NONE`) when neither prefill nor
autofill provides a value and the field isn't app-required.

## Ingestion workflow (per file)

The order is load-bearing and must read top-to-bottom in code (see
§1.1, the completion pipeline). Three validation points: prefill,
autofill-output, merged.

1. **Validate prefill metadata** — `POST /uploads`. Shape-only
   (content-type allowlist, size cap, metadata depth/size, and the
   `canonical_id` regex *if* one is supplied). Partial/empty allowed;
   `canonical_id` and descriptive fields not required here.
2. **Upload** — `sign-part` per part, browser PUTs direct to S3.
3. **Complete the multipart** — `POST /complete` opens the completion
   pipeline below (stages 4–9).
4. **Validate uploaded object** — `validate_uploaded_object` (size,
   magic-byte sniff, polyglot, PDF parse). Reject ⇒ wipe S3 + log.
5. **Extract text** — bounded in-backend extraction so the LLM can read
   the document. This is a **deliberate, capped exception** to the
   "bytes never traverse the backend" invariant (documented in
   `SECURITY.md`): the download is bounded by the validator's
   `pdf_max_input_bytes`, PDFs go through the sandboxed `pdftotext`
   discipline, text/markdown is a bounded read. See §1.1a.
6. **Autofill metadata** — feed the extracted text + prefill to the
   `MetadataExtractor` (LLM agent, deferred placeholder).
7. **Validate autofill output** — run the metadata validator over the
   extractor's result before trusting it into the merge (LLM output is
   untrusted). Invalid autofill is dropped, not fatal.
8. **Merge** — prefill over autofill; unset optional fields stay unset.
9. **Validate merged metadata** — final gate. App-required fields
   present, shape valid. Fail ⇒ reject (wipe S3 + log), no row written.
10. **Write document row** — `commit_upload`: `CREATE document:<doc_id>`
    + `document_content` insert + `upload_session` delete, one
    transaction.

## Decisions

- **Client driver:** Uppy core + `@uppy/aws-s3-multipart` +
  `@uppy/dashboard` (or `@uppy/drag-drop` + `@uppy/file-input`) +
  `@uppy/progress-bar`. Dashboard gives multi-file list UI for free.
- **Multi-file supported; form disabled for it.** Drop N files; each
  becomes its own upload session (own `doc_id`, own
  create→sign→complete cycle), driven concurrently by Uppy. The
  metadata form is only active for single-file selections.
- **Metadata form is an optional prefill, never required.** Empty form
  (or multi-file) ⇒ pure autofill.
- **Three validation points.** Prefill (at `/uploads`), autofill output
  (post-extract, pre-merge), and merged result (pre-commit). Same
  metadata validator, three call sites.
- **Text extraction is a first-class pipeline stage**, run after object
  validation and before autofill, so the LLM has text to read. Output
  persisted to `document_content`.
- **Server-side autofill, LLM-assisted, deferred.** A clean trait seam
  ships now with a no-op placeholder; the real LLM agent drops in
  later without touching callers.
- **S3 only, no LocalFs fallback.** `LocalFsObjectStore` deleted as
  step zero.
- **MinIO in both compose tiers.** Tier-1 exposes MinIO on `:9000` for
  the browser; tier-2 routes through Traefik.
- **`doc_id` is the identity and the index.** It equals the SurrealDB
  record id: `/complete` does `CREATE document:<doc_id>` so
  `document.id = "document:" + doc_id`. Record-id uniqueness *is* the
  primary key — no separate identity field.
- **`canonical_id` is optional.** Left unset for manual uploads. It
  exists only for natural-source dedup (a DOI, etc.); when present it
  gets a unique-when-set index, when absent the row is identified by
  `doc_id` alone. The client never sends it; autofill may populate it
  later if it recognises one.

## Non-goals

- Pause / resume across page reloads. Uppy resumes within a session;
  the task tracker is in-memory, so a full browser reload clears it
  (in-flight uploads are lost, committed rows are already in the feed).
- Cancel-mid-upload. Closing the tab is enough; the cleaner reaps.
- AV scanning (prod add-on, see [`SECURITY.md`](../SECURITY.md)).
- Per-file metadata forms. Single-file gets the prefill form;
  multi-file is pure autofill (form disabled).
- The real LLM extraction logic — this plan ships the seam + a
  placeholder only.

## Phase 0 — Remove `LocalFsObjectStore`

Pre-requisite. Single `ObjectStore` impl for the rest of the work.

### Delete

- `backend/src/object_store/local_fs.rs`
- `backend/tests/object_store_local_fs.rs`
- `backend/tests/object_store_url_dispatch.rs` (or trim to S3-only)

### Edit

- `backend/src/object_store/mod.rs` — drop `mod local_fs;` and the
  `pub use LocalFsObjectStore;`. Update the multipart doc-comment.
- `backend/src/object_store/url.rs` — drop the `file://` branch and the
  default-to-LocalFs fallback. Only `s3://` survives.
- `backend/src/object_store/s3.rs` — strip the "use `file://` during
  development" comment.
- `backend/src/object_store/multipart.rs` — drop `file://` from the
  `storage_uri` doc-comment.
- `backend/src/ingestion/uploads.rs` — remove the
  `bucket == "local"` branch in `complete_upload` (the `file:///{key}`
  storage_uri synthesis); the `bucket` field stops needing a `"local"`
  sentinel.
- `backend/src/api/mod.rs` — drop the
  `file:///var/lib/delphi/originals` default for `DELPHI_INGEST_OBJECT_STORE_URL`;
  make it required (boot fails with a clear error if unset).
- `backend/tests/common/mod.rs` — `TestApp::build_with_local_fs()`
  becomes `build_with_mem()` (or similar). See the test-backend note
  below.
- `backend/tests/ingestion_uploads.rs` — drop the
  `url.starts_with("local-multipart://")` assertion; assert on the
  `mem://` shim URL instead (see below).
- `docker-compose.yml` — drop the `file://` default + the
  `/var/lib/delphi/originals` volume mount.

**Test backend — move the in-process multipart shim, don't lose it.**
`LocalFsObjectStore` is today the *only* impl with a working in-process
multipart shim (`create_multipart_upload` / `presign_upload_part` /
`complete_multipart_upload` / `upload_part_direct` / listing,
`local_fs.rs:178-359`). `MemObjectStore` (`mem.rs`) implements only
put/get/delete/exists/get_range/head — the multipart trait methods fall
through to the `NotImplemented` defaults. The existing integration
suite (`ingestion_uploads.rs`, `upload_session_cross_tenant.rs`) drives
full create→sign→complete *in-process* through this shim, with no
Docker, matching `testing.md`'s no-testcontainers ethos. So Phase 0
**ports the multipart shim into `MemObjectStore`** (staging parts in the
in-memory map, emitting `mem-multipart://` URLs the tests assert on)
rather than deleting the capability. Only the `s3://` *production*
backend changes; the in-process test backend survives the rename. The
new `backend/tests/object_store_s3.rs` (Phase 1.7) is the only
MinIO-testcontainer test, gated by `MINIO_TEST_ENDPOINT` so CI without
Docker skips it.

### Docs

- `ingestion-v2.md` / `ingestion-v2-review.md` — strike LocalFs.
- `AUDIT.md` — H6 (tmp filename collision) → N/A (code removed).

### Verify

- `cargo check --all-targets` (both feature configs).
- `git grep -i "localfs\|local_fs\|file://"` clean in `backend/src` +
  `docs/`.

## Phase 1 — Backend: autofill seam + MinIO + `S3ObjectStore`

User-requested order: **autofill support first**, then frontend, then
the real agent. So Phase 1 lands the metadata-autofill seam (with a
placeholder) alongside the S3 plumbing the upload needs.

### 1.0 Make `canonical_id` optional — schema, types, and conflict check

This is bigger than a schema tweak: the field is `String` (required) in
the wire request, the storage models, *and* an app-level conflict
pre-check keys on it. All four layers must change together or the
second manual upload breaks. Concrete edits:

**(a) Schema** (`backend/schema.surql`). A field type change is **not**
idempotent under `DEFINE FIELD IF NOT EXISTS` (that's a no-op when the
field exists). Use `DEFINE FIELD OVERWRITE`:

- `document.canonical_id`: `TYPE string` → `OVERWRITE … TYPE option<string>`.
- `upload_session.canonical_id` (`schema.surql:300`): same.
- **Unique indexes — keep dedup as unique-when-set (decided).** Both
  `document_tenant_canonical` (`:157`) and `upload_session_canonical`
  (`:310`) are `UNIQUE (tenant_id, canonical_id)`. The dedup guarantee
  is preserved (the v1 ingest path relies on it): `canonical_id` stays
  unique per tenant **when present**, and `NONE` rows are exempt. With
  most rows now `NONE`, the mechanism depends on SurrealDB's
  NONE-in-unique behaviour (verify):
  - If SurrealDB excludes `NONE` from UNIQUE enforcement → keep both
    indexes as-is; they are already "unique-when-set."
  - If not → `REMOVE INDEX IF EXISTS` then redefine as a **filtered /
    partial unique index** enforcing uniqueness only where
    `canonical_id != NONE` (do **not** drop to plain non-unique — that
    would lose dedup). `REMOVE`+redefine is required; `IF NOT EXISTS`
    won't replace an existing index.
- `upload_session_doc_id` UNIQUE (`:309`) stays — `doc_id` is identity.

**(b) Storage types** — change to `Option<String>` with
`#[serde(skip_serializing_if = "Option::is_none")]` so `None`
serialises as absent, not `""`:

- `Document.canonical_id` (`storage/models.rs:77`).
- `DocumentWire.canonical_id` (`storage/surreal.rs:59`).
- `CreateUploadSessionParams.canonical_id` + `UploadSession.canonical_id`
  (`storage/models.rs:275,301`).
- `create_upload_session` bind (`surreal.rs:785`) — bind `NONE` not `""`.

**(c) Conflict pre-check — skip when `None`.** `commit_upload`
(`surreal.rs:837-847`) and `upsert_document` (`surreal.rs:224-246`) run
`SELECT id FROM document WHERE canonical_id = $cid`. With `canonical_id`
absent this must be **skipped entirely** (no conflict possible — manual
uploads aren't deduped; identity is the record id). Guard the pre-check
on `canonical_id.is_some()`. **This is the real landmine:** without it,
every manual upload after the first false-matches the prior `NONE` row
and 422s `canonical_id_conflict`.

**(d) Wire request + validator.** `CreateUploadRequest.canonical_id`
(`validation/metadata.rs:24`) → `Option<String>`; `source_uri` →
`Option<String>`; `source_type` server-defaulted to `"manual"` when
absent. `validate_ingestion_metadata` (`metadata.rs:127-151`): only run
the `canonical_id_pattern` / `is_plausible_uri` checks **when the field
is present**. Update the existing validator tests (`metadata.rs:233-417`),
the `ok_req()` fixture, and `create_body` (`tests/ingestion_uploads.rs:26-31`).

This is a *relaxation* for writers that still set `canonical_id` (v1
JSON ingest, future adapters) — they keep working. `get_document_by_canonical`
already returns `Option`, so readers are fine.

### 1.1 The completion pipeline (workflow as code)

The whole point of this section: the post-upload sequence must be
**legible top-to-bottom**. The `/complete` handler stays thin (CAS the
state, call the pipeline, map the result to HTTP). The ordered stages
live in one function whose body *is* the workflow.

New module `backend/src/ingestion/pipeline.rs`:

```rust
/// Ordered post-upload ingestion stages. The order is load-bearing;
/// read it top-to-bottom. Each stage is a named call, not inlined
/// logic, so the sequence stays the documentation.
pub async fn run_completion(
    ctx: &CompletionCtx<'_>,
) -> Result<DocId, CompletionError> {
    // 4. Bytes are committed + the object is sound.
    let object = validate_uploaded_object(ctx.object_args()).await
        .map_err(CompletionError::ObjectRejected)?;

    // 5. Extract raw text (LLM needs something to read; also persisted).
    let text = extract_text(ctx.object_store, &ctx.key, &ctx.content_type)
        .await
        .unwrap_or_default();   // extraction failure ⇒ empty text, non-fatal

    // 6. Autofill from text + prefill (deferred LLM; noop today).
    let autofilled = ctx.extractor
        .extract(&ExtractionContext { text: &text, prefill: ctx.prefill })
        .await
        .unwrap_or_default();   // autofill failure ⇒ prefill-only, non-fatal

    // 7. Validate the (untrusted) autofill output before merge.
    let autofilled = validate_descriptive_metadata(&autofilled, ctx.policy)
        .map(|()| autofilled)
        .unwrap_or_default();   // invalid autofill dropped, non-fatal

    // 8. Merge: prefill wins; unset optional fields stay unset.
    let merged = merge_metadata(ctx.prefill, &autofilled);

    // 9. Final gate: app-required fields present + shape valid.
    validate_descriptive_metadata(&merged, ctx.policy)
        .map_err(CompletionError::MetadataRejected)?;

    // 10. Commit: document:<doc_id> (+ document_content) + session delete.
    commit(ctx, object, text, merged).await
}
```

Stages 4, 9 are the fatal-reject points (wipe S3 + log rejection);
stages 5, 6, 7 degrade gracefully because the bytes are already
committed and the user can edit metadata later.

**`CompletionCtx` must carry the DB handles + auth.** The current reject
path deletes the session and writes the rejection row via **`SystemDb`**
(the helper `handle_object_reject` has no `AuthedDb` and the
`ingestion_rejection` table denies user writes, `surreal.rs:878-888`),
while the CAS/commit go through **`AuthedDb`**. So `CompletionCtx` needs
*both* DB handles plus `auth: &AuthContext`, not just `object_store` /
`key` / `content_type` / `extractor` / `prefill` / `policy`. The stage-9
merged-reject path must reuse the existing `handle_object_reject` flow
exactly, not open-code a new one.

### 1.1a Stage 5 (text extraction) + stage 10 (commit with content)

**Decision: synchronous, bounded, in-backend.** `/complete` extracts
text before autofill and commits `document_content` in the same
transaction. This is a documented exception to "bytes never traverse
the backend" — see the §SECURITY note below.

`extract_text` is **not** a thin wrapper over the existing
`text_extractor` (that module is PDF-only and returns `Vec<Word>` with
bounding boxes, `text_extractor/mod.rs`). Specify the adapter:

- **Download is bounded** by `ObjectPolicy.pdf_max_input_bytes` (reuse
  the existing knob, `object.rs`); abort-on-exceed, never an unbounded
  read.
- **PDF** → run the existing sandboxed `pdftotext` path (timeout +
  `kill_on_drop` + capped stdout, same H4 discipline the validator
  reserves at `object.rs:159-184`), then join `Vec<Word>` → flat text.
- **text/markdown** → bounded read of the capped bytes, UTF-8 validate.
- Output a `Content { text, format, extractor }` for the
  `document_content` insert (`upsert_content` already exists,
  `surreal.rs:303`).

**Commit (stage 10) extends the transaction.** `commit_upload` today is
`CREATE document` + `DELETE upload_session` (`surreal.rs:832-867`). Add
the `document_content` insert (`doc = document:<doc_id>`, `text`,
`format`, `extractor` — all required by schema, `schema.surql:181-185`)
inside the same `BEGIN/COMMIT`. `document_content_doc` is UNIQUE
(`schema.surql:187`), so a retried commit must upsert, not double-insert.
Change the signature to `commit_upload(doc_id, &Document, &Content)` (or
a new `commit_upload_with_content`).

**SECURITY.md exception note.** Add a short paragraph: the ingestion
validator and this extraction step are the *only* places the backend
reads object bytes; both are bounded (`pdf_max_input_bytes`, sandboxed
parser, capped stdout). The "bytes never traverse the backend" rule
still holds for the *upload* path (direct browser→S3); the read-back at
`/complete` is a deliberate, capped server-side operation, not a proxy
of the upload stream.

### 1.2 Metadata autofill seam (placeholder)

In `backend/src/ingestion/autofill.rs`:

```rust
/// Structured metadata an extractor can produce. Maps onto top-level
/// `Document` fields plus the free-form `metadata` blob. `None` /
/// empty means "the extractor had nothing" — never overwrites a set
/// prefill value.
#[derive(Debug, Default, Clone)]
pub struct ExtractedMetadata {
    pub title: Option<String>,
    pub authors: Vec<String>,
    pub summary: Option<String>,
    pub language: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    pub extra: serde_json::Value,   // merged into document.metadata
}

pub struct ExtractionContext<'a> {
    pub text: &'a str,             // raw text from stage 5
    pub prefill: &'a DocumentPrefill,
}

#[async_trait]
pub trait MetadataExtractor: Send + Sync {
    async fn extract(&self, ctx: &ExtractionContext<'_>) -> Result<ExtractedMetadata>;
}

/// Placeholder shipped now. Returns empty — only the user prefill is
/// used until the LLM extractor lands (Phase 3).
pub struct NoopExtractor;

#[async_trait]
impl MetadataExtractor for NoopExtractor {
    async fn extract(&self, _ctx: &ExtractionContext<'_>) -> Result<ExtractedMetadata> {
        Ok(ExtractedMetadata::default())
    }
}
```

Wired into `AppState` as `Arc<dyn MetadataExtractor>`, defaulting to
`NoopExtractor`. The Phase-3 `LlmExtractor` swaps in here.

**Merge policy** (`merge_metadata`, unit-tested): for each field,
prefill value if set, else autofill value if set, else leave unset.
Optional fields with neither source stay `NONE`. `authors` merges as
"prefill if non-empty, else autofill". `extra` is a shallow object
merge with prefill keys winning.

### 1.3 Metadata validation (two validators, three call sites)

These are **two distinct functions**, not one — the wire request and a
descriptive-metadata struct are different types:

- **Wire request**, at `POST /uploads`: the existing
  `validate_ingestion_metadata(req: &CreateUploadRequest, policy)`
  (`validation/metadata.rs:94`). Checks content-type, size, forbidden
  fields, and `canonical_id`/`source_uri` shape *when present* (per
  §1.0(d)). Unchanged in shape beyond the optionality edits.
- **Descriptive metadata**, at pipeline stages 7 (autofill output) and
  9 (merged): a **new** `validate_descriptive_metadata(meta:
  &DescriptiveMetadata, policy) -> Result<(), MetadataReject>`. Feeding
  `ExtractedMetadata` to `validate_ingestion_metadata` is a type and
  semantic error (an autofill result has no `content_type`/`size`).
  The new fn checks `title` length (reuse `max_title_chars`),
  `published_at` sanity, `extra` depth/size (reuse `json_depth` +
  `max_metadata_bytes`), and `required_fields`. It shares *helpers*
  with the wire validator, not its entry point.

`MetadataPolicy` gains `required_fields: HashSet<MetadataField>` (new
`MetadataField` enum: `Title | Authors | Summary | Language`), wired in
`UploadsConfig::from_env` (`uploads.rs:55-115`) + `test_default`,
defaulting **empty** (nothing app-required until the LLM extractor
lands — otherwise every multi-file upload fails). The hard DB-required
fields (`source_type`, `content_hash`, the record id) are server-derived
and enforced by the engine at `CREATE`, not by this validator.

### 1.4 `commit_upload`: doc_id is the record id, canonical_id optional

- **`CREATE document:<doc_id>` is a real SQL change.** `commit_upload`
  today does `CREATE ONLY document CONTENT $data RETURN id`
  (`surreal.rs:851-856`) — a *random* record id. Change it to
  `CREATE ONLY type::thing("document", $doc_id) CONTENT $data` so the
  record id is deterministic. Verify the `doc_id` (a lowercased ULID,
  `uploads.rs:257`) is a legal SurrealDB record-id key (ULIDs are
  `[0-9a-z]`, safe; no quoting needed). Unit test asserts
  `document.id == "document:" + doc_id`. This deterministic id is what
  lets `GET /uploads/:id` return `ready` (see §2 polling / B5 fix).
- `CreateUploadRequest.canonical_id` is `Option<String>` (§1.0(d)); the
  client never sends it for manual uploads. When `None` it's left unset
  on both rows — **not** defaulted to `doc_id`. Identity is the record
  id; `canonical_id` is dedup-only metadata for natural-source docs.
- **Manual uploads are not deduped.** `content_hash` is set from the S3
  ETag (`uploads.rs:521`), which for multipart is `md5-of-md5s-N`
  (depends on part count) — **not** a stable content hash, and its
  index is non-unique. Treat `content_hash` as opaque/informational,
  not a dedup key. If real content-dedup is ever wanted it needs a
  SHA-256 over the full bytes (which requires the in-backend download —
  see the text-extraction open question).

### 1.5 MinIO in both compose files

`minio` + `minio-init` services (bucket create + CORS):

```yaml
minio:
  image: minio/minio:RELEASE.2025-…
  command: server /data --console-address ":9001"
  environment:
    MINIO_ROOT_USER: ${MINIO_ROOT_USER:-delphi}
    MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:-delphi-minio-dev-secret-please-change}
  volumes: [ minio-data:/data ]
  ports: [ "9000:9000", "9001:9001" ]
  healthcheck: …

minio-init:
  image: minio/mc:latest
  depends_on: { minio: { condition: service_healthy } }
  entrypoint: >
    sh -c "mc alias set local http://minio:9000 $$MINIO_ROOT_USER $$MINIO_ROOT_PASSWORD &&
           mc mb --ignore-existing local/delphi &&
           mc anonymous set none local/delphi &&
           mc cors set local/delphi /cfg/cors.json"
```

**CORS is security-relevant and a common silent-breakage point.** The
browser PUTs parts cross-origin (tier-1) and Uppy reads each part's
`ETag` from the PUT response to pass to `completeMultipartUpload` — if
`ExposeHeaders` omits `ETag`, complete gets empty etags and fails.
`/cfg/cors.json` (mounted into `minio-init`):

```json
{ "CORSRules": [ {
  "AllowedOrigins": ["http://localhost:5173", "http://localhost"],
  "AllowedMethods": ["PUT", "GET", "HEAD"],
  "AllowedHeaders": ["*"],
  "ExposeHeaders": ["ETag"],
  "MaxAgeSeconds": 3600
} ] }
```

(Confirm `mc cors set` exists on the pinned `mc` version; some versions
use `mc admin config set … cors`. Pin and verify.)

- **Tier-1:** browser PUTs to `http://localhost:9000/delphi/...`; CORS
  allows the Vite origin (`:5173`).
- **Tier-2:** MinIO behind Traefik at `http://localhost/s3/*`; same
  origin as the SPA (CORS still needed if any cross-origin remains).

### 1.6 Internal vs public endpoint + presign correctness

Backend↔S3 uses the internal host; presigned URLs must carry the
browser-facing host.

| Var | Tier-1 | Tier-2 |
|---|---|---|
| `DELPHI_INGEST_S3_ENDPOINT_INTERNAL` | `http://minio:9000` | `http://minio:9000` |
| `DELPHI_INGEST_S3_ENDPOINT_PUBLIC` | `http://localhost:9000` | `http://localhost/s3` |

**SigV4 + path-style + the `/s3` prefix (decided: forward unstripped).**
With `force_path_style=true` the signed URL is
`<public>/<bucket>/<key>?X-Amz-…`, and SigV4 signs the **host header and
the full path including the `/s3` prefix**. Tier-2 Traefik routes
`localhost/s3/*` → `minio:9000` **without `StripPrefix`**, and the
backend presigns against `http://localhost/s3`, so the signed path
includes `/s3` and matches exactly what MinIO receives. (The alternative
— `StripPrefix` + presign without `/s3` — was rejected to keep one fewer
moving part in the signature.) A prefix mismatch → `SignatureDoesNotMatch`
on every browser PUT. Tier-1 sidesteps this entirely (direct `:9000`,
no prefix).

### 1.7 `S3ObjectStore` implementation

Replace the stub. Add `aws-sdk-s3` + `aws-config` (rustls, rt-tokio,
behavior-version-latest). The dual endpoint is the subtle part:

- **`S3Config` splits its single `endpoint: Option<String>`
  (`s3.rs:43`) into `endpoint_internal` + `endpoint_public`.**
  `S3ObjectStore::from_env` reads both (plus region/keys/path-style) and
  builds **two** `aws_sdk_s3::Client`s — one per endpoint — or one
  client plus a presign-only config pointed at the public endpoint.
- `presign_upload_part` signs with the **public** client; `head` / `get`
  / `get_range` / `complete` / `abort` / listing use the **internal**
  one.
- **`from_url` rewiring** (`object_store/url.rs:18`): the `s3://` branch
  (today `s3::not_yet_supported`, `url.rs:24`) calls
  `S3ObjectStore::from_env()`. `DELPHI_INGEST_OBJECT_STORE_URL` supplies the bucket;
  the endpoints/creds come from `INGEST_S3_*`, not the URL.

Implement the 11 trait methods; the four multipart ones
(`create_multipart_upload`, `presign_upload_part`,
`complete_multipart_upload`, `abort_multipart_upload`) are the only
non-trivial ones. Integration test `backend/tests/object_store_s3.rs`
against a MinIO testcontainer (gated by `MINIO_TEST_ENDPOINT`).

### 1.8 Env vars

| Var | Tier-1 | Tier-2 | Notes |
|---|---|---|---|
| `DELPHI_INGEST_OBJECT_STORE_URL` | `s3://delphi/` | `s3://delphi/` | Required. |
| `DELPHI_INGEST_S3_ENDPOINT_INTERNAL` | `http://minio:9000` | `http://minio:9000` | Backend → S3. |
| `DELPHI_INGEST_S3_ENDPOINT_PUBLIC` | `http://localhost:9000` | `http://localhost/s3` | In presigned URLs. |
| `DELPHI_INGEST_S3_REGION` | `us-east-1` | `us-east-1` | MinIO ignores. |
| `DELPHI_INGEST_S3_BUCKET` | `delphi` | `delphi` | One bucket. |
| `DELPHI_INGEST_S3_ACCESS_KEY_ID` / `_SECRET_ACCESS_KEY` | `${MINIO_ROOT_*}` | same | Pass-through. |
| `DELPHI_INGEST_S3_FORCE_PATH_STYLE` | `true` | `true` | Required for MinIO. |

## Phase 2 — Frontend: upload tab + global task tracker

Core principle: **the submit button frees the UI immediately.** Pressing
Upload hands the files to a global manager and returns; the route is
free to reset, navigate away, or close. Ingestion continues in the
background, tracked in a persistent widget visible on every tab. The
frontend tracks each task until its DB row is written (`ready`) or it
fails (`rejected` / error). Nothing about the in-flight upload lives in
the `/upload` route's component tree.

### 2.1 Architecture: tracking lives above the router

```
__root.tsx
 ├─ <UploadProvider>            ← owns the UploadManager (singleton)
 │   ├─ <aside> nav … <Link to="/upload">
 │   ├─ <Outlet/>               ← routes mount/unmount freely
 │   └─ <UploadTracker/>        ← always-mounted widget, reads manager
```

- **`UploadManager`** (`frontend/src/lib/uploadManager.ts`) — a plain
  TS controller, *not* a React component. Owns the Uppy instance(s),
  drives create→sign→complete→poll per file, holds the reactive task
  list, and survives route changes because it's instantiated once in
  the provider (above `<Outlet/>`). No new state library — it's a tiny
  observable exposing `subscribe()` / `getSnapshot()` consumed via
  React's `useSyncExternalStore`.
- **`UploadProvider`** mounts the manager once and puts it on context.
- **`UploadTracker`** renders the task list from the manager. Mounted
  in the root layout, so it's present regardless of the active route.

This is the whole reason the UI frees up: Uppy and the polling loop are
owned by the manager, not by `UploadDialog`. Unmounting the route does
not cancel anything.

### 2.2 Task model

```ts
type UploadTask = {
  id: string;            // client ULID until doc_id is known, then doc_id
  filename: string;
  size: number;
  state:
    | "queued"           // accepted, not started
    | "creating"         // POST /uploads in flight
    | "uploading"        // parts in flight; `progress` 0..1
    | "validating"       // Uppy done; polling /uploads/:id
    | "ready"            // DB row written; auto-dismiss after ~5 s
    | "failed";          // any error; reason set; manual/timed dismiss
  progress: number;      // 0..1 during uploading (Uppy bytesUploaded/total)
  docId?: string;
  reason?: string;       // friendly string when failed
};
```

The manager exposes `enqueue(files, prefill)`, `dismiss(taskId)`, and a
`tasks` snapshot. Per-task lifecycle:

`queued → creating → uploading(progress) → validating → ready|failed`

Errors at *any* stage (create 4xx/5xx, part PUT failure, complete 5xx,
poll → `rejected`, poll timeout) move the task to `failed` with a
mapped `reason`. Other tasks are unaffected — each is independent.

### 2.3 Manager internals (Uppy + polling)

One Uppy instance per manager, `autoProceed: false`. `enqueue(files,
prefill)` adds files and starts them. **Prefill is stashed per-file on
`uppy.setFileMeta` at enqueue time**, not closed over — otherwise a
second `enqueue` with a different prefill would race the
`createMultipartUpload` callbacks of files still being created. Each
callback reads `file.meta.prefill`:

```ts
.use(AwsS3Multipart, {
  createMultipartUpload: async (file) => {
    setState(file.id, "creating");
    const res = await api.createUpload({ ...file.meta.prefill, filename: file.name,
      content_type: file.type, size: file.size });   // source_type defaulted server-side
    uppy.setFileMeta(file.id, { doc_id: res.doc_id, part_size: res.part_size });
    promoteId(file.id, res.doc_id);            // task.id → doc_id
    return { uploadId: res.upload_id, key: res.key };
  },
  signPart: async (f, { partNumber }) =>
    ({ url: (await api.signUploadPart(f.meta.doc_id, partNumber)).url }),
  completeMultipartUpload: async (f, { parts }) => {
    const res = await api.completeUpload(f.meta.doc_id, parts);
    onComplete(f.meta.doc_id, res);            // see "terminal signal" below
    return {};
  },
  getChunkSize: (f) => f.meta.part_size,
})
```

- `upload-progress` event → update `task.progress`, state `uploading`.
- `upload-error` event → state `failed`, mapped reason.

**Terminal signal — the `/complete` response is authoritative (no
happy-path polling).** Because `/complete` is synchronous (validate →
extract → autofill → commit, then respond, per §1.1a), it returns
`{ doc_id, state: "ready" }` on success or a 422 reject in the **same
response**. The `completeMultipartUpload` callback awaits it and the
task goes straight to `ready` / `failed`. No polling loop on the happy
path. The task's `validating` state simply represents "the `/complete`
request is in flight" — which under option A can take seconds while
the LLM runs.

**Recovery poll (the one place `uploadStatus` is used).** Because
`/complete` is now a long request, a dropped connection or client-side
timeout can leave the manager without the response even though the
server committed. So: if the `/complete` await errors or times out, the
manager falls back to polling `GET /uploads/:doc_id` (single timer,
1 s, ≤60 s) to recover the real outcome. This makes the **B5 backend
fix required**: `GET /uploads/:doc_id` currently *cannot* return `ready`
(on commit the session row is deleted and the `Ready` arm is dead code,
`uploads.rs:190,643-665`) → a recovery poll would 404. Add a branch to
`get_upload_status`: after the session/rejection checks, look up
`document:<doc_id>` by record id under AuthedDb (deterministic per
§1.4) and return `StatusResponse::Ready { doc_id }`. The poll runs in
the manager so it survives navigation.

`lib/uploadRejectReason.ts` maps backend reason codes to friendly
strings.

### 2.4 The `/upload` route (thin enqueue surface)

`frontend/src/routes/upload.tsx` + `components/upload/UploadDialog.tsx`:

1. **Select / drop.** Drag-and-drop zone + "Choose files" button, one
   *or many* files. Selected files listed locally (pre-submit only).
2. **Metadata form** — single-file selection only; **disabled when >1
   file is selected** (greyed with a hint: "metadata is auto-filled for
   batch uploads"). Fields `title` / `summary` / `authors` /
   `language`, all optional. `@tanstack/react-form` + `zod`.
3. **Upload button** → `uploadManager.enqueue(files, prefill)` then
   immediately clears the local selection/form. The route shows a
   one-line "Added N files — track progress in the tracker" and is
   fully usable again. No upload state is held here.

**Wiring prerequisites (call out so the implementer doesn't miss them):**
- Add the nav `<Link to="/upload">Upload</Link>` to `__root.tsx`
  (`routes/__root.tsx:50-60`) and wrap the layout in `<UploadProvider>`
  / mount `<UploadTracker/>` there.
- New deps in `frontend/package.json`: `@uppy/core`,
  `@uppy/aws-s3-multipart`, `@uppy/dashboard` (or `@uppy/drag-drop` +
  `@uppy/file-input`), `@uppy/progress-bar`. `ulid` is already present
  (used for the client-side task id). Record the bundle-size delta.

### 2.5 Task tracker widget

`frontend/src/components/upload/UploadTracker.tsx`. A
notification-style stack pinned bottom-right (built from existing
`card` + `progress` + `badge` + `collapsible` + `scroll-area`
primitives — no new dep):

- One row per task: filename, state badge, progress bar while
  `uploading`, spinner while `validating`, check on `ready`, error +
  reason on `failed`.
- **Dismiss:** `ready` rows auto-dismiss after ~5 s; `failed` rows
  persist with an × to remove manually, and also auto-expire after a
  longer TTL (~30 s) so the tracker self-cleans.
- Collapses to a compact "N uploading / M done" pill when there are
  many tasks or the user collapses it. Hidden entirely when the task
  list is empty.
- A `ready` row links to the document in the feed (`docId`).

### 2.6 API client

`frontend/src/lib/api.ts`:

```ts
export type CreateUploadRequest = {
  title?: string;
  summary?: string;
  authors?: string[];
  language?: string;
  filename: string;
  // source_type omitted — server defaults to "manual"; canonical_id/source_uri not sent
  content_type: string;
  size: number;
};
export type CreateUploadResponse = {
  doc_id: string; key: string; upload_id: string; part_size: number;
};
export type UploadStatus =
  | { state: "uploading" | "validating" }
  | { state: "ready"; doc_id: string }
  | { state: "rejected"; reason: string };

export const api = {
  // …existing
  createUpload: (req: CreateUploadRequest) =>
    post<CreateUploadResponse>("/api/ingestion/uploads", req),
  signUploadPart: (docId: string, partNumber: number) =>
    post<{ url: string }>(`/api/ingestion/uploads/${docId}/sign-part`, { part_number: partNumber }),
  completeUpload: (docId: string, parts: PartRef[]) =>
    post(`/api/ingestion/uploads/${docId}/complete`, { parts }),
  uploadStatus: (docId: string) =>
    get<UploadStatus>(`/api/ingestion/uploads/${docId}`),
};
```

### 2.7 Frontend tests

Vitest + MSW, co-located. Per `testing.md`, Vitest runs under Node
(`make frontend-test`), and the timer-driven assertions
(auto-dismiss, poll interval) must use fake timers:

- `uploadManager.test.ts` — the controller in isolation (no Uppy DOM),
  fake timers: enqueue advances
  `queued→creating→uploading→validating→ready`; poll → `rejected` lands
  `failed` with mapped reason; create-error and timeout land `failed`;
  `dismiss` removes a task; auto-dismiss timer fires for `ready`. The
  single highest-value test — it's where the state machine lives.
- `UploadDialog.test.tsx` — single vs multi selection, form disabled
  when >1 file, submit calls `enqueue` then clears the form.
- `UploadTracker.test.tsx` — renders rows per task state, dismiss
  button removes a `failed` row, empty list hides the widget.

## Phase 3 — The LLM metadata extractor

Replaces `NoopExtractor` with a real `LlmExtractor` behind the same
`MetadataExtractor` trait. No caller changes — the text reaches it via
`ExtractionContext.text`, produced by whatever text-extraction stage
the B3 decision settles on.

- Build a prompt from `ctx.text` (first N chars) + the user prefill
  (so the agent knows what's set and what to fill).
- Call the `llm` module (rig-based) with a JSON-schema-constrained
  response so the agent returns `ExtractedMetadata` shape directly.
- Bound it: timeout, max input chars, graceful failure (returns
  `Default` ⇒ prefill-only).
- Wire `LlmExtractor` into `AppState` when an LLM provider is
  configured; keep `NoopExtractor` as the fallback when it isn't.

Tests: a fake LLM client (already exists in
`backend/tests/common/fake_llm.rs`) scripted to return canned metadata
JSON; assert it lands on the `document` row and that prefill overrides
agent output field-by-field.

## E2E (after Phase 2)

`tests/e2e/upload-flow.spec.ts`, tagged **`@tier2`** (real S3/MinIO +
full BFF; the upload path needs a real S3 endpoint, so this is not a
tier-1 test): log in, open `/upload`, `setInputFiles` with two small
fixture files, submit with an empty form, poll `/feed` until both
appear. A second case fills the form and asserts the prefilled title
wins over autofill (with the fake LLM scripted in the backend under
test).

**Role prerequisite:** all four ingestion endpoints require the
`ingester` role (`uploads.rs:205-211`). Confirm both the tier-2 IdP
(the Keycloak realm export must grant the e2e user `ingester` or a
composite `owner`) **and** the tier-1 `dev-auth` injector emit
`ingester` in `X-Auth-Roles` — otherwise every upload 403s. If the dev
injector doesn't currently include it, add it.

## Implementation order

1. **Phase 0** — delete `LocalFsObjectStore`.
2. **Phase 1.0** — schema: `canonical_id` → `option<string>` + rework
   the unique indexes. Standalone migration; lands first so the column
   shape is settled before handler work.
3. **Phase 1.1–1.4** — completion pipeline (`run_completion`) + autofill
   seam (`NoopExtractor`) + three-point metadata validation + `doc_id`
   record-id unification. Backend-only; ships behind the existing
   endpoints.
4. **Phase 1.5–1.8** — `S3ObjectStore` + MinIO in both tiers.
5. **Phase 2** — frontend Upload tab (single + multi).
6. **E2E** — Playwright spec.
7. **Phase 3** — real `LlmExtractor`.
8. **Docs sweep** — flip `ingestion-v2.md` S3 status, list the route
   in `ARCH.md`, populate `.env.example`, and add the bounded
   read-back exception note to `SECURITY.md` (§1.1a).

## Open questions

- **Text-extraction placement — RESOLVED: option A** (synchronous,
  bounded, in-backend at `/complete`). See §1.1a. A documented capped
  exception to the "bytes never traverse the backend" invariant; no
  happy-path polling. Kept here only as a pointer.
- **canonical_id dedup — RESOLVED: keep unique-when-set** (§1.0). The
  only build-time *verification* left is SurrealDB's NONE-in-unique
  behaviour, which selects the mechanism (keep-as-is vs filtered/partial
  unique index) — not whether dedup exists.
- **Traefik tier-2 routing — RESOLVED: forward `/s3` unstripped**, presign
  against `http://localhost/s3` (§1.6).
- **Autofill latency.** Under option A above, LLM extraction (Phase 3)
  runs inside `/complete`, lengthening that request. The background
  tracker makes the wait non-blocking for the user, so it's tolerable;
  option B removes it from the request path entirely. Either way the
  60 s poll/await timeout caps the visible `validating` window.
- **MinIO CORS config syntax.** `mc cors set` vs
  `mc admin config set` differs across versions; pin and verify.
