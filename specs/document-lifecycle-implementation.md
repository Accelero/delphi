# Implementation Spec: Event-Sourced Document Upload

**Audience:** an engineer or agent implementing this with no prior context on the
project. Read this document end to end before writing code.

> ## ⚠️ Superseded — historical design record
>
> **This spec is no longer the authority for what to build.** It was written
> before implementation, and the code has since deliberately diverged from it in
> ways this file does not reflect. Do not implement from it.
>
> The authority for both *what* and *why* is now
> `doc/content/docs/architecture/document-upload.md`, which is kept in step with
> the code.
>
> Known divergences, all intentional:
>
> | this spec says | the code does |
> | --- | --- |
> | preflight presigns a batch of parts | preflight presigns nothing; clients sign per part via `/renew`, and `count` is unbounded |
> | `upload_attempt` rows in Postgres, `AttemptStore` | one NATS KV record is the whole upload; Postgres holds no upload state |
> | an unreferenced-blob GC with a grace period | blobs are kept; no sweeper of any kind exists |
> | `GET /api/documents/{id}` reports `uploads_in_progress` | removed — a user-scoped keyspace cannot answer a cross-user question |
> | listing pages on a bare `updated_at` cursor | opaque `(updated_at, document_id)` keyset cursor |
> | services each ensure the NATS topology | `api-service` is the single author; everything else binds |
>
> It is kept for the reasoning behind decisions that did survive. Read it as
> history, not instruction.

**Reference architecture doc:** `doc/content/docs/architecture/document-upload.md`

---

## 0. Mandate — Read This First

**This is a greenfield rewrite of the document path. You are not constrained by
the existing implementation.**

- **Delete freely.** The current upload code (`services/api-service` ingestion
  handlers, the body of `services/document-worker`, the upload saga code in
  `crates/nats`, the upload types in `crates/contracts`) is superseded. Gut and
  rewrite in place rather than adapting. `services/document-worker` keeps its
  path and gains new responsibilities (§2).
- **Restructure freely.** New crates, new module boundaries, moved code, renamed
  types — all expected.
- **Breaking other features is acceptable.** If changing `crates/contracts`,
  `crates/nats`, or `crates/storage` breaks chat, the chat worker, or the
  realtime service, that is **fine**. They will be rewritten to the same
  architecture afterwards. Do not contort the document design to preserve them.
- **No backward compatibility.** No data migration, no API deprecation window, no
  dual-write period. Dev databases and buckets are reset.
- **Do not build temporary solutions.** Per `AGENTS.md`, implement the production
  end state. Where this spec says "stub", it means *a complete implementation of
  a well-defined port whose adapter is deliberately permissive for now* — not a
  shortcut. The seam must be real; only the behaviour behind it is deferred.

If this spec conflicts with existing code, the spec wins and the code goes.

---

## 1. Mission and Scope

Build one **complete vertical slice**: a file uploaded from the browser becomes a
document row in Postgres, by way of an append-only event log, with every
architectural seam in place for the stages that come later.

```text
browser form → presigned multipart → S3 → /complete → work queue
  → worker: assemble, validate (stubbed), scan (stubbed)
  → append DocumentCreated to the event log
  → projection loop folds it into Postgres
  → GET /api/documents/{id} returns it
```

Only **S3 and Postgres** are involved. No Qdrant, no graph store, no LLM.

### Build now

| Area | Deliverable |
| --- | --- |
| Migrations | ordered runner + document projection schema |
| Domain | event types, fold, validation, part geometry — pure, no IO |
| Event store | JetStream `Limits` stream with per-document CAS |
| Work queue | JetStream `WorkQueue` stream for upload commands |
| Upload context | one write-once NATS KV record |
| Upload attempt | Postgres row tracking an attempt through to its outcome |
| API | `POST /uploads`, `/renew`, `/complete`, `GET /uploads/{id}`, `GET /documents`, `GET /documents/{id}` |
| Worker | multipart completion, validation ports, event append |
| Projection | leader-elected loop, co-transactional checkpoint, rebuildable |
| GC | unreferenced blob sweeper, interlocked with projection freshness |
| Frontend | working upload form exercising the real contract |

### Stub now — port defined, adapter permissive

| Port | Now | Later |
| --- | --- | --- |
| `BlobScanner` | `PermissiveScanner` — returns `Clean` plus the digest; detects the EICAR test string so the reject path is testable | ClamAV INSTREAM |
| `ContentValidator` | `BasicContentValidator` — declared-size match + magic-byte sniff | format-specific deep validation |

### Defer entirely — define the event type, produce nothing

`DocumentMetadataChanged`, `DocumentReverted`, `DocumentDeleted`,
`DocumentTextExtracted`, `DocumentIndexed`, `DocumentStageFailed`,
`DocumentBlobPruned`. **The fold must handle all of them** so the projection is
complete when producers arrive; nothing emits them yet.

Out of scope: `PATCH` / `DELETE` / `revert` / `history` endpoints, Qdrant, graph,
realtime WebSocket, text extraction, and **version retention**.

`DocumentBlobPruned` therefore has no producer in this slice, but it is a
**reserved slot with a named future owner**, not an orphan. The planned mechanism
is enforcement at write time rather than by a sweeper: when the worker appends a
new blob version it also prunes the now-oldest beyond the retention count (e.g.
keep the last 10), deleting that object and appending `DocumentBlobPruned`.
Convenient, because the replace path (§9.6) already folds the document's full
history for its redelivery guard — the blob list is in hand at exactly the moment
the bound could be exceeded. Retention is then bounded by version count rather
than by age: a document that stops being updated keeps its versions indefinitely,
so storage grows with `documents × N`, not with time.

Note this is distinct from the GC sweeper, which reclaims *unreferenced* blobs and
**is** in scope.

Replace-mode upload (`document_id` supplied) **is in scope** — it exercises the
CAS path and the second-version fold.

### Decided against — do not add these back

- **No pending states in the event log.** An upload in flight produces no event.
  Consequently: no per-version state machine, no `uploading` event, no expiry
  sweeper over the event stream. In-flight visibility comes from the
  `upload_attempt` table, which is operational, not truth.
- **No idempotency-key store.** A lost `POST /uploads` response means the client
  retries and GC reclaims the orphaned empty multipart. Prevention is not worth
  the machinery when cleanup is automatic.
- **No `PATCH` that returns upload URLs.** One entry point per kind of change.

### Consequences accepted

- **Content revert is not supported, and the log cannot yet say so.** GC reclaims
  superseded blobs after the grace period, so only metadata can be reverted from
  history — but no `DocumentBlobPruned` event records that the bytes went, so a
  version list folded from the log will list versions whose blobs no longer exist.
  Do **not** build a "restore this version" affordance on top of this slice.
- **Retention must land before content history is promised to users.** When it
  does, it also changes the GC predicate: today GC deletes anything that is not
  some document's `current_blob`, which includes every historical version. A
  retention policy that keeps the last N versions must make those N *referenced*
  — otherwise GC reclaims at 48h whatever retention is trying to keep. The two
  are one change, not two.
- **A rejected upload cannot be retried at the same `upload_id`** within the
  dedupe window (§9.3). The recovery path is a fresh `POST /uploads`.

---

## 1.5 Inventory — What Exists and Why

| Kind | Tense | Lifetime | Can it fail? |
| --- | --- | --- | --- |
| Command / work item | imperative, *"do this"* | deleted on ack | yes — that is its purpose |
| Event | past, *"this happened"* | forever | no — it already happened |
| KV record | coordination state | TTL 24h | n/a |
| Projection row | derived | until rebuilt | n/a |
| Operational row | attempt tracking / UX | swept | n/a |

```text
commands   UploadCompleted → document_work.v1.upload_completed   (DOCUMENT_WORK)

events     produced now : DocumentCreated · DocumentBlobValidated
           folded only  : DocumentMetadataChanged · DocumentTextExtracted
                          DocumentIndexed · DocumentStageFailed
                          DocumentReverted · DocumentDeleted · DocumentBlobPruned

kv         UPLOAD_CONTEXT   <tenant>/<user>/<upload_id>   write-once, TTL 24h

postgres   document · projection_checkpoint · projection_failure · upload_attempt

s3         tenants/<tenant>/blobs/<upload_id>/original
```

Lifetime of one successful upload:

```text
after preflight   context ··· attempt(uploading) ··· multipart (0 bytes)
after part PUTs   context ··· attempt(uploading) ··· parts (real bytes)
after /complete   context ··· attempt(scanning) ···· parts ···· work item
worker succeeds   context ··· attempt(accepted) ···· object ··· ✗work item ··· EVENT
after projection  context ··· attempt(accepted) ···· object ··· EVENT ··· document row
24h later         ✗context ·· attempt(accepted) ···· object ··· EVENT ··· document row
```

**Only the event, the blob, and the projection row are load-bearing** — and the
row is rebuildable from the event. The `upload_attempt` row survives for support
and UX but is never consulted for correctness.

Two pairs that look redundant and are not:

- **Context vs. work item.** Different windows. The context spans preflight →
  `/complete`, when the client is uploading and there is no work to do yet but
  the upload's parameters must be remembered. The work item spans `/complete` →
  event, when there *is* work and it must survive a crash. Nothing drives a
  context; the work item does not exist during the upload.
- **Work item vs. event.** The command says "try to make this a document"; the
  event says "this is a document". A rejected scan runs its command to completion
  and emits **no event** — that asymmetry is why they are separate.

---

## 2. Architecture — Clean / Ports and Adapters

Dependencies point **inward only**.

```text
crates/document-domain     pure. events, fold, validation, part geometry.
                           no async, no IO, nothing beyond serde/chrono/thiserror.

crates/document-app        use cases + port traits. depends on document-domain.
                           knows "append an event", not "publish to JetStream".

crates/document-adapters   port implementations: JetStream, Postgres, S3.
                           depends on document-app. owns the migration runner.

services/api-service       wiring + HTTP. depends on app + adapters.
services/document-worker   wiring. runs two independent tasks:
                             (a) work-queue consumer — every instance
                             (b) projection loop     — leader-elected, §10.1
```

There is no separate projector service.

```d2
direction: right
Domain: "document-domain\npure logic"
App: "document-app\nuse cases + ports"
Adapters: "document-adapters\nJetStream / PG / S3"
Services: "services/*\nwiring"
Services -> Adapters
Services -> App
Adapters -> App
App -> Domain
```

**Rules:**

1. `document-domain` has no `async fn` and no IO. Testable with no fixtures, no
   containers, no runtime.
2. Ports are traits declared in `document-app`. Adapters implement them. A use
   case never names a concrete adapter.
3. Use cases return domain errors; the HTTP layer maps them to status codes. No
   `axum` types below the service layer.
4. Every port has a deterministic in-memory implementation used by tests.
5. `document-adapters` may be one crate now; split per technology when it grows.

Do not add document code to `crates/storage` or `crates/nats` — those keep their
current roles for chat.

---

## 3. Repository Orientation

Rust workspace, edition 2021. Current members:

```
crates/{auth,config,contracts,llm,nats,storage}
services/{api-service,chat-worker,document-worker,realtime-service}
```

Root `[workspace.dependencies]` provides `axum 0.8`, `sqlx 0.8` (**without the
`macros` feature** — use runtime `sqlx::query`/`query_as`; there is no database at
build time), `aws-sdk-s3 1.x`, `chrono 0.4`, `ulid 1`, `thiserror 2`, `tokio 1`,
`async-trait 0.1`.

**`async-nats` is *not* a workspace dependency** — it is declared only in
`crates/nats/Cargo.toml` at `0.45`. Promote it to `[workspace.dependencies]` so
`document-adapters` and `crates/nats` cannot drift.

You will also need to add: `include_dir` (or a build script) for §5.1, and
`proptest` + `proptest-derive` as dev-dependencies for §12.

Reusable as-is:

- `crates/auth` — `AuthContext` axum extractor, JWT verification, role
  normalization from `roles` / `realm_access.roles` / `resource_access.*.roles`.
  `auth.has_role("ingester")` is the write gate; `owner` is a realm composite
  including `ingester`.
- `crates/config` — `ServiceConfig::from_env`, `init_tracing`.

Infrastructure: `docker-compose.t2.yml` (traefik, keycloak, oauth2-proxy, redis,
nats, postgres, minio + `minio-init` + `minio-gc`, services). `make up`,
`make down`, `make logs`.

### 3.1 Traps in the existing repo

- **There is no migration runner.** `crates/storage/src/lib.rs:155` executes
  `include_str!("../../../migrations/0001_pg_cutover.sql")` on `PgRepository::connect`
  and is the only thing that creates any table. `migrations/0002_drop_upload_session.sql`
  is referenced by nothing and never runs.
- The existing `document` table carries `CHECK (state IN (...))` — a write-side
  rule in a read model. It is dropped.
- The current object key is
  `tenants/{tenant}/documents/{upload_id}/versions/1/original`
  (`services/api-service/src/main.rs:713`). Note the `documents/` segment already
  holds the **upload_id**, so blobs are effectively per-upload today despite the
  misleading path name.
- `crates/nats` is named for chat but hosts the old upload saga.

---

## 4. Conventions

From `AGENTS.md`: production end-state designs; plan for concurrency, failure
modes, observability, testability; end lengthy responses with `TL;DR` and
`Issues / Problems`.

- **Config:** new variables use `DELPHI_DOCUMENT_<NAME>` and are **required** —
  `std::env::var(...)?`, fail at startup. Do not add silent defaults. (Older code
  uses unprefixed `INGEST_UPLOAD_*`; do not copy that.)
- **Errors:** `thiserror` per crate. Internal detail is logged, never returned.
  Clients get a stable code and a safe message.
- **Shared types** live in `document-domain` and are used by producer and consumer
  alike; this is what keeps a projection un-poisonable (§6.3).
- **Tests:** inline `#[cfg(test)]`. `cargo test --workspace` and
  `cargo clippy --workspace --all-targets` must be clean.

---

## 5. Milestone 1 — Migrations and Schema

### 5.1 Ordered migration runner

Lives in `document-adapters`. Both `api-service` and `document-worker` invoke it
at startup — it is idempotent and advisory-locked, so concurrent startups are
safe.

- `CREATE TABLE schema_migration (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`.
- Embed the `migrations/` directory with `include_dir!` (a plain `include_str!`
  cannot enumerate a directory), sort by filename, apply each unapplied file in
  its own transaction, record it.
- Wrap the whole run in a Postgres **session advisory lock**.
- No checksum validation — dev databases are reset freely.

Delete the `include_str!` call from `crates/storage`. **Chat's schema is in the
same `migrations/` directory**, so the new runner covers it; `chat-worker` and
`realtime-service` do not run migrations and must tolerate starting before the
tables exist (they already fail and restart under compose).

### 5.2 Schema — `migrations/0003_document_projection.sql`

```sql
DROP TABLE IF EXISTS document CASCADE;
DROP TABLE IF EXISTS upload_session CASCADE;

CREATE TABLE document (
  tenant_id       text   NOT NULL,
  document_id     text   NOT NULL,
  owner_user_id   text   NOT NULL,

  version         bigint NOT NULL,      -- dense, user-visible
  stream_seq      bigint NOT NULL,      -- JetStream seq of last applied event

  state           text   NOT NULL,      -- 'active' | 'deleted'
  index_state     text   NOT NULL,      -- 'pending' | 'current' | 'failed'
  index_version   bigint,

  current_blob    text,                 -- upload_id of the serving blob
  filename        text,
  content_type    text,
  byte_size       bigint,
  checksum        text,

  title           text,
  tags            jsonb  NOT NULL DEFAULT '[]',
  description     text,
  metadata        jsonb  NOT NULL DEFAULT '{}',

  created_at      timestamptz NOT NULL,
  updated_at      timestamptz NOT NULL,

  PRIMARY KEY (tenant_id, document_id)
);

CREATE INDEX document_owner_updated_idx
  ON document (tenant_id, owner_user_id, updated_at DESC);

-- GC probes this per object; without it every probe is a sequential scan.
CREATE INDEX document_current_blob_idx
  ON document (tenant_id, current_blob);

CREATE TABLE projection_checkpoint (
  name        text   PRIMARY KEY,
  stream_seq  bigint NOT NULL,
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE projection_failure (
  name        text   NOT NULL,
  stream_seq  bigint NOT NULL,
  subject     text   NOT NULL,
  payload     jsonb  NOT NULL,
  error       text   NOT NULL,
  occurred_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (name, stream_seq)
);

-- One row per upload attempt, from preflight to terminal outcome.
-- Operational: drives GET /uploads/{id} and the "someone is uploading" hint.
-- Never consulted for correctness.
CREATE TABLE upload_attempt (
  tenant_id      text NOT NULL,
  upload_id      text NOT NULL,
  document_id    text NOT NULL,
  owner_user_id  text NOT NULL,
  mode           text NOT NULL,          -- 'create' | 'replace'
  status         text NOT NULL,          -- 'uploading' | 'scanning'
                                         -- | 'accepted' | 'rejected'
  filename       text NOT NULL,
  byte_size      bigint NOT NULL,
  version        bigint,                 -- set on accepted
  superseded     boolean NOT NULL DEFAULT false,
  reason         text,                   -- set on rejected
  started_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, upload_id)
);

CREATE INDEX upload_attempt_document_idx
  ON upload_attempt (tenant_id, document_id) WHERE status IN ('uploading', 'scanning');
```

Every write to `upload_attempt` from the worker must be
`INSERT … ON CONFLICT (tenant_id, upload_id) DO UPDATE`, because the reject path
and the final-delivery path can each run more than once.

**Schema rules — correctness requirements, not style:**

- `text`, never `varchar(n)`.
- No `CHECK` constraints on the projection.
- No foreign keys — events can arrive in an order that violates them.
- `jsonb` for anything not queried directly.

A projection that cannot reject an event cannot be poisoned by one.

---

## 6. Milestone 2 — `document-domain`

Pure crate. No async, no IO.

### 6.1 Events

```rust
pub const DOCUMENT_CONTRACT_VERSION: u16 = 1;

// NOTE: PartialEq only. `serde_json::Value` is not `Eq` (it contains f64),
// so deriving Eq anywhere in this tree will not compile.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DocumentEvent {
    pub v: u16,
    pub event_id: String,        // deterministic — see §9.6
    pub tenant_id: String,
    pub document_id: String,
    pub actor: Actor,
    pub version: u64,            // document version AFTER this event
    pub ts: DateTime<Utc>,
    pub payload: DocumentEventPayload,
}

#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Actor {
    User { user_id: String },
    System { component: String },
}

#[serde(tag = "type", rename_all = "snake_case")]
pub enum DocumentEventPayload {
    DocumentCreated(DocumentCreated),                            // produced now
    DocumentBlobValidated(DocumentBlobValidated),                // produced now
    DocumentMetadataChanged { patch: MetadataPatch },            // folded only
    DocumentTextExtracted(DocumentTextExtracted),                // folded only
    DocumentIndexed(DocumentIndexed),                            // folded only
    DocumentStageFailed(DocumentStageFailed),                    // folded only
    DocumentReverted { reverted_to: u64, patch: MetadataPatch }, // folded only
    DocumentDeleted { reason: String },                          // folded only
    DocumentBlobPruned { blob_ref: String, reason: String },     // folded only; see §1 retention
}

pub struct DocumentCreated {
    pub blob_ref: String,        // = upload_id; the object key derives from it
    pub filename: String,
    pub content_type: String,
    pub byte_size: u64,
    pub checksum: String,        // "sha256:<hex>"
    pub patch: MetadataPatch,
}

pub struct DocumentBlobValidated {
    pub blob_ref: String,
    pub filename: String,
    pub content_type: String,
    pub byte_size: u64,
    pub checksum: String,
    pub patch: MetadataPatch,
    /// Version the uploader was looking at. If not `version - 1`, this upload
    /// superseded a change its author had not seen.
    pub based_on_version: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MetadataPatch {
    #[serde(skip_serializing_if = "Option::is_none")] pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] pub tags: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")] pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] pub metadata: Option<serde_json::Value>,
}

pub struct DocumentTextExtracted { pub for_version: u64, pub extractor_version: String,
                                   pub char_count: u64, pub checksum: String }
pub struct DocumentIndexed       { pub for_version: u64, pub vector_count: u64,
                                   pub embedding_model: String }
pub struct DocumentStageFailed   { pub for_version: u64, pub stage: String,
                                   pub reason: String, pub attempts: u32 }
```

`MetadataPatch` is identical on both blob events (no `Option` wrapper — `Default`
makes it redundant) so consumers read one shape regardless of which event
carried the change.

**Version rules.** `DocumentCreated`, `DocumentBlobValidated`,
`DocumentMetadataChanged`, `DocumentReverted`, `DocumentDeleted` each increment
`version` by exactly one. `DocumentTextExtracted`, `DocumentIndexed`,
`DocumentStageFailed`, `DocumentBlobPruned` repeat the current `version` — they
record a fact *about* that version.

**`owner_user_id` comes from `actor`** on the creating event
(`Actor::User { user_id }`); a `System` actor on a create is a fold error.

### 6.2 The fold

```rust
pub struct DocumentState {
    pub tenant_id: String, pub document_id: String, pub owner_user_id: String,
    pub version: u64, pub stream_seq: u64,
    pub state: DocState,          // Active | Deleted
    pub index_state: IndexState,  // Pending | Current | Failed
    pub index_version: Option<u64>,
    pub current_blob: Option<String>,
    pub filename: Option<String>, pub content_type: Option<String>,
    pub byte_size: Option<u64>,   pub checksum: Option<String>,
    pub title: Option<String>,    pub tags: Vec<String>,
    pub description: Option<String>, pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>, pub updated_at: DateTime<Utc>,
}

pub fn apply(state: Option<DocumentState>, event: &DocumentEvent, stream_seq: u64)
    -> Result<DocumentState, FoldError>;
```

- `MetadataPatch` is **partial**: only `Some` fields are written.
- `current_blob` advances only on `DocumentCreated` / `DocumentBlobValidated`.
- `state` is `Active` until `DocumentDeleted`. `index_state` starts `Pending`.
- Deterministic: the same events in the same order always produce the same state.
- **`FoldError` is reserved for genuinely impossible input** — a non-create event
  arriving with `state = None`, or a `System` actor creating a document. Anything
  merely *unrecognised* is not a fold error; see §6.5.

`state` (`Active`/`Deleted`) is deliberately separate from `index_state`. A
document being re-indexed is still fully usable.

### 6.3 Validation

One function, used by every write path:

```rust
pub fn validate_metadata_patch(patch: &MetadataPatch) -> Result<(), ValidationError>;
```

Limits: `title` trimmed 1..=512 chars; each tag trimmed 1..=64 chars with no
control characters; ≤ 64 tags; `description` ≤ 8192 chars; `metadata` must be a
JSON object and ≤ 32 KB serialized (see §8.3 for why the cap matters).

Validation is a single runtime check on plain types rather than newtypes with
fallible constructors, because `MetadataPatch` must round-trip through `serde`
from both HTTP and NATS. Call it at every entry point — that discipline is what
keeps the projection un-poisonable.

### 6.4 Part geometry

```rust
pub const MAX_OBJECT_BYTES: u64 = 5 * 1024 * 1024 * 1024 * 1024;   // 5 TiB, S3 object cap
pub const MIN_PART_BYTES:   u64 = 5 * 1024 * 1024;
pub const MAX_PARTS:        u64 = 10_000;
pub const TARGET_PART_BYTES: u64 = 20 * 1024 * 1024;

pub fn part_size_bytes(file_size: u64) -> Result<u64, GeometryError>;
pub fn part_count(file_size: u64, part_size: u64) -> Result<u16, GeometryError>;
```

```text
reject if size == 0 or size > MAX_OBJECT_BYTES        // 5 TiB, NOT 50 TiB
part_size  = max(TARGET_PART_BYTES, ceil(size / MAX_PARTS))
part_count = ceil(size / part_size), must be 1..=10_000 and fit u16
part N covers [ (N-1)·part_size , min(N·part_size, size) )
```

`TARGET_PART_BYTES` (20 MiB) already exceeds `MIN_PART_BYTES`, so the 5 MiB floor
is never the binding term — it is asserted in a unit test rather than carried in
the formula.

The invariant the client relies on is stated with **`ceil`** everywhere:
`ceil(size / part_size) <= 10_000`. Uppy's internal clamp compares against the
same `ceil`, so a value produced here can never trigger it.

### 6.5 Forward compatibility

`#[serde(tag = "type")]` makes an unknown payload variant fail at
**deserialization**, before `apply` is ever called. The projection loop must
therefore handle two distinct cases:

1. **Undeserializable event** — log, write to `projection_failure`, advance the
   checkpoint, continue. This is what lets a producer of a new event type deploy
   before its consumers.
2. **`FoldError`** — a genuine domain violation; same handling, different log
   level and alert.

Neither may stall the checkpoint. Deserialize into `serde_json::Value` first and
attempt `DocumentEvent` from that, so the raw payload is available for
`projection_failure` either way.

---

## 7. Milestone 3 — `document-app` Ports and Use Cases

### 7.1 Ports

```rust
pub enum Expect { CreateOnly, Exactly(u64) }
pub struct Appended { pub stream_seq: u64, pub version: u64, pub duplicate: bool }

#[async_trait]
pub trait EventStore: Send + Sync + 'static {
    async fn append(&self, event: DocumentEvent, expect: Expect) -> Result<Appended, EventStoreError>;
    /// Authoritative (version, stream_seq), bypassing the projection.
    async fn last(&self, tenant: &str, document_id: &str) -> Result<Option<(u64, u64)>, EventStoreError>;
    async fn read_stream(&self, tenant: &str, document_id: &str) -> Result<Vec<(u64, DocumentEvent)>, EventStoreError>;
}

#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    async fn begin_multipart(&self, key: &str, content_type: &str) -> Result<String, BlobError>;
    async fn presign_part(&self, key: &str, upload: &str, part: u16, ttl: Duration)
        -> Result<PresignedPart, BlobError>;
    /// Paginated — S3 returns at most 1000 parts per call.
    async fn list_parts(&self, key: &str, upload: &str) -> Result<Option<Vec<UploadedPart>>, BlobError>;
    async fn complete_multipart(&self, key: &str, upload: &str, parts: &[CompletedPart])
        -> Result<(), BlobError>;
    async fn abort_multipart(&self, key: &str, upload: &str) -> Result<(), BlobError>;
    async fn head(&self, key: &str) -> Result<Option<BlobHead>, BlobError>;
    async fn open_read(&self, key: &str) -> Result<BoxAsyncRead, BlobError>;
    async fn delete(&self, key: &str) -> Result<(), BlobError>;
    /// Paginated via continuation token; GC must not materialise a whole bucket.
    async fn list_blobs(&self, prefix: &str, after: Option<String>, limit: usize)
        -> Result<BlobPage, BlobError>;
}

pub struct PresignedPart { pub part_number: u16, pub url: String,
                           pub method: &'static str, pub expires_at: DateTime<Utc> }
pub struct UploadedPart   { pub part_number: u16, pub etag: String, pub size: u64 }
pub struct CompletedPart  { pub part_number: u16, pub etag: String }
pub struct BlobHead       { pub byte_size: u64, pub content_type: Option<String>,
                            pub last_modified: DateTime<Utc> }
pub struct BlobPage       { pub items: Vec<BlobListing>, pub next: Option<String> }
pub struct BlobListing    { pub key: String, pub byte_size: u64,
                            pub last_modified: DateTime<Utc> }
pub type BoxAsyncRead = Pin<Box<dyn tokio::io::AsyncRead + Send>>;

/// Consumes the stream once and returns BOTH the verdict and the digest —
/// the worker has no other source for DocumentCreated.checksum.
#[async_trait]
pub trait BlobScanner: Send + Sync + 'static {
    async fn scan(&self, blob: BoxAsyncRead) -> Result<ScanOutcome, ScanError>;
}
pub struct ScanOutcome { pub verdict: ScanVerdict, pub sha256_hex: String, pub byte_count: u64 }
pub enum ScanVerdict { Clean, Infected { signature: String } }

#[async_trait]
pub trait ContentValidator: Send + Sync + 'static {
    async fn validate(&self, head: &BlobHead, prefix: &[u8], declared: &DeclaredContent)
        -> Result<ContentVerdict, ValidateError>;
}
pub struct DeclaredContent { pub filename: String, pub content_type: String, pub byte_size: u64 }
pub enum ContentVerdict { Ok, Rejected { reason: String } }

#[async_trait]
pub trait UploadContextStore: Send + Sync + 'static {
    /// KV `create`, not `put` — write-once is load-bearing (§7.2).
    async fn create(&self, ctx: &UploadContext) -> Result<(), ContextError>;
    async fn get(&self, tenant: &str, user: &str, upload_id: &str)
        -> Result<Option<UploadContext>, ContextError>;
}

#[async_trait]
pub trait AttemptStore: Send + Sync + 'static {
    async fn start(&self, a: &UploadAttempt) -> Result<(), StoreError>;
    async fn set_status(&self, tenant: &str, upload_id: &str, s: AttemptStatus)
        -> Result<(), StoreError>;   // upsert; may run more than once
    async fn get(&self, tenant: &str, upload_id: &str) -> Result<Option<UploadAttempt>, StoreError>;
    async fn active_for_document(&self, tenant: &str, document_id: &str)
        -> Result<Vec<UploadAttempt>, StoreError>;
    async fn sweep_older_than(&self, cutoff: DateTime<Utc>) -> Result<u64, StoreError>;
}

#[async_trait]
pub trait DocumentReadModel: Send + Sync + 'static {
    async fn get(&self, tenant: &str, document_id: &str) -> Result<Option<DocumentState>, ReadError>;
    async fn list(&self, tenant: &str, owner: &str, limit: u32, before: Option<DateTime<Utc>>)
        -> Result<Vec<DocumentState>, ReadError>;
    async fn checkpoint_lag(&self) -> Result<CheckpointLag, ReadError>;   // for GC, §10.2
}

#[async_trait]
pub trait WorkQueue: Send + Sync + 'static {
    async fn publish_upload_completed(&self, cmd: UploadCompleted) -> Result<(), QueueError>;
}

pub trait Clock: Send + Sync + 'static { fn now(&self) -> DateTime<Utc>; }
pub trait IdGen: Send + Sync + 'static { fn ulid(&self) -> String; }
```

`Clock` and `IdGen` are ports so use cases are deterministic under test.

### 7.2 The upload context

```rust
/// KV bucket UPLOAD_CONTEXT, key <tenant_id>/<user_id>/<upload_id>, max_age 24h.
/// WRITTEN ONCE via KV `create`. Read at /complete and /renew. Never mutated.
pub struct UploadContext {
    pub tenant_id: String,
    pub owner_user_id: String,
    pub upload_id: String,
    pub document_id: String,
    pub mode: UploadMode,            // Create | Replace
    pub storage_key: String,         // = key(tenant_id, upload_id)
    pub multipart_upload_id: String,
    pub filename: String,
    pub content_type: String,        // resolved, after defaulting
    pub declared_size: u64,
    pub part_size_bytes: u64,
    pub part_count: u16,
    pub created_at: DateTime<Utc>,
}

pub enum UploadMode { Create, Replace }
```

**No state field, no CAS, no mutation.** The worker never reads it. If it expires
before `/complete`, the client gets a clean `404` and re-uploads.

Deliberately not stored: presigned URLs (bearer capabilities that expire —
regenerate, never store), part ETags (they arrive in the `/complete` body), and
the metadata patch (supplied at `/complete`).

### 7.3 The work command

```rust
/// Self-contained. The worker MUST be able to finish using only this.
pub struct UploadCompleted {
    pub v: u16,
    pub command_id: String,          // upload-completed:<tenant>:<upload_id>
    pub tenant_id: String,
    pub owner_user_id: String,
    pub upload_id: String,
    pub document_id: String,
    pub mode: UploadMode,
    pub storage_key: String,
    pub multipart_upload_id: String,
    pub filename: String,
    pub content_type: String,
    pub declared_size: u64,
    pub if_match: Option<u64>,
    pub on_conflict: ConflictPolicy,  // Supersede (default) | Fail
    pub patch: MetadataPatch,
    pub parts: Vec<CompletedPart>,
    pub ts: DateTime<Utc>,
}
```

### 7.4 Appending events

Two distinct paths. **Do not use one helper for both** — a blind retry on a
create produces a second `DocumentCreated`.

```text
append_create(event):
    match store.append(event, Expect::CreateOnly):     // expected_last_subject_sequence(0)
        Ok(a)         => Ok(a)
        Err(Conflict) => Err(AlreadyCreated)   // caller treats as success (§9.6)
    // NEVER retry. The conflict IS the answer.

append_update(document_id, build_event, client_version: Option<u64>):
    for attempt in 0..3:
        (current_version, current_seq) = store.last(...)      // authoritative
        if let Some(v) = client_version, v != current_version:
            return Err(Conflict { current_version })
        match store.append(build_event(current_version + 1), Exactly(current_seq)):
            Ok(a)         => return Ok(a)
            Err(Conflict) => continue         // something landed in the window
    return Err(ConflictRetryExhausted)
```

For updates, two checks with different jobs: the **version check** rejects a
client acting on a stale document; the **sequence CAS** protects the window
between the read and the append. The retry makes them compose — an event that
moves `stream_seq` without moving `version` retries transparently.

**Always inspect `Appended.duplicate`.** When JetStream dedupes on `Nats-Msg-Id`
it returns the *original* sequence and does **not** evaluate the expected-sequence
header, so the locally computed `current_version + 1` may be wrong. On
`duplicate == true`, re-read via `last` and report that version.

---

## 8. Milestone 4 — Adapters and NATS Topology

### 8.1 Streams

```text
DOCUMENT_EVENTS
  subject          documents.<tenant_id>.<partition>.<document_id>
                   partition = crc32(document_id) % 16, zero-padded to 2 digits
  retention        Limits
  max_age          0 (infinite)
  discard          New          # never silently drop events if a limit is added
  storage          File
  allow_direct     true         # required for direct_get_last_for_subject
  duplicate_window 2h           # see the relation below

DOCUMENT_WORK
  subject          document_work.v1.upload_completed
  retention        WorkQueue
  max_age          24h          # a Termed message would otherwise live forever
  storage          File
  duplicate_window 2h
```

**The work subject root must not be `documents.`** — `documents.work.v1.upload_completed`
is four tokens and would be matched by both `documents.>` and
`documents.*.*.*`, so JetStream refuses to create the second stream (overlapping
subjects, err 10065), and the projection loop's `documents.>` filter would try to
fold work commands as events.

**`duplicate_window` must exceed the maximum redelivery span:**

```text
duplicate_window > (max_deliver × ack_wait) + max_pipeline_duration
```

With `max_deliver: 5` and `ack_wait: 120s` that floor is 10 minutes *plus* however
long a scan can legitimately run under progress heartbeats. 2h gives real margin;
a 10m window would leave replace-mode appends unprotected outside it.

**Pin the hash.** `crc32` (or any explicitly named, stable algorithm) — never
`std::collections::hash_map::DefaultHasher`, which is not stable across builds.
Changing the partition count or the hash forces a full rebuild, so both are
permanent contract.

**Never purge or roll up a document subject.** Both move the subject's last
sequence, invalidating outstanding CAS tokens and letting a create-on-zero
resurrect a deleted document id. Deletion is a tombstone event.

### 8.2 `EventStore` over JetStream

- `append` sets `Nats-Msg-Id = event.event_id`; `Expect::CreateOnly` maps to
  `expected_last_subject_sequence(0)`, `Expect::Exactly(n)` to `n`.
- Map `PublishErrorKind::WrongLastSequence` → `EventStoreError::Conflict`.
- Propagate `PublishAck::duplicate` into `Appended.duplicate` (§7.4).
- `last` uses `direct_get_last_for_subject`.
- `read_stream` uses an ephemeral ordered consumer filtered to the subject.

### 8.3 Work consumer and payload sizing

```rust
consumer::pull::Config {
    durable_name: Some("document-worker-upload-completed".into()),
    filter_subject: "document_work.v1.upload_completed".into(),
    ack_policy: AckPolicy::Explicit,
    ack_wait: Duration::from_secs(120),
    max_deliver: 5,          // MUST be finite — §10.2
    max_ack_pending: 64,
    ..Default::default()
}
```

The parts list dominates command size: 10 000 parts × ~55 bytes ≈ 550 KB. NATS
`max_payload` defaults to 1 MB and the compose service currently runs
`nats:2` with `command: ["-js", "-sd", "/data", "-m", "8222"]` — **add
`--max_payload=8388608`**. `/complete` must also reject a parts array whose
serialized command would exceed the limit with `413`, rather than discovering it
at publish time after the user has uploaded the whole file.

### 8.4 Stub adapters

```rust
pub struct PermissiveScanner;      // Clean + real sha256; EICAR string → Infected
pub struct BasicContentValidator;  // declared-size match + magic-byte sniff
```

Both are real implementations wired in production, logging at `info` what they
would eventually check. `PermissiveScanner` **must** compute a genuine digest —
it is the only source of `checksum`. Swapping in ClamAV must require no change
outside the adapter crate and one wiring line.

---

## 9. Milestone 5 — API and Worker

### 9.0 Authorization, Scoping, and Lifetimes

Every write endpoint requires the `ingester` realm role. **A role check alone is
not sufficient** — the upload context is capability-like.

**Only the user who started an upload may renew or complete it.**

```text
UPLOAD_CONTEXT key:  <tenant_id>/<user_id>/<upload_id>
```

`/complete` and `/renew` derive the key from the authenticated caller plus the
path `upload_id`. Another user hits a different key, finds nothing, and receives
`404` — structural, with no existence disclosure. Additionally verify the decoded
record's `owner_user_id`.

**Validate both `tenant_id` and `user_id` against `[A-Za-z0-9_-]`** and reject
otherwise. Both are NATS key segments, and `tenant_id` is also a *subject* token,
where `.`, `*`, and `>` would corrupt the space. Reject at the auth boundary so no
handler can be reached with an unsafe principal.

Replace mode requires write authority on the target document, checked at
**preflight**: tenant match plus `ingester`. Per-document ACLs are out of scope;
this is the single place to extend.

#### Object key scheme

```text
tenants/<tenant_id>/blobs/<upload_id>/original
```

A pure function of `(tenant_id, upload_id)`. No URL is ever persisted; the
projection's `current_blob` holds the `upload_id`.

`upload_id` is a ULID — 48 bits of timestamp plus 80 bits of randomness, so the
path is not guessable. Two conditions: generate with **`Ulid::new()`, never a
monotonic generator** (monotonic generators increment the random component within
a millisecond, making neighbours derivable from one known id); and keep the
bucket private with no endpoint ever presigning a caller-supplied key.

Accepted trade-off: `upload_id` also appears in responses, URL paths, and logs, so
anything logging one has logged the object location. That matters only if the two
protections above fail.

Never put a filename, title, or user-supplied string in the path.

#### Presigned URLs are bearer capabilities

The object key is cleartext in the URL path; only the signature is hashed, and it
obscures nothing. The signature authorises exactly that method, key, and part
number, and cannot be extended — but anyone holding the URL can write that part
until expiry, with arbitrary content (presigned PUTs use `UNSIGNED-PAYLOAD`).
Keep the part TTL short, use TLS, and **never log a presigned URL**. "Public
endpoint" means browser-reachable, not publicly readable.

#### Lifetimes

| Record | Store | Lifetime |
| --- | --- | --- |
| Upload context | NATS KV `UPLOAD_CONTEXT` (`max_age`) | `DELPHI_DOCUMENT_UPLOAD_TTL_SECS`, 24h |
| S3 multipart | S3, reaped by `minio-gc` | **48h** — strictly longer than the context TTL |
| `upload_attempt` row | Postgres, swept by age | 7d (support window) |
| Presigned part URL | signature expiry | `DELPHI_DOCUMENT_PART_URL_TTL_SECS`, 300s, re-issuable |
| Unreferenced object | GC sweeper | `DELPHI_DOCUMENT_GC_GRACE_SECS`, 48h |

The multipart reaper must be **longer** than the context TTL, or an upload started
at hour 23 has a live context pointing at a reaped multipart. Update `minio-gc`'s
`MINIO_INCOMPLETE_UPLOAD_MAX_AGE` to `48h`.

**Do not tie the context TTL to the part URL TTL.** An earlier design did, capping
any upload at five minutes. The part URL is short and re-issuable via `/renew`.

### 9.1 `POST /api/uploads`

Preflight. Must be called **before** the client constructs its uploader.

```jsonc
{
  "document_id": "01JZ…",     // omit = create, present = replace
  "filename": "annual-report.pdf",
  "size": 734003200,
  "content_type": "application/pdf"
}

201
{
  "upload_id": "01JZ8QM2…",
  "document_id": "01JZ8QK9…",
  "key": "tenants/acme/blobs/01JZ8QM2…/original",
  "part_size_bytes": 20971520,
  "part_count": 35,
  "part_url_ttl_secs": 300,
  "parts": [ { "part_number": 1, "url": "…", "method": "PUT", "expires_at": "…" } ]
}
```

`key` is included because Uppy's `createMultipartUpload` hook requires
`{ uploadId, key }`.

Flow:

1. Authorize; validate `filename` non-blank and geometry (§6.4).
2. Replace mode: resolve the target. Use **`EventStore::last`** for existence —
   the projection lags, and a document created seconds ago would 404 spuriously.
   Then read the projection for `state`; if the projection has not yet caught up
   (no row but `last` returned a version), treat as active. `404` if `last` is
   `None`, `403` on tenant mismatch, `409` if the projection says `deleted`.
   Create mode: mint `document_id`.
   **Authorising here is the point** — otherwise a user uploads 400 MB and only
   then learns they cannot write the target.
3. Mint `upload_id`.
4. `begin_multipart` at the derived key (internal endpoint).
5. Presign parts **1..=min(part_count, `DELPHI_DOCUMENT_PRESIGN_BATCH`)** (default
   1000). See below.
6. `UploadContextStore::create`.
7. `AttemptStore::start` with `status = 'uploading'`.
8. Return `201`.

Any failure after step 4 must `abort_multipart` **and** best-effort delete the KV
context if it was already written, so a stale context cannot outlive its
multipart. A crash between 4 and 6 leaves an empty multipart that GC reclaims.

**Presigning is batched, not exhaustive.** Presigning 10 000 URLs would produce a
multi-megabyte response in which every URL expires 300s later — useless for an
upload that takes hours. Issue at most `PRESIGN_BATCH` at a time and require
`/renew` for the rest. Presigning is local SigV4 computation with no network call,
so the cost is CPU and response size, not latency.

**Part size is server-owned.** The client MUST slice at exactly the returned
`part_size_bytes`. The server guarantees `ceil(size / part_size) <= 10_000`, so a
browser uploader's own clamp can never fire and change the geometry.

### 9.2 `POST /api/uploads/{upload_id}/renew`

```jsonc
{ "from_part": 1001, "count": 1000 }   // optional; defaults to the first missing window
→ 200 { part_size_bytes, part_url_ttl_secs, parts[] }
```

1. Read the context → `404` if absent or not the caller's.
2. `list_parts` (**paginated** — S3 returns at most 1000 per call) → `410 Gone` if
   the multipart no longer exists.
3. Presign the requested window, skipping parts `list_parts` reports as already
   uploaded.

This is why the part TTL stays at 300s while the upload window is 24h.

### 9.3 `POST /api/uploads/{upload_id}/complete`

```jsonc
{
  "parts": [ { "part_number": 1, "etag": "\"a54357…\"" } ],
  "if_match": 5,                  // replace mode only
  "on_conflict": "supersede",     // default; or "fail"
  "title": "Annual Report 2026",
  "tags": ["finance"]
}

202 { "state": "scanning" }
```

1. `400` on empty `parts`; `400` if `if_match` is present in create mode (the
   client cannot know a version for a document that does not exist);
   `413` if the serialized command would exceed `max_payload`; run
   `validate_metadata_patch`.
2. Read the context → `404` if absent or not the caller's.
3. Build `UploadCompleted` (§7.3) from the **context** plus the request's parts,
   `if_match`, `on_conflict`, and patch.
4. Publish to `DOCUMENT_WORK` with `Nats-Msg-Id = command_id`; await the `PubAck`.
5. `AttemptStore::set_status(scanning)`.
6. Return `202`.

**Appends no event and touches no document.** `document_id` is not accepted here —
it was fixed at preflight.

Duplicate or parallel calls are safe without CAS: JetStream dedupes on the message
id inside the duplicate window; outside it the worker is idempotent; and the
append-time CAS is the final arbiter.

**First part list wins, and a rejected upload is not retryable here.** The message
id derives from the upload id alone, so a second `/complete` with different ETags
is deduped and ignored — and after the worker `Term`s a poison command, a
re-`/complete` still returns `202` and does nothing until the dedupe window
elapses. The recovery path for a bad part list is a **fresh `POST /uploads`**;
surface that in the client rather than retrying `/complete`.

### 9.4 `GET /api/uploads/{upload_id}`

Reads `upload_attempt` — the single source for this endpoint.

```jsonc
{ "state": "uploading" }
{ "state": "scanning" }
{ "state": "accepted", "document_id": "…", "version": 7, "superseded": true }
{ "state": "rejected", "reason": "malware_detected" }
```

`404` if no attempt row exists for the caller's tenant. This is why
`upload_attempt` carries the outcome rather than being deleted: after a later
upload supersedes this one, no `document` row references this `upload_id`, so the
projection alone could never report `accepted`.

`accepted` and `rejected` are terminal — a `202` from `/complete` is not a
guarantee.

### 9.5 Document reads

```jsonc
GET /api/documents/{document_id}
200 {
  "document_id": "…", "version": 7, "state": "active", "index_state": "pending",
  "filename": "…", "content_type": "…", "byte_size": 812000000,
  "title": "…", "tags": [...], "description": null, "metadata": {},
  "created_at": "…", "updated_at": "…",
  "uploads_in_progress": [ { "upload_id": "…", "started_at": "…", "filename": "…" } ]
}
ETag: "7"

GET /api/documents?limit=50&before=<iso8601>
200 { "items": [ …same shape, without uploads_in_progress… ], "next": "<iso8601>|null" }
```

`404` until the first event is **folded** — the projection is the read path, so
there is a lag window after the event is durable. Expected, not an error.

`uploads_in_progress` comes from `AttemptStore::active_for_document` and lets the
client warn before starting a replace.

### 9.6 Worker — `FinishUpload`

One work item drives everything; the message stays unacked until the event is
durable.

```text
1. spawn a heartbeat task (see below)
2. complete_multipart(storage_key, multipart_upload_id, parts)
     Ok                       -> continue
     NoSuchUpload/AlreadyDone -> head(key); present -> continue (idempotent re-run)
                                             absent -> REJECT "multipart lost"
     InvalidPart/ETag mismatch/EntityTooSmall -> REJECT "invalid parts"   (permanent)
     network / 5xx / timeout                  -> NAK, let redelivery retry (transient)
3. head        -> byte_size must equal declared_size, else REJECT "size mismatch"
4. first 512B  -> ContentValidator, else REJECT with its reason
5. scan(open_read(key)) -> ScanOutcome { verdict, sha256_hex, byte_count }
     Infected -> REJECT "malware_detected"
     byte_count must equal declared_size, else REJECT "size mismatch"
6. append (see below)
7. AttemptStore::set_status(accepted { version, superseded })
8. ack
```

**Reject path:** `delete` the object, `set_status(rejected { reason })`, ack. On
the final delivery (`num_delivered == max_deliver`) do the same with the last
error, then `AckKind::Term`.

**Ack last, always.**

**Heartbeat, not one-shot progress.** `AckKind::Progress` extends the deadline by
one `ack_wait` *from when it is sent*, so a single call before a multi-minute scan
buys 120 seconds, not the duration of the scan. Spawn a task that sends `Progress`
every `ack_wait / 2` for the life of the message and is cancelled on
ack/term. Getting this wrong silently produces concurrent duplicate deliveries.

**Appending:**

```text
create mode:
    append_create(DocumentCreated { … })
      Ok               -> accepted
      AlreadyCreated   -> a previous delivery succeeded. Read `last`, report that
                          version, ack. NOT an error, and NOT retried.

replace mode:
    events = read_stream(tenant, document_id)
    if any event has blob_ref == this upload_id  -> already applied; ack.
        // NOTE: compare against the WHOLE history, not the current head.
        // A concurrent upload may have already superseded ours, so
        // `current_blob != upload_id` does not mean we have not applied.
    (current_version, _) = fold(events)
    if if_match is Some(v) and v != current_version:
        on_conflict == Fail       -> REJECT "version_conflict"
        on_conflict == Supersede  -> continue, recording based_on_version = v
    append_update(...)  with DocumentBlobValidated { …, based_on_version }
```

`superseded` for the attempt row is `based_on_version.is_some() && based_on_version != Some(version - 1)`.

**`event_id` must be deterministic** — `sha256("<tenant_id>|<upload_id>|created")`
or `…|blob_validated`, hex-truncated. Never `Ulid::new()`: a random id defeats
`Nats-Msg-Id` dedupe entirely, because a redelivery produces a different one. Pin
the construction; it is a permanent contract.

---

## 10. Milestone 6 — Projection and GC

### 10.1 Projection loop — a task inside `document-worker`

| Task | Instances | Model |
| --- | --- | --- |
| (a) work-queue consumer | every instance | competing consumers |
| (b) projection loop | exactly one | leader-elected |

```text
task (b):
  loop:
    conn = dedicated connection (not from the pool)
    if pg_try_advisory_lock(conn, DELPHI_PROJECTOR_LOCK_ID):
        run_projection_loop(conn)
    sleep(DELPHI_DOCUMENT_PROJECTOR_ELECTION_SECS)
```

Use a **session-scoped** lock (`pg_try_advisory_lock`, not `_xact_`) on a dedicated
held connection, with a hardcoded `bigint` id rather than `hashtext`, which is an
undocumented internal function.

**Re-verify ownership every batch.** If the connection drops (pool reconnect,
PgBouncer, a blip), Postgres releases the lock while the loop keeps projecting and
a standby acquires it — exactly the two-projector scenario the design forbids.
Before each commit, confirm on the same connection that the lock is still held
(`pg_advisory_lock`-owning query against `pg_locks`); abort the loop on loss.

```text
run_projection_loop(conn):
  checkpoint = SELECT stream_seq FROM projection_checkpoint WHERE name='document-pg'

  // An ORDERED EPHEMERAL consumer, positioned from the checkpoint on every start.
  // A durable consumer's deliver_policy is fixed at creation, so after a rebuild
  // (checkpoint reset to 0) it would resume from its own ack floor and replay
  // nothing.
  consumer = ordered_consumer(DOCUMENT_EVENTS,
                              filter="documents.>",
                              deliver_policy=ByStartSequence(checkpoint + 1))

  loop:
    batch = fetch up to 500
    BEGIN
      for raw in batch:
          match deserialize(raw):
            Ok(event) => match apply(state, event, seq) {
                Ok(next) => upsert(next),
                Err(e)   => record_projection_failure(seq, raw, e),   // skip, advance
            },
            Err(e)    => record_projection_failure(seq, raw, e),      // skip, advance
      UPDATE projection_checkpoint SET stream_seq = <last seq>, updated_at = now()
    COMMIT
```

Rules:

- Rows and checkpoint advance in **one transaction** — that is what makes the
  projection exactly-once. Keep the monotonic guard
  (`WHERE document.version < :version`) anyway; it costs nothing.
- Neither a deserialization failure nor a `FoldError` may stall the checkpoint
  (§6.5). Because the projection is keyed per document, a hole affects one
  document rather than freezing the read model.
- Parameterise the checkpoint name and target table so a rebuild can run alongside
  the live projection and be swapped in.
- Tasks (a) and (b) are supervised independently.

**Nothing except the projection loop writes the `document` table.**
`upload_attempt`, `projection_failure`, and `schema_migration` are operational
tables written elsewhere; that is not a violation.

### 10.2 GC sweeper

```text
every DELPHI_DOCUMENT_GC_INTERVAL_SECS:
    if checkpoint_lag() is unhealthy: SKIP THIS PASS      // see below
    for each page of list_blobs("tenants/", after, 1000):
        for each object older than DELPHI_DOCUMENT_GC_GRACE_SECS:
            upload_id = key segment
            delete unless EXISTS (SELECT 1 FROM document
                                  WHERE tenant_id = ? AND current_blob = upload_id)
```

**The projection-freshness interlock is not optional.** The predicate reads the
projection, so an empty or partial projection — during a rebuild, or while the
loop is down — would classify *every* object as unreferenced and delete the entire
bucket. A pass must abort unless:

- a `projection_checkpoint` row exists for `document-pg`, **and**
- its `updated_at` is within `DELPHI_DOCUMENT_GC_MAX_CHECKPOINT_AGE_SECS`, **and**
- its `stream_seq` is within a bounded distance of the stream head.

```text
GC_GRACE > max( UPLOAD_TTL,
                (max_deliver × ack_wait) + max_pipeline_duration )
```

Because the grace period exceeds the maximum upload lifetime, an in-flight
upload's object is always younger than the grace period, so the sweeper never
consults NATS KV. And because `max_deliver` is finite, a work item is provably
dead past its terminal deadline; with unlimited redelivery no grace period would
ever be safe.

This sweeper reclaims **unreferenced** blobs: abandoned uploads, rejected scans,
and blobs superseded by a later version. It emits no events.

**When retention lands, this predicate changes with it.** The future policy keeps
the last N versions, enforced by the worker at blob-update time (§1). Those N
versions must become *referenced* for GC purposes — extend the predicate to "not
referenced by any retained version" rather than "not `current_blob`" — or GC will
reclaim at `GC_GRACE` exactly what retention is trying to keep. Until then,
**content revert is impossible after `GC_GRACE`** because superseded bytes are
gone, and nothing in the log records their removal.

Also sweep `upload_attempt` rows older than the support window, and keep
`minio-gc` as a second line of defence for incomplete multiparts.

---

## 11. Milestone 7 — Frontend Upload Form

A working form that exercises the real contract end to end. Rebuild
`frontend/src/components/upload/UploadPage.tsx` and
`frontend/src/lib/documentUpload.ts`; the current versions are superseded.

Keep the Uppy `AwsS3` plugin — do not hand-roll the transfer. Uppy handles retry,
concurrency, and progress.

1. Update `frontend/src/lib/api.ts`: the endpoints move from
   `/api/ingestion/uploads` to `/api/uploads`, and `renew` is new.
2. **Preflight.** `POST /api/uploads` **before** constructing the Uppy instance.
   *Why:* Uppy fixes chunk boundaries in the `MultipartUploader` constructor,
   before the `createMultipartUpload` hook runs, so a part size fetched inside
   that hook arrives after the file is already sliced. The current code does read
   `created.part_size_bytes` into `getChunkSize` — the bug is purely the ordering.
3. `getChunkSize: () => created.part_size_bytes`; delete `DEFAULT_PART_SIZE_BYTES`.
   `createMultipartUpload` returns the stored `{ uploadId, key }` from the
   preflight response.
4. `signPart` serves URLs from the preflight batch; when a part number is outside
   the batch, or a PUT returns `403` (expired), call `/renew` for that window and
   continue.
5. `POST /complete` with parts, metadata, and `if_match` when replacing.
6. **Poll `GET /api/uploads/{id}` until `accepted` or `rejected`.** Do not report
   success on the `202`.
7. On `accepted`, fetch and display the document; surface `superseded: true` as
   "your upload replaced a newer version". On `rejected`, surface the reason and
   offer a fresh upload (not a `/complete` retry — §9.3).
8. Before starting a replace, `GET /api/documents/{id}` and warn if
   `uploads_in_progress` is non-empty.

Show file name, size, part count, progress, and phase. This is a contract test
harness; visual polish is not the goal. `bun run lint` must pass.

---

## 12. Testing

Domain (pure, no fixtures):

- Part geometry: the 20 MiB target, the 10 000-part clamp, the 5 TiB rejection,
  `size == 0`, and an assertion that the result never falls below 5 MiB.
- `validate_metadata_patch`: title length, tag count, control characters,
  non-object metadata, the 32 KB cap.
- Fold: every event type, partial `MetadataPatch`, monotonic guards,
  `owner_user_id` sourced from `Actor::User`.
- Version rules: which events bump and which do not.
- Determinism: the same events in the same order yield identical state.
- `FoldError` cases: a non-create event with no prior state; a `System` actor
  creating a document.

Property test (`proptest` + `proptest-derive`; you will need a strategy for
`DateTime<Utc>`):

```rust
proptest!(|(events in valid_event_sequence())| {
    // A sequence beginning with a create must fold without error, and
    // stream_seq must advance monotonically for every event in it.
    let mut state = None;
    for (seq, e) in events.iter().enumerate() {
        state = Some(apply(state, e, seq as u64 + 1).expect("valid sequence must fold"));
    }
});
```

Generate *valid sequences* rather than arbitrary single events — an arbitrary
`DocumentDeleted` against `None` is legitimately a `FoldError`, so asserting that
any event applies to any state would contradict §6.2.

Use-case tests with in-memory ports: create; replace; concurrent replace where
both apply; `on_conflict: fail`; every reject path; renew after expiry; renew of
a window beyond the preflight batch; `/complete` by a different user → `404`;
`if_match` in create mode → `400`.

Integration (`make up`):

- Create: upload → complete → poll → document at version 1.
- Replace: second upload → version 2, `current_blob` changed.
- Concurrent replace: two uploads from the same `if_match` → both apply, the later
  wins, the loser's attempt row reports `accepted` with `superseded: true`, and
  the loser's blob is reclaimed by GC.
- Redelivery: kill the worker between `complete_multipart` and the append → on
  restart exactly one document exists and exactly one event is in the stream.
- Redelivery after the dedupe window (temporarily shorten it) → still exactly one
  event, via the create conflict and the replace history scan.
- EICAR file → no document, attempt `rejected`, object deleted.
- Rebuild: truncate `document` and the checkpoint, restart the projection loop,
  resulting rows are identical.
- GC safety: with the checkpoint stale, a GC pass must delete **nothing**.
- GC liveness: an abandoned upload's object is removed after the grace period; a
  referenced object is not.

---

## 13. Known Traps

1. **Never use one append helper for create and update.** A blind retry on a
   create conflict produces a second `DocumentCreated` (§7.4).
2. **`AlreadyCreated` on redelivery is success**, not an error to retry.
3. **The replace redelivery guard must scan the whole history for `blob_ref`,**
   not compare against `current_blob`. A concurrent upload may already have
   superseded yours.
4. **`event_id` must be deterministic** or `Nats-Msg-Id` dedupe silently does
   nothing.
5. **Check `PublishAck::duplicate`** — on a dedupe, the locally computed version is
   wrong.
6. **`duplicate_window` must exceed the full redelivery span**, not just
   `max_deliver × ack_wait`.
7. **`AckKind::Progress` needs a periodic heartbeat**, not one call before a long
   operation.
8. **`max_deliver` must be finite** or GC can never be proven safe.
9. **GC must abort when the projection is stale** or it deletes the whole bucket
   during a rebuild.
10. **The work-queue subject must not share a root with the event subject** or
    stream creation fails.
11. **A durable consumer cannot be repositioned from the checkpoint.** Use an
    ordered ephemeral consumer.
12. **No foreign keys and no `CHECK` in the projection.** Out-of-order arrival
    violates them.
13. **`serde_json::Value` is not `Eq`.** Deriving `Eq` on the event tree will not
    compile.
14. **Unknown event types fail at deserialization, not at the fold.** Handle both,
    and advance the checkpoint for both.
15. **The upload context may expire mid-flight.** The worker works from the command
    payload alone and never reads it.
16. **Two projectors during a rolling deploy.** The advisory lock is required, and
    its liveness must be re-verified.
17. **Ack after append, never before.**
18. **Never delete or purge an event.** Compensation is always a new event.
19. **Projections fold; they never judge.** They may fail only for infrastructure
    reasons.
20. **The object key function and the partition hash are permanent contracts.**
21. **Use `Ulid::new()`, not a monotonic generator**, for `upload_id`.

---

## 14. Acceptance Criteria

- `cargo check --workspace --all-targets`, `cargo test --workspace`, and
  `cargo clippy --workspace --all-targets` clean for the new crates and services.
  Pre-existing chat code may be broken (§0) but must be listed explicitly in the
  final report.
- `bun run lint` passes in `frontend/`.
- All integration scenarios in §12 pass against `make up`, including both GC
  scenarios.
- A document's state can be reconstructed by truncating the projection and
  replaying the log, producing identical rows.
- Nothing but the projection loop writes the `document` table.
- Swapping `PermissiveScanner` for a real scanner requires changes only in the
  adapter crate and one wiring line.
- `document-domain` has zero async functions and no dependency on NATS, sqlx,
  aws-sdk, or axum.

---

## 15. Final Report

When done, report:

1. What was built, per milestone.
2. What was deleted or broken, explicitly — especially anything in chat.
3. Deviations from this spec and why.
4. Which ports remain stubbed and exactly where the real implementation plugs in.
5. `TL;DR` and `Issues / Problems` per `AGENTS.md`.
