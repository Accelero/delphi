# PG Cutover and Upload Pipeline Implementation Plan

## Summary

Implement this in three sequential milestones:

1. Replace SurrealDB with Postgres and move existing chat persistence to PG.
2. Add document upload v1: multipart S3 upload, event-driven complete flow, timeout worker, and PG document row creation.
3. Leave ingestion/vector work for the next milestone, but create the schema/event boundaries so ingestion can attach cleanly later.

Chosen defaults:

- Dev reset cutover: no Surreal data migration.
- Keep current multipart upload API shape.
- `/complete` publishes a NATS command; a worker completes S3 and writes PG.
- Use `sqlx` with runtime queries and migrations.
- Use text IDs, not UUID columns, because current tenant/user/conversation IDs are strings/ULIDs such as `tenant-a`.

## Key Changes

### Infrastructure

- Replace `surrealdb` service in `docker-compose.t2.yml` with `postgres`.
- Add `postgres-data` volume and remove `surreal-data`.
- Add service env:

```text
DATABASE_URL=postgres://delphi:delphi@postgres:5432/delphi
PG_MAX_CONNECTIONS=10
```

- Update `api-service`, `chat-worker`, and `realtime-service` to depend on `postgres`.
- Remove `SURREAL_*` env vars from compose after PG repository is wired.

### Rust Storage

- Add `sqlx` workspace dependency:

```toml
sqlx = { version = "...", default-features = false, features = [
  "runtime-tokio-rustls",
  "postgres",
  "chrono",
  "json"
] }
```

- Replace `SurrealChatRepository` with `PgRepository`.
- Keep existing `ChatRepository` trait so API/chat-worker/realtime code changes stay small.
- Add `DocumentRepository` methods for upload completion:

```rust
create_uploaded_document_snapshot(...)
get_document_by_upload_id_or_document_id(...)
```

- Remove Surreal-specific bootstrap/query code after PG parity is working.

### PG Schema

Add migrations under a new `migrations/` directory.

Core chat tables:

```sql
tenant (
  tenant_id text primary key,
  name text not null,
  metadata jsonb not null default '{}',
  created_at timestamptz not null default now()
)

app_user (
  tenant_id text not null,
  user_id text not null,
  email text,
  display_name text,
  created_at timestamptz not null default now(),
  last_seen_at timestamptz,
  primary key (tenant_id, user_id)
)

chat_conversation (
  tenant_id text not null,
  user_id text not null,
  conversation_id text not null,
  title text not null default 'New chat',
  next_message_ordinal bigint not null default 1,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  deleted_at timestamptz,
  primary key (tenant_id, conversation_id)
)

chat_message (
  tenant_id text not null,
  user_id text not null,
  conversation_id text not null,
  message_id text not null,
  role text not null check (role in ('system', 'user', 'assistant', 'tool')),
  content text not null default '',
  parent_message_id text,
  citations jsonb not null default '[]',
  turn_id text,
  interrupted boolean not null default false,
  finish_reason text,
  ordinal bigint not null,
  created_at timestamptz not null default now(),
  primary key (tenant_id, message_id),
  unique (tenant_id, conversation_id, ordinal)
)

chat_turn (
  tenant_id text not null,
  turn_id text not null,
  user_id text not null,
  conversation_id text not null,
  user_message_id text,
  assistant_message_id text,
  parent_message_id text,
  status text not null check (status in ('committed', 'interrupted', 'failed')),
  worker_id text,
  error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, turn_id)
)
```

Indexes:

```sql
chat_conversation(tenant_id, user_id, updated_at desc)
  where deleted_at is null

chat_message(tenant_id, user_id, conversation_id, ordinal)

chat_turn(tenant_id, conversation_id, created_at)
```

Document/upload tables:

```sql
document (
  tenant_id text not null,
  document_id text not null,
  owner_user_id text not null,
  document_version bigint not null,
  state text not null check (state in ('active', 'deleted', 'tombstoned', 'failed')),
  title text,
  metadata jsonb not null default '{}',
  object_key text,
  object_etag text,
  object_size_bytes bigint,
  content_sha256 text,
  content_type text,
  filename text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  deleted_at timestamptz,
  primary key (tenant_id, document_id)
)

outbox_event (
  event_id text primary key,
  subject text not null,
  event_type text not null,
  tenant_id text not null,
  aggregate_id text not null,
  aggregate_version bigint not null,
  payload jsonb not null,
  status text not null default 'pending'
    check (status in ('pending', 'publishing', 'published', 'failed')),
  publish_attempts int not null default 0,
  next_attempt_at timestamptz not null default now(),
  locked_by text,
  locked_until timestamptz,
  last_error text,
  created_at timestamptz not null default now(),
  published_at timestamptz
)
```

Indexes:

```sql
document(tenant_id, owner_user_id, updated_at desc)
  where state = 'active'

outbox_event(status, next_attempt_at, created_at)
  where status in ('pending', 'failed', 'publishing')
```

## Implementation Steps

### Milestone 1: PG Chat Cutover

- Add PG service and `DATABASE_URL` config.
- Update `ServiceConfig` to expose `database_url` and `pg_max_connections`.
- Add `PgRepository::connect(database_url, max_connections)`:
  - creates a shared `PgPool`;
  - runs migrations on startup;
  - uses a bounded pool, not one connection per request.
- Implement `ensure_principal` with `INSERT ... ON CONFLICT`.
- Implement chat methods with PG transactions:
  - list/create/get/rename/delete conversation;
  - assert tail parent;
  - record failed turn;
  - commit completed/interrupted turn.
- In `commit_turn`, lock the conversation row with `FOR UPDATE`, delete messages after parent ordinal, upsert deterministic user/assistant messages, upsert `chat_turn`, update conversation timestamp and `next_message_ordinal`.
- Switch `api-service`, `chat-worker`, and `realtime-service` from `SurrealChatRepository` to `PgRepository`.
- Remove Surreal container/env/dependency after tests pass.

### Milestone 2: Upload API and NATS KV Job State

Keep current HTTP contract:

```text
POST /api/ingestion/uploads
POST /api/ingestion/uploads/:upload_id/sign-part
POST /api/ingestion/uploads/:upload_id/complete
GET  /api/ingestion/uploads/:upload_id
```

`POST /api/ingestion/uploads`:

- Auth requires `member`, `ingester`, or `owner`.
- Validate filename and size.
- Generate:

```text
upload_id = ULID
document_id = upload_id
job_id = "<upload_id>:upload:v1"
object_key = "tenants/<tenant_id>/documents/<document_id>/versions/1/original"
```

- Create S3 multipart upload with pending tag/metadata:

```text
upload_state=pending
```

- Create NATS KV job only if absent:

```text
bucket: document_jobs
key: jobs.<tenant_id>.<document_id>.<job_id>
stage: awaiting_upload
operation: create
upload_id
multipart_upload_id
object_key
filename
content_type
declared_size
title
metadata
```

- Write timeout index key:

```text
bucket: document_job_timeouts
key: upload_timeouts.<epoch_minute>.<tenant_id>.<document_id>.<job_id>
```

- Return existing frontend-compatible response:

```json
{
  "upload_id": "...",
  "key": "...",
  "multipart_upload_id": "...",
  "part_size_bytes": 8388608,
  "part_url_ttl_secs": 900
}
```

`POST /sign-part`:

- Load KV job by upload/document/job.
- Require `stage = awaiting_upload`.
- Require authenticated tenant/user owns the job.
- Return presigned S3 part URL with short TTL.

`POST /complete`:

- Validate `parts` is non-empty.
- Load KV job and reject terminal/expired/missing jobs.
- Publish NATS command:

```text
subject: documents.uploads.v1.completed
Nats-Msg-Id: upload-completed:<tenant_id>:<upload_id>
```

Payload includes tenant/user/document/job IDs and uploaded parts.

- Wait for JetStream PubAck.
- Return `202` with existing response shape:

```json
{
  "result": "accepted",
  "document_id": "...",
  "job_id": "..."
}
```

`GET /uploads/:upload_id`:

- Read KV first.
- If KV is gone, fall back to PG `document` where `document_id = upload_id`.
- Return:
  - `uploading` for `awaiting_upload` or `completing_upload`;
  - `accepted` when PG document exists or KV stage is `ready`;
  - `failed` for `expired`, `aborted`, or `failed`.

### Milestone 3: Document Worker and Timeout Service

Add `services/document-worker` or a dedicated worker module/binary.

Responsibilities:

- Ensure NATS stream for `documents.uploads.v1.>`.
- Ensure KV buckets:
  - `document_jobs`
  - `document_job_timeouts`
- Subscribe to:
  - `documents.uploads.v1.completed`
  - `documents.uploads.v1.timeout_requested`
- Run timeout scheduler loop.

Complete worker flow:

```text
receive completed command
-> load KV job
-> if already ready, ack
-> if expired/aborted/failed, ack without resurrecting
-> CAS awaiting_upload -> completing_upload
-> complete S3 multipart upload
-> if S3 says already completed and object exists, continue
-> HEAD object
-> tag object upload_state=committed
-> PG transaction:
     insert document version 1 state active
     insert outbox_event with full document snapshot
-> CAS completing_upload -> ready
-> ack
```

PG insert behavior:

- `document_version = 1`
- `state = active`
- store title, metadata, filename, content type, object key, etag, object size.
- If the same document already exists with the same object key/version, treat as idempotent success.
- If a conflicting row exists with different object data, mark KV failed and ack.

Timeout scheduler flow:

```text
every UPLOAD_TIMEOUT_SCHEDULER_INTERVAL_SECS:
  for each due epoch_minute <= now:
    list document_job_timeouts/upload_timeouts.<epoch_minute>.>
    publish documents.uploads.v1.timeout_requested
      Nats-Msg-Id = upload-timeout:<tenant>:<document>:<job>
    delete timeout index key after PubAck, best effort
```

Timeout handler flow:

```text
receive timeout_requested
-> load KV job
-> if missing or stage advanced past awaiting_upload, ack
-> CAS awaiting_upload -> expiring_upload
-> abort S3 multipart upload
-> delete pending object if one exists
-> CAS expiring_upload -> expired
-> ack
```

Race rules:

- Complete wins if it CASes `awaiting_upload -> completing_upload` first.
- Timeout wins if it CASes `awaiting_upload -> expiring_upload` first.
- Timeout must not delete objects tagged `upload_state=committed`.
- Complete must not resurrect `expired`, `aborted`, or `failed` jobs.
- Worker redelivery repeats deterministic work and uses KV/PG guards.

### S3/MinIO Cleanup

- Add S3 helper methods:
  - `head_object`
  - `delete_object`
  - `put_object_tagging`
  - `get_object_tagging` if needed for safety
- Add pending tag at multipart creation.
- Mark committed after successful complete/HEAD.
- Configure MinIO bucket lifecycle in `minio-init` as fallback:
  - abort incomplete multipart uploads after one day;
  - expire/delete pending objects after one day if tag-based lifecycle is available.
- Treat lifecycle as last-line cleanup only; product-visible timeout comes from the worker.

## Tests

### Rust Checks

- `cargo check --workspace`
- Unit tests for:
  - PG repository chat CRUD parity;
  - `commit_turn` idempotence;
  - stale parent rejection;
  - deterministic upload key/job ID generation;
  - timeout bucket key calculation.

### Integration/E2E

- Existing chat E2E must pass unchanged:
  - create/list/get/rename/delete conversation;
  - submit first turn;
  - stale parent conflict.
- Add upload E2E:
  - create upload, sign parts, upload to MinIO, complete, poll until accepted, assert PG `document` row exists.
  - create upload and never complete; timeout worker expires it and aborts multipart upload.
  - complete vs timeout race: only one KV terminal path wins.
  - duplicate complete event: PG document row remains single and outbox event is not duplicated.
  - complete after expired upload returns conflict or remains failed by status.

### Manual Smoke

- `make rebuild-up`
- Login through frontend.
- Open upload page.
- Upload a small text/PDF file.
- Verify:
  - S3 object exists under document key;
  - PG `document` row exists;
  - `document_version = 1`;
  - `state = active`;
  - KV job reaches `ready`.

## Assumptions

- No Surreal data migration is required.
- Existing frontend multipart upload UI remains the compatibility target.
- `document_id = upload_id` for create-upload v1.
- Upload complete response means "accepted for processing"; the PG row may appear milliseconds later and status polling is authoritative.
- Ingestion/vector processing is out of scope for this step, but `outbox_event` is inserted now so the next milestone can consume document snapshots.
