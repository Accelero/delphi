---
title: PG Outbox Document CRUD
description: Selected initial document CRUD design using Postgres transactions, an outbox table, and NATS projection fanout.
---

# PG Outbox Document CRUD

> **Not the selected design.** The document architecture was settled in favour
> of an event-sourced model on NATS JetStream — see
> [Document Upload and Lifecycle](../architecture/document-upload). This page is
> retained as a record of the design space that was explored.

This page records the Postgres-first outbox alternative: the API commits the
current document row and its integration event in one transaction. The API commits the current document row and the
corresponding integration event in the same Postgres transaction. A publisher
service claims outbox rows, publishes them to NATS JetStream, waits for
`PubAck`, and marks the rows published. NATS then drives ingestion,
projection, realtime, and repair work.

This gives simple CRUD semantics and strong read-after-write behavior for the
current document row, while keeping expensive and multi-database work
asynchronous and idempotent.

## Ownership

| State | Owner |
| --- | --- |
| Current document row | Postgres |
| Document version | Postgres `document.document_version` |
| CRUD transaction atomicity | Postgres transaction |
| Event publication intent | Postgres `outbox_event` |
| Event delivery after publication | NATS JetStream |
| Live upload/job gate and realtime state | NATS KV |
| Upload timeout lookup | NATS KV time-bucket index |
| Object bytes | S3-compatible storage |
| Derived content/chunks | Postgres projection tables |
| Vector index | Qdrant projection |
| Graph index | NebulaGraph projection |

The outbox is not the job state machine. It only records committed PG events
that still need publication. Live job progress stays in NATS KV because KV
supports CAS gates, watch-based realtime updates, and short TTL cleanup.

## Data Model

### Document Row

Minimal current document row:

```sql
CREATE TABLE document (
  tenant_id uuid NOT NULL,
  document_id uuid NOT NULL,
  owner_user_id uuid NOT NULL,
  document_version bigint NOT NULL,
  state text NOT NULL CHECK (
    state IN ('active', 'deleted', 'tombstoned', 'failed')
  ),
  title text,
  metadata jsonb NOT NULL DEFAULT '{}',
  object_key text,
  object_etag text,
  object_size_bytes bigint,
  content_sha256 text,
  content_type text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz,
  PRIMARY KEY (tenant_id, document_id)
);
```

`document_version` is monotonic. The API assigns it server-side. Clients only
send the version they read.

### Outbox Event

Minimal outbox row:

```sql
CREATE TABLE outbox_event (
  event_id uuid PRIMARY KEY,
  subject text NOT NULL,
  event_type text NOT NULL,
  tenant_id uuid NOT NULL,
  aggregate_id uuid NOT NULL,
  aggregate_version bigint NOT NULL,
  payload jsonb NOT NULL,
  status text NOT NULL DEFAULT 'pending' CHECK (
    status IN ('pending', 'publishing', 'published', 'failed')
  ),
  publish_attempts int NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  locked_by text,
  locked_until timestamptz,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);

CREATE INDEX outbox_event_ready_idx
  ON outbox_event (status, next_attempt_at, created_at)
  WHERE status IN ('pending', 'failed', 'publishing');
```

`aggregate_version` is the committed `document.document_version`. The outbox
publisher must publish the stored payload as-is. It must not reread the live
document row and enrich the event later, because that can race with newer
document versions.

### NATS KV Job

Document jobs are short-lived workflow records:

```text
bucket: document_jobs
key: jobs.<tenant_id>.<document_id>.<job_id>
```

Minimal value:

```json
{
  "schema_version": 1,
  "tenant_id": "uuid",
  "document_id": "uuid",
  "job_id": "uuid",
  "operation": "create",
  "stage": "awaiting_upload",
  "upload_id": "uuid",
  "object_key": "documents/<tenant_id>/<document_id>/versions/7/original"
}
```

Upload-backed operations include `upload_id` and `object_key`. Metadata-only
operations may omit them.

Upload jobs expire by NATS KV TTL if the browser never completes. Incomplete
multipart data is reclaimed by MinIO GC, not by a document timeout workflow.

## Snapshot Events

Outbox events are full document-row snapshots. The latest snapshot is enough
for downstream projections to converge. This is not event sourcing; PG is the
current-state source of truth, and the outbox event is an integration event.

Example upsert event:

```json
{
  "event_type": "document.snapshot_upserted",
  "tenant_id": "uuid",
  "document_id": "uuid",
  "target_document_version": 7,
  "document": {
    "tenant_id": "uuid",
    "document_id": "uuid",
    "owner_user_id": "uuid",
    "document_version": 7,
    "state": "active",
    "title": "Quarterly report",
    "metadata": {},
    "object_key": "documents/<tenant_id>/<document_id>/versions/7/original",
    "object_etag": "etag",
    "object_size_bytes": 12345,
    "content_sha256": "hex",
    "content_type": "application/pdf"
  }
}
```

Example delete event:

```json
{
  "event_type": "document.snapshot_deleted",
  "tenant_id": "uuid",
  "document_id": "uuid",
  "target_document_version": 8,
  "document": {
    "tenant_id": "uuid",
    "document_id": "uuid",
    "owner_user_id": "uuid",
    "document_version": 8,
    "state": "deleted",
    "title": "Quarterly report",
    "metadata": {},
    "object_key": "documents/<tenant_id>/<document_id>/versions/7/original",
    "object_etag": "etag",
    "object_size_bytes": 12345,
    "content_sha256": "hex",
    "content_type": "application/pdf",
    "deleted_at": "2026-06-15T12:00:00Z"
  }
}
```

Restore and purge are intentionally omitted for now. CRUD against deleted or
tombstoned documents is rejected.

## CRUD Commit Flow

The API applies CRUD to PG and writes the outbox event in one transaction:

```text
HTTP CRUD request
-> authorize and validate
-> compute target_document_version server-side
-> BEGIN
-> insert/update/delete-tombstone document row with version CAS
-> insert outbox_event with full document snapshot and same version
-> COMMIT
-> return success/accepted
```

Clients never choose the outcome version:

```text
expected_document_version = version from GET /api/documents/:document_id
target_document_version = expected_document_version + 1
```

Update CAS:

```sql
UPDATE document
SET
  document_version = $target_document_version,
  title = $title,
  metadata = $metadata,
  object_key = $object_key,
  object_etag = $object_etag,
  object_size_bytes = $object_size_bytes,
  content_sha256 = $content_sha256,
  content_type = $content_type,
  state = $state,
  updated_at = now()
WHERE tenant_id = $tenant_id
  AND document_id = $document_id
  AND document_version = $expected_document_version
  AND state NOT IN ('deleted', 'tombstoned')
RETURNING *;
```

If no row is returned, the request is stale or invalid. The API returns a
conflict/error and does not insert an outbox event.

Create inserts only if `(tenant_id, document_id)` does not exist. Delete writes
a tombstone snapshot at the next server-assigned version. Normal user delete
targets the current version the user read.

## Upload Pre-Stage

Upload happens before the PG snapshot commit because bytes go directly to S3.
The document source state changes only when upload completion validates the
object and commits the PG row/outbox transaction.

Start upload:

```text
POST /api/documents/uploads
-> API authorizes
-> API creates document_id, job_id, upload_id, object_key
-> API creates NATS KV job stage = awaiting_upload
-> API creates S3 multipart upload
-> API returns short-lived presigned part URLs
```

Client uploads parts directly to S3. The presigned URLs are temporary write
grants only; S3 multipart completion remains server-side.

```text
INGEST_UPLOAD_WINDOW_SECS=300
INGEST_UPLOAD_PART_URL_TTL_SECS=300
UPLOAD_SAGA_KV_TTL_OFFSET_SECONDS=300
```

The upload KV TTL is `INGEST_UPLOAD_PART_URL_TTL_SECS +
UPLOAD_SAGA_KV_TTL_OFFSET_SECONDS`, capped by the configured upload window for
URL validity. The upload KV state expires if the client never completes.
MinIO GC aborts incomplete multipart uploads older than the fallback cleanup
window.

Complete upload:

```text
POST /api/documents/uploads/:upload_id/complete
-> API publishes documents.uploads.v1.completed to NATS
-> API returns after JetStream PubAck
-> upload completion worker CASes KV awaiting_upload -> completing_upload
-> worker validates S3 HEAD/checksum/content type
-> worker tags object upload_state=committed
-> worker computes target_document_version from PG
-> PG transaction writes document row and outbox snapshot event
-> worker CASes KV to projecting or ready according to work required
```

The complete endpoint does not directly mutate the document row. It records the
completion command durably in NATS. The worker owns the transition from upload
pre-stage into the PG CRUD transaction.

Abort:

```text
awaiting_upload -> aborting_upload -> aborted
```

Explicit abort deletes the pending multipart upload best-effort. Abandoned
browser uploads do not need a product timeout event: the KV state expires, and
MinIO GC aborts incomplete multipart uploads that were never completed.

## Abandoned Upload Cleanup

There is no timeout scheduler for abandoned browser uploads. The client either
calls `/complete`, or the upload state expires from NATS KV. MinIO GC aborts
incomplete multipart uploads older than the configured cleanup window.

## Outbox Publisher Service

The publisher service claims rows with timeout locks and `FOR UPDATE SKIP
LOCKED`. Multiple instances can run concurrently.

Claim:

```sql
BEGIN;

WITH ready AS (
  SELECT event_id
  FROM outbox_event
  WHERE status IN ('pending', 'failed')
    AND next_attempt_at <= now()
  ORDER BY created_at
  LIMIT $batch_size
  FOR UPDATE SKIP LOCKED
)
UPDATE outbox_event
SET status = 'publishing',
    locked_by = $publisher_id,
    locked_until = now() + interval '30 seconds',
    publish_attempts = publish_attempts + 1
WHERE event_id IN (SELECT event_id FROM ready)
RETURNING *;

COMMIT;
```

Recovery of abandoned publishing locks:

```sql
UPDATE outbox_event
SET status = 'failed',
    locked_by = NULL,
    locked_until = NULL,
    next_attempt_at = now()
WHERE status = 'publishing'
  AND locked_until < now();
```

Publish each claimed row:

```text
publish outbox.subject to NATS
Nats-Msg-Id = outbox.event_id
headers include tenant_id, document_id, aggregate_version
wait for PubAck
mark outbox row published
```

Mark published:

```sql
UPDATE outbox_event
SET status = 'published',
    published_at = now(),
    locked_by = NULL,
    locked_until = NULL,
    last_error = NULL
WHERE event_id = $event_id
  AND locked_by = $publisher_id;
```

On publish error:

```sql
UPDATE outbox_event
SET status = 'failed',
    locked_by = NULL,
    locked_until = NULL,
    last_error = $error,
    next_attempt_at = now() + $backoff
WHERE event_id = $event_id
  AND locked_by = $publisher_id;
```

If a publisher crashes after NATS `PubAck` but before marking published,
another publisher republishes with the same `Nats-Msg-Id`. NATS duplicate
detection and projection version guards make that safe.

## NATS Subjects

Upload pre-stage commands publish directly to JetStream because the document
row does not exist or has not changed yet:

```text
documents.uploads.v1.completed
documents.uploads.v1.aborted
```

Outbox events publish to NATS subjects:

```text
documents.snapshots.v1.<tenant_id>.<document_id>.upsert
documents.snapshots.v1.<tenant_id>.<document_id>.delete
```

Projection work events use:

```text
documents.work.v1.ingest.validate
documents.work.v1.ingest.extract
documents.work.v1.ingest.chunk
documents.work.v1.vector.qdrant.upsert
documents.work.v1.vector.qdrant.delete
documents.work.v1.graph.nebula.upsert
documents.work.v1.graph.nebula.delete
```

## Latest Wins And Regression Prevention

PG stores the full current document state. Ordering is not required for
projections because each event is a complete snapshot and carries its committed
version. The newest version wins.

Every projection stores the source version it reflects:

```text
PG content.source_document_version
PG chunk.source_document_version
Qdrant payload.projection_version
NebulaGraph projection_version
document_projection_state.last_applied_document_version
```

Apply rule:

```text
incoming_version > stored_version -> apply
incoming_version = stored_version -> no-op duplicate
incoming_version < stored_version -> skip stale
```

Before expensive work such as extraction, embedding, or graph building, a
worker may check PG or projection state. If a newer document version already
exists, the worker skips the old job and ACKs. This prevents old NATS messages
from regressing Qdrant, NebulaGraph, derived PG content, or realtime state.

The outbox publisher never rereads PG to build event payloads. It publishes the
versioned snapshot that was written in the same transaction as the document
row. That atomic capture is what lets downstream consumers distinguish old
events from current state.

## Job Progress: KV, Not Outbox

Do not track live job progress in the outbox table.

Use NATS KV:

```text
bucket: document_jobs
key: jobs.<tenant_id>.<document_id>.<job_id>
```

Why KV:

- realtime can watch it and push websocket updates;
- workers can CAS stage transitions;
- redelivery can resume cleanup or republish missing next work;
- values are short-lived and can expire by bucket TTL;
- it avoids turning the outbox into a mutable workflow table.

Why not outbox:

- outbox rows are publication intents, not workflow state;
- progress updates would create noisy PG write load;
- realtime would need polling instead of KV watch;
- outbox retention is operational event-publish history, not live job TTL.

The outbox answers:

```text
which committed PG events still need publishing?
```

The KV job answers:

```text
where is this live upload/ingestion/projection job right now?
```

## Projection Flow

After NATS receives the outbox event:

```text
document snapshot event
-> PG content/chunk projection checks if object fields changed
-> validate/extract/chunk if needed
-> write derived PG rows with source_document_version
-> publish Qdrant/Nebula projection work if needed
-> Qdrant/Nebula apply version-guarded writes
-> realtime service watches NATS KV job state
```

Qdrant and NebulaGraph are eventually consistent projections. They are not
part of the PG CRUD transaction. The UI can use `document_projection_state` to
show whether vectors/graph are caught up to the current document version.

## Retention And History

The outbox is not long-term document history. It may be retained briefly for
debugging and then archived or deleted. If Delphi needs rollback/history
without full event sourcing, add a retained snapshot stream or snapshot table
that stores the last N full document snapshots and keeps referenced S3 objects.

Rollback should create a new higher-version document snapshot copied from an
older retained snapshot. Versions never decrement.

## Failure Matrix

| Failure | Recovery |
| --- | --- |
| API crashes before PG commit | No document change and no outbox row exist. Client retries. |
| API crashes after PG commit before response | Document change and outbox row are durable. Client retry sees current version/idempotency result. |
| Publisher crashes before NATS publish | Row remains pending/publishing until lock expires; another publisher retries. |
| Publisher crashes after NATS PubAck before marking published | Row republishes with same `Nats-Msg-Id`; NATS/projections dedupe. |
| NATS consumer crashes after side effect before ACK | JetStream redelivers; projection version guards converge. |
| Newer document version overtakes old work | Old worker observes newer version or fails projection guard and ACKs skip. |
| Upload complete loses race with abort/timeout | KV gate blocks completion; pending S3 object is deleted or lifecycle-cleaned. |
| Timeout scheduler republishes old timeout | Timeout handler reads KV gate and no-ops if the job advanced. |

## When To Move Beyond This

The PG outbox design was not selected. Move to retained NATS
snapshots or EventStoreDB only if PG CRUD throughput, audit/replay, or
cross-projection rebuild requirements justify the additional infrastructure.
