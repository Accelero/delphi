---
title: Document CRUD Pipeline
description: Alternative NATS-first document CRUD architecture using async projections.
---

# Document CRUD Pipeline

> **Not the selected design.** The document architecture was settled in favour
> of an event-sourced model on NATS JetStream — see
> [Document Upload and Lifecycle](../architecture/document-upload). This page is
> retained as a record of the design space that was explored.

This page records an early NATS event-first CRUD pipeline sketch.

Document handling in this variant is a NATS event-first CRUD system with ingestion as the
`create` path. API commands publish durable NATS events. Postgres, S3 state,
Qdrant, NebulaGraph, and realtime WebSocket fanout are projections or side
effects of those events.

The architecture for this variant is:

```text
NATS JetStream = durable command/work delivery, redelivery, dedupe, fanout
NATS KV        = optional short-lived saga/ingestion state
Postgres       = document read model, status, ingestion outputs, repair state
S3             = immutable object bytes and derived object artifacts
Qdrant         = selected vector projection
NebulaGraph    = selected graph projection
```

CRUD success means the command event is durable in NATS. The user-visible
finished state is the converged Postgres/Qdrant/NebulaGraph/S3 projection
state. Immediate PG read-after-write is not a core invariant.

## Production Shape

```text
Browser / API client
  +- CRUD/status/WebSocket --------------------> api-service / realtime-service
  `- direct PUT/GET for bytes -----------------> S3-compatible object store

api-service
  +- validates tenant/user access
  +- creates ids and presigned S3 operations
  +- publishes document command events to NATS JetStream
  `- returns after PubAck

NATS JetStream
  +- owns command durability and work fanout
  +- redelivers unacked work
  `- dedupes deterministic Nats-Msg-Id values

workers
  +- consume NATS messages with explicit ACK
  +- update PG document/status projection
  +- write idempotent S3/Qdrant/NebulaGraph outputs
  +- publish next NATS work events
  `- ACK only after durable progress and required PubAck

realtime-service
  +- watches NATS KV job progress snapshots
  +- may read PG for status snapshots
  `- fans out authorized updates to browsers
```

## Projection Stores

Start with NATS, PG, S3, and realtime. Add Qdrant and NebulaGraph as projection
workers after the canonical CRUD path and PG ingestion outputs are stable.

| Projection | Initial status | Output |
| --- | --- | --- |
| `pg_document` | enabled | document row, status, object metadata |
| `pg_content` | enabled | validation, extracted text, chunks, ingestion status |
| `ui_status` | enabled | live CRUD and ingestion status snapshots in NATS KV |
| `qdrant_vectors` | disabled until configured | chunk vectors in Qdrant |
| `nebulagraph_graph` | disabled until configured | document/entity graph in NebulaGraph |

Projection workers are independent. Qdrant and NebulaGraph can run in parallel
once the PG content/chunk/entity prerequisites exist.

Qdrant is written in Rust and exposes REST/gRPC APIs with a Qdrant-specific
structured query DSL. Its storage model uses collection segments, WAL-backed
durability, HNSW indexes, and in-memory or memory-mapped vector storage.

NebulaGraph is mostly C++. Its storage service uses a custom distributed
KVStore over RocksDB with Multi Group Raft. Its query language is nGQL:
NebulaGraph-specific, SQL-like, and partially openCypher-compatible.

## State Ownership

| State | Owner |
| --- | --- |
| Command durability and redelivery | NATS JetStream |
| Saga/ingestion lifetime state | NATS KV workflow gate and job progress snapshot |
| Document status/read model | Postgres projection |
| Extracted text/chunks/entities | Postgres projection |
| Object bytes | S3-compatible storage |
| Vector search index | Qdrant projection |
| Graph index | NebulaGraph projection |
| Browser live status | Realtime service watches NATS KV and fans out over WebSocket |

Postgres does not start the command. It reflects the current known document
state as consumers process NATS events.

## Core PG Tables

The exact SQL can evolve, but these ownership boundaries should remain stable.

```sql
CREATE TABLE document (
  tenant_id uuid NOT NULL,
  document_id uuid NOT NULL,
  owner_user_id uuid NOT NULL,
  state text NOT NULL CHECK (
    state IN ('creating', 'active', 'deleting', 'deleted', 'tombstoned', 'failed')
  ),
  title text,
  metadata jsonb NOT NULL DEFAULT '{}',
  object_key text,
  object_etag text,
  object_size_bytes bigint,
  content_sha256 text,
  content_type text,
  ingestion_state text NOT NULL DEFAULT 'uploaded' CHECK (
    ingestion_state IN ('uploaded', 'validating', 'extracting', 'chunking',
                        'ready', 'failed')
  ),
  document_version bigint NOT NULL DEFAULT 0,
  last_event_id uuid,
  last_correlation_id uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  ready_at timestamptz,
  failed_at timestamptz,
  error_code text,
  error_message text,
  deleted_at timestamptz,
  PRIMARY KEY (tenant_id, document_id)
);

CREATE TABLE document_upload_session (
  tenant_id uuid NOT NULL,
  upload_id uuid NOT NULL,
  owner_user_id uuid NOT NULL,
  document_id uuid,
  idempotency_key text NOT NULL,
  object_key text NOT NULL,
  state text NOT NULL CHECK (
    state IN ('initiated', 'uploading', 'completing', 'completed',
              'aborting', 'aborted', 'expired')
  ),
  expected_size_bytes bigint,
  expected_content_type text,
  s3_upload_id text,
  completed_at timestamptz,
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, upload_id),
  UNIQUE (tenant_id, owner_user_id, idempotency_key)
);
```

`document_upload_session` is a PG projection of upload command/status events.
The API may generate upload ids and presigned URLs before this row exists; the
projection should converge quickly from NATS.

Derived PG tables such as `document_validation`, `document_content`, and
`document_chunk` store only current content. They use deterministic keys:

```text
tenant_id + document_id
tenant_id + document_id + chunk_ordinal
```

Each row stores the source object key/hash and source aggregate version that
produced it. Duplicate or stale events become no-ops.

## Event Families

Primary command events:

```text
document.upload_started
document.upload_completed
document.upload_abort_requested
document.upload_timeout_requested
document.snapshot_upsert_requested
document.snapshot_delete_requested
document.reindex_requested
```

Internal work events:

```text
document.ingest.validate_requested
document.ingest.extract_requested
document.ingest.chunk_requested
document.ingest.ready
document.ingest.failed
```

Projection work events:

```text
document.vector.qdrant.upsert_requested
document.vector.qdrant.delete_requested
document.graph.nebula.upsert_requested
document.graph.nebula.delete_requested
document.projection.applied
document.projection.failed
```

Optional durable UI/progress events:

```text
document.ui.changed
document.ui.ingestion_progress
document.ui.projection_progress
document.ui.failed
```

Every event has a stable id, document/aggregate version, causation id,
correlation id, tenant id, document id, and event contract version.

## Snapshot CRUD Contract

Document CRUD commands are snapshot commands, not patch commands. The command
event contains the complete desired PG `document` row state for the target
version. The last accepted snapshot is enough to recreate the current document
metadata without replaying earlier document events.

Clients do not choose the outcome version. Mutating requests target the version
the client read:

```text
expected_document_version = current version from GET /api/documents/:document_id
```

The API or command builder assigns:

```text
target_document_version = expected_document_version + 1
```

Create is the only command without an existing version; it requires that the
document does not already exist and assigns the first server-side version.
Updates and deletes must target an existing, non-deleted document. CRUD against
deleted/tombstoned documents is rejected. Restore and purge are intentionally
not part of the current API.

Snapshot upsert event shape:

```json
{
  "event_type": "document.snapshot_upsert_requested",
  "expected_document_version": 6,
  "target_document_version": 7,
  "document": {
    "tenant_id": "uuid",
    "document_id": "uuid",
    "owner_user_id": "uuid",
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

Snapshot delete event shape:

```json
{
  "event_type": "document.snapshot_delete_requested",
  "expected_document_version": 7,
  "target_document_version": 8,
  "document": {
    "tenant_id": "uuid",
    "document_id": "uuid",
    "owner_user_id": "uuid",
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

PG applies a snapshot only when the version rule matches. Derived projections
are regenerated from the snapshot when source fields relevant to that
projection changed. Older in-flight jobs can skip expensive work when a newer
snapshot has already superseded them.

## API Contract

Document commands use explicit REST routes. Command routes return once NATS
has acknowledged the durable command event.

| Operation | Route | Durable result |
| --- | --- | --- |
| Start create upload | `POST /api/documents/uploads` | Publishes `document.upload_started` and returns presigned upload data. |
| Complete upload | `POST /api/documents/uploads/:upload_id/complete` | Publishes `document.upload_completed`; the upload worker validates S3 and emits the final snapshot upsert. |
| Abort upload | `DELETE /api/documents/uploads/:upload_id` | Publishes upload abort command. |
| List documents | `GET /api/documents` | Reads authorized document summaries from PG projection. |
| Get document | `GET /api/documents/:document_id` | Reads canonical document detail from PG projection. |
| Get status | `GET /api/documents/:document_id/status` | Reads ingestion/projection status from PG projection. |
| Get bytes URL | `GET /api/documents/:document_id/view-url` | Returns a short-lived S3 URL after authorization. |
| Update metadata | `PATCH /api/documents/:document_id` | Publishes full-row snapshot upsert command targeting the current document version. |
| Start replacement upload | `POST /api/documents/:document_id/uploads` | Publishes replacement-upload command and returns presigned upload data. |
| Delete document | `DELETE /api/documents/:document_id` | Publishes full-row snapshot delete command targeting the current document version. |
| Reindex document | `POST /api/documents/:document_id/reindex` | Publishes reindex command. |

Read routes are eventually consistent because they read PG projections.
Browsers should combine WebSocket updates with status reloads on reconnect.

## Create And S3 Ingestion

Create starts one document job lifecycle. Upload, ingestion, projection, and
live progress share the same NATS KV gate:

```text
bucket: document_jobs
key: jobs.<tenant_id>.<document_id>.<job_id>
```

The upload timeout index is separate because KV cannot query inside values by
`expires_at`:

```text
bucket: document_job_timeouts
key: upload_timeouts.<epoch_minute>.<tenant_id>.<document_id>.<job_id>
value: { tenant_id, document_id, job_id, timeout_event_id }
```

The document job gate uses `stage` as the next uncompleted stage:

```text
awaiting_upload
  -> completing_upload -> validating -> extracting -> chunking -> projecting -> ready
  -> aborting_upload   -> aborted
  -> expiring_upload   -> expired
  -> failed
```

`uploaded` is not required as a separate gate state. `completing_upload` is
the crash-recovery stage between winning the upload race and making the object
safe for ingestion.

1. `POST /api/documents/uploads`
   - API authorizes the user.
   - API creates `upload_id`, `document_id`, `job_id`, object key, and
     `expires_at`.
   - API idempotently ensures the configured S3 bucket exists; buckets are not
     created per upload.
   - API publishes `document.upload_started` and waits for `PubAck`.
   - API returns short-lived presigned S3 upload URL(s), for example five
     minutes.

2. `document.upload_started` worker:
   - creates the document job KV key with create-if-absent/CAS;
   - sets `stage = awaiting_upload`;
   - stores object key, upload id, owner, expected metadata, `expires_at`, and
     deterministic next event ids;
   - writes the timeout index key for the expiration bucket;
   - updates the live progress snapshot.

3. Browser uploads bytes directly to S3.
   - The upload URL writes the object with `upload_state=pending` metadata or
     tags when supported.
   - MinIO lifecycle deletes `upload_state=pending` objects after the cleanup
     window, for example one day. This is the final safety net, not the
     user-visible timeout mechanism.

4. `POST /api/documents/uploads/:upload_id/complete`
   - API authorizes the user.
   - API may perform a light S3 `HEAD` to fail obvious missing-object requests
     early.
   - API publishes `document.upload_completed` and returns after `PubAck`.

5. `document.upload_completed` worker:
   - reads the document job KV gate;
   - if `stage = awaiting_upload`, CASes it to `completing_upload`;
   - if the gate is `expiring_upload`, `expired`, `aborting_upload`, or
     `aborted`, it does not resurrect the upload;
   - validates S3 `HEAD`, content type, size, checksum/etag as available;
   - changes object metadata/tag from `upload_state=pending` to
     `upload_state=committed` before any document snapshot can reference it;
   - builds the complete desired PG `document` row snapshot with the new S3
     object pointer and server-assigned `target_document_version`;
   - publishes `document.snapshot_upsert_requested` with deterministic
     `Nats-Msg-Id`;
   - deletes the timeout index key best-effort;
   - CASes the KV gate from `completing_upload` to `validating` or the next
     ingestion stage;
   - updates the live progress snapshot;
   - continues the ingestion pipeline or publishes the validate wakeup.

The upload job is a pre-stage for content create/replace. It prepares the S3
object and emits the final snapshot CRUD event; the document source state does
not change before that snapshot event is durably published and applied.

6. Timeout scheduler:
   - wakes on a fixed cadence, for example once per minute;
   - lists due keys in `document_job_timeouts` by bucket:
     `upload_timeouts.<epoch_minute>.>`;
   - publishes `document.upload_timeout_requested` with deterministic
     `Nats-Msg-Id`;
   - deletes the timeout index key after `PubAck`, or leaves it for bucket TTL
     if deletion fails.

7. `document.upload_timeout_requested` worker:
   - reads the document job KV gate;
   - if the gate already advanced past `awaiting_upload`, it ACKs;
   - if `stage = awaiting_upload`, CASes it to `expiring_upload`;
   - deletes the S3 object if present;
   - CASes the gate to `expired`;
   - updates live progress and ACKs.

8. `DELETE /api/documents/uploads/:upload_id`
   - API authorizes the user.
   - API publishes `document.upload_abort_requested`.

9. `document.upload_abort_requested` worker:
   - if `stage = awaiting_upload`, CASes it to `aborting_upload`;
   - deletes the S3 object if present;
   - deletes the timeout index key best-effort;
   - CASes the gate to `aborted`;
   - updates live progress and ACKs.

10. Ingestion stages continue from `validating`:
   - validate reads S3 `HEAD` and bounded object ranges;
   - extract writes current content;
   - chunk writes deterministic chunks;
   - projection stages run enabled PG/Qdrant/NebulaGraph work.

11. Chunk/ready stage sets `document.ingestion_state = ready`,
   `document.state = active`, publishes UI progress, and publishes Qdrant and
   NebulaGraph upsert work for enabled projections.

Crash gap handling:

- If API crashes before command `PubAck`, the client retries with the same
  idempotency key and deterministic event id.
- If API crashes after `PubAck` before HTTP response, the command is durable;
  retry dedupes or the PG status projection catches up.
- If a worker crashes after side effects but before ACK, NATS redelivers and
  idempotent projection writes converge.
- If timeout/abort wins the upload gate, complete cannot resurrect the upload.
- If complete wins the upload gate, timeout/abort events become no-ops for the
  upload and later user intent must use document delete.

## Metadata Update

1. `PATCH /api/documents/:document_id`
   - API authorizes tenant/user access.
   - API validates the requested metadata state.
   - API loads or receives the client-targeted `expected_document_version`.
   - API builds the complete desired PG `document` row snapshot.
   - API assigns `target_document_version = expected_document_version + 1`.
   - API publishes `document.snapshot_upsert_requested`.

2. PG document projector applies the full-row snapshot only if the target
   document exists, is not deleted/tombstoned, and the version rule matches.

3. Projection workers regenerate only projections affected by changed source
   fields.

## Content Replacement

Content replacement overwrites the current content pointer for the same
`document_id`. Previous content snapshots and S3 object pointers can be
retained by snapshot-history policy for rollback.

1. `POST /api/documents/:document_id/uploads`
   - API verifies the target document exists and is not deleted/tombstoned.
   - API records the client-targeted `expected_document_version`.
   - API publishes `document.upload_started` and returns presigned URLs.

2. Browser uploads the new bytes to S3.

3. `POST /api/documents/uploads/:upload_id/complete`
   - API publishes `document.upload_completed`.

4. The upload-complete worker validates/tags S3, builds the complete document
   snapshot with the new object pointer, assigns the server-side target version,
   and publishes `document.snapshot_upsert_requested`.

5. Ingestion and projection workers rebuild content, chunks, Qdrant, and
   NebulaGraph from the snapshot source fields when those fields changed.

Older S3 objects are retained while referenced by retained snapshots. Snapshot
retention cleanup can delete unreferenced old objects later.

## Delete And Reindex

Delete is a snapshot state transition. It targets an existing, non-deleted
document version and emits a full-row tombstone snapshot.

1. `DELETE /api/documents/:document_id`
   - API authorizes tenant/user access.
   - API verifies the target document exists and is not already deleted or
     tombstoned.
   - API assigns `target_document_version = expected_document_version + 1`.
   - API publishes `document.snapshot_delete_requested`.

2. PG applies the full-row delete snapshot if the version rule matches.

3. Qdrant and NebulaGraph delete/tombstone derived rows by `tenant_id +
   document_id` or advance their projection state to the delete snapshot
   version.

Restore and purge are intentionally omitted for now. They can be added later
as explicit operations with their own version rules.

Reindex is explicit repair work:

```text
POST /api/documents/:document_id/reindex
-> document.reindex_requested
-> selected projection workers rebuild from PG/S3
```

## Worker Stage Rule

For stages that persist progress, the durable rule is:

```text
consume NATS event
-> read JetStream delivery metadata
-> if delivery count is low, run idempotent work optimistically before a gate
   pre-check
-> if delivery count is elevated, read the NATS KV gate before heavy work
-> if KV already advanced past this stage, republish the deterministic next
   wakeup and ACK
-> if delivery count exceeds the stage retry budget, CAS the KV gate to a
   terminal/compensating state before writing terminal PG state
-> otherwise run or rerun the idempotent side effect
-> update PG projection/status state with CAS/version guards
-> advance NATS KV workflow gate with CAS; if it already advanced, republish
   the deterministic next wakeup; if it is blocked, do not resurrect
-> publish next NATS wakeup with deterministic Nats-Msg-Id
-> ACK current NATS message
```

NATS owns workflow progression and retry accounting. Workers do not persist
per-event attempt rows in Postgres. They use JetStream delivery count as the
single retry counter for a work message. NATS KV stores the workflow gate.
The gate is defined as the next uncompleted stage. If `stage = extract`, the
extract stage has not passed yet. A
successful extract worker advances the gate from `extract` to `chunk` with
CAS. PG remains the read model; it is not queried as the workflow checkpoint.

Workers use the gate differently based on delivery count and work cost. Early
deliveries can do cheap idempotent work optimistically before pre-checking the
gate, then attempt to advance the gate with CAS. If that CAS fails because the
gate already advanced, the worker republishes the deterministic next wakeup and
ACKs. If the CAS fails because the gate is blocked or terminal, the worker does
not publish success or resurrect PG state. On elevated redelivery counts,
workers should check the KV gate before expensive work. If the gate already
advanced past the event's stage, the worker does not redo the heavy work; it
republishes the deterministic next wakeup and ACKs.

When a redelivered message finds KV already advanced past its stage, the worker
must still republish the deterministic next wakeup before ACKing. This closes
the crash gap where the previous delivery updated PG and/or KV but died before
publishing the next event. NATS duplicate detection makes the republish safe.

When a delivery exceeds the configured stage retry budget, the worker enters
abort mode. It first attempts to block the gate by CASing KV from the current
stage to a terminal or compensating state such as `failed` or
`compensating`. If that CAS fails because another worker already advanced the
gate, the stage succeeded first; the worker republishes the deterministic next
wakeup and ACKs. If the CAS succeeds, late workers can no longer pass the
gate. The aborting worker then force-writes the terminal PG state for the
document/projection, such as `failed` or `tombstoned` depending on the workflow,
publishes `document.ingest.failed`, `document.projection.failed`, or the next
compensation event with a deterministic `Nats-Msg-Id`, publishes UI failure
status as needed, and ACKs the exhausted message.

Late workers that complete after the gate is blocked must fail their KV CAS.
Their PG writes must also be guarded so terminal document states cannot be
resurrected by stale success writes.

Consumer `MaxDeliver` must be configured high enough that the worker receives
the terminal delivery and can perform this semantic failure transition. There
are no compensation steps for now; failures are reflected in PG
document/projection state.

The document job KV value is intentionally minimal. It contains the gate,
routing ids, the operation kind for realtime/UI interpretation, and upload
fields needed for S3 actions:

```json
{
  "schema_version": 1,
  "tenant_id": "uuid",
  "document_id": "uuid",
  "job_id": "uuid",
  "operation": "create",
  "stage": "awaiting_upload",
  "upload_id": "uuid",
  "object_key": "documents/<tenant_id>/<document_id>/original"
}
```

For non-upload operations, upload-specific fields are omitted:

```json
{
  "schema_version": 1,
  "tenant_id": "uuid",
  "document_id": "uuid",
  "job_id": "uuid",
  "operation": "reindex",
  "stage": "validating"
}
```

Valid `operation` values include:

```text
create
replace_content
reindex
delete
```

Valid upload/ingestion stages include:

```text
awaiting_upload
completing_upload
validating
extracting
chunking
projecting
ready
aborting_upload
aborted
expiring_upload
expired
failed
```

The timeout index value is only a pointer back to the document job:

```json
{
  "tenant_id": "uuid",
  "document_id": "uuid",
  "job_id": "uuid"
}
```

The timeout event id is deterministic from the job id and timeout bucket; it
does not need to be stored in KV.

## Projection State

Use PG only for current projection status. Per-message retry count and delivery
attempts remain NATS transport state, not document state.

```sql
CREATE TABLE document_projection_state (
  tenant_id uuid NOT NULL,
  document_id uuid NOT NULL,
  projection_name text NOT NULL,
  status text NOT NULL CHECK (
    status IN ('disabled', 'pending', 'running', 'applied', 'stale', 'failed')
  ),
  last_applied_version bigint NOT NULL DEFAULT 0,
  last_event_id uuid,
  last_error_code text,
  last_error_message text,
  updated_at timestamptz NOT NULL DEFAULT now(),
  applied_at timestamptz,
  failed_at timestamptz,
  PRIMARY KEY (tenant_id, document_id, projection_name)
);
```

This table records the latest known projection state and supports status
queries and repair. It is not a retry ledger; NATS owns delivery and retry
counts.

## Qdrant Vector Projection

Qdrant uses deterministic point ids and payload version guards:

```text
point_id = tenant_id:document_id:chunk_ordinal:model
payload.projection_version = target_document_version
```

The worker applies a point only when the existing payload has no
`projection_version` or has `projection_version < target_document_version`.
Equal versions are no-ops. Greater versions make the event stale.

## NebulaGraph Graph Projection

NebulaGraph uses deterministic vertex/edge ids and version properties:

```ngql
UPDATE VERTEX $vertex_id
SET entity.projection_version = $target_document_version
WHEN entity.projection_version < $target_document_version
YIELD entity.projection_version;
```

## UI Live Updates

Browsers do not connect to NATS directly. The realtime service authorizes
WebSocket subscriptions, watches NATS KV job state keys, and pushes stage
snapshots to subscribed browsers. PG remains the durable read model for API
reloads and completed document status; NATS KV is the short-lived live job
state.

The pipeline worker updates the same KV value used as the workflow gate after
each visible stage. Realtime derives the user-visible label from `operation`
and `stage`:

```json
{
  "schema_version": 1,
  "tenant_id": "uuid",
  "document_id": "uuid",
  "job_id": "uuid",
  "operation": "create",
  "stage": "extracting",
  "upload_id": "uuid",
  "object_key": "documents/<tenant_id>/<document_id>/original"
}
```

Richer progress fields such as percent or message can be added later, but are
not part of the first KV schema.

KV keys are scoped per job and expire after completion or terminal failure:

```text
bucket: document_jobs
key: jobs.<tenant_id>.<document_id>.<job_id>
```

The realtime service may watch tenant/document/job key prefixes and filter
updates against authorized WebSocket subscriptions. On reconnect, the browser
loads the latest snapshot from KV if the job is still live, or from PG if the
job has completed and the KV key expired.

Optional JetStream progress events are only needed when Delphi needs a durable
progress timeline, audit trail, or replayable UI history:

```text
documents.progress.v1.tenant.<tenant_id>.document.<document_id>.job.<job_id>
```

By default, do not create one stream per job. Use one shared progress stream
with wildcard subjects if history is required. Otherwise, KV watch is the
notification source of truth for live progress.

## Failure Matrix

| Crash or race point | Durable state after failure | Recovery behavior |
| --- | --- | --- |
| API crashes before command PubAck | Command may not be durable. | Client retries with same idempotency key/event id. |
| API crashes after PubAck before HTTP response | Command is durable in NATS. | Retry dedupes or PG projection catches up. |
| Browser uploads bytes but never completes | S3 object may exist with `upload_state=pending`. | Timeout event moves the job to `expired` and deletes the object best-effort; MinIO lifecycle deletes pending objects as final fallback. |
| Worker crashes before ACK | NATS message is unacked. | JetStream redelivers. |
| Worker crashes after side effect before PG update | Partial deterministic output may exist. | Redelivery repeats idempotent upsert/delete. |
| Worker crashes after PG update before next wakeup | Projection state is durable; next work may be missing. | Redelivery or repair publisher emits deterministic wakeup. |
| Duplicate delivery | Same event may run twice. | CAS/idempotent writes make one path a no-op. |
| Worker receives terminal retry delivery | Worker skips normal side effect, marks projection/document state `failed`, publishes failure event, and ACKs. | UI reflects failed state; repair command can be published. |
| Realtime service crashes | NATS and PG projection remain. | Browser reconnects or reloads status. |

## Implementation Requirements

1. NATS streams for document commands, work, projections, and optional durable
   progress events.
2. API command publishers using deterministic event ids and `PubAck`.
3. PG document/status projection workers.
4. S3 direct upload API with idempotency keys, upload aborts, timeout events,
   and MinIO lifecycle fallback for pending objects.
5. PG/S3 ingestion workers for validate, extract, and chunk.
6. Projection worker interfaces for Qdrant vectors and NebulaGraph graph data.
7. Realtime service that fans authorized status events to WebSocket clients.
8. NATS KV workflow gate and live job progress state for ingestion,
   projections, and repair workflows.
9. Repair workflows for replaying/recreating work events, reindexing a
   document, and clearing terminal projection failures.
