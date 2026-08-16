---
title: Document Event Sourcing
description: Full EventStoreDB-backed document source-of-truth design with NATS work fanout.
---

# Document Event Sourcing

> **Not the selected design.** The document architecture was settled in favour
> of an event-sourced model on NATS JetStream — see
> [Document Upload and Lifecycle](../architecture/document-upload). This page is
> retained as a record of the design space that was explored.

This page describes the full event-sourcing architecture for document CRUD if
Delphi moves beyond retained snapshot commands. EventStoreDB owns document
truth, per-document ordering, and expected-version CAS. NATS owns delivery to
workers, redelivery, retries, and fanout. NATS KV owns short-lived live job
state and WebSocket progress snapshots.

This is a stronger model than the JetStream-based design that was adopted. Use it only
when audit-grade event history, replay, aggregate rebuilds, and projection
watermarks are worth the extra operational complexity.

## Ownership

| State | Owner |
| --- | --- |
| Document aggregate truth | EventStoreDB document stream |
| Document version/CAS | EventStoreDB expected revision |
| Worker fanout and retries | NATS JetStream |
| Live job gate and realtime state | NATS KV |
| Current document read model | Postgres projection |
| Object bytes | S3-compatible storage |
| Vector index | Qdrant projection |
| Graph index | NebulaGraph projection |
| Projection checkpoints | Postgres or projection-owned durable store |
| User annotations | Postgres CRUD, version-bound to document version |

Postgres is no longer the document source of truth in this model. It is a
rebuildable projection from EventStoreDB.

## Streams

Each document has one aggregate stream:

```text
document.<tenant_id>.<document_id>
```

The stream revision is the document version. A command targeting version `6`
appends with expected revision `6`. If the append succeeds, EventStoreDB
commits the next event at version `7`. If the expected revision does not
match, the command is stale or the document is busy/deleted according to the
aggregate state.

Optional aggregate snapshots can be stored in separate streams or tables for
faster command handling:

```text
document_snapshot.<tenant_id>.<document_id>
```

Snapshots are an optimization. The event stream remains authoritative.

## Event Types

Document events are domain facts. They may carry full document-row snapshots
when that keeps projection rebuilds simple, but they are still appended to the
ordered aggregate stream.

Initial event set:

```text
document.created
document.metadata_replaced
document.content_replaced
document.deleted
document.reindex_requested
document.job_failed
```

Restore and purge are intentionally omitted for now. CRUD against
deleted/tombstoned documents is rejected.

Example content replacement event:

```json
{
  "event_id": "uuid",
  "event_type": "document.content_replaced",
  "tenant_id": "uuid",
  "document_id": "uuid",
  "document_version": 7,
  "actor_user_id": "uuid",
  "correlation_id": "uuid",
  "occurred_at": "2026-06-15T12:00:00Z",
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

For metadata-only changes, the event can still carry the complete current
document snapshot. That makes PG recovery possible from the latest aggregate
snapshot plus later events, and keeps projection handlers simple.

## Command Flow

Document CRUD commands append to EventStoreDB first. NATS receives committed
events from a relay/subscription after EventStoreDB accepts the write.

```text
HTTP command
-> authorize and validate
-> load aggregate state from EventStoreDB snapshot + tail events
-> reject if deleted/tombstoned or stale
-> append event with expected revision
-> EventStoreDB commits event and assigns stream revision
-> relay publishes committed event to NATS
-> projections and workers converge
```

The API should not let clients set the outcome version. Clients provide the
version they read:

```text
expected_document_version = current document stream revision
```

EventStoreDB assigns the next revision by accepting the append.

Create uses expected revision "no stream" or "stream does not exist". Update
and delete require the current expected revision and a non-deleted aggregate
state.

## EventStoreDB To NATS Relay

A relay consumes committed EventStoreDB events and publishes them to NATS
JetStream with deterministic message ids:

```text
EventStoreDB committed event
-> relay publish documents.events.v1.<event_type>
-> Nats-Msg-Id = event_id
-> wait for PubAck
-> advance relay checkpoint
```

NATS subjects:

```text
documents.events.v1.document.created
documents.events.v1.document.metadata_replaced
documents.events.v1.document.content_replaced
documents.events.v1.document.deleted
documents.events.v1.document.reindex_requested
documents.events.v1.document.job_failed
```

The relay checkpoint stores the last EventStoreDB commit position published to
NATS. If the relay crashes after publishing but before checkpointing, it
republishes with the same `Nats-Msg-Id`; JetStream dedupe and projection
idempotency make that safe.

## Projection Watermarks

Every projection stores its own replay checkpoint:

```sql
CREATE TABLE projection_checkpoint (
  projection_name text PRIMARY KEY,
  eventstore_commit_position text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
```

Projection outputs also record the source document version they reflect:

```text
PG document.document_version
PG content.source_document_version
PG chunk.source_document_version
Qdrant payload.projection_version
NebulaGraph projection_version
document_projection_state.last_applied_document_version
```

Recovery:

```text
projection starts from checkpoint
-> reads EventStoreDB or NATS replay stream
-> applies idempotent writes
-> advances checkpoint after durable output
```

For a full rebuild, clear projection output and replay EventStoreDB events from
the beginning or from aggregate snapshots.

## Upload Pre-Stage

S3 upload is not the document source-of-truth event. It is a pre-stage that
produces an immutable object reference. The document changes only after the
upload completes and EventStoreDB accepts the resulting document event.

```text
POST /api/documents/uploads
-> API creates document_id/job_id/upload_id/object_key
-> API publishes document.upload_started to NATS
-> API returns short-lived presigned S3 URL
```

NATS KV document job:

```json
{
  "schema_version": 1,
  "tenant_id": "uuid",
  "document_id": "uuid",
  "job_id": "uuid",
  "operation": "create",
  "stage": "awaiting_upload",
  "upload_id": "uuid",
  "object_key": "documents/<tenant_id>/<document_id>/versions/pending/original"
}
```

Timeout index:

```text
bucket: document_job_timeouts
key: upload_timeouts.<epoch_minute>.<tenant_id>.<document_id>.<job_id>
```

Complete:

```text
POST /api/documents/uploads/:upload_id/complete
-> API publishes document.upload_completed to NATS
-> upload worker CASes KV awaiting_upload -> completing_upload
-> validates S3 HEAD/checksum/content type
-> tags object upload_state=committed
-> appends document.created or document.content_replaced to EventStoreDB
   with expected revision
-> CASes KV to validating or failed/stale
```

If the append fails due to expected-revision conflict, the job is stale. The
worker marks the KV job failed/stale, emits a realtime update, and leaves the
existing document aggregate unchanged.

Abort and timeout:

```text
awaiting_upload -> aborting_upload -> aborted
awaiting_upload -> expiring_upload -> expired
```

Abort/timeout delete the pending S3 object best-effort. MinIO lifecycle deletes
`upload_state=pending` objects after the configured cleanup window as final
fallback.

## NATS KV Job Gate

NATS KV remains short-lived workflow state. It is not the document source of
truth and is not replayed for document recovery.

Valid stages:

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
stale
```

Workers use the same gate rules as the NATS-first design:

```text
read delivery count
if low and work is cheap, work optimistically
if redelivered or work is heavy, check KV gate first
if gate advanced, republish missing deterministic next event and ACK
if retry budget exhausted, CAS gate to failed/compensating before terminal writes
```

## Projection Flow

Committed document events drive projections:

```text
EventStoreDB document event
-> NATS relay
-> PG projection updates current document row
-> content extraction/chunking if object fields changed
-> Qdrant/Nebula projection if derived inputs changed
-> realtime KV state updated for live UI
```

Projection writes are idempotent:

```text
apply if incoming document_version > stored projection_version
no-op if equal
skip if older
```

Qdrant and NebulaGraph are not part of the EventStoreDB append transaction.
They eventually converge to the committed document version. The UI can show
projection lag from `document_projection_state`.

## Snapshot Aggregation

EventStoreDB replay from the beginning can be expensive for long-lived
documents. Store aggregate snapshots periodically:

```text
snapshot stream: document_snapshot.<tenant_id>.<document_id>
snapshot payload:
  document_version
  document state
  last_event_id
  last_commit_position
```

Command handlers load:

```text
latest aggregate snapshot
-> events after snapshot
-> current aggregate state
```

Projection watermarks are separate from aggregate snapshots. A projection
checkpoint says how far one read model has processed. An aggregate snapshot
says how to load one document aggregate quickly.

## Rollback

Rollback is a new command, not replaying an old event as if it happened now.

```text
current document version = 12
operator chooses state from version 8
append document.content_replaced or document.metadata_replaced at version 13
payload copies the version 8 desired state/object pointer
```

Versions remain monotonic. S3 objects referenced by retained or rolled-back-to
states must not be deleted.

## Annotations And User Artifacts

User annotations are not part of the document aggregate stream. They are
user-specific CRUD state scoped by:

```text
tenant_id + document_id + user_id
```

Store annotations in Postgres:

```text
document_annotation(
  tenant_id,
  document_id,
  user_id,
  annotation_id,
  document_version,
  status,
  anchor_json,
  body_json
)
```

When a document advances, annotations for older versions become stale or can be
reanchored asynchronously. Versions/S3 objects with live annotations should not
be purged unless the annotations are archived or deleted by policy.

## Failure Behavior

| Failure | Recovery |
| --- | --- |
| API crashes before EventStoreDB append | No source event exists. Client retries with same idempotency key. |
| API crashes after EventStoreDB append before response | Event is committed. Retry detects duplicate command/event id or current version. |
| Relay crashes after NATS PubAck before checkpoint | Relay republishes with same `Nats-Msg-Id`; NATS/projections no-op duplicates. |
| Worker crashes after projection write before ACK | NATS redelivers; projection version guards converge. |
| Upload complete append conflicts | Job is stale; KV moves to failed/stale; document aggregate unchanged. |
| Projection falls behind | Replay from projection checkpoint or rebuild from EventStoreDB. |

## When To Use This

Use this EventStoreDB design if Delphi needs:

- audit-grade document history;
- exact replay/debugging of document aggregate state;
- rebuildable projections from a durable event store;
- expected-revision CAS outside Postgres;
- long-lived event retention beyond NATS work-stream retention.

If the latest document state plus bounded rollback history is enough, the
snapshot-first JetStream design is simpler.
