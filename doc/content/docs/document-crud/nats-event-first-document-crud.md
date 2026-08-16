---
title: NATS Event-First Document CRUD
description: Durable NATS command/event publishing for document CRUD.
---

# NATS Event-First Document CRUD

This page records the NATS event-first design, whose approach was ultimately
adopted — see [Document Upload and Lifecycle](../architecture/document-upload)
for the design as built. Document CRUD commands start by publishing durable NATS
JetStream events. There
is no PG transactional outbox or WAL CDC publisher in this variant.
Postgres is a projection/read model that consumes the same document events as
Qdrant, NebulaGraph, ingestion workers, and realtime fanout.

This gives one systematic model:

```text
API command -> NATS durable event -> projections converge
```

Delphi is using event-sourcing mechanics without committing to a permanent
application event store. JetStream is the operational event/work stream with
retention, replay, redelivery, and dedupe. Long-term document truth is the
converged projection state in Postgres/S3/Qdrant/NebulaGraph.

## API Publish Rule

Every document command follows this pattern:

```text
authorize request
validate command shape
assign event_id, document_id, correlation ids, and server-side target version
publish to NATS JetStream
wait for PubAck
return accepted
```

The API does not synchronously mutate the Postgres document row as the command
commit. A successful API response means the command event is durable in NATS.
The Postgres projection catches up asynchronously, normally fast enough for UI
status to update through WebSocket events or a status reload.

## Command Subjects

Primary command events use `DOC_COMMANDS`:

```text
documents.commands.v1.upload.started
documents.commands.v1.upload.completed
documents.commands.v1.upload.abort
documents.commands.v1.upload.timeout
documents.commands.v1.reindex
```

Full document snapshot CRUD events use `DOC_SNAPSHOTS`:

```text
documents.snapshots.v1.<tenant_id>.<document_id>.upsert
documents.snapshots.v1.<tenant_id>.<document_id>.delete
```

Work and projection events use `DOC_EVENTS`:

```text
documents.work.v1.ingest.validate
documents.work.v1.ingest.extract
documents.work.v1.ingest.chunk
documents.work.v1.vector.qdrant.upsert
documents.work.v1.vector.qdrant.delete
documents.work.v1.graph.nebula.upsert
documents.work.v1.graph.nebula.delete
```

Live UI progress uses NATS KV job snapshots watched by the realtime service:

```text
bucket: document_jobs
key: jobs.<tenant_id>.<document_id>.<job_id>
```

Optional durable progress/history events use `DOC_PROGRESS` only when Delphi
needs a replayable progress timeline:

```text
documents.progress.v1.tenant.<tenant_id>.document.<document_id>.job.<job_id>
```

## Event Envelope

All command and work messages use the same envelope shape:

```json
{
  "event_id": "7e6f...",
  "event_version": 1,
  "event_type": "document.snapshot_upsert_requested",
  "aggregate_type": "document",
  "aggregate_id": "document uuid",
  "aggregate_version": 7,
  "tenant_id": "tenant uuid",
  "actor_user_id": "user uuid",
  "causation_event_id": null,
  "correlation_id": "request uuid",
  "occurred_at": "2026-06-11T12:00:00Z",
  "payload": {
    "expected_document_version": 6,
    "target_document_version": 7,
    "document": {
      "tenant_id": "tenant uuid",
      "document_id": "document uuid",
      "owner_user_id": "user uuid",
      "state": "active",
      "title": "Quarterly report",
      "metadata": {},
      "object_key": "documents/tenant/document/versions/7/original",
      "object_etag": "etag",
      "object_size_bytes": 12345,
      "content_sha256": "hex",
      "content_type": "application/pdf"
    }
  }
}
```

NATS headers:

```text
Nats-Msg-Id: <event_id>
Delphi-Event-Type: <event_type>
Delphi-Tenant-Id: <tenant_id>
Delphi-Aggregate-Id: <aggregate_id>
Delphi-Aggregate-Version: <aggregate_version>
Delphi-Correlation-Id: <correlation_id>
```

`Nats-Msg-Id` is mandatory. The event id must be deterministic for retried API
commands that use the same idempotency key.

For snapshot CRUD commands, `aggregate_version` is the server-assigned
`target_document_version`. Clients provide only `expected_document_version`;
the API or command builder assigns the outcome version.

## Stream Configuration

Use separate streams for command durability, retained snapshot history, and
work distribution. Live UI progress uses NATS KV. Add a progress stream only
when durable progress history is required:

```text
DOC_COMMANDS
  subjects:
    documents.commands.v1.>

DOC_SNAPSHOTS
  subjects:
    documents.snapshots.v1.>

DOC_EVENTS
  subjects:
    documents.work.v1.>
    documents.projections.v1.>

DOC_PROGRESS optional
  subjects:
    documents.progress.v1.>
```

Configure streams and consumers with:

- explicit ACK;
- duplicate detection window sized for API and publisher retries;
- bounded `AckWait`;
- `MaxDeliver` high enough that workers receive their semantic terminal
  delivery and can publish failure state;
- dead-letter or parking subjects only for transport-level poison messages
  that cannot be decoded or routed;
- tenant/document ids in headers for observability and routing.

`DOC_SNAPSHOTS` stores full document row snapshots for rollback/history. It is
not event sourcing; rollback reads an old retained snapshot and publishes a new
snapshot with a newer server-assigned version. Configure it with bounded
retention, for example `MaxMsgsPerSubject = N`, so each document keeps the last
N snapshots. Work streams remain operational retention, not auditable history.
If permanent event sourcing becomes a requirement later, add a dedicated event
store rather than overloading the current work streams.

## Idempotency

Idempotency exists at multiple layers:

| Layer | Key |
| --- | --- |
| API command retry | idempotency key -> deterministic `event_id` |
| NATS duplicate publish | `Nats-Msg-Id = event_id` |
| NATS retry count | JetStream delivery count |
| Workflow checkpoint | NATS KV key scoped by tenant/document/job |
| Snapshot history | `DOC_SNAPSHOTS` subject by tenant/document with bounded per-subject retention |
| Postgres projection | `(tenant_id, document_id)` and monotonic target document version |
| Qdrant projection | deterministic point ids and `projection_version` |
| NebulaGraph projection | deterministic vertex/edge ids and `projection_version` |

Duplicate events are acceptable. Every projection must converge by doing a
no-op when it sees the same or older target document version.

## Document Job State

NATS owns workflow progression. Document upload, ingestion, and projection
workflows use one NATS KV document job as the short-lived stage checkpoint and
live progress snapshot so redelivery can reconstruct missing next wakeups
without treating Postgres as workflow state:

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
  "object_key": "documents/<tenant_id>/<document_id>/original"
}
```

Upload expiry uses a separate time-bucket KV index:

```text
bucket: document_job_timeouts
key: upload_timeouts.<epoch_minute>.<tenant_id>.<document_id>.<job_id>
```

The timeout index value contains only `tenant_id`, `document_id`, and
`job_id`.

Do not use NATS KV as the document read model or permanent event store.
Postgres remains the document status/read projection.

## Crash Behavior

| Crash point | Durable state | Recovery |
| --- | --- | --- |
| API crashes before PubAck | Command may not be durable. | Client retries with same idempotency key and event id. |
| API crashes after PubAck before response | Command is durable in NATS. | Client retry sees duplicate publish suppressed or status catches up. |
| Consumer crashes before ACK | NATS redelivers. | Idempotent projection repeats or no-ops. |
| Consumer crashes after projection write before ACK | Projection may be applied. | Redelivery repeats deterministic work, advances/reads KV, republishes the next wakeup if needed, and ACKs. |
| Consumer crashes after publishing next wakeup before ACK | Next message may already be durable. | Redelivery republishes with same `Nats-Msg-Id` and ACKs. |
| Workflow fails terminally | PG document status becomes `failed`. | UI reflects failure; repair command may be published later. |

No compensation steps are required for normal document CRUD. Failed state is
explicit state.
