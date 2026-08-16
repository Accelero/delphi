---
title: Document CRUD Sync Pattern
description: Alternative NATS event-first pattern for document CRUD, ingestion, and projections.
---

# Document CRUD Sync Pattern

> **Not the selected design.** The document architecture was settled in favour
> of an event-sourced model on NATS JetStream — see
> [Document Upload and Lifecycle](../architecture/document-upload). This page is
> retained as a record of the design space that was explored.

This page records an early NATS event-first sync pattern. Document CRUD is
event-first and API commands publish durable NATS events.
Postgres, Qdrant, NebulaGraph, realtime fanout, and ingestion workers consume
those events and converge their own projections.

This is event-sourcing shaped, but Delphi does not keep a permanent application
event store yet. NATS JetStream owns durable delivery, replay within retention,
redelivery, dedupe, and work fanout. Postgres owns the current document read
model and status projection.

The rollout order is deliberate:

```text
phase 1: NATS + Postgres document projection + S3 + PG ingestion outputs + UI live events
phase 2: add Qdrant vector projection
phase 3: add NebulaGraph graph projection
```

## Selected Pattern

| Concern | Decision |
| --- | --- |
| Command entry point | API publishes durable NATS command/event and waits for `PubAck` |
| Saga/workflow owner | NATS JetStream delivery plus NATS KV short-lived stage checkpoints |
| Document read model | Postgres `document` row with current content metadata and status |
| Object bytes | S3-compatible private bucket |
| Work distribution | NATS JetStream explicit-ACK consumers |
| Worker progress | PG projection/status rows plus NATS delivery metadata |
| UI live updates | Realtime service consumes NATS events and/or PG status updates |
| Vector store | Qdrant |
| Graph store | NebulaGraph |
| Retry authority | NATS redelivery and worker retry budgets |
| Compensation | None; terminal failures become visible failed document states |

## Projection Store Selection

Qdrant is the selected vector store. It is a Rust vector database with a
purpose-built segment/WAL storage design, HNSW indexing, REST/gRPC APIs, and a
Qdrant-specific JSON/protobuf query DSL. Projection writes use deterministic
point ids and payload version guards so stale events cannot overwrite newer
projection state.

NebulaGraph is the selected graph store. It is a mostly C++ distributed graph
database with storage/query separation, RocksDB-backed local persistence, and
Multi Group Raft for replicated storage consistency. Projection writes use
deterministic vertex/edge ids and nGQL `UPDATE`/`UPSERT ... WHEN` conditions
against `projection_version`.

Both stores are projections, not command authorities. Postgres is also a
projection. NATS owns the durable command/work stream and saga choreography.

## Durable Flow

API commands:

```text
HTTP command
-> authorize and validate request
-> for CRUD snapshots, target the client-read document version
   and assign the target version server-side
-> publish durable NATS event with deterministic Nats-Msg-Id
-> wait for PubAck
-> return accepted
```

Consumers:

```text
NATS event
-> idempotent side effect
-> update projection/status state
-> publish any next direct NATS wakeup
-> ACK NATS event
```

The practical behavior is close to the previous PG-first CRUD flow, but the
start of the pipeline is now the NATS event. PG updates happen as projections
instead of being the synchronous command commit.

CRUD events are full document row snapshots. Metadata update, content replace,
and delete do not publish field patches; they publish the complete desired PG
document row for the server-assigned target version. Deleted/tombstoned
documents are not valid CRUD targets in the current API.

## Event Families

Primary document command events:

```text
document.upload_started
document.upload_completed
document.upload_abort_requested
document.upload_timeout_requested
document.snapshot_upsert_requested
document.snapshot_delete_requested
document.reindex_requested
```

Ingestion work events:

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

Live UI progress is delivered from NATS KV job snapshots by default. The
events above are optional progress/history events when Delphi needs a durable
timeline beyond the latest live snapshot.

Every event has a stable id, aggregate version, causation id, correlation id,
tenant id, document id, and event contract version. Workers use those fields
for idempotency, stale-event detection, logging, and tracing.

## State Ownership

| State | Owner |
| --- | --- |
| Command/work delivery | NATS JetStream |
| Saga lifetime state | NATS KV short-lived stage checkpoints |
| Current document status/read model | Postgres projection |
| Object bytes | S3-compatible storage |
| Vector index | Qdrant projection |
| Graph index | NebulaGraph projection |
| Browser live delivery | Realtime service watching NATS KV job snapshots |

Postgres still reflects document state throughout ingestion:

```text
creating -> validating -> extracting -> chunking -> active
creating -> failed
deleting -> deleted/tombstoned
```

But those state changes are projection results driven by NATS events.

## No Compensation

The document workflow is forward-only. We do not add compensating actions for
failed ingestion, projection, or cleanup stages. A failure is reflected in the
Postgres document status and surfaced to the UI. Repair is done by publishing a
new command such as `document.reindex_requested` or by replaying/recreating
work events within operational retention.

## Related Pages

- [Document CRUD Pipeline](./document-crud-pipeline): full NATS-first
  document CRUD, ingestion, projections, and UI flow.
- [NATS Event-First Document CRUD](./nats-event-first-document-crud): command/event
  contract and publish requirements.
- [NATS Projection Flow](./nats-projection-flow): worker operations,
  redelivery behavior, crash matrix, and realtime fanout.
