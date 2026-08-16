---
title: Delphi Docs
description: Architecture, runtime workflows, and operating notes for the Delphi project.
---

Delphi is split into focused services coordinated through Postgres, SurrealDB,
S3-compatible object storage, NATS JetStream, NATS KV, and WebSocket fanout.
These docs track the current project architecture and the decisions that shape
it.

## Start Here

- [Chat System](architecture/chat-system): service boundaries, NATS subjects,
  state ownership, and realtime fanout.
- [Chat Migration](architecture/chat-migration): consolidated migration status,
  old-system differences, improvements, and remaining open work.
- [Chat Request Flow](architecture/chat-request-flow): submit-to-commit flow,
  active-turn state, stop behavior, and browser reconciliation.
- [Chat Failure Analysis](architecture/chat-failure-analysis): crash matrices,
  recovery expectations, and required invariants.
- [Document Upload and Lifecycle](architecture/document-upload): the selected
  document design — event-sourced upload, concurrency, projections, and
  reclamation.
- [Document CRUD Pipeline](document-crud/document-crud-pipeline): alternative NATS-first
  document CRUD with PG/S3/Qdrant/NebulaGraph projections.
- [Document Event Sourcing](document-crud/document-event-sourcing):
  EventStoreDB-backed document source of truth with NATS work fanout.
- [Document CRUD Sync Pattern](document-crud/document-crud-sync): alternative
  NATS event-first projection pattern and state ownership.
- [NATS Event-First Document CRUD](document-crud/nats-event-first-document-crud):
  command/event contract for document CRUD.
- [Design Notes](architecture/alteration): open workflow decisions and
  architecture refinements under review.

## Operating Model

- NATS JetStream owns document truth as an append-only event log.
- Postgres holds the document read model, rebuilt by folding that log.
- NATS JetStream owns upload work delivery, redelivery, and backpressure.
- NATS KV holds short-lived upload context, never read by workers.
- S3-compatible object storage owns document bytes, immutable per upload.
- Qdrant owns derived vector search projections.
- NebulaGraph owns derived graph projections.
- Postgres owns committed conversation and message truth.
- NATS JetStream carries durable commands and live chat events.
- NATS KV coordinates active locks and replay state.
- WebSocket delivery is authorized per conversation before fanout.

## Local Preview

```bash
make docs-serve
```

The local Fumadocs server runs at `http://127.0.0.1:8003/docs`.
