# Delphi Greenfield Architecture

This is the architecture entrypoint for the greenfield microservice rewrite.
The previous implementation is preserved under `old/` and should be used as
reference, not as the target structure.

## Contents

- [Microservice migration plan](./microservice-migration-plan.md) — master
  plan for the incremental rewrite across chat, ingestion, and feed.
- [Chat microservice migration](./chat-microservice-migration.md) — first
  rebuild slice: API service, realtime WebSocket service, chat worker,
  NATS/JetStream session state, frontend chat UI, and validation plan.
- [Chat realtime replay plan](./chat-realtime-replay-plan.md) — backend plan
  for JetStream replay, late joiners, reconnects, per-turn purge, and replay
  KV retention.
- [Ingestion microservice migration](./ingestion-microservice-migration.md) —
  second rebuild slice: upload API, object validation, saga work queues,
  extraction, chunking, embedding, publish, and reconciliation.
- [Feed microservice migration](./feed-microservice-migration.md) — third
  rebuild slice: feed product rework, durable feed reads, and live NATS
  fan-out.

## Current Direction

- Build against the full-auth Tier 2 shape first.
- Use Rust services.
- Use React, shadcn/ui, and Tailwind for the frontend.
- Use NATS/JetStream from the first chat implementation.
- Keep SurrealDB as initial durable storage with a new clean schema.
- Defer Tier 1/dev-auth, ingestion, and feed until chat is manually and
  automatically validated.
