---
title: Delphi Docs
description: Architecture, runtime workflows, and operating notes for the Delphi project.
---

Delphi is split into focused services coordinated through SurrealDB, NATS
JetStream, NATS KV, and WebSocket fanout. These docs track the current project
architecture and the decisions that shape it.

## Start Here

- [Chat System](architecture/chat-system): service boundaries, NATS subjects,
  state ownership, and realtime fanout.
- [Chat Request Flow](architecture/chat-request-flow): submit-to-commit flow,
  active-turn state, stop behavior, and browser reconciliation.
- [Chat Failure Analysis](architecture/chat-failure-analysis): crash matrices,
  recovery expectations, and required invariants.
- [Design Notes](architecture/alteration): open workflow decisions and
  architecture refinements under review.

## Operating Model

- SurrealDB owns committed conversation and message truth.
- NATS JetStream carries durable commands and live chat events.
- NATS KV coordinates active locks and replay state.
- WebSocket delivery is authorized per conversation before fanout.

## Local Preview

```bash
make docs-serve
```

The local Fumadocs server runs at `http://127.0.0.1:8003/docs`.
