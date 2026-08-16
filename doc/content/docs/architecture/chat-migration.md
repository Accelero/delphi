---
title: Chat Migration
description: Consolidated current state, old-system differences, and remaining work for the chat migration.
---

# Chat Migration

This page consolidates the former chat gap analysis, realtime replay plan, and
chat microservice migration plan. It tracks the current implementation, what is
still open, and how the new chat system differs from the old single-backend
chat path.

Use this page for migration status. Use [Chat System](chat-system) for service
boundaries, [Chat Request Flow](chat-request-flow) for submit/stop sequence
details, and [Chat Failure Analysis](chat-failure-analysis) for crash behavior.

## Current Baseline

The new chat slice is implemented as a Tier 2 microservice stack:

- `api-service` exposes authenticated chat HTTP APIs for auth, conversation
  CRUD, turn submit, and stop.
- `realtime-service` owns `/ws/chat`, validates the user, authorizes each
  conversation subscription through SurrealDB, replays retained events, and
  fans out live events to local sockets.
- `chat-worker` consumes turn commands, owns LLM streaming, handles stop
  requests, commits terminal chat state, and emits realtime events.
- Shared Rust crates define auth extraction, config, contracts, storage,
  NATS/JetStream/KV access, and LLM provider wiring.
- The React frontend uses TanStack Router, TanStack Query, a WebSocket chat
  hook, a compact chat layout, smoothed assistant rendering, rich markdown,
  route-aware conversation navigation, stop controls, copy actions, and live
  title refresh.
- The local stack is composed with Traefik, oauth2-proxy, Keycloak, Redis,
  NATS JetStream, SurrealDB, frontend, API, realtime, chat worker, and the
  title LLM sidecar.

The current implementation already uses real provider streaming through
`rig`. RAG retrieval is not wired yet, so citation contracts exist but worker
responses currently persist empty citation arrays.

## New Runtime Model

The migration replaces the old in-process/SSE shape with service-owned state:

| Concern | New owner |
| --- | --- |
| Auth and accepted user intent | `api-service` |
| Active-turn coordination | NATS KV `CHAT_LOCKS` |
| Command durability | NATS JetStream `CHAT_COMMANDS` |
| LLM execution | `chat-worker` |
| Live turn events | NATS JetStream `CHAT_EVENTS` |
| Replay cursor metadata | NATS KV `CHAT_REPLAY` |
| Committed conversations/messages | SurrealDB |
| Browser fanout and replay | `realtime-service` |

Core rule: HTTP creates intent, NATS carries live session state, SurrealDB
stores committed truth, and WebSocket only relays authorized events.

### Storage

The chat slice uses schemafull SurrealDB tables:

- `tenant`
- `app_user`
- `chat_conversation`
- `chat_message`
- `chat_turn`

Every chat domain row carries `tenant_id`; conversation and message rows also
carry `user_id` for the current owner-only model. `chat_message.ordinal` is the
monotonic per-conversation order key, while `message_id` remains the stable
idempotency key. Branch pruning happens at commit by deleting messages newer
than the submitted parent.

`chat_turn` is terminal-only in durable storage. Active states such as
`requested` and `running` live in `CHAT_LOCKS`; SurrealDB records final
`committed`, `interrupted`, or `failed` outcomes.

### Commands, Locks, And Events

The submit endpoint validates ULIDs, non-empty text, conversation access, and
parent-tail freshness. It then creates a `CHAT_LOCKS` entry with the bounded
prompt payload before publishing a deterministic `TurnRequested` wakeup to:

```text
chat.commands.turn_requested
```

The command payload is intentionally small. The worker loads the prompt and
ownership state from `CHAT_LOCKS`, claims the lock with compare-and-set
semantics, renews the lease while streaming, writes terminal DB state, marks
the lock terminal, publishes the terminal event, ACKs the command, then
releases the lock.

Realtime events are published to conversation-scoped subjects:

```text
chat.events.<tenant_id>.<conversation_id>
```

JetStream sequence numbers become browser `event_id` values. `CHAT_REPLAY`
keeps the current and previous turn sequence windows so late joiners and
reconnecting sockets can replay events when the cursor is still retained.

### Stop

Stop is modeled as a turn interrupt, not as direct UI mutation. The API
authorizes the conversation, sets `stop_requested` on `CHAT_LOCKS`, and, if the
lock is already owned by a worker, publishes a low-latency wakeup to:

```text
chat.control.worker.<worker_id>.stop
```

The wakeup is an optimization. `CHAT_LOCKS.stop_requested` is authoritative, so
a missed wakeup is recovered by worker polling or command redelivery. Stop is
scoped by tenant, conversation, and turn id.

The new product behavior intentionally differs from the old clear-on-stop
behavior: interrupted turns commit the user message plus partial assistant
content with `interrupted = true` and
`finish_reason = user_interrupted`.

### Realtime Replay And Fanout

The realtime service creates exact-subject JetStream consumers only for
authorized active conversations. Multiple tabs on the same realtime replica
share a local fanout hub and one NATS consumer for that conversation.

Replay behavior:

- A fresh late joiner receives the current in-flight turn if it is still
  retained and not already committed in durable history.
- A reconnect with `last_event_id` replays events after that JetStream
  sequence if the cursor falls inside the previous or current retained turn.
- A stale cursor, missing replay metadata, or incomplete JetStream range sends
  `resync_required`.
- The frontend handles `resync_required` by clearing transient live state and
  refetching authoritative conversation history.

Socket outbound queues and local broadcast buffers are bounded. Broadcast lag
or queue pressure turns into resync or disconnect/reconnect instead of blocking
the shared fanout path.

## Differences From The Old Chat

The old chat path remains reference material for behavior, not structure. The
new system intentionally diverges in these ways:

| Area | Old implementation | New implementation |
| --- | --- | --- |
| Backend shape | Single backend owned API, worker behavior, and SSE | API, realtime, and worker services with explicit ownership |
| Live transport | SSE/EventSource frames | Structured JSON over WebSocket |
| Event replay | SSE cursor behavior in backend path | JetStream sequence cursors plus `CHAT_REPLAY` windows |
| Active state | Mostly process/backend-local flow | NATS KV locks with leases, ownership, stop flags, and terminal handoff markers |
| Stop semantics | Cleared uncommitted UI | Persists interrupted user + partial assistant turn |
| Scaling | Coupled to old backend process | Horizontal API/realtime/worker replicas with NATS coordination |
| Realtime fanout | Backend stream path | One exact NATS consumer per active conversation per realtime replica, local tab fanout |
| Frontend state | Old chat stream hook and component tree | TanStack Router/Query plus local live overlay state |
| Markdown | Rich old renderer | Streamdown-based rich renderer with smoothing, math, mermaid, code, CJK, and reasoning blocks |
| Titles | Generated after first assistant response | Same behavior restored through title LLM and live `title_updated` events |
| RAG/citations | Retrieval and citation behavior existed in old product surface | Contracts and frontend rendering exist; backend retrieval is still open |

## New Or Improved Capabilities

- Service boundaries are explicit: API accepts commands, worker executes turns,
  realtime relays events, and SurrealDB stores committed truth.
- Single-flight per conversation is enforced by `CHAT_LOCKS`, not by frontend
  optimism or process-local state.
- Worker redelivery is idempotent: fresh running locks are not rerun, stale
  running locks are failed and cleaned up, and terminal KV markers let
  redelivery publish missing terminal events without re-calling the provider.
- Stop is race-safe because stop flags and worker ownership live on the same KV
  key.
- Replay is shared across realtime replicas through JetStream sequence numbers
  and `CHAT_REPLAY` metadata.
- Slow or lagging WebSocket clients are isolated through bounded queues and
  explicit resync.
- Conversation routing, deletion, 404 recovery, terminal refresh, title
  updates, and list/detail cache updates now converge through TanStack Query.
- First-turn title generation is restored and decoupled through the title LLM
  sidecar.
- Rich assistant rendering has been restored with Streamdown, Shiki-backed code
  blocks, KaTeX math, Mermaid diagrams, CJK handling, incomplete-markdown
  support, reasoning block parsing, citation marker rewriting, and copy
  actions.
- Scroll behavior now follows the old turn model more closely: new turns
  re-enter follow mode, pin the user message to the top, size the last turn
  from the actual scroll viewport, and keep a sentinel backup for late layout
  shifts.

## Still Open

### Product Behavior

- RAG retrieval, grounded prompt construction, citation event publication, and
  assistant citation persistence are not implemented.
- Citation source rendering on reload depends on backend citation population.
- Product should explicitly confirm the new interrupted-turn persistence
  behavior so old clear-and-persist-nothing semantics do not leak back in.
- Future shared conversations need a membership/ACL table and repository
  checks beyond owner `user_id`.

### Testing

Current coverage is mostly crate/unit-level plus focused API or browser
coverage. The highest-value missing tests are:

- Backend integration tests against real NATS and SurrealDB.
- WebSocket e2e coverage for subscribe, reconnect, stale cursor resync, replay
  during current and previous retained turns, and late join during streaming.
- Two-tab fanout on one realtime replica.
- Two realtime replicas sharing replay/fanout behavior.
- Two chat-worker replicas with one owner per turn.
- Stop from a non-submitting tab.
- Worker crash/redelivery before provider start, mid-stream, after DB commit,
  and before final ACK.
- Frontend coverage for deletes, navigation, title updates, terminal refresh,
  404 recovery, resync convergence, smoothing, interrupted state, and scroll
  follow/escape/re-entry behavior.
- Full-stack coverage for first-turn title generation and live sidebar/title
  refresh.

### Operations

- Add deeper readiness checks that actively verify NATS and SurrealDB
  dependency operations from each service.
- Add metrics for command lag, lock age, replay failures, socket counts,
  reconnects, worker errors, event fanout costs, and redeliveries.
- Add structured tracing across request id, command id, turn id, worker id,
  NATS sequence/event id, conversation id, tenant id, and user id.
- Define and document NATS service subject authorization boundaries. Browser
  clients never connect to NATS, so per-user conversation access must remain in
  SurrealDB-backed application authorization.
- Add admin/debug views or runbooks for stuck turns, terminal KV markers, and
  replay windows.
- Load test consumer counts, socket counts, queue sizes, and event fanout under
  realistic stream sizes.
- Tune `REALTIME_WS_EVENT_QUEUE_SIZE`,
  `REALTIME_WS_OUTBOUND_QUEUE_SIZE`, and
  `REALTIME_CONVERSATION_EVENT_BUFFER_SIZE`.

### Migration Hygiene

- Schema is still applied during service bootstrap rather than through a
  versioned migration runner.
- `tenant` and `app_user` rows are minimal identity anchors only; broader
  account management is not implemented.
- Chat table names intentionally use `chat_conversation` and `chat_message`
  rather than the generic `conversation` and `message` names from the original
  plan. Keep that stable before other services depend on chat storage.

## Recommended Next Order

1. Add backend integration tests for real NATS/SurrealDB stop, replay,
   redelivery, crash recovery, and multi-worker behavior.
2. Add Playwright or equivalent e2e coverage for streaming, stop, late join,
   reconnect, stale cursor resync, and two-tab fanout.
3. Implement RAG retrieval, grounded prompt construction, citation events, and
   citation persistence.
4. Harden operations with dependency readiness, metrics, tracing, NATS auth
   documentation, and stuck-turn runbooks.
5. Add multi-replica realtime and worker tests, then load test queue sizes and
   consumer counts.
6. Introduce a versioned storage migration path before the chat schema becomes
   a dependency for more services.

## Highest Risks

- Reconnect, late-join replay, and backpressure behavior are implemented but
  still under-tested in the full stack.
- Worker crash/redelivery semantics are designed for idempotency but still
  need real NATS/SurrealDB crash tests.
- RAG and citation grounding are absent, so the current chat product surface is
  still below the old implementation in that area.
- Operations visibility is thin for a distributed chat path; stuck-turn and
  replay-window debugging needs explicit tooling or runbooks.
