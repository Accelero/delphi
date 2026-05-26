# Chat System Gap Analysis

Status: snapshot after the first working chat microservice slice.

This document compares the current implementation against:

- [Chat microservice migration plan](./architecture/chat-microservice-migration.md)
- [Chat realtime replay plan](./architecture/chat-realtime-replay-plan.md)
- Old chat implementation under `old/`

## Current Baseline

Implemented:

- T2 auth stack with oauth2-proxy, Keycloak, Traefik, Redis, NATS, SurrealDB,
  frontend, `api-service`, `realtime-service`, and `chat-worker`.
- Authenticated chat API for conversation CRUD, turn submit, and stop.
- SurrealDB-backed `tenant`, `app_user`, `chat_conversation`,
  `chat_message`, and `chat_turn` state with schemafull table definitions.
- Message ordering and branch pruning by monotonic per-conversation
  `chat_message.ordinal`.
- NATS JetStream command stream, event stream, replay KV, lock KV, command
  progress ACKs, and worker-specific stop wakeups.
- Chat worker with real LLM provider streaming through `rig`.
- WebSocket realtime service with SurrealDB authorization, NATS event relay,
  and replay from JetStream sequence windows.
- React chat UI with sidebar, chat pane, WebSocket hook, smoothed assistant
  rendering, scroll sentinel, send/stop control, and interrupted event handling.

## Gaps Against The Plan

### Storage Schema

~~The plan requires schemafull `tenant`, `app_user`, `conversation`,
`message`, and `chat_turn` tables with ordinal ordering.~~

Implemented for the chat slice: schemafull `tenant`, `app_user`,
`chat_conversation`, `chat_message`, and `chat_turn` tables are bootstrapped
by `delphi-storage`. All chat domain rows carry `tenant_id`; conversation and
message access also carries `user_id` for the current owner-only model.
`chat_message.ordinal` is the monotonic per-conversation order key and
`message_id` remains the stable idempotency key.

Residual points:

- Schema is still applied by service bootstrap, not by a versioned migration
  runner.
- `tenant` and `app_user` rows are minimal identity anchors only; broader
  account management is not implemented yet.
- Future sharing will need an access membership table and repository checks
  beyond owner `user_id`.
- Table names intentionally use chat-specific names (`chat_conversation`,
  `chat_message`) instead of the generic plan names (`conversation`,
  `message`). This is acceptable for the microservice slice, but should stay
  documented before more services depend on these names.

### Durable Turn State

~~The plan requires durable `chat_turn` lifecycle rows for `requested`,
`running`, `committed`, `interrupted`, and `failed`.~~

Implemented for the normal turn path: API creates/updates
`chat_turn(status=requested)`, worker marks `running`, commits mark
`committed` or `interrupted`, and failed pre-commit paths mark `failed` with
an error string.

Residual points:

- There is no recovery/reconciler job for stale `requested` or `running` rows.
- Turn state is not exposed through admin/debug APIs yet.
- Redelivery behavior is not fully proven against durable turn state. A
  redelivered command should consult the stored turn state before repeating
  provider side effects or producing duplicate terminal events.

### Crash Recovery

Lock TTL, lock renewal, and JetStream redelivery exist. A reconciler does not.

Open points:

- Add a stale-turn recovery path that marks abandoned turns failed/cancelled.
- Publish `clear` when a turn cannot be completed after worker death.
- Add tests for worker crash before provider start, mid-stream, and after DB
  commit but before final ACK.
- Make crash cases converge without wedged locks, permanent streaming UI, or
  ambiguous committed state.

### Idempotency And Redelivery

NATS JetStream gives at-least-once delivery. Commands use deterministic NATS
message IDs and worker progress ACKs, and message IDs are stable ULIDs, but
the full redelivery state machine is not yet proven.

Open points:

- Worker should check `chat_turn` before executing a redelivered command.
- A terminal `committed`, `interrupted`, or `failed` turn should not re-run the
  provider.
- Duplicate commits should converge on the same persisted visible history or
  return a clear stale/resync path.
- Add integration tests for redelivery after provider start, after DB commit,
  and before final JetStream ACK.

### Realtime Fanout

The plan calls for one NATS/JetStream consumer per active conversation per
realtime replica with local tab fanout. Current realtime service uses one
wildcard event subscriber per realtime process and each WebSocket filters
events locally.

Open points:

- Decide whether wildcard process-level subscription is acceptable for v1.
- If not, implement per-conversation subscription/fanout registries.
- Add bounded outbound queues so one slow socket cannot block other sockets.
- Track socket counts and event fanout costs so we know when the wildcard
  approach stops being acceptable.

### WebSocket Reconnect

~~Server-side replay exists. Client-side reconnect/backoff does not.~~

Implemented: the frontend reconnects with bounded backoff, tracks
`last_event_id` per conversation, resubscribes after reconnect, and refetches
committed state when the realtime service sends `resync_required`.

Residual points:

- Reconnect/resync behavior still needs Playwright coverage against the full
  Docker stack.
- The UI has only a minimal reconnecting indicator.

### Replay And Backpressure

Replay logic exists, but uncertainty is not always converted into a required
resync. If replay metadata names a range and JetStream returns an incomplete
event set, the service should prefer `resync_required` over partial replay.

Open points:

- Convert broadcast lag and missing retained events into explicit
  `resync_required`.
- Add bounded per-socket outbound queues and close/resync lagging clients.
- Test fresh late join during current turn.
- Test reconnect with cursor inside current/previous retained turn.
- Test stale cursor returning `resync_required`.
- Test broadcast lag or slow socket behavior.

### Frontend Data Layer

The plan mentions typed query hooks and query invalidation. Current frontend
uses local state and direct API calls.

Open points:

- Add query layer or explicitly keep the simpler local state for v1.
- Invalidate/refetch conversation and list on `finish`, `interrupted`, `clear`,
  and `title_updated`.
- Add route state for `/chat` and `/chat/:conversationId`.
- Make deletes, navigation, terminal events, title updates, and resyncs
  converge without manual refresh.

### Frontend Rendering

Current rendering is React Markdown plus GFM and smoothing. The plan and old
frontend include richer behavior.

Open points:

- Streaming-safe markdown renderer.
- Math, mermaid, CJK/code handling.
- `<think>...</think>` reasoning block parsing.
- Citation marker rewrite from `[N]` to source links.
- Copy message action.

### Scroll Behavior

The current UI has the sentinel and bottom-follow pattern. It does not fully
match the old scroll model.

Open points:

- Re-enable follow mode when a new turn starts.
- Pin the new turn to the top with `scrollIntoView({ block: "start" })`.
- Base last-turn min-height on the scroll viewport rather than a fixed viewport
  expression.
- Preserve the old post-paint layout-shift backup behavior.

### Title Generation

Current storage sets the title to the first 48 user-message characters when the
title is `New chat`. Old chat generated a title after the first assistant
response, persisted it, and pushed a live title event.

Open points:

- Add title generation after first successful assistant commit.
- Persist the generated title durably.
- Publish `title_updated`.
- Patch the sidebar and active conversation title on the frontend.

### RAG And Citations

Citation contracts and storage fields exist, but chat worker currently passes
empty citations and does no retrieval.

Open points:

- Add retrieval stage before provider call.
- Build grounded system prompt from retrieved chunks.
- Publish `citations` before text deltas.
- Persist assistant citations and render historical citation links.
- Restore citation ordering guarantees from the old tests: citation frame
  before text, persisted metadata on assistant rows, and source-link rendering
  on reload.

### Tests

Current coverage is mostly crate unit tests plus Playwright API e2e.

Open points:

- Backend integration tests for real NATS and SurrealDB.
- WebSocket e2e tests.
- Two-tab fanout.
- Late join and reconnect replay.
- Stop from a non-submitting tab.
- Worker crash/redelivery.
- Two chat-worker replicas.
- Two realtime-service replicas.
- Frontend unit/component tests for smoothing, stop state, replay reset,
  interrupted state, and scroll behavior.

### Operations

The service split is in place, but production hardening is thin.

Open points:

- Add readiness checks that include NATS and SurrealDB dependencies.
- Add metrics for command lag, lock age, replay failures, socket counts,
  reconnects, worker errors, and redeliveries.
- Add structured tracing across API command id, turn id, worker id, NATS
  sequence/event id, and conversation id.
- Define NATS subject authorization boundaries and document the current
  app-level tenant isolation model.
- Add runbooks or admin/debug views for stuck turns.

## Gaps Against Old Implementation

Old behavior not yet ported:

- RAG retrieval and citation prompt construction.
- Citation persistence and citation marker rendering.
- First-turn title generation and live title push.
- Rich markdown/rendering behavior including reasoning block support.
- More complete scroll pinning/follow behavior.
- Broader chat behavior test suite:
  - stop
  - late subscribe
  - commit/abort race
  - concurrent post
  - last-writer-wins pruning
  - citation persistence

Intentional difference:

- Old stop cancelled and cleared uncommitted UI. New stop persists the user
  message plus partial assistant message with `interrupted = true` and
  `finish_reason = user_interrupted`. Old tests should be rewritten around the
  new interrupted-turn semantics.
- Product should explicitly confirm this interrupted-persistence behavior so
  the system does not accidentally carry both old clear-and-persist-nothing
  semantics and new persisted-partial semantics.

## Recommended Next Order

1. ~~Add WebSocket reconnect/backoff and resubscribe with `last_event_id`.~~
   Done; remaining work is full-stack e2e coverage.
2. Add realtime backpressure behavior and `resync_required` on lag.
3. Add stale-turn recovery and redelivery/idempotency handling around
   `chat_turn`.
4. ~~Decide and implement the `chat_turn` lifecycle table.~~ Done for the
   normal path; remaining work is stale-turn recovery and redelivery behavior.
5. Port old frontend scroll pinning and richer markdown/citation rendering.
6. Add RAG/citations and title generation after the transport path is stable.
7. Harden operations with dependency readiness, metrics, tracing, NATS auth
   policy documentation, and stuck-turn runbooks.
8. Add Playwright e2e for streaming, stop, late join, reconnect, and two-tab
   fanout.
9. Add backend integration tests for NATS/SurrealDB stop, replay, redelivery,
   crash recovery, and multi-replica behavior.

## Highest Risks

- Reconnect and late-join correctness are not yet proven by tests.
- Worker crash/redelivery semantics are not yet proven by tests.
- Realtime wildcard fanout may become inefficient as event volume grows.
- `chat_turn` exists, but stale-turn recovery and redelivery behavior are not
  proven yet.
- Frontend parity is incomplete for citations, titles, rich rendering, and
  route/query behavior.
- RAG citations, source grounding, and title updates are still absent, so the
  current chat is functionally below the old product surface.
