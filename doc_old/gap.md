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
`message_id` remains the stable idempotency key. `chat_conversation` now also
enforces global uniqueness for `conversation_id`, while tenant and user fields
remain explicit isolation and authorization dimensions.

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

Superseded by the KV ownership model: `requested` and `running` now live in
NATS KV `CHAT_LOCKS` with the bounded prompt payload, worker ownership, lease,
stop flags, and terminal handoff markers. SurrealDB `chat_turn` is terminal
only: `committed`, `interrupted`, or `failed`.

Residual points:

- Turn state is not exposed through admin/debug APIs yet.
- Full-stack tests still need to prove the KV ownership path under real
  redelivery.

### Crash Recovery

~~Lock TTL, lock renewal, and JetStream redelivery exist. A reconciler does not.~~

Implemented for the chat-worker path: redelivery checks `CHAT_LOCKS` before any
provider call. Fresh `running` turns are not rerun; stale `running` turns are
marked `failed`, write terminal failure state, publish cleanup, ACK, and
release. Terminal KV markers (`committed`, `interrupted`, `failed`) allow a
redelivered command to publish any missing terminal event, ACK, and release
without rerunning the provider.

Residual points:

- Add admin/debug views or runbooks for inspecting terminal/stuck turn state.

### Idempotency And Redelivery

~~NATS JetStream gives at-least-once delivery. Commands use deterministic NATS
message IDs and worker progress ACKs, and message IDs are stable ULIDs, but
the full redelivery state machine is not yet proven.~~

Implemented: `TurnRequested` is now a small wakeup; the prompt payload and
ownership state live in `CHAT_LOCKS`. The worker loads the KV state before
claiming/running, transitions `requested -> running` with CAS, renews the
lease while streaming, writes terminal Surreal state, marks KV terminal before
publishing the terminal event, ACKs the command, then releases the KV marker.

Residual points:

- None for implementation. Integration tests remain tracked under Tests.

### Realtime Fanout

~~The plan calls for one NATS/JetStream consumer per active conversation per
realtime replica with local tab fanout.~~

Implemented: the realtime service no longer opens a process-wide
`chat.events.>` subscriber. Each replica creates an exact-subject JetStream
consumer for `chat.events.<tenant_id>.<conversation_id>` only when at least one
local WebSocket is subscribed to that conversation, then fans those events out
through a local per-conversation broadcast channel. Multiple tabs on the same
replica share the same NATS consumer.

Open points:

- Add full-stack tests for two tabs on one replica and two realtime replicas.
- Track socket counts, active conversation hubs, NATS consumer counts, and
  event fanout costs.
- Tune `REALTIME_WS_EVENT_QUEUE_SIZE`,
  `REALTIME_WS_OUTBOUND_QUEUE_SIZE`, and
  `REALTIME_CONVERSATION_EVENT_BUFFER_SIZE` under realistic stream sizes.

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

~~Replay logic exists, but uncertainty is not always converted into a required
resync.~~ If replay metadata names a range and JetStream returns an incomplete
event set, the service sends `resync_required` instead of partial replay.
Realtime sockets now use bounded outbound queues, and broadcast lag is
converted into explicit `resync_required` messages for subscribed
conversations.

Residual points:

- Backpressure and lag behavior still need full-stack tests.
- Queue size is configurable via `REALTIME_WS_OUTBOUND_QUEUE_SIZE`, but the
  default may need tuning under real stream sizes.
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
- ~~Add route state for `/chat` and `/chat/:conversationId`.~~
- Make deletes, navigation, terminal events, title updates, and resyncs
  converge without manual refresh.

### Frontend Rendering

~~Current rendering is React Markdown plus GFM and smoothing. The plan and old
frontend include richer behavior.~~

Implemented: assistant messages now lazy-load the same streaming-oriented
rendering stack used by the old frontend: Streamdown with CJK handling, Shiki
code blocks, KaTeX math, Mermaid diagrams, and incomplete-markdown streaming
support. `<think>...</think>` blocks are split into a collapsible reasoning
section. Direct `react-markdown`/`remark-gfm` dependencies were removed because
Streamdown owns the GFM path.

Open points:

- ~~Streaming-safe markdown renderer.~~
- ~~Math, mermaid, CJK/code handling.~~
- ~~`<think>...</think>` reasoning block parsing.~~
- ~~Citation marker rewrite from `[N]` to source links.~~ Frontend support
  exists for current `CitationEntry` URLs; backend RAG/citation population
  remains under RAG And Citations.
- Copy message action.

### Scroll Behavior

~~The current UI has the sentinel and bottom-follow pattern. It does not fully
match the old scroll model.~~

Implemented: the chat pane now uses the old ref-driven scroll model. A new
turn re-engages follow mode and pins the turn top into view, the last turn uses
the measured scroll viewport height for its minimum height, and the sentinel
observer remains as a post-paint layout-shift backup.

Open points:

- ~~Re-enable follow mode when a new turn starts.~~
- ~~Pin the new turn to the top with `scrollIntoView({ block: "start" })`.~~
- ~~Base last-turn min-height on the scroll viewport rather than a fixed
  viewport expression.~~
- ~~Preserve the old post-paint layout-shift backup behavior.~~
- Add browser coverage for new-turn pinning, follow escape, follow re-entry,
  and late layout shifts.

### Title Generation

~~Current storage sets the title to the first 48 user-message characters when
the title is `New chat`. Old chat generated a title after the first assistant
response, persisted it, and pushed a live title event.~~

Implemented: the first successful assistant commit now starts a best-effort
title generation task using the configurable title LLM client. The default
points at the bundled OpenAI-compatible `title-llm` llama.cpp sidecar running
`Qwen2.5-0.5B-Instruct-GGUF:Q4_K_M`. Generated titles are persisted only if
the conversation is still `New chat`, then `title_updated` is published for
live frontend updates. The first-48-user-character fallback was removed.

Open points:

- ~~Add title generation after first successful assistant commit.~~
- ~~Persist the generated title durably.~~
- ~~Publish `title_updated`.~~
- ~~Patch the sidebar and active conversation title on the frontend.~~
- Add full-stack coverage for first-turn title generation and live title
  refresh.

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

- ~~Add readiness checks and Compose startup ordering for local stack
  services.~~ API, realtime, chat-worker, frontend, Keycloak, Redis, and
  title-llm now have Compose healthchecks, and Compose waits on health where
  the images expose usable checks. NATS and SurrealDB still use restart policy
  plus dependent service readiness because their images are effectively
  distroless in this stack.
- Add deeper readiness checks that actively verify NATS and SurrealDB
  dependency operations from each service, not just process HTTP health.
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
- ~~First-turn title generation and live title push.~~
- ~~Rich markdown/rendering behavior including reasoning block support.~~
- ~~More complete scroll pinning/follow behavior.~~
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
2. ~~Add realtime backpressure behavior and `resync_required` on lag.~~ Done;
   remaining work is full-stack e2e coverage and tuning.
3. ~~Add stale-turn recovery and redelivery/idempotency handling around
   active chat turns.~~ Done via the NATS KV ownership state machine; remaining
   work is integration coverage.
4. ~~Decide and implement turn lifecycle ownership.~~ Done: `requested` and
   `running` live in NATS KV, while terminal outcomes live in SurrealDB.
5. ~~Add first-turn title generation and live title push.~~ Done via the
   title-llm sidecar path; remaining work is full-stack coverage.
6. ~~Add Compose healthchecks and local startup ordering.~~ Done for local
   stack services where the images support checks; deeper dependency
   readiness remains under Operations.
7. Add RAG/citations.
8. ~~Port richer markdown rendering.~~ Done via Streamdown, CJK, Shiki, KaTeX,
   Mermaid, and reasoning blocks; citation source population remains under
   RAG/citations.
9. Harden operations with metrics, tracing, NATS auth policy documentation,
   deeper dependency readiness, and stuck-turn runbooks.
10. Add Playwright e2e for streaming, stop, late join, reconnect, and two-tab
   fanout.
11. Add backend integration tests for NATS/SurrealDB stop, replay, redelivery,
   crash recovery, and multi-replica behavior.

## Highest Risks

- Reconnect and late-join correctness are not yet proven by tests.
- Worker crash/redelivery semantics are implemented but not yet proven by
  full-stack tests.
- Realtime fanout now avoids wildcard event consumption, but multi-replica
  behavior and consumer-count limits still need load testing.
- `chat_turn` is terminal-only; active-turn recovery now lives in NATS KV but
  still needs full-stack crash tests.
- Frontend parity is incomplete for citations, message copy, scroll behavior
  test coverage, and route/query convergence.
- RAG citations and source grounding are still absent, so the current chat is
  functionally below the old product surface.
