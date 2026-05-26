# Chat Streaming v4

Status: **implemented** for a single backend replica with
`InProcessBus`. Horizontal scale-out is planned as `NatsBus`; see
[`scaling-nats.md`](./scaling-nats.md). Functional requirements live in
[`../specs/chat.md`](../specs/chat.md).

## Purpose

Chat v4 makes the server the authority for live turn state. A
conversation can be open in multiple tabs and every tab sees the same
user message, citations, token deltas, stop/clear, and finish events in
the same order.

The design separates:

- **Durable state:** committed conversations/messages/citations in
  SurrealDB.
- **Ephemeral state:** the in-flight turn log, cancel token, and
  single-flight guard behind `TurnBus`.

Finished turns are always reloaded from history. Live SSE only carries
the current in-flight state and a small replay window.

## API Surface

| Endpoint | Purpose |
|---|---|
| `GET /api/chat/conversations` | List conversations. |
| `POST /api/chat/conversations` | Create a conversation. |
| `GET /api/chat/conversations/:id` | Read conversation + committed history. |
| `PATCH /api/chat/conversations/:id` | Rename. |
| `DELETE /api/chat/conversations/:id` | Delete. |
| `POST /api/chat/conversations/:id/messages` | Start one turn. |
| `GET /api/chat/conversations/:id/stream` | SSE stream for live turn frames. |
| `POST /api/chat/conversations/:id/stop` | Cancel the in-flight turn, if any. |

Handlers run under `AuthedDb`, so SurrealDB permissions enforce tenant and
user scope on conversations and messages.

## TurnBus

`backend/src/chat/bus.rs` defines the transport seam:

- `try_start(conv, user_frame)` atomically claims the per-conversation
  single-flight slot and appends the first live frame.
- `subscribe(conv, from_cursor)` returns already formatted SSE frames,
  replaying from a cursor and then following live appends.
- `cancel(conv)` flips the in-flight cancellation token.
- `emit(conv, frame)` pushes best-effort live-only frames outside the turn
  lifecycle, currently used for title updates.

`AppState.turn_bus` is `Arc<dyn TurnBus>`. Today it is
`InProcessBus`; the NATS plan adds a second implementation without
changing handlers, worker, wire format, or frontend code.

## InProcessBus

`backend/src/chat/inprocess.rs` is the shipped Phase-1 backend.

- One live `Session` per conversation, indexed by a weak `DashMap` entry.
- Strong owners are reader streams and the worker `TurnHandle`.
- When the last owner drops, the session prunes its own map entry.
- There is no background GC sweeper.

Each session stores:

- a bounded `Vec<(Cursor, Bytes)>` of whole SSE frames,
- a `turn_cursor` marking the current turn start,
- a monotonic `next` cursor,
- a `running` flag,
- the current turn cancellation token.

The buffer retains at most **previous turn + current turn**. At the next
`try_start`, frames older than the previous turn are trimmed. A subscriber
one turn behind can resume seamlessly; a subscriber that fell outside the
retained window receives `resync`.

Cursor values are opaque on the wire. In-process cursors are `u64` values
with a generation in the high bits and sequence in the low bits. A
session reincarnation gets a new generation, so stale `Last-Event-Id`
values naturally fall below the new floor and trigger `resync`.

## Wire Contract

The SSE stream carries whole frames:

```text
id: 4294967297
event: text
data: "hello"

```

The `sse::` writers emit `event:` and `data:`. The bus reader prepends
`id:` when sending. Browser reconnect uses `Last-Event-Id`; the server
parses it as an opaque cursor.

Important events:

- `user_message` — live optimistic user message for the in-flight turn.
- `citations` — retrieved citation table, before text deltas.
- `text` — assistant token/text delta.
- `finish` — turn committed; carries persisted assistant message id.
- `clear` — turn cancelled or abandoned; clients roll back in-flight UI.
- `resync` — cursor fell outside the retained window; client refetches
  committed history and clears live overlay.
- `title` — best-effort live title update after first-turn title
  generation.

The stream is the only live path. The submitting tab does not receive a
special local echo.

## Worker Lifecycle

Starting a turn:

1. `POST /messages` validates conversation ownership and parent/staleness.
2. It calls `turn_bus.try_start`.
3. `409 {"reason":"in_flight"}` means another turn is already running.
4. On success, a worker owns the `TurnHandle`.

The worker is the only writer for a turn:

1. It emits citations if retrieval ran.
2. It streams LLM deltas as `text` frames.
3. It races the LLM stream against the cancellation token.
4. On cancel, it terminates with `clear` and writes nothing.
5. On completion, it commits user + assistant + citations atomically, then
   terminates with `finish`.

`TurnHandle::Drop` is the panic/abandon guard. If a worker exits without
terminating, the handle emits `clear` and releases the single-flight slot.
That makes `clear` XOR `finish` structural; there is no phase machine.

## Persistence

Durable message state lives in SurrealDB:

- user and assistant rows are written together by `commit_turn`,
- cancelled turns write no rows,
- assistant rows persist citations so reloaded history renders `[N]`
  markers without rerunning retrieval,
- title generation runs best-effort after the first assistant response and
  persists through `rename_conversation`.

The live log is a cache over this durable state. Any client told to
`resync` can recover by refetching history.

## Frontend Contract

`frontend/src/hooks/useChatStream.ts` treats SSE as authoritative for
in-flight state and history fetches as authoritative for committed state.

- On first connect, the hook refetches history.
- On `resync`, it clears live overlay/citations and refetches history.
- `user_message` resets the in-flight overlay for the new turn.
- `finish` triggers history reconciliation.
- `clear` rolls back the in-flight UI on every tab.

## NATS Migration

The future `NatsBus` keeps this contract:

- JetStream stream sequence becomes the cursor.
- JetStream stream retention replaces the in-process two-turn buffer.
- JetStream KV or a comparable CAS primitive owns the single-flight lock.
- Core NATS publishes cancellation to the running replica.
- A lease watcher emits `clear` for orphaned crashed turns.

See [`scaling-nats.md`](./scaling-nats.md) for the migration plan and
open decisions.
