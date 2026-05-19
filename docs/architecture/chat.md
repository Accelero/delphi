# Chat Streaming — v3 (multi-tab via SSE)

Status: shipped. Sister doc to [`ARCH.md`](../ARCH.md). Implementation
plan and design rationale live in `chat-streaming-v3-plan.md` (the
land-time spec used by the implementing agent — kept around as the
detailed reference).

## Implements

This document is the architecture side of the
[`specs/chat.md`](../specs/chat.md) functional spec. Cross-reference
of arch sections to spec requirements:

| Arch section | Spec requirements satisfied |
|---|---|
| [Endpoints](#endpoints) — `GET /conversations/{key}` | R1 (history is authoritative read) |
| [Endpoints](#endpoints) — `GET /stream` + [Wire format](#wire-format) | R2 (multi-tab live updates), R10 (citations in-band) |
| [Late-join replay](#late-join-and-reconnect) via `SessionState.subscribe()` | R3 (late join), R4 (reconnect) |
| [Endpoints](#endpoints) — `POST /stop` + [`abort()` semantics](#commitabort-race) + `clear` SSE frame | R5 (conv-scoped stop visible to all tabs) |
| [Concurrency: one turn per conversation](#concurrency-one-turn-per-conversation) | R6 (single-flight per conversation), R8 (optimistic concurrency) |
| [Session state](#session-state) — commit-at-end via `commit_turn` | R7 (atomic commit) |
| Frontend hook — `submitted/streaming` status drives the Stop UI | R9 (stop visibility tracks live state) |

## One-sentence summary

> POST `/messages` is fire-and-forget; a long-lived SSE stream per
> conversation is the single source of truth that every tab subscribes
> to; `/stop` is scoped to the conversation; user message + assistant
> reply are written atomically to DB only at the end of a successful
> turn.

## Endpoints

| Method | Path | Body / response |
|---|---|---|
| `GET`  | `/api/chat/conversations/{key}` | Committed history + conversation metadata. No coordination with workers. |
| `GET`  | `/api/chat/conversations/{key}/stream` | Long-lived `text/event-stream`. On connect: replays the current turn's buffered frames (if any), then forwards live frames as the worker emits them. `:\n\n` heartbeat every 15s. |
| `POST` | `/api/chat/conversations/{key}/messages` | Body `{ id, text, parent_id }`. **202 Accepted** with empty body on success. **409 Conflict** if `parent_id` is stale (`reason: "stale_parent"`) or a turn is already in flight (`reason: "in_flight"`). Never streams. |
| `POST` | `/api/chat/conversations/{key}/stop` | Cancels the in-flight turn (if any), drops the buffer, broadcasts a `clear` frame. **204** idempotent. **No `task_id` segment** — stop is conversation-scoped. |

There is **no** `task_id` in the public HTTP surface. Internally
[`TaskId`] is kept as a log/trace correlation handle.

## Wire format

Real SSE (`text/event-stream`). Each frame is `event: <name>\ndata:
<json>\n\n`. Native browser `EventSource` parses it; named-event
handlers fire on the per-event JS callbacks.

```
event: user_message
data: {"id":"message:01J...","content":"hi"}

event: citations
data: [{"n":1,"chunk_id":"chunk:...","doc_id":"document:...","doc_title":"...","page":3}]

event: text
data: "hello"

event: error
data: "llm stream error"

event: finish
data: {"finishReason":"stop","assistantMessageId":"message:..."}

event: clear
data: null
```

Frame ordering for one turn:

```
user_message → [citations] → text* → (finish | error+finish | clear)
```

### Reset rule (load-bearing)

**Every `user_message` event clears the client's assistant overlay and
text accumulator, unconditionally** — regardless of whether it's a
brand-new turn or a reconnect-replay of an existing turn. This is the
only way replay can be idempotent without per-event `Last-Event-Id`
bookkeeping. Without it, a mid-turn reconnect produces
`hellohelloworld`.

## Session state

`backend/src/chat/session.rs` owns one [`SessionState`] per
ever-touched conversation; the [`SessionRegistry`] (`registry.rs`)
keys them by `ConversationId`. Each `SessionState` serialises:

- the in-flight turn's [`CancellationToken`]
- a `Vec<Bytes>` of SSE-formatted frames for replay-on-subscribe
- a `Vec<mpsc::Sender<Bytes>>` of live subscribers
- a [`TurnPhase`] (`Streaming` → `Committing` → `Committed`)

Concurrency: a single `std::sync::Mutex` guards inner state. No
`.await` is ever held under the lock. Operations are short and at
chat-rate cadence so contention is negligible.

### Commit↔abort race

The phase machine closes the bug where a `/stop` arrives between the
last LLM delta and the `commit_turn` returning. Algorithm:

1. Worker exits LLM loop, calls `enter_committing()`. If it returns
   `false` (raced an abort), worker bails without writing to DB.
2. On `true`, worker runs `commit_turn`, then `finish()` (emits
   `finish`, clears `current`).
3. `abort()` only emits `clear` and clears `current` when phase is
   `Streaming`. During `Committing`/`Committed` it's a no-op for both
   the wire and the buffer — the worker will see its commit through.

Two outcomes are admissible; the inconsistent "clear emitted AND rows
persisted" never happens.

### Panic guard

The worker body runs inside a `WorkerGuard` whose `Drop` calls
`session.abort()` on unwind. Without it a panic mid-turn would leave
`current` permanently `Some` and every subsequent POST for that
conversation would return 409 forever.

### GC

None in v1. One `SessionState` per ever-touched conversation key
leaks (a `Vec<Bytes>` capacity per visited conversation). Acceptable
for the v1 use cases; eviction `current.is_none() &&
subscribers.is_empty() && idle_for > 1h` is tracked as a
`TODO(v2)` in `registry.rs`.

## Pool-starvation fix (SSE handler, structural)

The identity middleware attaches `Arc<AuthedDb>` to every protected
request. For a long-lived SSE response this would mean one pool slot
held per open tab — the pool deadlocks at `REQUEST_DB_POOL_SIZE + 1`
tabs.

Fix: the SSE handler in `backend/src/api/chat_stream.rs` pulls
`AuthedDb` out of the request extensions explicitly, performs the
`get_conversation` permission check, then **drops the handle before
constructing the streaming body**. The streaming body itself only
fans bytes from the session's mpsc into the SSE response and doesn't
need a DB handle.

This is structural, not a config knob; we don't ship without it.

## End-to-end multi-tab flow

A worked example of two tabs (A and B) open on the same conversation,
where A submits and both render the live stream. Time flows top to
bottom; horizontal axis is process boundary.

```
Tab A                Tab B                Backend (POST/stream/stop      Worker             DB
                                          + SessionState)
  │                    │                       │                            │                │
  │── GET /…/{key} ───────────────────────────►│ list_messages              │                │
  │◄── history JSON ───────────────────────────│                            │                │
  │── GET /…/stream (open EventSource) ───────►│ subscribe()  → mpsc        │                │
  │                    │                       │   (no current turn,        │                │
  │                    │                       │    replay buffer empty)    │                │
  │                    │── GET /…/{key} ──────►│ list_messages              │                │
  │                    │◄── history JSON ──────│                            │                │
  │                    │── GET /…/stream ─────►│ subscribe()  → mpsc        │                │
  │                    │                       │                            │                │
  │── POST /…/messages ───────────────────────►│ start_turn(user_msg)       │                │
  │                                            │  ├─ build user_message     │                │
  │                                            │  │  frame; current=Some(…) │                │
  │                                            │  └─ fan out → both mpsc    │                │
  │◄── 202 Accepted (empty body) ──────────────│                            │                │
  │                                            │ spawn worker ─────────────►│                │
  │◄══ event: user_message ════════════════════│                            │                │
  │                    │◄══ event: user_msg ═══│                            │                │
  │                                            │                            │ pool.acquire() │
  │                                            │                            │ list_messages ├►│
  │                                            │                            │ retrieve_for_… │
  │                                            │ ←── session.emit(citations)│                │
  │◄══ event: citations ═══════════════════════│                            │                │
  │                    │◄══ event: citations ══│                            │                │
  │                                            │                            │ llm.stream_chat│
  │                                            │ ←── session.emit(text)     │ loop: deltas   │
  │◄══ event: text "He" ═══════════════════════│                            │                │
  │                    │◄══ event: text "He" ══│                            │                │
  │◄══ event: text "llo" ══════════════════════│                            │                │
  │                    │                       │                            │                │
  │ User clicks Stop in tab A                  │                            │                │
  │── POST /…/stop ────────────────────────────►│ session.abort()           │                │
  │                                            │  phase==Streaming →        │                │
  │                                            │  ├─ cancel.cancel()        │ select! wakes  │
  │                                            │  ├─ build clear frame      │ break Cancelled│
  │                                            │  └─ fan out → both mpsc    │                │
  │◄── 204 No Content ─────────────────────────│  clear current             │ (no DB write)  │
  │◄══ event: clear ═══════════════════════════│                            │                │
  │                    │◄══ event: clear ══════│                            │                │
  │                    │                       │                            │ guard.armed=false
  │ overlay dropped,                           │                            │ pool.release   │
  │ user msg dropped                           │                            │                │
  │                    │ same rollback         │                            │                │
```

The non-stop / natural-finish branch instead ends with:

```
                                              │ ←── enter_committing() ok  │
                                              │                            │ db.commit_turn ├►│
                                              │                            │   (user+assistant
                                              │                            │    persisted)   │
                                              │ ←── session.finish(…)      │                │
  ◄══ event: finish (assistantMessageId) ═════│                            │                │
                       ◄══ event: finish ═════│  current cleared           │                │
                                              │                            │ detached title │
                                              │                            │   tokio::spawn ├►│
```

The originating tab (A) and the late tab (B) consume exactly the same
event sequence; "A initiated the POST" leaves no trace on the SSE wire.

If a third tab connects mid-stream (e.g. after the second `text` frame),
the `subscribe()` snapshot pushes the buffered frames — `user_message`,
`citations`, both `text`s — into its mpsc under the lock before any
further live frame is fanned out, so it observes the same prefix and
then joins live deltas. This is the late-join (R3) mechanism.

## Disconnect behaviour

- **Single tab closes mid-turn:** the worker keeps going. On the next
  `text` it emits, every still-connected subscriber receives the
  frame; the disconnected one drops out of the subscriber list
  (closed mpsc) but the worker still completes and commits.
- **Network blip, EventSource auto-reconnects:** browser opens a new
  GET to the stream URL after backoff. The handler replays the
  current turn's buffer, then forwards live frames. The reset rule
  on `user_message` keeps the client overlay consistent.
- **Tab subscribes after the turn ended:** the buffer was cleared on
  `finish` / `clear`, so subscribe gets no replay. The `onopen`
  refetch-when-overlay-non-empty rule on the client doesn't fire (no
  overlay), so the tab just shows the committed history from the
  initial GET. Cross-tab consistency comes via the
  `queryClient.invalidateQueries` the originating tab fires from its
  `onTurnEnd`.

## Late join and reconnect

Mechanism for spec [R3](../specs/chat.md#r3-late-join-replay) and
[R4](../specs/chat.md#r4-reconnect-tolerates-network-blips):

- **Late join (turn in progress):** `SessionState.subscribe()` builds
  the new subscriber's mpsc, drains `current.frames` into it
  synchronously **under the same lock that worker `emit()` takes**, and
  only then registers the subscriber for future fan-out. The two
  orderings cannot interleave; the new subscriber is guaranteed to see
  the buffered prefix followed by every subsequent live frame, with no
  duplicates and no missed deltas.
- **Reconnect, turn still running:** `EventSource` reopens. Backend
  treats it as a fresh subscription — same replay-then-live mechanism.
  The client's `user_message` event handler unconditionally resets the
  assistant overlay; this is what closes the "rebuild idempotently"
  invariant. Without the reset, a re-delivered `text "He"` followed by
  the live `text "llo"` would render `HeHello`.
- **Reconnect, turn ended while down:** Backend's `current` is `None`,
  replay is empty, no events arrive. Client's `onopen` callback checks
  whether the local overlay is non-empty; if it is, it fires
  `onTurnEnd?.()`, which makes the caller refetch history. The
  committed pair replaces the stale overlay.

## Concurrency: one turn per conversation

The second POST against a conversation with an in-flight turn returns
**409** with `reason: "in_flight"`. This is a deliberate regression
vs. v2 (which allowed two tabs to submit simultaneously and resolved
via last-writer-wins at commit time) in exchange for:

- much simpler state machine,
- no wasted LLM work,
- no LWW reasoning across two workers,
- a clear multi-tab live-feedback story.

The `commit_turn` storage-layer LWW behaviour is preserved as a
defence-in-depth but is no longer the primary concurrency mechanism.

## Frontend hook contract

`useChatStream(conversationKey, opts)` (in `frontend/src/hooks/`):

- Opens an `EventSource` on mount, closes on unmount.
- Registers per-event handlers (`user_message`, `text`, `citations`,
  `error`, `finish`, `clear`) and an `open` handler that triggers
  `onTurnEnd?.()` whenever the overlay is non-empty (catches the
  case where the turn finished while the connection was down).
- `submit(text)` POSTs `{id: ulid(), text, parent_id}`; on 202 returns
  and waits for SSE. On 409 sets a "conversation changed" error,
  fires `onTurnEnd`. **No optimistic insert** — the user message
  arrives via SSE.
- `stop()` POSTs `/stop` (fire-and-forget). Rollback comes via the
  SSE `clear` event.

## Files

| File | Role |
|---|---|
| `backend/src/api/sse.rs` | SSE writers (`user_message`, `text`, `citations`, `error`, `finish`, `clear`). Snapshot tests pin byte layout. |
| `backend/src/api/chat.rs` | POST handler. 202 on success, 409 on stale-parent / in-flight. |
| `backend/src/api/chat_stream.rs` | GET stream handler. Drops `AuthedDb` extension before streaming. |
| `backend/src/api/chat_stop.rs` | POST /stop handler. Conversation-scoped. |
| `backend/src/chat/session.rs` | `SessionState`, `InFlightTurn`, `TurnPhase`. |
| `backend/src/chat/registry.rs` | `SessionRegistry` (DashMap<ConversationId, Arc<SessionState>>). |
| `backend/src/chat/worker.rs` | Per-turn worker; emits via `session.emit`. Panic guard + commit-phase flip. |
| `frontend/src/hooks/useChatStream.ts` | EventSource-driven hook. |
| `frontend/src/lib/api.ts` | `submitMessage` → `{ok}|{ok,status}`; `stopChat(key)`. |

## What this v3 does NOT do

- No schema migration. `message` is already shaped right.
- No instant revocation hook for stop (cancel-during-commit relies on
  the phase machine, not a backend-side blacklist).
- No GC of `SessionState` entries (v1 use cases are bounded; TODO in
  `registry.rs`).
- No `Last-Event-Id` resumption. EventSource auto-reconnect +
  replay-on-subscribe + the `user_message` reset rule cover it.
- No re-enabling of concurrent turns per conversation.

[`TaskId`]: ../../backend/src/chat/registry.rs
[`SessionState`]: ../../backend/src/chat/session.rs
[`SessionRegistry`]: ../../backend/src/chat/registry.rs
[`TurnPhase`]: ../../backend/src/chat/session.rs
[`CancellationToken`]: https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html
