# Chat streaming v3 — multi-tab via SSE + per-conversation session

## Context

The current chat surface (landed 2026-05-18, spec at
`docs/architecture/chat-streaming.md`) collapses everything into one POST
whose response body **is** the AI SDK data-stream. That is elegant for a
single-tab user but actively wrong for multi-tab:

- The user's other tabs (or other devices viewing the same conversation)
  don't see in-flight tokens, only the committed pair on refetch.
- The originating tab is special — only it can drive the stop button.
- A late-joining tab in the middle of a turn sees nothing live.

The v3 contract:

- **POST `/messages`** persists nothing immediately; just hands the turn
  to a backend worker and returns.
- **GET `/conversations/{id}/stream`** is a long-lived SSE every tab
  subscribes to on mount. It is the **single source of truth** for what
  the UI renders during a turn.
- The worker writes frames into a per-conversation buffer that fans out
  to every subscriber. A late-joining tab gets the buffer replayed on
  connect, then live frames.
- **POST `/conversations/{id}/stop`** kills the in-flight worker for
  that conversation, drops the buffer, and broadcasts a `clear` frame
  so every tab rolls back the in-flight overlay. **Scoped to the
  conversation, not to a turn id** — since the design enforces one
  in-flight turn per conversation, there is no ambiguity about which
  turn gets stopped, and the public API has no notion of a `task_id`.
- One turn per conversation at a time. A second POST while a turn is in
  progress → **409**. A late `/stop` → **204** (idempotent).
- User message is only persisted after a successful commit. `stop`
  therefore needs no DB rollback.
- The originating tab is **not special**: it POSTs, then waits for the
  SSE echo like everyone else. No optimistic insert, no dedup.

### Trade-off vs. v2 explicitly acknowledged

v2 allowed two tabs to submit simultaneously and resolved via
last-writer-wins at commit time. v3 rejects the second POST with 409.
This is a deliberate regression in the rare "two windows open and I
forgot" case, in exchange for a much simpler state machine, no wasted
LLM work, no LWW reasoning, and a clear multi-tab live-feedback story.

## Endpoint contract

| Method | Path | Body / response |
|---|---|---|
| `GET`  | `/api/chat/conversations/{key}` | Unchanged — DB-only history fetch. |
| `GET`  | `/api/chat/conversations/{key}/stream` | **New.** Long-lived SSE. On connect: replays current turn's buffer if any; then forwards live frames. `text/event-stream`. |
| `POST` | `/api/chat/conversations/{key}/messages` | Body `{id, text, parent_id}`. Returns **202 Accepted** (empty body) on success. **409 Conflict** if `parent_id` stale OR a turn is already in progress. Never streams. |
| `POST` | `/api/chat/conversations/{key}/stop` | **204** idempotent. Cancels the in-flight worker for the conversation (if any), drops the buffer, broadcasts `clear`. |

## Wire format — SSE events

Real SSE (`text/event-stream`), one event per line block:

```
event: user_message
data: {"id":"message:01J...","content":"hi"}

event: citations
data: [{"n":1,"chunkId":"chunk:...","docId":"document:...","docTitle":"...","page":3}]

event: text
data: "hello"

event: error
data: "llm stream error"

event: finish
data: {"finishReason":"stop","assistantMessageId":"message:..."}

event: clear
data: null
```

Plus a `:\n\n` heartbeat comment every 15s to keep proxies from idling
the connection.

`user_message` doubles as the turn-start signal. Frame ordering inside
one turn:

```
user_message → [citations] → text* → (finish | error+finish | clear)
```

Native browser `EventSource` auto-reconnects with backoff; on reconnect
the server replays the current turn's buffer so the tab catches up.

### Reset rule (load-bearing, do not skip)

**Every `user_message` event clears the assistant overlay and resets
the text accumulator on the client, unconditionally** — regardless of
whether it's a brand-new turn or a reconnect-replay of an existing
turn. This is the only way replay can be idempotent without per-event
`Last-Event-Id` bookkeeping (which we explicitly defer). Without it, a
mid-turn reconnect produces `hellohelloworld`.

## Session state

```rust
// backend/src/chat/session.rs (new)

pub struct SessionState {
    inner: Mutex<Inner>,   // std::sync::Mutex — no awaits held
}

struct Inner {
    current: Option<InFlightTurn>,
    subscribers: Vec<mpsc::Sender<Bytes>>,
}

pub struct InFlightTurn {
    pub task_id: TaskId,                 // internal only; for logs/tracing
    pub cancel: CancellationToken,
    pub frames: Vec<Bytes>,              // SSE-formatted bytes, replayable verbatim
    pub phase: TurnPhase,                // see commit/abort race below
}

#[derive(Clone, Copy)]
enum TurnPhase {
    Streaming,    // worker still pulling deltas; abort emits `clear`
    Committing,   // worker past LLM loop, inside commit_turn; abort is no-op
    Committed,    // commit_turn returned ok; `finish` already emitted (or about to)
}
```

Operations (all serialised by the same `Inner` mutex):

- `subscribe() -> mpsc::Receiver<Bytes>` — under lock: build new bounded
  `(tx, rx)` channel (capacity 4096), push current `frames` into `tx`,
  register `tx` in `subscribers`, return `rx`. The replay therefore
  cannot interleave with live frames; both are appended in order under
  the same lock.
- `start_turn(user_msg) -> Result<TaskId, AlreadyRunning>` —
  **construction order is load-bearing**. Under lock: if `current` is
  Some, return `AlreadyRunning`. Build `InFlightTurn { task_id, cancel,
  frames: vec![user_message_frame], phase: Streaming }`, write `current =
  Some(...)`, **then** fan out to subscribers. (Equivalently, fan out
  with the frame after it's already appended to `current.frames`.) The
  rule: a frame must be visible to future `subscribe()` callers
  (replay) before it's visible to any live subscriber, or both at the
  same instant.
- `emit(frame: Bytes)` — under lock: if `current` is None, drop the
  frame (worker raced an abort; nothing to do). Else append to
  `current.frames`, then `try_send` to every subscriber. On
  `try_send` Err (Closed or Full) → drop that subscriber from the list
  (the client will reconnect via EventSource and get a fresh replay).
- `enter_committing()` — under lock: if `current.phase == Streaming`,
  flip to `Committing` and return true. Else (already aborted) return
  false; worker should bail without further DB write.
- `finish(finish_frame: Bytes)` — under lock: emit `finish` to
  subscribers + buffer, set `phase = Committed`, then clear `current`.
- `abort()` — under lock: if `current` is Some AND
  `phase == Streaming`, cancel the token, emit `clear`, clear `current`.
  If `phase != Streaming`, cancel the token (harmless if already past)
  but **do not** emit `clear` and do not touch `current` — the worker
  will finish its commit and emit `finish` on its own. This is what
  closes the commit↔stop race.

```rust
// backend/src/chat/registry.rs (rewritten; old TaskRegistry deleted)

pub struct SessionRegistry {
    inner: Arc<DashMap<ConversationId, Arc<SessionState>>>,
}

impl SessionRegistry {
    pub fn for_conversation(&self, id: &ConversationId) -> Arc<SessionState>;
    pub fn lookup(&self, id: &ConversationId) -> Option<Arc<SessionState>>;
}
```

`ConversationId` is `surrealdb::RecordId` (already `Hash + Eq`); the
registry keys on the record id, not on the URL string. The `/stop`
handler parses the URL `{key}` to `ConversationId` for lookup.

GC: **none in v1**. One `SessionState` per ever-touched conversation
key. For single-user dev this is trivially bounded. For SaaS this is a
slow memory leak (entry + Vec retained capacity per ever-visited
conversation). Acceptable for v1; a `// TODO` notes the eviction
condition (`current is None && subscribers empty for > 1h`) for a
follow-up.

## SSE handler: avoiding pool starvation

**Critical.** The identity middleware attaches `Arc<AuthedDb>` as a
request extension that lives for the entire request. For a streaming
SSE response, that means one pool slot held per open tab — with default
`REQUEST_DB_POOL_SIZE=8`, the pool deadlocks after ~9 tabs.

Fix: the SSE handler performs the `get_conversation` permission check
up front while it still has the `AuthedDb`, then **drops the
extension** before constructing the streaming response body. The
streaming body itself does not touch the DB; it only fans bytes from
the session's mpsc to the SSE response, which doesn't need an
authenticated DB handle.

```rust
// backend/src/api/chat_stream.rs (new)

pub async fn stream(
    State(state): State<AppState>,
    mut req: axum::extract::Request,
    Path(key): Path<String>,
) -> Response {
    let conv_id = match parse_conversation_id(&key) {
        Ok(id) => id,
        Err(r) => return r,
    };
    // Permission check while we still hold the pooled handle.
    let db = req.extensions_mut().remove::<Arc<AuthedDb>>()
        .expect("identity middleware attaches AuthedDb");
    match db.get_conversation(&conv_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return (StatusCode::NOT_FOUND, "not found").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "stream: get_conversation failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response();
        }
    }
    drop(db); // <-- pool slot returned BEFORE we start streaming.

    let session = state.sessions.for_conversation(&conv_id);
    let rx = session.subscribe();
    let stream = ReceiverStream::new(rx).map(Ok::<Bytes, std::convert::Infallible>);
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}
```

(If `Extension` extraction in this position is awkward, the equivalent
fix is to mount the SSE route on a sub-router that runs only the JWT
extractor — not the AuthedDb attach — and have the handler call
`state.pool.acquire(&bearer)` itself for the perm check, then drop.)

## Backend module layout

```
backend/src/chat/
├── mod.rs        re-exports: SessionRegistry, SessionState, TaskId,
│                 spawn_worker, turn_request, TurnRequest
├── session.rs    NEW — SessionState, InFlightTurn, Inner, TurnPhase
├── registry.rs   REWRITTEN — SessionRegistry (DashMap-backed)
└── worker.rs     MODIFIED — emits via SessionState; phase transitions
```

`backend/src/api/`:

- `chat.rs` — POST handler returns 202; calls `session.start_turn`
  (which handles the 409 in-progress check), spawns worker, returns.
- `chat_stream.rs` — **new** GET handler. Permission check, drop
  `AuthedDb`, subscribe, SSE response with keep-alive.
- `chat_stop.rs` — modified. Permissions gate, call `session.abort()`.
  No task_id in the URL or handler.

`backend/src/api/stream.rs` → **rename to `sse.rs`**, rewrite writers
for SSE format:

```rust
pub fn user_message(id: &str, content: &str) -> Bytes
pub fn text(delta: &str) -> Bytes
pub fn citations(entries: &[CitationEntry]) -> Bytes
pub fn error(msg: &str) -> Bytes
pub fn finish(reason: &str, assistant_message_id: &str) -> Bytes
pub fn clear() -> Bytes
```

Each returns `Bytes` formatted as `event: <name>\ndata: <json>\n\n`.
**Snapshot tests preserve byte-level rigor** — exact `\n`, exact JSON
key ordering. Don't soften to "roughly-shaped" assertions; the snapshot
tests are the testing-strategy guardrail against silent protocol drift.

## Worker changes

`backend/src/chat/worker.rs`:

- Replace `tx: mpsc::Sender<Bytes>` parameter with
  `session: Arc<SessionState>`.
- Replace every `tx.try_send(Bytes::from(proto::X(...)))` with
  `session.emit(sse::X(...))`.
- Drop the leading task frame; `user_message` is emitted by
  `session.start_turn` synchronously from the POST handler.
- `spawn_worker` signature:
  ```rust
  pub fn spawn_worker(
      session: Arc<SessionState>,
      task_id: TaskId,
      cancel: CancellationToken,
      req: TurnRequest,
  );  // no return — POST already returned 202
  ```

### Commit/abort phase transition

```
1. LLM stream loop runs to Eof | Error (Cancelled handled here too).
2. If Cancelled: return. session.abort() already cleared everything.
3. session.enter_committing() → if false, return (raced an abort).
4. db.commit_turn(...) → assistant_id.
5. session.finish(sse::finish(reason, &assistant_id_str));
   (sets phase = Committed, emits + clears `current`.)
6. Spawn detached title generation (see below).
```

Title generation moves out of the request path: `tokio::spawn` it
**after** `session.finish` so the SSE `finish` is delivered immediately
and the UI unblocks. The title task acquires its own `AuthedDb` from
the pool, generates, calls `rename_conversation`, releases. The
existing `onTurnEnd` refetch in the frontend will pick up the new
title on the next sidebar refresh; if it lands a few hundred ms later,
no UX impact.

### Panic guard (do not skip)

Wrap the worker body in a guard that calls `session.abort()` on unwind.
Without it, a panic mid-turn leaves `current` permanently `Some`, and
every subsequent POST for that conversation returns 409 forever.

```rust
struct WorkerGuard { session: Arc<SessionState>, armed: bool }
impl Drop for WorkerGuard {
    fn drop(&mut self) {
        if self.armed { self.session.abort(); }
    }
}
// At the top of run(): let mut guard = WorkerGuard { session: ..., armed: true };
// Just before normal return: guard.armed = false;
```

The worker still holds an `AuthedDb` for the entire turn — same pool
sizing concerns as today. Unrelated to the SSE pool fix above (SSE is
the new long-lived endpoint).

## Storage / schema

**No schema changes.** The existing `message` table with `parent_id`
already supports last-writer-wins (which v3 doesn't need anymore, since
it rejects concurrent turns with 409 — but the field is harmless and
useful for any future relaxation). `commit_turn` signature stays
as-is.

## Frontend layout

```
frontend/src/hooks/
├── useChatStream.ts        NEW — replaces useSessionStream
├── useChatStream.test.ts   rewrite test against SSE events
```

`useSessionStream.ts` is deleted. The component import in
`components/chat/Chat.tsx` switches to `useChatStream`. The hook's
public API stays close to the existing shape:

```ts
type UseChatStreamReturn = {
  messages: LocalMessage[];
  status: "ready" | "submitted" | "streaming" | "error";
  citations: CitationEntry[];
  error: string | null;
  submit(text: string): Promise<void>;
  stop(): Promise<void>;
};
```

Internals:

- `useEffect` on mount opens `new EventSource("/api/chat/conversations/<key>/stream")`,
  registers named-event handlers, closes on unmount.
- State:
  - `messages`: seeded from `initialMessages`, appended on `user_message`
    and on `finish` (commits the assistant overlay).
  - `assistantOverlay: string` — accumulates `text` deltas; rendered as
    the last assistant message while `status === "streaming"`.
  - `status`: `submitted` from POST until first `text` arrives;
    `streaming` thereafter; `ready` after `finish`/`clear`.
- Event handlers:
  - `user_message`: **always** clear `assistantOverlay` (the reset
    rule). If the message id is not already in `messages`, append it.
    Set status to `streaming` (or keep as `submitted` until first
    `text`).
  - `text`: append delta to overlay; set status `streaming`.
  - `citations`: replace citations state.
  - `error`: set error state, status `error`. Wait for `finish` or
    `clear` to clean up.
  - `finish`: commit overlay as `{id: assistantMessageId, role:
    "assistant", content: overlay}` into `messages`; clear overlay;
    update `lastKnownMessageId = assistantMessageId`; status `ready`.
  - `clear`: drop overlay; if the last message in `messages` is the
    in-flight `user_message` (id matches the most recent
    `user_message` event whose `finish` we haven't seen), drop it
    too; status `ready`.
  - EventSource `onopen` (fires on every (re)connect): if
    `assistantOverlay` is non-empty OR status is not `ready`, trigger
    a history refetch via `onTurnEnd?.()` — this handles the
    quiet-window case where the turn ended while the connection was
    down and the buffer is now empty.
- `submit(text)`:
  1. POST `{id: ulid(), text, parent_id: lastKnownMessageId}`.
  2. Status → `submitted` immediately (UI swaps send→stop).
  3. **No optimistic insert** — the user message arrives via SSE
     within a few ms.
  4. On 409: set error toast "conversation changed", revert status to
     `ready`, fire `onTurnEnd` so the caller refetches history.
- `stop()`: POST `/conversations/{key}/stop` (fire and forget). UI
  rollback comes via the SSE `clear` event. **No `task_id` — stop is
  scoped to the conversation; the in-flight turn (if any) is the
  target.** The originating tab can stop immediately regardless of SSE
  state; the URL alone identifies the target.

`frontend/src/lib/api.ts`:

- `submitMessage(key, body)` returns `{ ok: true } | { ok: false; status: number }`
  (no body streaming).
- `stopTask` → `stopChat(key)` — drops the task-id argument; URL
  changes to `/conversations/{key}/stop`.
- `getConversation` unchanged.

## Files modified

| File | Change |
|---|---|
| `backend/src/chat/mod.rs` | Re-exports updated (new `SessionRegistry`, drop public `TaskRegistry`) |
| `backend/src/chat/registry.rs` | Rewrite: `SessionRegistry` over `DashMap<ConversationId, Arc<SessionState>>` |
| `backend/src/chat/session.rs` | **New** — `SessionState`, `InFlightTurn`, `Inner`, `TurnPhase` |
| `backend/src/chat/worker.rs` | Worker emits via `session.emit(...)`; phase transitions; panic guard; detached title gen |
| `backend/src/api/stream.rs` → `sse.rs` | Rename + rewrite writers for SSE format; new `user_message`, `clear`; drop `task`. Snapshot tests preserved at byte level. |
| `backend/src/api/chat.rs` | POST returns 202; routes to `session.start_turn`, spawns worker |
| `backend/src/api/chat_stream.rs` | **New** — GET handler builds SSE response; drops `AuthedDb` extension before streaming |
| `backend/src/api/chat_stop.rs` | URL drops `/tasks/{task_id}`; calls `session.abort()` |
| `backend/src/api/mod.rs` | Add `GET /conversations/{key}/stream`; change `/stop` route |
| `backend/src/state.rs` | Replace `tasks: Arc<TaskRegistry>` with `sessions: Arc<SessionRegistry>` |
| `backend/src/lib.rs` | Construct `SessionRegistry::new()` instead of `TaskRegistry::new()` |
| `frontend/src/hooks/useSessionStream.ts` | Delete |
| `frontend/src/hooks/useChatStream.ts` | **New** — EventSource-driven |
| `frontend/src/hooks/useSessionStream.test.ts` | Rename + rewrite for SSE events |
| `frontend/src/components/chat/Chat.tsx` | Swap import; minor wiring changes |
| `frontend/src/lib/api.ts` | `submitMessage` returns `{ok, status}`; `stopChat(key)` |
| `backend/tests/chat_streaming.rs` | Rewrite: subscribe to SSE, POST, assert event order |
| `backend/tests/chat_stop.rs` | Update for new URL + buffer-clear semantics |
| `backend/tests/chat_concurrent_post.rs` | **New** — second POST during in-flight turn → 409 |
| `backend/tests/chat_late_subscribe.rs` | **New** — subscribe mid-turn, assert replay |
| `backend/tests/chat_commit_abort_race.rs` | **New** — stop arriving during commit must not produce ghost messages |
| `docs/architecture/chat-streaming.md` | Rewrite for v3 design (lands with step 10) |

## Implementation order

The chat surface temporarily breaks across several steps. **Step 6 is
the resync point — the build is green and tests pass from step 6
onward.** Steps 1-5 produce intermediate compile errors that are
healed by the subsequent step; do not panic when the build is red
mid-sequence.

Commit per step, short message matching the recent house style (no
Claude trailer).

1. **SSE writers** — rename `api/stream.rs` to `api/sse.rs`, rewrite
   to SSE format, add `user_message`, `clear`, drop `task` + `finish`'s
   AI-SDK shape replaced by SSE shape. Update snapshot tests at
   byte-level rigor. Worker and chat handler still reference the old
   `proto::` symbols → break, fixed in step 3.

2. **Session module** — add `backend/src/chat/session.rs` with
   `SessionState`, `Inner`, `InFlightTurn`, `TurnPhase`. Rewrite
   `registry.rs` to `SessionRegistry`. Update `chat::mod.rs` exports.
   Update `state.rs` field rename and `lib.rs` construction. Chat
   handlers still reference old types → break, fixed in steps 3-5.
   Add unit tests in `session.rs`:
   - subscribe-then-emit ordering (replay sees frames in same order as
     live subscribers)
   - reject-second-start (`AlreadyRunning`)
   - replay-on-subscribe (frames appended before subscriber registration
     are pushed into the new channel)
   - prune-dead-subscribers (closed mpsc removed on next emit)
   - phase guard (abort during Committing is a no-op for clear/current)

3. **Rewrite worker** — switch `spawn_worker` to take
   `Arc<SessionState>` + cancel token, swap all `tx.try_send` for
   `session.emit(...)`, add phase transitions (`enter_committing`,
   `finish`), add panic guard (`WorkerGuard`), detach title generation
   into `tokio::spawn` after `finish`. `cargo build` passes; handlers
   still broken.

4. **Rewrite POST handler** — `api/chat.rs::post_message` returns 202,
   calls `session.start_turn` (409 if `AlreadyRunning`), spawns worker,
   returns. Drops the body-stream construction.

5. **Add GET stream handler** — `api/chat_stream.rs`: permission check
   while holding `AuthedDb`, **then drop it explicitly**, then
   `session.subscribe()` → `ReceiverStream` →
   `axum::response::sse::Sse` with
   `.keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))`.
   Mount route in `api/mod.rs`.

6. **Rewrite stop handler** — `api/chat_stop.rs` becomes
   conversation-scoped: parse URL `{key}`, perm check, look up session,
   call `session.abort()`. Always 204. No `task_id` in the URL pattern
   or handler. Drop the `/tasks/{task_id}` segment from the routes in
   `api/mod.rs`. **Resync point: `cargo build && cargo test` should be
   green now**, modulo the chat integration tests, which step 7 fixes.

7. **Update / add backend integration tests**:
   - **Rewrite** `chat_streaming.rs`: spawn the app, subscribe to SSE
     via `tower::ServiceExt::oneshot` + chunk-reader, POST the message,
     assert event order (`user_message`, `text`s, `finish`), assert DB
     has the committed pair.
   - **Update** `chat_stop.rs`: assert `clear` event arrives on
     subscribers, assert no DB rows from the stopped turn, new URL.
   - **Add** `chat_concurrent_post.rs`: open one turn, POST a second
     time → 409.
   - **Add** `chat_late_subscribe.rs`: start a turn, mid-stream open a
     second subscriber, assert it receives `user_message` + at least
     the deltas emitted so far.
   - **Add** `chat_commit_abort_race.rs`: scripted FakeLlm finishes
     instantly; fire `/stop` concurrently with the commit window;
     assert either (a) we see `clear` and no DB rows, OR (b) we see
     `finish` and the rows are persisted — never the inconsistent
     "clear emitted but rows in DB" combination.
   - `chat_last_writer_wins.rs`: keep as-is (storage-level guarantee
     still holds even though v3 doesn't trigger LWW under normal flow).
   - Verify: `cargo test`.

8. **Frontend hook rewrite** — replace `useSessionStream.ts` with
   `useChatStream.ts` driven by `EventSource`. Implement the reset
   rule on every `user_message`. Implement the refetch-on-reopen rule
   when overlay is non-empty. Update Vitest file: stub
   `window.EventSource`, push events synchronously, assert state.
   - Verify: `make frontend-test`.

9. **API helpers + component wiring** — update `lib/api.ts`
   (`submitMessage` no longer returns streaming Response; `stopChat`
   replaces `stopTask`); `components/chat/Chat.tsx` swap import.
   - Verify: `make frontend-test` and a `bun run build`.

10. **Update spec doc** — rewrite
    `docs/architecture/chat-streaming.md` for v3. Land in the same
    commit as step 11 so docs and behaviour match.

11. **End-to-end smoke** — Tier 1 stack:
    - Open a conversation, send a message, see streaming + commit.
    - Open the same conversation in a second tab. Send from tab A; tab
      B sees `user_message` + token stream live.
    - In tab B, click stop mid-stream. Both tabs roll back via `clear`;
      neither DB-persisted message survives.
    - Send a message; while it's streaming, send another from tab B →
      tab B sees a 409 toast; nothing else changes.
    - Refresh tab B mid-stream → it reconnects, replays the buffer,
      shows the same partial assistant content tab A is seeing (and
      `user_message` reset rule means the overlay isn't doubled).
    - Disconnect network on tab B mid-stream, wait for the turn to
      finish naturally on tab A, reconnect tab B → SSE reopens, no
      replay (turn over), hook refetches history, tab B catches up.

## Reused infrastructure

These exist today and stay verbatim:

- `Storage::commit_turn` and the last-writer-wins delete inside it —
  `backend/src/storage/surreal.rs`. (Not load-bearing for v3 but
  harmless.)
- `TaskId` (just `Ulid` wrapper) — kept for internal logging /
  tracing; not exposed in the public API.
- LLM trait + `stream_chat` + `LlmDelta::Text` — `backend/src/llm/mod.rs`.
- RAG helpers in the worker (`retrieve_for_query`, `build_system_prompt`,
  `citation_entries`, `history_to_llm`) — `backend/src/chat/worker.rs`.
- Title generation (`generate_title`, `clean_title`) — moved into a
  detached task; bodies unchanged.
- `AuthedDb` pool + identity middleware — used unchanged on POST /
  GET-history / stop; the SSE handler explicitly drops the extension
  before streaming.
- `Conversation` / `ChatMessage` / `MessageId` types and
  `list_messages` for the GET history endpoint.
- Frontend: `Chat.tsx` rendering (turn grouping, scroll machinery,
  citations rendering), `ConversationSidebar`, `useConversations`,
  TanStack Router loader, `MessageBody`, `PromptInputSubmit`.

## Verification (end-to-end)

After all steps land:

```bash
# Backend
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test

# Frontend
make frontend-test
cd frontend && bun run build

# Tier 1 smoke (manual)
make down && make up
# Then run through the 6 scenarios in step 11.
```

E2E Playwright suite under `tests/`: an existing
`chat-persistence.spec.ts` should still pass after `submitMessage`'s
return-shape change is reflected in the test helper. A new optional
`chat-multi-tab.spec.ts` with two browser contexts would lock in the
"second context sees live tokens" property; not required for merge.

## What this plan does NOT do

- No schema migration. `message` table is already shaped right.
- No new auth / identity work, but the SSE handler is required to
  drop its `AuthedDb` extension before streaming — this is structural,
  not a config knob.
- No session-eviction / GC logic. One `SessionState` per ever-touched
  conversation; bounded in practice for the v1 use cases. A TODO marks
  the eviction trigger condition for a follow-up.
- No `Last-Event-Id` resumption. EventSource auto-reconnects and the
  server replays the current turn's buffer — that, plus the reset rule
  on `user_message`, is sufficient for idempotency. Resumption across
  already-committed turns relies on the GET history refetch the hook
  fires on every SSE `(re)open` while overlay is non-empty.
- No change to corpus chat vs. document chat split — there's only the
  corpus chat surface today, and both pillars converge here when the
  document chat lands.
- No re-enabling of concurrent turns per conversation. Documented
  trade-off vs. v2: stricter, simpler, costs the rare two-windows
  case.
