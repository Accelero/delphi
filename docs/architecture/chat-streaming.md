# Chat Streaming — Detached Per-Session Worker

Status: design, not yet implemented. Companion to
[`ARCH.md`](../ARCH.md) (Backend / Frontend sections) and
[`testing.md`](./testing.md).

## Problem

Today the streaming chat endpoint runs the LLM call **inside the POST
request that submitted the message**. The response body is the stream;
when the submitting tab closes or navigates away, the connection drops,
the worker is cancelled, and any in-flight tokens are lost. Other tabs
of the same conversation see nothing — there is no fan-out. A tab
opened mid-turn sees no live stream and (until the worker commits) no
DB row either.

Symptoms users hit:

- Switching the active conversation in the UI mid-stream loses the
  reply.
- Two browser tabs of the same chat see divergent state.
- Closing the tab that submitted a long-running message kills it.

## Goals

1. **The turn survives the submitting tab.** Once the LLM call starts,
   it runs to completion (and commits to DB) regardless of who is
   connected.
2. **Fan-out across tabs.** Every tab attached to a session sees the
   same byte stream as it arrives.
3. **Late-joiners catch up.** A tab opening the conversation URL
   mid-turn replays the tokens already produced, then tails the rest.
4. **Single source of truth.** The in-flight turn lives in either the
   live session state or the DB, never both, never neither.
5. **No churn on the Vercel AI SDK protocol bytes.** Existing
   `proto::text` / `proto::error` / `proto::finish` writers stay as-is;
   citations frame stays as-is. Wire format unchanged.

## Non-goals

- **Resilience to backend crashes mid-stream.** Explicitly out of
  scope. A crashed backend loses the in-flight turn; the user resubmits.
- **Multi-instance backend.** SessionState is in-process. Going
  multi-instance later requires either sticky routing per session or
  moving the buffer/notify onto Redis. Acceptable for current
  deployment shape (single backend per ARCH.md).
- **Incremental DB writes per delta.** We persist once, at stream end.

## Design overview

### The three primitives

Per active session, one `SessionState`:

```rust
struct SessionState {
    buf: RwLock<BytesMut>,        // append-only byte log of framed bytes
    base_offset: AtomicU64,       // absolute position of buf[0] (for cleared windows)
    notify: Notify,               // pulsed by worker on each append
    turn_lock: Semaphore,         // permits = 1; serialises concurrent turns
    finalize_lock: Mutex<()>,     // held during DB-write + registry-remove
}
```

- **`buf` is the file-like object.** Writer appends complete framed
  bytes (one or more whole `proto::*` records per `extend_from_slice`).
  Readers slice `buf[(cursor - base_offset)..]`, copy out, release
  lock, write to socket.
- **`base_offset`** lets the worker `buf.clear()` between turns
  without invalidating reader cursors. Readers hold `cursor: u64`
  (absolute byte position); a cleared buffer makes their slice empty
  on the next read, then they park on `notify` until the next turn.
- **`notify`** is the wake-up. Worker calls `notify_waiters()` after
  every append. Readers register `notified()` **before** draining and
  await **after** — `Notify`'s one-permit semantics make this race-
  free without dedupe logic.
- **`turn_lock`** ensures one in-flight turn per session, in
  submission order. Two tabs submitting concurrently → second waits.
- **`finalize_lock`** is held during the DB-commit + registry-remove
  critical section, and acquired by the new-tab handshake before it
  decides "live session present?" vs "load from DB only." Prevents the
  duplicate-message race described below.

### Lifetime

- **Registry**: `RwLock<HashMap<ConversationId, Weak<SessionState>>>`.
- **Creation**: any participant (writer or reader) that needs the
  session takes the registry write-lock, tries `Weak::upgrade()`; if
  it fails (or no entry), constructs a fresh `SessionState`, stores
  the `Weak`, returns the `Arc`.
- **Death**: refcount. State drops exactly when the last `Arc` is
  released — i.e. last reader closed and no writer running. The
  registry's `Weak` goes dead and is reaped on next lookup (lazy).
- A reader that stays attached across multiple turns keeps the state
  warm; the buffer is cleared after each commit, but the state object
  survives.

### Worker (per-turn)

Spawned by the POST handler. Holds an `Arc<SessionState>` and a
snapshot of the caller's claims (see [Worker DB access](#worker-db-access)).

1. `turn_lock.acquire().await` — wait for any prior turn to commit.
2. Build the prompt: history (DB) + new user message + RAG citations.
   Persist the user message to DB *now*, so a crash leaves it in the
   log.
3. Open the LLM stream.
4. Per delta: build the framed bytes via `proto::text(...)`, take
   `buf.write()`, `extend_from_slice`, drop lock, `notify_waiters()`.
5. On stream end: emit `proto::finish(...)` into the buffer
   identically. **Do not** close the buffer or signal EOF to readers;
   the session may continue with another turn.
6. Acquire `finalize_lock`, write assistant message to DB (via the
   worker's own freshly-checked-out `AuthedDb`), then with the
   registry write-lock and `buf.write()` held simultaneously: clear
   the buffer, advance `base_offset` by old length. Release in
   reverse. The `Weak` stays installed; if no `Arc` other than the
   worker's exists, dropping the worker's `Arc` next collects the
   session.
7. Drop the `Arc`, releasing `turn_lock` via Drop.

Title generation runs after step 6, in the same task, using the same
fresh `AuthedDb`. The frontend re-fetches the conversation row on
next poll (or via SSE — see [Open](#open-questions)). It does not
block the finish marker, because the finish marker is already in the
buffer from step 5.

### Reader (per SSE connection)

`SessionReader { state: Arc<SessionState>, cursor: u64 }`. Implements
`tokio::io::AsyncRead`. `poll_read`:

1. `notified = state.notify.notified(); tokio::pin!(notified);` —
   register the wake-up future first.
2. Take `state.buf.read()`. Compute `i = (cursor - base_offset) as
   usize`. If `i < buf.len()`, copy `buf[i..]` (capped to caller's
   buffer length) into the caller's buffer, advance `cursor`, drop
   lock, return `Poll::Ready(Ok(n))`.
3. If caught up, drop lock, poll `notified` once; if `Ready`, loop;
   else `Poll::Pending` with the waker registered.

The reader **never returns `Ok(0)`** under this design — the session
has no EOF. The SSE connection closes when the client disconnects
(the axum response stream drops, `SessionReader` drops, refcount
falls). Browser refresh / tab close → kernel sends FIN → axum drops.

### Handshake (the duplicate-message race)

A new tab opens `/corpus/$sessionId` and needs both history and a
live subscription. The endpoint MUST do both under one critical
section so the worker can't commit-and-remove between them:

```rust
let session_guard = state.session_registry.handshake_lock(id).await;
// Take a *cheap* registry read first to find or create state for the
// session — but DO NOT subscribe yet.
let live = state.session_registry.get_or_create(id).await;
let _hold = live.finalize_lock.lock().await;   // blocks worker commit
let history = db.list_messages(id).await?;     // committed turns only
let reader  = SessionReader::new(live.clone()); // attaches at current cursor
drop(_hold);
drop(session_guard);
// Return (history, reader). Frontend renders history then appends
// reader bytes (which contain at most the current in-flight turn).
```

The invariant: the in-flight turn is either in `history` (worker
already committed before the handshake reached `finalize_lock`) or in
`reader`'s upcoming bytes (worker is still streaming or hasn't
started). Never both. Never neither.

## API contract

Three endpoints. POST decouples from streaming; the SSE stream is a
separate long-lived GET.

| Method | Path | Purpose |
|---|---|---|
| `GET`  | `/api/chat/conversations/{id}` | Committed history + conversation metadata. **Unchanged shape**, but the implementation acquires `finalize_lock` to coordinate with the worker (see Handshake). |
| `POST` | `/api/chat/conversations/{id}/messages` | Submit one user message. Returns `202 Accepted` with empty body once the turn is enqueued (i.e. the user message is persisted and the worker is spawned). Does **not** stream the response. |
| `GET`  | `/api/chat/conversations/{id}/stream` | Open SSE subscription. Returns `text/event-stream` (or, equivalently, the AI SDK data-stream bytes) and stays open until the client disconnects. Replays any in-flight turn from the buffer's current `base_offset` then tails. |
| `POST` | `/api/chat/conversations/{id}/stop` | Stop the in-flight turn for this conversation. Idempotent: returns `204 No Content` whether or not a turn was active. Any tab may call it; all tabs see the stop reflected in the stream. |

Notes:

- The submitting tab opens the GET stream **before** sending the POST
  (or at session-page mount; see Frontend) so it doesn't miss bytes.
- 202 from POST is fire-and-forget from the submitter's perspective.
  If `turn_lock` is held by a prior turn, the POST handler still
  returns 202 once the user message is persisted; the worker awaits
  the semaphore before driving the LLM call.
- Auth: GET stream goes through the normal identity middleware. The
  SessionState lookup is keyed on `(tenant_id, conversation_id)` —
  enforced by the same DB-PERMISSIONS pre-check that the existing
  conversation GET performs (404 if the caller cannot see the
  conversation).

## Worker DB access

The worker outlives the HTTP request that spawned it. `AuthedDb` is
`!Clone` and pool-released per request (see comment in
`backend/src/api/chat.rs:14-18`). So:

1. The POST handler **snapshots** the caller's claims (`AuthContext`
   is already `Clone`-friendly: user_id, tenant_id, issuer, email,
   roles).
2. The spawned worker calls `storage.checkout_for(&claims).await?` to
   get its own `AuthedDb` from the pool when it needs to commit.
3. Pool release on the worker side is deterministic (Drop), same as
   request-bound `AuthedDb`. No leak.

If the pool checkout helper doesn't exist yet, it lives in
`backend/src/storage/mod.rs` next to whatever currently builds the
per-request `AuthedDb` in the identity middleware. We reuse that path
verbatim, not a parallel one.

PERMISSIONS still gate writes — the worker's claims are the
submitter's claims; no privilege escalation. This satisfies the
"identity at the edge" guideline because the worker is morally a
continuation of the request, not a privileged background job.

## Stop button

Any tab can stop the in-flight turn; all tabs see the stop reflected
the moment the worker reacts.

### Backend

`SessionState` gains a per-turn cancellation handle:

```rust
struct SessionState {
    // ...
    current_turn_cancel: Mutex<Option<CancellationToken>>,
}
```

Set by the worker when it acquires `turn_lock`; cleared when the
worker finishes (cleanly or via stop). Worker loop selects:

```rust
tokio::select! {
    biased;
    _ = cancel.cancelled() => StopReason::User,
    item = upstream.next() => match item {
        Some(Ok(delta)) => { /* append framed bytes, notify */ continue }
        Some(Err(e))    => StopReason::Error(e),
        None            => StopReason::Eof,
    }
}
```

On `StopReason::User`:

1. Drop the upstream LLM stream (cancels the provider request via
   `rig`'s stream Drop).
2. Append `proto::finish("stop")` to the buffer; `notify_waiters()`.
   All readers now see the turn's end frame just as if the LLM had
   completed normally.
3. Acquire `finalize_lock`, write whatever bytes accumulated as the
   assistant message (same path as the clean-finish case — partial
   reply is still a reply, identical to today's behavior on stream
   error).
4. Run title generation if needed (same as clean finish).
5. Clear buffer, advance `base_offset`, release `turn_lock`. The
   next queued submission (if any) acquires the semaphore and runs
   immediately.

The `POST /stop` handler is one-liner-ish:

```rust
if let Some(state) = registry.lookup(id).await {
    if let Some(token) = state.current_turn_cancel.lock().await.as_ref() {
        token.cancel();
    }
}
StatusCode::NO_CONTENT
```

Idempotent: cancelling an already-cancelled token is a no-op; no
in-flight turn → nothing to cancel. The handler returns immediately;
the worker reacts asynchronously.

### Queued message fires next

The stop releases `turn_lock` as part of the worker's normal exit
path. Any submission whose worker was parked on
`turn_lock.acquire()` proceeds at that moment, runs the LLM stream
normally, and emits its own `d:` finish frame. No special "fire next"
plumbing — the semaphore already does this.

### Frontend stop UI

"Is a turn in flight?" is derived state from the open stream, not
backend-queried. Same parser that handles `0:`/`2:`/`3:`/`d:`
records flips `streaming = true` on the first `0:` or `2:` of a turn
and back to `false` on the `d:`. Because every tab reads the same
buffer, every tab's `streaming` flag agrees. Render the stop button
whenever `streaming === true`; clicking it calls
`fetch(/api/chat/conversations/{id}/stop, { method: "POST" })` and
takes no other action — the resulting `d:{"finishReason":"stop"}`
arrives via the same open stream and flips `streaming` back to
`false` on all tabs simultaneously.

There is no per-tab "I clicked stop" optimistic state to manage.
The buffer is the source of truth; the UI is a pure function of it.

## Concurrent turns from the same conversation

`turn_lock: Semaphore::new(1)` per `SessionState`. Second tab's
submission persists the user message immediately, the worker waits on
the semaphore, runs in arrival order. Readers see Turn A's bytes,
then Turn A's `finish` marker, then (after a quiet window) Turn B's
bytes and `finish`. The frontend chat renderer treats each `finish`
as a turn boundary.

This is intentional: it matches the user mental model of "one chat,
one conversation, replies arrive in order."

## Backend layout

New module: `backend/src/chat/` (sibling of `api/`). Public interface
`mod.rs` exports:

- `SessionRegistry` — the `RwLock<HashMap<ConversationId,
  Weak<SessionState>>>` holder, plus `get_or_create`,
  `handshake_lock`, lookup helpers.
- `SessionState` — opaque except for `subscribe()` returning a
  `SessionReader` and `submit(turn: TurnRequest) -> JoinHandle`.
- `SessionReader` — `impl AsyncRead`.

`AppState` (in `backend/src/state.rs`) gains:

```rust
pub session_registry: Arc<SessionRegistry>,
```

The existing `api/chat.rs` shrinks dramatically: the handler becomes
"persist user msg, build TurnRequest (history + claims + prompt
inputs), call `session.submit(...)`, return 202." All LLM streaming
and citation prep moves into the worker, which lives in
`backend/src/chat/worker.rs`.

`api/stream.rs` stays unchanged. The worker imports it.

New handler module: `backend/src/api/chat_stream.rs` for `GET
/api/chat/conversations/{id}/stream`. It calls
`SessionRegistry::handshake_lock` *only* to ensure consistent attach
ordering w.r.t. an in-flight commit — for stream-only attaches there
is no history to coalesce. (The combined history-plus-stream
operation lives in `conversations::get` after this change.)

Tree:

```
backend/src/chat/
├── mod.rs           // public interface
├── registry.rs      // SessionRegistry, weak-map, handshake_lock
├── state.rs         // SessionState (the three+two primitives)
├── reader.rs        // SessionReader: impl AsyncRead
└── worker.rs        // per-turn task: LLM stream, RAG, commit, title

backend/src/api/
├── chat.rs          // POST /messages — now ~80 lines, no streaming
├── chat_stream.rs   // GET /stream — new
├── conversations.rs // GET /{id} — now coordinates with finalize_lock
└── stream.rs        // unchanged (proto::* writers)
```

Module boundary rules (per `.claude/CLAUDE.md`): `api/*` depends on
`chat::` via its public interface; `chat::` does not import from
`api::` (we move `stream::` references *into* worker by importing
`crate::api::stream` — that's a sibling, not an ancestor, so it's
allowed).

## Frontend

Drop `@ai-sdk/react`'s `useChat` from `Chat.tsx`. Replace with a thin
custom hook that mirrors what `useChat` gave us minus the
"POST-is-the-stream" assumption.

### `useSessionStream(conversationId)` — new

State managed:

- `messages: Message[]` — committed turns from the GET fetch +
  whatever the stream has accumulated for the in-flight turn.
- `streaming: boolean` — true while a `0:` / `2:` is being received
  and no terminal `d:` yet.
- `citations: CitationEntry[] | null` — last seen `2:` block, scoped
  to the current in-flight turn.
- `error: string | null` — last `3:` payload, cleared on next submit.

Lifecycle on `conversationId` change:

1. `fetch(/api/chat/conversations/{id})` → seed `messages`, conversation
   meta (title).
2. Open `GET /api/chat/conversations/{id}/stream` with `fetch()` +
   `response.body.getReader()` (NOT `EventSource` — we're consuming
   the AI SDK data-stream format, which is line-prefixed records, not
   `data: ...\n\n` SSE frames). Parse incrementally; each record
   (`0:"..."\n`, `2:[...]\n`, `3:"..."\n`, `d:{...}\n`) maps to the
   same state transitions `useChat` used to handle.
3. On unmount or id change: `reader.cancel()` and close the response.

### `submit(text: string)` — replaces `append()`

```ts
await fetch(`/api/chat/conversations/${id}/messages`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ messages: [{ role: "user", content: text }] }),
});
// Stream is already open; the user's message appears in `messages`
// via an optimistic insert; the assistant reply arrives via the
// open stream.
```

Optimistic insert of the user message: since the backend persists it
synchronously inside POST before returning 202, the frontend can
mirror that — append immediately on submit so the input clears and
the UI updates. If the POST returns non-2xx, roll back.

### Multi-tab behaviour (free)

Two tabs both run `useSessionStream` on mount. Both have a GET stream
open. Tab A submits → backend worker streams into the shared buffer
→ both tabs' streams emit the same bytes. Tab B sees A's reply
appear live.

### Files changed

- `frontend/src/components/chat/Chat.tsx` — replace `useChat(...)`
  with `useSessionStream(id)`; submit handler calls `submit()`.
- `frontend/src/hooks/useSessionStream.ts` — new.
- `frontend/src/lib/api.ts` — drop the `chatEndpoint(key)` helper
  (only used by `useChat`'s `api:` option); add `submitMessage(key,
  text)` and `openMessageStream(key, signal)`.

## Open questions

- **Title-update propagation.** Today the frontend learns the new
  title only on next conversation list re-fetch. With the worker
  detached, we could push a frame from the worker into the buffer
  (e.g. a new `proto::meta(...)` record) so the open stream sees
  "title changed" live. Lower-friction alternative: poll
  `GET /api/chat/conversations/{id}` after a `d:` marker. Default to
  the polling path in v1; defer the `proto::meta` channel until we
  need more push events.
- **Session reaper.** With Weak refs, idle sessions collapse
  naturally. But if a tab is left open indefinitely with no
  activity, the SessionState lives forever (empty buffer, near-zero
  cost). Acceptable. Revisit only if a leak surfaces.

## Future: edit-and-resubmit (deferred)

Not in this milestone. Sketched here so the SessionState design
doesn't paint us into a corner.

Behaviour: user clicks edit on a past user message, modifies the
text, hits resubmit. Everything from that message onward (including
the assistant reply that followed it, plus any subsequent turns) is
discarded; the edited message is treated as a fresh submission and a
new turn streams in its place.

Backend shape that drops into the current design without rework:

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/chat/conversations/{id}/messages/{msg_id}/edit` | Replace the body of an existing user message and re-run from that point. |

Worker logic:

1. Acquire `turn_lock` (same as a fresh submission — serialises
   against any in-flight or queued turn).
2. Acquire `finalize_lock` and, in the same transaction: delete all
   messages with `ordinal >= target.ordinal` from the conversation
   (cascade in the existing `messages` schema, or a single ranged
   delete), then insert the edited user message at the same ordinal.
3. Release `finalize_lock`. From here, the worker runs the standard
   per-turn path: build prompt from new history, stream LLM, commit,
   etc.
4. Buffer treatment: same as a normal turn. Readers see a `d:`
   from the prior turn (if there was one), then the new turn's
   bytes. The frontend already throws away anything past the
   edited message in its local `messages` array on edit (the
   subsequent GET re-fetch would do the same), so no special
   reader-side handling.

Stop button interaction: editing while a turn is in flight is a
two-step user action — stop first, then edit. We don't auto-stop
the in-flight turn from the edit endpoint; if `turn_lock` is held,
the edit waits, identical to a queued submit.

Frontend: the existing `useSessionStream` hook gets a `editMessage(id,
text)` action; everything else is reuse. The optimistic update on
edit truncates the local `messages` array and calls submit-style
mutation on the dedicated edit endpoint.

This is a thin layer on top of what we're building, not a
re-architecture. Captured here so step 5 (`worker.rs`) doesn't
accidentally hard-code "appends to the end" assumptions that an
edit would need to undo.

## Implementation order

Each step compiles and passes tests on its own.

1. **Skeleton `chat::` module.** Define `SessionState`,
   `SessionReader` (no notify wiring yet — just the type),
   `SessionRegistry` with `get_or_create`. Inline unit tests cover
   weak-upgrade-or-create.
2. **`SessionReader as AsyncRead`.** Implement `poll_read` with the
   notify-before-drain pattern. Test by spawning a writer task that
   appends in a loop and a reader that consumes — verify no lost
   bytes, no spurious wakes, blocks when caught up.
3. **`buf.clear()` + `base_offset` semantics.** Test that a reader
   left at the end of buffer A sees an empty read after clear, then
   sees buffer-B contents on next append. No panics, cursor
   arithmetic correct across the boundary.
4. **`finalize_lock` + handshake helper.** Test that
   `handshake_lock` blocks during a worker commit and unblocks
   after.
5. **Per-turn worker.** Port the LLM-streaming + RAG logic out of
   `api/chat.rs` into `chat/worker.rs`. Worker takes a `TurnRequest`
   (claims snapshot, user_text, conversation_id, app state). Commits
   via fresh `AuthedDb`. Backend integration test:
   `chat_streaming.rs` (listed in `testing.md` as not yet
   implemented) — fakes the LLM, posts a message, attaches a stream,
   asserts bytes match and DB persists.
6. **POST handler shrink.** `api/chat.rs::post_message` becomes
   persist-user-msg + `session.submit(...)` + return 202. The
   handler no longer needs the LLM client or embedder directly;
   those move into worker context.
7. **GET stream handler.** `api/chat_stream.rs::stream`. Returns
   `Body::from_stream(ReaderStream::new(reader))`. Header set
   matches what the AI SDK expects: `x-vercel-ai-data-stream: v1`,
   `cache-control: no-cache`, `content-type: text/plain;
   charset=utf-8`. (Same headers `post_message` used to set on its
   response body.)
8. **Handshake into `conversations::get`.** Wrap the existing
   history fetch with `handshake_lock` so the response is consistent
   with any concurrent commit. Add an integration test for the
   commit-during-fetch race using `tokio::join!` of a worker commit
   and a get.
9. **Frontend hook.** Build `useSessionStream`. Unit-test the
   AI-SDK line parser (snapshot-style: feed a stream of byte
   chunks at arbitrary split points, assert message-state
   transitions).
10. **Frontend wiring.** Replace `useChat` in `Chat.tsx`. Manual
    test: two tabs, send a message in one, see live in the other.
11. **E2E update.** Update `tests/e2e/chat-persistence.spec.ts` to
    cover the new flow (POST returns 202; stream arrives via the
    separate GET). Promote the in-progress
    `tests/e2e/chat-session-isolation.spec.ts` to assert the
    cross-tab fan-out invariant.

Each step is an independent commit. Stops 1–4 are pure type/test work
with no behavior change; users see nothing until step 6.

## Tests

Backend:

- `backend/src/chat/state.rs` — `#[cfg(test)] mod tests`: notify
  ordering, base_offset arithmetic, semaphore serialization.
- `backend/src/chat/reader.rs` — same: AsyncRead semantics, no lost
  bytes, no spurious wakes, blocks when caught up.
- `backend/tests/chat_streaming.rs` — integration: POST → stream →
  DB persistence. Fake LLM via `tests/common/fake_llm.rs`.
- `backend/tests/chat_handshake.rs` — integration: race the
  worker's commit against a fresh GET; assert no duplicates, no
  drops.

Frontend:

- `frontend/src/hooks/useSessionStream.test.ts` — parser snapshot
  tests over arbitrary chunk splits.
- `frontend/src/components/chat/Chat.test.tsx` — render Chat,
  simulate POST 202 + streamed bytes via MSW; assert message
  rendering.

E2E (Playwright, root `tests/`):

- `chat-persistence.spec.ts` — update to new endpoints.
- `chat-session-isolation.spec.ts` — promote to multi-tab fan-out
  assertion.

The `proto::*` snapshot tests in `api/stream.rs` are untouched.
That layer is what the worker writes; nothing under it changes.

## Mental model

The buffer is a file. The worker is one writer that opens it for
append, writes a turn, then truncates it and walks away. The readers
are `tail -f` processes: they hold their own offset, read whatever's
there, sleep on a watcher when caught up, and exit when the user
closes their terminal. The truncation doesn't surprise them because
they think in absolute byte positions, not file offsets. The DB is
the long-term archive; the file is scratch space for the in-flight
turn only.
