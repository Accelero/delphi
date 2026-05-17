# Chat Streaming — ChatGPT-Style Simplification (Plan)

Status: design. Replaces the previous multi-reader / SessionState design,
most of which is now dead code. This document is both the target
architecture and the implementation plan; a fresh agent should be able
to execute from this alone.

## Target architecture (the endgame)

One sentence: **POST `/messages` returns the SSE stream as its own
response body, the backend worker is detached so it survives the client
disconnecting, the user message + assistant reply are written atomically
to DB only at the end of a successful turn, and `/stop/{task_id}` is the
only mechanism that aborts a worker (and discards its work).**

### Endpoints

| Method | Path | Body / response |
|---|---|---|
| `GET`  | `/api/chat/conversations/{id}` | Returns committed history + conversation metadata. No coordination with workers — just a DB read. |
| `POST` | `/api/chat/conversations/{id}/messages` | Body `{ id, text, parent_id }`. On success: returns `200 OK` with `Content-Type: text/plain; charset=utf-8`, `x-vercel-ai-data-stream: v1`, `cache-control: no-cache`, body = the AI SDK data-stream bytes. First frame carries the `task_id`. Last frame is `d:` carrying `assistantMessageId`. On 409 (parent mismatch): empty body, frontend refetches + shows toast. |
| `POST` | `/api/chat/conversations/{id}/tasks/{task_id}/stop` | Cancels the named worker. Returns `204` whether or not the task was found. Idempotent. |

There is **no** `GET /api/chat/conversations/{id}/stream` endpoint and
**no** re-attach mechanism. If the client disconnects, the stream is
gone from the client's perspective; whether the backend keeps generating
depends on the cause (see "Disconnect handling" below).

### POST body

```json
{
  "id": "01HXY...",                 // client-generated ULID for the user message
  "text": "What time is it?",       // user message content
  "parent_id": "message:k9d8..."    // last known assistant message id (or null for first turn)
}
```

The server validates `parent_id == history.last().id` (or both
null/empty). If they don't match → **409 Conflict**, no body. The client
treats this as "your view is stale" — refetch the conversation, show a
toast, let the user re-submit.

### Stream frames

The on-wire format stays exactly the Vercel AI SDK data-stream
protocol (`api::stream::proto::*`). Two changes:

1. **Add `8:<json>\n` "task" frame** (or use `2:<json>\n` data-array with
   `{type: "task"}` — either works; pick whichever is one fewer line of
   parser code on the frontend). Emitted **first** on the stream so the
   client knows the task_id before any text arrives.
   ```
   8:{"taskId":"01HXY..."}
   ```
2. **Extend the `d:` finish frame** to include `assistantMessageId`:
   ```
   d:{"finishReason":"stop","assistantMessageId":"message:k9d8..."}
   ```
   The client uses this id as `parent_id` for the next submit.

`0:`, `2:` (citations), `3:` (error) are unchanged.

### Worker lifecycle

```
POST /messages
  ├─ validate parent_id against DB (no lock; monotonic property)
  │  ├─ mismatch → 409, return
  │  └─ match    → continue
  ├─ task_id = ULID; cancel = CancellationToken; (sender, receiver) = mpsc::channel(64)
  ├─ tasks.insert(task_id, cancel.clone())            // DashMap
  ├─ tokio::spawn(worker(...))                        // detached
  └─ return 200 with Body::from_stream(receiver)
                                                         ┌──────────────────────────────┐
       worker:                                           │ tasks: DashMap<TaskId,       │
         1. acquire AuthedDb from pool                   │        CancellationToken>    │
         2. load history (DB)                            └──────────────────────────────┘
         3. open LLM stream
         4. send 8:{"taskId":...} via mpsc                                   ▲
         5. emit 2:{citations} if any                                        │
         6. loop {                                                           │
              select! {                                                      │
                _ = cancel.cancelled() => break Cancelled,                   │
                item = upstream.next() => {                                  │
                  send 0:"..." via mpsc (try_send; ignore Err)               │
                  accumulate locally                                         │
                  if None => break NaturalEnd                                │
                  if Err  => send 3:"..."; break ErrorEnd                    │
                }                                                            │
              }                                                              │
            }                                                                │
         7. tasks.remove(task_id)  ──────────────────────────────────────────┘
         8. branch on stop reason:
            Cancelled  → DISCARD everything; do not write to DB; just exit.
                          (Client already aborted; nothing else to do.)
            NaturalEnd → atomic commit_turn(...) on DB:
                          BEGIN;
                            -- "last writer wins": delete anything that arrived
                            -- after our parent during the LLM call
                            DELETE message
                              WHERE conversation = $conv
                                AND created_at > (SELECT created_at FROM ONLY $parent_id)
                            -- (skip when parent_id is null & conversation was empty)
                            CREATE message:$client_id CONTENT {
                              conversation: $conv, role: 'user',
                              content: $user_text, parent_id: $parent_id
                            }
                            CREATE message CONTENT {
                              conversation: $conv, role: 'assistant',
                              content: $asst_text, parent_id: message:$client_id
                            } RETURN id
                            UPDATE $conv SET updated_at = time::now()
                          COMMIT
                         emit d:{"finishReason":"stop","assistantMessageId":"..."}
            ErrorEnd   → same commit_turn (partial reply is still a reply)
                         emit d:{"finishReason":"error","assistantMessageId":"..."}
```

### Disconnect handling

Three causes of "the response body stops being read":

| Cause | Backend signal | Worker behavior | Outcome |
|---|---|---|---|
| Stop button | POST `/tasks/{id}/stop` → `cancel.cancel()` | Reaches `Cancelled` branch → **discard** | Nothing persisted. Client cleared its in-flight state on the abort. |
| Chat switch | `mpsc::Sender::try_send` returns Err (receiver dropped) | Worker **ignores** the error, keeps pulling from the LLM, commits at end | DB has the message. User refreshes / visits the conversation later → message is there. |
| Tab close | Same as chat switch | Same | Same |

The stop endpoint is the only way to abort. Anything else (incidental
disconnect) is treated as "the client doesn't want to watch, but the
turn is still going."

### Concurrent turns

Allowed. No registry-level prevention. If two tabs submit with the same
`parent_id`:

1. Both POSTs pass parent_id validation (DB unchanged at that instant).
2. Both spawn workers with different task_ids.
3. Both stream independently to their own clients.
4. At commit time, both run the `commit_turn` transaction. The
   `DELETE message WHERE created_at > parent.created_at` step in each
   transaction removes the other's pair if it landed first. The
   second-to-commit overwrites the first. "Last writer wins."
5. The losing tab's user sees the LLM response locally but a next-turn
   submit will 409 (their `parent_id` is now stale — the winning tab's
   assistant id is on top). They refetch and see only the winning turn.

This is the agreed-upon trade-off — wasted LLM work in the rare
two-simultaneous-submit case, but no registry coordination needed and no
spurious 409s.

### Frontend behavior

- **On conversation page mount**: fetch history via existing
  `getConversation` query (route loader). No stream subscription.
- **On `submit(text)`**:
  1. Generate a ULID for the user message.
  2. Optimistically insert the user message into local state (so it
     appears immediately).
  3. POST `{ id, text, parent_id }` where `parent_id = lastKnownMessageId`
     (tracked in hook state; null for first turn).
  4. If 409: roll back the optimistic insert, show "conversation has
     changed, refreshing…" toast, invalidate the conversation query
     (triggers refetch).
  5. If 200: consume `response.body` via `getReader()`. Parse incoming
     frames:
     - `8:{"taskId":...}` → store taskId for /stop.
     - `0:"..."` → append to streaming assistant overlay.
     - `2:[...]` (citations) → set citations state.
     - `3:"..."` → set error state.
     - `d:{...}` → store `assistantMessageId` as `lastKnownMessageId`;
       clear in-flight overlay; clear taskId; invalidate the
       conversation query (refetches the committed pair, replaces the
       optimistic insert with the persisted rows).
- **On `stop()`**:
  - POST `/tasks/{taskId}/stop` (fire and forget).
  - `controller.abort()` on the fetch immediately — the client doesn't
    need to wait for any more bytes; the optimistic + streaming
    assistant overlay is dropped locally; the user message stays in
    place (or rolls back — decide based on UX; recommend rolling back
    since the turn was cancelled and never persisted).
- **No** persistent connection to `/stream`. **No** sessionStorage
  re-attach flag. **No** multi-tab fanout. Other tabs see updates only
  when they refetch (React Query `refetchOnWindowFocus` covers this for
  free).

## Schema changes

`backend/schema.surql`, `message` table:

```surql
DEFINE FIELD IF NOT EXISTS parent_id ON message TYPE option<record<message>>;
DEFINE INDEX IF NOT EXISTS message_conversation_created ON message FIELDS conversation, created_at;
```

The index supports the `WHERE conversation = $conv AND created_at > $t`
query inside `commit_turn`.

No migration of existing rows needed — `parent_id` defaults to `NONE`
for already-persisted messages, and queries that read `parent_id` should
treat `NONE` as the "no parent / first message in conversation" case.

## What gets deleted

### Backend files

- `backend/src/chat/reader.rs` — `SessionReader` (impl AsyncRead) goes away
- `backend/src/api/chat_stream.rs` — GET /stream endpoint goes away
- `backend/tests/chat_handshake.rs` — no handshake race to test

### Backend code inside surviving files

- `backend/src/chat/state.rs`: nearly all of it. The `SessionState` type
  (buf, base_offset, subscribe_cursor, notify, turn_lock, finalize_lock,
  `current_turn_cancel`, `mark_turn_*`, `append`, `clear_for_new_turn`,
  `subscribe`, etc.) is gone. File becomes either empty or contains a
  thin type alias for legacy imports during the refactor — final state
  is "this file no longer exists." Prefer deletion.
- `backend/src/chat/registry.rs`: rewrite. Old `SessionRegistry`
  (Weak-map of SessionState) is gone. New type is `TaskRegistry` (thin
  wrapper around `Arc<DashMap<TaskId, CancellationToken>>`) with
  `insert(task_id, token)`, `remove(task_id) -> Option<CancellationToken>`,
  `cancel(task_id)` (which is `remove + token.cancel()` for the /stop path).
- `backend/src/chat/mod.rs`: re-export `TaskRegistry`, `TaskId`,
  `spawn_worker`, `TurnRequest`. Drop everything else.
- `backend/src/api/conversations.rs::get`: drop the `lock_finalize`
  dance, drop the `session_registry.lookup` call. Just fetch
  conversation + messages from DB. ~10 lines simpler.
- `backend/src/api/mod.rs`: drop the `/stream` route; change `/stop`
  route to `POST /api/chat/conversations/{key}/tasks/{task_id}/stop`.
- `backend/src/state.rs`: rename `session_registry: Arc<SessionRegistry>`
  to `tasks: Arc<TaskRegistry>`.

### Frontend files

- `tests/e2e/chat-session-isolation.spec.ts` — multi-tab fan-out is not
  a feature.

### Frontend code

- `frontend/src/hooks/useSessionStream.ts`: rewrite. The `useEffect`
  that opens `GET /stream` on mount goes away entirely. The hook no
  longer holds a persistent connection. `submit` and `stop` become the
  only async surface (plus the local state for messages / overlay /
  status / citations / error / lastKnownMessageId / currentTaskId).
- `frontend/src/lib/api.ts`: drop `openMessageStream`. `submitMessage`
  returns the raw `Response` (so the caller can read `response.body`).
  Rename / re-shape `stopMessage` to `stopTask(conversationKey, taskId)`
  hitting the new URL.
- `frontend/src/components/chat/Chat.tsx`: minor — the `useSessionStream`
  signature/return shape changes slightly. Mostly mechanical.

## What stays unchanged

- `backend/src/api/stream.rs` (the `proto::text` / `proto::citations` /
  `proto::error` / `proto::finish` writers). The `proto::finish` writer
  needs a small extension to include `assistantMessageId` in the JSON
  body — described in [Stream frames](#stream-frames). A new helper
  `proto::task(task_id)` is added for the leading `8:` frame.
- `AuthedDb` pool + identity middleware. Worker still snapshots the
  caller's bearer at POST time and checks out its own `AuthedDb` from
  the pool inside the spawned task. Same pattern as the current worker.
- Stop button UX: still there, still works, just plumbed through
  `task_id` instead of conversation_id-implicit.
- The `proto::*` snapshot tests in `api/stream.rs::tests` (with one new
  case for the task frame and an updated case for the finish frame).

## Implementation order

Each step compiles and runs the full test suite cleanly. Commit after
each, focused message. Match the style of recent commits (no Claude
co-author trailer).

1. **Schema + storage trait + storage impl**
   - Add `parent_id ON message TYPE option<record<message>>` to
     `backend/schema.surql`.
   - Add `parent_id: Option<MessageId>` field to `ChatMessage` in
     `backend/src/storage/models.rs` (and to `ChatMessageWire` if that
     differs).
   - Update `list_messages` SELECT to include `parent_id`.
   - Add a `commit_turn` method to the `Storage` trait in
     `backend/src/storage/mod.rs`:
     ```rust
     async fn commit_turn(
         &self,
         conv: &ConversationId,
         user_message_id: &str,        // client-provided ULID; server prepends "message:"
         user_text: &str,
         parent_id: Option<&MessageId>,
         assistant_text: &str,
     ) -> Result<MessageId>;            // returns the assistant message id
     ```
   - Implement on `SurrealStorage` (`backend/src/storage/surreal.rs`)
     using a single multi-statement query wrapped in `BEGIN/COMMIT`.
     Include the "delete anything created after parent" step (no-op
     when `parent_id` is null and conversation is empty).
   - **Keep** `append_message` for now (it's used by tests and may be
     used elsewhere). Mark with a doc-comment noting that production
     chat writes go through `commit_turn`.
   - Update existing unit/integration tests that touched
     `append_message` to use `commit_turn` where appropriate, and add
     a test that exercises "last writer wins" — two concurrent
     `commit_turn` calls against the same `parent_id`, assert only one
     pair survives.
   - Verify: `cargo test`.

2. **`TaskRegistry` (replaces `SessionRegistry`)**
   - Rewrite `backend/src/chat/registry.rs` to wrap
     `Arc<DashMap<TaskId, CancellationToken>>`. Public methods:
     `new() -> Self`, `insert(task_id, token)`, `remove(task_id) ->
     Option<CancellationToken>`, `cancel(task_id) -> bool` (returns
     whether something was cancelled).
   - Define `TaskId(Ulid)` (or just `TaskId(String)` if ulid crate isn't
     pulled in; recommended: add `ulid = "1"` dep).
   - Delete `backend/src/chat/state.rs` entirely (or shrink to just
     re-exports).
   - Delete `backend/src/chat/reader.rs`.
   - Update `backend/src/chat/mod.rs` to export `TaskRegistry`, `TaskId`,
     `TurnRequest`, `spawn_worker`. Drop everything else.
   - Update `backend/src/state.rs`: replace `session_registry` field
     with `tasks: Arc<TaskRegistry>`.
   - Wherever `AppState` is constructed (`backend/src/lib.rs` or
     `backend/src/api/mod.rs`), construct `TaskRegistry::new()` instead
     of `SessionRegistry::new()`.
   - Verify: `cargo build`. (Tests for the chat module are still being
     rewritten in step 4 — temporarily there'll be compile errors in
     `worker.rs` and `chat.rs` from the field rename; that's expected
     between steps 2 and 5.)

3. **`proto::*` extensions**
   - Add `proto::task(task_id: &str) -> String` in
     `backend/src/api/stream.rs`:
     ```rust
     pub fn task(task_id: &str) -> String {
         format!("8:{}\n", serde_json::to_string(&json!({"taskId": task_id})).unwrap())
     }
     ```
   - Change `proto::finish` signature to take both `reason` and
     `assistant_message_id`:
     ```rust
     pub fn finish(reason: &str, assistant_message_id: &str) -> String {
         let body = json!({
             "finishReason": reason,
             "assistantMessageId": assistant_message_id,
         });
         format!("d:{}\n", body)
     }
     ```
     The `assistant_message_id` is empty string when no message was
     persisted (cancelled / pre-LLM error). Update the existing
     snapshot tests and add one for the task frame.
   - Verify: `cargo test --lib api::stream::tests`.

4. **Rewrite `chat::worker`**
   - `backend/src/chat/worker.rs`:
     - New `TurnRequest` shape:
       ```rust
       pub struct TurnRequest {
           pub conversation_id: ConversationId,
           pub user_message_id: String,    // client ULID, no "message:" prefix
           pub user_text: String,
           pub parent_id: Option<MessageId>,
           pub bearer: String,
           pub auth: AuthContext,
           pub llm: Arc<dyn LlmClient>,
           pub chunk_embedder: Option<Arc<dyn Embedder>>,
           pub pool: RequestDbPool,
       }
       ```
     - `pub fn spawn_worker(tasks: Arc<TaskRegistry>, req: TurnRequest)
       -> (TaskId, mpsc::Receiver<Bytes>)`. This is the new entry point
       — it creates the cancel token, allocates the task_id, inserts
       into `tasks`, creates the mpsc, spawns the task with
       `tokio::spawn`, returns (task_id, receiver).
     - Worker body:
       - Subscribe to the cancel token (clone it).
       - Acquire `AuthedDb` from `req.pool`.
       - Load history; run RAG; build prompt (the existing helpers
         `history_to_llm`, `retrieve_for_query`, `build_system_prompt`,
         `citation_entries` carry over almost unchanged — copy from
         the current `chat::worker`).
       - Emit `proto::task(&task_id)` via `try_send` on the mpsc
         (ignore Err — see below).
       - Emit `proto::citations(...)` if any.
       - Open the LLM stream.
       - Loop with `tokio::select!`:
         ```rust
         tokio::select! {
             biased;
             _ = cancel.cancelled() => break StopReason::Cancelled,
             item = upstream.next() => {
                 match item {
                     Some(Ok(LlmDelta::Text(t))) => {
                         assistant_buf.push_str(&t);
                         let _ = sender.try_send(Bytes::from(proto::text(&t)));
                     }
                     Some(Err(e)) => {
                         let _ = sender.try_send(Bytes::from(proto::error("llm stream error")));
                         break StopReason::Error;
                     }
                     None => break StopReason::Eof,
                 }
             }
         }
         ```
       - On `Cancelled` exit: remove from `tasks`, drop sender, RETURN —
         no DB write, no `d:` frame (client already aborted).
       - On `Eof` / `Error` exit: remove from `tasks`. Call
         `db.commit_turn(...)`. If `commit_turn` fails (e.g. another
         worker's transaction beat us and our DELETE removed nothing
         but our CREATE conflicted... actually SurrealDB transactions
         should serialise; if the conflict is unrecoverable, log a
         warn and emit `proto::error`). On success, emit
         `proto::finish(reason, assistant_message_id.to_string())`.
       - `try_send` errors throughout: ignore. The client disconnected;
         we continue the turn anyway. The mpsc backpressure is fine
         because we sized the channel at 64 — generous for chat-rate
         streaming, won't block.
     - Title generation: same flow as today, runs after `commit_turn`
       succeeds and before the `d:` frame. Best-effort; failure does
       not affect the `d:` emission.
   - **Important**: the worker holds `AuthedDb` for the entire turn,
     which holds a pool slot. With pool size 8, that's fine for typical
     concurrency; if you ever bump concurrent-user expectations,
     increase `REQUEST_DB_POOL_SIZE`.
   - Verify: `cargo build` (the API handler will still be broken; fix
     it in step 5).

5. **Rewrite `api::chat::post_message`**
   - `backend/src/api/chat.rs`:
     - New request body:
       ```rust
       #[derive(Debug, Deserialize)]
       pub struct ChatRequest {
           pub id: String,                       // client-generated ULID
           pub text: String,
           #[serde(default)]
           pub parent_id: Option<String>,        // raw "message:..." string, or None
       }
       ```
     - Handler:
       1. Parse conversation id.
       2. Validate `id` looks like a sane ULID (length, character set —
          reject if not, 400).
       3. Validate `text` is non-empty after trim (400 if not).
       4. Load history via `db.list_messages(&conv_id)`.
       5. Compare last message id (or None) to `req.parent_id`. If
          mismatch → 409 with body `{"reason":"stale_parent"}`.
       6. Build `TurnRequest { conversation_id, user_message_id: req.id,
          user_text: req.text, parent_id: <parsed Option<MessageId>>,
          bearer, auth, llm, chunk_embedder, pool }`.
       7. `let (task_id, rx) = chat::spawn_worker(state.tasks.clone(), req);`
       8. Convert `mpsc::Receiver<Bytes>` to a `Stream<Item =
          Result<Bytes, Infallible>>` (e.g. via
          `tokio_stream::wrappers::ReceiverStream`).
       9. Return:
          ```rust
          Response::builder()
              .status(StatusCode::OK)
              .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
              .header("x-vercel-ai-data-stream", "v1")
              .header(header::CACHE_CONTROL, "no-cache")
              .body(Body::from_stream(stream))
              .unwrap()
          ```
   - The handler shrinks from current 110-ish lines to maybe 80.
   - Verify: `cargo build`. Run the integration tests under
     `backend/tests/` that touch the chat handler (especially
     `chat_streaming.rs` if it exists, or write a fresh one — see
     step 8).

6. **Rewrite `api::chat_stop`**
   - File `backend/src/api/chat_stop.rs`:
     - New route is `POST /api/chat/conversations/{key}/tasks/{task_id}/stop`.
     - Handler:
       ```rust
       pub async fn stop(
           State(app): State<AppState>,
           Extension(db): Extension<Arc<AuthedDb>>,
           Path((conv_key, task_id)): Path<(String, String)>,
       ) -> Response {
           // PERMISSIONS gate — same shape as the old /stream handler.
           let conv_id = match parse_conversation_id(&conv_key) {
               Ok(id) => id,
               Err(r) => return r,
           };
           match db.get_conversation(&conv_id).await {
               Ok(Some(_)) => {}
               Ok(None) => return (StatusCode::NOT_FOUND, "not found").into_response(),
               Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response(),
           }
           let task_id = match TaskId::parse(&task_id) {
               Ok(t) => t,
               Err(_) => return (StatusCode::BAD_REQUEST, "invalid task id").into_response(),
           };
           app.tasks.cancel(&task_id);   // no-op if absent — that's fine
           StatusCode::NO_CONTENT.into_response()
       }
       ```
   - The PERMISSIONS check is still useful — it stops anonymous /stop
     calls from probing for valid task_ids by cross-conversation.
   - Delete `backend/src/api/chat_stream.rs`.
   - Update `backend/src/api/mod.rs`: remove the `/stream` route, change
     the `/stop` route to the new path pattern, ensure no stale `use`
     lines remain.
   - Verify: `cargo build && cargo test`.

7. **Simplify `api::conversations::get`**
   - Drop the `lock_finalize` dance. No `session_registry.lookup`. Just:
     ```rust
     let conversation = match db.get_conversation(&id).await { ... };
     let messages = match db.list_messages(&id).await { ... };
     (StatusCode::OK, Json(GetResponse { conversation, messages })).into_response()
     ```
   - Verify: existing `conversations` integration tests should still pass
     (they didn't depend on the lock semantics; just the result shape).

8. **Update / add integration tests**
   - **Delete** `backend/tests/chat_handshake.rs`.
   - **Update** `backend/tests/chat_stop.rs`: the test now POSTs to
     `/api/chat/conversations/{conv_key}/tasks/{task_id}/stop` instead
     of the old path; the white-box hook (`set_current_turn_for_test`)
     is no longer needed — assert via `TaskRegistry::cancel` directly
     in a unit-style test, or assert via the full POST flow.
   - **Update or add** `backend/tests/chat_streaming.rs` (it's in the
     "not yet implemented" list per `docs/architecture/testing.md`):
     fake LLM, POST a message, consume the response body, assert frames
     in order (`8:` task, `0:`s, `d:` with assistant id), assert DB has
     the persisted pair.
   - **Add** `backend/tests/chat_last_writer_wins.rs`: two concurrent
     `commit_turn` calls against the same parent — assert only one pair
     remains.
   - **Add** `backend/tests/chat_parent_mismatch.rs`: POST with stale
     parent_id — assert 409, assert nothing was persisted.
   - **Update** `backend/tests/conversations.rs`: the GET shape is
     unchanged but `ChatMessage` now carries `parent_id`. Add an
     assertion that the field round-trips.
   - **Update** `backend/tests/rag_retrieval.rs`: was using
     `append_message`; switch to `commit_turn`.
   - Verify: `cargo test`.

9. **Frontend: rewrite `useSessionStream`**
   - `frontend/src/hooks/useSessionStream.ts`:
     - Drop the persistent stream `useEffect`. The hook no longer
       opens any connection on mount.
     - State:
       - `messages: LocalMessage[]` — seeded from `initialMessages`,
         updated optimistically on submit, replaced wholesale by the
         next `onTurnEnd` refetch.
       - `status: StreamStatus` (ready / submitted / streaming / error).
       - `citations: CitationEntry[]`.
       - `error: string | null`.
       - `streamingAssistant: string` — accumulator for the in-flight
         assistant message (rendered as the last message while
         `status === "streaming"`; cleared on `d:`).
       - `lastKnownMessageId: string | null` — used as `parent_id` for
         next submit; updated on every `onTurnEnd` (read from refetched
         history) and on `d:` (from `assistantMessageId` in the frame).
       - `currentTaskId: string | null` — set on `8:` frame, cleared on
         `d:` or `stop()`.
       - `abortControllerRef: useRef<AbortController | null>`.
     - `submit(text)`:
       1. Generate `id = ulid()` (use the `ulid` npm package; tiny dep,
          or roll a small ULID generator inline — picking the package
          is fine, < 2 KB gzipped).
       2. Optimistic insert `{ id, role: "user", content: text }` into
          `messages`.
       3. `setStatus("submitted")`.
       4. Build the controller; store in ref.
       5. `fetch("/api/chat/conversations/.../messages", { method: "POST",
          credentials: "same-origin", headers: { "Content-Type":
          "application/json" }, body: JSON.stringify({ id, text,
          parent_id: lastKnownMessageId }), signal: controller.signal });`
       6. On 409: roll back optimistic insert, set error toast, invalidate
          conversation query (caller's `onTurnEnd` fires the refetch
          path), return.
       7. On non-OK other: similar — set error, drop optimistic, return.
       8. On OK: `getReader()` on `response.body`, loop parsing frames
          via the existing `StreamParser`. Apply records:
          - `8:`: `setCurrentTaskId(rec.value.taskId)`.
          - `0:`: append to `streamingAssistant`; `setStatus("streaming")`.
          - `2:`: set citations.
          - `3:`: setError, setStatus("error").
          - `d:`: store `rec.value.assistantMessageId` in
            `lastKnownMessageId`, clear `streamingAssistant`,
            clear `currentTaskId`, setStatus("ready"), call
            `onTurnEnd()` (caller invalidates query → refetch →
            `initialMessages` prop updates → see step below).
       9. On fetch abort or read error: same cleanup as `d:` minus the
          query invalidation (no committed turn to refetch).
     - `stop()`:
       1. If `currentTaskId` is null, no-op.
       2. POST `/api/chat/conversations/{key}/tasks/{currentTaskId}/stop`
          (fire and forget, no body, ignore response).
       3. `abortControllerRef.current?.abort()` — kills the read loop.
       4. Roll back optimistic insert (the user message), clear
          `streamingAssistant`, clear `currentTaskId`, setStatus("ready").
   - **Important**: when `initialMessages` prop changes (because
     `onTurnEnd` triggered a refetch), the hook needs to reconcile —
     replace its local `messages` with the new committed history. Add
     a `useEffect(() => { if (status === "ready") setMessages(seed); },
     [seed])` guarded by `status` so a mid-stream prop refresh doesn't
     wipe in-flight state.
   - Also update `lastKnownMessageId` from the latest committed
     history on every `seed` change while `ready` — so a tab that
     refetches due to another tab's commit picks up the new
     `parent_id` automatically.
   - Update the existing parser unit tests
     (`useSessionStream.test.ts`): add a case for the `8:` task frame
     and update the `d:` case to assert `assistantMessageId`.

10. **Frontend: `lib/api.ts`**
    - Drop `openMessageStream`.
    - Drop `stopMessage`. Replace with:
      ```ts
      submitMessage(key, body): Promise<Response>     // returns raw Response so caller can read body
      stopTask(key, taskId): Promise<void>            // fire and forget
      ```
    - The `body` shape is `{ id: string; text: string; parent_id: string | null }`.

11. **Frontend: `Chat.tsx`**
    - Mostly mechanical. The hook's return shape gains `currentTaskId`
      (or hides it; only `stop()` cares). Status mapping is unchanged.
      The optimistic message id is now a real ULID, but rendering
      doesn't care.
    - The 409 toast: add a tiny toast component if not present, or
      surface via the existing error banner.

12. **E2E**
    - Delete `tests/e2e/chat-session-isolation.spec.ts` — not a feature.
    - Update `tests/e2e/chat-persistence.spec.ts`: new POST body shape
      (`{ id, text, parent_id }`), new response shape (POST returns a
      stream body; no separate /stream call), still asserts persistence
      after reload.
    - Add (optional but recommended) `tests/e2e/chat-stale-parent.spec.ts`:
      open two tabs (or simulate via two contexts), submit in one,
      assert the other sees the new message on focus refetch but
      cannot submit until refreshing.

## Verification per step

After each step, run:

- Backend code change → `cargo test` (under `backend/`)
- Frontend code change → `make frontend-test`
- Step 12 → ideally run Tier 1 (`make down && make up`) and execute the
  Playwright suite, but at minimum compile-check the specs.

At the very end:

- `make down && make up` to rebuild Tier 1.
- Manual smoke:
  1. Open a corpus conversation. Send a message. See the assistant
     reply stream in. Reload the page mid-stream — page shows user
     message only at first; reload again after the LLM finishes → see
     the committed reply. (Verifies "chat switch / refresh survives.")
  2. Send a message. Click stop. Reload. Nothing persisted from the
     stopped turn. (Verifies stop discards.)
  3. Open the same conversation in a second tab. Submit in the first
     tab. The second tab does NOT see live updates. Refocus the second
     tab (window blur/focus) → React Query refetches → second tab now
     shows the new pair. (Verifies "no multi-tab fanout, but refresh
     is automatic.")
  4. Submit in both tabs simultaneously. Both stream independently;
     after both commit, the later one's pair is visible, the earlier
     one's is gone. The losing tab's next submit returns 409, refetches,
     submits clean. (Verifies "last writer wins" + 409 path.)

## Migration / rollback notes

This is a one-direction change. After the commit:

- Old `SessionState`/`SessionReader`/`SessionRegistry` types are gone —
  reverting requires the whole commit.
- The wire protocol gains a `8:` frame and a richer `d:` payload —
  pre-update clients would ignore the `8:` (unknown tag) but the
  `d:` is parsed by the AI SDK loosely (it just looks for
  `finishReason`), so old clients shouldn't break catastrophically.
- The schema migration (`parent_id` field) is additive and reversible.
- Database rows persisted by the old code have no `parent_id`; the
  new query path treats `NONE` as "first message" and so handles them
  gracefully on read. New writes always set it.

If we ever want re-attach (Claude iOS-style), the mechanism we just
deleted was the right shape — re-introducing it would be a focused
project on top of this baseline rather than a redo.

## Mental model

The chat backend is a thin shell around `tokio::spawn(worker)`. The
worker is the entire model — it owns the LLM call, the local
accumulation, the commit. Everything else exists to deliver bytes from
the worker to the client (mpsc + Body::from_stream), and to give the
client one way to cancel (DashMap<TaskId, Cancel> + /stop endpoint).
No buffer, no notify, no cursor, no multi-reader. POST is one async
HTTP request that streams its response body. The receiver of the body
is the one and only consumer; if it goes away, the worker either keeps
going (incidental disconnect) or aborts (explicit /stop).
