# Chat Streaming — v4 (server-authoritative replication, scale-ready)

Status: **Phase 1 implemented** (single instance, in-memory; Redis is
Phase 2). Supersedes [`chat.md`](./chat.md) (v3); companion to the
functional spec [`specs/chat.md`](../specs/chat.md). The design sections
below describe the shipped Phase-1 shape; the
[Phase-1 implementation plan](#phase-1-implementation-plan) is retained as
the build record (all steps landed).

> **Phase 1.5 (planned):** [`chat-v4-lifetime.md`](./chat-v4-lifetime.md)
> refines the buffer/cursor rules (§4.1) and GC (§9) — refcount-based
> session lifetime (no sweeper/grace/idle timers) and a bounded two-turn
> buffer keyed by a single cursor (no turn-boundary `resync`). Where it
> disagrees with §4.1/§9, that doc wins once implemented.

> **For an implementing agent:** the design sections (1–9) give the model
> and the *why*; the **[Phase-1 implementation plan](#phase-1-implementation-plan)**
> at the bottom is the ordered, file-by-file work. Read the model first,
> then work the plan top to bottom. Decisions already made:
> single-instance/in-memory now (Redis is Phase 2), the **pull** read
> model, and a **full `resync` + first-connect-refetch** client.

## 1. Why v4

v3 is correct on one process and wrong the moment a second backend
replica exists. Everything in `SessionRegistry`/`SessionState` assumes a
single process; with two replicas behind a load balancer **fan-out**,
**stop**, and **single-flight** all break (the worker, buffer, cancel
token and lock are process-local). We **ship single-instance now** but
rework the abstraction so going multi-backend is a drop-in: a second
`TurnBus` impl, **unchanged wire contract, worker, handlers, client.**

## 2. The model in one picture

```
DURABLE (SurrealDB)                 EPHEMERAL (in-memory now / Redis later)
─────────────────────              ──────────────────────────────────────
committed messages                  per conversation:
  user + assistant text               head   = last committed message id
  assistant carries citations         active = the in-flight turn's
read via GET history (R1)                      delta log (frames + cursors),
                                               or empty
```

**Guiding principle: the ephemeral state is a cache over durable state.**
Every byte is reconstructible from SurrealDB, so we discard it when it
isn't in active use, and any client we cut off recovers by re-reading the
durable rows. That property makes GC trivial and makes `resync` a
complete safety net.

## 3. Design principles

- **Durable vs ephemeral are different state, replicated differently.**
  Committed messages → SurrealDB, read via `GET history`. In-flight
  frames → the ephemeral log, gone after the turn.
- **The worker is the single writer of a turn's stream.** `/stop` only
  flips a cancel token; the worker emits `clear` itself. This makes the
  commit↔abort race structurally impossible (§8) — **no phase machine**.
- **Cursors are opaque.** Every data frame carries an SSE `id:`; the
  client only echoes it. In-memory it's a monotonic `u64`; in Redis the
  stream entry id. The wire is identical.
- **`resync` is the universal correctness mechanism.** Any client cut
  off by GC is told to re-read history. Because the durable copy exists
  *before* any ephemeral byte is trimmed, no GC policy can lose data —
  so GC stays simple and impl-private.
- **The transport lives behind the `TurnBus` trait.**

## 4. Backend state

Per conversation the ephemeral state is two things:

- **`head`** — id of the last *committed* message; the resume boundary
  into SurrealDB. An optimization (re-derivable from the last row), so an
  idle conversation can drop it.
- **`active`** — the in-flight turn's delta log: an ordered buffer of SSE
  frames each with a cursor. Empty when idle.

The log **only ever holds the current in-flight turn.** Committed turns
are durable rows. So the snapshot ends exactly where the live log begins:
no overlap in the common case. **Message-id dedup** is kept only as the
safety for the race where a turn commits in the gap between a fresh tab's
history fetch and its subscribe (the client already dedupes
`user_message` by id; v4 makes that load-bearing).

### 4.1 Cursor & buffer semantics (the crux — implement exactly)

**The atomic unit is one whole SSE frame.** The worker turns each event —
an LLM delta, the citations table, the terminal `finish`/`clear` — into a
single complete, pre-formatted frame via the `sse::` writers
(`event: <name>\ndata: <json>\n\n`). The buffer stores, and readers
slice, **whole frames only** — never characters, tokens, or partial
frames. One `append` = one frame = one cursor. (A frame is opaque
`Bytes`; the log never inspects its contents.)

In-process the log is `frames: Vec<(Cursor, Bytes)>` plus two integers:
`base` (cursor of `frames[0]`) and `next` (cursor to assign next).
`Cursor` is a monotonic `u64`, **never reset** for the life of the
session. `try_start` **clears `frames` and sets `base = next`** before
appending the new turn's `user_message`, so the buffer holds *only the
current turn*.

`subscribe(from)` computes each batch under the lock:

- **`from = None`** (fresh connect): if a turn is in flight, return all
  `frames` (current turn from its start); if idle, return **empty** (the
  client relies on history). *Note the idle case must check "is a turn in
  flight", not just "are frames non-empty" — a just-finished turn's
  frames linger during the grace window (§7) and must not be replayed to
  a fresh joiner.*
- **`from = Some(c)`** (client has through cursor `c`, wants `> c`):
  - `c + 1 >= base` → return `frames` with cursor `> c` (maybe empty if
    caught up). Valid resume — covers a transient blip and a reconnect to
    a still-lingering finished turn (including its terminal frame).
  - `c + 1 < base` → the wanted cursor was trimmed: emit **`resync`**,
    then continue as a fresh (`None`) subscriber.

Each emitted frame is prefixed `id: {cursor}\n`. On reconnect the browser
resends `Last-Event-Id: {cursor}` → `from = Some(cursor)`; no header →
`from = None`.

`resync` therefore fires **exactly when a client missed a completed turn
while disconnected** (its cursor predates `base`). A client caught up to a
turn's terminal cursor, reconnecting between turns, does **not** resync —
it waits, and the next turn's `user_message` (reset rule) transitions it.

## 5. Wire contract

Two additions to v3's [wire format](./chat.md#wire-format); every existing
frame is byte-identical except for the new `id:` line:

1. **`id:` on every data frame** (SSE-native sequencing; not on the
   `:\n\n` heartbeat).
2. **`resync`** control frame (`event: resync\ndata: null\n\n`).

**Frame anatomy on the wire.** The `sse::` writers emit only
`event:`/`data:`. The **reader prepends `id: {cursor}\n`** at send time,
so the buffered frame stays replayable and the cursor lives beside it
(not baked into the stored `Bytes`):

```
id: 7
event: text
data: "llo"

```

Three connect cases, three reconciliations:

| Connect | Mechanism |
|---|---|
| **Fresh mount** | GET history *after* `onopen`; in-flight turn streams on top; message-id dedup covers the gap |
| **Transient blip** | browser resends `Last-Event-Id`; backend resumes from that cursor |
| **Out of window** | backend sends `resync` → client refetches history, keeps streaming |

## 6. The `TurnBus` abstraction

`TurnBus` bundles the single-flight lock, the ordered log (replay + live),
and cancel delivery — they share one backing store. It replaces
`SessionRegistry` + `SessionState`.

```rust
#[async_trait]
pub trait TurnBus: Send + Sync {
    /// Single-flight: atomically claim the turn slot and append the
    /// first (`user_message`) frame. Err ⇒ 409 in_flight.
    async fn try_start(&self, conv: &ConversationId, user_message: Bytes)
        -> Result<TurnHandle, AlreadyRunning>;

    /// Subscribe from an opaque cursor (see §4.1). Items already include
    /// their `id:` line — the SSE handler writes them verbatim.
    async fn subscribe(&self, conv: &ConversationId, from: Option<Cursor>)
        -> BoxStream<'static, Bytes>;

    /// Flip the in-flight turn's cancel token. No-op if idle. Idempotent.
    async fn cancel(&self, conv: &ConversationId);
}

pub struct TurnHandle { /* Arc<Session> + CancellationToken + done: bool */ }
impl TurnHandle {
    async fn append(&self, frame: Bytes);        // assign cursor, buffer, wake readers
    async fn terminate(&mut self, frame: Bytes); // append terminal frame, release slot
    fn cancelled(&self) -> WaitForCancellation;  // worker awaits in select!
    // Drop: if !done (panic/unwind), append `clear` and release the slot.
}
```

No `enter_committing`, no `TurnPhase` (§8). `Cursor` is an opaque newtype
over `String` (in-process: decimal `u64`). `AppState` gains
`turn_bus: Arc<dyn TurnBus>`, built once at startup (`InProcessBus`).

## 7. How consumers read the log (the read path)

**Pull, not poll** — "pull" in async Rust means awaited reads woken by
the writer, never an interval timer. Each SSE connection holds **its own
cursor** and reads the shared per-conversation log: drain after my cursor
→ write the batch to my socket → park until the writer signals → repeat.
This is the in-memory analog of Redis `XREAD BLOCK`, which is why
`subscribe(from) → stream` is one trait across both.

```rust
Body::from_stream(async_stream::stream! {
    let mut cursor = from;                  // Option<Cursor>
    let mut rx = session.notify.subscribe();// tokio::sync::watch::Receiver<u64>
    loop {
        let batch = {
            let g = session.lock();          // std::sync::Mutex — NO await inside
            g.read_from(cursor)              // §4.1: Frames(Vec) | Resync
        };                                   // lock released here
        match batch {
            Read::Frames(frames) => {
                for (c, f) in frames {
                    yield Ok(prepend_id(c, f)); // "id: {c}\n" + frame; await socket, no lock
                    cursor = Some(c);
                }
            }
            Read::Resync => { yield Ok(sse::resync()); cursor = None; continue; }
        }
        if rx.changed().await.is_err() { return; } // sender dropped = entry evicted
    }
})
```

Primitive mapping (plain `tokio`):

| Role | Primitive |
|---|---|
| Shared log buffer (Vec + base/next) | **`std::sync::Mutex`** — *not* `tokio::sync::Mutex` |
| "Frame appended" wakeup | **`tokio::sync::watch<u64>`** (carries `next`) |
| Subscriber count (for GC) | **`watch::Sender::receiver_count()`** |
| Per-turn cancel | **`tokio_util::sync::CancellationToken`** |
| Worker task | **`tokio::spawn`** |
| SSE body | reader loop as a `Stream` (**`async-stream`**) → `Body::from_stream` |
| 15s heartbeat | **`tokio::time::interval`** merged in (as v3) |

Two easy-to-get-wrong points: (a) **`std::sync::Mutex`** — never hold it
across `.await`; copy frames out, release, then await the socket and
`changed()`. (b) **`watch`, not raw `Notify`** — `watch.changed()` tracks
a version internally so a frame appended *during* a socket write isn't a
lost wakeup. The reader marks the receiver seen (`borrow_and_update`)
*before* slicing, and the writer `watch.send(next)` *after* releasing the
lock.

**Locking, when & why:** one mutex per conversation, taken only to push
one frame [writer] or copy out a slice [reader]; contents are
pointer/refcount/int work (frames are pre-formatted `Bytes` — clone =
refcount bump); no I/O, no allocation, no await under it. Reads and
writes are **whole-frame** (never sub-frame). One writer per conversation
(single-flight) ⇒ zero writer-writer contention; ≈2 readers at **frame**
cadence ⇒ negligible. Per-conversation isolation ⇒ no cross-conversation
contention. `std::sync::Mutex` (not `RwLock`): tiny whole-frame critical
sections + one frequent writer + ~2 readers means there is no contention
for `RwLock`'s reader-parallelism to relieve, and a reader-preferring
`RwLock` would risk starving the writer.

**Eager/lazy:** lazy pull, batch-eager per wake. A consumer fetches the
next batch only after its current write completes (natural backpressure —
the writer never blocks on a slow tab and never buffers private copies),
and drains all available frames per wake. A slow tab reads slower; if it
falls past `base` it gets `resync`.

**Under Redis:** two-tier — each instance runs one `XREAD BLOCK` loop per
active conversation (long-poll, not interval) and fans to local sockets
via this same path. No sticky routing.

## 8. Cancellation, stop, and the (now-absent) race

Single-writer **eliminates the v3 `TurnPhase` machine.** Because the
worker is the only writer of the stream and the only decider of
clear-vs-finish, the decision is sequential and local:

- Worker loops `select!`(biased) over `handle.cancelled()` vs the LLM
  stream's next delta.
- Broke on **cancel** (observed mid-stream) → `terminate(clear)`, no DB
  write.
- Broke on **EOF/error** → commit → `terminate(finish)`.

`/stop` → `bus.cancel(conv)` only flips the token. If the worker already
left the select on the EOF branch, the flipped token is never re-checked
→ it commits and emits `finish`. Therefore:

- `clear` emitted ⟺ cancel branch ⟺ **no commit**.
- `finish` emitted ⟺ EOF/error branch ⟺ **committed**.

Mutually exclusive — "clear emitted **and** rows persisted" is
structurally impossible, no phase machine required. R5's "stop a
millisecond too late" falls out: a cancel after EOF is a no-op, the turn
commits, `/stop` returns 204, the wire shows `finish`.

**Panic guard:** `TurnHandle::Drop` appends `clear` and releases the slot
if `terminate` was never called (unwind) — replacing v3's `WorkerGuard`,
folded into the handle. The same single-writer property holds under
Redis: the worker is the sole `XADD`er; `/stop` publishes a cancel its
replica turns into a token flip. No distributed phase machine.

## 9. Persistence, GC, and what doesn't change

**One ephemeral→durable transition: successful turn completion.** At
commit we atomically write `{user message, assistant message,
citations}` and advance `head`. Nothing else persists (deltas collapse
into `content`; control frames are lifecycle-only; a cancelled turn
writes nothing — R7 preserved).

**Persist ≠ trim.** Dropping the log at `finish` rug-pulls clients still
draining. Order: commit (durable, data safe) → append `finish` *to the
log* → log **lingers a grace window** → trimmed. Because the data is
already durable, the grace window is purely about clean delivery; anyone
who lags past it gets `resync`.

**GC (in-process, via one background sweep task):**
- **grace-trim:** an idle session whose `finished_at + GRACE_WINDOW`
  passed → empty `frames`, `base = next`. Bounds memory for long-lived
  connections that span many turns.
- **evict:** an idle session with `receiver_count() == 0` past an idle
  cap → remove from the `DashMap`. Recreated + re-derived on next
  subscribe. (Kills the v3 leak.)
- guards: prune dead subscribers; a backstop cap so a frozen reader can't
  pin the buffer forever.

Redis expresses the same as stream `MAXLEN` + TTL and a lock TTL.

**What does NOT change:** R7 atomic commit; R1 history read path; the
pool-starvation fix (drop `AuthedDb` before streaming — the Redis body
needs no DB handle at all); the `sse.rs` frame bytes (we only add the
`id:` prefix + one `resync` writer); spec R1–R10 (v4 changes *how*, not
*what*).

## 10. Schema / storage change (citations)

Citations become durable so a reloaded message renders its `[N]` markers:
add a `citations` field to the assistant `message` row and widen
`commit_turn` to write it. "SurrealDB holds message history" stays
literally true — the message carries its own citation table. (v3 never
persisted them; today they're live-only.)

## 11. Docs to reconcile while here

- `testing.md`'s "Vibe-coded guardrails" cites `api/stream.rs` with
  `proto::text`/`proto::error`/`proto::finish`. Stale: it's `api/sse.rs`
  with `sse::text`/`sse::error`/`sse::finish`. Fix when touching
  streaming code.

## 12. Build vs buy (decided: build)

Full sync engines (Convex/Zero/Electric/…) want to *be* the data layer →
invert our SurrealDB-as-source-of-truth → out. Fanout/recovery servers
(self-hosted **Centrifugo**: SSE-native, Redis-backed history + recovery
with an *epoch* = our `resync`; or managed **Ably**) do the generic half
but not the turn coordination, which is ours regardless. **Decision:**
build, on Redis. The `TurnBus` seam keeps a future Centrifugo impl
reversible.

---

# Phase-1 implementation plan

Single instance, in-memory, no Redis. Pull read model, full
`resync`+first-connect-refetch client. Each step lists the files, the
change, and a check. Steps are in dependency order; **Step 0 is
independent and may ship on its own.**

Module rules (`.claude/CLAUDE.md`): cross-module access only via public
interface files; `chat::` may use `crate::api::sse` (sibling) but storage
must not depend on `api` types.

### Step 0 — Persist citations *(independent, shippable alone)*

- `backend/schema.surql`: add a `citations` field to the `message` table
  (flexible array of objects; assistant rows only).
- `backend/src/storage/models.rs`: add `pub struct Citation { n: usize,
  chunk_id: String, doc_id: String, doc_title: Option<String>, page:
  Option<i64> }` (storage-owned — no `api` dep) and
  `pub citations: Option<Vec<Citation>>` on `ChatMessage`.
- `backend/src/storage/{mod,surreal,request,system}.rs`: widen
  `commit_turn(..., citations: &[Citation])`; write onto the assistant
  row; `ChatMessageWire` (surreal.rs) round-trips the field.
- `backend/src/chat/worker.rs`: map the worker's retrieved citations into
  `storage::Citation` and pass to `commit_turn`.
- Conversation-history API handler: include `citations` per assistant
  message in the response (map `storage::Citation` → the wire shape,
  identical to `sse::CitationEntry`).
- Frontend: `useConversation` types + `MessageBody` render citations from
  the message (history), not only live hook state.
- Tests: storage round-trip (commit_turn persists+returns citations); a
  frontend test that a reloaded assistant message renders `[N]`.
- **Check:** reload a conversation → citation markers resolve.

### Step 1 — `TurnBus` trait + `InProcessBus` (replace `SessionState`/`SessionRegistry`)

- New `backend/src/chat/bus.rs`: `trait TurnBus`, `struct TurnHandle`,
  `type Cursor`, `struct AlreadyRunning` (move `TaskId` here or keep a
  trimmed `id.rs`).
- New `backend/src/chat/inprocess.rs`:
  - `struct InProcessBus { sessions: DashMap<ConversationId, Arc<Session>> }`
  - `struct Session { inner: Mutex<Inner>, notify: watch::Sender<u64> }`
  - `struct Inner { frames: Vec<(Cursor, Bytes)>, base: Cursor, next:
    Cursor, running: bool, cancel: Option<CancellationToken>,
    finished_at: Option<Instant> }`
  - implement `try_start` (clear frames, `base=next`, push user_message,
    `running=true`), `subscribe` (the §7 reader), `cancel` (flip token),
    `TurnHandle::{append, terminate, cancelled}` + `Drop`, and
    `Inner::read_from(cursor) -> Read` per §4.1.
- Delete `session.rs` + `registry.rs`; port their unit tests into
  `inprocess.rs` (subscribe-replay, single-flight reject, prune-dead,
  plus new tests for the §4.1 `read_from` rules incl. the `resync`
  condition and the fresh-`None`-while-lingering case).
- `backend/src/chat/mod.rs`: export `TurnBus, InProcessBus, TurnHandle,
  Cursor, AlreadyRunning, TaskId, spawn_worker, turn_request,
  TurnRequest`; drop `SessionState, SessionRegistry, TurnPhase`.
- `backend/src/state.rs`: `AppState.sessions: SessionRegistry` →
  `turn_bus: Arc<dyn TurnBus>` = `Arc::new(InProcessBus::new())`; spawn
  the GC sweeper (Step 5) here.
- **Check:** `cargo build`; new `inprocess.rs` unit tests pass.

### Step 2 — Worker → single-writer (drop `TurnPhase`)

- `backend/src/chat/worker.rs`: `spawn_worker`/`run` take a `TurnHandle`
  (not `Arc<SessionState>` + token + task). `drive_turn`:
  `handle.append(citations)` then per-delta `select!`(biased) cancel vs
  next → `append(text)`; on cancel branch `handle.terminate(sse::clear())`
  and return (no DB write); on EOF/error `commit_turn(...)` then
  `handle.terminate(sse::finish(...))`. Remove `enter_committing` and the
  `WorkerGuard` (Drop on `TurnHandle` replaces it). Title-gen stays
  detached after terminate.
- **Check:** `cargo test` chat unit + integration; update/port any
  commit-abort-race test to assert the §8 invariant (clear XOR finish).

### Step 3 — SSE: cursors + `resync`

- `backend/src/api/sse.rs`: add `pub fn resync() -> Bytes`
  (`event: resync\ndata: null\n\n`) with a snapshot test; the `id:`
  prefix is applied by the reader (Step 1), not baked into the writers.
- `backend/src/api/chat_stream.rs`: parse `Last-Event-Id` →
  `Option<Cursor>`; call `state.turn_bus.subscribe(conv, from)`; stream
  the returned `Bytes` verbatim; keep the `AuthedDb`-drop + 15s heartbeat.
- **Check:** integration test — late subscriber over HTTP sees `id:`
  lines; a `from` past the window yields a `resync` frame.

### Step 4 — POST + `/stop` onto the bus

- `backend/src/api/chat.rs`: replace `sessions.for_conversation` +
  `start_turn` with `state.turn_bus.try_start(conv, user_frame)`; `Err`
  → 409 `{"reason":"in_flight"}`; pass the `TurnHandle` to `spawn_worker`.
  Keep the perm + `stale_parent` checks *before* `try_start`.
- `backend/src/api/chat_stop.rs`: replace lookup+abort with
  `state.turn_bus.cancel(conv)`; keep the perm check; 204.
- **Check:** integration — submit/stop round-trip; concurrent submit →
  409; stop mid-stream → `clear` over SSE; the existing
  `backend/tests/chat_late_subscribe.rs` ported to the bus API.

### Step 5 — GC sweeper

- In `inprocess.rs`: `fn spawn_gc(bus: Arc<InProcessBus>)` — every
  `GC_SWEEP_INTERVAL` (10 s) iterate `sessions`: idle &&
  `finished_at + GRACE_WINDOW (30 s) < now` → empty frames (`base=next`);
  idle && `receiver_count()==0` && idle ≥ `EVICT_IDLE` → remove entry.
  Constants as module consts.
- **Check:** a `tokio::time`-paused test: finished-turn frames trimmed
  after grace; idle no-subscriber conversation evicted.

### Step 6 — Frontend: `resync` + first-connect refetch

- `frontend/src/hooks/useChatStream.ts`: add a `resync` listener (reset
  overlay/citations/in-flight, fire `onTurnEnd`); change the `open`
  handler to fire `onTurnEnd` **only on first connect** (a
  `hasConnectedRef`), not every reopen — cursor resume + `resync` cover
  reconnects. `Last-Event-Id` is handled by the browser (server sets
  `id:`). Keep the `user_message` reset rule.
- `frontend/src/hooks/useChatStream.test.ts`: add a `resync` test; adjust
  the working-tree "fires onTurnEnd on reopen" tests to first-connect +
  `resync` semantics.
- **Check:** `make frontend-test`.

### Step 7 — Docs + acceptance

- Fix the `testing.md` `api/stream.rs`/`proto::` drift (§11).
- Point `chat.md` at v4 (or leave until ship).
- **Acceptance:** spec scenarios 1–6 reproduce by hand; `cargo test`
  (both feature configs) + frontend tests green; manual two-tab live
  fan-out + mid-stream stop + refresh-mid-stream.

---

# Phase 2 — multi-backend, Redis (forward sketch, not now)

`RedisBus`: Stream `chat:turn:{conv}` (`XADD`/`XREAD BLOCK`, `MAXLEN`),
`SET NX`+lease lock, pub/sub cancel, a lease-watcher that emits `clear`
for an orphaned (crashed-worker) turn, TTL/`MAXLEN` GC. Selected by
config; default stays `InProcessBus` for Tier 1 dev. **No wire, handler,
worker, or client change** — that is what Phase 1's trait + cursor/resync
contract buys. Update `ARCH.md`'s Redis bullet (edge-infra → app
dependency) when it lands.

## Open questions

- **Queue (deferred).** A future feature queues concurrent submits
  instead of 409. Not built now, not precluded: the lock guards
  *processing*, the state extends to `{head, queue, active}`. R8
  (`parent_id`/`stale_parent`) gets fuzzier with a queue — settle then.
- **One stream per conversation vs per turn** (Phase 2) — assume
  per-conversation (monotonic cursors, `MAXLEN` trim); revisit if awkward.
- **LB affinity** — confirm `/stream` needs none under Redis (it
  shouldn't; any replica reads the stream).
