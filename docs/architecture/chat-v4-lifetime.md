# Chat v4 — Phase 1.5: deterministic lifetime + bounded two-turn buffer

Status: **Steps A + B implemented** (`backend/src/chat/inprocess.rs`,
`bus.rs`, one deletion in `api/mod.rs`). Step C (frontend visibility
shedding) is still an optional, independent follow-up. Refines the shipped
Phase-1 internals in [`chat-v4.md`](./chat-v4.md) — specifically the
buffer/cursor rules (§4.1) and the GC/persistence section (§9). **No change
to the wire contract, `TurnBus` trait, worker, HTTP handlers, or — for
Steps A/B — the client.**

> **Implementation note — generation-tagged cursors.** Refcount lifetime
> (Step A) means a session can be *freed and reincarnated* within one
> conversation's life: a sole tab's SSE blips, its reader drops, the
> session frees (no worker, no other subs), then EventSource reconnects
> ~3s later with its old `Last-Event-Id`. A naive per-session counter
> would restart at 0, so that stale cursor would either silently park
> (missing the next turn's early frames) or resume mid-stream — data loss,
> and a violation of the "cursors never reset" invariant (§4). The fix,
> added during implementation: each incarnation's [`Cursor`] is seeded with
> a unique, monotonically increasing **generation** in the high bits (a
> bus-scoped `AtomicU64`), sequence in the low bits. A later incarnation
> has numerically higher cursors than any earlier one, so a stale cursor
> falls below the new buffer's floor and the *ordinary* `c + 1 >= floor`
> window check resyncs it — **no generation-aware branching** in
> `read_from`. This is the in-process analogue of Redis's never-reused
> stream entry ids (Phase 2). See `Cursor::generation` in `bus.rs`.

## 1. Why

Phase 1 has two timer/heuristic-driven behaviours we can replace with
deterministic, event-driven ones:

1. **GC sweeper.** A background task (`spawn_gc`) trims idle sessions after
   a grace window and evicts subscriber-less ones after an idle cap, using
   tuned `Duration` constants. It works (and `resync` makes its timing
   non-load-bearing), but it's a background task + three magic numbers, and
   session lifetime is non-deterministic.
2. **`clear`-at-`try_start`.** The buffer holds exactly one turn; the next
   turn clears it. A subscriber still draining the *previous* turn at
   turnover finds its cursor below the new `base` → `resync` → history
   refetch. Harmless (lossless) but, with clients of varying latency, it
   can recur at turn boundaries and adds avoidable DB reads + a visual snap.

Replace both:

- **(A) Refcount lifetime.** A `Session` lives exactly as long as a
  *consumer* holds it — a subscriber's reader stream or the worker's
  `TurnHandle`. When the last one drops, the session is freed and removes
  its own map entry. No sweeper, no grace window, no idle cap, no
  durations.
- **(B) Bounded two-turn buffer.** Keep `[previous turn][current turn]`
  (never more than two) via a single `turn_cursor`, trimmed at `try_start`.
  A client up to one turn behind resumes seamlessly; only a client 2+ turns
  behind (genuinely stuck) gets `resync`.

Together: zero timers, deterministic cleanup, bounded memory, and no
turn-boundary resync churn.

## 2. Model changes

### 2.1 Refcount lifetime (revises §9 GC)

- The map holds a **weak** reference: `DashMap<ConversationId, Weak<Session>>`
  (was `Arc<Session>`). The map is a rendezvous index, not an owner.
- **Strong owners:** each subscriber's reader stream holds an
  `Arc<Session>` (already captured by `reader`); the worker's `TurnHandle`
  holds an `Arc<Session>` for the whole turn (already the case). So:
  - the **worker always finishes + persists** even if every tab closes
    mid-turn (its handle keeps the session alive);
  - **clients finish streaming** (their reader holds a ref);
  - the session dies exactly when **worker done AND last subscriber gone**.
- **Self-removal:** `Session::drop` (fires at strong-count 0) prunes its own
  entry: `map.remove_if(&conv, |_, w| w.upgrade().is_none())`. Both the
  create path (`entry` API) and `remove_if` run under the DashMap shard
  lock, so the create-vs-drop race is clean: if a newer live session
  replaced the entry, its weak upgrades → we don't touch it; only a truly
  dead entry is removed.
- **Deleted:** `spawn_gc`, `maybe_trim`, `is_evictable`, `subscriber_count`,
  the `finished_at` / `created_at` fields, `GC_SWEEP_INTERVAL`,
  `GRACE_WINDOW`, `EVICT_IDLE`, and the `std::time::{Duration, Instant}`
  imports.

> **Idle connections.** With refcount, an open-but-idle tab keeps its
> session alive as long as the SSE is connected. That's intended; an idle
> connection here is a parked task + a socket fd + the `Arc` (no DB pool
> slot). Shedding background tabs is Step C (frontend), not a backend timer.

### 2.2 Bounded two-turn buffer (revises §4.1)

`Inner` drops `base`/`finished_at`/`created_at` and keeps **one** explicit
boundary cursor, `turn_cursor` = the first cursor of the **current** turn:

```rust
struct Inner {
    frames: Vec<(Cursor, Bytes)>,  // [previous turn][current turn]
    turn_cursor: Cursor,           // start of the current turn
    next: Cursor,                  // next cursor to assign (monotonic)
    running: bool,
    cancel: Option<CancellationToken>,
}
```

Three roles, two of them the *same* stored value, the third **derived**:

| Role | Value |
|---|---|
| Trim boundary (`try_start`) | `turn_cursor` |
| Fresh-join replay start (`read_from(None)`) | `turn_cursor` |
| Resume/resync floor (`read_from(Some c)`) | `frames.first().cursor` (derived, O(1)) |

**`try_begin` (new turn):**

```rust
if g.running { return None; }
g.frames.retain(|(c, _)| *c >= g.turn_cursor); // drop the turn 2-ago
g.turn_cursor = g.next;                          // current turn begins here
let c = g.next; g.next = c.next();
g.frames.push((c, user_message));
g.running = true;
```

Trace (cursors per frame): turn1 `[0,1,2]` (tc=0) → turn2 start keeps all,
tc=3 → `[0,1,2,3,4,5]` → turn3 start retains `>=3` (drops turn1), tc=6 →
`[3,4,5,6,…]`. Always `[prev][cur]`, ≤2 turns.

**`read_from`:**

```rust
None      => if running { frames where c >= turn_cursor } else { empty }
Some(c)   => {
    let floor = frames.first().map(|(fc,_)| *fc).unwrap_or(next);
    if c+1 >= floor { frames where fc > c } else { Resync }
}
```

- Fresh joiner replays the **current** turn only (`>= turn_cursor`); a
  finished-but-lingering turn is *not* replayed to a fresh joiner (relies on
  history) — same rule as §4.1, now expressed via `running`.
- A resumer up to one turn behind (`c` in the previous turn) has
  `c+1 >= frames.first()` → streams continuously across the boundary, **no
  resync**. Only `c` below the oldest retained frame → `Resync`.
- **Do not** use `turn_cursor` as the resync floor — it's the start of the
  *current* turn, so it would wrongly resync clients still in the previous
  turn and defeat the ≤2-turn guarantee. The floor is `frames.first()`.

`terminate` / `clear_if_running` keep their terminal-frame push but drop the
`finished_at` stamp. `emit_standalone` (the `title` sideband) is unchanged —
its frame appends at `next` (≥ `turn_cursor`), so it reaches live readers and
is trimmed with the current turn; fresh joiners get the title from the DB.

## 3. Implementation plan

### Step A — refcount lifetime (`inprocess.rs`, `api/mod.rs`)

- `InProcessBus { sessions: Arc<DashMap<ConversationId, Weak<Session>>> }`.
- `Session` gains `conv: ConversationId` + `map: Weak<DashMap<…>>` (set in
  `Session::new`, passed by `get_or_create`). Add `impl Drop for Session`
  with the `remove_if(dead)` prune above. (No strong cycle: map→`Weak<Session>`,
  session→`Weak<Map>`.)
- Rewrite `get_or_create` to upgrade-or-insert atomically via the DashMap
  `entry` API (retry loop; insert only when the existing weak is dead).
- `cancel` / `emit`: `sessions.get(conv).and_then(|w| w.upgrade())` then act;
  no-op if `None`.
- Delete `spawn_gc`, `maybe_trim`, `is_evictable`, `subscriber_count`, the
  GC consts, `finished_at`/`created_at`, and the `Instant`/`Duration` imports.
- `api/mod.rs`: delete the `InProcessBus::spawn_gc(turn_bus.clone());` line.
- **Check (done):** `session_freed_when_last_consumer_drops` (try_start,
  drop the handle with no subscribers → `sessions` empty) and
  `reincarnated_session_resyncs_stale_cursor` (a stale cursor against a
  freed-then-recreated session resyncs, exercising the generation guard).
  The shard-lock serialisation of `entry` vs. `remove_if` makes the
  create-vs-drop race correct by construction (documented at the `Drop`
  impl); a deterministic concurrency test wasn't added.

### Step B — two-turn buffer / single cursor (`inprocess.rs`)

- Swap `base` for `turn_cursor` in `Inner`; implement `try_begin`,
  `read_from`, and the derived floor exactly as §2.2.
- Update the unit tests:
  - retention: after two turns both are present; after a third, the oldest
    is gone (≤2 turns);
  - **no boundary resync:** subscribe, run turn 1 to `finish`, start turn 2,
    resume from a cursor inside turn 1 → `Frames` (not `Resync`);
  - resync only when 2+ turns behind (cursor below `frames.first()`);
  - fresh `None` join replays the current turn only; idle (not running) →
    empty.
- Delete the old `base`-based and GC-based tests.
- **Check (done):** `cargo test --lib chat::` + the chat integration suites
  (`chat_streaming`, `chat_late_subscribe`, `chat_stop`,
  `chat_commit_abort_race`, `conversations`) green. One adaptation was
  forced by Step A (not B): `chat_commit_abort_race` previously resumed from
  cursor 0 *after* the turn to read the lingering buffer; with refcount an
  unsubscribed session is freed at `terminate`, so the test now subscribes
  *before* the turn and drains live (which is how an open tab actually
  observes clear-XOR-finish). The wire and worker are unchanged.

### Step C — frontend visibility shedding *(optional, independent)*

- `useChatStream.ts`: close the `EventSource` on
  `document.visibilitychange → hidden`, reopen on `visible` (reuse the
  existing first-connect refetch for catch-up). A hidden tab then drops its
  reader → with Step A its session frees if it was the last consumer → zero
  idle cost; foreground tabs keep the full live experience.
- **Check:** `make frontend-test`; manual — background a tab, confirm the
  stream closes (and the backend session frees when it was the only
  consumer), foreground it, confirm it reconnects and reconciles.

## 4. Invariants this refactor must preserve

- **Joiner correctness is ordering + dedup, not locking.** The client
  subscribes *before* `GET history`; anything committed before the read is
  in history, anything after is delivered live, and the overlap collapses by
  **message id** (client-ULID user key + server assistant id, both durable).
  Do not "fix" this with a lock spanning DB + buffer.
- **One `std::sync::Mutex` per session**, never held across `.await`; copy
  frames out under the lock, release, then write the socket. (Not `RwLock`:
  ~2 readers + nanosecond critical sections ⇒ no contention to relieve.)
- **Logical monotonic cursors**, never reset; the SSE `id:` is the cursor.
  (Not vector offsets — they'd break `Last-Event-Id` resume and the Redis
  path, and force the writer to track every consumer's position.) Under
  refcount this is realised by generation-tagging (high bits per
  incarnation, low bits the sequence) — see the implementation note above —
  so cursors stay globally monotonic even across a freed/recreated session.
- **Single writer per turn** (the worker); single-flight via `running`.
- Wire frames, `resync`, the `TurnBus` trait, and `commit_turn` (persists
  the worker's local `assistant_buf`, never the frame buffer) are unchanged.

## 5. Rejected alternatives (so they aren't re-litigated)

- **Offset cursors for O(1) lookup.** Rejected: breaks stable
  `Last-Event-Id`, breaks the Redis stream-id path, and requires the writer
  to mutate every consumer's cursor on trim. The buffer is ≤2 turns, so the
  scan is trivial (binary-search the start index if ever needed).
- **`RwLock` on the buffer.** Rejected: no concurrent-reader contention to
  relieve here; heavier per-op + writer-starvation risk.
- **Read-lock-during-persist.** Moot: `commit_turn` persists the worker's
  local `assistant_buf` and never reads the frame buffer, so the flush never
  holds the buffer lock.
- **Low-water-mark (per-reader pointer) trimming.** Deferred: the two-turn
  window bounds memory without per-reader cursor tracking. Revisit only if a
  workload needs >2 turns retained without any boundary resync.

## 6. Phase 2 (Redis) note

`RedisBus` realises the same contract: `XADD`/`XREAD BLOCK` for the log,
`MAXLEN ~2 turns` for the bounded buffer, stream entry ids as cursors, and
key TTL / `SET NX` lease for lifetime (the distributed analogue of refcount
— last reader's `XREAD` lease gone + no producer ⇒ key expires). The trait,
wire, worker, and client are unchanged, exactly as Phase 1 intended.
