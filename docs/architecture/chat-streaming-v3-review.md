# Review of `chat-streaming-v3-plan.md`

Scope: I read the plan against the current backend (`backend/src/chat/*`,
`backend/src/api/chat.rs`, `backend/src/api/stream.rs`, `backend/src/storage/request.rs`)
and frontend (`frontend/src/hooks/useSessionStream.ts`) so the critique
reflects what's actually there, not what the plan says is there.

The overall shape — POST-202 + long-lived SSE + per-conversation session +
single in-flight turn — is sound and achieves the stated goals (multi-tab,
late-join, originating-tab-not-special, no rollback). What follows is the
list of places where, as written, the plan would not work, has an
unstated trade-off, or leaves an ambiguity an implementer will have to
invent under.

Severity tags: **[blocker]** = would not compile/work as written.
**[bug]** = compiles but has incorrect runtime behavior.
**[gap]** = under-specified; an implementer has to invent the answer.
**[trade-off]** = a real choice the plan makes implicitly that deserves
to be called out.

---

## 1. [blocker] SSE connection holds an `AuthedDb` for the conversation's lifetime → pool starvation

The plan inherits the identity-middleware pipeline unchanged for the new
GET `/conversations/{key}/stream` endpoint, and says so explicitly:

> SSE inherits the BFF cookie → bearer → AuthedDb pipeline already in
> place.

But that pipeline (`backend/src/auth/middleware.rs:135` returns
`(AuthedDb, AuthContext)`, attached to the request as
`Extension<Arc<AuthedDb>>`) holds the `AuthedDb` for the **entire
request lifetime**. The default pool size is 8
(`backend/src/storage/request.rs:43`). An SSE response in axum
(`Body::from_stream(...)` / `Sse::new(...)`) keeps the request alive
until the client disconnects.

Consequence: every tab that mounts the chat surface holds one pool slot
**forever**. Nine simultaneous tabs across the whole deployment → next
request blocks indefinitely on pool acquire. This is not a theoretical
edge case — it's the steady state after a few minutes of normal use.

Three workable fixes (the plan picks none):

1. Have the SSE handler do the permission check up front, then **drop
   the `AuthedDb` Extension** before returning the stream response.
   Requires either a new middleware-skipping route or popping the
   extension before returning. The cleanest path.
2. Use a different (much larger / dedicated) pool sized for "long-lived
   subscribers, mostly idle." Operationally ugly.
3. Make `AuthedDb` cheap enough to skip pooling for SSE (e.g.
   per-request connect-and-release on `db.authenticate`). Probably
   re-introduces the cost the pool was added to amortise.

Until this is resolved, the design as written is incompatible with the
existing pool model and will deadlock under modest concurrency.

## 2. [bug] `EventSource` replay duplicates frames unless the client treats `turn_start` as a hard reset

> Native browser `EventSource` auto-reconnects with backoff; on reconnect
> the server replays the current turn's buffer so the tab catches up.
> […]
> The replay re-fires `turn_start` + `user_message` + prior `text` deltas,
> which rebuild local state idempotently.

Two problems:

a. **Native `EventSource` has no per-event `id:` set in the plan's wire
   format**, so it cannot send `Last-Event-Id` on reconnect. Therefore
   the server cannot tell "this is a reconnect, replay from cursor X"
   from "fresh connect, replay all." The plan implicitly assumes
   *always* replay-all-from-current-buffer. That is fine, **but only if
   the client resets its accumulator on every `turn_start`**.

b. The frontend section says "On EventSource reconnect: clear overlay,
   accept the replayed buffer as authoritative." There is no such
   signal in native `EventSource`. `onopen` fires on every (re)open and
   you can't distinguish first-open from N-th-open without bookkeeping.
   The actual idempotency mechanism therefore has to be:

   > Every received `turn_start` clears the assistant overlay and
   > resets the text accumulator, regardless of whether the
   > `taskId` is new or matches the current one.

   That sentence needs to be in the plan; "rebuild idempotently" alone
   doesn't pin it down. Without it, the second time a reconnect happens
   mid-turn you'll get `helloworld` rendered as `hellohelloworld`.

   *(Adding `id:` per event and honouring `Last-Event-Id` would solve it
   too, but the plan explicitly defers that — fine, then the reset rule
   above is the only thing that makes the contract work.)*

## 3. [bug] Race between worker commit and `/stop` → committed message but UI sees `clear`

The worker on `Eof`/`Error` does:

1. `commit_turn` to DB
2. `session.emit(finish)`
3. `session.finish()` (clears `current`)

If `/stop` arrives between step 1 and step 2:

- `abort()` cancels the cancel token (worker already past the
  `select!`, so cancellation has no effect on this path).
- `abort()` emits `clear` and clears `current`.
- Worker then emits `finish` — but the plan says **`emit` is a no-op if
  `current` is None**. So `finish` is silently dropped.
- The assistant message **is** persisted to DB.
- All subscribers saw `clear` and rolled back their UI.
- Until the next history refetch, the UI shows nothing; the row is
  present in DB.

This contradicts the plan's claim that "`stop` therefore needs no DB
rollback" — it doesn't roll back, but the resulting state is
inconsistent between SSE-driven UI and the database. The window is
small (one mutex hop + one DB transaction), but not zero, and it's
exactly the kind of thing that creates "ghost messages on refresh."

Fixes (pick one, document it):

- After `commit_turn` succeeds, check the cancel token; if cancelled,
  *still* emit `finish` (the message exists, the UI should know).
- Equivalently: `abort()` should only cancel + drop the cancel token,
  not emit `clear` if the worker has passed its commit point. Track
  worker phase in `InFlightTurn`.
- Or: serialise commit and abort behind the same `Inner` mutex so they
  can't interleave.

## 4. [bug] `start_turn` frame-ordering pseudocode would lose initial frames from the replay buffer

> `start_turn(parent_id, user_msg)` — validates "no current turn", mints
> `TaskId` + cancel token, builds initial `turn_start` + `user_message`
> frames, **fans them out**, **sets `current`**.

Read literally, fan-out precedes "sets `current`". `emit` is defined as
"append to `current.frames`, `try_send` to every subscriber, no-op if
`current` is `None`". If `current` is None at fan-out time, frames are
emitted to live subscribers but **never appended to the replay buffer**.
A subscriber that connects 50 ms later misses `turn_start` entirely.

The actual semantics need to be: construct `InFlightTurn { frames:
vec![turn_start, user_message], … }`, write `current = Some(...)`, *then*
fan out (or, equivalently, fan out from inside the constructed
`InFlightTurn` already living in `Inner`). All under one lock.

Trivial to get right; trivial to get wrong if the plan is followed
verbatim.

## 5. [gap] `subscribe()` replay can overflow the per-subscriber mpsc

`subscribe()` pushes the entire `frames` vector into a fresh `mpsc::Sender`
before registering. The plan doesn't specify the channel capacity, but
the current code uses `STREAM_CHANNEL_CAPACITY = 64`. A long turn can
easily exceed 64 frames (one `text` frame per token; a 500-token reply
= 500 frames). The replay loop will hit `try_send` `Full` and drop
frames on the floor for late subscribers.

Either:

- Size the per-subscriber channel ≥ a comfortable max frame count
  (≥ ~2000), or
- Replay synchronously inside a `Bytes` buffer that the SSE handler
  drains first before switching to the live channel, or
- Use an unbounded channel for subscribers (acceptable here — the
  upstream is rate-limited by LLM token cadence, not by client speed),
  or
- Just call out the policy: "slow / late subscriber may drop frames;
  client must refetch history on connect for authoritative state."

Pick one and write it down.

## 6. [trade-off] Slow-subscriber backpressure policy is unspecified

Adjacent to (5) but for live frames during a turn: `session.emit` does
`try_send` to every subscriber and prunes "disconnected senders." But
`try_send` returns `Full` for slow-but-alive subscribers, not just
dead ones. The plan says "prune disconnected" — which `Full ≠ Closed`.
Silent gaps in a slow tab's stream, with no detection.

The plan should explicitly choose:

- Drop frames silently (current `try_send` behavior on `Full`), accept
  visual artifacts on slow subscribers; **or**
- Use a `send().await` and let one slow subscriber slow the whole turn;
  **or**
- Disconnect a slow subscriber and let `EventSource` reconnect (the
  client will get a fresh replay).

## 7. [trade-off] Originating tab cannot stop a turn unless SSE delivered `turn_start` first

By design, POST returns 202 with no body; the originating tab learns
`task_id` only via SSE `turn_start`. Therefore:

- If SSE is broken when POST returns, the stop button on the
  originating tab is non-functional until SSE recovers.
- The user might have submitted, lost SSE, and now has no way to abort
  a turn they can see is still running (or rather, can't see).

The plan documents the "originating tab is not special" decision but
does not surface this consequence. Options:

- POST returns `202 { taskId }` in the body. The tab uses that for stop;
  SSE is only for the rendered stream. Doesn't break "not special" —
  every tab can still stop any turn by its taskId; the POSTer just
  happens to learn it earlier.
- Document and accept the limitation.

## 8. [gap] `/stop` mismatched-task-id semantics

> `/stop` handler looks up the session by conversation key, verifies
> the requested `task_id` matches the current turn, and calls
> `session.abort()`.

What if the requested `task_id` does **not** match `current.task_id`
(stale UI; turn already ended; new turn started)? Plan implies 204
"still returns 204" but is ambiguous. State it:

- No current turn → 204 (idempotent).
- Current turn exists but task_id differs → 204 (idempotent), do
  **not** abort the unrelated current turn.

The second case is the dangerous one; an implementer following the
text loosely could accidentally abort the wrong turn.

## 9. [gap] EventSource reconnect during the *quiet* window misses the prior turn entirely

The buffer is per-current-turn and is cleared on `finish`/`abort`. If a
tab is mid-turn, network blips for 30 s, EventSource auto-reconnects,
**but the turn has finished in the meantime**: the replay buffer is
empty, no events arrive, the tab still shows its partial overlay
forever (or until the next user action triggers a refetch).

The plan addresses this only by reference: "Resumption across
already-committed turns relies on the GET history fetch." But the hook
in the plan does not refetch on EventSource reconnect — only on
`finish`. So the tab is stuck.

Either:

- Hook triggers a history refetch on every EventSource (re)open while
  it has a non-empty assistant overlay; or
- Server emits a `noop` / `sync` frame whose presence-or-absence the
  client can use to detect "no current turn, you should refetch"; or
- Document the user-visible glitch and the manual recovery.

## 10. [trade-off] "GC: never" is a documented memory leak

> GC: never. One `SessionState` per distinct conversation key ever
> referenced. Bounded by user behaviour; acceptable for v1.

Fine for single-user dev. For Tier-2 / SaaS this is a slow leak: every
ever-visited conversation occupies an entry plus its frame buffer's
peak retained capacity (Vec doesn't shrink on clear unless explicitly
shrunk). At 10k conversations and ~few KB per `SessionState`, this
isn't catastrophic, but it should be a `// TODO(M??)` with the trigger
condition documented (e.g. "evict if `current` is None and no
subscribers for > 1 h").

The plan acknowledges this in "What this plan does NOT do" — that's
enough provided the v1-acceptable scope is genuine.

## 11. [bug] "Each step compiles and `cargo test` passes" is not what the steps do

Steps 1, 2, 3 of the implementation order explicitly say things like:

> Worker and chat handler still reference the old `proto::` symbols —
> temporarily break, fixed in step 3.
> […]
> `cargo build` passes; handlers still broken.

Then the verification block claims "Each step compiles and `cargo test`
passes (or the failures are the ones the step is explicitly removing)."

These contradict. The plan should either:

- Reorder so the build is green at every step (probably impossible
  without a feature flag), or
- Drop the "compiles cleanly" promise and instead state which step is
  the resync point (likely step 6 or 7).

A reader following the plan will be confused when step 1 produces
compile errors that don't match the description.

## 12. [gap] No description of `commit_turn` ordering vs. `finish` frame

The current worker calls `commit_turn`, generates a title, then emits
`finish`. The new plan keeps `commit_turn` unchanged but adds the
session abstraction. Question:

- If `commit_turn` succeeds but title generation hangs for 10 s, does
  the SSE `finish` frame wait? The current code waits (sequential).
  In the new design this means the SSE shows "..." for 10 s after the
  last token. Worth documenting that title generation should be
  spawned in a detached task **after** the `finish` emission so the
  UI is unblocked.

## 13. [gap] Drop / panic safety of `InFlightTurn`

If the worker panics partway through, who clears `current` and
broadcasts `clear`? The plan removes the existing `scopeguard` ("goes
away — there's no `TaskRegistry`") without replacing it. Need an
equivalent: a guard on the worker that calls `session.abort()` on
unwind. Otherwise a panic leaves the session permanently "turn in
progress" and every subsequent POST returns 409.

Trivial to add (one scopeguard in `run`), but it must be in the plan.

## 14. [gap] How does the SSE handler join the session before frames are missed?

POST handler synchronously calls `start_turn` (which fans out
`turn_start` + `user_message` to existing subscribers) and returns 202.
The originating tab then waits for the SSE `turn_start` to learn the
task_id.

But there's a window: the originating tab's POST returns before its
GET `/stream` has finished its TCP handshake / TLS / oauth2-proxy
forwarding. In that window the `turn_start` event is in the replay
buffer (good) — so when SSE connects, it's replayed. **This relies on
the replay-buffer correctness in (4).** As long as (4) is fixed, this
works; the plan should note that the replay buffer is precisely the
mechanism that lets the originating tab subscribe lazily after POST.

## 15. [trade-off] One in-flight turn per conversation breaks parallel-tabs UX

The current (v2) design allows two tabs to submit simultaneously
("last writer wins"). The v3 plan rejects that: second POST → 409.
That's a regression in UX for the rare-but-real case of "I have two
windows open and I forgot." Not a flaw — a deliberate trade-off — but
the plan should explicitly acknowledge the regression vs. v2 so a
future reader doesn't read it as just "stricter, better."

## 16. [nit] `chat::registry.rs` keying

> `DashMap<ConversationId, Arc<SessionState>>`

`ConversationId` is a `surrealdb::RecordId` (per `chat/worker.rs:47`).
`RecordId` is `Hash + Eq` so this works, but the plan should note that
keys are `RecordId` not bare strings — the `/stop` handler parses the
URL to `ConversationId` for lookup, not to a string.

## 17. [nit] `proto::*` snapshot tests get fully rewritten — losing the guardrail

The existing snapshot tests on `api/stream.rs` are explicitly called
out as a "vibe-coded guardrail" in `docs/architecture/testing.md`. The
plan rewrites them wholesale to test SSE bytes. That's fine — but the
new tests need the same byte-level rigor (exact `\n`, exact JSON key
ordering) or the guardrail is weaker than what it replaces. The plan
says "Snapshot tests in `sse::tests` for every writer," which is the
right shape; just make sure the byte-level discipline carries over,
not a softer "roughly-shaped" assertion.

---

## Verdict

**Achievable, but not as written.** The blocker (1) is the only thing
that would actually prevent the design from working at all. The bugs
(2, 3, 4) are subtle but each will produce a user-visible glitch that
will be hard to debug after the fact. The gaps (5, 6, 8, 9, 12, 13, 14)
are places where two implementers will make different decisions and
the system's behavior will depend on which one got the keyboard.

Order of fixes I would want in the plan before starting:

1. Resolve the AuthedDb / pool lifetime issue (§1) — affects backend
   shape, so it has to be decided up front.
2. Pin down replay/reset semantics (§2, §4) — affects both worker and
   client.
3. Add the worker-state-machine ordering for commit-vs-abort (§3, §13)
   — small but load-bearing.
4. Specify channel capacity and slow-subscriber policy (§5, §6).
5. The rest can be inline comments in the doc.

None of those require redesigning the architecture — they're all
clarifications and small constraints on top of the structure the plan
already lays out. After they land, this design is a real improvement
over v2 for the multi-tab use case it's targeting.
