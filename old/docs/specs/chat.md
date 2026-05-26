# Chat — Functional Specification

Scope: the conversational surface that backs **Pillar 2 — Exploration
(RAG chat with the corpus)** in [`SPEC.md`](./SPEC.md) and, when it
ships, **Pillar 3 — Analysis (per-document chat)**. Both pillars
converge on one chat surface; this spec covers its behaviour.

This document defines **what** the chat surface must do. **How** it is
built is in [`../architecture/chat-v4.md`](../architecture/chat-v4.md).

## Goals

- A user can hold a conversation with an LLM against their corpus,
  with citations into the source documents (the RAG behaviour comes
  from the LLM/retrieval stack; this spec only concerns the chat
  contract).
- A user with **the same conversation open in multiple tabs / devices
  / browsers** sees identical, live state on every surface — including
  the tokens streaming in mid-turn, citations, stop, error.
- No tab is special. The tab that submitted a turn must not have any
  capability — display, history, or control — that other tabs lack.
- Conversation state survives client disconnects (network blip, tab
  close, refresh). A turn that started must run to completion and be
  persisted unless explicitly cancelled.

## Non-goals

- Real-time presence ("Alice is typing"). Not in scope.
- Resumption of streaming across browser sessions (close laptop today,
  open tomorrow, see the rest of yesterday's reply stream). Tabs see
  what is currently in flight; finished turns are read from history.
- Multi-user collaborative chat (two different humans in the same
  conversation). The multi-tab story is about one user across surfaces.
- Per-message edit / delete / branch. A turn is a user message plus
  exactly one assistant reply; both land atomically.

## Functional requirements

Each requirement carries a stable id (`R<n>`) so the architecture doc
and tests can cite it.

### R1. Conversation history is the authoritative read

Opening a conversation must yield its full committed history in
chronological order. The history endpoint is independent of any
streaming machinery — it reads from persistent storage.

### R2. Multi-tab live updates

When *any* tab subscribed to a conversation produces a new turn, all
other tabs subscribed to the same conversation see, in order:

1. The user message,
2. Any citation table the LLM is grounded on (if RAG was used),
3. Each assistant-token delta as it arrives,
4. A turn-end signal that includes the persisted assistant message id,
5. — or, in lieu of (4), an explicit cancellation signal (see R5).

These updates happen without any user-visible latency budget beyond
the LLM's own token cadence and one network hop.

### R3. Late-join replay

A tab that subscribes to a conversation *while a turn is in progress*
must, on connect, receive all events the turn has emitted so far, in
order, before it starts seeing live events. From the user's
perspective the tab catches up "instantly," showing the in-progress
user message and partial assistant reply.

After a turn completes, late-joining tabs see no replay (the
in-progress state is gone) and rely on R1 for the now-committed pair.

### R4. Reconnect tolerates network blips

A subscribed tab whose underlying transport drops and reconnects
(network blip, proxy idle timeout) must converge to the correct state
without user intervention:

- If a turn is still in progress, it sees the replay and the live
  tail.
- If the turn finished while the connection was down, it sees no
  replay; the tab reconciles by refetching history (R1).

The rendering must not show duplicate user messages, doubled assistant
text, or stale "thinking…" indicators after reconnect.

### R5. Stop is conversation-scoped and visible to all tabs

Any tab can cancel the in-flight turn for its conversation. The
cancellation is communicated to all other subscribed tabs, which
roll back their in-flight UI state to identical pre-turn condition:

- The streaming assistant overlay disappears.
- The in-flight user message disappears (the user message is not
  persisted until the turn commits — see R7).
- The stop button reverts to a send button.

A stop that arrives after the turn has already finished naturally is
a no-op (idempotent success). A stop that arrives while the worker is
already past the LLM loop and inside the persistence step does **not**
roll back the in-flight UI — the turn is treated as completed and the
normal turn-end signal (R2.4) wins. (The user-visible effect of "click
stop a millisecond too late" is "stop apparently didn't work" — which
is the only acceptable outcome since the message is now persisted.)

### R6. One turn per conversation at a time

A user cannot start a second turn in a conversation that already has
a turn in progress. The system rejects a concurrent submit attempt
with a clear "conversation is busy" signal that the UI surfaces to
the user. The user's recourse is to wait, or to stop the in-flight
turn and resubmit.

This is a deliberate constraint over allowing parallel turns with
last-writer-wins resolution. The trade-off is "two windows open and I
forgot" → one window sees a 'busy' message, in exchange for a much
simpler shared-state model.

### R7. User and assistant messages persist atomically at commit time

A submitted user message does **not** land in persistent storage until
the LLM has finished generating its reply and both rows are written
together. As a consequence:

- A cancelled turn leaves no rows behind.
- A failed turn (LLM error mid-stream) may persist a partial assistant
  reply alongside the user message, but never the user message alone.
- A refresh during an in-flight turn shows no rows from that turn yet;
  the next history fetch after the turn commits shows both.

This guarantees no orphan user messages and makes cancellation purely
a UI rollback, with no storage migration.

### R8. Optimistic concurrency on submit

Each submit declares the user's view of "what the last message was."
If that view is stale (another tab committed a turn in between), the
submit is rejected with a "conversation has moved" signal. The
client's recourse is to refetch history and let the user retry.

### R9. Stop button visibility tracks live state

The UI must show a stop affordance exactly when the conversation has
a turn in progress (which the tab learns from R2). When no turn is in
progress, the UI shows a send affordance.

This must be **the same on every tab** — if tab A submitted a turn,
tab B's stop button must also become visible.

### R10. Citations are part of the turn

When the LLM is grounded on retrieved corpus chunks, the citation
table arrives over the same channel as the streaming text and before
the first text delta, so the UI can render `[N]` markers as they
appear in the assistant reply.

## Out of scope, will revisit

- **Multi-conversation live updates.** A tab subscribed to conversation
  A is not notified when conversation B receives a turn. Sidebar
  refresh happens on user navigation or refocus, not as a push.
- **Cross-device handoff with resume.** Closing a laptop mid-stream
  and re-opening on a phone shows whatever has been committed by then;
  the in-progress stream does not resume on the new device.
- **Editing or branching past turns.** Out of v1.
- **Concurrent turns per conversation** (see R6) — explicitly chosen
  against.

## Acceptance scenarios

The system passes if all of the following can be reproduced by hand:

1. **Single tab round-trip.** Open a conversation, send a message,
   watch streaming tokens, see the turn commit, send a follow-up.
2. **Live fan-out.** Open the same conversation in two tabs, send
   from tab A, observe tab B render the user message and stream the
   reply live.
3. **Mid-stream stop, both tabs.** With a turn streaming in both
   tabs, click stop in tab B. Both tabs roll back to pre-turn state
   identically. Reload either tab: nothing persisted from the
   cancelled turn.
4. **Concurrent submit rejected.** With a turn streaming in tab A,
   submit from tab B. Tab B is told the conversation is busy; tab A's
   turn is unaffected; tab A commits normally.
5. **Refresh mid-stream.** With a turn streaming in tab A, refresh
   tab B (or open a new tab on the same conversation). Tab B
   reconstructs the in-flight state — user message present, partial
   assistant reply present — and converges to the same final state as
   tab A.
6. **Network blip after turn ends.** Disconnect tab B's network
   mid-stream, let tab A finish naturally, reconnect tab B. Tab B
   converges to the committed history without showing stale
   "thinking…" forever.
