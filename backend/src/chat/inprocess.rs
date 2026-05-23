//! In-process [`TurnBus`] — single instance, in-memory (Phase 1).
//!
//! One [`Session`] per *live* `ConversationId`. Each session is the v4
//! ephemeral state for one conversation: a bounded delta log (whole SSE
//! frames, each with a monotonic [`Cursor`]) plus the single-flight
//! `running` flag and the per-turn cancel token.
//!
//! ### Lifetime — refcount, not GC (§9)
//!
//! The `DashMap` holds a **weak** reference; the strong owners are the
//! consumers — each subscriber's reader stream and the worker's
//! `TurnHandle`. A session therefore lives exactly as long as someone is
//! using it: the worker keeps it alive for the whole turn (so a turn always
//! finishes and persists even if every tab closes mid-stream), and a
//! subscriber keeps it alive while it streams. When the last strong owner
//! drops, `Session::drop` prunes its own (now-dead) map entry. No sweeper,
//! no grace window, no idle cap, no timers.
//!
//! Reincarnation is safe: a session freed between a sole tab's blip and its
//! reconnect comes back with a fresh [`Cursor`] generation (high bits), so
//! the old `Last-Event-Id` falls below the new floor and resyncs (§4.1).
//!
//! ### Bounded buffer — two turns (§4.1)
//!
//! The log retains at most `[previous turn][current turn]`, trimmed at
//! `try_begin` via a single `turn_cursor` (the start of the current turn).
//! A client up to one turn behind resumes seamlessly across the boundary;
//! only a client 2+ turns behind (genuinely stuck) gets `resync`.
//!
//! ### Concurrency (§7)
//!
//! A single `std::sync::Mutex<Inner>` guards each session's buffer. The
//! critical sections are tiny and whole-frame — push one `Bytes` [writer]
//! or clone out a slice [reader] — with **no `.await` held across the
//! lock**. One writer per conversation (single-flight) ⇒ no writer-writer
//! contention; ≈2 readers at frame cadence ⇒ negligible. A
//! `tokio::sync::watch<u64>` (carrying `next`) is the "frame appended"
//! wakeup; readers park on `changed()` rather than polling. `watch`, not
//! raw `Notify`, so a frame appended *during* a socket write isn't a lost
//! wakeup (the receiver tracks a version).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures::stream::BoxStream;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::api::sse;
use crate::storage::ConversationId;

use super::bus::{AlreadyRunning, Cursor, TurnBus, TurnHandle};

/// The weak index from conversation to its live session.
type SessionMap = DashMap<ConversationId, Weak<Session>>;

/// Result of [`Session::read_from`] (§4.1).
enum Read {
    /// Whole frames the caller hasn't seen yet (possibly empty when
    /// caught up). Each carries its cursor for the `id:` prefix.
    Frames(Vec<(Cursor, Bytes)>),
    /// The caller's cursor fell out of the window — re-read history.
    Resync,
}

/// Mutable per-conversation state. The log holds at most two turns —
/// `[previous][current]` — bounded by `turn_cursor`; older turns and all
/// committed content are durable SurrealDB rows.
struct Inner {
    /// Buffered SSE frames, oldest first: the previous turn followed by the
    /// in-flight (or just-finished) one. Each is one pre-formatted SSE
    /// frame (`event:`/`data:`); the `id:` line is prepended by the reader
    /// at send time, not stored here.
    frames: Vec<(Cursor, Bytes)>,
    /// First cursor of the **current** turn. Doubles as the trim boundary
    /// (`try_begin` drops everything below it) and the fresh-join replay
    /// start (`read_from(None)`). The resume/resync floor is *derived*
    /// from `frames.first()`, not this — see [`Session::read_from`].
    turn_cursor: Cursor,
    /// Next cursor to assign. Monotonic within this incarnation — **never
    /// reset** — so a cursor never repeats across turns; a new incarnation
    /// gets a fresh generation (high bits) instead.
    next: Cursor,
    /// True while a turn is in flight (single-flight gate).
    running: bool,
    /// Current turn's cancel token; `None` when idle.
    cancel: Option<CancellationToken>,
}

/// Per-conversation session. Cheap to share (`Arc`); freed when the last
/// strong owner (a reader stream or the worker's handle) drops.
pub(super) struct Session {
    inner: Mutex<Inner>,
    /// "Frame appended" wakeup, carrying the latest `next`. Readers park on
    /// `changed()`.
    notify: watch::Sender<u64>,
    /// This session's key, and a weak handle to the owning map, so `Drop`
    /// can prune its own entry. Weak ⇒ no strong cycle (map → `Weak<Session>`,
    /// session → `Weak<SessionMap>`).
    conv: ConversationId,
    map: Weak<SessionMap>,
}

impl Session {
    fn new(conv: ConversationId, map: Weak<SessionMap>, gen: u64) -> Self {
        let (notify, _rx) = watch::channel(0);
        // Seed this incarnation's cursor space at the start of its
        // generation (sequence 0). Sequences run contiguously from here.
        let start = Cursor::generation(gen);
        Self {
            inner: Mutex::new(Inner {
                frames: Vec::new(),
                turn_cursor: start,
                next: start,
                running: false,
                cancel: None,
            }),
            notify,
            conv,
            map,
        }
    }

    /// Recover rather than cascade on poison: `TurnHandle::Drop` retakes
    /// this lock during unwind, so a poisoned guard there would double-
    /// panic → abort. The state behind it is plain data; recovering is
    /// safe and keeps the conversation unwedged.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Single-flight claim. Returns the new turn's cancel token, or `None`
    /// if a turn is already running. Trims the buffer to at most two turns
    /// (drop everything below the *old* `turn_cursor`), then begins the new
    /// turn at the live edge before appending `user_message`.
    pub(super) fn try_begin(&self, user_message: Bytes) -> Option<CancellationToken> {
        let (token, next) = {
            let mut g = self.lock();
            if g.running {
                return None;
            }
            // Keep only the current turn (cursors >= turn_cursor); this
            // drops the turn two-ago. The new turn then starts at `next`.
            let keep_from = g.turn_cursor;
            g.frames.retain(|(c, _)| *c >= keep_from);
            g.turn_cursor = g.next;
            let c = g.next;
            g.next = c.next();
            g.frames.push((c, user_message));
            g.running = true;
            let token = CancellationToken::new();
            g.cancel = Some(token.clone());
            (token, g.next.get())
        };
        let _ = self.notify.send_replace(next);
        Some(token)
    }

    /// Append one whole frame to the in-flight turn and wake readers.
    /// No-op if the turn already ended.
    pub(super) fn append(&self, frame: Bytes) {
        let next = {
            let mut g = self.lock();
            if !g.running {
                return;
            }
            let c = g.next;
            g.next = c.next();
            g.frames.push((c, frame));
            g.next.get()
        };
        let _ = self.notify.send_replace(next);
    }

    /// Append the terminal frame and release the slot. Frames linger (not
    /// cleared) so still-draining readers see the terminal frame, and a
    /// reconnecting client one turn behind can still resume; they're
    /// trimmed when the next turn begins.
    pub(super) fn terminate(&self, frame: Bytes) {
        let next = self.close_with(frame);
        let _ = self.notify.send_replace(next);
    }

    /// Append a one-off frame **outside** the turn lifecycle (e.g. a
    /// `title` update after `finish`) and wake readers. Unlike `append`
    /// this does not require a running turn — it's the post-turn live-push
    /// path behind [`super::TurnBus::emit`]. The frame takes the next
    /// cursor (≥ `turn_cursor`, so live readers and the current-turn replay
    /// see it) and is trimmed with the current turn; a fresh joiner that
    /// missed it recovers the same state from the DB.
    pub(super) fn emit_standalone(&self, frame: Bytes) {
        let next = {
            let mut g = self.lock();
            let c = g.next;
            g.next = c.next();
            g.frames.push((c, frame));
            g.next.get()
        };
        let _ = self.notify.send_replace(next);
    }

    /// `TurnHandle::Drop` path: if a turn is still running (worker
    /// panicked / handle abandoned without `terminate`), emit `clear` and
    /// release the slot so the conversation isn't wedged at 409.
    pub(super) fn clear_if_running(&self) {
        let next = {
            let g = self.lock();
            if !g.running {
                return;
            }
            drop(g);
            self.close_with(sse::clear())
        };
        let _ = self.notify.send_replace(next);
    }

    /// Push a terminal frame and mark idle. Returns the new `next` for the
    /// wake. Caller sends the wake after releasing.
    fn close_with(&self, frame: Bytes) -> u64 {
        let mut g = self.lock();
        let c = g.next;
        g.next = c.next();
        g.frames.push((c, frame));
        g.running = false;
        g.cancel = None;
        g.next.get()
    }

    /// Flip the in-flight turn's cancel token (no-op if idle).
    pub(super) fn request_cancel(&self) {
        if let Some(tok) = self.lock().cancel.as_ref() {
            tok.cancel();
        }
    }

    /// Compute the next batch for a subscriber at `cursor` (§4.1).
    fn read_from(&self, cursor: Option<Cursor>) -> Read {
        let g = self.lock();
        match cursor {
            // Fresh connect: replay the *current* turn from its start
            // (`>= turn_cursor`), or nothing if idle. A lingering finished
            // turn (running=false, frames still buffered) must NOT be
            // replayed to a fresh joiner — it relies on history. A
            // previous-turn frame still in the two-turn buffer is likewise
            // excluded (only the current turn is replayed live).
            None => {
                if g.running {
                    let tc = g.turn_cursor;
                    let batch = g
                        .frames
                        .iter()
                        .filter(|(c, _)| *c >= tc)
                        .cloned()
                        .collect();
                    Read::Frames(batch)
                } else {
                    Read::Frames(Vec::new())
                }
            }
            // Resume after cursor `c`. The floor is the oldest *retained*
            // frame (derived, O(1)) — NOT `turn_cursor`: a resumer one turn
            // behind sits in the previous turn and must stream continuously
            // across the boundary without resync. A cursor from an earlier
            // incarnation (lower generation) is numerically below this
            // floor, so it resyncs here too with no generation branching.
            Some(c) => {
                let floor = g.frames.first().map(|(fc, _)| *fc).unwrap_or(g.next);
                if c.get() + 1 >= floor.get() {
                    let batch = g
                        .frames
                        .iter()
                        .filter(|(fc, _)| fc.get() > c.get())
                        .cloned()
                        .collect();
                    Read::Frames(batch)
                } else {
                    Read::Resync
                }
            }
        }
    }

    /// Build the SSE reader stream (§7): pull-based, woken by `notify`.
    /// Yields already-`id:`-prefixed frames; the SSE handler writes them
    /// verbatim. Holding `self: Arc<Self>` is what keeps the session alive
    /// for as long as this stream is consumed (refcount lifetime).
    pub(super) fn reader(self: Arc<Self>, from: Option<Cursor>) -> BoxStream<'static, Bytes> {
        Box::pin(async_stream::stream! {
            let mut cursor = from;
            let mut rx = self.notify.subscribe();
            loop {
                // Mark the current version seen BEFORE slicing, so a frame
                // appended during the socket write (the `yield`) below is
                // not a lost wakeup: if `send_replace` races our read,
                // `changed()` returns immediately on the next iteration.
                rx.borrow_and_update();
                match self.read_from(cursor) {
                    Read::Frames(frames) => {
                        for (c, f) in frames {
                            yield prepend_id(c, &f);
                            cursor = Some(c);
                        }
                    }
                    Read::Resync => {
                        yield sse::resync();
                        // Continue as a fresh subscriber: re-read now.
                        cursor = None;
                        continue;
                    }
                }
                if rx.changed().await.is_err() {
                    // Sender dropped = session freed (we were the last
                    // strong owner). End cleanly.
                    return;
                }
            }
        })
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // Self-prune: remove our own map entry, but only if it still points
        // at *us* (a dead weak). If a newer incarnation already replaced the
        // entry during our teardown, its weak upgrades and we leave it
        // alone. Both this `remove_if` and `get_or_create`'s `entry` run
        // under the DashMap shard lock, so the create-vs-drop race is clean.
        if let Some(map) = self.map.upgrade() {
            map.remove_if(&self.conv, |_, w| w.upgrade().is_none());
        }
    }
}

/// Prepend the SSE `id:` line to a buffered frame at send time, so the
/// stored frame stays replayable and the cursor lives beside it rather
/// than baked into the bytes.
fn prepend_id(c: Cursor, frame: &Bytes) -> Bytes {
    let prefix = format!("id: {c}\n");
    let mut buf = BytesMut::with_capacity(prefix.len() + frame.len());
    buf.extend_from_slice(prefix.as_bytes());
    buf.extend_from_slice(frame);
    buf.freeze()
}

/// In-process [`TurnBus`]. One per backend process; held in `AppState` as
/// `Arc<dyn TurnBus>`.
pub struct InProcessBus {
    /// Weak index from conversation to its live session. `Arc` so sessions
    /// can hold a `Weak<SessionMap>` for self-pruning on drop.
    sessions: Arc<SessionMap>,
    /// Source of per-incarnation [`Cursor`] generations. Bumped once per
    /// session creation; bus-scoped (not a process global) so tests on a
    /// fresh bus get deterministic generation 0.
    next_gen: AtomicU64,
}

impl InProcessBus {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
            next_gen: AtomicU64::new(0),
        }
    }

    /// Mint a fresh session for `conv` with the next generation.
    fn new_session(&self, conv: &ConversationId) -> Arc<Session> {
        let gen = self.next_gen.fetch_add(1, Ordering::Relaxed);
        Arc::new(Session::new(
            conv.clone(),
            Arc::downgrade(&self.sessions),
            gen,
        ))
    }

    /// The live session for `conv`, creating one if none exists (or the
    /// mapped weak is dead). Multiple callers share the same `Arc` so they
    /// read/write one buffer.
    fn get_or_create(&self, conv: &ConversationId) -> Arc<Session> {
        // Fast path: a live session already mapped (read lock only).
        if let Some(w) = self.sessions.get(conv) {
            if let Some(s) = w.upgrade() {
                return s;
            }
        }
        // Slow path under the shard write lock via the `entry` API. Re-check
        // in case we raced another creator or a concurrent self-pruning
        // `Drop`; insert only when there is no live session.
        match self.sessions.entry(conv.clone()) {
            Entry::Occupied(mut e) => match e.get().upgrade() {
                Some(s) => s,
                None => {
                    let s = self.new_session(conv);
                    e.insert(Arc::downgrade(&s));
                    s
                }
            },
            Entry::Vacant(e) => {
                let s = self.new_session(conv);
                e.insert(Arc::downgrade(&s));
                s
            }
        }
    }

    /// Look up a live session without creating one.
    fn get(&self, conv: &ConversationId) -> Option<Arc<Session>> {
        self.sessions.get(conv).and_then(|w| w.upgrade())
    }
}

impl Default for InProcessBus {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TurnBus for InProcessBus {
    async fn try_start(
        &self,
        conv: &ConversationId,
        user_message: Bytes,
    ) -> Result<TurnHandle, AlreadyRunning> {
        let session = self.get_or_create(conv);
        match session.try_begin(user_message) {
            Some(cancel) => Ok(TurnHandle::new(session, cancel)),
            None => Err(AlreadyRunning),
        }
    }

    async fn subscribe(
        &self,
        conv: &ConversationId,
        from: Option<Cursor>,
    ) -> BoxStream<'static, Bytes> {
        self.get_or_create(conv).reader(from)
    }

    async fn cancel(&self, conv: &ConversationId) {
        // Look up without creating — nothing to cancel for a conversation
        // that has no live session.
        if let Some(session) = self.get(conv) {
            session.request_cancel();
        }
    }

    async fn emit(&self, conv: &ConversationId, frame: Bytes) {
        // Look up without creating — if nobody's subscribed there's
        // nothing live to push to, and the DB already holds the truth.
        if let Some(session) = self.get(conv) {
            session.emit_standalone(frame);
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests — single-flight reject, the §4.1 `read_from` rules (fresh-join
// replay, two-turn retention, resume-across-boundary, resync), and the
// refcount lifetime (self-pruning + safe reincarnation).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    fn s(b: &Bytes) -> String {
        String::from_utf8(b.to_vec()).unwrap()
    }

    /// A standalone session (generation 0, not mapped) for the pure
    /// buffer/cursor tests that don't exercise the bus or lifetime.
    fn detached() -> Session {
        Session::new(
            surrealdb::RecordId::from(("conversation", "test")),
            Weak::new(),
            0,
        )
    }

    #[test]
    fn try_begin_rejects_second_concurrent() {
        let sess = detached();
        assert!(sess.try_begin(sse::user_message("message:01J", "hi")).is_some());
        assert!(
            sess.try_begin(sse::user_message("message:02J", "again"))
                .is_none(),
            "second begin must be rejected while running"
        );
    }

    #[test]
    fn read_from_none_replays_in_flight_but_not_lingering() {
        let sess = detached();
        sess.try_begin(sse::user_message("message:01J", "hi"));
        sess.append(sse::text("hello"));
        // In flight: fresh joiner replays the whole turn.
        match sess.read_from(None) {
            Read::Frames(f) => assert_eq!(f.len(), 2, "user_message + text"),
            Read::Resync => panic!("unexpected resync"),
        }
        // Terminate: frames linger, but a fresh joiner must NOT replay them.
        sess.terminate(sse::finish("stop", "message:a1"));
        match sess.read_from(None) {
            Read::Frames(f) => assert!(f.is_empty(), "lingering finished turn not replayed"),
            Read::Resync => panic!("unexpected resync"),
        }
    }

    #[test]
    fn buffer_retains_at_most_two_turns() {
        let sess = detached();
        // Three single-exchange turns (user_message + finish = 2 frames each).
        for i in 0..3 {
            sess.try_begin(sse::user_message(&format!("message:u{i}"), "hi"));
            sess.terminate(sse::finish("stop", &format!("message:a{i}")));
        }
        let g = sess.lock();
        assert_eq!(g.frames.len(), 4, "at most two turns buffered");
        // The oldest retained frame is the start of the second-to-last turn
        // (turn 1 was trimmed when turn 2 began). Cursors: turn0 [0,1],
        // turn1 [2,3], turn2 [4,5]; turn0 dropped → first is cursor 2.
        assert_eq!(g.frames.first().unwrap().0, Cursor(2));
    }

    #[test]
    fn resume_across_turn_boundary_does_not_resync() {
        let sess = detached();
        // Turn 1: cursors 0,1,2.
        sess.try_begin(sse::user_message("message:01J", "hi"));
        sess.append(sse::text("a"));
        sess.terminate(sse::finish("stop", "message:a1"));
        // Turn 2 starts: turn 1 is retained (two-turn buffer).
        sess.try_begin(sse::user_message("message:02J", "next")); // cursor 3
        // A client still draining turn 1 (cursor 0) resumes seamlessly into
        // turn 2 — no resync at the boundary.
        match sess.read_from(Some(Cursor(0))) {
            Read::Frames(f) => assert_eq!(f.len(), 3, "frames 1,2,3 after cursor 0"),
            Read::Resync => panic!("one turn behind must not resync"),
        }
    }

    #[test]
    fn resync_only_when_two_turns_behind() {
        let sess = detached();
        sess.try_begin(sse::user_message("message:01J", "a")); // 0
        sess.terminate(sse::finish("stop", "message:a1")); // 1
        sess.try_begin(sse::user_message("message:02J", "b")); // 2 (turn1 retained)
        sess.terminate(sse::finish("stop", "message:a2")); // 3
        // Still only one turn back from cursor 0 → resumes.
        assert!(matches!(sess.read_from(Some(Cursor(0))), Read::Frames(_)));
        // Turn 3 starts: turn 1 trimmed, oldest retained is turn 2 (cursor 2).
        sess.try_begin(sse::user_message("message:03J", "c")); // 4
        // Cursor 0 is now two turns behind the oldest retained frame → resync.
        assert!(
            matches!(sess.read_from(Some(Cursor(0))), Read::Resync),
            "cursor below the retained window must resync"
        );
        // A client at the start of the retained window resumes normally.
        assert!(matches!(sess.read_from(Some(Cursor(2))), Read::Frames(_)));
    }

    #[tokio::test]
    async fn reader_replays_then_streams_live() {
        let bus = InProcessBus::new();
        let conv: ConversationId = surrealdb::RecordId::from(("conversation", "abc"));
        let mut handle = bus
            .try_start(&conv, sse::user_message("message:u1", "hi"))
            .await
            .expect("start");
        let mut stream = bus.subscribe(&conv, None).await;

        // Replayed user_message, id-prefixed (generation 0 → cursor 0).
        let first = stream.next().await.expect("frame");
        assert_eq!(
            s(&first),
            "id: 0\nevent: user_message\ndata: {\"id\":\"message:u1\",\"content\":\"hi\"}\n\n"
        );

        // Live text delta wakes the parked reader.
        handle.append(sse::text("yo")).await;
        let second = stream.next().await.expect("frame");
        assert_eq!(s(&second), "id: 1\nevent: text\ndata: \"yo\"\n\n");

        // Terminal frame.
        handle.terminate(sse::finish("stop", "message:a1")).await;
        let third = stream.next().await.expect("frame");
        assert!(s(&third).starts_with("id: 2\nevent: finish"));
    }

    #[tokio::test]
    async fn reader_emits_resync_for_stale_cursor() {
        let bus = InProcessBus::new();
        let conv: ConversationId = surrealdb::RecordId::from(("conversation", "stale"));
        // Hold a subscriber so the session (one incarnation) survives across
        // turns and cursors keep advancing within the same generation.
        let _keepalive = bus.subscribe(&conv, None).await;
        // Three turns so the two-turn buffer trims turn 1 (cursor 0).
        for i in 0..3 {
            let mut h = bus
                .try_start(&conv, sse::user_message(&format!("message:u{i}"), "x"))
                .await
                .expect("start");
            h.terminate(sse::finish("stop", &format!("message:a{i}"))).await;
        }
        // A subscriber resuming from the now-trimmed cursor 0 is told to
        // resync, then replays fresh.
        let mut stream = bus.subscribe(&conv, Some(Cursor(0))).await;
        let first = stream.next().await.expect("frame");
        assert_eq!(s(&first), "event: resync\ndata: null\n\n");
    }

    #[tokio::test]
    async fn idle_fresh_subscriber_gets_no_replay() {
        let bus = InProcessBus::new();
        let conv: ConversationId = surrealdb::RecordId::from(("conversation", "idle"));
        // Never started a turn: a fresh subscriber must block (no frame),
        // relying on history. Assert nothing is immediately available.
        let mut stream = bus.subscribe(&conv, None).await;
        let pending =
            tokio::time::timeout(std::time::Duration::from_millis(50), stream.next()).await;
        assert!(pending.is_err(), "idle session must not yield a replay frame");
    }

    #[tokio::test]
    async fn cancel_flips_token_observed_by_handle() {
        let bus = InProcessBus::new();
        let conv: ConversationId = surrealdb::RecordId::from(("conversation", "cancel"));
        let handle = bus
            .try_start(&conv, sse::user_message("message:u1", "hi"))
            .await
            .expect("start");
        bus.cancel(&conv).await;
        // The handle's cancel future resolves promptly.
        tokio::time::timeout(std::time::Duration::from_millis(50), handle.cancelled())
            .await
            .expect("cancel observed");
    }

    #[tokio::test]
    async fn session_freed_when_last_consumer_drops() {
        let bus = InProcessBus::new();
        let conv: ConversationId = surrealdb::RecordId::from(("conversation", "life"));
        {
            // The handle is the only strong owner (no subscribers).
            let _h = bus
                .try_start(&conv, sse::user_message("message:u1", "hi"))
                .await
                .expect("start");
            assert_eq!(bus.sessions.len(), 1, "session created and mapped");
        }
        // Handle dropped → `clear_if_running` then `Session::drop` prunes
        // the dead map entry. No GC, no timer.
        assert!(
            bus.sessions.is_empty(),
            "session freed and unmapped when last consumer drops"
        );
    }

    #[tokio::test]
    async fn reincarnated_session_resyncs_stale_cursor() {
        let bus = InProcessBus::new();
        let conv: ConversationId = surrealdb::RecordId::from(("conversation", "reborn"));
        // First incarnation (generation 0) runs a turn, then is freed when
        // its handle drops with no subscribers.
        {
            let mut h = bus
                .try_start(&conv, sse::user_message("message:u1", "a"))
                .await
                .expect("start");
            h.terminate(sse::finish("stop", "message:a1")).await;
        }
        assert!(bus.sessions.is_empty(), "freed after the turn (no consumers)");

        // A client reconnects with a cursor minted by the freed incarnation.
        // `subscribe` creates a fresh incarnation (generation 1); the stale
        // cursor is below its floor → resync (this is the cursor-reuse guard
        // the generation packing buys us — without it the client would
        // silently miss the next turn's early frames).
        let mut stream = bus.subscribe(&conv, Some(Cursor(0))).await;
        let first = stream.next().await.expect("frame");
        assert_eq!(s(&first), "event: resync\ndata: null\n\n");
    }

    #[tokio::test]
    async fn dropped_handle_without_terminate_emits_clear() {
        let bus = InProcessBus::new();
        let conv: ConversationId = surrealdb::RecordId::from(("conversation", "panic"));
        let mut stream = bus.subscribe(&conv, None).await;
        let handle = bus
            .try_start(&conv, sse::user_message("message:u1", "hi"))
            .await
            .expect("start");
        // Read the user_message while the turn is in flight (a fresh `None`
        // subscriber only replays a *live* turn — §4.1).
        let first = stream.next().await.expect("user_message");
        assert!(s(&first).contains("event: user_message"));
        // Drop the handle without `terminate` (worker panic/unwind): `clear`
        // is emitted and the slot released. The subscriber's `Arc` keeps the
        // session alive long enough to observe it.
        drop(handle);
        let second = stream.next().await.expect("clear");
        assert!(s(&second).ends_with("event: clear\ndata: null\n\n"));
    }
}
