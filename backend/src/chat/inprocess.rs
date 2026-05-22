//! In-process [`TurnBus`] — single instance, in-memory (Phase 1).
//!
//! One [`Session`] per ever-touched `ConversationId` in a `DashMap`. Each
//! session is the v4 ephemeral state for one conversation: the in-flight
//! turn's delta log (whole SSE frames, each with a monotonic [`Cursor`])
//! plus the single-flight `running` flag and the per-turn cancel token.
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

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use futures::stream::BoxStream;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::api::sse;
use crate::storage::ConversationId;

use super::bus::{AlreadyRunning, Cursor, TurnBus, TurnHandle};

/// Result of [`Session::read_from`] (§4.1).
enum Read {
    /// Whole frames the caller hasn't seen yet (possibly empty when
    /// caught up). Each carries its cursor for the `id:` prefix.
    Frames(Vec<(Cursor, Bytes)>),
    /// The caller's cursor fell out of the window — re-read history.
    Resync,
}

/// Mutable per-conversation state. The log holds **only the current
/// in-flight turn** (cleared at `try_begin`); committed turns are durable
/// SurrealDB rows.
struct Inner {
    /// In-flight turn's frames, oldest first. Each is one pre-formatted
    /// SSE frame (`event:`/`data:`); the `id:` line is prepended by the
    /// reader at send time, not stored here.
    frames: Vec<(Cursor, Bytes)>,
    /// Cursor of `frames[0]` (or `next` when empty). The window floor:
    /// a resume cursor below `base - 1` triggers `resync`.
    base: Cursor,
    /// Next cursor to assign. Monotonic — **never reset** for the life of
    /// the session, so a cursor never repeats across turns.
    next: Cursor,
    /// True while a turn is in flight (single-flight gate).
    running: bool,
    /// Current turn's cancel token; `None` when idle.
    cancel: Option<CancellationToken>,
    /// When the last turn terminated. Drives the GC grace window (§9).
    finished_at: Option<Instant>,
    /// When this session was created. Idle-eviction reference for a
    /// session that has never run a turn (no `finished_at`).
    created_at: Instant,
}

/// Per-conversation session. Cheap to share (`Arc`).
pub(super) struct Session {
    inner: Mutex<Inner>,
    /// "Frame appended" wakeup, carrying the latest `next`. Subscriber
    /// count (`receiver_count`) doubles as the GC liveness signal.
    notify: watch::Sender<u64>,
}

impl Session {
    fn new() -> Self {
        let (notify, _rx) = watch::channel(0);
        Self {
            inner: Mutex::new(Inner {
                frames: Vec::new(),
                base: Cursor::ZERO,
                next: Cursor::ZERO,
                running: false,
                cancel: None,
                finished_at: None,
                created_at: Instant::now(),
            }),
            notify,
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
    /// if a turn is already running. Resets the log to hold only this turn
    /// (clear frames, `base = next`) before appending `user_message`.
    pub(super) fn try_begin(&self, user_message: Bytes) -> Option<CancellationToken> {
        let (token, next) = {
            let mut g = self.lock();
            if g.running {
                return None;
            }
            g.frames.clear();
            g.base = g.next;
            let c = g.next;
            g.next = c.next();
            g.frames.push((c, user_message));
            g.running = true;
            g.finished_at = None;
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

    /// Append the terminal frame and release the slot. Frames linger
    /// (not cleared) so still-draining readers see the terminal frame;
    /// GC trims them after the grace window.
    pub(super) fn terminate(&self, frame: Bytes) {
        let next = self.close_with(frame);
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

    /// Push a terminal frame, mark idle, stamp `finished_at`. Returns the
    /// new `next` for the wake. Caller sends the wake after releasing.
    fn close_with(&self, frame: Bytes) -> u64 {
        let mut g = self.lock();
        let c = g.next;
        g.next = c.next();
        g.frames.push((c, frame));
        g.running = false;
        g.cancel = None;
        g.finished_at = Some(Instant::now());
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
            // Fresh connect: replay the in-flight turn from its start, or
            // nothing if idle. A *lingering finished* turn (running=false,
            // frames still present during the grace window) must NOT be
            // replayed to a fresh joiner — it relies on history.
            None => {
                if g.running {
                    Read::Frames(g.frames.clone())
                } else {
                    Read::Frames(Vec::new())
                }
            }
            // Resume after cursor `c`.
            Some(c) => {
                if c.get() + 1 >= g.base.get() {
                    // Valid resume: hand back everything strictly after c
                    // (maybe empty if caught up). Covers a transient blip
                    // and a reconnect to a still-lingering finished turn,
                    // including its terminal frame.
                    let batch = g
                        .frames
                        .iter()
                        .filter(|(fc, _)| fc.get() > c.get())
                        .cloned()
                        .collect();
                    Read::Frames(batch)
                } else {
                    // Wanted cursor was trimmed (a completed turn was
                    // missed while disconnected) → resync.
                    Read::Resync
                }
            }
        }
    }

    /// Build the SSE reader stream (§7): pull-based, woken by `notify`.
    /// Yields already-`id:`-prefixed frames; the SSE handler writes them
    /// verbatim.
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
                    // Sender dropped = session evicted by GC. End cleanly.
                    return;
                }
            }
        })
    }

    // ---- GC support (the sweeper, §9) ----------------------------------

    /// Open subscriber count — the GC liveness signal.
    pub(super) fn subscriber_count(&self) -> usize {
        self.notify.receiver_count()
    }

    /// Grace-trim: an idle session whose last turn finished more than
    /// `grace` ago drops its frames and advances `base` to the live edge.
    /// Bounds memory for a long-lived connection spanning many turns; the
    /// data is already durable, so a straggler past the window gets
    /// `resync`. No wake is sent — parked readers have nothing new to see.
    pub(super) fn maybe_trim(&self, grace: Duration) {
        let mut g = self.lock();
        if g.running || g.frames.is_empty() {
            return;
        }
        if g.finished_at.map(|t| t.elapsed() >= grace).unwrap_or(false) {
            g.frames.clear();
            g.base = g.next;
        }
    }

    /// Evictable when idle (no in-flight turn), with no open subscribers,
    /// and idle for at least `idle_cap`. The idle reference is the last
    /// turn's end (or creation time if it never ran). State is fully
    /// reconstructible from SurrealDB, so a re-access just recreates it.
    pub(super) fn is_evictable(&self, idle_cap: Duration) -> bool {
        let g = self.lock();
        if g.running {
            return false;
        }
        let idle_ref = g.finished_at.unwrap_or(g.created_at);
        idle_ref.elapsed() >= idle_cap && self.subscriber_count() == 0
    }
}

/// How often the GC sweep runs.
const GC_SWEEP_INTERVAL: Duration = Duration::from_secs(10);
/// Grace window after a turn finishes before its frames are trimmed.
/// Purely about clean delivery to still-draining readers (the data is
/// already durable); a reader lagging past it gets `resync`.
const GRACE_WINDOW: Duration = Duration::from_secs(30);
/// Idle duration after which a subscriber-less session is evicted.
const EVICT_IDLE: Duration = Duration::from_secs(60);

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
    sessions: DashMap<ConversationId, Arc<Session>>,
}

impl InProcessBus {
    pub fn new() -> Self {
        Self {
            sessions: DashMap::new(),
        }
    }

    /// The (possibly freshly created) session for `conv`. Multiple callers
    /// share the same `Arc` so they read/write one buffer.
    fn get_or_create(&self, conv: &ConversationId) -> Arc<Session> {
        if let Some(s) = self.sessions.get(conv) {
            return s.clone();
        }
        self.sessions
            .entry(conv.clone())
            .or_insert_with(|| Arc::new(Session::new()))
            .clone()
    }
}

impl Default for InProcessBus {
    fn default() -> Self {
        Self::new()
    }
}

impl InProcessBus {
    /// Spawn the background GC sweep (§9): every [`GC_SWEEP_INTERVAL`],
    /// grace-trim finished sessions and evict idle, subscriber-less ones.
    /// Holds an `Arc` to the bus; the task ends when the last `Arc` drops
    /// (process shutdown).
    pub fn spawn_gc(bus: Arc<InProcessBus>) {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(GC_SWEEP_INTERVAL);
            loop {
                tick.tick().await;
                // Trim in place during the scan; collect evict candidates
                // and remove them after (removing inside `iter` would
                // deadlock the shard).
                let mut evict = Vec::new();
                for entry in bus.sessions.iter() {
                    entry.value().maybe_trim(GRACE_WINDOW);
                    if entry.value().is_evictable(EVICT_IDLE) {
                        evict.push(entry.key().clone());
                    }
                }
                for k in evict {
                    // Re-check under the shard write lock: a turn that
                    // started between the scan and here flips `running`,
                    // so `is_evictable` returns false and we keep it.
                    bus.sessions.remove_if(&k, |_, s| s.is_evictable(EVICT_IDLE));
                }
            }
        });
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
        match session.clone().try_begin(user_message) {
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
        // that has never had a turn.
        if let Some(session) = self.sessions.get(conv) {
            session.request_cancel();
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests — ported from v3 `session.rs` (subscribe-replay, single-flight
// reject) plus the new §4.1 `read_from` rules (resync condition, the
// fresh-`None`-while-lingering case).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    fn s(b: &Bytes) -> String {
        String::from_utf8(b.to_vec()).unwrap()
    }

    #[test]
    fn try_begin_rejects_second_concurrent() {
        let sess = Session::new();
        assert!(sess.try_begin(sse::user_message("message:01J", "hi")).is_some());
        assert!(
            sess.try_begin(sse::user_message("message:02J", "again"))
                .is_none(),
            "second begin must be rejected while running"
        );
    }

    #[test]
    fn read_from_none_replays_in_flight_but_not_lingering() {
        let sess = Session::new();
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
    fn read_from_cursor_resumes_then_resyncs_when_stale() {
        let sess = Session::new();
        // Turn 1 occupies cursors 0,1,2 (user, text, finish).
        sess.try_begin(sse::user_message("message:01J", "hi")); // cursor 0
        sess.append(sse::text("a")); // cursor 1
        sess.terminate(sse::finish("stop", "message:a1")); // cursor 2
        // Resume from cursor 0 while turn 1 still lingers → frames 1,2.
        match sess.read_from(Some(Cursor(0))) {
            Read::Frames(f) => assert_eq!(f.len(), 2, "frames after cursor 0"),
            Read::Resync => panic!("valid resume must not resync"),
        }

        // Turn 2 starts → base advances past turn 1's cursors.
        sess.try_begin(sse::user_message("message:02J", "next")); // cursor 3, base=3
        // A client still holding cursor 0 (predates base-1) → resync.
        assert!(
            matches!(sess.read_from(Some(Cursor(0))), Read::Resync),
            "cursor below the window must resync"
        );
        // A client caught up to turn 2's start resumes normally.
        assert!(matches!(sess.read_from(Some(Cursor(3))), Read::Frames(_)));
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

        // Replayed user_message, id-prefixed.
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
        // Turn 1 then turn 2 so the window floor moves past cursor 0.
        {
            let mut h = bus
                .try_start(&conv, sse::user_message("message:u1", "a"))
                .await
                .expect("start1");
            h.terminate(sse::finish("stop", "message:a1")).await;
        }
        let _h2 = bus
            .try_start(&conv, sse::user_message("message:u2", "b"))
            .await
            .expect("start2");

        // A subscriber resuming from the now-trimmed cursor 0 is told to
        // resync, then replays turn 2 fresh.
        let mut stream = bus.subscribe(&conv, Some(Cursor(0))).await;
        let first = stream.next().await.expect("frame");
        assert_eq!(s(&first), "event: resync\ndata: null\n\n");
        let second = stream.next().await.expect("frame");
        assert!(
            s(&second).starts_with("id: "),
            "fresh replay of turn 2 after resync"
        );
        assert!(s(&second).contains("event: user_message"));
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

    #[test]
    fn grace_trim_clears_finished_turn_frames() {
        let sess = Session::new();
        sess.try_begin(sse::user_message("message:u1", "hi")); // 0
        sess.append(sse::text("a")); // 1
        sess.terminate(sse::finish("stop", "message:a1")); // 2
        // Before the grace window elapses: frames stay, a resume from
        // cursor 0 still replays.
        sess.maybe_trim(Duration::from_secs(3600));
        assert!(matches!(sess.read_from(Some(Cursor(0))), Read::Frames(_)));
        // Grace elapsed (zero window): frames trimmed, base advanced, so a
        // stale resume now resyncs.
        sess.maybe_trim(Duration::ZERO);
        assert!(matches!(sess.read_from(Some(Cursor(0))), Read::Resync));
    }

    #[test]
    fn evictable_only_when_idle_and_unsubscribed() {
        let sess = Session::new();
        // Idle, never ran, no subscribers: evictable past a zero idle cap.
        assert!(sess.is_evictable(Duration::ZERO));
        // Not evictable while a turn is running.
        sess.try_begin(sse::user_message("message:u1", "hi"));
        assert!(!sess.is_evictable(Duration::ZERO));
        // Idle again after terminate → evictable past a zero cap, but a
        // long idle cap keeps it (hasn't been idle long enough).
        sess.terminate(sse::finish("stop", "message:a1"));
        assert!(sess.is_evictable(Duration::ZERO));
        assert!(!sess.is_evictable(Duration::from_secs(3600)));
    }

    #[tokio::test]
    async fn gc_sweep_evicts_idle_unsubscribed_session() {
        // Exercise the sweep body (same as `spawn_gc`) with zero
        // thresholds so it runs without the real 30s/60s waits.
        let bus = InProcessBus::new();
        let conv: ConversationId = surrealdb::RecordId::from(("conversation", "gc"));
        {
            // Start then drop the handle without terminate → idle (clear).
            let _h = bus
                .try_start(&conv, sse::user_message("message:u1", "hi"))
                .await
                .expect("start");
        }
        assert_eq!(bus.sessions.len(), 1, "session created");
        // Manual sweep with zero thresholds.
        let mut evict = Vec::new();
        for entry in bus.sessions.iter() {
            entry.value().maybe_trim(Duration::ZERO);
            if entry.value().is_evictable(Duration::ZERO) {
                evict.push(entry.key().clone());
            }
        }
        for k in &evict {
            bus.sessions.remove_if(k, |_, s| s.is_evictable(Duration::ZERO));
        }
        assert!(
            bus.sessions.is_empty(),
            "idle, unsubscribed session must be evicted"
        );
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
        // Read the user_message while the turn is in flight (a fresh
        // `None` subscriber only replays a *live* turn — §4.1).
        let first = stream.next().await.expect("user_message");
        assert!(s(&first).contains("event: user_message"));
        // Drop the handle without `terminate` (worker panic/unwind):
        // `clear` is emitted and the slot released.
        drop(handle);
        let second = stream.next().await.expect("clear");
        assert!(s(&second).ends_with("event: clear\ndata: null\n\n"));
    }
}
