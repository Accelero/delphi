//! Per-conversation chat session state.
//!
//! One [`SessionState`] per ever-touched `ConversationId` lives in the
//! process-global [`super::registry::SessionRegistry`]. It serialises
//! everything about an in-flight turn:
//!
//! - the worker's [`CancellationToken`]
//! - the buffered frames (SSE-formatted bytes) for replay on subscribe
//! - the live subscriber list (one `mpsc::Sender<Bytes>` per open tab)
//! - the [`TurnPhase`] that closes the commit↔abort race
//!
//! Concurrency model: a single `std::sync::Mutex` guards `Inner`. We
//! never `.await` while holding it; every public method locks, mutates,
//! unlocks. The mutex is contended only at frame cadence (chat-rate
//! tokens + subscribe events), which is cheap.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::registry::TaskId;

/// Buffer size for each subscriber's mpsc. Sized generously for
/// chat-rate streaming so worker `emit` never trips `Full` on a sane
/// network path; if a subscriber is so slow we fill 4096 frames the
/// drop-and-reconnect path is the right answer.
const SUBSCRIBER_CHANNEL_CAPACITY: usize = 4096;

/// Returned from [`SessionState::start_turn`] when a turn is already in
/// flight for this conversation. The POST handler translates this into
/// a `409 Conflict` response.
#[derive(Debug)]
pub struct AlreadyRunning;

/// Live state of the current turn. `None` when the conversation is idle.
pub struct InFlightTurn {
    pub task_id: TaskId,
    pub cancel: CancellationToken,
    /// SSE-formatted frames emitted so far, replayable verbatim to any
    /// late-joining subscriber.
    pub frames: Vec<Bytes>,
    pub phase: TurnPhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnPhase {
    /// Worker is still pulling LLM deltas. `abort()` cancels the token
    /// and emits a `clear` frame.
    Streaming,
    /// Worker passed the LLM loop and is inside `commit_turn`. `abort()`
    /// is a no-op for clear/current — the worker will see the commit
    /// through and emit its own `finish`.
    Committing,
    /// `commit_turn` returned ok; `finish` already emitted (or about to
    /// be). Transient — `finish()` immediately clears `current`.
    Committed,
}

struct Inner {
    current: Option<InFlightTurn>,
    subscribers: Vec<mpsc::Sender<Bytes>>,
}

/// Per-conversation chat session. Cheap to clone (`Arc` internals).
pub struct SessionState {
    inner: Mutex<Inner>,
}

impl Default for SessionState {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionState {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                current: None,
                subscribers: Vec::new(),
            }),
        }
    }

    /// Open a new subscriber channel. The current turn's buffered frames
    /// (if any) are pushed into the channel **under lock** before the
    /// subscriber is registered, so replay cannot interleave with live
    /// frames — both append in order under the same mutex.
    pub fn subscribe(&self) -> mpsc::Receiver<Bytes> {
        let (tx, rx) = mpsc::channel(SUBSCRIBER_CHANNEL_CAPACITY);
        let mut g = self.lock();
        if let Some(turn) = g.current.as_ref() {
            for f in &turn.frames {
                // `tx` is fresh and has capacity == channel cap; the
                // only way this fails is if the receiver has been
                // dropped before we even returned it (cannot happen
                // synchronously).
                let _ = tx.try_send(f.clone());
            }
        }
        g.subscribers.push(tx);
        rx
    }

    /// Start a new turn. Returns `Err(AlreadyRunning)` if one is in
    /// flight. The `user_message_frame` is pushed into `current.frames`
    /// **before** fan-out to subscribers, so a `subscribe()` call that
    /// raced into the same lock either:
    ///
    ///   - sees `current == None` and gets no replay (we haven't taken
    ///     the lock yet), or
    ///   - sees `current == Some(...)` with the frame already buffered.
    ///
    /// It can never see a live frame before the buffered copy.
    pub fn start_turn(
        &self,
        task_id: TaskId,
        cancel: CancellationToken,
        user_message_frame: Bytes,
    ) -> Result<(), AlreadyRunning> {
        let mut g = self.lock();
        if g.current.is_some() {
            return Err(AlreadyRunning);
        }
        let turn = InFlightTurn {
            task_id,
            cancel,
            frames: vec![user_message_frame.clone()],
            phase: TurnPhase::Streaming,
        };
        g.current = Some(turn);
        fanout(&mut g, user_message_frame);
        Ok(())
    }

    /// Append a frame to the buffer and fan it out to every subscriber.
    /// If there's no current turn (worker raced an abort), the frame is
    /// silently dropped.
    pub fn emit(&self, frame: Bytes) {
        let mut g = self.lock();
        if let Some(turn) = g.current.as_mut() {
            turn.frames.push(frame.clone());
        } else {
            return;
        }
        fanout(&mut g, frame);
    }

    /// Flip the current turn from `Streaming` to `Committing`. Returns
    /// `true` on success, `false` if the turn has already been aborted
    /// (in which case the worker should bail without writing to the DB).
    pub fn enter_committing(&self) -> bool {
        let mut g = self.lock();
        match g.current.as_mut() {
            Some(turn) if turn.phase == TurnPhase::Streaming => {
                turn.phase = TurnPhase::Committing;
                true
            }
            _ => false,
        }
    }

    /// Emit the `finish` frame, mark the phase `Committed`, and clear
    /// `current`. Idempotent: if there's no current turn (raced an
    /// abort), this is a no-op.
    pub fn finish(&self, finish_frame: Bytes) {
        let mut g = self.lock();
        if let Some(turn) = g.current.as_mut() {
            turn.frames.push(finish_frame.clone());
            turn.phase = TurnPhase::Committed;
        } else {
            return;
        }
        fanout(&mut g, finish_frame);
        g.current = None;
    }

    /// Abort the in-flight turn (if any). Behaviour depends on phase:
    ///
    /// - `Streaming`: cancel the worker, emit `clear`, clear `current`.
    /// - `Committing`/`Committed`: cancel the token (harmless if already
    ///   past) but **don't** emit `clear` and **don't** touch `current`
    ///   — the worker is past the point of no return and will emit
    ///   `finish` on its own.
    ///
    /// This is what closes the commit↔stop race called out in the v3
    /// plan: a stop arriving in the commit window must never produce a
    /// `clear`-emitted-but-rows-in-DB inconsistency.
    pub fn abort(&self) {
        let clear_frame = {
            let mut g = self.lock();
            let phase = match g.current.as_ref() {
                Some(turn) => turn.phase,
                None => return,
            };
            // Always cancel the token if it's still live — for Streaming
            // we need it to unblock the LLM loop; for Committing it's a
            // no-op but harmless.
            if let Some(turn) = g.current.as_ref() {
                turn.cancel.cancel();
            }
            if phase != TurnPhase::Streaming {
                return;
            }
            // Streaming abort: emit clear and clear current.
            let frame = crate::api::sse::clear();
            if let Some(turn) = g.current.as_mut() {
                turn.frames.push(frame.clone());
            }
            fanout(&mut g, frame.clone());
            g.current = None;
            frame
        };
        // Frame already fanned out above; this binding is here so the
        // borrow checker can see the path closing cleanly.
        let _ = clear_frame;
    }

    /// Test helper: snapshot the current phase. Public for the unit
    /// tests in this file plus `chat_commit_abort_race.rs`.
    #[cfg(test)]
    pub(crate) fn phase(&self) -> Option<TurnPhase> {
        self.lock().current.as_ref().map(|t| t.phase)
    }

    /// Test helper: subscriber count. Useful for prune-dead-subscribers
    /// assertions.
    #[cfg(test)]
    pub(crate) fn subscriber_count(&self) -> usize {
        self.lock().subscribers.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // Poisoning would only happen if a previous panic was holding
        // the lock; the worker's `WorkerGuard` calls `abort()` on
        // unwind which retakes this same lock, so a poison would
        // cascade into 500s. Treat as fatal — there's no recovery story.
        self.inner.lock().expect("SessionState mutex poisoned")
    }
}

/// Fan out a frame to every registered subscriber. Drops any subscriber
/// whose channel is `Closed` (tab gone) or `Full` (slow client — they
/// will reconnect via EventSource and get a fresh replay).
fn fanout(g: &mut std::sync::MutexGuard<'_, Inner>, frame: Bytes) {
    g.subscribers.retain(|tx| tx.try_send(frame.clone()).is_ok());
}

// ---------------------------------------------------------------------------
// Unit tests — exercise the invariants the v3 plan calls out:
//   1. subscribe-then-emit ordering (replay sees frames in same order as live)
//   2. reject-second-start (AlreadyRunning)
//   3. replay-on-subscribe
//   4. prune-dead-subscribers
//   5. phase guard (abort during Committing is a no-op for clear/current)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::sse;
    use std::sync::Arc;

    fn frame(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[tokio::test]
    async fn subscribe_then_emit_orders_frames() {
        let s = Arc::new(SessionState::new());
        let cancel = CancellationToken::new();
        s.start_turn(TaskId::new(), cancel, sse::user_message("message:01J", "hi"))
            .expect("start");
        let mut rx = s.subscribe();
        s.emit(sse::text("hello"));
        s.emit(sse::text(" world"));

        // Subscriber sees the buffered user_message first, then the live
        // text deltas in order.
        let mut received: Vec<Bytes> = Vec::new();
        for _ in 0..3 {
            received.push(rx.recv().await.expect("frame"));
        }
        let strs: Vec<String> = received
            .iter()
            .map(|b| String::from_utf8(b.to_vec()).unwrap())
            .collect();
        assert!(strs[0].starts_with("event: user_message"));
        assert!(strs[1].starts_with("event: text"));
        assert!(strs[1].contains("\"hello\""));
        assert!(strs[2].starts_with("event: text"));
        assert!(strs[2].contains("\" world\""));
    }

    #[tokio::test]
    async fn start_turn_rejects_second_concurrent() {
        let s = SessionState::new();
        s.start_turn(
            TaskId::new(),
            CancellationToken::new(),
            sse::user_message("message:01J", "hi"),
        )
        .expect("first");
        let err = s.start_turn(
            TaskId::new(),
            CancellationToken::new(),
            sse::user_message("message:02J", "again"),
        );
        assert!(err.is_err(), "second start must be AlreadyRunning");
    }

    #[tokio::test]
    async fn subscribe_replays_existing_frames() {
        let s = SessionState::new();
        s.start_turn(
            TaskId::new(),
            CancellationToken::new(),
            sse::user_message("message:01J", "hi"),
        )
        .expect("start");
        s.emit(sse::text("part1"));
        s.emit(sse::text("part2"));

        let mut rx = s.subscribe();
        // All three frames are already in the channel — drain without
        // waiting on the worker.
        let mut got = Vec::new();
        for _ in 0..3 {
            got.push(rx.try_recv().expect("buffered"));
        }
        assert_eq!(got.len(), 3);
    }

    #[tokio::test]
    async fn emit_prunes_dead_subscribers() {
        let s = SessionState::new();
        s.start_turn(
            TaskId::new(),
            CancellationToken::new(),
            sse::user_message("message:01J", "hi"),
        )
        .expect("start");
        let rx = s.subscribe();
        assert_eq!(s.subscriber_count(), 1);
        drop(rx); // simulate the tab closing
        s.emit(sse::text("after-drop"));
        assert_eq!(
            s.subscriber_count(),
            0,
            "closed subscriber should be pruned on next emit"
        );
    }

    #[tokio::test]
    async fn abort_during_committing_does_not_emit_clear() {
        let s = SessionState::new();
        s.start_turn(
            TaskId::new(),
            CancellationToken::new(),
            sse::user_message("message:01J", "hi"),
        )
        .expect("start");
        let mut rx = s.subscribe();
        // Drain the buffered user_message frame.
        let _ = rx.recv().await.expect("user_message");

        assert!(s.enter_committing(), "phase flip ok");
        assert_eq!(s.phase(), Some(TurnPhase::Committing));

        // Abort during Committing: no clear frame emitted, current
        // untouched, the worker will commit and call finish().
        s.abort();
        assert_eq!(
            s.phase(),
            Some(TurnPhase::Committing),
            "abort must not clear current during commit"
        );
        // No new frame should be in the channel.
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        // Now the worker finishes — emits finish, clears current.
        s.finish(sse::finish("stop", "message:abc"));
        let f = rx.recv().await.expect("finish frame");
        assert!(String::from_utf8(f.to_vec()).unwrap().starts_with("event: finish"));
        assert!(s.phase().is_none(), "current cleared after finish");
    }

    #[tokio::test]
    async fn abort_during_streaming_emits_clear_and_clears_current() {
        let s = SessionState::new();
        let cancel = CancellationToken::new();
        s.start_turn(
            TaskId::new(),
            cancel.clone(),
            sse::user_message("message:01J", "hi"),
        )
        .expect("start");
        let mut rx = s.subscribe();
        let _ = rx.recv().await.expect("user_message");

        s.abort();
        let f = rx.recv().await.expect("clear frame");
        assert_eq!(
            String::from_utf8(f.to_vec()).unwrap(),
            "event: clear\ndata: null\n\n"
        );
        assert!(s.phase().is_none(), "current cleared after streaming abort");
        assert!(cancel.is_cancelled(), "worker token was cancelled");
    }

    #[tokio::test]
    async fn emit_with_no_current_is_dropped() {
        let s = SessionState::new();
        // No start_turn — emit is a no-op (no panic, no allocation
        // observable to a subscriber).
        let mut rx = s.subscribe();
        s.emit(sse::text("ghost"));
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}
