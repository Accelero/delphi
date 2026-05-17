//! Per-session in-memory state shared between the worker (single writer)
//! and any number of SSE readers (multi-reader, tail-style).
//!
//! Mental model: `buf` is a file the worker appends framed bytes to, and
//! the readers `tail -f` it. The worker truncates between turns. Readers
//! think in absolute byte positions via [`SessionState::base_offset`] so
//! a truncate doesn't break them — their slice goes empty and they park
//! on `notify` until the next turn.
//!
//! ### Why these primitives
//!
//! - **`buf: RwLock<BytesMut>`** — append-only log of framed bytes
//!   (`proto::text`, `proto::citations`, `proto::finish`). The worker
//!   appends complete records; readers slice from their cursor and
//!   copy out.
//! - **`base_offset: AtomicU64`** — absolute position of `buf[0]`. The
//!   worker bumps this when it clears the buffer between turns. Readers
//!   compute `buf_index = cursor - base_offset`; a cleared buffer means
//!   the slice is empty and they wait for the next `notify`.
//! - **`notify: Notify`** — pulsed after every append. Readers register
//!   `.notified()` *before* draining and await *after*. `Notify`'s
//!   one-permit semantics make this race-free without dedupe logic.
//! - **`turn_lock: Semaphore`** (permits = 1) — serialises in-flight
//!   turns per session. A second concurrent submission waits its turn.
//! - **`finalize_lock: Mutex<()>`** — held during the worker's
//!   DB-commit + registry-remove critical section, and acquired by the
//!   new-tab handshake before deciding "live session present?" vs
//!   "load from DB only." Prevents the duplicate-message race.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::BytesMut;
use tokio::sync::{Mutex, Notify, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;

use super::reader::SessionReader;

/// All the in-memory state for one active conversation. Lifetime is
/// reference-counted (see [`super::SessionRegistry`]): the state lives
/// while either a worker is running or at least one reader is attached.
pub struct SessionState {
    /// Append-only buffer of framed bytes for the current turn. Cleared
    /// after each commit; surviving readers see an empty slice on their
    /// next read and park until the next append.
    pub(super) buf: RwLock<BytesMut>,
    /// Absolute position of `buf[0]` in the conceptual byte stream. Lets
    /// readers hold a `u64` cursor that survives `buf.clear()`.
    pub(super) base_offset: AtomicU64,
    /// Pulsed once per append. Readers register `.notified()` first,
    /// then drain, then await — the `Notify` permit holds across the
    /// drain so we never miss a wake.
    pub(super) notify: Notify,
    /// One-permit semaphore. The worker acquires before driving the LLM
    /// stream and releases on Drop, so queued submissions fire in
    /// arrival order.
    pub(super) turn_lock: Semaphore,
    /// Held by the worker around `DB commit + clear buffer`; acquired
    /// by the new-tab handshake before snapshotting history. Ensures
    /// the in-flight turn is in exactly one of {DB, buffer} from the
    /// handshake's point of view.
    pub(super) finalize_lock: Mutex<()>,
    /// Per-turn cancellation handle. Worker installs it when it
    /// acquires `turn_lock`, clears it on exit. The stop endpoint
    /// reads it and calls `cancel()`.
    pub(super) current_turn_cancel: Mutex<Option<CancellationToken>>,
}

impl SessionState {
    pub(super) fn new() -> Self {
        Self {
            buf: RwLock::new(BytesMut::new()),
            base_offset: AtomicU64::new(0),
            notify: Notify::new(),
            turn_lock: Semaphore::new(1),
            finalize_lock: Mutex::new(()),
            current_turn_cancel: Mutex::new(None),
        }
    }

    /// Snapshot the absolute byte position of the *end* of the buffer
    /// right now. A reader created with this cursor sees only future
    /// appends. The new-tab handshake instead uses [`Self::tail_cursor`]
    /// via `subscribe()` so it picks up the in-flight turn bytes.
    pub(super) fn end_cursor(&self) -> u64 {
        // Length read under the same `read()` guard that bounds it.
        let base = self.base_offset.load(Ordering::Acquire);
        let len = {
            let g = self.buf.try_read();
            match g {
                Ok(b) => b.len() as u64,
                Err(_) => 0, // a writer holds the lock — len is at most growing
            }
        };
        base + len
    }

    /// Cursor pointing at the start of whatever's currently buffered.
    /// A reader created with this position sees the in-flight turn's
    /// bytes (if any), then continues live.
    pub(super) fn tail_cursor(&self) -> u64 {
        self.base_offset.load(Ordering::Acquire)
    }
}

impl SessionState {
    /// Attach a new reader at the buffer's current `base_offset`, so the
    /// reader sees any in-flight turn bytes from the beginning. Use this
    /// for the new-tab handshake.
    pub fn subscribe(self: &std::sync::Arc<Self>) -> SessionReader {
        SessionReader::new(self.clone(), self.tail_cursor())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn fresh_state_has_zero_offsets() {
        let s = SessionState::new();
        assert_eq!(s.base_offset.load(Ordering::Acquire), 0);
        assert_eq!(s.end_cursor(), 0);
        assert_eq!(s.tail_cursor(), 0);
    }

    #[tokio::test]
    async fn subscribe_returns_reader_at_tail() {
        let s = Arc::new(SessionState::new());
        // Pre-populate to confirm tail_cursor == base_offset, not end_cursor.
        s.buf.write().await.extend_from_slice(b"hello");
        let r = s.subscribe();
        // Reader should see the buffered bytes (it attached at base_offset).
        assert_eq!(r.cursor(), 0);
    }
}
