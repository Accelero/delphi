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
    #[cfg(test)]
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

    /// Append framed bytes (one or more whole `proto::*` records) and
    /// wake any parked readers. The worker is the only caller.
    pub(crate) async fn append(&self, bytes: &[u8]) {
        let mut g = self.buf.write().await;
        g.extend_from_slice(bytes);
        drop(g);
        self.notify.notify_waiters();
    }

    /// Acquire the finalize lock. The worker holds this around its
    /// DB-commit + [`Self::clear_after_commit`] critical section; the
    /// new-tab handshake acquires it before snapshotting history so it
    /// can't observe a "neither in DB nor in buffer" state.
    pub async fn lock_finalize(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.finalize_lock.lock().await
    }

    /// Truncate the buffer at the end of a committed turn. Bumps
    /// `base_offset` by the cleared length so reader cursors stay
    /// consistent across the boundary — a reader whose cursor is past
    /// the new `base_offset` simply sees "caught up" on its next read
    /// and parks.
    ///
    /// Caller must hold the [`finalize_lock`] across DB-commit + this
    /// clear so the handshake sees a coherent view.
    ///
    /// [`finalize_lock`]: SessionState::finalize_lock
    pub(crate) async fn clear_after_commit(&self) {
        let mut g = self.buf.write().await;
        let old_len = g.len() as u64;
        g.clear();
        // Order matters: bump the offset *after* truncating so an
        // observer can't see `base_offset > buf-end`.
        self.base_offset.fetch_add(old_len, Ordering::AcqRel);
        // Wake everyone so cursors past the new base get to re-check
        // (they'll see "caught up" and re-park, which is the intended
        // behaviour — they're now waiting for the next turn).
        self.notify.notify_waiters();
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

    #[tokio::test]
    async fn clear_advances_base_offset_by_old_len() {
        let s = Arc::new(SessionState::new());
        s.append(b"first turn payload").await;
        assert_eq!(s.base_offset.load(Ordering::Acquire), 0);
        assert_eq!(s.end_cursor(), b"first turn payload".len() as u64);

        s.clear_after_commit().await;
        assert_eq!(
            s.base_offset.load(Ordering::Acquire),
            b"first turn payload".len() as u64
        );
        assert_eq!(s.buf.read().await.len(), 0);
    }

    #[tokio::test]
    async fn reader_past_end_sees_empty_after_clear_then_next_turn() {
        use tokio::io::AsyncReadExt;
        use tokio::time::{timeout, Duration};

        let s = Arc::new(SessionState::new());
        s.append(b"turnA").await;

        let mut r = s.subscribe();
        let mut out = [0u8; 32];
        let n = r.read(&mut out).await.unwrap();
        assert_eq!(&out[..n], b"turnA");

        // Worker commits: clear + bump base_offset.
        s.clear_after_commit().await;

        // Reader is now "past the new base". Next read parks until the
        // worker appends turn B.
        let parked = timeout(Duration::from_millis(50), r.read(&mut out)).await;
        assert!(parked.is_err(), "reader should park after clear");

        // Append turn B, reader wakes and resumes from its absolute cursor.
        s.append(b"turnB-bytes").await;
        let n = timeout(Duration::from_millis(500), r.read(&mut out))
            .await
            .expect("woke")
            .expect("read ok");
        assert_eq!(&out[..n], b"turnB-bytes");
    }

    #[tokio::test]
    async fn finalize_lock_serialises_commit_and_handshake() {
        use tokio::time::{sleep, timeout, Duration};

        let s = Arc::new(SessionState::new());

        // "Worker" task takes the finalize lock and holds it briefly.
        let s_worker = s.clone();
        let worker = tokio::spawn(async move {
            let _g = s_worker.lock_finalize().await;
            sleep(Duration::from_millis(80)).await;
            // Commit + clear under the lock.
            s_worker.clear_after_commit().await;
        });

        // Give the worker a head start to acquire the lock.
        sleep(Duration::from_millis(10)).await;

        // "Handshake" must block on the lock until the worker releases.
        let s_hs = s.clone();
        let handshake = tokio::spawn(async move {
            let _g = s_hs.lock_finalize().await;
            // Returns the moment the lock is free.
            std::time::Instant::now()
        });

        let start = std::time::Instant::now();
        let when_unblocked = timeout(Duration::from_secs(1), handshake)
            .await
            .expect("handshake didn't deadlock")
            .expect("join");
        worker.await.unwrap();

        // The handshake should have been blocked for at least ~50ms.
        // We don't check the exact 80ms to leave slack for CI jitter,
        // but anything < 40ms means the locks weren't serialising.
        let blocked_for = when_unblocked - start;
        assert!(
            blocked_for >= Duration::from_millis(40),
            "handshake unblocked too quickly: {blocked_for:?}"
        );
    }

    #[tokio::test]
    async fn fresh_reader_after_clear_sees_only_new_turn() {
        use tokio::io::AsyncReadExt;

        let s = Arc::new(SessionState::new());
        s.append(b"turnA").await;
        s.clear_after_commit().await;
        s.append(b"turnB").await;

        // A reader created AFTER the clear should see only turn B
        // (its cursor starts at the new base_offset, which is past A).
        let mut r = s.subscribe();
        let mut out = [0u8; 32];
        let n = r.read(&mut out).await.unwrap();
        assert_eq!(&out[..n], b"turnB");
    }
}
