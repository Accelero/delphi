//! Per-connection reader over a [`SessionState`] buffer.
//!
//! Holds an `Arc<SessionState>` (which keeps the session alive while
//! any reader is attached) and a `u64` absolute byte cursor.
//!
//! ## `AsyncRead` semantics
//!
//! `poll_read` follows the "notify-before-drain" pattern so we never
//! lose a wake-up:
//!
//! 1. Register `state.notify.notified()` *first*. `Notify` holds one
//!    permit; any concurrent `notify_waiters` between registration and
//!    the eventual `.poll()` is captured by that permit, so the await
//!    completes immediately rather than parking forever.
//! 2. Take `buf.read()`, slice `buf[(cursor - base_offset)..]`, copy
//!    out, drop lock, return Ready(n) if non-empty.
//! 3. If empty: poll the registered `notified` once. If Ready (the
//!    writer raced us), loop. Else return Pending with the waker
//!    parked in `Notify`.
//!
//! The reader **never** returns `Ok(0)`. The session has no EOF; the
//! response stream ends only when the client disconnects (the axum
//! body stream drops, which drops this reader, which decrements the
//! `Arc`).
//!
//! ## Cursor arithmetic across `buf.clear()`
//!
//! Readers hold `cursor: u64` in the *absolute* byte space. The worker
//! advances `state.base_offset` by `old_len` when it clears the buffer
//! between turns. After that, `(cursor - base_offset)` overflows or
//! exceeds `buf.len()`; we treat both as "caught up" and park. When
//! the next turn appends, the reader resumes — they've effectively
//! skipped the truncated bytes (which are now in the DB anyway).

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::futures::Notified;

use super::state::SessionState;

/// A single SSE subscription. Implements [`tokio::io::AsyncRead`] so
/// axum can wrap it directly in a `ReaderStream` for the response body.
/// Drop releases its `Arc<SessionState>`; the session is reaped when
/// the last `Arc` (worker or any reader) is gone.
pub struct SessionReader {
    state: Arc<SessionState>,
    cursor: u64,
    /// In-flight `Notified` future kept across `poll_read` calls so we
    /// don't lose the wake-up registration when the poll returns
    /// Pending. Pinned in-place via a `Box` because `Notified<'a>`
    /// borrows from `state` and we need a stable address.
    ///
    /// Stored as `Option<Pin<Box<Notified<'static>>>>` after we transmute
    /// the lifetime to `'static` — sound because the borrow target
    /// (`state.notify`) lives as long as `self.state`, which is the
    /// same lifetime as the future.
    waiter: Option<Pin<Box<Notified<'static>>>>,
}

impl SessionReader {
    pub(super) fn new(state: Arc<SessionState>, cursor: u64) -> Self {
        Self {
            state,
            cursor,
            waiter: None,
        }
    }

    /// Current absolute byte position (next byte the reader will read).
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    #[allow(dead_code)] // wired by the stream handler in step 7
    pub(crate) fn state(&self) -> &SessionState {
        &self.state
    }

    /// Register a fresh `notified()` future on `state.notify`, replacing
    /// any prior one. Called *before* we inspect the buffer — that's
    /// the half of the notify-before-drain pattern that guarantees we
    /// don't miss a wake.
    fn arm_waiter(&mut self) {
        // SAFETY: the `Notified<'_>` future borrows `state.notify` for
        // its lifetime. We extend the borrow to `'static` because we
        // hold an `Arc<SessionState>` (`self.state`) that we will not
        // drop while the future is alive. The future is stored inside
        // `self` alongside the `Arc`; both drop together.
        let notify_ref: &'static tokio::sync::Notify =
            unsafe { std::mem::transmute(&self.state.notify) };
        self.waiter = Some(Box::pin(notify_ref.notified()));
    }
}

impl AsyncRead for SessionReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // SAFETY: `SessionReader` is `Unpin` w.r.t. its public fields;
        // we project through &mut Self for the internal state shuffle.
        let me = self.get_mut();

        loop {
            // Step 1: arm the wake-up *before* we read the buffer. If the
            // writer appends + notifies between now and our poll, the
            // permit is captured by this future.
            me.arm_waiter();

            // Step 2: drain whatever's available.
            //
            // `try_read` rather than blocking — `RwLock::read()` is async
            // but doesn't have a non-async fast path; we use try_read here
            // because under normal load contention is zero (one writer,
            // many readers, mostly non-overlapping). On contention we
            // fall back to scheduling another poll.
            let base = me.state.base_offset.load(Ordering::Acquire);
            let n_copied = {
                let guard = match me.state.buf.try_read() {
                    Ok(g) => g,
                    Err(_) => {
                        // Lock briefly held by the writer. Re-poll on
                        // next wake; the notify will fire when the
                        // writer commits its append.
                        // Poll the waiter once to register the waker.
                        let w = me.waiter.as_mut().expect("waiter armed above");
                        let _ = w.as_mut().poll(cx);
                        return Poll::Pending;
                    }
                };
                let i = me.cursor.saturating_sub(base) as usize;
                if i >= guard.len() {
                    0
                } else {
                    let remaining_capacity = buf.remaining();
                    let available = guard.len() - i;
                    let n = remaining_capacity.min(available);
                    buf.put_slice(&guard[i..i + n]);
                    n
                }
            };

            if n_copied > 0 {
                me.cursor += n_copied as u64;
                // Drop the waiter — we made progress, the next poll
                // will re-arm. This avoids hoarding a notify permit
                // beyond the read we used it for.
                me.waiter = None;
                return Poll::Ready(Ok(()));
            }

            // Step 3: nothing available. Poll the (already-armed) waiter.
            let w = me.waiter.as_mut().expect("waiter armed above");
            match w.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    // Writer appended after we armed but before we drained.
                    // Loop to drain.
                    me.waiter = None;
                    continue;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chat::SessionRegistry;
    use surrealdb::RecordId;
    use tokio::io::AsyncReadExt;
    use tokio::time::{timeout, Duration};

    fn cid(k: &str) -> crate::storage::ConversationId {
        RecordId::from(("conversation", k))
    }

    /// Helper to append bytes + notify under the same lock the worker would.
    async fn writer_append(state: &SessionState, bytes: &[u8]) {
        let mut g = state.buf.write().await;
        g.extend_from_slice(bytes);
        drop(g);
        state.notify.notify_waiters();
    }

    #[tokio::test]
    async fn reads_existing_buffer_immediately() {
        let reg = SessionRegistry::new();
        let s = reg.get_or_create(&cid("a")).await;
        writer_append(&s, b"hello world").await;

        let mut r = s.subscribe();
        let mut out = [0u8; 16];
        let n = r.read(&mut out).await.expect("read");
        assert_eq!(n, b"hello world".len());
        assert_eq!(&out[..n], b"hello world");
    }

    #[tokio::test]
    async fn blocks_when_caught_up_then_resumes_on_append() {
        let reg = SessionRegistry::new();
        let s = reg.get_or_create(&cid("b")).await;
        let mut r = s.subscribe();

        // No data yet — the read must Pending. Use a short timeout.
        let mut out = [0u8; 8];
        let result = timeout(Duration::from_millis(50), r.read(&mut out)).await;
        assert!(result.is_err(), "read should park when buffer is empty");

        // Append from another task — the parked read wakes.
        let s2 = s.clone();
        let join = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            writer_append(&s2, b"abc").await;
        });
        let n = timeout(Duration::from_millis(500), r.read(&mut out))
            .await
            .expect("woke up")
            .expect("read ok");
        assert_eq!(&out[..n], b"abc");
        join.await.unwrap();
    }

    #[tokio::test]
    async fn no_lost_bytes_under_burst_writes() {
        // Writer task fires off many small appends in a tight loop.
        // Reader concurrently drains. We assert the reader saw every
        // byte exactly once, in order.
        let reg = SessionRegistry::new();
        let s = reg.get_or_create(&cid("c")).await;
        let mut r = s.subscribe();

        let s_writer = s.clone();
        let writer = tokio::spawn(async move {
            for i in 0u8..200 {
                writer_append(&s_writer, &[i]).await;
                // No sleep — maximise overlap with the reader.
            }
        });

        let reader = tokio::spawn(async move {
            let mut received = Vec::with_capacity(200);
            let mut tmp = [0u8; 32];
            while received.len() < 200 {
                let n = timeout(Duration::from_secs(2), r.read(&mut tmp))
                    .await
                    .expect("reader didn't park forever")
                    .expect("read ok");
                received.extend_from_slice(&tmp[..n]);
            }
            received
        });

        writer.await.unwrap();
        let got = reader.await.unwrap();
        let expected: Vec<u8> = (0u8..200).collect();
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn no_spurious_wake_after_drain() {
        // After draining all bytes, an immediate second read must park
        // (otherwise we'd be spinning the executor).
        let reg = SessionRegistry::new();
        let s = reg.get_or_create(&cid("d")).await;
        writer_append(&s, b"x").await;

        let mut r = s.subscribe();
        let mut out = [0u8; 8];
        let n = r.read(&mut out).await.unwrap();
        assert_eq!(n, 1);

        let result = timeout(Duration::from_millis(50), r.read(&mut out)).await;
        assert!(result.is_err(), "second read should park, not spin");
    }
}
