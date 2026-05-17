//! Per-connection reader over a [`SessionState`] buffer.
//!
//! Holds an `Arc<SessionState>` (which keeps the session alive while
//! any reader is attached) and a `u64` absolute byte cursor. The actual
//! [`AsyncRead`] impl arrives in step 2 of the chat-streaming rollout
//! — for now we just expose the type so the registry / state APIs can
//! compile.
//!
//! [`AsyncRead`]: tokio::io::AsyncRead

use std::sync::Arc;

use super::state::SessionState;

/// A single SSE subscription. Implements [`tokio::io::AsyncRead`] so
/// axum can wrap it directly in a `ReaderStream` for the response body.
/// Drop releases its `Arc<SessionState>`; the session is reaped when
/// the last `Arc` (worker or any reader) is gone.
pub struct SessionReader {
    state: Arc<SessionState>,
    cursor: u64,
}

impl SessionReader {
    pub(super) fn new(state: Arc<SessionState>, cursor: u64) -> Self {
        Self { state, cursor }
    }

    /// Current absolute byte position (next byte the reader will read).
    /// Useful for tests and tracing; not used by the read path.
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Borrow the underlying session — kept `pub(crate)` so the
    /// AsyncRead impl in the next step can access the buffer / notify.
    #[allow(dead_code)] // wired in step 2
    pub(crate) fn state(&self) -> &SessionState {
        &self.state
    }
}
