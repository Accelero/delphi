//! `TurnBus` — the transport seam for chat turns (v4).
//!
//! A `TurnBus` bundles the three things a per-conversation turn needs
//! that share one backing store:
//!
//!  1. **Single-flight** — `try_start` atomically claims the turn slot and
//!     appends the first (`user_message`) frame, or rejects with
//!     [`AlreadyRunning`] (→ HTTP 409).
//!  2. **An ordered log** — `subscribe` hands back a `Stream` of
//!     already-`id:`-prefixed SSE frames: replay from a cursor, then live.
//!  3. **Cancel delivery** — `cancel` flips the in-flight turn's token.
//!
//! It replaces v3's `SessionRegistry` + `SessionState`. Phase 1 ships one
//! implementation, [`super::inprocess::InProcessBus`] (single instance,
//! in-memory). Phase 2 adds a Redis-backed impl behind this same trait;
//! the wire contract (opaque [`Cursor`] on every frame + `resync`) is what
//! makes that swap invisible to the worker, handlers, and client. See
//! [`docs/architecture/chat-v4.md`](../../../docs/architecture/chat-v4.md).

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};
use ulid::Ulid;

use crate::storage::ConversationId;

use super::inprocess::Session;

/// Opaque stream cursor. Every data frame the bus emits carries one as
/// its SSE `id:` line; clients only ever echo it back via
/// `Last-Event-Id`. In-process it is a monotonic `u64` rendered as
/// decimal; under Redis (Phase 2) it becomes the stream entry id. Code
/// outside the `chat` module treats it as opaque — parse from / format to
/// the wire string, never interpret. The `u64` payload is `pub(crate)` so
/// the in-process buffer can do the `c + 1 >= base` window arithmetic
/// (§4.1); a future multi-impl world would hide that behind the impl.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cursor(pub(crate) u64);

impl Cursor {
    /// The cursor a brand-new session starts at, before its first frame.
    pub(crate) const ZERO: Cursor = Cursor(0);

    pub(crate) fn get(self) -> u64 {
        self.0
    }

    pub(crate) fn next(self) -> Cursor {
        Cursor(self.0 + 1)
    }
}

impl fmt::Display for Cursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Cursor {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.trim().parse::<u64>().map(Cursor)
    }
}

/// Returned by [`TurnBus::try_start`] when a turn is already in flight for
/// the conversation. The POST handler maps it to `409 {"reason":"in_flight"}`.
#[derive(Debug)]
pub struct AlreadyRunning;

/// The transport behind chat turns. One implementation per deployment
/// shape (in-memory now, Redis later); selected once at startup and held
/// in `AppState` as `Arc<dyn TurnBus>`.
#[async_trait]
pub trait TurnBus: Send + Sync {
    /// Single-flight: atomically claim the turn slot for `conv` and append
    /// the first (`user_message`) frame. `Err(AlreadyRunning)` ⇒ a turn is
    /// already running ⇒ 409.
    async fn try_start(
        &self,
        conv: &ConversationId,
        user_message: Bytes,
    ) -> Result<TurnHandle, AlreadyRunning>;

    /// Subscribe to `conv`'s log from an opaque cursor (§4.1). Items
    /// already include their `id:` line — the SSE handler writes them
    /// verbatim. `from = None` is a fresh connect; `Some(c)` resumes after
    /// cursor `c` (or yields a `resync` frame if `c` fell out of the
    /// window).
    async fn subscribe(
        &self,
        conv: &ConversationId,
        from: Option<Cursor>,
    ) -> BoxStream<'static, Bytes>;

    /// Flip the in-flight turn's cancel token. No-op if idle. Idempotent.
    /// The worker (sole writer) turns the flipped token into a `clear`
    /// frame; `cancel` itself emits nothing.
    async fn cancel(&self, conv: &ConversationId);
}

/// Handle to the single in-flight turn, held by the worker. The worker is
/// the **only** writer of a turn's stream (§8): it `append`s data frames
/// and `terminate`s with the trailing `finish`/`clear`. Cancel is
/// observed via [`TurnHandle::cancelled`].
///
/// Concrete and in-process for Phase 1 (the seam that survives the Redis
/// swap is [`TurnBus`] + the cursor/`resync` wire, not this handle).
pub struct TurnHandle {
    session: Arc<Session>,
    cancel: CancellationToken,
    /// Set once `terminate` ran. While `false`, `Drop` treats the handle
    /// as an unwinding/abandoned turn and emits a `clear` so the
    /// conversation isn't wedged at 409 forever (replaces v3's
    /// `WorkerGuard`).
    done: bool,
}

impl TurnHandle {
    pub(super) fn new(session: Arc<Session>, cancel: CancellationToken) -> Self {
        Self {
            session,
            cancel,
            done: false,
        }
    }

    /// Append one whole SSE frame: assign the next cursor, buffer it, wake
    /// readers. No-op if the turn is no longer running.
    pub async fn append(&self, frame: Bytes) {
        self.session.append(frame);
    }

    /// Append the terminal frame (`finish` or `clear`), release the
    /// single-flight slot, and disarm the `Drop` guard. The buffered
    /// frames linger for the GC grace window (§9) so still-draining
    /// readers see the terminal frame.
    pub async fn terminate(&mut self, frame: Bytes) {
        self.session.terminate(frame);
        self.done = true;
    }

    /// Future that resolves when `/stop` flips this turn's token. The
    /// worker awaits it in a biased `select!` against the LLM stream.
    pub fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        self.cancel.cancelled()
    }
}

impl Drop for TurnHandle {
    fn drop(&mut self) {
        if !self.done {
            // Unwind / abandoned turn: emit `clear` and release the slot.
            self.session.clear_if_running();
        }
    }
}

/// Opaque worker identifier. Wraps a ULID — short, sortable, never
/// collides at our cadence. **Internal only**: surfaces in logs / tracing
/// spans so a worker's events can be correlated; never exposed to clients
/// (no `task_id` in URLs or response bodies — stop is conversation-scoped).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(Ulid);

impl TaskId {
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    pub fn parse(s: &str) -> Result<Self, ulid::DecodeError> {
        Ulid::from_string(s).map(Self)
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for TaskId {
    type Err = ulid::DecodeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trips_through_string() {
        let c = Cursor(42);
        assert_eq!(c.to_string(), "42");
        assert_eq!("42".parse::<Cursor>().unwrap(), c);
        assert_eq!(" 7 ".parse::<Cursor>().unwrap(), Cursor(7));
        assert!("notanumber".parse::<Cursor>().is_err());
    }

    #[test]
    fn task_id_round_trips_through_string() {
        let id = TaskId::new();
        let s = id.to_string();
        assert_eq!(s.len(), 26, "ULID canonical form is 26 chars");
        assert_eq!(TaskId::parse(&s).expect("parse back"), id);
    }

    #[test]
    fn task_id_rejects_garbage() {
        assert!(TaskId::parse("not a ulid").is_err());
        assert!(TaskId::parse("").is_err());
    }
}
