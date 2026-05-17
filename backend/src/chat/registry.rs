//! Process-global directory of in-flight chat workers.
//!
//! Maps `TaskId → CancellationToken`. The POST handler inserts on
//! worker spawn; the worker removes when it exits; the `/stop` endpoint
//! cancels by id.
//!
//! Lock-free via `DashMap` — every operation is one shard lookup, no
//! global mutex. The registry is cheap to clone (`Arc` internals) and
//! lives in [`crate::state::AppState`] for the lifetime of the process.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

/// Opaque task identifier. Wraps a ULID — short, sortable, URL-safe,
/// and never collides at our cadence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(Ulid);

impl TaskId {
    /// Mint a fresh, monotonically-increasing id. Used by the POST
    /// handler when spawning a worker.
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    /// Parse the canonical 26-character Crockford-base32 representation
    /// the client / `/stop` URL carries.
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

/// Live chat-worker handles, keyed by `TaskId`. Cloneable cheaply —
/// internals are `Arc`-counted.
#[derive(Clone, Default)]
pub struct TaskRegistry {
    inner: Arc<DashMap<TaskId, CancellationToken>>,
}

impl TaskRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    /// Register a new task. Overwrites any prior entry for the same id
    /// (id collisions don't happen in practice — ULIDs are unique by
    /// construction).
    pub fn insert(&self, id: TaskId, token: CancellationToken) {
        self.inner.insert(id, token);
    }

    /// Drop the task's entry. Returns its `CancellationToken` so the
    /// caller (the worker on exit, typically) can drop it explicitly if
    /// they want.
    pub fn remove(&self, id: &TaskId) -> Option<CancellationToken> {
        self.inner.remove(id).map(|(_, t)| t)
    }

    /// Cancel the named task. Returns `true` if the task was present
    /// (the `/stop` endpoint uses this just to log; the response is
    /// 204 either way per the design doc).
    pub fn cancel(&self, id: &TaskId) -> bool {
        match self.inner.remove(id) {
            Some((_, token)) => {
                token.cancel();
                true
            }
            None => false,
        }
    }

    /// Number of live tasks. Convenience for tracing / metrics; not
    /// load-bearing.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_id_round_trips_through_string() {
        let id = TaskId::new();
        let s = id.to_string();
        assert_eq!(s.len(), 26, "ULID canonical form is 26 chars");
        let parsed = TaskId::parse(&s).expect("parse back");
        assert_eq!(id, parsed);
    }

    #[test]
    fn task_id_rejects_garbage() {
        assert!(TaskId::parse("not a ulid").is_err());
        assert!(TaskId::parse("").is_err());
    }

    #[tokio::test]
    async fn insert_then_cancel_flips_the_token() {
        let r = TaskRegistry::new();
        let id = TaskId::new();
        let token = CancellationToken::new();
        r.insert(id, token.clone());
        assert!(!token.is_cancelled());
        assert!(r.cancel(&id));
        assert!(token.is_cancelled());
        // Cancelling again is a no-op (entry already removed).
        assert!(!r.cancel(&id));
    }

    #[tokio::test]
    async fn remove_returns_the_token_without_cancelling() {
        let r = TaskRegistry::new();
        let id = TaskId::new();
        let token = CancellationToken::new();
        r.insert(id, token.clone());
        let returned = r.remove(&id).expect("present");
        assert!(!returned.is_cancelled(), "remove must not cancel");
        assert!(r.is_empty());
    }

    #[tokio::test]
    async fn cancel_unknown_is_false() {
        let r = TaskRegistry::new();
        assert!(!r.cancel(&TaskId::new()));
    }
}
