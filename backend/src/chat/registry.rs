//! Process-global directory of per-conversation chat sessions.
//!
//! Maps [`ConversationId`] → [`Arc<SessionState>`]. One entry per
//! ever-touched conversation key.
//!
//! GC: **none in v1.** For single-user dev this is trivially bounded.
//! For SaaS this is a slow memory leak (one `SessionState` entry +
//! retained `Vec` capacity per ever-visited conversation). The
//! eviction trigger condition for a follow-up is
//! `current.is_none() && subscribers.is_empty() && idle_for > 1h`.
//!
//! Lock-free via `DashMap` — every operation is one shard lookup.
//! [`TaskId`] is kept as an internal-only handle for logs/tracing; it is
//! **not** part of the public HTTP API (no `task_id` in URLs or response
//! bodies — stop is conversation-scoped).
//!
// TODO(v2): evict `SessionState` entries whose `current` is None,
// `subscribers` is empty, and have been idle for > 1 hour.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use dashmap::DashMap;
use ulid::Ulid;

use crate::storage::ConversationId;

use super::session::SessionState;

/// Opaque worker identifier. Wraps a ULID — short, sortable, and never
/// collides at our cadence. **Internal only**: surfaces in logs /
/// tracing spans so we can correlate a worker's events; never exposed
/// to clients in v3.
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

/// One [`SessionState`] per [`ConversationId`] that has ever been
/// touched in this process. Cheaply cloneable — internals are
/// `Arc`-counted.
#[derive(Clone, Default)]
pub struct SessionRegistry {
    inner: Arc<DashMap<ConversationId, Arc<SessionState>>>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    /// Return the (possibly freshly-created) session for the given
    /// conversation. Multiple callers receive the same `Arc` so they
    /// share the buffer + subscriber list.
    pub fn for_conversation(&self, id: &ConversationId) -> Arc<SessionState> {
        if let Some(s) = self.inner.get(id) {
            return s.clone();
        }
        // Two threads can race past the `get`; `entry().or_insert_with`
        // serialises construction so the loser drops their fresh
        // SessionState before any subscriber sees it.
        self.inner
            .entry(id.clone())
            .or_insert_with(|| Arc::new(SessionState::new()))
            .clone()
    }

    /// Look up an existing session **without** creating one. The `/stop`
    /// handler uses this — there's no point materialising state for a
    /// conversation that has never had a turn.
    pub fn lookup(&self, id: &ConversationId) -> Option<Arc<SessionState>> {
        self.inner.get(id).map(|s| s.clone())
    }

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
    use surrealdb::RecordId;

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

    #[test]
    fn for_conversation_is_idempotent() {
        let r = SessionRegistry::new();
        let id: ConversationId = RecordId::from(("conversation", "abc"));
        let a = r.for_conversation(&id);
        let b = r.for_conversation(&id);
        assert!(Arc::ptr_eq(&a, &b), "same conv id ⇒ same Arc");
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn lookup_does_not_create() {
        let r = SessionRegistry::new();
        let id: ConversationId = RecordId::from(("conversation", "missing"));
        assert!(r.lookup(&id).is_none());
        assert!(r.is_empty());
    }
}
