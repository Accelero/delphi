//! Process-global directory of live [`SessionState`]s.
//!
//! Keyed by `ConversationId`. Entries are [`Weak`] so a session
//! collapses naturally when no `Arc` remains (worker finished + every
//! reader disconnected). Lookups are lazy: a dead `Weak` is replaced
//! on the next `get_or_create` call.
//!
//! The registry is cheap to clone (`Arc` internals) and lives in
//! [`crate::state::AppState`] for the lifetime of the process.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use tokio::sync::RwLock;

use crate::storage::ConversationId;

use super::state::SessionState;

/// Multi-conversation lookup table. One instance per backend process,
/// kept inside `AppState`.
pub struct SessionRegistry {
    inner: RwLock<HashMap<ConversationId, Weak<SessionState>>>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Return the existing `Arc<SessionState>` for `id`, or construct
    /// (and register) a fresh one. Reaps a stale `Weak` if upgrade
    /// fails. Take the write-lock once; we don't bother with the
    /// read-then-upgrade dance because contention here is low (one
    /// look-up per submit / per subscribe).
    pub async fn get_or_create(&self, id: &ConversationId) -> Arc<SessionState> {
        let mut g = self.inner.write().await;
        if let Some(weak) = g.get(id) {
            if let Some(strong) = weak.upgrade() {
                return strong;
            }
        }
        let fresh = Arc::new(SessionState::new());
        g.insert(id.clone(), Arc::downgrade(&fresh));
        fresh
    }

    /// Read-only lookup. Returns `None` if no live session is registered
    /// (either never created, or the `Weak` has gone dead). Used by the
    /// stop endpoint, which has nothing to cancel when no worker is
    /// running.
    pub async fn lookup(&self, id: &ConversationId) -> Option<Arc<SessionState>> {
        let g = self.inner.read().await;
        g.get(id).and_then(Weak::upgrade)
    }
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use surrealdb::RecordId;

    fn cid(k: &str) -> ConversationId {
        RecordId::from(("conversation", k))
    }

    #[tokio::test]
    async fn get_or_create_returns_same_arc_for_same_id() {
        let r = SessionRegistry::new();
        let a = r.get_or_create(&cid("alpha")).await;
        let b = r.get_or_create(&cid("alpha")).await;
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[tokio::test]
    async fn distinct_ids_get_distinct_state() {
        let r = SessionRegistry::new();
        let a = r.get_or_create(&cid("alpha")).await;
        let b = r.get_or_create(&cid("beta")).await;
        assert!(!Arc::ptr_eq(&a, &b));
    }

    #[tokio::test]
    async fn lookup_is_none_for_unknown_id() {
        let r = SessionRegistry::new();
        assert!(r.lookup(&cid("nope")).await.is_none());
    }

    #[tokio::test]
    async fn dead_weak_is_reaped_and_replaced() {
        let r = SessionRegistry::new();
        let a = r.get_or_create(&cid("alpha")).await;
        let a_ptr = Arc::as_ptr(&a) as usize;
        drop(a);
        // After all strong refs dropped, the next get_or_create returns a
        // brand-new state, not the dead weak's address.
        let b = r.get_or_create(&cid("alpha")).await;
        assert_ne!(Arc::as_ptr(&b) as usize, a_ptr);
    }

    #[tokio::test]
    async fn lookup_returns_live_state_only() {
        let r = SessionRegistry::new();
        let a = r.get_or_create(&cid("alpha")).await;
        assert!(r.lookup(&cid("alpha")).await.is_some());
        drop(a);
        // Weak still in the map but upgrade fails ⇒ lookup is None.
        assert!(r.lookup(&cid("alpha")).await.is_none());
    }
}
