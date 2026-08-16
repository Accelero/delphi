//! The one way an upload's status ever moves.
//!
//! Both the API (`/complete` → `scanning`) and the worker (→ `accepted` /
//! `rejected`) write this record, and they are **not ordered**: `/complete`
//! publishes the work item before marking the record, so a worker that finishes
//! a small file inside that window writes its terminal answer first. Every
//! write therefore goes through here, and here enforces one rule:
//!
//! > A terminal status is final.
//!
//! Without it the api-service's late `scanning` lands on top of `accepted`, and
//! the upload reports `scanning` forever with nothing left to advance it.

use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::errors::{ContextError, DocumentError};
use crate::ports::UploadStateStore;
use crate::upload_state::{UploadState, UploadStatus};

/// How many times a CAS re-reads before giving up. Two writers at most, and a
/// terminal status ends the contest, so this cannot spin.
const CAS_ATTEMPTS: u32 = 3;

/// What happened to the requested transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transition {
    /// The record now holds the requested status.
    Applied(UploadState),
    /// The record was already terminal, so the request was dropped. Carries the
    /// status that won — the caller usually wants to report *that*.
    AlreadyTerminal(UploadState),
    /// The record is gone: the bucket TTL elapsed. The caller must clean up.
    Expired,
}

/// Move an upload to `status`, unless it has already finished.
pub async fn set_status(
    store: &Arc<dyn UploadStateStore>,
    tenant: &str,
    user: &str,
    upload_id: &str,
    status: UploadStatus,
    now: DateTime<Utc>,
) -> Result<Transition, DocumentError> {
    for _ in 0..CAS_ATTEMPTS {
        let Some(stored) = store.get(tenant, user, upload_id).await? else {
            return Ok(Transition::Expired);
        };
        if stored.state.status.is_terminal() {
            return Ok(Transition::AlreadyTerminal(stored.state));
        }

        let next = stored.state.with_status(status.clone(), now);
        match store.update(&next, stored.revision).await {
            Ok(_) => return Ok(Transition::Applied(next)),
            // Someone wrote between the read and the CAS. Re-read: they may
            // have written the terminal answer we must now defer to.
            Err(ContextError::Conflict) => continue,
            Err(ContextError::Expired) => return Ok(Transition::Expired),
            Err(error) => return Err(error.into()),
        }
    }

    Err(DocumentError::internal(
        "upload state",
        "gave up after repeated compare-and-swap conflicts",
    ))
}

/// Load a record, distinguishing "expired" from "not yours".
///
/// Both look identical from the store — the caller is *in the key*, so another
/// user's read simply misses. The distinction is made by who is asking: this is
/// only ever called with the authenticated principal, so a miss means the TTL
/// elapsed rather than that someone is probing.
pub async fn load(
    store: &Arc<dyn UploadStateStore>,
    tenant: &str,
    user: &str,
    upload_id: &str,
) -> Result<Option<UploadState>, DocumentError> {
    let Some(stored) = store.get(tenant, user, upload_id).await? else {
        return Ok(None);
    };
    // Belt and braces against a key-construction mistake: the key already
    // contains both, so a mismatch here means the key function changed.
    if stored.state.owner_user_id != user || stored.state.tenant_id != tenant {
        return Ok(None);
    }
    Ok(Some(stored.state))
}

/// Convenience for the worker, which has no revision in hand and does not care
/// which of the two branches applied — only that it must clean up on `Expired`.
pub async fn finish_with(
    store: &Arc<dyn UploadStateStore>,
    state_key: (&str, &str, &str),
    status: UploadStatus,
    now: DateTime<Utc>,
) -> Result<Transition, DocumentError> {
    let (tenant, user, upload_id) = state_key;
    set_status(store, tenant, user, upload_id, status, now).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::*;
    use crate::upload_state::UploadMode;

    fn seeded() -> (Arc<dyn UploadStateStore>, Arc<MemoryUploadStateStore>) {
        let store = Arc::new(MemoryUploadStateStore::default());
        let state = UploadState {
            tenant_id: "acme".to_owned(),
            owner_user_id: "user-1".to_owned(),
            upload_id: "u1".to_owned(),
            document_id: "d1".to_owned(),
            mode: UploadMode::Create,
            storage_key: "tenants/acme/blobs/u1/original".to_owned(),
            multipart_upload_id: "mp-1".to_owned(),
            filename: "report.pdf".to_owned(),
            content_type: "application/pdf".to_owned(),
            declared_size: 1024,
            part_size_bytes: 1024,
            part_count: 1,
            status: UploadStatus::Uploading,
            created_at: fixed_time(0),
            updated_at: fixed_time(0),
        };
        store.seed(state);
        (store.clone(), store)
    }

    #[tokio::test]
    async fn a_normal_transition_applies() {
        let (store, _) = seeded();
        let outcome = set_status(
            &store,
            "acme",
            "user-1",
            "u1",
            UploadStatus::Scanning,
            fixed_time(1),
        )
        .await
        .expect("transition");
        assert!(matches!(outcome, Transition::Applied(state) if state.status == UploadStatus::Scanning));
    }

    #[tokio::test]
    async fn a_late_scanning_write_cannot_undo_a_terminal_answer() {
        // The race: the worker finishes before /complete writes `scanning`.
        let (store, _) = seeded();
        let accepted = UploadStatus::Accepted {
            version: 1,
            superseded: false,
        };
        set_status(&store, "acme", "user-1", "u1", accepted.clone(), fixed_time(1))
            .await
            .expect("worker wins the race");

        let late = set_status(
            &store,
            "acme",
            "user-1",
            "u1",
            UploadStatus::Scanning,
            fixed_time(2),
        )
        .await
        .expect("transition");

        match late {
            Transition::AlreadyTerminal(state) => assert_eq!(state.status, accepted),
            other => panic!("expected the terminal status to win, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_expired_record_is_reported_rather_than_recreated() {
        let (store, memory) = seeded();
        memory.expire_all();

        let outcome = set_status(
            &store,
            "acme",
            "user-1",
            "u1",
            UploadStatus::Scanning,
            fixed_time(1),
        )
        .await
        .expect("transition");
        assert_eq!(outcome, Transition::Expired);
        assert!(
            memory.get("acme", "user-1", "u1").await.expect("get").is_none(),
            "a transition must never resurrect an expired record"
        );
    }

    #[tokio::test]
    async fn a_lost_cas_re_reads_and_defers_to_the_winner() {
        let (store, memory) = seeded();
        // The next update fails its CAS once; the re-read then finds the
        // terminal status the other writer landed.
        memory.fail_next_update_with_conflict(UploadStatus::Rejected {
            reason: "size_mismatch".to_owned(),
        });

        let outcome = set_status(
            &store,
            "acme",
            "user-1",
            "u1",
            UploadStatus::Scanning,
            fixed_time(1),
        )
        .await
        .expect("transition");

        assert!(matches!(
            outcome,
            Transition::AlreadyTerminal(state)
                if matches!(state.status, UploadStatus::Rejected { .. })
        ));
    }
}
