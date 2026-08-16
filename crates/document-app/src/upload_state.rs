//! The upload's whole state, from preflight to its terminal answer.
//!
//! One record, in NATS KV, keyed `<tenant_id>/<user_id>/<upload_id>`. It is the
//! **only** tracker an upload has. Postgres holds no upload state at all; it is
//! the document projection and nothing else.
//!
//! ```text
//! preflight ─ uploading ─ /complete ─ scanning ─ worker ─ accepted | rejected
//!             └──────────────── one KV record, TTL-bounded ──────────────┘
//!                                                            └ doc event ┘
//! ```
//!
//! The record is the crossover point. Everything before the event is
//! **user-scoped and temporary**: only the uploader can see it, and the bucket's
//! `max_age` deletes it whether or not anyone looked. Everything after the event
//! is **tenant-scoped and permanent**: it is in the log, and the projection
//! serves it to every member of the tenant.
//!
//! Two consequences worth being explicit about, because they were the price of
//! collapsing two stores into one:
//!
//! * **There is no history of old uploads.** Once the TTL elapses the record is
//!   gone, and a rejection leaves nothing behind — no event is appended for one.
//!   Ask within the window or not at all.
//! * **Nobody can ask "who else is uploading to this document?"** That is a
//!   cross-user query over a user-scoped keyspace, which KV cannot answer.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadMode {
    Create,
    Replace,
}

impl UploadMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Replace => "replace",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum UploadStatus {
    /// Preflight has run; the client is putting parts.
    Uploading,
    /// `/complete` has queued the work item.
    Scanning,
    /// Terminal. The event is durable.
    Accepted { version: u64, superseded: bool },
    /// Terminal. The object is gone and this says why.
    Rejected { reason: String },
}

impl UploadStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Uploading => "uploading",
            Self::Scanning => "scanning",
            Self::Accepted { .. } => "accepted",
            Self::Rejected { .. } => "rejected",
        }
    }

    /// A terminal status is **final**. Nothing may move a record out of one.
    ///
    /// Two writers reach this record and they are not ordered: `/complete`
    /// publishes the work item *before* marking the record `scanning`, so a
    /// worker that finishes a small file inside that window has already written
    /// `accepted`. Letting the later write land would report `scanning` forever,
    /// with nothing left to advance it. First terminal answer wins — which also
    /// keeps the *first* reject reason, the one that says what actually went
    /// wrong rather than what a redelivery found after its own cleanup.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Accepted { .. } | Self::Rejected { .. })
    }
}

/// What preflight decided, plus where the upload has got to.
///
/// The immutable half is fixed at preflight and never rewritten: nothing may
/// change an upload's parameters after the client has started slicing to them.
/// Only `status` and `updated_at` move, and only through a compare-and-swap on
/// the record's revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadState {
    pub tenant_id: String,
    pub owner_user_id: String,
    pub upload_id: String,
    pub document_id: String,
    pub mode: UploadMode,
    /// `= object_key(tenant_id, upload_id)`; stored so the worker's command can
    /// be self-contained without re-deriving it.
    pub storage_key: String,
    pub multipart_upload_id: String,
    pub filename: String,
    /// Resolved, after defaulting.
    pub content_type: String,
    pub declared_size: u64,
    pub part_size_bytes: u64,
    pub part_count: u16,
    pub status: UploadStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl UploadState {
    /// `<tenant_id>/<user_id>/<upload_id>`.
    ///
    /// The caller is *in the key*, so another user derives a different key and
    /// finds nothing — a structural `404` with no existence disclosure, rather
    /// than an ownership check someone can forget to write.
    ///
    /// Both principal segments are validated at the auth boundary, so this
    /// cannot escape its namespace.
    pub fn key(tenant_id: &str, user_id: &str, upload_id: &str) -> String {
        format!("{tenant_id}/{user_id}/{upload_id}")
    }

    pub fn own_key(&self) -> String {
        Self::key(&self.tenant_id, &self.owner_user_id, &self.upload_id)
    }

    /// Deliberately *not* stored: presigned URLs (bearer capabilities that
    /// expire — regenerate, never store), part ETags (they arrive in the
    /// `/complete` body), and the metadata patch (supplied at `/complete`).
    pub fn with_status(&self, status: UploadStatus, now: DateTime<Utc>) -> Self {
        Self {
            status,
            updated_at: now,
            ..self.clone()
        }
    }
}

/// A record together with the revision it was read at, for compare-and-swap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredUpload {
    pub state: UploadState,
    pub revision: u64,
}

/// Stable reject reasons. Clients branch on these; the text after them is for
/// humans.
pub mod reject_reason {
    pub const MULTIPART_LOST: &str = "multipart_lost";
    pub const INVALID_PARTS: &str = "invalid_parts";
    pub const SIZE_MISMATCH: &str = "size_mismatch";
    pub const MALWARE_DETECTED: &str = "malware_detected";
    pub const VERSION_CONFLICT: &str = "version_conflict";
    pub const CONTENT_REJECTED: &str = "content_rejected";
    pub const CORRUPT_HISTORY: &str = "corrupt_history";
    pub const PIPELINE_FAILED: &str = "pipeline_failed";
    /// The record expired before the work item ran. Nothing is left to record
    /// it against — the reason exists for the log line, not for a client.
    pub const UPLOAD_EXPIRED: &str = "upload_expired";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_key_puts_the_caller_in_the_keyspace() {
        assert_eq!(UploadState::key("acme", "user-1", "u1"), "acme/user-1/u1");
        assert_ne!(
            UploadState::key("acme", "user-1", "u1"),
            UploadState::key("acme", "user-2", "u1"),
            "another user must derive a different key"
        );
    }

    #[test]
    fn only_the_two_outcome_states_are_terminal() {
        assert!(!UploadStatus::Uploading.is_terminal());
        assert!(!UploadStatus::Scanning.is_terminal());
        assert!(UploadStatus::Accepted {
            version: 1,
            superseded: false
        }
        .is_terminal());
        assert!(UploadStatus::Rejected {
            reason: reject_reason::SIZE_MISMATCH.to_owned()
        }
        .is_terminal());
    }

    #[test]
    fn the_status_wire_form_is_a_tagged_union_clients_can_branch_on() {
        let json = serde_json::to_value(UploadStatus::Accepted {
            version: 7,
            superseded: true,
        })
        .expect("serialize");
        assert_eq!(json["state"], "accepted");
        assert_eq!(json["version"], 7);
        assert_eq!(json["superseded"], true);
    }
}
