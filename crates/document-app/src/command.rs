use chrono::{DateTime, Utc};
use delphi_document_domain::{MetadataPatch, DOCUMENT_CONTRACT_VERSION};
use serde::{Deserialize, Serialize};

use crate::upload_state::UploadMode;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictPolicy {
    /// Apply anyway and record what the uploader was looking at, so the loser
    /// of a race learns it was superseded instead of silently losing.
    #[default]
    Supersede,
    Fail,
}

/// The one work item in this slice.
///
/// **Self-contained**: every parameter the worker needs is copied in here at
/// `/complete` time, so the pipeline can run with no other read.
///
/// The worker does consult the upload record, but only for two questions the
/// command cannot answer: *has this upload already been given a terminal
/// answer* (replay it rather than re-scanning a large object), and *has the
/// record expired* (reclaim the bytes). It never takes a parameter from there,
/// which is what lets the record die mid-pipeline without stranding anything.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UploadCompleted {
    pub v: u16,
    /// `upload-completed:<tenant>:<upload_id>`, used as `Nats-Msg-Id`.
    pub command_id: String,
    pub tenant_id: String,
    pub owner_user_id: String,
    pub upload_id: String,
    pub document_id: String,
    pub mode: UploadMode,
    pub storage_key: String,
    pub multipart_upload_id: String,
    pub filename: String,
    pub content_type: String,
    pub declared_size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub if_match: Option<u64>,
    #[serde(default)]
    pub on_conflict: ConflictPolicy,
    #[serde(default)]
    pub patch: MetadataPatch,
    pub ts: DateTime<Utc>,
}

/// `max_payload` in `ops/nats/nats.conf` — the hard limit on one published
/// message, and the only ceiling this command has.
///
/// There is deliberately no second, smaller limit checked per request. One
/// existed while the client's parts list rode along and made the worst case
/// 0.68 MiB; it cost a full extra serialisation of every command, to guard a
/// bound no valid input could reach. With the parts list gone the worst case is
/// around 40 KiB, and what bounds it is `validate_metadata_patch` — so the
/// regression worth catching is a loosened metadata limit, and
/// `the_largest_valid_command_is_far_under_the_transport_limit` catches that at
/// build time rather than on every upload.
pub const NATS_MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;

impl UploadCompleted {
    pub fn contract_version() -> u16 {
        DOCUMENT_CONTRACT_VERSION
    }
}
