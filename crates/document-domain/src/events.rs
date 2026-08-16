use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Bumped only when the envelope shape changes incompatibly. Payload variants
/// are added without bumping it; consumers that do not know a variant fail at
/// deserialization and skip the event (see the projection loop).
pub const DOCUMENT_CONTRACT_VERSION: u16 = 1;

/// One durable fact about one document.
///
/// NOTE: `PartialEq` only. `serde_json::Value` is not `Eq` (it can hold an
/// `f64`), so deriving `Eq` anywhere in this tree will not compile.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DocumentEvent {
    pub v: u16,
    /// Deterministic, derived from the work that produced it. Used as
    /// `Nats-Msg-Id`, so a random id would defeat dedupe on redelivery.
    pub event_id: String,
    pub tenant_id: String,
    pub document_id: String,
    pub actor: Actor,
    /// Document version *after* this event.
    pub version: u64,
    pub ts: DateTime<Utc>,
    pub payload: DocumentEventPayload,
}

impl DocumentEvent {
    /// `true` when this event advances the document version by one, `false`
    /// when it records a fact *about* the current version and repeats it.
    pub fn advances_version(&self) -> bool {
        self.payload.advances_version()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Actor {
    User { user_id: String },
    System { component: String },
}

impl Actor {
    pub fn user_id(&self) -> Option<&str> {
        match self {
            Self::User { user_id } => Some(user_id),
            Self::System { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DocumentEventPayload {
    /// Produced now, by the worker, on the create path.
    DocumentCreated(DocumentCreated),
    /// Produced now, by the worker, on the replace path.
    DocumentBlobValidated(DocumentBlobValidated),
    /// Folded only. No producer in this slice.
    DocumentMetadataChanged { patch: MetadataPatch },
    /// Folded only. No producer in this slice.
    DocumentTextExtracted(DocumentTextExtracted),
    /// Folded only. No producer in this slice.
    DocumentIndexed(DocumentIndexed),
    /// Folded only. No producer in this slice.
    DocumentStageFailed(DocumentStageFailed),
    /// Folded only. No producer in this slice.
    DocumentReverted {
        reverted_to: u64,
        patch: MetadataPatch,
    },
    /// Folded only. No producer in this slice.
    DocumentDeleted { reason: String },
    /// Folded only. No producer in this slice; reserved for version retention,
    /// which the worker will enforce at blob-update time. See
    /// `specs/document-lifecycle-implementation.md` §1.
    DocumentBlobPruned { blob_ref: String, reason: String },
}

impl DocumentEventPayload {
    pub fn advances_version(&self) -> bool {
        match self {
            Self::DocumentCreated(_)
            | Self::DocumentBlobValidated(_)
            | Self::DocumentMetadataChanged { .. }
            | Self::DocumentReverted { .. }
            | Self::DocumentDeleted { .. } => true,
            Self::DocumentTextExtracted(_)
            | Self::DocumentIndexed(_)
            | Self::DocumentStageFailed(_)
            | Self::DocumentBlobPruned { .. } => false,
        }
    }

    /// The blob this event introduced, if any.
    ///
    /// The replace path's redelivery guard scans a document's *whole* history
    /// with this: a concurrent upload may already have superseded ours, so
    /// `current_blob != upload_id` does not mean we have not applied.
    pub fn blob_ref(&self) -> Option<&str> {
        match self {
            Self::DocumentCreated(created) => Some(&created.blob_ref),
            Self::DocumentBlobValidated(validated) => Some(&validated.blob_ref),
            // `DocumentBlobPruned` also names a blob, but the blob it *removes*.
            // Reporting it here would make the guard treat a pruned upload as
            // one that had been applied.
            Self::DocumentBlobPruned { .. } => None,
            Self::DocumentMetadataChanged { .. }
            | Self::DocumentTextExtracted(_)
            | Self::DocumentIndexed(_)
            | Self::DocumentStageFailed(_)
            | Self::DocumentReverted { .. }
            | Self::DocumentDeleted { .. } => None,
        }
    }

    /// Stable token for logs and metrics.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::DocumentCreated(_) => "document_created",
            Self::DocumentBlobValidated(_) => "document_blob_validated",
            Self::DocumentMetadataChanged { .. } => "document_metadata_changed",
            Self::DocumentTextExtracted(_) => "document_text_extracted",
            Self::DocumentIndexed(_) => "document_indexed",
            Self::DocumentStageFailed(_) => "document_stage_failed",
            Self::DocumentReverted { .. } => "document_reverted",
            Self::DocumentDeleted { .. } => "document_deleted",
            Self::DocumentBlobPruned { .. } => "document_blob_pruned",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DocumentCreated {
    /// The `upload_id`. The object key is a pure function of
    /// `(tenant_id, blob_ref)`; no URL is ever persisted.
    pub blob_ref: String,
    pub filename: String,
    pub content_type: String,
    pub byte_size: u64,
    /// `"sha256:<hex>"`.
    pub checksum: String,
    pub patch: MetadataPatch,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DocumentBlobValidated {
    pub blob_ref: String,
    pub filename: String,
    pub content_type: String,
    pub byte_size: u64,
    pub checksum: String,
    pub patch: MetadataPatch,
    /// Version the uploader was looking at. If not `version - 1`, this upload
    /// superseded a change its author had not seen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub based_on_version: Option<u64>,
}

/// Partial by construction: only `Some` fields are written by the fold, so the
/// same shape serves both "set these fields" and "change nothing".
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MetadataPatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl MetadataPatch {
    pub fn is_empty(&self) -> bool {
        self.title.is_none()
            && self.tags.is_none()
            && self.description.is_none()
            && self.metadata.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DocumentTextExtracted {
    pub for_version: u64,
    pub extractor_version: String,
    pub char_count: u64,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DocumentIndexed {
    pub for_version: u64,
    pub vector_count: u64,
    pub embedding_model: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DocumentStageFailed {
    pub for_version: u64,
    pub stage: String,
    pub reason: String,
    pub attempts: u32,
}

/// The stage token whose failure marks the index unusable. Other stages fail
/// without touching `index_state`.
pub const INDEX_STAGE: &str = "index";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_payload_variant_fails_at_deserialization() {
        // This is why the projection loop must handle a deserialization
        // failure separately from a FoldError: an event type added by a newer
        // producer never reaches `apply` at all.
        let raw = serde_json::json!({
            "v": 1,
            "event_id": "e1",
            "tenant_id": "t",
            "document_id": "d",
            "actor": { "kind": "system", "component": "worker" },
            "version": 1,
            "ts": "2026-01-01T00:00:00Z",
            "payload": { "type": "document_teleported", "destination": "mars" }
        });
        assert!(serde_json::from_value::<DocumentEvent>(raw).is_err());
    }

    #[test]
    fn payloads_round_trip_through_serde() {
        let event = DocumentEvent {
            v: DOCUMENT_CONTRACT_VERSION,
            event_id: "e1".to_owned(),
            tenant_id: "t".to_owned(),
            document_id: "d".to_owned(),
            actor: Actor::User {
                user_id: "u".to_owned(),
            },
            version: 1,
            ts: chrono::Utc::now(),
            payload: DocumentEventPayload::DocumentCreated(DocumentCreated {
                blob_ref: "b".to_owned(),
                filename: "f.pdf".to_owned(),
                content_type: "application/pdf".to_owned(),
                byte_size: 10,
                checksum: "sha256:ab".to_owned(),
                patch: MetadataPatch {
                    title: Some("t".to_owned()),
                    ..Default::default()
                },
            }),
        };
        let json = serde_json::to_string(&event).expect("serialize");
        let back: DocumentEvent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(event, back);
    }

    #[test]
    fn version_rules_split_cleanly_between_advancing_and_repeating() {
        let advancing = [
            DocumentEventPayload::DocumentMetadataChanged {
                patch: MetadataPatch::default(),
            },
            DocumentEventPayload::DocumentReverted {
                reverted_to: 1,
                patch: MetadataPatch::default(),
            },
            DocumentEventPayload::DocumentDeleted {
                reason: "r".to_owned(),
            },
        ];
        for payload in advancing {
            assert!(payload.advances_version(), "{}", payload.type_name());
        }

        let repeating = [
            DocumentEventPayload::DocumentIndexed(DocumentIndexed {
                for_version: 1,
                vector_count: 1,
                embedding_model: "m".to_owned(),
            }),
            DocumentEventPayload::DocumentStageFailed(DocumentStageFailed {
                for_version: 1,
                stage: INDEX_STAGE.to_owned(),
                reason: "r".to_owned(),
                attempts: 1,
            }),
            DocumentEventPayload::DocumentBlobPruned {
                blob_ref: "b".to_owned(),
                reason: "retention".to_owned(),
            },
        ];
        for payload in repeating {
            assert!(!payload.advances_version(), "{}", payload.type_name());
        }
    }

    #[test]
    fn empty_metadata_patch_serializes_to_an_empty_object() {
        let json = serde_json::to_string(&MetadataPatch::default()).expect("serialize");
        assert_eq!(json, "{}");
    }
}
