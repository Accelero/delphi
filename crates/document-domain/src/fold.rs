use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::events::{
    Actor, DocumentEvent, DocumentEventPayload, MetadataPatch, INDEX_STAGE,
};

/// The folded state of one document. This is exactly what the `document` table
/// holds; the table is a cache of this and can be rebuilt from the log.
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentState {
    pub tenant_id: String,
    pub document_id: String,
    pub owner_user_id: String,
    pub version: u64,
    /// JetStream sequence of the last event folded into this state. Strictly
    /// increasing, which is what makes it a safe monotonic guard for the
    /// projection upsert (`version` is not: several event types repeat it).
    pub stream_seq: u64,
    pub state: DocState,
    pub index_state: IndexState,
    pub index_version: Option<u64>,
    pub current_blob: Option<String>,
    pub filename: Option<String>,
    pub content_type: Option<String>,
    pub byte_size: Option<u64>,
    pub checksum: Option<String>,
    pub title: Option<String>,
    pub tags: Vec<String>,
    pub description: Option<String>,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Lifecycle, deliberately separate from `index_state`: a document being
/// re-indexed is still fully usable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocState {
    Active,
    Deleted,
}

impl DocState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Deleted => "deleted",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "active" => Some(Self::Active),
            "deleted" => Some(Self::Deleted),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    Pending,
    Current,
    Failed,
}

impl IndexState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Current => "current",
            Self::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "current" => Some(Self::Current),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// Reserved for genuinely impossible input. Anything merely *unrecognised*
/// fails earlier, at deserialization, and is not a fold error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FoldError {
    #[error("event {event_type} for document {document_id} arrived with no prior state")]
    NoPriorState {
        document_id: String,
        event_type: &'static str,
    },
    #[error("document {document_id} was created by a system actor; a create must name a user")]
    SystemActorCreate { document_id: String },
    #[error("event for document {actual} folded into state for {expected}")]
    DocumentMismatch { expected: String, actual: String },
}

/// Fold one event into the state. Deterministic: the same events in the same
/// order always produce the same state.
pub fn apply(
    state: Option<DocumentState>,
    event: &DocumentEvent,
    stream_seq: u64,
) -> Result<DocumentState, FoldError> {
    if let Some(existing) = &state {
        if existing.document_id != event.document_id {
            return Err(FoldError::DocumentMismatch {
                expected: existing.document_id.clone(),
                actual: event.document_id.clone(),
            });
        }
    }

    match &event.payload {
        DocumentEventPayload::DocumentCreated(created) => {
            let Actor::User { user_id } = &event.actor else {
                return Err(FoldError::SystemActorCreate {
                    document_id: event.document_id.clone(),
                });
            };
            // A create against existing state is a redelivery the event store
            // should already have rejected. Folding it is still deterministic:
            // it re-seeds the same fields.
            let created_at = state
                .as_ref()
                .map(|s| s.created_at)
                .unwrap_or(event.ts);
            let mut next = DocumentState {
                tenant_id: event.tenant_id.clone(),
                document_id: event.document_id.clone(),
                owner_user_id: user_id.clone(),
                version: event.version,
                stream_seq,
                state: DocState::Active,
                index_state: IndexState::Pending,
                index_version: None,
                current_blob: Some(created.blob_ref.clone()),
                filename: Some(created.filename.clone()),
                content_type: Some(created.content_type.clone()),
                byte_size: Some(created.byte_size),
                checksum: Some(created.checksum.clone()),
                title: None,
                tags: Vec::new(),
                description: None,
                metadata: serde_json::Value::Object(serde_json::Map::new()),
                created_at,
                updated_at: event.ts,
            };
            merge_patch(&mut next, &created.patch);
            Ok(next)
        }
        payload => {
            let mut next = state.ok_or_else(|| FoldError::NoPriorState {
                document_id: event.document_id.clone(),
                event_type: payload.type_name(),
            })?;
            next.version = event.version;
            next.stream_seq = stream_seq;
            next.updated_at = event.ts;

            match payload {
                DocumentEventPayload::DocumentCreated(_) => unreachable!("handled above"),
                DocumentEventPayload::DocumentBlobValidated(validated) => {
                    next.current_blob = Some(validated.blob_ref.clone());
                    next.filename = Some(validated.filename.clone());
                    next.content_type = Some(validated.content_type.clone());
                    next.byte_size = Some(validated.byte_size);
                    next.checksum = Some(validated.checksum.clone());
                    // New bytes invalidate whatever was indexed for the old
                    // ones. The index catches up; the document stays usable.
                    next.index_state = IndexState::Pending;
                    merge_patch(&mut next, &validated.patch);
                }
                DocumentEventPayload::DocumentMetadataChanged { patch } => {
                    merge_patch(&mut next, patch);
                }
                DocumentEventPayload::DocumentReverted { patch, .. } => {
                    merge_patch(&mut next, patch);
                }
                DocumentEventPayload::DocumentDeleted { .. } => {
                    next.state = DocState::Deleted;
                }
                DocumentEventPayload::DocumentIndexed(indexed) => {
                    next.index_version = Some(indexed.for_version);
                    // An index built for an older version is stale the moment
                    // a newer version exists.
                    next.index_state = if indexed.for_version >= next.version {
                        IndexState::Current
                    } else {
                        IndexState::Pending
                    };
                }
                DocumentEventPayload::DocumentStageFailed(failed) => {
                    if failed.stage == INDEX_STAGE {
                        next.index_state = IndexState::Failed;
                    }
                }
                // Text extraction and blob pruning have no projected column in
                // this slice; they only mark the document as touched.
                DocumentEventPayload::DocumentTextExtracted(_)
                | DocumentEventPayload::DocumentBlobPruned { .. } => {}
            }

            Ok(next)
        }
    }
}

fn merge_patch(state: &mut DocumentState, patch: &MetadataPatch) {
    if let Some(title) = &patch.title {
        state.title = Some(title.clone());
    }
    if let Some(tags) = &patch.tags {
        state.tags = tags.clone();
    }
    if let Some(description) = &patch.description {
        state.description = Some(description.clone());
    }
    if let Some(metadata) = &patch.metadata {
        state.metadata = metadata.clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{
        DocumentBlobValidated, DocumentCreated, DocumentIndexed, DocumentStageFailed,
        DocumentTextExtracted, DOCUMENT_CONTRACT_VERSION,
    };
    use chrono::TimeZone;

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
    }

    fn event(version: u64, payload: DocumentEventPayload) -> DocumentEvent {
        DocumentEvent {
            v: DOCUMENT_CONTRACT_VERSION,
            event_id: format!("e{version}"),
            tenant_id: "acme".to_owned(),
            document_id: "doc-1".to_owned(),
            actor: Actor::User {
                user_id: "user-1".to_owned(),
            },
            version,
            ts: ts(version as i64),
            payload,
        }
    }

    fn created(patch: MetadataPatch) -> DocumentEventPayload {
        DocumentEventPayload::DocumentCreated(DocumentCreated {
            blob_ref: "upload-1".to_owned(),
            filename: "report.pdf".to_owned(),
            content_type: "application/pdf".to_owned(),
            byte_size: 1024,
            checksum: "sha256:aa".to_owned(),
            patch,
        })
    }

    fn validated(blob: &str, patch: MetadataPatch) -> DocumentEventPayload {
        DocumentEventPayload::DocumentBlobValidated(DocumentBlobValidated {
            blob_ref: blob.to_owned(),
            filename: "report-v2.pdf".to_owned(),
            content_type: "application/pdf".to_owned(),
            byte_size: 2048,
            checksum: "sha256:bb".to_owned(),
            patch,
            based_on_version: Some(1),
        })
    }

    #[test]
    fn create_seeds_owner_from_the_user_actor() {
        let state = apply(None, &event(1, created(MetadataPatch::default())), 1).expect("fold");
        assert_eq!(state.owner_user_id, "user-1");
        assert_eq!(state.version, 1);
        assert_eq!(state.stream_seq, 1);
        assert_eq!(state.state, DocState::Active);
        assert_eq!(state.index_state, IndexState::Pending);
        assert_eq!(state.current_blob.as_deref(), Some("upload-1"));
        assert_eq!(state.created_at, state.updated_at);
        assert!(state.tags.is_empty());
    }

    #[test]
    fn create_by_a_system_actor_is_a_fold_error() {
        let mut e = event(1, created(MetadataPatch::default()));
        e.actor = Actor::System {
            component: "worker".to_owned(),
        };
        assert!(matches!(
            apply(None, &e, 1),
            Err(FoldError::SystemActorCreate { .. })
        ));
    }

    #[test]
    fn a_non_create_event_with_no_prior_state_is_a_fold_error() {
        let e = event(
            2,
            DocumentEventPayload::DocumentDeleted {
                reason: "gone".to_owned(),
            },
        );
        assert!(matches!(
            apply(None, &e, 1),
            Err(FoldError::NoPriorState { .. })
        ));
    }

    #[test]
    fn an_event_for_a_different_document_is_a_fold_error() {
        let state = apply(None, &event(1, created(MetadataPatch::default())), 1).expect("fold");
        let mut e = event(
            2,
            DocumentEventPayload::DocumentDeleted {
                reason: "gone".to_owned(),
            },
        );
        e.document_id = "doc-2".to_owned();
        assert!(matches!(
            apply(Some(state), &e, 2),
            Err(FoldError::DocumentMismatch { .. })
        ));
    }

    #[test]
    fn metadata_patch_is_partial() {
        let state = apply(
            None,
            &event(
                1,
                created(MetadataPatch {
                    title: Some("Annual Report".to_owned()),
                    tags: Some(vec!["finance".to_owned()]),
                    description: Some("desc".to_owned()),
                    metadata: Some(serde_json::json!({ "k": "v" })),
                }),
            ),
            1,
        )
        .expect("fold");

        // Only `title` is Some, so the other three must survive untouched.
        let next = apply(
            Some(state),
            &event(
                2,
                DocumentEventPayload::DocumentMetadataChanged {
                    patch: MetadataPatch {
                        title: Some("Annual Report 2026".to_owned()),
                        ..Default::default()
                    },
                },
            ),
            2,
        )
        .expect("fold");

        assert_eq!(next.title.as_deref(), Some("Annual Report 2026"));
        assert_eq!(next.tags, vec!["finance".to_owned()]);
        assert_eq!(next.description.as_deref(), Some("desc"));
        assert_eq!(next.metadata, serde_json::json!({ "k": "v" }));
    }

    #[test]
    fn replacing_the_blob_advances_content_and_resets_the_index() {
        let state = apply(None, &event(1, created(MetadataPatch::default())), 1).expect("fold");
        let state = apply(
            Some(state),
            &event(
                1,
                DocumentEventPayload::DocumentIndexed(DocumentIndexed {
                    for_version: 1,
                    vector_count: 12,
                    embedding_model: "m".to_owned(),
                }),
            ),
            2,
        )
        .expect("fold");
        assert_eq!(state.index_state, IndexState::Current);

        let state = apply(
            Some(state),
            &event(2, validated("upload-2", MetadataPatch::default())),
            3,
        )
        .expect("fold");

        assert_eq!(state.version, 2);
        assert_eq!(state.current_blob.as_deref(), Some("upload-2"));
        assert_eq!(state.byte_size, Some(2048));
        assert_eq!(state.index_state, IndexState::Pending);
        assert_eq!(state.index_version, Some(1));
    }

    #[test]
    fn an_index_for_an_older_version_leaves_the_index_pending() {
        let state = apply(None, &event(1, created(MetadataPatch::default())), 1).expect("fold");
        let state = apply(
            Some(state),
            &event(2, validated("upload-2", MetadataPatch::default())),
            2,
        )
        .expect("fold");
        // Indexing finished for v1 after v2 landed: still stale.
        let state = apply(
            Some(state),
            &event(
                2,
                DocumentEventPayload::DocumentIndexed(DocumentIndexed {
                    for_version: 1,
                    vector_count: 12,
                    embedding_model: "m".to_owned(),
                }),
            ),
            3,
        )
        .expect("fold");
        assert_eq!(state.index_state, IndexState::Pending);
    }

    #[test]
    fn only_an_index_stage_failure_marks_the_index_failed() {
        let state = apply(None, &event(1, created(MetadataPatch::default())), 1).expect("fold");

        let other = apply(
            Some(state.clone()),
            &event(
                1,
                DocumentEventPayload::DocumentStageFailed(DocumentStageFailed {
                    for_version: 1,
                    stage: "extract".to_owned(),
                    reason: "bad pdf".to_owned(),
                    attempts: 3,
                }),
            ),
            2,
        )
        .expect("fold");
        assert_eq!(other.index_state, IndexState::Pending);

        let indexed = apply(
            Some(state),
            &event(
                1,
                DocumentEventPayload::DocumentStageFailed(DocumentStageFailed {
                    for_version: 1,
                    stage: INDEX_STAGE.to_owned(),
                    reason: "embeddings down".to_owned(),
                    attempts: 5,
                }),
            ),
            2,
        )
        .expect("fold");
        assert_eq!(indexed.index_state, IndexState::Failed);
    }

    #[test]
    fn deferred_event_types_all_fold() {
        let mut state = apply(None, &event(1, created(MetadataPatch::default())), 1)
            .expect("fold");
        let deferred = [
            DocumentEventPayload::DocumentTextExtracted(DocumentTextExtracted {
                for_version: 1,
                extractor_version: "v1".to_owned(),
                char_count: 100,
                checksum: "sha256:cc".to_owned(),
            }),
            DocumentEventPayload::DocumentBlobPruned {
                blob_ref: "upload-0".to_owned(),
                reason: "retention".to_owned(),
            },
            DocumentEventPayload::DocumentReverted {
                reverted_to: 1,
                patch: MetadataPatch {
                    title: Some("Back To v1".to_owned()),
                    ..Default::default()
                },
            },
            DocumentEventPayload::DocumentDeleted {
                reason: "user request".to_owned(),
            },
        ];
        for (offset, payload) in deferred.into_iter().enumerate() {
            let seq = offset as u64 + 2;
            let version = if payload.advances_version() {
                state.version + 1
            } else {
                state.version
            };
            state = apply(Some(state), &event(version, payload), seq).expect("fold");
        }
        assert_eq!(state.state, DocState::Deleted);
        assert_eq!(state.title.as_deref(), Some("Back To v1"));
    }

    #[test]
    fn folding_the_same_events_twice_produces_identical_state() {
        let events = [
            event(
                1,
                created(MetadataPatch {
                    title: Some("t".to_owned()),
                    ..Default::default()
                }),
            ),
            event(2, validated("upload-2", MetadataPatch::default())),
            event(
                2,
                DocumentEventPayload::DocumentIndexed(DocumentIndexed {
                    for_version: 2,
                    vector_count: 5,
                    embedding_model: "m".to_owned(),
                }),
            ),
        ];
        let fold_all = || {
            let mut state = None;
            for (offset, e) in events.iter().enumerate() {
                state = Some(apply(state, e, offset as u64 + 1).expect("fold"));
            }
            state.expect("state")
        };
        assert_eq!(fold_all(), fold_all());
    }
}
