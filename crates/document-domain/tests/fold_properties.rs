//! Property tests for the fold.
//!
//! These generate *valid sequences* rather than arbitrary single events. An
//! arbitrary `DocumentDeleted` against `None` is legitimately a `FoldError`, so
//! asserting that any event applies to any state would contradict the fold's
//! contract instead of testing it.

use chrono::{DateTime, TimeZone, Utc};
use delphi_document_domain::{
    apply, Actor, DocumentBlobValidated, DocumentCreated, DocumentEvent, DocumentEventPayload,
    DocumentIndexed, DocumentStageFailed, DocumentTextExtracted, MetadataPatch,
    DOCUMENT_CONTRACT_VERSION,
};
use proptest::prelude::*;

const TENANT: &str = "acme";
const DOCUMENT: &str = "doc-1";

fn timestamp(offset: u32) -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + i64::from(offset), 0)
        .single()
        .expect("timestamp is unambiguous")
}

fn metadata_patch() -> impl Strategy<Value = MetadataPatch> {
    (
        proptest::option::of("[a-zA-Z ]{1,40}"),
        proptest::option::of(proptest::collection::vec("[a-z]{1,10}", 0..5)),
        proptest::option::of("[a-zA-Z ]{0,80}"),
        proptest::option::of(Just(serde_json::json!({ "source": "test" }))),
    )
        .prop_map(|(title, tags, description, metadata)| MetadataPatch {
            title,
            tags,
            description,
            metadata,
        })
}

/// A payload template, resolved into a real payload once its position in the
/// sequence (and therefore the document version) is known.
#[derive(Debug, Clone)]
enum Step {
    ReplaceBlob(MetadataPatch),
    ChangeMetadata(MetadataPatch),
    Extract,
    Index,
    StageFailed,
    Prune,
    Revert(MetadataPatch),
    Delete,
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        metadata_patch().prop_map(Step::ReplaceBlob),
        metadata_patch().prop_map(Step::ChangeMetadata),
        Just(Step::Extract),
        Just(Step::Index),
        Just(Step::StageFailed),
        Just(Step::Prune),
        metadata_patch().prop_map(Step::Revert),
        Just(Step::Delete),
    ]
}

/// A create followed by any number of well-formed follow-ups, with versions
/// assigned by the same rule the producers use.
fn valid_event_sequence() -> impl Strategy<Value = Vec<DocumentEvent>> {
    (metadata_patch(), proptest::collection::vec(step(), 0..12)).prop_map(|(patch, steps)| {
        let mut events = Vec::with_capacity(steps.len() + 1);
        let mut version = 1_u64;
        events.push(event(
            version,
            0,
            Actor::User {
                user_id: "user-1".to_owned(),
            },
            DocumentEventPayload::DocumentCreated(DocumentCreated {
                blob_ref: "upload-1".to_owned(),
                filename: "report.pdf".to_owned(),
                content_type: "application/pdf".to_owned(),
                byte_size: 1024,
                checksum: "sha256:aa".to_owned(),
                patch,
            }),
        ));

        for (offset, step) in steps.into_iter().enumerate() {
            let index = offset as u32 + 1;
            let (payload, advances) = match step {
                Step::ReplaceBlob(patch) => (
                    DocumentEventPayload::DocumentBlobValidated(DocumentBlobValidated {
                        blob_ref: format!("upload-{index}"),
                        filename: "report.pdf".to_owned(),
                        content_type: "application/pdf".to_owned(),
                        byte_size: 2048,
                        checksum: "sha256:bb".to_owned(),
                        patch,
                        based_on_version: Some(version),
                    }),
                    true,
                ),
                Step::ChangeMetadata(patch) => (
                    DocumentEventPayload::DocumentMetadataChanged { patch },
                    true,
                ),
                Step::Extract => (
                    DocumentEventPayload::DocumentTextExtracted(DocumentTextExtracted {
                        for_version: version,
                        extractor_version: "v1".to_owned(),
                        char_count: 128,
                        checksum: "sha256:cc".to_owned(),
                    }),
                    false,
                ),
                Step::Index => (
                    DocumentEventPayload::DocumentIndexed(DocumentIndexed {
                        for_version: version,
                        vector_count: 7,
                        embedding_model: "m".to_owned(),
                    }),
                    false,
                ),
                Step::StageFailed => (
                    DocumentEventPayload::DocumentStageFailed(DocumentStageFailed {
                        for_version: version,
                        stage: "index".to_owned(),
                        reason: "unavailable".to_owned(),
                        attempts: 3,
                    }),
                    false,
                ),
                Step::Prune => (
                    DocumentEventPayload::DocumentBlobPruned {
                        blob_ref: format!("upload-{index}-old"),
                        reason: "retention".to_owned(),
                    },
                    false,
                ),
                Step::Revert(patch) => (
                    DocumentEventPayload::DocumentReverted {
                        reverted_to: 1,
                        patch,
                    },
                    true,
                ),
                Step::Delete => (
                    DocumentEventPayload::DocumentDeleted {
                        reason: "user request".to_owned(),
                    },
                    true,
                ),
            };

            if advances {
                version += 1;
            }
            events.push(event(
                version,
                index,
                Actor::System {
                    component: "document-worker".to_owned(),
                },
                payload,
            ));
        }

        events
    })
}

fn event(version: u64, offset: u32, actor: Actor, payload: DocumentEventPayload) -> DocumentEvent {
    DocumentEvent {
        v: DOCUMENT_CONTRACT_VERSION,
        event_id: format!("event-{offset}"),
        tenant_id: TENANT.to_owned(),
        document_id: DOCUMENT.to_owned(),
        actor,
        version,
        ts: timestamp(offset),
        payload,
    }
}

proptest! {
    #[test]
    fn a_valid_sequence_folds_without_error_and_advances_monotonically(
        events in valid_event_sequence()
    ) {
        let mut state = None;
        let mut previous_version = 0_u64;

        for (offset, e) in events.iter().enumerate() {
            let seq = offset as u64 + 1;
            let next = apply(state, e, seq).expect("valid sequence must fold");

            prop_assert_eq!(next.stream_seq, seq);
            prop_assert_eq!(next.tenant_id.as_str(), TENANT);
            prop_assert_eq!(next.document_id.as_str(), DOCUMENT);
            prop_assert!(next.version >= previous_version);
            prop_assert_eq!(next.version, e.version);

            previous_version = next.version;
            state = Some(next);
        }

        let state = state.expect("a sequence always starts with a create");
        // The create names the user; later system events must not steal it.
        prop_assert_eq!(state.owner_user_id.as_str(), "user-1");
    }

    #[test]
    fn folding_is_deterministic(events in valid_event_sequence()) {
        let fold_all = || {
            let mut state = None;
            for (offset, e) in events.iter().enumerate() {
                state = Some(apply(state, e, offset as u64 + 1).expect("valid sequence must fold"));
            }
            state.expect("state")
        };
        prop_assert_eq!(fold_all(), fold_all());
    }

    #[test]
    fn a_serde_round_trip_folds_to_the_same_state(events in valid_event_sequence()) {
        let direct = {
            let mut state = None;
            for (offset, e) in events.iter().enumerate() {
                state = Some(apply(state, e, offset as u64 + 1).expect("fold"));
            }
            state.expect("state")
        };
        let round_tripped = {
            let mut state = None;
            for (offset, e) in events.iter().enumerate() {
                let json = serde_json::to_vec(e).expect("serialize");
                let decoded: DocumentEvent = serde_json::from_slice(&json).expect("deserialize");
                state = Some(apply(state, &decoded, offset as u64 + 1).expect("fold"));
            }
            state.expect("state")
        };
        prop_assert_eq!(direct, round_tripped);
    }
}
