//! The decision logic of the projection loop, separated from its transaction.
//!
//! The loop itself lives in the adapter, because "one transaction" means a
//! Postgres transaction. What belongs here is the rule that **neither a
//! deserialization failure nor a `FoldError` may stall the checkpoint**, and
//! that they are two distinct cases.

use delphi_document_domain::{apply, DocumentEvent, DocumentState};

/// A payload that either decoded or did not, with the raw JSON kept either way
/// so `projection_failure` always has something to store.
#[derive(Debug, Clone)]
pub enum DecodedEvent {
    Event(Box<DocumentEvent>),
    /// The payload is JSON but not a `DocumentEvent` we know. This is what lets
    /// a producer of a new event type deploy before its consumers.
    Unknown {
        raw: serde_json::Value,
        error: String,
    },
    /// Not even JSON.
    Malformed { error: String },
}

pub fn decode_event(payload: &[u8]) -> DecodedEvent {
    let raw: serde_json::Value = match serde_json::from_slice(payload) {
        Ok(raw) => raw,
        Err(error) => {
            return DecodedEvent::Malformed {
                error: error.to_string(),
            }
        }
    };
    match serde_json::from_value::<DocumentEvent>(raw.clone()) {
        Ok(event) => DecodedEvent::Event(Box::new(event)),
        Err(error) => DecodedEvent::Unknown {
            raw,
            error: error.to_string(),
        },
    }
}

#[derive(Debug, Clone)]
pub enum ProjectionOutcome {
    /// Fold succeeded; write this row and advance.
    Upsert(Box<DocumentState>),
    /// Record it and advance anyway. Because the projection is keyed per
    /// document, a hole affects one document rather than freezing the whole
    /// read model.
    Failure {
        payload: serde_json::Value,
        error: String,
        /// `true` for a `FoldError` — a genuine domain violation worth an
        /// alert — and `false` for an event this build simply does not know.
        domain_violation: bool,
    },
}

/// Decode and fold one event. Never returns an error: every input has an
/// outcome that advances the checkpoint.
pub fn project_event(
    prior: Option<DocumentState>,
    payload: &[u8],
    stream_seq: u64,
) -> ProjectionOutcome {
    match decode_event(payload) {
        DecodedEvent::Event(event) => match apply(prior, &event, stream_seq) {
            Ok(state) => ProjectionOutcome::Upsert(Box::new(state)),
            Err(error) => ProjectionOutcome::Failure {
                payload: serde_json::to_value(&*event).unwrap_or(serde_json::Value::Null),
                error: error.to_string(),
                domain_violation: true,
            },
        },
        DecodedEvent::Unknown { raw, error } => ProjectionOutcome::Failure {
            payload: raw,
            error,
            domain_violation: false,
        },
        DecodedEvent::Malformed { error } => ProjectionOutcome::Failure {
            payload: serde_json::Value::Null,
            error,
            domain_violation: false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::sample_created_event;

    #[test]
    fn a_known_event_folds() {
        let event = sample_created_event("acme", "doc-1", "upload-1");
        let payload = serde_json::to_vec(&event).expect("serialize");
        let outcome = project_event(None, &payload, 1);
        match outcome {
            ProjectionOutcome::Upsert(state) => {
                assert_eq!(state.document_id, "doc-1");
                assert_eq!(state.stream_seq, 1);
            }
            other => panic!("expected an upsert, got {other:?}"),
        }
    }

    #[test]
    fn an_event_type_this_build_does_not_know_is_recorded_not_a_domain_violation() {
        let payload = serde_json::to_vec(&serde_json::json!({
            "v": 1,
            "event_id": "e1",
            "tenant_id": "acme",
            "document_id": "doc-1",
            "actor": { "kind": "system", "component": "future-worker" },
            "version": 2,
            "ts": "2026-01-01T00:00:00Z",
            "payload": { "type": "document_summarised", "summary": "…" }
        }))
        .expect("serialize");

        match project_event(None, &payload, 7) {
            ProjectionOutcome::Failure {
                domain_violation,
                payload,
                ..
            } => {
                assert!(!domain_violation);
                // The raw payload survives so the row is inspectable later.
                assert_eq!(payload["payload"]["type"], "document_summarised");
            }
            other => panic!("expected a recorded failure, got {other:?}"),
        }
    }

    #[test]
    fn a_fold_error_is_recorded_as_a_domain_violation() {
        let mut event = sample_created_event("acme", "doc-1", "upload-1");
        event.actor = delphi_document_domain::Actor::System {
            component: "worker".to_owned(),
        };
        let payload = serde_json::to_vec(&event).expect("serialize");

        match project_event(None, &payload, 3) {
            ProjectionOutcome::Failure {
                domain_violation, ..
            } => assert!(domain_violation),
            other => panic!("expected a recorded failure, got {other:?}"),
        }
    }

    #[test]
    fn a_payload_that_is_not_json_still_advances() {
        match project_event(None, b"not json at all", 4) {
            ProjectionOutcome::Failure { payload, .. } => {
                assert_eq!(payload, serde_json::Value::Null);
            }
            other => panic!("expected a recorded failure, got {other:?}"),
        }
    }
}
