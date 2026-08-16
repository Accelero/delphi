//! The two append paths.
//!
//! **Do not merge these into one helper.** A blind retry on a create conflict
//! produces a second `DocumentCreated`; on a create, the conflict *is* the
//! answer.

use delphi_document_domain::DocumentEvent;

use crate::errors::{AppendError, EventStoreError};
use crate::ports::{Appended, EventStore, Expect};

/// How many times an update re-reads and re-CASes before giving up.
const UPDATE_ATTEMPTS: u32 = 3;

/// Append the first event for a document.
///
/// Never retried. `Expect::CreateOnly` means the subject must be empty; a
/// conflict means someone already created it, which on the worker's create path
/// is a redelivery of work that already succeeded.
pub async fn append_create(
    store: &dyn EventStore,
    event: DocumentEvent,
) -> Result<Appended, AppendError> {
    let tenant = event.tenant_id.clone();
    let document_id = event.document_id.clone();

    match store.append(event, Expect::CreateOnly).await {
        Ok(appended) => resolve_duplicate(store, &tenant, &document_id, appended).await,
        Err(EventStoreError::Conflict) => Err(AppendError::AlreadyCreated),
        Err(error) => Err(AppendError::Store(error)),
    }
}

/// Append a subsequent event, with two checks that do different jobs.
///
/// The **version check** rejects a client acting on a stale document. The
/// **sequence CAS** protects the window between reading the current sequence
/// and appending. The retry is what makes them compose: an event that moves
/// `stream_seq` without moving `version` — an index or extraction result —
/// retries transparently instead of surfacing as a spurious conflict.
///
/// `client_version` is the caller's `if_match`. Pass `None` when the caller has
/// already resolved the conflict itself (the supersede path does).
pub async fn append_update<F>(
    store: &dyn EventStore,
    tenant: &str,
    document_id: &str,
    client_version: Option<u64>,
    build_event: F,
) -> Result<Appended, AppendError>
where
    F: Fn(u64) -> DocumentEvent,
{
    for _ in 0..UPDATE_ATTEMPTS {
        let (current_version, current_seq) = store
            .last(tenant, document_id)
            .await?
            .ok_or(AppendError::NoSuchDocument)?;

        if let Some(client) = client_version {
            if client != current_version {
                return Err(AppendError::VersionMismatch {
                    client,
                    current: current_version,
                });
            }
        }

        let event = build_event(current_version + 1);
        match store.append(event, Expect::Exactly(current_seq)).await {
            Ok(appended) => {
                return resolve_duplicate(store, tenant, document_id, appended).await;
            }
            // Something landed in the window. Re-read and try again.
            Err(EventStoreError::Conflict) => continue,
            Err(error) => return Err(AppendError::Store(error)),
        }
    }

    Err(AppendError::ConflictRetryExhausted)
}

/// On a dedupe the store returns the *original* sequence and never evaluated
/// the expected-sequence header, so the version we computed locally may be
/// wrong. Re-read the authoritative pair.
async fn resolve_duplicate(
    store: &dyn EventStore,
    tenant: &str,
    document_id: &str,
    appended: Appended,
) -> Result<Appended, AppendError> {
    if !appended.duplicate {
        return Ok(appended);
    }
    let (version, stream_seq) = store
        .last(tenant, document_id)
        .await?
        .ok_or(AppendError::NoSuchDocument)?;
    Ok(Appended {
        stream_seq,
        version,
        duplicate: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{sample_created_event, MemoryEventStore};
    use std::sync::Arc;

    fn store() -> Arc<MemoryEventStore> {
        Arc::new(MemoryEventStore::default())
    }

    #[tokio::test]
    async fn a_create_succeeds_once_and_reports_already_created_after() {
        let store = store();
        let event = sample_created_event("acme", "doc-1", "upload-1");

        let first = append_create(store.as_ref(), event.clone())
            .await
            .expect("first create");
        assert_eq!(first.version, 1);

        // A second create with a *different* event id skips dedupe and hits the
        // expected-sequence check instead — the case dedupe cannot cover.
        let mut other = event.clone();
        other.event_id = "different".to_owned();
        let second = append_create(store.as_ref(), other).await;
        assert!(matches!(second, Err(AppendError::AlreadyCreated)));
    }

    #[tokio::test]
    async fn a_redelivered_create_dedupes_and_reports_the_original_version() {
        let store = store();
        let event = sample_created_event("acme", "doc-1", "upload-1");

        append_create(store.as_ref(), event.clone())
            .await
            .expect("first create");
        let again = append_create(store.as_ref(), event)
            .await
            .expect("dedupe is success");

        assert!(again.duplicate);
        assert_eq!(again.version, 1);
        assert_eq!(store.event_count("acme", "doc-1"), 1);
    }

    #[tokio::test]
    async fn an_update_with_a_stale_client_version_conflicts_without_appending() {
        let store = store();
        append_create(
            store.as_ref(),
            sample_created_event("acme", "doc-1", "upload-1"),
        )
        .await
        .expect("create");

        let result = append_update(store.as_ref(), "acme", "doc-1", Some(5), |version| {
            crate::testing::sample_validated_event("acme", "doc-1", "upload-2", version)
        })
        .await;

        assert!(matches!(
            result,
            Err(AppendError::VersionMismatch {
                client: 5,
                current: 1
            })
        ));
        assert_eq!(store.event_count("acme", "doc-1"), 1);
    }

    #[tokio::test]
    async fn an_update_retries_past_an_event_that_moved_the_sequence_only() {
        let store = store();
        append_create(
            store.as_ref(),
            sample_created_event("acme", "doc-1", "upload-1"),
        )
        .await
        .expect("create");

        // Simulate an indexing result landing between the read and the append:
        // it moves stream_seq but not version, so the CAS fails once and the
        // retry must succeed with the same client version.
        store.inject_conflict_once();

        let appended = append_update(store.as_ref(), "acme", "doc-1", Some(1), |version| {
            crate::testing::sample_validated_event("acme", "doc-1", "upload-2", version)
        })
        .await
        .expect("retry succeeds");

        assert_eq!(appended.version, 2);
    }

    #[tokio::test]
    async fn an_update_gives_up_after_repeated_conflicts() {
        let store = store();
        append_create(
            store.as_ref(),
            sample_created_event("acme", "doc-1", "upload-1"),
        )
        .await
        .expect("create");
        store.always_conflict();

        let result = append_update(store.as_ref(), "acme", "doc-1", None, |version| {
            crate::testing::sample_validated_event("acme", "doc-1", "upload-2", version)
        })
        .await;

        assert!(matches!(result, Err(AppendError::ConflictRetryExhausted)));
    }

    #[tokio::test]
    async fn updating_a_document_that_does_not_exist_is_not_a_create() {
        let store = store();
        let result = append_update(store.as_ref(), "acme", "ghost", None, |version| {
            crate::testing::sample_validated_event("acme", "ghost", "upload-2", version)
        })
        .await;

        assert!(matches!(result, Err(AppendError::NoSuchDocument)));
        assert_eq!(store.event_count("acme", "ghost"), 0);
    }
}
