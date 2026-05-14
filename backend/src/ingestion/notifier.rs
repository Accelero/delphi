//! [`NotifyingSink`]: an [`IngestSink`] middleware that broadcasts a
//! [`FeedItemEvent`] after every successful first-time ingest.
//!
//! Composed around the canonical [`Pipeline`] in `api::serve`; both the
//! HTTP `/api/ingestion/documents` handler and the in-process scheduler
//! run requests through this same wrapped sink, so any new accepted
//! document — regardless of source — fans out to every subscribed
//! Discovery-feed SSE client.
//!
//! Only the `Created` outcome fires an event. `Unchanged` and
//! `Versioned` deliberately don't.
//!
//! Payload is a [`Document`] — the same shape `/api/discovery/feed`
//! returns. SPA receivers prepend it directly into the React Query
//! cache — no extra refetch, no risk of drift between SSE and feed
//! responses, because there's only one wire shape.
//!
//! Best-effort by design. The broadcast channel is bounded; if a slow
//! SSE client falls behind, it drops events rather than blocking
//! ingestion.
//!
//! Multi-tenancy: `FeedItemEvent` carries the `tenant_id` inside its
//! [`Document`] payload (under `document.tenant_id`). SSE handlers
//! filter their stream against the connection's tenant — closes audit
//! finding C2 (no cross-tenant event leakage).
//!
//! [`Pipeline`]: super::Pipeline

use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::error::Result;
use crate::storage::{Document, Storage};

use super::{IngestOutcome, IngestRequest, IngestSink};

/// Event payload broadcast by [`NotifyingSink`] on each `Created`
/// outcome. `document` is the same shape `/api/discovery/feed` emits —
/// clients prepend it into their cache directly.
#[derive(Debug, Clone, Serialize)]
pub struct FeedItemEvent {
    pub document: Document,
}

/// Default channel capacity. Sized so even a momentary ingest burst
/// (e.g. a backfill cycle) doesn't immediately push receivers into
/// `Lagged`. SSE handlers convert `Lagged` into a stream gap, not an
/// error.
pub const DEFAULT_BROADCAST_CAPACITY: usize = 256;

/// Wraps an inner [`IngestSink`] and broadcasts a [`FeedItemEvent`]
/// after every `Created` outcome. Needs the [`Storage`] handle to read
/// back the canonical `Document` (with engine-stamped `id` and
/// `ingested_at`) before broadcasting — this guarantees the SSE event
/// shape matches what `/api/discovery/feed` would return.
#[derive(Clone)]
pub struct NotifyingSink {
    inner: Arc<dyn IngestSink>,
    storage: Arc<dyn Storage>,
    tx: broadcast::Sender<FeedItemEvent>,
}

impl NotifyingSink {
    pub fn new(
        inner: Arc<dyn IngestSink>,
        storage: Arc<dyn Storage>,
        tx: broadcast::Sender<FeedItemEvent>,
    ) -> Self {
        Self { inner, storage, tx }
    }
}

#[async_trait]
impl IngestSink for NotifyingSink {
    async fn ingest(&self, req: IngestRequest) -> Result<IngestOutcome> {
        // Snapshot tenant before `req` moves into the inner sink — we
        // need it to read the doc back below.
        let tenant_id = req.tenant_id.clone();

        let outcome = self.inner.ingest(req).await?;

        if let IngestOutcome::Created { id, .. } = &outcome {
            // Read back the canonical row so the broadcast carries the
            // engine-stamped `id` and `ingested_at`. Cost: one extra
            // small read on first-time ingests only (Unchanged /
            // Versioned skip this branch).
            match self.storage.get_document(&tenant_id, id).await {
                Ok(Some(document)) => {
                    // SendError just means "no subscribers" — fine in
                    // headless deployments and in tests with no SSE
                    // client connected.
                    let _ = self.tx.send(FeedItemEvent { document });
                }
                Ok(None) => {
                    // Race against an external delete between upsert
                    // and read. Skip the broadcast — the doc isn't
                    // there to show.
                    tracing::warn!(
                        ?id,
                        "ingested document vanished before broadcast read-back"
                    );
                }
                Err(e) => {
                    // Read failures shouldn't kill ingest; just skip the
                    // SSE fan-out for this row.
                    tracing::warn!(error = %e, ?id, "broadcast read-back failed");
                }
            }
        }

        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::Pipeline;
    use crate::storage::SystemDb;
    use surrealdb::RecordId;

    async fn fresh_sink() -> (NotifyingSink, broadcast::Receiver<FeedItemEvent>, RecordId) {
        let system = SystemDb::in_memory("notifier_test", "main")
            .await
            .unwrap();
        system.init_schema().await.unwrap();
        let mut r = system
            .raw()
            .query("CREATE tenant CONTENT { slug: 'test', name: 'Test' } RETURN id")
            .await
            .unwrap();
        #[derive(serde::Deserialize)]
        struct IdRow {
            id: RecordId,
        }
        let row: Option<IdRow> = r.take(0).unwrap();
        let tenant = row.unwrap().id;

        let storage: Arc<dyn Storage> = system.storage();
        let inner: Arc<dyn IngestSink> = Arc::new(Pipeline::new(storage.clone()));
        let (tx, rx) = broadcast::channel(8);
        (NotifyingSink::new(inner, storage, tx), rx, tenant)
    }

    fn req(tenant: &RecordId, canonical_id: &str) -> IngestRequest {
        IngestRequest {
            tenant_id: tenant.clone(),
            canonical_id: canonical_id.into(),
            source_type: "test".into(),
            source_uri: format!("https://test/{canonical_id}"),
            title: Some(format!("Title {canonical_id}")),
            authors: vec![],
            published_at: None,
            language: None,
            summary: None,
            raw_text: None,
            storage_uri: None,
            metadata: serde_json::Value::Null,
        }
    }

    #[tokio::test]
    async fn created_outcome_broadcasts_event() {
        let (sink, mut rx, t) = fresh_sink().await;
        sink.ingest(req(&t, "doc-1")).await.unwrap();
        let event = rx.try_recv().expect("event delivered");
        assert_eq!(event.document.canonical_id, "doc-1");
        assert_eq!(event.document.source_type, "test");
        assert_eq!(event.document.tenant_id, t);
        assert!(event.document.id.is_some(), "id stamped from DB");
        assert!(event.document.ingested_at.is_some(), "ingested_at stamped from DB");
    }

    #[tokio::test]
    async fn unchanged_outcome_does_not_broadcast() {
        let (sink, mut rx, t) = fresh_sink().await;
        sink.ingest(req(&t, "doc-1")).await.unwrap();
        let _ = rx.try_recv().unwrap();

        sink.ingest(req(&t, "doc-1")).await.unwrap();
        assert!(
            matches!(rx.try_recv(), Err(broadcast::error::TryRecvError::Empty)),
            "no event expected on Unchanged outcome"
        );
    }

    #[tokio::test]
    async fn no_subscribers_is_not_an_error() {
        let (sink, rx, t) = fresh_sink().await;
        drop(rx);
        sink.ingest(req(&t, "doc-1")).await.unwrap();
    }
}
