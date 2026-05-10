//! [`NotifyingSink`]: an [`IngestSink`] middleware that broadcasts a
//! `NewDocumentEvent` after every successful first-time ingest.
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
//! Best-effort by design. The broadcast channel is bounded; if a slow
//! SSE client falls behind, it drops events rather than blocking
//! ingestion.
//!
//! Multi-tenancy: `NewDocumentEvent` carries `tenant_id`. SSE handlers
//! filter their stream against the connection's tenant — closes audit
//! finding C2 (no cross-tenant event leakage).
//!
//! [`Pipeline`]: super::Pipeline

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::RecordId;
use tokio::sync::broadcast;

use crate::error::Result;
use crate::storage::DocId;

use super::{IngestOutcome, IngestRequest, IngestSink};

/// Event payload broadcast by [`NotifyingSink`] on each `Created`
/// outcome. Consumers (the SSE endpoint) re-serialize to JSON for the
/// wire — `tenant_id` stays in the payload so the consumer can filter
/// on it before sending to a client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewDocumentEvent {
    pub id: DocId,
    pub tenant_id: RecordId,
    pub canonical_id: String,
    pub source_type: String,
    pub title: Option<String>,
    pub ingested_at: DateTime<Utc>,
}

/// Default channel capacity. Sized so even a momentary ingest burst
/// (e.g. a backfill cycle) doesn't immediately push receivers into
/// `Lagged`. SSE handlers convert `Lagged` into a stream gap, not an
/// error.
pub const DEFAULT_BROADCAST_CAPACITY: usize = 256;

/// Wraps an inner [`IngestSink`] and broadcasts a [`NewDocumentEvent`]
/// after every `Created` outcome.
#[derive(Clone)]
pub struct NotifyingSink {
    inner: Arc<dyn IngestSink>,
    tx: broadcast::Sender<NewDocumentEvent>,
}

impl NotifyingSink {
    pub fn new(inner: Arc<dyn IngestSink>, tx: broadcast::Sender<NewDocumentEvent>) -> Self {
        Self { inner, tx }
    }
}

#[async_trait]
impl IngestSink for NotifyingSink {
    async fn ingest(&self, req: IngestRequest) -> Result<IngestOutcome> {
        // Snapshot the bits we need for the event before `req` moves
        // into the inner sink.
        let tenant_id = req.tenant_id.clone();
        let canonical_id = req.canonical_id.clone();
        let source_type = req.source_type.clone();
        let title = req.title.clone();

        let outcome = self.inner.ingest(req).await?;

        if let IngestOutcome::Created { id, .. } = &outcome {
            let event = NewDocumentEvent {
                id: id.clone(),
                tenant_id,
                canonical_id,
                source_type,
                title,
                ingested_at: Utc::now(),
            };
            // SendError just means "no subscribers" — fine in headless
            // deployments and in tests with no SSE client connected.
            let _ = self.tx.send(event);
        }

        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::Pipeline;
    use crate::storage::{Storage, SystemDb};

    async fn fresh_sink() -> (NotifyingSink, broadcast::Receiver<NewDocumentEvent>, RecordId) {
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
        let inner: Arc<dyn IngestSink> = Arc::new(Pipeline::new(storage));
        let (tx, rx) = broadcast::channel(8);
        (NotifyingSink::new(inner, tx), rx, tenant)
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
        assert_eq!(event.canonical_id, "doc-1");
        assert_eq!(event.source_type, "test");
        assert_eq!(event.tenant_id, t);
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
