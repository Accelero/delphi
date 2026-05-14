//! [`NotifyingSink`]: an [`IngestSink`] middleware that broadcasts a
//! [`FeedItemEvent`] after every successful first-time ingest.
//!
//! Built **per request** in the HTTP ingestion handler, wrapping the
//! per-request [`Pipeline`] off the request's `AuthedDb`. The broadcast
//! channel itself is process-global (lives in `AppState`); SSE
//! subscribers filter on receive against the connecting user's tenant.
//!
//! Only the `Created` outcome fires an event. `Unchanged` and
//! `Versioned` deliberately don't.
//!
//! Payload is a [`Document`] — the same shape `/api/discovery/feed`
//! returns. SPA receivers prepend it directly into the React Query
//! cache.
//!
//! Best-effort by design. The broadcast channel is bounded; if a slow
//! SSE client falls behind, it drops events rather than blocking
//! ingestion.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::error::Result;
use crate::storage::{Document, Storage};

use super::{IngestOutcome, IngestRequest, IngestSink};

/// Event payload broadcast by [`NotifyingSink`] on each `Created`
/// outcome.
#[derive(Debug, Clone, Serialize)]
pub struct FeedItemEvent {
    pub document: Document,
}

/// Default channel capacity.
pub const DEFAULT_BROADCAST_CAPACITY: usize = 256;

/// Wraps an inner [`IngestSink`] and broadcasts a [`FeedItemEvent`]
/// after every `Created` outcome. The same `Storage` handle is used to
/// read back the canonical row (with engine-stamped `id`,
/// `ingested_at`, and `tenant_id`) so the SSE event shape matches
/// `/api/discovery/feed`.
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
        let outcome = self.inner.ingest(req).await?;

        if let IngestOutcome::Created { id, .. } = &outcome {
            match self.storage.get_document(id).await {
                Ok(Some(document)) => {
                    let _ = self.tx.send(FeedItemEvent { document });
                }
                Ok(None) => {
                    tracing::warn!(
                        ?id,
                        "ingested document vanished before broadcast read-back"
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, ?id, "broadcast read-back failed");
                }
            }
        }

        Ok(outcome)
    }
}
