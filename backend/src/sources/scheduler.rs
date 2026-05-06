use std::sync::Arc;

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::interval;

use crate::error::Result;
use crate::filter::{Decision, IngestFilter};
use crate::ingestion::IngestSink;
use crate::storage::Storage;

use super::{AdapterRegistry, SourceAdapter};

/// Returned by [`run_scheduler`]. Call `.shutdown().await` to stop all
/// adapter tasks. Holding it alive keeps the scheduler running.
pub struct SchedulerHandle {
    shutdown: Arc<Notify>,
    handles: Vec<JoinHandle<()>>,
}

impl SchedulerHandle {
    pub async fn shutdown(self) {
        self.shutdown.notify_waiters();
        for h in self.handles {
            let _ = h.await;
        }
    }
}

/// Spawn one task per adapter and return a handle for shutdown.
///
/// Each task ticks on `adapter.poll_interval()`, calls
/// `adapter.fetch(cursor)`, hands every returned `IngestRequest` to
/// the filter, and on `Accept` funnels it through `sink.ingest`. The
/// filter is the *only* thing on this path that the HTTP `POST
/// /api/ingestion/documents` handler doesn't run — manual pushes
/// bypass filtering by design.
///
/// Errors at any stage are logged and the task continues — no DLQ in
/// slice 2.
///
/// Note: `tokio::time::interval` fires its first tick immediately, so
/// adapters poll right at startup (the desired behaviour after a
/// restart).
pub fn run_scheduler(
    sink: Arc<dyn IngestSink>,
    filter: Arc<dyn IngestFilter>,
    storage: Arc<dyn Storage>,
    registry: AdapterRegistry,
) -> SchedulerHandle {
    let shutdown = Arc::new(Notify::new());
    let mut handles = Vec::new();
    for adapter in registry.into_inner() {
        let sink = sink.clone();
        let filter = filter.clone();
        let storage = storage.clone();
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(adapter_loop(
            adapter, sink, filter, storage, shutdown,
        )));
    }
    SchedulerHandle { shutdown, handles }
}

async fn adapter_loop(
    adapter: Arc<dyn SourceAdapter>,
    sink: Arc<dyn IngestSink>,
    filter: Arc<dyn IngestFilter>,
    storage: Arc<dyn Storage>,
    shutdown: Arc<Notify>,
) {
    let name = adapter.name().to_string();
    let mut ticker = interval(adapter.poll_interval());

    loop {
        tokio::select! {
            biased;
            _ = shutdown.notified() => {
                tracing::info!(adapter = %name, "scheduler shutdown");
                return;
            }
            _ = ticker.tick() => {
                if let Err(e) = run_once(
                    adapter.as_ref(),
                    sink.as_ref(),
                    filter.as_ref(),
                    storage.as_ref(),
                )
                .await
                {
                    tracing::error!(adapter = %name, error = %e, "adapter cycle failed");
                }
            }
        }
    }
}

async fn run_once(
    adapter: &dyn SourceAdapter,
    sink: &dyn IngestSink,
    filter: &dyn IngestFilter,
    storage: &dyn Storage,
) -> Result<()> {
    let cursor = storage.get_source_cursor(adapter.name()).await?;
    let fetched = adapter.fetch(cursor).await?;

    let total = fetched.items.len();
    let mut accepted = 0usize;
    let mut rejected = 0usize;

    for item in fetched.items {
        match filter.evaluate(&item).await {
            Decision::Accept => {
                accepted += 1;
                match sink.ingest(item).await {
                    Ok(outcome) => {
                        tracing::debug!(adapter = adapter.name(), ?outcome, "ingested")
                    }
                    Err(e) => {
                        tracing::error!(adapter = adapter.name(), error = %e, "ingest failed")
                    }
                }
            }
            Decision::Reject { reason } => {
                rejected += 1;
                tracing::info!(
                    adapter = adapter.name(),
                    canonical_id = %item.canonical_id,
                    %reason,
                    "filter rejected document"
                );
            }
        }
    }

    if let Some(c) = fetched.next_cursor {
        storage.put_source_cursor(adapter.name(), &c).await?;
    }
    tracing::info!(
        adapter = adapter.name(),
        total,
        accepted,
        rejected,
        "adapter cycle complete"
    );
    Ok(())
}
