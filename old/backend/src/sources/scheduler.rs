use std::sync::Arc;
use std::time::Instant;

use surrealdb::types::RecordId;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::interval_at;

use crate::error::Result;
use crate::filter::{Decision, IngestFilter};
use crate::storage::SystemDb;

use super::{AdapterRegistry, IngestApiClient, SourceAdapter};

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
/// `adapter.fetch(cursor)`, runs each item through the filter, and on
/// `Accept` POSTs an [`IngestRequestBody`] to `/api/ingestion/documents`
/// via [`IngestApiClient`]. The handler stamps `tenant_id` from the
/// service-identity JWT — the scheduler does not see it on the ingest
/// path.
///
/// `cursor_tenant_id` is used **only** for system-path cursor
/// persistence (the `source_state` row that records "where this adapter
/// got to"). It must match the tenant the service identity carries so
/// cursor reads and ingest writes land together; in `api::serve` both
/// derive from the same `DELPHI_SOURCES_DEFAULT_TENANT`.
///
/// Tasks wait one full `poll_interval` before their first tick: the
/// scheduler is spawned from `api::serve` before the HTTP listener
/// starts accepting connections, so an immediate tick would race the
/// `axum::serve` bind. After the initial wait the cadence is the
/// standard fixed-interval poll.
pub fn run_scheduler(
    ingest: Arc<IngestApiClient>,
    filter: Arc<dyn IngestFilter>,
    system: Arc<SystemDb>,
    cursor_tenant_id: RecordId,
    registry: AdapterRegistry,
) -> SchedulerHandle {
    let shutdown = Arc::new(Notify::new());
    let mut handles = Vec::new();
    for adapter in registry.into_inner() {
        let ingest = ingest.clone();
        let filter = filter.clone();
        let system = system.clone();
        let cursor_tenant_id = cursor_tenant_id.clone();
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(adapter_loop(
            adapter,
            ingest,
            filter,
            system,
            cursor_tenant_id,
            shutdown,
        )));
    }
    SchedulerHandle { shutdown, handles }
}

async fn adapter_loop(
    adapter: Arc<dyn SourceAdapter>,
    ingest: Arc<IngestApiClient>,
    filter: Arc<dyn IngestFilter>,
    system: Arc<SystemDb>,
    cursor_tenant_id: RecordId,
    shutdown: Arc<Notify>,
) {
    let name = adapter.name().to_string();
    let interval = adapter.poll_interval();
    // `interval_at(now + period, period)` skips the immediate first
    // tick `tokio::time::interval` would otherwise fire — see the
    // doc-comment on `run_scheduler` for why.
    let start = tokio::time::Instant::from_std(Instant::now() + interval);
    let mut ticker = interval_at(start, interval);

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
                    ingest.as_ref(),
                    filter.as_ref(),
                    system.as_ref(),
                    &cursor_tenant_id,
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
    ingest: &IngestApiClient,
    filter: &dyn IngestFilter,
    system: &SystemDb,
    cursor_tenant_id: &RecordId,
) -> Result<()> {
    let cursor = system
        .get_source_cursor(cursor_tenant_id, adapter.name())
        .await?;
    let fetched = adapter.fetch(cursor).await?;

    let total = fetched.items.len();
    let mut accepted = 0usize;
    let mut rejected = 0usize;

    for item in fetched.items {
        match filter.evaluate(&item).await {
            Decision::Accept => {
                accepted += 1;
                match ingest.ingest(item).await {
                    Ok(outcome) => {
                        tracing::debug!(adapter = adapter.name(), ?outcome, "ingested via API")
                    }
                    Err(e) => {
                        tracing::error!(
                            adapter = adapter.name(),
                            error = %e,
                            "ingest API call failed"
                        );
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
        system
            .put_source_cursor(cursor_tenant_id, adapter.name(), &c)
            .await?;
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
