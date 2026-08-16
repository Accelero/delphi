//! The document worker.
//!
//! Two independently supervised tasks:
//!
//! | Task                | Instances      | Model               |
//! | ------------------- | -------------- | ------------------- |
//! | work-queue consumer | every instance | competing consumers |
//! | projection loop     | exactly one    | leader-elected      |
//!
//! There is no separate projector service: the projection loop is a task here,
//! gated on a Postgres session advisory lock.
//!
//! **Nothing here sweeps anything.** Blobs are kept; upload state expires with
//! its KV bucket's `max_age`; incomplete multiparts are storage's own problem.
//! The third leader-elected task this worker used to run existed only to age
//! out Postgres upload rows, and those no longer exist.

use std::sync::Arc;
use std::time::Duration;

use axum::{routing::get, Router};
use delphi_config::{init_tracing, ServiceConfig};
use delphi_document_adapters::jetstream::{JetStreamEventStore, WorkItem, WorkQueueConsumer};
use delphi_document_adapters::postgres::{ProjectionLoop, ProjectorLease};
use delphi_document_adapters::{config, DocumentInfra, SystemClock};
use delphi_document_app::{FinishOutcome, UploadFinisher};
use delphi_document_adapters::verification::{BasicContentValidator, PermissiveScanner};

/// How long a naked transient failure waits before redelivery. Short, because
/// `max_deliver` bounds the total number of attempts and the work stream's
/// `max_age` bounds their span.
const NAK_DELAY: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let service_config = ServiceConfig::from_env(3004)?;
    let worker_config = config::WorkerConfig::from_env()?;

    let infra = DocumentInfra::connect(
        &service_config.database_url,
        service_config.pg_max_connections,
        &service_config.nats_url,
    )
    .await?;

    let health_listener = tokio::net::TcpListener::bind(service_config.bind_addr).await?;
    tokio::spawn(async move {
        let app = Router::new().route("/healthz", get(healthz));
        if let Err(error) = axum::serve(health_listener, app).await {
            tracing::error!(?error, "document-worker health server failed");
        }
    });

    tracing::info!(addr = %service_config.bind_addr, "starting document-worker");

    let work = tokio::spawn(run_work_queue(infra.clone(), worker_config.clone()));
    let projection = tokio::spawn(run_projection(
        infra.clone(),
        worker_config.clone(),
        service_config.database_url.clone(),
    ));
    // Supervised independently: a projection that stops must not silently take
    // the work queue with it, and vice versa.
    tokio::select! {
        result = work => tracing::error!(?result, "work-queue task exited"),
        result = projection => tracing::error!(?result, "projection task exited"),
    }

    Ok(())
}

async fn healthz() -> &'static str {
    "ok"
}

// ------------------------------------------------------------- work queue task

async fn run_work_queue(infra: DocumentInfra, config: config::WorkerConfig) {
    let finisher = Arc::new(UploadFinisher::new(
        infra.blobs.clone(),
        // Swapping in a real engine is this line plus one adapter.
        Arc::new(PermissiveScanner),
        Arc::new(BasicContentValidator),
        infra.events.clone(),
        infra.uploads.clone(),
        Arc::new(SystemClock),
    ));

    loop {
        let consumer = match WorkQueueConsumer::connect(
            &infra.js,
            config.ack_wait,
            config.max_deliver,
            config.max_ack_pending,
            config.work_concurrency,
        )
        .await
        {
            Ok(consumer) => consumer,
            Err(error) => {
                tracing::error!(%error, "could not open the work queue; retrying");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let finisher = finisher.clone();
        let result = consumer
            .run(move |item| {
                let finisher = finisher.clone();
                async move { handle_work_item(&finisher, item).await }
            })
            .await;

        match result {
            Ok(()) => tracing::warn!("work queue ended; reconnecting"),
            Err(error) => tracing::error!(%error, "work queue failed; reconnecting"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn handle_work_item(finisher: &UploadFinisher, item: WorkItem) {
    let upload_id = item.command.upload_id.clone();
    let outcome = finisher
        .finish(&item.command, item.is_final_delivery)
        .await;

    // The message stays unacked until the event is durable. Ack last, always.
    match outcome {
        FinishOutcome::Accepted {
            document_id,
            version,
            superseded,
        } => {
            tracing::info!(%upload_id, %document_id, version, superseded, "upload accepted");
            item.ack().await;
        }
        FinishOutcome::Rejected { reason } => {
            tracing::info!(%upload_id, %reason, "upload rejected");
            if item.is_final_delivery {
                // Poison: never redeliver. The object and the attempt row have
                // already been dealt with by the use case.
                item.term().await;
            } else {
                item.ack().await;
            }
        }
        FinishOutcome::Retry { error } => {
            tracing::warn!(
                %upload_id,
                %error,
                delivery = item.num_delivered,
                "upload hit a transient failure; will retry"
            );
            item.nak(NAK_DELAY).await;
        }
    }
}

// -------------------------------------------------------------- projection task

async fn run_projection(
    infra: DocumentInfra,
    config: config::WorkerConfig,
    database_url: String,
) {
    let loop_runner = ProjectionLoop::new(
        JetStreamEventStore::stream(&infra.events).clone(),
        config.projection_batch,
    );

    loop {
        match ProjectorLease::try_acquire(&database_url, config.projector_lock_id).await {
            Ok(Some(mut lease)) => {
                tracing::info!("acquired the projector lease");
                if let Err(error) = loop_runner.run(&mut lease).await {
                    tracing::error!(%error, "projection loop failed");
                }
                lease.release().await;
                tracing::warn!("released the projector lease");
            }
            Ok(None) => {
                tracing::debug!("another instance holds the projector lease");
            }
            Err(error) => {
                tracing::error!(%error, "could not contend for the projector lease");
            }
        }
        tokio::time::sleep(config.projector_election_interval).await;
    }
}
