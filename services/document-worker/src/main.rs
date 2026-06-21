mod object_store;

use axum::{routing::get, Router};
use chrono::Utc;
use delphi_config::{init_tracing, ServiceConfig};
use delphi_contracts::{
    CreateIngestionDocument, IngestStageRequested, IngestionStage, CONTRACT_VERSION,
};
use delphi_nats::{IngestionBus, NatsChatBus, NatsChatBusOptions, UploadSaga, UploadSagaState};
use delphi_storage::{IngestionRepository, PgRepository};
use object_store::S3MultipartStore;
use std::time::Duration;
use tokio::time::MissedTickBehavior;

#[derive(Clone)]
struct WorkerState {
    repo: PgRepository,
    bus: NatsChatBus,
    store: S3MultipartStore,
    pipeline_version: u32,
    poll_interval: Duration,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let config = ServiceConfig::from_env(3004)?;
    let state = WorkerState {
        repo: PgRepository::connect(&config.database_url, config.pg_max_connections).await?,
        bus: NatsChatBus::connect(&config.nats_url, NatsChatBusOptions::default()).await?,
        store: S3MultipartStore::from_env()?,
        pipeline_version: env_u32("INGEST_PIPELINE_VERSION", 1),
        poll_interval: Duration::from_secs(env_u64("DOCUMENT_WORKER_POLL_INTERVAL_SECS", 2)),
    };

    let health_listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    tokio::spawn(async move {
        let app = Router::new().route("/healthz", get(healthz));
        if let Err(error) = axum::serve(health_listener, app).await {
            tracing::error!(?error, "document-worker health server failed");
        }
    });

    tracing::info!(addr = %config.bind_addr, "starting document-worker");
    let mut ticker = tokio::time::interval(state.poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if let Err(error) = run_once(&state).await {
            tracing::warn!(?error, "document worker pass failed");
        }
    }
}

async fn healthz() -> &'static str {
    "ok"
}

async fn run_once(state: &WorkerState) -> anyhow::Result<()> {
    let sagas = state.bus.list_upload_sagas().await?;
    let now = Utc::now();
    for saga in sagas {
        match saga.state {
            UploadSagaState::Completing => complete_upload_saga(state, saga).await,
            UploadSagaState::Aborting => abort_upload_saga(state, saga).await,
            UploadSagaState::Uploading if saga.expires_at <= now => {
                match state
                    .bus
                    .claim_upload_cleanup(&saga.tenant_id, &saga.user_id, &saga.upload_id, now)
                    .await
                {
                    Ok(Some(claimed)) => abort_upload_saga(state, claimed).await,
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(%error, upload_id = %saga.upload_id, "claim expired upload failed");
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

async fn complete_upload_saga(state: &WorkerState, mut saga: UploadSaga) {
    if !saga.object_completed {
        if let Err(error) = state
            .store
            .complete_multipart_upload(&saga.storage_key, &saga.multipart_upload_id, &saga.parts)
            .await
        {
            let object_exists = state
                .store
                .object_exists(&saga.storage_key)
                .await
                .unwrap_or(false);
            if !object_exists {
                let _ = state
                    .bus
                    .mark_upload_failed(
                        &saga.tenant_id,
                        &saga.user_id,
                        &saga.upload_id,
                        &format!("complete multipart upload: {error}"),
                    )
                    .await;
                let _ = state
                    .store
                    .abort_multipart_upload(&saga.storage_key, &saga.multipart_upload_id)
                    .await;
                tracing::warn!(%error, upload_id = %saga.upload_id, "multipart completion failed");
                return;
            }
        }
        match state
            .bus
            .mark_upload_object_completed(&saga.tenant_id, &saga.user_id, &saga.upload_id)
            .await
        {
            Ok(updated) => saga = updated,
            Err(error) => {
                tracing::warn!(%error, upload_id = %saga.upload_id, "mark object completed failed");
                return;
            }
        }
    }

    if let Err(error) = accept_upload_saga(state, &saga).await {
        let _ = state
            .bus
            .mark_upload_failed(
                &saga.tenant_id,
                &saga.user_id,
                &saga.upload_id,
                &format!("accept upload: {error}"),
            )
            .await;
        tracing::warn!(?error, upload_id = %saga.upload_id, "accept upload failed");
    }
}

async fn abort_upload_saga(state: &WorkerState, saga: UploadSaga) {
    match state
        .store
        .abort_multipart_upload(&saga.storage_key, &saga.multipart_upload_id)
        .await
    {
        Ok(()) => {
            let reason = saga
                .error
                .as_deref()
                .unwrap_or("upload expired; multipart upload aborted");
            let _ = state
                .bus
                .mark_upload_aborted(&saga.tenant_id, &saga.user_id, &saga.upload_id, reason)
                .await;
            let _ = state
                .repo
                .mark_upload_failed(&saga.tenant_id, &saga.user_id, &saga.upload_id, reason)
                .await;
        }
        Err(error) => {
            let _ = state
                .bus
                .defer_upload_cleanup(
                    &saga.tenant_id,
                    &saga.user_id,
                    &saga.upload_id,
                    &format!("abort multipart upload: {error}"),
                )
                .await;
            tracing::warn!(%error, upload_id = %saga.upload_id, "abort multipart upload failed");
        }
    }
}

async fn accept_upload_saga(state: &WorkerState, saga: &UploadSaga) -> anyhow::Result<()> {
    let upload_id = saga.upload_id.clone();
    let job = state
        .repo
        .create_ingestion_job(
            &saga.tenant_id,
            &saga.user_id,
            CreateIngestionDocument {
                document_id: Some(upload_id.clone()),
                job_id: Some(upload_job_id(&upload_id, state.pipeline_version)),
                title: saga.title.clone(),
                source_type: "manual".to_owned(),
                source_uri: saga
                    .source_uri
                    .clone()
                    .or_else(|| Some(format!("urn:delphi:upload:{upload_id}"))),
                storage_key: saga.storage_key.clone(),
                filename: Some(saga.filename.clone()),
                content_type: saga.content_type.clone(),
                declared_size: saga.declared_size,
                metadata: saga.metadata.clone(),
            },
            state.pipeline_version,
        )
        .await?;
    state
        .repo
        .mark_upload_accepted(
            &saga.tenant_id,
            &saga.user_id,
            &upload_id,
            &job.document_id,
            &job.id,
        )
        .await?;
    state
        .bus
        .mark_upload_accepted(
            &saga.tenant_id,
            &saga.user_id,
            &upload_id,
            &job.document_id,
            &job.id,
        )
        .await?;

    let command = IngestStageRequested {
        v: CONTRACT_VERSION,
        command_id: format!("{}:validate:{}", job.id, job.attempt),
        tenant_id: saga.tenant_id.clone(),
        user_id: saga.user_id.clone(),
        job_id: job.id,
        document_id: job.document_id,
        stage: IngestionStage::Validate,
        pipeline_version: job.pipeline_version,
        attempt: job.attempt,
        causation_id: upload_id,
        ts: Utc::now(),
    };
    if let Err(error) = state.bus.publish_ingest_stage(command).await {
        tracing::warn!(%error, "failed to publish initial ingestion stage");
    }
    Ok(())
}

fn upload_job_id(upload_id: &str, pipeline_version: u32) -> String {
    format!("{upload_id}:ingest:v{pipeline_version}")
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}
