mod object_store;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use delphi_auth::{AuthContext, AuthError, AuthVerifier};
use delphi_config::{init_tracing, ServiceConfig};
use delphi_contracts::{
    ApiError, ApiErrorBody, ApiErrorCode, AuthUser, CompleteUploadRequest, CompleteUploadResponse,
    ConversationDetail, ConversationSummary, CreateConversationRequest, CreateIngestionDocument,
    CreateUploadRequest, CreateUploadResponse, IngestStageRequested, IngestionStage,
    SignUploadPartRequest, SignUploadPartResponse, SubmitChatMessageRequest, SubmitTurnRequest,
    TurnAccepted, TurnRequested, UpdateConversationRequest, UploadStatusResponse, CONTRACT_VERSION,
};
use delphi_nats::{
    ChatBus, ChatBusError, ChatLock, IngestionBus, NatsChatBus, NatsChatBusOptions, UploadSaga,
    UploadSagaState,
};
use delphi_storage::{
    ChatRepository, CreateUploadSession, IngestionRepository, PgRepository, StorageError,
};
use object_store::S3MultipartStore;
use std::time::Duration;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
struct AppState {
    auth: AuthVerifier,
    repo: PgRepository,
    bus: NatsChatBus,
    uploads: UploadConfig,
    object_store: Option<S3MultipartStore>,
}

impl axum::extract::FromRef<AppState> for AuthVerifier {
    fn from_ref(state: &AppState) -> Self {
        state.auth.clone()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let config = ServiceConfig::from_env(3001)?;
    let state = AppState {
        auth: AuthVerifier::from_env()?,
        repo: PgRepository::connect(&config.database_url, config.pg_max_connections).await?,
        bus: NatsChatBus::connect(&config.nats_url, NatsChatBusOptions::default()).await?,
        uploads: UploadConfig::from_env(),
        object_store: match S3MultipartStore::from_env() {
            Ok(store) => Some(store),
            Err(error) => {
                tracing::warn!(%error, "ingestion object store is not configured; upload endpoints will be unavailable");
                None
            }
        },
    };
    if state.object_store.is_some() {
        tokio::spawn(run_upload_saga_reconciler(state.clone()));
    }

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/api/auth/me", get(me))
        .route(
            "/api/chat/conversations",
            get(list_conversations).post(create_conversation),
        )
        .route(
            "/api/chat",
            get(list_conversations).post(create_conversation),
        )
        .route(
            "/api/chat/conversations/{conversation_id}",
            get(get_conversation)
                .patch(rename_conversation)
                .delete(delete_conversation),
        )
        .route(
            "/api/chat/{conversation_id}",
            get(get_conversation)
                .post(submit_chat_message)
                .patch(rename_conversation)
                .delete(delete_conversation),
        )
        .route(
            "/api/chat/conversations/{conversation_id}/turns",
            post(submit_turn),
        )
        .route(
            "/api/chat/conversations/{conversation_id}/stop",
            post(stop_turn),
        )
        .route("/api/ingestion/uploads", post(create_upload))
        .route("/api/ingestion/uploads/{upload_id}", get(get_upload_status))
        .route(
            "/api/ingestion/uploads/{upload_id}/sign-part",
            post(sign_upload_part),
        )
        .route(
            "/api/ingestion/uploads/{upload_id}/complete",
            post(complete_upload),
        )
        .route("/chat", get(list_conversations).post(create_conversation))
        .route(
            "/chat/{conversation_id}",
            get(get_conversation).post(submit_chat_message),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    tracing::info!(addr = %config.bind_addr, "starting api-service");
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn healthz() -> &'static str {
    "ok"
}

async fn me(auth: AuthContext) -> Result<Json<AuthUser>, ServiceError> {
    Ok(Json(auth.public_user()))
}

#[derive(Debug, Clone)]
struct UploadConfig {
    part_size_bytes: u64,
    part_url_ttl: Duration,
    pipeline_version: u32,
    cleanup_interval: Duration,
}

impl UploadConfig {
    fn from_env() -> Self {
        Self {
            part_size_bytes: env_u64("INGEST_UPLOAD_PART_SIZE_BYTES", 8 * 1024 * 1024),
            part_url_ttl: Duration::from_secs(env_u64("INGEST_UPLOAD_PART_URL_TTL_SECS", 900)),
            pipeline_version: env_u32("INGEST_PIPELINE_VERSION", 1),
            cleanup_interval: Duration::from_secs(env_u64(
                "INGEST_UPLOAD_CLEANUP_INTERVAL_SECS",
                60,
            )),
        }
    }
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

async fn run_upload_saga_reconciler(state: AppState) {
    let Some(store) = state.object_store.clone() else {
        return;
    };
    loop {
        tokio::time::sleep(state.uploads.cleanup_interval).await;
        let now = Utc::now();
        let sagas = match state.bus.list_upload_sagas().await {
            Ok(sagas) => sagas,
            Err(error) => {
                tracing::warn!(%error, "upload saga cleanup could not list sagas");
                continue;
            }
        };
        for saga in sagas {
            if saga.is_terminal() || saga.expires_at > now {
                continue;
            }
            let claimed = match state
                .bus
                .claim_upload_cleanup(&saga.tenant_id, &saga.user_id, &saga.upload_id, now)
                .await
            {
                Ok(Some(claimed)) => claimed,
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!(%error, upload_id = %saga.upload_id, "upload saga cleanup claim failed");
                    continue;
                }
            };
            reconcile_claimed_upload_saga(&state, &store, claimed).await;
        }
    }
}

async fn reconcile_claimed_upload_saga(
    state: &AppState,
    store: &S3MultipartStore,
    saga: UploadSaga,
) {
    let object_exists = saga.object_completed
        || match store.object_exists(&saga.storage_key).await {
            Ok(exists) => exists,
            Err(error) => {
                let _ = state
                    .bus
                    .defer_upload_cleanup(
                        &saga.tenant_id,
                        &saga.user_id,
                        &saga.upload_id,
                        &format!("check completed object before cleanup: {error}"),
                    )
                    .await;
                tracing::warn!(%error, upload_id = %saga.upload_id, "upload saga cleanup could not check object state");
                return;
            }
        };

    if object_exists {
        let saga = match state
            .bus
            .mark_upload_object_completed(&saga.tenant_id, &saga.user_id, &saga.upload_id)
            .await
        {
            Ok(saga) => saga,
            Err(error) => {
                tracing::warn!(%error, upload_id = %saga.upload_id, "upload saga cleanup could not mark object complete");
                return;
            }
        };
        if let Err(error) = accept_upload_saga(state, &saga).await {
            let _ = state
                .bus
                .defer_upload_cleanup(
                    &saga.tenant_id,
                    &saga.user_id,
                    &saga.upload_id,
                    &format!("resume accepted object: {error:?}"),
                )
                .await;
            tracing::warn!(?error, upload_id = %saga.upload_id, "upload saga cleanup could not resume accepted object");
        }
        return;
    }

    match store
        .abort_multipart_upload(&saga.storage_key, &saga.multipart_upload_id)
        .await
    {
        Ok(()) => {
            let reason = "upload expired; multipart upload aborted";
            let _ = state
                .bus
                .mark_upload_aborted(&saga.tenant_id, &saga.user_id, &saga.upload_id, reason)
                .await;
            let _ = state
                .repo
                .mark_upload_failed(&saga.tenant_id, &saga.user_id, &saga.upload_id, reason)
                .await;
            tracing::info!(upload_id = %saga.upload_id, "upload saga cleanup aborted multipart upload");
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
            tracing::warn!(%error, upload_id = %saga.upload_id, "upload saga cleanup abort failed; will retry");
        }
    }
}

async fn list_conversations(
    auth: AuthContext,
    State(state): State<AppState>,
) -> Result<Json<Vec<ConversationSummary>>, ServiceError> {
    Ok(Json(
        state
            .repo
            .list_conversations(&auth.tenant_id, &auth.user_id)
            .await?,
    ))
}

async fn create_conversation(
    auth: AuthContext,
    State(state): State<AppState>,
    Json(body): Json<CreateConversationRequest>,
) -> Result<(StatusCode, Json<ConversationDetail>), ServiceError> {
    let detail = state
        .repo
        .create_conversation(&auth.tenant_id, &auth.user_id, body.title)
        .await?;
    Ok((StatusCode::CREATED, Json(detail)))
}

async fn get_conversation(
    auth: AuthContext,
    State(state): State<AppState>,
    Path(conversation_id): Path<String>,
) -> Result<Json<ConversationDetail>, ServiceError> {
    Ok(Json(
        state
            .repo
            .get_conversation(&auth.tenant_id, &auth.user_id, &conversation_id)
            .await?,
    ))
}

async fn rename_conversation(
    auth: AuthContext,
    State(state): State<AppState>,
    Path(conversation_id): Path<String>,
    Json(body): Json<UpdateConversationRequest>,
) -> Result<Json<ConversationDetail>, ServiceError> {
    if body.title.trim().is_empty() {
        return Err(ServiceError::InvalidRequest("title cannot be empty".into()));
    }
    Ok(Json(
        state
            .repo
            .rename_conversation(
                &auth.tenant_id,
                &auth.user_id,
                &conversation_id,
                body.title.trim().to_owned(),
            )
            .await?,
    ))
}

async fn delete_conversation(
    auth: AuthContext,
    State(state): State<AppState>,
    Path(conversation_id): Path<String>,
) -> Result<StatusCode, ServiceError> {
    state
        .repo
        .delete_conversation(&auth.tenant_id, &auth.user_id, &conversation_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn submit_turn(
    auth: AuthContext,
    State(state): State<AppState>,
    Path(conversation_id): Path<String>,
    Json(body): Json<SubmitTurnRequest>,
) -> Result<(StatusCode, Json<TurnAccepted>), ServiceError> {
    enqueue_turn(
        &auth,
        &state,
        conversation_id,
        body.user_message_id,
        body.turn_id,
        body.text,
        body.parent_message_id,
    )
    .await
}

async fn submit_chat_message(
    auth: AuthContext,
    State(state): State<AppState>,
    Path(conversation_id): Path<String>,
    Json(body): Json<SubmitChatMessageRequest>,
) -> Result<(StatusCode, Json<TurnAccepted>), ServiceError> {
    let user_message_id = body
        .user_message_id
        .unwrap_or_else(|| ulid::Ulid::new().to_string());
    let turn_id = body
        .turn_id
        .unwrap_or_else(|| ulid::Ulid::new().to_string());
    enqueue_turn(
        &auth,
        &state,
        conversation_id,
        user_message_id,
        turn_id,
        body.text,
        body.parent_message_id,
    )
    .await
}

async fn enqueue_turn(
    auth: &AuthContext,
    state: &AppState,
    conversation_id: String,
    user_message_id: String,
    turn_id: String,
    text: String,
    parent_message_id: Option<String>,
) -> Result<(StatusCode, Json<TurnAccepted>), ServiceError> {
    validate_ulid(&user_message_id, "user_message_id")?;
    validate_ulid(&turn_id, "turn_id")?;
    if text.trim().is_empty() {
        return Err(ServiceError::InvalidRequest(
            "message text cannot be empty".into(),
        ));
    }
    state
        .repo
        .assert_parent_tail(
            &auth.tenant_id,
            &auth.user_id,
            &conversation_id,
            parent_message_id.as_deref(),
        )
        .await?;

    state
        .bus
        .acquire_lock(ChatLock::requested(
            auth.tenant_id.clone(),
            auth.user_id.clone(),
            conversation_id.clone(),
            turn_id.clone(),
            user_message_id.clone(),
            text.clone(),
            parent_message_id.clone(),
            auth.bearer_subject.clone(),
        ))
        .await?;

    let command = TurnRequested {
        v: CONTRACT_VERSION,
        command_id: turn_id.clone(),
        tenant_id: auth.tenant_id.clone(),
        user_id: auth.user_id.clone(),
        conversation_id: conversation_id.clone(),
        turn_id: turn_id.clone(),
        ts: Utc::now(),
    };

    if let Err(error) = state.bus.publish_turn_requested(command).await {
        let _ = state
            .repo
            .record_turn_failed(
                &auth.tenant_id,
                &auth.user_id,
                &conversation_id,
                &turn_id,
                &format!("failed to publish turn request: {error}"),
            )
            .await;
        state
            .bus
            .release_lock(&auth.tenant_id, &conversation_id, &turn_id)
            .await;
        return Err(error.into());
    }

    Ok((StatusCode::ACCEPTED, Json(TurnAccepted { turn_id })))
}

async fn stop_turn(
    auth: AuthContext,
    State(state): State<AppState>,
    Path(conversation_id): Path<String>,
) -> Result<StatusCode, ServiceError> {
    state
        .repo
        .get_conversation(&auth.tenant_id, &auth.user_id, &conversation_id)
        .await?;
    if let Some(lock) = state
        .bus
        .request_stop(&auth.tenant_id, &conversation_id, &auth.user_id)
        .await?
    {
        if let Some(worker_id) = lock.worker_id.as_deref() {
            state
                .bus
                .publish_stop(worker_id, &auth.tenant_id, &conversation_id, &lock.turn_id)
                .await?;
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn create_upload(
    auth: AuthContext,
    State(state): State<AppState>,
    Json(body): Json<CreateUploadRequest>,
) -> Result<(StatusCode, Json<CreateUploadResponse>), ServiceError> {
    require_upload_author(&auth)?;
    let store = state.object_store.as_ref().ok_or_else(|| {
        ServiceError::Internal("ingestion object store is not configured".to_owned())
    })?;
    if body.filename.trim().is_empty() {
        return Err(ServiceError::InvalidRequest(
            "filename cannot be empty".to_owned(),
        ));
    }
    if body.size == 0 {
        return Err(ServiceError::InvalidRequest(
            "file size must be greater than zero".to_owned(),
        ));
    }

    let upload_id = ulid::Ulid::new().to_string();
    let key = upload_key(&auth.tenant_id, &upload_id);
    let content_type = body
        .content_type
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("application/octet-stream")
        .to_owned();
    let multipart_upload_id = match store.create_multipart_upload(&key, &content_type).await {
        Ok(id) => id,
        Err(error) => {
            return Err(ServiceError::Internal(format!(
                "create multipart upload: {error}"
            )))
        }
    };

    let filename = body.filename;
    let title = body.title;
    let source_uri = body.source_uri.filter(|value| !value.trim().is_empty());
    let metadata = normalize_upload_metadata(body.metadata);
    if let Err(error) = state
        .bus
        .start_upload_saga(UploadSaga::uploading(
            auth.tenant_id.clone(),
            auth.user_id.clone(),
            upload_id.clone(),
            key.clone(),
            multipart_upload_id.clone(),
            filename.clone(),
            Some(content_type.clone()),
            body.size,
            title.clone(),
            source_uri.clone(),
            metadata.clone(),
        ))
        .await
    {
        let _ = store
            .abort_multipart_upload(&key, &multipart_upload_id)
            .await;
        return Err(ServiceError::Internal(format!(
            "start upload saga: {error}"
        )));
    }

    match state
        .repo
        .create_upload_session(
            &auth.tenant_id,
            &auth.user_id,
            CreateUploadSession {
                upload_id: upload_id.clone(),
                storage_key: key.clone(),
                multipart_upload_id: multipart_upload_id.clone(),
                filename,
                content_type: Some(content_type.clone()),
                declared_size: body.size,
                title,
                source_uri,
                metadata,
            },
        )
        .await
    {
        Ok(_) => Ok((
            StatusCode::CREATED,
            Json(CreateUploadResponse {
                upload_id,
                key,
                multipart_upload_id,
                part_size_bytes: state.uploads.part_size_bytes,
                part_url_ttl_secs: state.uploads.part_url_ttl.as_secs(),
            }),
        )),
        Err(error) => {
            let _ = state
                .bus
                .mark_upload_failed(
                    &auth.tenant_id,
                    &auth.user_id,
                    &upload_id,
                    &error.to_string(),
                )
                .await;
            let _ = store
                .abort_multipart_upload(&key, &multipart_upload_id)
                .await;
            Err(error.into())
        }
    }
}

async fn sign_upload_part(
    auth: AuthContext,
    State(state): State<AppState>,
    Path(upload_id): Path<String>,
    Json(body): Json<SignUploadPartRequest>,
) -> Result<Json<SignUploadPartResponse>, ServiceError> {
    require_upload_author(&auth)?;
    let store = state.object_store.as_ref().ok_or_else(|| {
        ServiceError::Internal("ingestion object store is not configured".to_owned())
    })?;
    if body.part_number == 0 || body.part_number > 10_000 {
        return Err(ServiceError::InvalidRequest(
            "part_number must be between 1 and 10000".to_owned(),
        ));
    }
    let saga = state
        .bus
        .load_upload_saga(&auth.tenant_id, &auth.user_id, &upload_id)
        .await
        .map_err(|error| ServiceError::Internal(format!("load upload saga: {error}")))?
        .ok_or(StorageError::NotFound)?;
    if saga.state != UploadSagaState::Uploading {
        return Err(ServiceError::InFlight);
    }
    let grant = store
        .presign_upload_part(
            &saga.storage_key,
            &saga.multipart_upload_id,
            body.part_number,
            state.uploads.part_url_ttl,
        )
        .await
        .map_err(|error| ServiceError::Internal(format!("presign upload part: {error}")))?;
    Ok(Json(SignUploadPartResponse {
        url: grant.url,
        method: grant.method,
        expires_at: grant.expires_at,
    }))
}

async fn complete_upload(
    auth: AuthContext,
    State(state): State<AppState>,
    Path(upload_id): Path<String>,
    Json(body): Json<CompleteUploadRequest>,
) -> Result<(StatusCode, Json<CompleteUploadResponse>), ServiceError> {
    require_upload_author(&auth)?;
    if body.parts.is_empty() {
        return Err(ServiceError::InvalidRequest(
            "at least one uploaded part is required".to_owned(),
        ));
    }
    let saga = state
        .bus
        .claim_upload_completion(&auth.tenant_id, &auth.user_id, &upload_id, body.parts)
        .await
        .map_err(|error| match error {
            ChatBusError::InFlight => ServiceError::InFlight,
            other => ServiceError::Internal(format!("claim upload saga: {other}")),
        })?;
    if saga.state == UploadSagaState::Accepted {
        let document_id = saga.document_id.ok_or_else(|| {
            ServiceError::Internal("accepted upload saga missing document_id".to_owned())
        })?;
        let job_id = saga.job_id.ok_or_else(|| {
            ServiceError::Internal("accepted upload saga missing job_id".to_owned())
        })?;
        return Ok((
            StatusCode::ACCEPTED,
            Json(CompleteUploadResponse::Accepted {
                document_id,
                job_id,
            }),
        ));
    }
    if saga.state != UploadSagaState::Completing {
        return Err(ServiceError::InFlight);
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(CompleteUploadResponse::Accepted {
            document_id: upload_id.clone(),
            job_id: format!("{}:ingest:v{}", upload_id, state.uploads.pipeline_version),
        }),
    ))
}

async fn accept_upload_saga(
    state: &AppState,
    saga: &UploadSaga,
) -> Result<CompleteUploadResponse, ServiceError> {
    let upload_id = saga.upload_id.clone();
    let job = state
        .repo
        .create_ingestion_job(
            &saga.tenant_id,
            &saga.user_id,
            CreateIngestionDocument {
                document_id: Some(upload_id.clone()),
                job_id: Some(format!(
                    "{}:ingest:v{}",
                    upload_id, state.uploads.pipeline_version
                )),
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
            state.uploads.pipeline_version,
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
        .await
        .map_err(|error| ServiceError::Internal(format!("mark upload saga accepted: {error}")))?;

    let command = IngestStageRequested {
        v: CONTRACT_VERSION,
        command_id: format!("{}:validate:{}", job.id, job.attempt),
        tenant_id: saga.tenant_id.clone(),
        user_id: saga.user_id.clone(),
        job_id: job.id.clone(),
        document_id: job.document_id.clone(),
        stage: IngestionStage::Validate,
        pipeline_version: job.pipeline_version,
        attempt: job.attempt,
        causation_id: upload_id,
        ts: Utc::now(),
    };
    if let Err(error) = state.bus.publish_ingest_stage(command).await {
        tracing::warn!(%error, job_id = %job.id, "failed to publish initial ingestion stage; reconciler must recover");
    }

    Ok(CompleteUploadResponse::Accepted {
        document_id: job.document_id,
        job_id: job.id,
    })
}

async fn get_upload_status(
    auth: AuthContext,
    State(state): State<AppState>,
    Path(upload_id): Path<String>,
) -> Result<Json<UploadStatusResponse>, ServiceError> {
    require_upload_author(&auth)?;
    if let Some(saga) = state
        .bus
        .load_upload_saga(&auth.tenant_id, &auth.user_id, &upload_id)
        .await
        .map_err(|error| ServiceError::Internal(format!("load upload saga: {error}")))?
    {
        let status = match saga.state {
            UploadSagaState::Uploading | UploadSagaState::Completing => {
                UploadStatusResponse::Uploading
            }
            UploadSagaState::Accepted => UploadStatusResponse::Accepted {
                document_id: saga.document_id.ok_or_else(|| {
                    ServiceError::Internal("accepted upload saga missing document_id".to_owned())
                })?,
                job_id: saga.job_id.ok_or_else(|| {
                    ServiceError::Internal("accepted upload saga missing job_id".to_owned())
                })?,
            },
            UploadSagaState::Aborting | UploadSagaState::Failed | UploadSagaState::Aborted => {
                UploadStatusResponse::Failed {
                    message: saga.error.unwrap_or_else(|| "upload failed".to_owned()),
                }
            }
        };
        return Ok(Json(status));
    }
    let session = state
        .repo
        .get_upload_session(&auth.tenant_id, &auth.user_id, &upload_id)
        .await?;
    let status = match session.state.as_str() {
        "uploading" => UploadStatusResponse::Uploading,
        "accepted" => UploadStatusResponse::Accepted {
            document_id: session.document_id.ok_or_else(|| {
                ServiceError::Internal("accepted upload missing document_id".to_owned())
            })?,
            job_id: session.job_id.ok_or_else(|| {
                ServiceError::Internal("accepted upload missing job_id".to_owned())
            })?,
        },
        "failed" => UploadStatusResponse::Failed {
            message: "upload failed".to_owned(),
        },
        _ => UploadStatusResponse::Failed {
            message: "unknown upload state".to_owned(),
        },
    };
    Ok(Json(status))
}

fn require_upload_author(auth: &AuthContext) -> Result<(), ServiceError> {
    if auth
        .roles
        .iter()
        .any(|role| role == "member" || role == "ingester" || role == "owner")
    {
        Ok(())
    } else {
        Err(ServiceError::Forbidden)
    }
}

fn upload_key(tenant_id: &str, upload_id: &str) -> String {
    let tenant = tenant_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    format!("tenants/{tenant}/documents/{upload_id}/versions/1/original")
}

fn normalize_upload_metadata(metadata: Option<serde_json::Value>) -> serde_json::Value {
    match metadata {
        Some(value) if value.is_object() => value,
        _ => serde_json::json!({}),
    }
}

fn validate_ulid(value: &str, field: &str) -> Result<(), ServiceError> {
    value
        .parse::<ulid::Ulid>()
        .map(|_| ())
        .map_err(|_| ServiceError::InvalidRequest(format!("{field} must be a ULID")))
}

#[derive(Debug)]
enum ServiceError {
    Auth,
    NotFound,
    Forbidden,
    StaleParent,
    InFlight,
    InvalidRequest(String),
    Internal(String),
}

impl From<AuthError> for ServiceError {
    fn from(_: AuthError) -> Self {
        Self::Auth
    }
}

impl From<StorageError> for ServiceError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::NotFound => Self::NotFound,
            StorageError::Forbidden => Self::Forbidden,
            StorageError::StaleParent => Self::StaleParent,
            StorageError::Internal(message) => Self::Internal(message),
        }
    }
}

impl From<ChatBusError> for ServiceError {
    fn from(error: ChatBusError) -> Self {
        match error {
            ChatBusError::InFlight => Self::InFlight,
            ChatBusError::Unavailable => Self::Internal("chat bus unavailable".to_owned()),
            ChatBusError::Payload(message) => Self::Internal(message),
        }
    }
}

impl IntoResponse for ServiceError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::Auth => (
                StatusCode::UNAUTHORIZED,
                ApiErrorCode::Unauthorized,
                "unauthorized".into(),
            ),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                ApiErrorCode::NotFound,
                "not found".into(),
            ),
            Self::Forbidden => (
                StatusCode::FORBIDDEN,
                ApiErrorCode::Forbidden,
                "forbidden".into(),
            ),
            Self::StaleParent => (
                StatusCode::CONFLICT,
                ApiErrorCode::StaleParent,
                "conversation changed; refresh before sending".into(),
            ),
            Self::InFlight => (
                StatusCode::CONFLICT,
                ApiErrorCode::InFlight,
                "conversation already has a running turn".into(),
            ),
            Self::InvalidRequest(message) => (
                StatusCode::BAD_REQUEST,
                ApiErrorCode::InvalidRequest,
                message,
            ),
            Self::Internal(message) => {
                tracing::error!(%message, "api request failed with internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ApiErrorCode::Internal,
                    "internal error".into(),
                )
            }
        };
        let body = ApiError {
            error: ApiErrorBody { code, message },
        };
        (status, Json(body)).into_response()
    }
}
