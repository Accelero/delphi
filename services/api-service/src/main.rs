use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use delphi_auth::{AuthContext, AuthError, AuthVerifier};
use delphi_config::{init_tracing, ServiceConfig};
use delphi_contracts::{
    ApiError, ApiErrorBody, ApiErrorCode, AuthUser, ConversationDetail, ConversationSummary,
    CreateConversationRequest, SubmitChatMessageRequest, SubmitTurnRequest, TurnAccepted,
    TurnRequested, UpdateConversationRequest, CONTRACT_VERSION,
};
use delphi_nats::{ChatBus, ChatBusError, ChatLock, NatsChatBus, NatsChatBusOptions};
use delphi_storage::{ChatRepository, StorageError, SurrealChatRepository};
use tower_http::trace::TraceLayer;

#[derive(Clone)]
struct AppState {
    auth: AuthVerifier,
    repo: SurrealChatRepository,
    bus: NatsChatBus,
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
        repo: SurrealChatRepository::connect(
            &config.surreal_url,
            &config.surreal_namespace,
            &config.surreal_database,
            &config.surreal_user,
            &config.surreal_password,
        )
        .await?,
        bus: NatsChatBus::connect(&config.nats_url, NatsChatBusOptions::default()).await?,
    };

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
            conversation_id.clone(),
            turn_id.clone(),
        ))
        .await?;

    if let Err(error) = state
        .repo
        .record_turn_requested(
            &auth.tenant_id,
            &auth.user_id,
            &conversation_id,
            &turn_id,
            &user_message_id,
            parent_message_id.as_deref(),
        )
        .await
    {
        state
            .bus
            .release_lock(&auth.tenant_id, &conversation_id, &turn_id)
            .await;
        return Err(error.into());
    }

    let command = TurnRequested {
        v: CONTRACT_VERSION,
        command_id: turn_id.clone(),
        tenant_id: auth.tenant_id.clone(),
        user_id: auth.user_id.clone(),
        conversation_id: conversation_id.clone(),
        turn_id: turn_id.clone(),
        user_message_id,
        text,
        parent_message_id,
        bearer_subject: auth.bearer_subject.clone(),
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
