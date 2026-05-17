//! Stop the in-flight turn for a conversation.
//!
//! Route: `POST /api/chat/conversations/{id}/stop`.
//!
//! Idempotent: returns `204 No Content` whether or not a worker was
//! actually running. Any tab attached to the conversation can call this;
//! all of them see the worker's `proto::finish("stop")` frame on the
//! same open stream the moment the worker reacts.
//!
//! See [`docs/architecture/chat-streaming.md`](../../../docs/architecture/chat-streaming.md)
//! § Stop button for the design.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use surrealdb::RecordId;

use crate::state::AppState;
use crate::storage::{AuthedDb, ConversationId, Storage};

fn parse_conversation_id(key: &str) -> Result<ConversationId, Response> {
    let k = key.trim();
    if k.is_empty() || k.contains(':') || k.len() != key.len() {
        return Err((StatusCode::BAD_REQUEST, "invalid conversation key").into_response());
    }
    Ok(RecordId::from(("conversation", k)))
}

/// POST handler. Cancels the per-turn token if one is installed; returns
/// 204 either way. The worker reacts asynchronously: its `tokio::select!`
/// breaks on `cancel.cancelled()`, the trailing `proto::finish("stop")`
/// frame goes into the buffer, the partial assistant reply is persisted,
/// and the semaphore permit drops so any queued submission proceeds.
pub async fn stop_message(
    State(app): State<AppState>,
    Extension(db): Extension<Arc<AuthedDb>>,
    Path(key): Path<String>,
) -> Response {
    let conv_id = match parse_conversation_id(&key) {
        Ok(id) => id,
        Err(r) => return r,
    };

    // PERMISSIONS gate: identical to the GET handlers — a caller without
    // visibility on this conversation gets the same 404 they'd see
    // anywhere else, rather than a 204 leak.
    match db.get_conversation(&conv_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return (StatusCode::NOT_FOUND, "not found").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "stop: get_conversation failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response();
        }
    }

    if let Some(session) = app.session_registry.lookup(&conv_id).await {
        session.cancel_current_turn().await;
    }
    StatusCode::NO_CONTENT.into_response()
}
