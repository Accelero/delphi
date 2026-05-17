//! Stop a named chat-worker.
//!
//! Route: `POST /api/chat/conversations/{key}/tasks/{task_id}/stop`.
//!
//! Idempotent: returns `204 No Content` whether or not the task was
//! found. Any caller authenticated for the conversation can hit it.
//!
//! The PERMISSIONS pre-check (via `get_conversation`) is what keeps
//! anonymous callers from probing for valid task ids across
//! conversations they don't own — without it, a /stop would 204 for
//! any string and leak the task id space.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use surrealdb::RecordId;

use crate::chat::TaskId;
use crate::state::AppState;
use crate::storage::{AuthedDb, ConversationId, Storage};

fn parse_conversation_id(key: &str) -> Result<ConversationId, Response> {
    let k = key.trim();
    if k.is_empty() || k.contains(':') || k.len() != key.len() {
        return Err((StatusCode::BAD_REQUEST, "invalid conversation key").into_response());
    }
    Ok(RecordId::from(("conversation", k)))
}

pub async fn stop(
    State(app): State<AppState>,
    Extension(db): Extension<Arc<AuthedDb>>,
    Path((conv_key, task_key)): Path<(String, String)>,
) -> Response {
    let conv_id = match parse_conversation_id(&conv_key) {
        Ok(id) => id,
        Err(r) => return r,
    };

    // PERMISSIONS gate: same shape as the chat endpoints — a caller
    // who can't see the conversation gets the same 404 they'd see
    // anywhere else, rather than a 204 leak.
    match db.get_conversation(&conv_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return (StatusCode::NOT_FOUND, "not found").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "stop: get_conversation failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response();
        }
    }

    let task_id = match TaskId::parse(&task_key) {
        Ok(t) => t,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid task id").into_response(),
    };

    // Best-effort: no-op if the task is absent (already completed or
    // never existed). The design doc spells this out — the response is
    // 204 either way.
    let _ = app.tasks.cancel(&task_id);
    StatusCode::NO_CONTENT.into_response()
}
