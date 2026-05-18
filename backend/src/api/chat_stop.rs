//! Stop the in-flight turn for a conversation.
//!
//! Route: `POST /api/chat/conversations/{key}/stop`.
//!
//! Idempotent: returns `204 No Content` whether or not there was an
//! in-flight turn. Any caller authenticated for the conversation can
//! hit it. **Conversation-scoped — no task id in the public API.**
//! Since v3 allows only one in-flight turn per conversation, there is
//! no ambiguity about which turn gets cancelled.
//!
//! The PERMISSIONS pre-check (via `get_conversation`) keeps anonymous
//! callers from probing for valid conversation keys.

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

pub async fn stop(
    State(app): State<AppState>,
    Extension(db): Extension<Arc<AuthedDb>>,
    Path(conv_key): Path<String>,
) -> Response {
    let conv_id = match parse_conversation_id(&conv_key) {
        Ok(id) => id,
        Err(r) => return r,
    };

    // PERMISSIONS gate: a caller who can't see the conversation gets
    // the same 404 they'd see anywhere else, rather than a 204 leak.
    match db.get_conversation(&conv_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return (StatusCode::NOT_FOUND, "not found").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "stop: get_conversation failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response();
        }
    }

    // Look up the session WITHOUT creating one — if no turn has ever
    // touched this conversation, there is nothing to stop.
    if let Some(session) = app.sessions.lookup(&conv_id) {
        session.abort();
    }
    StatusCode::NO_CONTENT.into_response()
}
