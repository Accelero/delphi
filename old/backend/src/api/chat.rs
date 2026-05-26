//! Submit one user message to a persisted conversation.
//!
//! Route: `POST /api/chat/conversations/{key}/messages`.
//!
//! v4 contract (see
//! [`docs/architecture/chat-v4.md`](../../../docs/architecture/chat-v4.md)):
//! the POST is **fire-and-forget**. The handler:
//!
//!  1. Parses + validates `{ id, text, parent_id }`.
//!  2. PERMISSIONS gate + parent-id check on the caller's `AuthedDb`.
//!  3. Claims the in-flight slot via [`crate::chat::TurnBus::try_start`],
//!     which buffers the `user_message` SSE frame atomically. If a turn
//!     is already in flight for this conversation, returns **409**.
//!  4. Spawns the worker with the returned `TurnHandle`.
//!  5. Returns **202 Accepted** with an empty body.
//!
//! The client doesn't read this response for streaming bytes — every
//! tab subscribes to the per-conversation SSE stream and receives the
//! same buffered + live frames as everyone else.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::Deserialize;
use surrealdb::types::{RecordId, ToSql};
use tracing::{error, info};

use crate::api::sse;
use crate::auth::AuthContext;
use crate::chat::{spawn_worker, turn_request, TaskId};
use crate::state::AppState;
use crate::storage::{AuthedDb, ConversationId, MessageId, Storage};

/// Body sent by the SPA's submit hook.
#[derive(Debug, Deserialize)]
pub struct ChatRequest {
    /// Client-generated ULID for the user message. The server takes
    /// this verbatim as the record key (`message:<id>`).
    pub id: String,
    /// User text. Must be non-empty after trim.
    pub text: String,
    /// Last assistant message id the client knows about
    /// (`"message:k9d8…"`). `None` declares "first turn".
    #[serde(default)]
    pub parent_id: Option<String>,
}

fn parse_conversation_id(key: &str) -> Result<ConversationId, Response> {
    let k = key.trim();
    if k.is_empty() || k.contains(':') || k.len() != key.len() {
        return Err((StatusCode::BAD_REQUEST, "invalid conversation key").into_response());
    }
    Ok(RecordId::new("conversation", k))
}

/// Cheap syntactic check on the user-supplied ULID.
fn looks_like_ulid(s: &str) -> bool {
    ulid::Ulid::from_string(s).is_ok()
}

/// Parse a `parent_id` from the request body. Accepts `"message:<key>"`
/// only.
fn parse_parent_id(s: &str) -> Result<MessageId, Response> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "empty parent_id").into_response());
    }
    let (table, key) = trimmed.split_once(':').ok_or_else(|| {
        (StatusCode::BAD_REQUEST, "parent_id missing 'message:' prefix").into_response()
    })?;
    if table != "message" || key.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "parent_id must be 'message:<key>'").into_response());
    }
    Ok(RecordId::new("message", key))
}

fn bearer_from_headers(headers: &axum::http::HeaderMap) -> Option<String> {
    let v = headers.get(axum::http::header::AUTHORIZATION)?.to_str().ok()?;
    let s = v
        .strip_prefix("Bearer ")
        .or_else(|| v.strip_prefix("bearer "))?;
    Some(s.to_string())
}

pub async fn post_message(
    State(state): State<AppState>,
    Extension(db): Extension<Arc<AuthedDb>>,
    auth: AuthContext,
    headers: axum::http::HeaderMap,
    Path(key): Path<String>,
    Json(req): Json<ChatRequest>,
) -> Response {
    let conv_id = match parse_conversation_id(&key) {
        Ok(id) => id,
        Err(r) => return r,
    };

    if !looks_like_ulid(&req.id) {
        return (StatusCode::BAD_REQUEST, "invalid message id").into_response();
    }
    if req.text.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "empty text").into_response();
    }
    let parent_id: Option<MessageId> = match req.parent_id.as_deref() {
        Some(s) => match parse_parent_id(s) {
            Ok(p) => Some(p),
            Err(r) => return r,
        },
        None => None,
    };

    // PERMISSIONS gate + existence check.
    match db.get_conversation(&conv_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return (StatusCode::NOT_FOUND, "conversation not found").into_response(),
        Err(e) => {
            error!(error = %e, "get_conversation failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response();
        }
    };

    // Optimistic concurrency: parent_id must match the conversation's
    // current tail (or both must be None/empty).
    let history = match db.list_messages(&conv_id).await {
        Ok(m) => m,
        Err(e) => {
            error!(error = %e, "list_messages failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response();
        }
    };
    let tail_id = history.last().and_then(|m| m.id.as_ref());
    if tail_id != parent_id.as_ref() {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"reason": "stale_parent"})),
        )
            .into_response();
    }

    let bearer = match bearer_from_headers(&headers) {
        Some(b) => b,
        None => return (StatusCode::UNAUTHORIZED, "missing bearer").into_response(),
    };

    // Claim the in-flight slot, buffering the `user_message` frame
    // atomically. On AlreadyRunning we 409 — the client should wait for
    // the existing turn to finish (it sees the same live SSE stream).
    let user_record_id = format!("message:{}", req.id);
    let user_frame = sse::user_message(&user_record_id, &req.text);
    let handle = match state.turn_bus.try_start(&conv_id, user_frame).await {
        Ok(h) => h,
        Err(_already) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({"reason": "in_flight"})),
            )
                .into_response();
        }
    };

    let task_id = TaskId::new();
    let turn = turn_request(
        conv_id.clone(),
        req.id,
        req.text,
        parent_id,
        bearer,
        auth.clone(),
        &state,
    );
    spawn_worker(handle, task_id, turn);

    info!(
        user_id = %auth.user_id.to_sql(),
        conversation = %conv_id.to_sql(),
        task = %task_id,
        "turn submitted"
    );

    StatusCode::ACCEPTED.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ulid_validator_accepts_canonical_form() {
        assert!(looks_like_ulid("01HXY0000000000000000000ZZ"));
    }

    #[test]
    fn ulid_validator_rejects_short_or_garbage() {
        assert!(!looks_like_ulid(""));
        assert!(!looks_like_ulid("too short"));
        assert!(!looks_like_ulid("01HXY0000000000000000000Z!"));
        assert!(!looks_like_ulid("01HXY0000000000000000000II"));
    }

    #[test]
    fn parse_parent_id_round_trips() {
        let id = parse_parent_id("message:abc123").expect("ok");
        assert_eq!(id.to_sql(), "message:abc123");
    }

    #[test]
    fn parse_parent_id_rejects_wrong_table() {
        assert!(parse_parent_id("conversation:abc").is_err());
        assert!(parse_parent_id("not-a-record-id").is_err());
        assert!(parse_parent_id("message:").is_err());
    }
}
