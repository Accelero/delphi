//! Submit one user message to a persisted conversation.
//!
//! Route: `POST /api/chat/conversations/{id}/messages`.
//!
//! Lifecycle (post-redesign — see
//! [`docs/architecture/chat-streaming.md`](../../../docs/architecture/chat-streaming.md)):
//!
//!  1. Verify the conversation exists for this caller (PERMISSIONS gate).
//!  2. Persist the user message synchronously — a crash here still
//!     leaves the user's words in the log.
//!  3. Look up (or create) the live [`SessionState`] for this
//!     conversation in the [`SessionRegistry`].
//!  4. Spawn the per-turn worker. The worker checks out its own
//!     `AuthedDb` from the pool, runs RAG + LLM, and commits the
//!     assistant message — all detached from this request.
//!  5. Return **202 Accepted** with an empty body.
//!
//! The response is *not* the LLM stream. Clients subscribe to
//! `GET /api/chat/conversations/{id}/stream` to receive bytes, and
//! that subscription survives both the POST request and the tab that
//! sent it.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::Deserialize;
use std::sync::Arc;
use surrealdb::RecordId;
use tracing::{error, info};

use crate::auth::AuthContext;
use crate::chat::{spawn_worker, turn_request};
use crate::state::AppState;
use crate::storage::{AuthedDb, ConversationId, Storage};

/// Body sent by the SPA's submit hook. Extra fields are ignored.
#[derive(Debug, Deserialize)]
pub struct ChatRequest {
    #[serde(default)]
    pub messages: Vec<ChatRequestMessage>,
}

#[derive(Debug, Deserialize)]
pub struct ChatRequestMessage {
    pub role: String,
    #[serde(default)]
    pub content: String,
    /// Some clients send the AI-SDK v3+ `parts` array; we flatten any
    /// text parts into `content` when `content` is empty.
    #[serde(default)]
    pub parts: Vec<MessagePart>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum MessagePart {
    Text {
        #[serde(default)]
        text: String,
    },
    #[serde(other)]
    Other,
}

impl ChatRequestMessage {
    fn collapse_text(&self) -> String {
        if !self.content.is_empty() {
            return self.content.clone();
        }
        self.parts
            .iter()
            .filter_map(|p| match p {
                MessagePart::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("")
    }
}

fn parse_conversation_id(key: &str) -> Result<ConversationId, Response> {
    let k = key.trim();
    if k.is_empty() || k.contains(':') || k.len() != key.len() {
        return Err((StatusCode::BAD_REQUEST, "invalid conversation key").into_response());
    }
    Ok(RecordId::from(("conversation", k)))
}

/// Extract `Authorization: Bearer <jwt>` from the request headers. The
/// identity middleware has already validated it; we just need the
/// string so the spawned worker can re-authenticate its own pool
/// checkout.
fn bearer_from_headers(headers: &axum::http::HeaderMap) -> Option<String> {
    let v = headers.get(axum::http::header::AUTHORIZATION)?.to_str().ok()?;
    let s = v
        .strip_prefix("Bearer ")
        .or_else(|| v.strip_prefix("bearer "))?;
    Some(s.to_string())
}

/// POST handler. Persists the user message, spawns the worker, returns
/// `202 Accepted`. The streaming reply is delivered out-of-band via
/// `GET /api/chat/conversations/{id}/stream` (a separate subscription
/// the SPA opens on session-page mount).
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

    // 1. Verify the conversation exists for this caller.
    let conversation = match db.get_conversation(&conv_id).await {
        Ok(Some(c)) => c,
        Ok(None) => return (StatusCode::NOT_FOUND, "conversation not found").into_response(),
        Err(e) => {
            error!(error = %e, "get_conversation failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response();
        }
    };

    // 2. Pull the trailing user message out of the request body.
    let last_user_text = req
        .messages
        .iter()
        .rev()
        .find(|m| m.role == "user")
        .map(|m| m.collapse_text())
        .unwrap_or_default();
    if last_user_text.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "no user message").into_response();
    }

    // 3. Persist the user message synchronously. A crash before the
    //    worker commits the assistant reply still leaves the user's
    //    message in the log; the user can resubmit.
    if let Err(e) = db.append_message(&conv_id, "user", &last_user_text).await {
        error!(error = %e, "append user message failed");
        return (StatusCode::INTERNAL_SERVER_ERROR, "append failed").into_response();
    }

    // 4. Capture the bearer so the worker can check out its own
    //    `AuthedDb`. The middleware already validated it.
    let bearer = match bearer_from_headers(&headers) {
        Some(b) => b,
        None => {
            // The identity middleware would have already 401'd if the
            // bearer were missing, but be paranoid.
            return (StatusCode::UNAUTHORIZED, "missing bearer").into_response();
        }
    };

    // 5. Get-or-create the per-conversation SessionState and spawn the
    //    worker. The worker holds its own `Arc<SessionState>` plus a
    //    `turn_lock` permit; this request can return immediately.
    let session = state.session_registry.get_or_create(&conv_id).await;
    let turn = turn_request(
        conv_id.clone(),
        last_user_text,
        conversation.title.is_some(),
        bearer,
        auth.clone(),
        &state,
    );
    spawn_worker(session, turn);

    info!(
        user_id = %auth.user_id,
        conversation = %conv_id,
        "turn submitted"
    );

    StatusCode::ACCEPTED.into_response()
}
