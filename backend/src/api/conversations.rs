//! Conversation CRUD under `/api/chat/conversations`.
//!
//! Sidebar / list / rename / delete live here. The actual streaming
//! message endpoint (`POST /api/chat/conversations/{id}/messages`)
//! lives in [`super::chat`] because its lifecycle (LLM streaming,
//! best-effort title generation) is too entangled with the chat module
//! to split off without churning the call graph.
//!
//! All queries run through the per-request [`AuthedDb`], so engine-side
//! PERMISSIONS scope reads/writes to the caller's `(tenant_id, user)`.
//! No application-side tenant filtering.

use std::sync::Arc;

use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use surrealdb::RecordId;

use crate::storage::{AuthedDb, ChatMessage, Conversation, ConversationId, Storage};

/// Reject obviously-malformed keys at the API boundary (no `:`, no
/// surrounding whitespace, non-empty). Lets handlers assume their
/// `key` is at least syntactically sane before they try to build a
/// `RecordId` from it.
fn parse_conversation_id(key: &str) -> Result<ConversationId, Response> {
    let k = key.trim();
    if k.is_empty() || k.contains(':') || k.len() != key.len() {
        return Err((StatusCode::BAD_REQUEST, "invalid conversation key").into_response());
    }
    Ok(RecordId::from(("conversation", k)))
}

pub async fn list(Extension(db): Extension<Arc<AuthedDb>>) -> Response {
    match db.list_conversations().await {
        Ok(items) => (StatusCode::OK, Json(items)).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "list_conversations failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "list failed").into_response()
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct CreateRequest {
    #[serde(default)]
    pub title: Option<String>,
}

pub async fn create(
    Extension(db): Extension<Arc<AuthedDb>>,
    body: Option<Json<CreateRequest>>,
) -> Response {
    let title = body.and_then(|Json(b)| b.title);
    let id = match db.create_conversation(title.as_deref()).await {
        Ok(id) => id,
        Err(e) => {
            tracing::error!(error = %e, "create_conversation failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "create failed").into_response();
        }
    };
    match db.get_conversation(&id).await {
        Ok(Some(c)) => (StatusCode::CREATED, Json(c)).into_response(),
        Ok(None) => {
            tracing::error!("conversation disappeared after create");
            (StatusCode::INTERNAL_SERVER_ERROR, "create failed").into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "get_conversation after create failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "create failed").into_response()
        }
    }
}

#[derive(Debug, Serialize)]
pub struct GetResponse {
    pub conversation: Conversation,
    pub messages: Vec<ChatMessage>,
}

pub async fn get(Extension(db): Extension<Arc<AuthedDb>>, Path(key): Path<String>) -> Response {
    let id = match parse_conversation_id(&key) {
        Ok(id) => id,
        Err(r) => return r,
    };
    let conversation = match db.get_conversation(&id).await {
        Ok(Some(c)) => c,
        Ok(None) => return (StatusCode::NOT_FOUND, "not found").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "get_conversation failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response();
        }
    };
    let messages = match db.list_messages(&id).await {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "list_messages failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response();
        }
    };
    (
        StatusCode::OK,
        Json(GetResponse {
            conversation,
            messages,
        }),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
pub struct PatchRequest {
    pub title: String,
}

pub async fn patch(
    Extension(db): Extension<Arc<AuthedDb>>,
    Path(key): Path<String>,
    Json(body): Json<PatchRequest>,
) -> Response {
    let id = match parse_conversation_id(&key) {
        Ok(id) => id,
        Err(r) => return r,
    };
    let title = body.title.trim();
    if title.is_empty() || title.chars().count() > 200 {
        return (StatusCode::BAD_REQUEST, "title must be 1..=200 chars").into_response();
    }
    // Pre-check existence so the response is a useful 404 instead of a
    // silent no-op when the row is missing (or PERMISSIONS hide it).
    match db.get_conversation(&id).await {
        Ok(Some(_)) => {}
        Ok(None) => return (StatusCode::NOT_FOUND, "not found").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "patch: get_conversation failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "patch failed").into_response();
        }
    }
    if let Err(e) = db.rename_conversation(&id, title).await {
        tracing::error!(error = %e, "rename_conversation failed");
        return (StatusCode::INTERNAL_SERVER_ERROR, "patch failed").into_response();
    }
    StatusCode::NO_CONTENT.into_response()
}

pub async fn delete(Extension(db): Extension<Arc<AuthedDb>>, Path(key): Path<String>) -> Response {
    let id = match parse_conversation_id(&key) {
        Ok(id) => id,
        Err(r) => return r,
    };
    // Idempotent: missing → still 204. We don't pre-check existence;
    // the cascade is a no-op when the row isn't there.
    if let Err(e) = db.delete_conversation(&id).await {
        tracing::error!(error = %e, "delete_conversation failed");
        return (StatusCode::INTERNAL_SERVER_ERROR, "delete failed").into_response();
    }
    StatusCode::NO_CONTENT.into_response()
}
