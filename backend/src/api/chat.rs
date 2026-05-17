//! Submit one user message to a persisted conversation.
//!
//! Route: `POST /api/chat/conversations/{id}/messages`.
//!
//! Post-redesign (see
//! [`docs/architecture/chat-streaming.md`](../../../docs/architecture/chat-streaming.md)):
//! the POST **is** the stream. The response body is the AI SDK
//! data-stream emitted by the worker spawned for this turn. There is no
//! separate `/stream` subscription.
//!
//! Lifecycle:
//!
//!  1. Parse + basic validation on `{ id, text, parent_id }`.
//!  2. PERMISSIONS gate: `get_conversation` on the caller's
//!     [`AuthedDb`].
//!  3. Optimistic concurrency check: `parent_id` must match the
//!     conversation's tail. Mismatch → `409 Conflict`.
//!  4. Spawn the worker; wrap its mpsc receiver in a streaming
//!     response body.
//!  5. Return 200 with the AI SDK headers and the stream as body.
//!
//! The first frame on the stream is the `8:` task frame, so the client
//! knows the `task_id` before any text arrives.

use std::convert::Infallible;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use bytes::Bytes;
use serde::Deserialize;
use surrealdb::RecordId;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt as _;
use tracing::{error, info};

use crate::auth::AuthContext;
use crate::chat::{spawn_worker, turn_request};
use crate::state::AppState;
use crate::storage::{AuthedDb, ConversationId, MessageId, Storage};

/// Body sent by the SPA's submit hook.
#[derive(Debug, Deserialize)]
pub struct ChatRequest {
    /// Client-generated ULID for the user message. The server takes
    /// this verbatim as the record key (`message:<id>`) so the
    /// optimistic insert and the persisted row agree from byte zero.
    pub id: String,
    /// User text. Must be non-empty after trim.
    pub text: String,
    /// Last assistant message id the client knows about, e.g.
    /// `"message:k9d8…"`. `None` (or absent) declares "first turn".
    #[serde(default)]
    pub parent_id: Option<String>,
}

fn parse_conversation_id(key: &str) -> Result<ConversationId, Response> {
    let k = key.trim();
    if k.is_empty() || k.contains(':') || k.len() != key.len() {
        return Err((StatusCode::BAD_REQUEST, "invalid conversation key").into_response());
    }
    Ok(RecordId::from(("conversation", k)))
}

/// Cheap syntactic check on the user-supplied ULID: 26 chars, valid
/// Crockford-base32 charset. Anything else → 400.
fn looks_like_ulid(s: &str) -> bool {
    if s.len() != 26 {
        return false;
    }
    // Crockford excludes I, L, O, U from the alphabet; we accept upper
    // and lower case (ULIDs are usually upper, but the spec is case-
    // insensitive on decode).
    s.bytes().all(|b| {
        matches!(b,
            b'0'..=b'9' |
            b'A'..=b'H' | b'J' | b'K' | b'M' | b'N' | b'P'..=b'T' | b'V'..=b'Z' |
            b'a'..=b'h' | b'j' | b'k' | b'm' | b'n' | b'p'..=b't' | b'v'..=b'z')
    })
}

/// Parse a `parent_id` from the request body. Accepts `"message:<key>"`
/// only; anything else is a 400 (the client never builds an id that
/// isn't a record id stringified).
fn parse_parent_id(s: &str) -> Result<MessageId, Response> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "empty parent_id").into_response());
    }
    let (table, key) = trimmed
        .split_once(':')
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "parent_id missing 'message:' prefix").into_response())?;
    if table != "message" || key.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "parent_id must be 'message:<key>'").into_response());
    }
    Ok(RecordId::from(("message", key)))
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

    // Validate the client-provided message id BEFORE we touch the DB.
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

    let turn = turn_request(
        conv_id.clone(),
        req.id,
        req.text,
        parent_id,
        bearer,
        auth.clone(),
        &state,
    );
    let (task_id, rx) = spawn_worker(state.tasks.clone(), turn);

    info!(
        user_id = %auth.user_id,
        conversation = %conv_id,
        task = %task_id,
        "turn submitted"
    );

    let stream = ReceiverStream::new(rx).map(Ok::<Bytes, Infallible>);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .header("x-vercel-ai-data-stream", "v1")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(stream))
        .expect("static headers, valid body")
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
        // 'I', 'L', 'O', 'U' are not in Crockford base32
        assert!(!looks_like_ulid("01HXY0000000000000000000II"));
    }

    #[test]
    fn parse_parent_id_round_trips() {
        let id = parse_parent_id("message:abc123").expect("ok");
        assert_eq!(id.to_string(), "message:abc123");
    }

    #[test]
    fn parse_parent_id_rejects_wrong_table() {
        assert!(parse_parent_id("conversation:abc").is_err());
        assert!(parse_parent_id("not-a-record-id").is_err());
        assert!(parse_parent_id("message:").is_err());
    }
}
