//! Subscribe to the per-conversation byte log.
//!
//! Route: `GET /api/chat/conversations/{id}/stream`.
//!
//! The endpoint is a long-lived response whose body is whatever the
//! worker currently has in the session buffer, followed by anything the
//! worker appends thereafter. The framing on the wire is the unchanged
//! Vercel AI SDK data-stream format produced by [`crate::api::stream`].
//!
//! Lifecycle:
//!
//!  1. PERMISSIONS gate: try `get_conversation(id)` on the caller's
//!     [`AuthedDb`]. If it returns `None`, the caller can't see this
//!     conversation — respond 404. (Identical pre-check to what
//!     `conversations::get` performs.)
//!  2. Get-or-create the [`SessionState`] for `id` in the registry.
//!  3. Acquire `finalize_lock` *briefly* so the subscribe point is
//!     ordered against any in-flight worker commit. Without this an
//!     attaching reader could land between "worker emitted last byte"
//!     and "worker clears the buffer" and end up replaying bytes that
//!     are also already in the DB. Holding the lock during
//!     `state.subscribe()` rules that out.
//!  4. Return the response with the agreed AI SDK headers and a body
//!     stream backed by the new [`SessionReader`].
//!
//! The body has no EOF — it ends only when the client disconnects (axum
//! drops the body stream, which drops the `SessionReader`, which
//! decrements the `Arc<SessionState>` refcount).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use axum::body::Body;
use surrealdb::RecordId;
use tokio_util::io::ReaderStream;

use crate::state::AppState;
use crate::storage::{AuthedDb, ConversationId, Storage};

/// Parse the `{key}` path segment into a `conversation:<key>` record id.
/// Same rules as the other chat endpoints (no `:`, no whitespace,
/// non-empty) — kept local because it's a trivial helper and we want
/// `api/*` modules independent.
fn parse_conversation_id(key: &str) -> Result<ConversationId, Response> {
    let k = key.trim();
    if k.is_empty() || k.contains(':') || k.len() != key.len() {
        return Err((StatusCode::BAD_REQUEST, "invalid conversation key").into_response());
    }
    Ok(RecordId::from(("conversation", k)))
}

/// GET handler. Returns the live byte stream for the given conversation.
pub async fn stream(
    State(app): State<AppState>,
    Extension(db): Extension<Arc<AuthedDb>>,
    Path(key): Path<String>,
) -> Response {
    let conv_id = match parse_conversation_id(&key) {
        Ok(id) => id,
        Err(r) => return r,
    };

    // PERMISSIONS check via the caller's DB handle. A non-existent
    // conversation OR one the caller cannot see both surface as `None`
    // here — same indistinguishability we already rely on in `get`.
    match db.get_conversation(&conv_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return (StatusCode::NOT_FOUND, "not found").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "get_conversation failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response();
        }
    }

    // Attach under finalize_lock so the subscribe point is ordered
    // against any in-flight commit. The guard lifetime is the
    // subscribe() call — we don't hold it across the response body.
    let session = app.session_registry.get_or_create(&conv_id).await;
    let reader = {
        let _g = session.lock_finalize().await;
        session.subscribe()
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .header("x-vercel-ai-data-stream", "v1")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(ReaderStream::new(reader)))
        .expect("static headers, valid body")
}
