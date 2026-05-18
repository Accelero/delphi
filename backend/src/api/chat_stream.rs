//! Long-lived per-conversation SSE subscription.
//!
//! Route: `GET /api/chat/conversations/{key}/stream`.
//!
//! Every tab opens one of these on mount. The server replays the
//! current turn's buffered SSE frames (if any) and then forwards live
//! frames as the worker emits them.
//!
//! ### Pool starvation fix (critical)
//!
//! The identity middleware attaches an `Arc<AuthedDb>` extension that
//! lives for the entire request. For a long-lived SSE response, that
//! would mean one pool slot held per open tab — with default
//! `REQUEST_DB_POOL_SIZE=8`, the pool deadlocks after ~9 tabs.
//!
//! The fix is structural: perform the `get_conversation` permission
//! check up front while we still hold the handle, then **explicitly
//! drop the extension** before constructing the streaming body. The
//! streaming body itself only fans bytes from the session's mpsc into
//! the SSE response and doesn't touch the DB.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::StreamExt;
use surrealdb::RecordId;
use tokio_stream::wrappers::ReceiverStream;

use crate::state::AppState;
use crate::storage::{AuthedDb, ConversationId, Storage};

fn parse_conversation_id(key: &str) -> Result<ConversationId, Response> {
    let k = key.trim();
    if k.is_empty() || k.contains(':') || k.len() != key.len() {
        return Err((StatusCode::BAD_REQUEST, "invalid conversation key").into_response());
    }
    Ok(RecordId::from(("conversation", k)))
}

pub async fn stream(
    State(state): State<AppState>,
    Path(key): Path<String>,
    mut req: Request,
) -> Response {
    let conv_id = match parse_conversation_id(&key) {
        Ok(id) => id,
        Err(r) => return r,
    };

    // Permission check while we still hold the pooled handle. We grab
    // it out of the extensions ourselves (rather than via
    // `Extension(db)` in the signature) so we can drop it explicitly
    // before the streaming body starts.
    let db = match req.extensions_mut().remove::<Arc<AuthedDb>>() {
        Some(d) => d,
        None => {
            // Identity middleware always attaches this on protected
            // routes; absence is a programmer error, not a user one.
            tracing::error!("chat stream: AuthedDb extension missing");
            return (StatusCode::INTERNAL_SERVER_ERROR, "auth missing").into_response();
        }
    };
    match db.get_conversation(&conv_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return (StatusCode::NOT_FOUND, "not found").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "stream: get_conversation failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response();
        }
    }
    // Return the pool slot BEFORE we start streaming. The Arc may have
    // other clones (the request extension wrapper) — explicitly drop
    // ours; the wrapper's clone goes when `req` falls out of scope at
    // the end of this function. By the time the streaming body runs,
    // both are gone and the `AuthedDb` has been released.
    drop(db);

    let session = state.sessions.for_conversation(&conv_id);
    let rx = session.subscribe();
    let stream = ReceiverStream::new(rx).map(Ok::<Bytes, Infallible>);

    // We build the response body manually rather than via axum's
    // `Sse<Stream<Item = Event>>` adapter because our session emits
    // already-formatted SSE frames as `Bytes` (so we can replay them
    // verbatim to late subscribers). We add an explicit 15s heartbeat
    // task via a `tokio::time::interval` that injects `:\n\n` comment
    // lines through a fan-in stream.
    let heartbeat = futures::stream::unfold((), |()| async {
        tokio::time::sleep(Duration::from_secs(15)).await;
        Some((Ok::<Bytes, Infallible>(Bytes::from_static(b":\n\n")), ()))
    });
    let body = futures::stream::select(stream, heartbeat);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        // `X-Accel-Buffering: no` is the de-facto signal to nginx /
        // Traefik-with-buffering / oauth2-proxy to stop coalescing.
        .header("x-accel-buffering", "no")
        .body(Body::from_stream(body))
        .expect("static headers, valid body")
}
