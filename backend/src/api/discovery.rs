//! Discovery feed API.
//!
//! Three handlers, all mounted under `/api/discovery`:
//!
//! - `GET  /api/discovery/feed?sort=recency&cursor=<opaque>&limit=N`
//!   Cursor-paginated list of documents with per-user read state.
//! - `POST /api/discovery/items/{key}/read` — mark read.
//! - `DELETE /api/discovery/items/{key}/read` — mark unread.
//! - `GET  /api/discovery/feed/events` — server-sent events stream that
//!   pushes a `new_document` record every time an ingest produces a
//!   `Created` outcome (see `ingestion::NotifyingSink`).
//!
//! `{key}` is the bare SurrealDB record key (everything after
//! `document:`), so the URL path stays free of the table prefix and
//! survives client-side URL handling.

use std::convert::Infallible;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use surrealdb::{Datetime, RecordId};
use tokio::sync::broadcast;

use crate::auth::AuthContext;
use crate::ingestion::NewDocumentEvent;
use crate::state::AppState;
use crate::storage::{FeedCursor, FeedItem};

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 200;

#[derive(Debug, Deserialize)]
pub struct FeedQuery {
    /// Sort algorithm. Only `recency` is recognised today; unknown
    /// values fall through to the default rather than 400-ing, so the
    /// frontend can ship new sort modes without a backend gate.
    #[serde(default)]
    pub sort: Option<String>,
    /// Opaque cursor produced by a previous response. Decoding errors
    /// (truncated, tampered) → 400.
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct FeedResponse {
    pub items: Vec<FeedItem>,
    /// Present iff the page was full — i.e. there *might* be more.
    /// `None` signals end-of-feed and the client should hide "load more".
    pub next_cursor: Option<String>,
}

/// Wire-format for the cursor before base64. Decoupled from the
/// internal [`FeedCursor`] so we can change the storage shape without
/// changing the wire shape, and vice versa.
#[derive(Debug, Serialize, Deserialize)]
struct CursorWire {
    /// Document `ingested_at` of the last item in the previous page,
    /// as RFC3339.
    p: String,
    /// Document record key (no table prefix).
    i: String,
}

pub async fn feed(
    State(state): State<AppState>,
    auth: AuthContext,
    Query(q): Query<FeedQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    // Only one sort algorithm today; the field is part of the wire so
    // future sorts (`relevance`, `score`) drop in without an API change.
    let _sort = q.sort.as_deref().unwrap_or("recency");
    let cursor = match q.cursor.as_deref() {
        Some(s) => match decode_cursor(s) {
            Ok(c) => Some(c),
            Err(e) => {
                tracing::debug!(error = %e, "invalid feed cursor");
                return (StatusCode::BAD_REQUEST, "invalid cursor").into_response();
            }
        },
        None => None,
    };

    let items = match state.storage.list_feed(&auth.user_id, cursor, limit).await {
        Ok(items) => items,
        Err(e) => {
            tracing::error!(error = %e, "feed query failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "feed query failed")
                .into_response();
        }
    };

    let next_cursor = if items.len() == limit {
        items.last().and_then(|item| {
            let ts = item.document.ingested_at.as_ref()?;
            let id = item.document.id.as_ref()?;
            Some(encode_cursor(ts.clone(), id.clone()))
        })
    } else {
        None
    };

    (
        StatusCode::OK,
        Json(FeedResponse { items, next_cursor }),
    )
        .into_response()
}

pub async fn mark_read(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(key): Path<String>,
) -> Response {
    let doc_id = RecordId::from(("document", key.as_str()));
    match state.storage.mark_read(&auth.user_id, &doc_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "mark_read failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "mark_read failed").into_response()
        }
    }
}

pub async fn mark_unread(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(key): Path<String>,
) -> Response {
    let doc_id = RecordId::from(("document", key.as_str()));
    match state.storage.mark_unread(&auth.user_id, &doc_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "mark_unread failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "mark_unread failed").into_response()
        }
    }
}

/// Subscribes to the new-document broadcast and emits one
/// `event: new_document` SSE record per incoming event. Drops on
/// `Lagged` (slow consumer); ends on `Closed` (channel torn down).
///
/// `KeepAlive::default()` injects a comment line periodically so
/// idle connections stay open through proxies.
pub async fn events(
    State(state): State<AppState>,
    _auth: AuthContext,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.events.subscribe();
    Sse::new(broadcast_to_sse(rx)).keep_alive(KeepAlive::default())
}

fn broadcast_to_sse(
    rx: broadcast::Receiver<NewDocumentEvent>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    futures::stream::unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    // `json_data` only fails if the value can't be
                    // serialized — `NewDocumentEvent` derives Serialize
                    // and contains no exotic types, so the unwrap is
                    // sound. If it ever isn't, we'd rather see the panic
                    // in tests than silently drop events.
                    let sse = Event::default()
                        .event("new_document")
                        .json_data(&event)
                        .expect("NewDocumentEvent must serialize");
                    return Some((Ok(sse), rx));
                }
                // Slow client missed N events — skip the gap, keep going.
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(missed = n, "discovery SSE client lagged");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    })
}

fn encode_cursor(ingested_at: Datetime, id: RecordId) -> String {
    // surrealdb::Datetime's Display is not RFC3339; round-trip via the
    // inner core type, which exposes Into<DateTime<Utc>>.
    let chrono_dt: chrono::DateTime<chrono::Utc> = ingested_at.into_inner().into();
    let wire = CursorWire {
        p: chrono_dt.to_rfc3339(),
        i: id.key().to_string(),
    };
    let json = serde_json::to_vec(&wire).expect("CursorWire serialize");
    URL_SAFE_NO_PAD.encode(json)
}

fn decode_cursor(s: &str) -> std::result::Result<FeedCursor, CursorError> {
    let bytes = URL_SAFE_NO_PAD.decode(s).map_err(|_| CursorError)?;
    let wire: CursorWire = serde_json::from_slice(&bytes).map_err(|_| CursorError)?;
    let parsed = chrono::DateTime::parse_from_rfc3339(&wire.p).map_err(|_| CursorError)?;
    let utc = parsed.with_timezone(&chrono::Utc);
    Ok(FeedCursor {
        ingested_at: Datetime::from(utc),
        id: RecordId::from(("document", wire.i.as_str())),
    })
}

#[derive(Debug, thiserror::Error)]
#[error("invalid cursor")]
pub struct CursorError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_roundtrip() {
        let id = RecordId::from(("document", "abc123"));
        let now = chrono::Utc::now();
        let ts = Datetime::from(now);
        let encoded = encode_cursor(ts, id);
        let decoded = decode_cursor(&encoded).expect("decode");
        assert_eq!(decoded.id.key().to_string(), "abc123");
        let decoded_chrono: chrono::DateTime<chrono::Utc> = decoded.ingested_at.into_inner().into();
        assert_eq!(decoded_chrono, now);
    }

    #[test]
    fn invalid_cursor_rejected() {
        assert!(decode_cursor("not-base64!").is_err());
        assert!(decode_cursor("YWJj").is_err()); // valid base64, not JSON
    }
}
