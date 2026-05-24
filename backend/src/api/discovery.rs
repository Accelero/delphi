//! Discovery feed API.
//!
//! Two handlers, both mounted under `/api/discovery`:
//!
//! - `GET  /api/discovery/feed?sort=recency&cursor=<opaque>&limit=N`
//!   Cursor-paginated list of documents.
//! - `GET  /api/discovery/feed/events` — server-sent events stream that
//!   pushes a `new_document` record every time an ingest produces a
//!   `Created` outcome (see `ingestion::NotifyingSink`).
//!
//! Tenant scoping for `feed` is engine-side: the request's
//! JWT-authenticated `AuthedDb` runs every query under a RECORD
//! session whose PERMISSIONS clauses constrain it to its own tenant.
//! The SSE handler filters the broadcast stream against the
//! connecting user's tenant claim (read from `AuthContext` at
//! connection setup) and force-closes after ~1h with jitter so
//! reconnects pick up any tenant/role/revocation changes.

use std::convert::Infallible;
use std::time::Duration;

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Utc};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use surrealdb::types::RecordId;
use tokio::sync::broadcast;

use crate::auth::AuthContext;
use crate::ingestion::FeedItemEvent;
use crate::state::AppState;
use crate::storage::{AuthedDb, Document, FeedCursor, Storage};

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 200;

/// SSE connection lifetime: backend closes the stream after this and
/// relies on EventSource's auto-reconnect to drive a fresh auth round.
/// Plus jitter so a server restart doesn't trigger a herd reconnect.
const SSE_LIFETIME: Duration = Duration::from_secs(3600);
const SSE_JITTER: Duration = Duration::from_secs(300);

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
    pub items: Vec<Document>,
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
    Extension(db): Extension<Arc<AuthedDb>>,
    Query(q): Query<FeedQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
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

    let items = match db.list_feed(cursor, limit).await {
        Ok(items) => items,
        Err(e) => {
            tracing::error!(error = %e, "feed query failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "feed query failed").into_response();
        }
    };

    let next_cursor = if items.len() == limit {
        items.last().and_then(|doc| {
            let ts = doc.ingested_at.as_ref()?;
            let id = doc.id.as_ref()?;
            Some(encode_cursor(*ts, id.clone()))
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

/// Subscribes to the new-document broadcast and emits one
/// `event: new_document` SSE record per incoming event whose tenant
/// matches the caller's. Drops on `Lagged` (slow consumer); ends on
/// `Closed`. Force-closes after `SSE_LIFETIME ± SSE_JITTER` so the
/// browser auto-reconnects through fresh auth.
///
/// `KeepAlive::default()` injects a comment line periodically so
/// idle connections stay open through proxies.
pub async fn events(
    State(state): State<AppState>,
    auth: AuthContext,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.events.subscribe();
    let deadline = tokio::time::Instant::now() + sse_lifetime_with_jitter();
    Sse::new(broadcast_to_sse(rx, auth.tenant_id, deadline)).keep_alive(KeepAlive::default())
}

fn sse_lifetime_with_jitter() -> Duration {
    // Cheap "jitter": derive ± offset from a 64-bit nanosecond clock
    // sample. Spreads reconnects across a 5-minute window after a
    // restart without pulling in a full RNG dependency.
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let offset_ms = now_ns % (SSE_JITTER.as_millis() as u64 * 2);
    let signed_offset = offset_ms as i64 - SSE_JITTER.as_millis() as i64;
    let base = SSE_LIFETIME.as_millis() as i64;
    Duration::from_millis((base + signed_offset).max(60_000) as u64)
}

fn broadcast_to_sse(
    rx: broadcast::Receiver<FeedItemEvent>,
    tenant: RecordId,
    deadline: tokio::time::Instant,
) -> impl Stream<Item = Result<Event, Infallible>> {
    futures::stream::unfold(
        (rx, tenant, deadline),
        |(mut rx, tenant, deadline)| async move {
            loop {
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(deadline) => return None,
                    recv = rx.recv() => match recv {
                        Ok(event) if event.document.tenant_id.as_ref() == Some(&tenant) => {
                            // Send the Document itself — same wire shape as
                            // /api/discovery/feed items, so SPA receivers
                            // can prepend it directly into the cache without
                            // a refetch. `json_data` only fails on non-
                            // serializable values; Document is plain data.
                            let sse = Event::default()
                                .event("new_document")
                                .json_data(&event.document)
                                .expect("Document must serialize");
                            return Some((Ok(sse), (rx, tenant, deadline)));
                        }
                        Ok(_) => continue, // event for a different tenant — drop
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(missed = n, "discovery SSE client lagged");
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => return None,
                    }
                }
            }
        },
    )
}

fn encode_cursor(ingested_at: DateTime<Utc>, id: RecordId) -> String {
    let wire = CursorWire {
        p: ingested_at.to_rfc3339(),
        i: crate::storage::record_key(&id),
    };
    let json = serde_json::to_vec(&wire).expect("CursorWire serialize");
    URL_SAFE_NO_PAD.encode(json)
}

fn decode_cursor(s: &str) -> std::result::Result<FeedCursor, CursorError> {
    let bytes = URL_SAFE_NO_PAD.decode(s).map_err(|_| CursorError)?;
    let wire: CursorWire = serde_json::from_slice(&bytes).map_err(|_| CursorError)?;
    let parsed = chrono::DateTime::parse_from_rfc3339(&wire.p).map_err(|_| CursorError)?;
    let utc = parsed.with_timezone(&Utc);
    Ok(FeedCursor {
        ingested_at: utc,
        id: RecordId::new("document", wire.i.as_str()),
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
        let id = RecordId::new("document", "abc123");
        let now = Utc::now();
        let encoded = encode_cursor(now, id);
        let decoded = decode_cursor(&encoded).expect("decode");
        assert_eq!(crate::storage::record_key(&decoded.id), "abc123");
        assert_eq!(decoded.ingested_at, now);
    }

    #[test]
    fn invalid_cursor_rejected() {
        assert!(decode_cursor("not-base64!").is_err());
        assert!(decode_cursor("YWJj").is_err()); // valid base64, not JSON
    }
}
