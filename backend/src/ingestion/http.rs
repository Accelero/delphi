use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

use crate::auth::AuthContext;
use crate::state::AppState;

use super::IngestRequest;

/// Roles permitted to push documents in via the HTTP path. Scheduler-driven
/// ingest skips this gate entirely (it has no AuthContext and is trusted as
/// a system identity inside the binary).
const INGESTER_ROLES: &[&str] = &["ingester", "owner"];

/// `POST /api/ingestion/documents`
///
/// Thin wrapper around [`super::IngestSink::ingest`]: deserialize body,
/// role-gate, delegate. The same `IngestSink` instance the in-process
/// scheduler uses serves this request.
pub async fn ingest_documents(
    State(state): State<AppState>,
    auth: AuthContext,
    Json(req): Json<IngestRequest>,
) -> Response {
    let allowed = auth
        .roles
        .iter()
        .any(|r| INGESTER_ROLES.contains(&r.as_str()));
    if !allowed {
        return (StatusCode::FORBIDDEN, "ingester role required").into_response();
    }
    match state.sink.ingest(req).await {
        Ok(outcome) => (StatusCode::OK, Json(outcome)).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "ingestion failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "ingestion failed").into_response()
        }
    }
}
