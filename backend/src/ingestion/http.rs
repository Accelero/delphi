use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use surrealdb::RecordId;

use crate::auth::AuthContext;
use crate::state::AppState;

use super::IngestRequest;

/// Roles permitted to push documents in via the HTTP path. Scheduler-driven
/// ingest skips this gate entirely (it has no AuthContext and is trusted as
/// a system identity inside the binary).
const INGESTER_ROLES: &[&str] = &["ingester", "owner"];

/// Wire shape: identical to [`IngestRequest`] except `tenant_id` is
/// **not** read from the request body. The handler stamps it from
/// `AuthContext.tenant_id` so a caller can never smuggle a foreign
/// tenant via the JSON payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestRequestBody {
    pub canonical_id: String,
    pub source_type: String,
    pub source_uri: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub authors: Vec<String>,
    #[serde(default)]
    pub published_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub raw_text: Option<String>,
    #[serde(default)]
    pub storage_uri: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

impl IngestRequestBody {
    fn into_request(self, tenant_id: RecordId) -> IngestRequest {
        IngestRequest {
            tenant_id,
            canonical_id: self.canonical_id,
            source_type: self.source_type,
            source_uri: self.source_uri,
            title: self.title,
            authors: self.authors,
            published_at: self.published_at,
            language: self.language,
            summary: self.summary,
            raw_text: self.raw_text,
            storage_uri: self.storage_uri,
            metadata: self.metadata,
        }
    }
}

/// `POST /api/ingestion/documents`
///
/// Thin wrapper around [`super::IngestSink::ingest`]: deserialize body,
/// role-gate, stamp tenant from auth, delegate. The same `IngestSink`
/// instance the in-process scheduler uses serves this request.
pub async fn ingest_documents(
    State(state): State<AppState>,
    auth: AuthContext,
    Json(body): Json<IngestRequestBody>,
) -> Response {
    let allowed = auth
        .roles
        .iter()
        .any(|r| INGESTER_ROLES.contains(&r.as_str()));
    if !allowed {
        return (StatusCode::FORBIDDEN, "ingester role required").into_response();
    }
    let req = body.into_request(auth.tenant_id);
    match state.sink.ingest(req).await {
        Ok(outcome) => (StatusCode::OK, Json(outcome)).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "ingestion failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "ingestion failed").into_response()
        }
    }
}
