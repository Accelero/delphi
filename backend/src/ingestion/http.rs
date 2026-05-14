use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};

use crate::auth::AuthContext;
use crate::state::AppState;
use crate::storage::AuthedDb;

use super::{IngestRequest, IngestSink, NotifyingSink, Pipeline};

/// Roles permitted to push documents in via the HTTP path.
const INGESTER_ROLES: &[&str] = &["ingester", "owner"];

/// Wire shape: identical to [`IngestRequest`].
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

impl From<IngestRequestBody> for IngestRequest {
    fn from(b: IngestRequestBody) -> Self {
        Self {
            canonical_id: b.canonical_id,
            source_type: b.source_type,
            source_uri: b.source_uri,
            title: b.title,
            authors: b.authors,
            published_at: b.published_at,
            language: b.language,
            summary: b.summary,
            raw_text: b.raw_text,
            storage_uri: b.storage_uri,
            metadata: b.metadata,
        }
    }
}

/// `POST /api/ingestion/documents`
///
/// Builds a per-request [`Pipeline`] off the request's JWT-authenticated
/// `AuthedDb`. SurrealDB PERMISSIONS clauses enforce tenant scoping on
/// every write. The handler then publishes a `FeedItemEvent` to the
/// process-global broadcast channel for SSE consumers.
pub async fn ingest_documents(
    State(state): State<AppState>,
    Extension(db): Extension<Arc<AuthedDb>>,
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

    let storage = db.as_storage();
    let pipeline: Arc<dyn IngestSink> = Arc::new(Pipeline::new(storage.clone()));
    let sink = NotifyingSink::new(pipeline, storage, state.events.clone());

    match sink.ingest(body.into()).await {
        Ok(outcome) => (StatusCode::OK, Json(outcome)).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "ingestion failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "ingestion failed").into_response()
        }
    }
}
