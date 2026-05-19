use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};

use async_trait::async_trait;
use bytes::Bytes;

use crate::chunker::ChunkConfig;
use crate::state::AppState;
use crate::storage::AuthedDb;
use crate::text_extractor::{TextExtractor, Word};

use super::{IngestRequest, IngestSink, NotifyingSink, Pipeline, RagSink};

/// Stand-in extractor used when only the document embedder is wired
/// up — the chunking branch needs *some* `TextExtractor`, but we'd
/// rather skip extraction than fail loudly.
struct NoopExtractor;

#[async_trait]
impl TextExtractor for NoopExtractor {
    async fn extract(&self, _bytes: Bytes) -> crate::error::Result<Vec<Word>> {
        Ok(Vec::new())
    }
}

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
    Json(body): Json<IngestRequestBody>,
) -> Response {
    let storage = db.as_storage();
    let pipeline: Arc<dyn IngestSink> = Arc::new(Pipeline::new(storage.clone()));
    // Wrap with the RAG decorator when at least one embedder is wired up.
    // The chunking branch additionally needs a text extractor; if only
    // the document embedder is configured the decorator still runs and
    // populates `paper_embedding` without touching chunks.
    let pipeline: Arc<dyn IngestSink> =
        if state.chunk_embedder.is_some() || state.document_embedder.is_some() {
            // For the chunking path we need an extractor; when missing,
            // pass a no-op that returns empty word streams so the chunk
            // branch is a fast no-op while the doc-embedding branch
            // still runs.
            let extractor: Arc<dyn crate::text_extractor::TextExtractor> = state
                .text_extractor
                .clone()
                .unwrap_or_else(|| Arc::new(NoopExtractor));
            Arc::new(RagSink::new(
                pipeline,
                storage.clone(),
                state.object_store.clone(),
                extractor,
                state.chunk_embedder.clone(),
                state.document_embedder.clone(),
                ChunkConfig::default(),
            ))
        } else {
            pipeline
        };
    let sink = NotifyingSink::new(pipeline, storage, state.events.clone());

    match sink.ingest(body.into()).await {
        Ok(outcome) => (StatusCode::OK, Json(outcome)).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "ingestion failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "ingestion failed").into_response()
        }
    }
}
