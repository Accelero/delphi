//! `GET /api/chunks/:id` — tenant-scoped chunk lookup for the PDF
//! viewer's highlight-overlay path.
//!
//! Returns the chunk's text + line-rectangle list (`bboxes`) + linked
//! `doc_id`. The frontend uses `bboxes` to draw CSS overlays on the
//! right page(s) and `doc_id` to load the PDF bytes.
//!
//! Auth: any authenticated user; SurrealDB's PERMISSIONS clauses refuse
//! the SELECT engine-side if the chunk isn't in the caller's tenant
//! (the handler then sees `Ok(None)` and returns 404).

use std::sync::Arc;

use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::Serialize;
use surrealdb::types::{RecordId, ToSql};

use crate::storage::{AuthedDb, Bbox, Storage};

#[derive(Debug, Serialize)]
pub struct ChunkResponse {
    /// `chunk:<key>` wire form, matching the rest of the API.
    pub id: String,
    /// `document:<key>`.
    pub doc_id: String,
    pub ordinal: i64,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bboxes: Option<Vec<Bbox>>,
}

pub async fn get_chunk(
    Extension(db): Extension<Arc<AuthedDb>>,
    Path(key): Path<String>,
) -> Response {
    let id = RecordId::new("chunk", key.as_str());
    let chunk = match db.get_chunk(&id).await {
        Ok(Some(c)) => c,
        Ok(None) => return (StatusCode::NOT_FOUND, "chunk not found").into_response(),
        Err(e) => {
            tracing::error!(error = %e, key, "get_chunk failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response();
        }
    };
    let Some(doc) = chunk.doc.clone() else {
        tracing::warn!(?id, "chunk row missing `doc` link");
        return (StatusCode::INTERNAL_SERVER_ERROR, "malformed chunk").into_response();
    };
    let body = ChunkResponse {
        id: chunk
            .id
            .map(|r| r.to_sql())
            .unwrap_or_else(|| format!("chunk:{key}")),
        doc_id: doc.to_sql(),
        ordinal: chunk.ordinal,
        text: chunk.text,
        bboxes: chunk.bboxes,
    };
    (StatusCode::OK, Json(body)).into_response()
}
