//! Per-document HTTP endpoints. Today: serving the stored original
//! artefact (PDF, …) byte-for-byte for in-browser viewers.
//!
//! `GET /api/documents/:key/file` — looks up the document by record key,
//! dereferences `Document.storage_uri` via the shared `ObjectStore`, and
//! returns the bytes inline. The handler runs queries through the
//! request's `AuthedDb` (JWT-bound RECORD session), so SurrealDB's
//! PERMISSIONS clause refuses the read engine-side if the row isn't in
//! the caller's tenant — the handler then sees `Ok(None)` and returns
//! 404. No application-side tenant check needed.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use surrealdb::RecordId;

use crate::state::AppState;
use crate::storage::{AuthedDb, Storage};

pub async fn file(
    State(state): State<AppState>,
    Extension(db): Extension<Arc<AuthedDb>>,
    Path(key): Path<String>,
) -> Response {
    let id = RecordId::from(("document", key.as_str()));

    let doc = match db.get_document(&id).await {
        Ok(Some(d)) => d,
        Ok(None) => return (StatusCode::NOT_FOUND, "document not found").into_response(),
        Err(e) => {
            tracing::error!(error = %e, key, "get_document failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response();
        }
    };

    let storage_uri = match doc.storage_uri.as_deref() {
        Some(s) => s,
        None => return (StatusCode::NOT_FOUND, "no stored original").into_response(),
    };

    let bytes = match state.object_store.get_by_url(storage_uri).await {
        Ok(b) => b,
        Err(e) => {
            tracing::error!(error = %e, storage_uri, "object_store.get_by_url failed");
            return (StatusCode::NOT_FOUND, "stored original unavailable").into_response();
        }
    };

    // Only PDFs ship through this path today; the value matches what
    // adapters store. When other formats land, derive from a content-
    // type column on the Document row.
    let filename = format!("{}.pdf", doc.canonical_id);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/pdf")
        .header(
            header::CONTENT_DISPOSITION,
            format!("inline; filename=\"{filename}\""),
        )
        .header(header::CACHE_CONTROL, "private, max-age=300")
        .body(Body::from(bytes))
        .expect("static headers")
}
