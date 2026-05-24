//! Per-document HTTP endpoints. Today: minting a short-lived,
//! direct-to-storage download URL for in-browser viewers.
//!
//! `GET /api/documents/:key/view-url` — looks up the document by record
//! key, then mints a presigned `GET` against the object store and returns
//! `{ url, expires_at }`. The client (PDF.js) fetches the bytes **directly
//! from the store** (with range requests) — the backend is no longer in
//! the byte path. The handler runs the lookup through the request's
//! `AuthedDb` (JWT-bound RECORD session), so SurrealDB's PERMISSIONS
//! clause refuses the read engine-side if the row isn't in the caller's
//! tenant — the handler then sees `Ok(None)` and returns 404. That tenant
//! check **is** the authz decision; minting only happens after it passes.
//! See `docs/architecture/object-access.md`.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::Serialize;
use surrealdb::types::RecordId;

use crate::object_store::AccessOp;
use crate::state::AppState;
use crate::storage::{AuthedDb, Storage};

#[derive(Debug, Serialize)]
pub struct ViewUrlResponse {
    /// The client fetches the object bytes directly from this URL.
    pub url: String,
    /// RFC3339 instant after which the URL stops working.
    pub expires_at: String,
}

pub async fn view_url(
    State(state): State<AppState>,
    Extension(db): Extension<Arc<AuthedDb>>,
    Path(key): Path<String>,
) -> Response {
    let id = RecordId::new("document", key.as_str());

    // Authz decision: tenant-scoped lookup through the JWT-bound session.
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

    // The minter is keyed on the storage key, not the full URL. Strip the
    // `s3://<bucket>/` prefix to recover it; mem-backed URIs (tests) carry
    // a `<scheme>://` prefix too.
    let storage_key = match storage_key_from_uri(storage_uri) {
        Some(k) => k,
        None => {
            tracing::error!(storage_uri, "unrecognised storage_uri form");
            return (StatusCode::NOT_FOUND, "stored original unavailable").into_response();
        }
    };

    let grant = match state
        .access
        .mint(
            storage_key,
            AccessOp::Download,
            state.uploads_config.download_url_ttl,
        )
        .await
    {
        Ok(g) => g,
        Err(e) => {
            tracing::error!(error = %e, storage_uri, "mint download url failed");
            return (StatusCode::BAD_GATEWAY, "could not mint download url").into_response();
        }
    };

    (
        StatusCode::OK,
        Json(ViewUrlResponse {
            url: grant.url,
            expires_at: grant.expires_at.to_rfc3339(),
        }),
    )
        .into_response()
}

/// Recover the storage key from a canonical storage URI. The minter
/// signs against its own configured bucket, so we only need the key part.
///
/// - `s3://<bucket>/<key>` — strip the scheme + bucket authority.
/// - `mem://<key>` / `mem-multipart://<key>` (tests) — the whole
///   remainder after `://` is the key (keys contain slashes, so there is
///   no authority segment to strip).
fn storage_key_from_uri(uri: &str) -> Option<&str> {
    if let Some(rest) = uri.strip_prefix("s3://") {
        // Drop the bucket authority; everything after the first `/` is key.
        return rest.split_once('/').map(|(_bucket, key)| key);
    }
    if let Some(rest) = uri.strip_prefix("mem-multipart://") {
        return Some(rest);
    }
    if let Some(rest) = uri.strip_prefix("mem://") {
        return Some(rest);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_s3_bucket_authority() {
        assert_eq!(
            storage_key_from_uri("s3://delphi/tenants/test/abc"),
            Some("tenants/test/abc")
        );
    }

    #[test]
    fn handles_mem_uris() {
        assert_eq!(storage_key_from_uri("mem://k/abc"), Some("k/abc"));
        assert_eq!(
            storage_key_from_uri("mem-multipart://tenants/test/abc"),
            Some("tenants/test/abc")
        );
    }

    #[test]
    fn rejects_non_url() {
        assert_eq!(storage_key_from_uri("not-a-url"), None);
    }
}
