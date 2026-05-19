//! Ingestion v2 — the four upload endpoints.
//!
//! ```text
//! POST   /api/ingestion/uploads                 → create_upload
//! POST   /api/ingestion/uploads/:id/sign-part   → sign_upload_part
//! POST   /api/ingestion/uploads/:id/complete    → complete_upload
//! GET    /api/ingestion/uploads/:id             → get_upload_status
//! ```
//!
//! All four require the `ingester` role in the JWT (or `owner`, which is
//! a Keycloak composite role including `ingester` — the backend checks
//! only for the leaf). Engine PERMISSIONS scope `upload_session` to
//! `(tenant_id, user_id)`; the handler additionally double-checks the
//! loaded row's identity against `AuthContext` (belt-and-suspenders).
//!
//! Bytes never traverse the backend: each `/sign-part` returns a
//! presigned URL the client uploads directly to. Validation happens at
//! `/complete` against the committed S3 object.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};

use crate::auth::AuthContext;
use crate::error::Error;
use crate::object_store::storage_uri_for_key;
use crate::state::AppState;
use crate::storage::{AuthedDb, CreateUploadSessionParams, Document, IngestionRejection, Storage};

use super::validation::{
    validate_ingestion_metadata, validate_uploaded_object, CreateUploadRequest, MetadataPolicy,
    MetadataReject, ObjectPolicy, ObjectReject,
};

/// Per-process ingestion-v2 runtime config. Constructed once at boot,
/// shared via `AppState`. Handlers never re-parse env.
#[derive(Debug, Clone)]
pub struct UploadsConfig {
    pub part_size_bytes: u64,
    pub part_url_ttl: Duration,
    pub session_ttl: Duration,
    pub metadata_policy: MetadataPolicy,
    pub object_policy: ObjectPolicy,
    /// `INGEST_S3_BUCKET`. Used to render the canonical `storage_uri`.
    /// Falls back to `"local"` when no S3 bucket is configured (single-
    /// user / dev deployments backed by `LocalFsObjectStore`).
    pub bucket: String,
}

impl UploadsConfig {
    pub fn from_env() -> Self {
        let part_size_bytes = parse_env_u64("INGEST_UPLOAD_PART_SIZE_BYTES", 8 * 1024 * 1024);
        let part_url_ttl =
            Duration::from_secs(parse_env_u64("INGEST_UPLOAD_PART_URL_TTL_SECS", 900));
        let session_ttl =
            Duration::from_secs(parse_env_u64("INGEST_UPLOAD_SESSION_TTL_SECS", 3600));
        let bucket = std::env::var("INGEST_S3_BUCKET").unwrap_or_else(|_| "local".into());

        let mut metadata_policy = MetadataPolicy::default();
        if let Ok(max) = std::env::var("INGEST_UPLOAD_MAX_FILE_SIZE_BYTES") {
            if let Ok(v) = max.parse::<u64>() {
                metadata_policy.max_size_bytes = v;
            }
        }
        if let Ok(types) = std::env::var("INGEST_ALLOWED_CONTENT_TYPES") {
            metadata_policy.allowed_content_types = types
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
        }

        let mut object_policy = ObjectPolicy {
            allowed_content_types: metadata_policy.allowed_content_types.clone(),
            ..ObjectPolicy::default()
        };
        if let Ok(v) = std::env::var("INGEST_VALIDATOR_SNIFF_WINDOW_BYTES") {
            if let Ok(n) = v.parse::<usize>() {
                object_policy.sniff_window_bytes = n;
            }
        }
        if let Ok(v) = std::env::var("INGEST_VALIDATOR_PDF_MAX_INPUT_BYTES") {
            if let Ok(n) = v.parse::<u64>() {
                object_policy.pdf_max_input_bytes = n;
            }
        }
        if let Ok(v) = std::env::var("INGEST_VALIDATOR_PDF_MAX_PAGES") {
            if let Ok(n) = v.parse::<usize>() {
                object_policy.pdf_max_pages = n;
            }
        }
        if let Ok(v) = std::env::var("INGEST_VALIDATOR_PDF_TIMEOUT_SECS") {
            if let Ok(n) = v.parse::<u64>() {
                object_policy.pdf_parse_timeout = Duration::from_secs(n);
            }
        }
        if let Ok(v) = std::env::var("INGEST_VALIDATOR_REJECT_POLYGLOTS") {
            object_policy.reject_polyglots = matches!(v.as_str(), "true" | "1" | "yes");
        }

        Self {
            part_size_bytes,
            part_url_ttl,
            session_ttl,
            metadata_policy,
            object_policy,
            bucket,
        }
    }

    /// In-test default. Identical defaults to `from_env` minus the env
    /// reads, so unit/integration tests can construct an `AppState`
    /// without polluting the process environment.
    pub fn test_default() -> Self {
        Self {
            part_size_bytes: 8 * 1024 * 1024,
            part_url_ttl: Duration::from_secs(900),
            session_ttl: Duration::from_secs(3600),
            metadata_policy: MetadataPolicy::default(),
            object_policy: ObjectPolicy::default(),
            bucket: "test".into(),
        }
    }
}

fn parse_env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default)
}

// ============================================================================
// Wire shapes
// ============================================================================

#[derive(Debug, Serialize)]
pub struct CreateUploadResponse {
    pub doc_id: String,
    pub key: String,
    pub upload_id: String,
    pub part_size_bytes: u64,
    pub part_url_ttl_secs: u64,
}

#[derive(Debug, Deserialize)]
pub struct SignPartRequest {
    pub part_number: u16,
}

#[derive(Debug, Serialize)]
pub struct SignPartResponse {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct CompleteRequest {
    pub parts: Vec<crate::object_store::PartRef>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum CompleteResponse {
    Ready {
        doc_id: String,
    },
    Conflict {
        state: String,
        existing_doc_id: Option<String>,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum StatusResponse {
    Uploading,
    Validating,
    /// Reserved for the document-by-doc_id lookup once that wiring
    /// lands — today the SPA learns the document id from the
    /// `/complete` response directly.
    #[allow(dead_code)]
    Ready {
        doc_id: String,
    },
    Rejected {
        reason: String,
    },
}

// ============================================================================
// Helpers
// ============================================================================

const INGESTER_ROLE: &str = "ingester";

fn require_ingester(auth: &AuthContext) -> Option<Response> {
    if auth.roles.iter().any(|r| r == INGESTER_ROLE) {
        None
    } else {
        Some((StatusCode::FORBIDDEN, "ingester role required").into_response())
    }
}

fn metadata_reject_status(r: &MetadataReject) -> StatusCode {
    match r {
        MetadataReject::DisallowedContentType
        | MetadataReject::SizeExceedsLimit
        | MetadataReject::TitleTooLong
        | MetadataReject::MetadataTooDeep
        | MetadataReject::MetadataTooLarge
        | MetadataReject::InvalidCanonicalId
        | MetadataReject::InvalidSourceUri
        | MetadataReject::MalformedRequest(_) => StatusCode::BAD_REQUEST,
    }
}

fn key_for(tenant_slug: &str, doc_id: &str) -> String {
    format!("tenants/{tenant_slug}/{doc_id}")
}

// ============================================================================
// 1. POST /api/ingestion/uploads
// ============================================================================

pub async fn create_upload(
    State(state): State<AppState>,
    Extension(db): Extension<Arc<AuthedDb>>,
    auth: AuthContext,
    Json(req): Json<CreateUploadRequest>,
) -> Response {
    if let Some(resp) = require_ingester(&auth) {
        return resp;
    }
    if let Err(rej) = validate_ingestion_metadata(&req, &state.uploads_config.metadata_policy) {
        let code = metadata_reject_status(&rej);
        let body = serde_json::json!({ "error": format!("{rej:?}") });
        return (code, Json(body)).into_response();
    }

    let tenant_slug = match db.resolve_tenant_slug().await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "resolve_tenant_slug failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "tenant resolve failed").into_response();
        }
    };

    let doc_id = ulid::Ulid::new().to_string().to_lowercase();
    let key = key_for(&tenant_slug, &doc_id);

    let upload_id = match state
        .object_store
        .create_multipart_upload(&key, &req.content_type)
        .await
    {
        Ok(id) => id,
        Err(e) => {
            tracing::error!(error = %e, key, "create_multipart_upload failed");
            return (
                StatusCode::BAD_GATEWAY,
                "object store create_multipart_upload failed",
            )
                .into_response();
        }
    };

    let params = CreateUploadSessionParams {
        doc_id: doc_id.clone(),
        s3_key: key.clone(),
        s3_upload_id: upload_id.clone(),
        canonical_id: req.canonical_id.clone(),
        source_type: req.source_type.clone(),
        source_uri: req.source_uri.clone(),
        title: req.title.clone(),
        declared_size: req.size,
        declared_content_type: req.content_type.clone(),
        declared_metadata: req.metadata.clone(),
    };
    match db.create_upload_session(&params).await {
        Ok(_session) => (
            StatusCode::OK,
            Json(CreateUploadResponse {
                doc_id,
                key,
                upload_id,
                part_size_bytes: state.uploads_config.part_size_bytes,
                part_url_ttl_secs: state.uploads_config.part_url_ttl.as_secs(),
            }),
        )
            .into_response(),
        Err(e) => {
            // Best-effort: clean up the freshly-opened multipart, since the
            // session row is what tracks it. Failure here only leaks an
            // orphan; the cleaner reaps.
            let _ = state
                .object_store
                .abort_multipart_upload(&key, &upload_id)
                .await;
            let msg = e.to_string();
            if msg.contains("already exists") || msg.contains("upload_session_canonical") {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({
                        "error": "canonical_id already in flight"
                    })),
                )
                    .into_response();
            }
            tracing::error!(error = %e, "create_upload_session failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "create session failed").into_response()
        }
    }
}

// ============================================================================
// 2. POST /api/ingestion/uploads/:doc_id/sign-part
// ============================================================================

pub async fn sign_upload_part(
    State(state): State<AppState>,
    Extension(db): Extension<Arc<AuthedDb>>,
    auth: AuthContext,
    Path(doc_id): Path<String>,
    Json(req): Json<SignPartRequest>,
) -> Response {
    if let Some(resp) = require_ingester(&auth) {
        return resp;
    }
    if req.part_number == 0 || req.part_number > 10_000 {
        return (StatusCode::BAD_REQUEST, "part_number out of range").into_response();
    }

    let session = match db.get_upload_session(&doc_id).await {
        Ok(Some(s)) => s,
        Ok(None) => return (StatusCode::NOT_FOUND, "session not found").into_response(),
        Err(e) => {
            tracing::error!(error = %e, doc_id, "get_upload_session failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response();
        }
    };

    // Belt-and-suspenders identity check on top of engine PERMISSIONS.
    if session.tenant_id.as_ref() != Some(&auth.tenant_id)
        || session.user_id.as_ref() != Some(&auth.user_id)
    {
        return (StatusCode::NOT_FOUND, "session not found").into_response();
    }

    if session.state != "uploading" {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "state": session.state })),
        )
            .into_response();
    }

    // Session TTL gate.
    if let Some(started_at) = session.started_at {
        let age = chrono::Utc::now() - started_at;
        if age
            > chrono::Duration::from_std(state.uploads_config.session_ttl)
                .unwrap_or(chrono::Duration::MAX)
        {
            return (StatusCode::GONE, "session expired").into_response();
        }
    }

    let url = match state
        .object_store
        .presign_upload_part(
            &session.s3_key,
            &session.s3_upload_id,
            req.part_number,
            state.uploads_config.part_url_ttl,
        )
        .await
    {
        Ok(u) => u,
        Err(e) => {
            tracing::error!(error = %e, doc_id, "presign_upload_part failed");
            return (StatusCode::BAD_GATEWAY, "presign failed").into_response();
        }
    };
    (
        StatusCode::OK,
        Json(SignPartResponse {
            url: url.into_inner(),
        }),
    )
        .into_response()
}

// ============================================================================
// 3. POST /api/ingestion/uploads/:doc_id/complete
// ============================================================================

pub async fn complete_upload(
    State(state): State<AppState>,
    Extension(db): Extension<Arc<AuthedDb>>,
    auth: AuthContext,
    Path(doc_id): Path<String>,
    Json(req): Json<CompleteRequest>,
) -> Response {
    if let Some(resp) = require_ingester(&auth) {
        return resp;
    }

    // 1. CAS uploading → validating. Single-flight against this session.
    let acquired = match db
        .cas_upload_session_state(&doc_id, "uploading", "validating")
        .await
    {
        Ok(b) => b,
        Err(e) => {
            tracing::error!(error = %e, doc_id, "CAS failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "cas failed").into_response();
        }
    };
    if !acquired {
        // Either session doesn't exist (for this caller) or another caller
        // owns the validating step. Look up to disambiguate.
        let snap = db.get_upload_session(&doc_id).await.unwrap_or(None);
        return match snap {
            Some(s) => {
                let body = Json(CompleteResponse::Conflict {
                    state: s.state,
                    existing_doc_id: None,
                });
                (StatusCode::CONFLICT, body).into_response()
            }
            None => (StatusCode::NOT_FOUND, "session not found").into_response(),
        };
    }

    // Load the session (post-CAS). PERMISSIONS scope to caller.
    let session = match db.get_upload_session(&doc_id).await {
        Ok(Some(s)) => s,
        _ => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "session disappeared mid-flight",
            )
                .into_response();
        }
    };
    if session.tenant_id.as_ref() != Some(&auth.tenant_id)
        || session.user_id.as_ref() != Some(&auth.user_id)
    {
        return (StatusCode::NOT_FOUND, "session not found").into_response();
    }

    // 2. complete_multipart_upload at S3. On error roll the CAS back so
    //    the SPA can retry.
    if let Err(e) = state
        .object_store
        .complete_multipart_upload(&session.s3_key, &session.s3_upload_id, &req.parts)
        .await
    {
        let msg = e.to_string();
        // Idempotent: if S3 reports the upload already completed, fall
        // through to validation.
        let already_complete = msg.contains("already") || msg.contains("NoSuchUpload");
        if !already_complete {
            let _ = db
                .cas_upload_session_state(&doc_id, "validating", "uploading")
                .await;
            tracing::error!(error = %e, doc_id, "complete_multipart_upload failed");
            return (StatusCode::BAD_GATEWAY, "complete failed").into_response();
        }
    }

    // 3. Object validator.
    let validated = match validate_uploaded_object(
        &session.s3_key,
        session.declared_size as u64,
        &session.declared_content_type,
        &*state.object_store,
        &state.uploads_config.object_policy,
    )
    .await
    {
        Ok(v) => v,
        Err(rej) => {
            return handle_object_reject(state.clone(), &auth, &session, rej).await;
        }
    };

    // 4. Commit transaction.
    let storage_uri = if state.uploads_config.bucket == "local" {
        // Local-FS backend: storage_uri is the file:// URL the
        // LocalFsObjectStore produced. Look up via head to get the
        // canonical form back; for simplicity we re-derive from the key.
        format!("file:///{}", session.s3_key)
    } else {
        storage_uri_for_key(&state.uploads_config.bucket, &session.s3_key)
    };
    let doc = Document {
        id: None,
        tenant_id: None,
        canonical_id: session.canonical_id.clone(),
        source_type: session.source_type.clone(),
        source_uri: session.source_uri.clone(),
        storage_uri: Some(storage_uri),
        title: session.title.clone(),
        authors: Vec::new(),
        published_at: None,
        ingested_at: None,
        language: None,
        summary: None,
        paper_embedding: None,
        paper_embedding_model: None,
        content_hash: validated.etag.trim_matches('"').to_string(),
        version: 1,
        metadata: session.declared_metadata.clone(),
    };
    match db.commit_upload(&doc_id, &doc).await {
        Ok(doc_record) => (
            StatusCode::OK,
            Json(CompleteResponse::Ready {
                doc_id: doc_record.to_string(),
            }),
        )
            .into_response(),
        Err(Error::CanonicalIdConflict { existing_doc_id }) => {
            // Clean up: delete the S3 object + session, record rejection.
            let _ = state.object_store.delete(&session.s3_key).await;
            let _ = db.delete_upload_session(&doc_id).await;
            let rej = IngestionRejection {
                id: None,
                tenant_id: Some(auth.tenant_id.clone()),
                user_id: Some(auth.user_id.clone()),
                doc_id: doc_id.clone(),
                reason: "canonical_id_conflict".into(),
                sniffed_type: None,
                rejected_at: None,
            };
            let _ = state
                .system_db
                .storage_for(auth.tenant_id.clone())
                .record_ingestion_rejection(&rej)
                .await;
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(CompleteResponse::Conflict {
                    state: "rejected".into(),
                    existing_doc_id: Some(existing_doc_id),
                }),
            )
                .into_response()
        }
        Err(e) => {
            // Leave session in `validating` (cleaner reaps); the S3
            // object becomes an orphan and is reaped too.
            tracing::error!(error = %e, doc_id, "commit_upload failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "commit failed").into_response()
        }
    }
}

async fn handle_object_reject(
    state: AppState,
    auth: &AuthContext,
    session: &crate::storage::UploadSession,
    rej: ObjectReject,
) -> Response {
    let reason = rej.reason_code().to_string();
    let _ = state.object_store.delete(&session.s3_key).await;
    let _ = state
        .object_store
        .abort_multipart_upload(&session.s3_key, &session.s3_upload_id)
        .await;

    // Acquire a fresh authed handle? No — we already hold one. But the
    // delete + rejection writes use SystemDb for the rejection (PERMISSIONS
    // deny user writes), and AuthedDb-via-extension for the session delete.
    // We don't have an Extension<AuthedDb> here, so go via SystemDb for
    // both.
    let sys = state.system_db.storage_for(auth.tenant_id.clone());
    let _ = sys.delete_upload_session(&session.doc_id).await;

    let sniffed = match &rej {
        ObjectReject::ContentTypeMismatch { sniffed, .. } => Some(sniffed.clone()),
        _ => None,
    };
    let rec = IngestionRejection {
        id: None,
        tenant_id: Some(auth.tenant_id.clone()),
        user_id: Some(auth.user_id.clone()),
        doc_id: session.doc_id.clone(),
        reason: reason.clone(),
        sniffed_type: sniffed,
        rejected_at: None,
    };
    if let Err(e) = sys.record_ingestion_rejection(&rec).await {
        tracing::error!(error = %e, doc_id = %session.doc_id, "record_ingestion_rejection failed");
    }
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(CompleteResponse::Rejected { reason }),
    )
        .into_response()
}

// ============================================================================
// 4. GET /api/ingestion/uploads/:doc_id
// ============================================================================

pub async fn get_upload_status(
    Extension(db): Extension<Arc<AuthedDb>>,
    auth: AuthContext,
    Path(doc_id): Path<String>,
) -> Response {
    if let Some(resp) = require_ingester(&auth) {
        return resp;
    }

    // 1. In-flight session?
    if let Ok(Some(s)) = db.get_upload_session(&doc_id).await {
        if s.tenant_id.as_ref() == Some(&auth.tenant_id)
            && s.user_id.as_ref() == Some(&auth.user_id)
        {
            let resp = match s.state.as_str() {
                "uploading" => StatusResponse::Uploading,
                "validating" => StatusResponse::Validating,
                other => {
                    tracing::warn!(other, doc_id, "unknown upload_session state");
                    StatusResponse::Uploading
                }
            };
            return (StatusCode::OK, Json(resp)).into_response();
        }
    }

    // 2. Committed document? (Engine PERMISSIONS restrict by tenant.)
    // We don't have a `get_document_by_doc_id` lookup keyed on the
    // session's `doc_id` string — the session never carried the document
    // record id. The canonical_id from the session is what links the two,
    // but we already deleted the session on commit. The status endpoint
    // therefore relies on the `ingestion_rejection` table for failed
    // uploads and on the SPA polling stop point for successful ones (the
    // SPA knows `doc_id` from the create response).
    //
    // Look up by canonical_id stashed in the session if it's still
    // around; otherwise rely on the rejection / 404 fallback.

    // 3. Rejection?
    if let Ok(Some(rej)) = db.get_ingestion_rejection(&doc_id).await {
        return (
            StatusCode::OK,
            Json(StatusResponse::Rejected { reason: rej.reason }),
        )
            .into_response();
    }

    (StatusCode::NOT_FOUND, "upload not found").into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_format_is_tenants_slash_slug_slash_doc_id() {
        // Format is load-bearing — the cleaner globs by `tenants/`
        // prefix and the canonical storage_uri depends on it.
        assert_eq!(key_for("test", "abc"), "tenants/test/abc");
    }

    #[test]
    fn require_ingester_accepts_role() {
        let mut auth = stub_auth();
        auth.roles = vec!["ingester".into()];
        assert!(require_ingester(&auth).is_none());
    }

    #[test]
    fn require_ingester_rejects_others() {
        let mut auth = stub_auth();
        auth.roles = vec!["viewer".into()];
        assert!(require_ingester(&auth).is_some());
    }

    fn stub_auth() -> AuthContext {
        AuthContext {
            user_id: surrealdb::RecordId::from(("app_user", "u")),
            tenant_id: surrealdb::RecordId::from(("tenant", "t")),
            email: "u@t".into(),
            display_name: None,
            iss: "x".into(),
            sub: "u".into(),
            roles: Vec::new(),
            is_dev: false,
        }
    }
}
