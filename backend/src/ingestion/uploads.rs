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
use crate::state::AppState;
use crate::storage::{AuthedDb, CreateUploadSessionParams, IngestionRejection, Storage};

use super::autofill::DocumentPrefill;
use super::completion::{run_completion, CompletionCtx, CompletionError};
use super::validation::{
    validate_ingestion_metadata, CreateUploadRequest, MetadataField, MetadataPolicy,
    MetadataReject, ObjectPolicy, ObjectReject,
};

/// Per-process ingestion-v2 runtime config. Constructed once at boot,
/// shared via `AppState`. Handlers never re-parse env.
#[derive(Debug, Clone)]
pub struct UploadsConfig {
    pub part_size_bytes: u64,
    pub part_url_ttl: Duration,
    /// TTL on minted download URLs. Download is the confidentiality-
    /// sensitive direction, so this is short (default 120s). From
    /// `INGEST_DOWNLOAD_URL_TTL_SECS`.
    pub download_url_ttl: Duration,
    pub session_ttl: Duration,
    pub metadata_policy: MetadataPolicy,
    pub object_policy: ObjectPolicy,
    /// `DELPHI_INGEST_S3_BUCKET`. Used to render the canonical `storage_uri`.
    pub bucket: String,
}

impl UploadsConfig {
    pub fn from_env() -> Self {
        let part_size_bytes = parse_env_u64("INGEST_UPLOAD_PART_SIZE_BYTES", 8 * 1024 * 1024);
        let part_url_ttl =
            Duration::from_secs(parse_env_u64("INGEST_UPLOAD_PART_URL_TTL_SECS", 900));
        let download_url_ttl =
            Duration::from_secs(parse_env_u64("INGEST_DOWNLOAD_URL_TTL_SECS", 120));
        let session_ttl =
            Duration::from_secs(parse_env_u64("INGEST_UPLOAD_SESSION_TTL_SECS", 3600));
        let bucket = std::env::var("DELPHI_INGEST_S3_BUCKET").unwrap_or_else(|_| "delphi".into());

        let mut metadata_policy = MetadataPolicy::default();
        if let Ok(max) = std::env::var("DELPHI_INGEST_UPLOAD_MAX_FILE_SIZE_BYTES") {
            if let Ok(v) = max.parse::<u64>() {
                metadata_policy.max_size_bytes = v;
            }
        }
        if let Ok(types) = std::env::var("DELPHI_INGEST_ALLOWED_CONTENT_TYPES") {
            metadata_policy.allowed_content_types = types
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
        }
        // App-required descriptive fields after merge. Defaults empty
        // (autofill is a noop today). Comma-separated: title,authors,
        // summary,language.
        if let Ok(fields) = std::env::var("DELPHI_INGEST_REQUIRED_METADATA_FIELDS") {
            metadata_policy.required_fields = fields
                .split(',')
                .map(str::trim)
                .filter_map(|s| match s.to_ascii_lowercase().as_str() {
                    "title" => Some(MetadataField::Title),
                    "authors" => Some(MetadataField::Authors),
                    "summary" => Some(MetadataField::Summary),
                    "language" => Some(MetadataField::Language),
                    _ => None,
                })
                .collect();
        }

        let mut object_policy = ObjectPolicy {
            allowed_content_types: metadata_policy.allowed_content_types.clone(),
            ..ObjectPolicy::default()
        };
        if let Ok(v) = std::env::var("DELPHI_INGEST_VALIDATOR_SNIFF_WINDOW_BYTES") {
            if let Ok(n) = v.parse::<usize>() {
                object_policy.sniff_window_bytes = n;
            }
        }
        if let Ok(v) = std::env::var("DELPHI_INGEST_VALIDATOR_PDF_MAX_INPUT_BYTES") {
            if let Ok(n) = v.parse::<u64>() {
                object_policy.pdf_max_input_bytes = n;
            }
        }
        if let Ok(v) = std::env::var("DELPHI_INGEST_VALIDATOR_PDF_MAX_PAGES") {
            if let Ok(n) = v.parse::<usize>() {
                object_policy.pdf_max_pages = n;
            }
        }
        if let Ok(v) = std::env::var("DELPHI_INGEST_VALIDATOR_PDF_TIMEOUT_SECS") {
            if let Ok(n) = v.parse::<u64>() {
                object_policy.pdf_parse_timeout = Duration::from_secs(n);
            }
        }
        if let Ok(v) = std::env::var("DELPHI_INGEST_VALIDATOR_REJECT_POLYGLOTS") {
            object_policy.reject_polyglots = matches!(v.as_str(), "true" | "1" | "yes");
        }
        if let Ok(v) = std::env::var("DELPHI_INGEST_VALIDATOR_REJECT_PDF_ACTIVE_CONTENT") {
            object_policy.reject_pdf_active_content = matches!(v.as_str(), "true" | "1" | "yes");
        }

        Self {
            part_size_bytes,
            part_url_ttl,
            download_url_ttl,
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
            download_url_ttl: Duration::from_secs(120),
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
    /// Returned by the record-id lookup once the document is committed
    /// (`document:<doc_id>`). The SPA's recovery poll uses this to learn
    /// a dropped `/complete` actually succeeded.
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

fn key_for(tenant_slug: &str, doc_id: &str) -> String {
    format!("tenants/{tenant_slug}/{doc_id}")
}

// ============================================================================
// 1. POST /api/ingestion/uploads
// ============================================================================

/// Map a metadata rejection to an HTTP status: resource/size limits → 413,
/// everything else (shape, forbidden fields) → 400.
fn metadata_reject_status(rej: &MetadataReject) -> StatusCode {
    match rej {
        MetadataReject::SizeExceedsLimit
        | MetadataReject::MetadataTooLarge
        | MetadataReject::MetadataTooDeep => StatusCode::PAYLOAD_TOO_LARGE,
        _ => StatusCode::BAD_REQUEST,
    }
}

pub async fn create_upload(
    State(state): State<AppState>,
    Extension(db): Extension<Arc<AuthedDb>>,
    auth: AuthContext,
    Json(req): Json<CreateUploadRequest>,
) -> Response {
    if let Some(resp) = require_ingester(&auth) {
        return resp;
    }
    // Layer-1 request gate: forbidden server-derived fields, file size,
    // canonical_id / source_uri shape, and the M8 metadata depth/size caps.
    // Pure + property-tested (validation::metadata). Descriptive text
    // (title/authors/summary) is sanitized in place later at /complete, not
    // rejected here.
    if let Err(rej) = validate_ingestion_metadata(&req, &state.uploads_config.metadata_policy) {
        let code = metadata_reject_status(&rej);
        return (code, Json(serde_json::json!({ "error": format!("{rej:?}") }))).into_response();
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

    // The client no longer declares a MIME type. Stamp a neutral
    // content-type on the S3 object; the real type is resolved from the
    // bytes by the object validator at `/complete`.
    let content_type = "application/octet-stream".to_string();

    let upload_id = match state
        .object_store
        .create_multipart_upload(&key, &content_type)
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
        // Manual uploads send no canonical_id (None → stored NONE);
        // natural-source writers still set it. source_type defaults to
        // "manual"; source_uri defaults to a placeholder URN when absent
        // (the schema column is TYPE string, non-option).
        canonical_id: req.canonical_id.clone(),
        // Per-tenant dedup index value; None for manual uploads. Must
        // match what `commit_upload` computes so the session and the
        // committed document share the same key namespace.
        dedup_key: crate::storage::dedup_key(&auth.tenant_id, req.canonical_id.as_deref()),
        source_type: req.resolved_source_type(),
        source_uri: req
            .source_uri
            .clone()
            .unwrap_or_else(|| format!("urn:delphi:manual:{doc_id}")),
        title: req.title.clone(),
        filename: req.filename.clone(),
        declared_size: req.size,
        declared_content_type: content_type.clone(),
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

    // Route through the access-minting seam instead of calling the
    // object store's presign directly. Returned URL shape is identical
    // today (presigned PUT against the public endpoint); the indirection
    // is the swap point for future CDN/STS minters.
    let grant = match state
        .access
        .mint(
            &session.s3_key,
            crate::object_store::AccessOp::UploadPart {
                upload_id: session.s3_upload_id.clone(),
                part_number: req.part_number,
            },
            state.uploads_config.part_url_ttl,
        )
        .await
    {
        Ok(g) => g,
        Err(e) => {
            tracing::error!(error = %e, doc_id, "mint upload-part url failed");
            return (StatusCode::BAD_GATEWAY, "presign failed").into_response();
        }
    };
    (StatusCode::OK, Json(SignPartResponse { url: grant.url })).into_response()
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

    // 3. Run the ordered completion pipeline (validate object → extract
    //    text → autofill → validate autofill → merge → validate merged →
    //    commit). The handler stays thin: it builds the ctx, runs the
    //    workflow, and maps the terminal result to HTTP.
    let prefill = DocumentPrefill {
        title: session.title.clone(),
        // Single-file prefill only carries title today on the session;
        // authors/summary/language are threaded once the SPA sends them
        // (it does — they live in declared_metadata). Pull what the SPA
        // stashed in declared_metadata.
        authors: prefill_authors(&session.declared_metadata),
        summary: prefill_str(&session.declared_metadata, "summary"),
        language: prefill_str(&session.declared_metadata, "language"),
    };
    let ctx = CompletionCtx {
        object_store: &*state.object_store,
        authed_db: &db,
        system_db: &state.system_db,
        auth: &auth,
        session: &session,
        extractor: &*state.metadata_extractor,
        policy: &state.uploads_config.metadata_policy,
        object_policy: &state.uploads_config.object_policy,
        prefill: &prefill,
        bucket: &state.uploads_config.bucket,
    };

    match run_completion(&ctx).await {
        Ok(doc_record) => (
            StatusCode::OK,
            Json(CompleteResponse::Ready {
                doc_id: doc_record.to_string(),
            }),
        )
            .into_response(),
        Err(CompletionError::ObjectRejected(rej)) => {
            let sniffed = match &rej {
                ObjectReject::ContentTypeMismatch { sniffed, .. } => Some(sniffed.clone()),
                _ => None,
            };
            handle_reject(&state, &auth, &session, rej.reason_code(), sniffed).await
        }
        Err(CompletionError::MetadataRejected(reason)) => {
            handle_reject(&state, &auth, &session, "metadata_rejected", Some(reason)).await
        }
        Err(CompletionError::CanonicalIdConflict { existing_doc_id }) => {
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
        Err(CompletionError::CommitFailed(e)) => {
            // Leave session in `validating` (cleaner reaps); the S3
            // object becomes an orphan and is reaped too.
            tracing::error!(error = %e, doc_id, "commit_upload failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "commit failed").into_response()
        }
    }
}

/// Pull prefill `authors` from the SPA-supplied `declared_metadata`
/// (a JSON array of strings under the `authors` key).
fn prefill_authors(meta: &serde_json::Value) -> Vec<String> {
    meta.get("authors")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

fn prefill_str(meta: &serde_json::Value, key: &str) -> Option<String> {
    meta.get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// Shared reject flow for both stage-4 (object) and stage-9 (merged
/// metadata) rejections: wipe the S3 object + abort the multipart, delete
/// the session, and log the rejection through SystemDb (the
/// `ingestion_rejection` table denies user-session writes). Returns the
/// 422 response.
async fn handle_reject(
    state: &AppState,
    auth: &AuthContext,
    session: &crate::storage::UploadSession,
    reason_code: &str,
    sniffed: Option<String>,
) -> Response {
    let reason = reason_code.to_string();
    let _ = state.object_store.delete(&session.s3_key).await;
    let _ = state
        .object_store
        .abort_multipart_upload(&session.s3_key, &session.s3_upload_id)
        .await;

    // Rejection write must go through SystemDb (PERMISSIONS deny
    // user-session writes); the session delete also routes through it
    // since this helper has no Extension<AuthedDb> handle.
    let sys = state.system_db.storage_for(auth.tenant_id.clone());
    let _ = sys.delete_upload_session(&session.doc_id).await;

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

    // 2. Committed document? Now that `/complete` does
    //    `CREATE document:<doc_id>` (deterministic record id, §1.4), the
    //    document record id is `document:<doc_id>` — so after the session
    //    row is gone on commit we can resolve `ready` by a direct
    //    record-id lookup. Engine PERMISSIONS scope by tenant; this is
    //    what the SPA's recovery poll relies on (§2.3 / B5).
    let rid = surrealdb::RecordId::from(("document", doc_id.as_str()));
    if let Ok(Some(_doc)) = db.get_document(&rid).await {
        return (
            StatusCode::OK,
            Json(StatusResponse::Ready {
                doc_id: rid.to_string(),
            }),
        )
            .into_response();
    }

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
