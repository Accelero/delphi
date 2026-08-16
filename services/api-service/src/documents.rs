//! HTTP surface for the document lifecycle.
//!
//! This module is the only place that knows about status codes. Everything
//! below it speaks [`DocumentError`], which is what keeps `axum` out of the use
//! cases.

use std::sync::Arc;

use axum::extract::{FromRef, Path, Query, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use delphi_auth::{AuthContext, AuthVerifier};
use delphi_contracts::{ApiError, ApiErrorBody, ApiErrorCode};
use delphi_document_app::{
    CompleteRequest, ConflictPolicy, DocumentCursor, DocumentError, DocumentService,
    Principal, PreflightRequest, PresignedPart, RenewRequest, UploadStatus,
};
use delphi_document_domain::{DocumentState, MetadataPatch};
use serde::{Deserialize, Serialize};

/// Write access to the document path. `owner` is a realm composite that
/// includes this.
const WRITE_ROLE: &str = "ingester";

const DEFAULT_LIST_LIMIT: u32 = 50;

#[derive(Clone)]
pub struct DocumentApiState {
    pub auth: AuthVerifier,
    pub service: Arc<DocumentService>,
}

impl FromRef<DocumentApiState> for AuthVerifier {
    fn from_ref(state: &DocumentApiState) -> Self {
        state.auth.clone()
    }
}

pub fn routes<S>(state: DocumentApiState) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/api/uploads", post(create_upload))
        .route("/api/uploads/{upload_id}", get(get_upload))
        .route("/api/uploads/{upload_id}/parts", get(list_upload_parts))
        .route("/api/uploads/{upload_id}/renew", post(renew_upload))
        .route("/api/uploads/{upload_id}/complete", post(complete_upload))
        .route("/api/documents", get(list_documents))
        .route("/api/documents/{document_id}", get(get_document))
        .with_state(state)
}

// ------------------------------------------------------------------- requests

#[derive(Debug, Deserialize)]
pub struct CreateUploadBody {
    /// Omit to create, supply to replace.
    #[serde(default)]
    pub document_id: Option<String>,
    pub filename: String,
    pub size: u64,
    #[serde(default)]
    pub content_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RenewBody {
    #[serde(default)]
    pub from_part: Option<u16>,
    #[serde(default)]
    pub count: Option<u16>,
}

#[derive(Debug, Deserialize)]
pub struct CompleteBody {
    /// Replace mode only.
    #[serde(default)]
    pub if_match: Option<u64>,
    #[serde(default)]
    pub on_conflict: ConflictPolicy,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    #[serde(default)]
    pub limit: Option<u32>,
    /// Opaque: hand back the previous page's `next` verbatim. It encodes the
    /// full ordering key, not just a timestamp, because `updated_at` is not
    /// unique and paging on it alone silently drops rows.
    #[serde(default)]
    pub cursor: Option<String>,
}

// ------------------------------------------------------------------ responses

#[derive(Debug, Serialize)]
pub struct CreateUploadResponse {
    pub upload_id: String,
    pub document_id: String,
    /// Included because browser uploaders need `{ uploadId, key }` from their
    /// create hook.
    pub key: String,
    pub part_size_bytes: u64,
    pub part_count: u16,
}

/// Geometry is fixed at preflight and not echoed; each part carries its own
/// `expires_at`, which is all a client batching a window needs.
#[derive(Debug, Serialize)]
pub struct RenewResponseBody {
    pub parts: Vec<PresignedPart>,
}

#[derive(Debug, Serialize)]
pub struct UploadedPartsBody {
    /// Echoed so a resuming client can re-derive its slicing without having
    /// kept the preflight response.
    pub part_size_bytes: u64,
    pub part_count: u16,
    pub parts: Vec<UploadedPartBody>,
}

#[derive(Debug, Serialize)]
pub struct UploadedPartBody {
    pub part_number: u16,
    /// Quoted exactly as S3 returned it; it goes back verbatim in `/complete`.
    pub etag: String,
    pub size: u64,
}

/// The upload's status, plus the `document_id` a client needs once it is
/// accepted. `UploadStatus` serialises as the tagged union; the id is flattened
/// alongside it rather than living on every variant.
#[derive(Debug, Serialize)]
pub struct UploadStatusBody {
    #[serde(flatten)]
    pub status: UploadStatus,
    /// Known from preflight, so it is reported at every stage — a client that
    /// lost its preflight response can still find the document it is making.
    pub document_id: String,
}

#[derive(Debug, Serialize)]
pub struct DocumentBody {
    pub document_id: String,
    pub version: u64,
    pub state: &'static str,
    pub index_state: &'static str,
    pub filename: Option<String>,
    pub content_type: Option<String>,
    pub byte_size: Option<u64>,
    pub checksum: Option<String>,
    pub title: Option<String>,
    pub tags: Vec<String>,
    pub description: Option<String>,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct DocumentListBody {
    pub items: Vec<DocumentBody>,
    /// Pass back as `?cursor=`, or `null` at the end of the listing.
    pub next: Option<String>,
}

fn to_body(document: DocumentState) -> DocumentBody {
    DocumentBody {
        document_id: document.document_id,
        version: document.version,
        state: document.state.as_str(),
        index_state: document.index_state.as_str(),
        filename: document.filename,
        content_type: document.content_type,
        byte_size: document.byte_size,
        checksum: document.checksum,
        title: document.title,
        tags: document.tags,
        description: document.description,
        metadata: document.metadata,
        created_at: document.created_at,
        updated_at: document.updated_at,
    }
}

// ------------------------------------------------------------------- handlers

async fn create_upload(
    auth: AuthContext,
    State(state): State<DocumentApiState>,
    Json(body): Json<CreateUploadBody>,
) -> Result<(StatusCode, Json<CreateUploadResponse>), ApiFailure> {
    let principal = authorize(&auth)?;
    let response = state
        .service
        .preflight(
            principal.tenant_id(),
            principal.user_id(),
            PreflightRequest {
                document_id: body.document_id,
                filename: body.filename,
                size: body.size,
                content_type: body.content_type,
            },
        )
        .await?;

    Ok((
        StatusCode::CREATED,
        Json(CreateUploadResponse {
            upload_id: response.upload_id,
            document_id: response.document_id,
            key: response.key,
            part_size_bytes: response.part_size_bytes,
            part_count: response.part_count,
        }),
    ))
}

/// `GET /api/uploads/{upload_id}/parts`.
///
/// A write-path route despite being a GET: it exists to let an interrupted
/// upload continue, and it reveals the ETags needed to finish one.
async fn list_upload_parts(
    auth: AuthContext,
    State(state): State<DocumentApiState>,
    Path(upload_id): Path<String>,
) -> Result<Json<UploadedPartsBody>, ApiFailure> {
    let principal = authorize(&auth)?;
    let response = state
        .service
        .uploaded_parts(principal.tenant_id(), principal.user_id(), &upload_id)
        .await?;
    Ok(Json(UploadedPartsBody {
        part_size_bytes: response.part_size_bytes,
        part_count: response.part_count,
        parts: response
            .parts
            .into_iter()
            .map(|part| UploadedPartBody {
                part_number: part.part_number,
                etag: part.etag,
                size: part.size,
            })
            .collect(),
    }))
}

async fn renew_upload(
    auth: AuthContext,
    State(state): State<DocumentApiState>,
    Path(upload_id): Path<String>,
    body: Option<Json<RenewBody>>,
) -> Result<Json<RenewResponseBody>, ApiFailure> {
    let principal = authorize(&auth)?;
    let Json(body) = body.unwrap_or(Json(RenewBody {
        from_part: None,
        count: None,
    }));
    let response = state
        .service
        .renew(
            principal.tenant_id(),
            principal.user_id(),
            &upload_id,
            RenewRequest {
                from_part: body.from_part,
                count: body.count,
            },
        )
        .await?;
    Ok(Json(RenewResponseBody {
        parts: response.parts,
    }))
}

async fn complete_upload(
    auth: AuthContext,
    State(state): State<DocumentApiState>,
    Path(upload_id): Path<String>,
    Json(body): Json<CompleteBody>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiFailure> {
    let principal = authorize(&auth)?;
    state
        .service
        .complete(
            principal.tenant_id(),
            principal.user_id(),
            &upload_id,
            CompleteRequest {
                if_match: body.if_match,
                on_conflict: body.on_conflict,
                patch: MetadataPatch {
                    title: body.title,
                    tags: body.tags,
                    description: body.description,
                    metadata: body.metadata,
                },
            },
        )
        .await?;

    // 202, not 200: the document does not exist yet, and may never. The client
    // must poll `GET /api/uploads/{id}` for the terminal answer.
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "state": "scanning" })),
    ))
}

async fn get_upload(
    auth: AuthContext,
    State(state): State<DocumentApiState>,
    Path(upload_id): Path<String>,
) -> Result<Json<UploadStatusBody>, ApiFailure> {
    let principal = authorize_read(&auth)?;
    let state = state
        .service
        .upload_state(principal.tenant_id(), principal.user_id(), &upload_id)
        .await?;
    Ok(Json(UploadStatusBody {
        status: state.status,
        document_id: state.document_id,
    }))
}

async fn get_document(
    auth: AuthContext,
    State(state): State<DocumentApiState>,
    Path(document_id): Path<String>,
) -> Result<Response, ApiFailure> {
    let principal = authorize_read(&auth)?;
    let document = state
        .service
        .get_document(principal.tenant_id(), &document_id)
        .await?;

    let version = document.version;
    let mut response = Json(to_body(document)).into_response();
    if let Ok(etag) = HeaderValue::from_str(&format!("\"{version}\"")) {
        response.headers_mut().insert(axum::http::header::ETAG, etag);
    }
    Ok(response)
}

async fn list_documents(
    auth: AuthContext,
    State(state): State<DocumentApiState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<DocumentListBody>, ApiFailure> {
    let principal = authorize_read(&auth)?;
    // A cursor we did not mint is a 400, never "start from the beginning":
    // silently restarting would loop a paging client forever.
    let cursor = query
        .cursor
        .as_deref()
        .map(|value| {
            DocumentCursor::decode(value)
                .ok_or_else(|| DocumentError::Invalid("cursor is not valid".to_owned()))
        })
        .transpose()?;

    // The page and its `next` are decided together, inside the service, next to
    // the clamp on `limit`. Computing `next` out here is what previously let a
    // clamped page claim the listing had ended.
    let page = state
        .service
        .list_documents(
            principal.tenant_id(),
            query.limit.unwrap_or(DEFAULT_LIST_LIMIT),
            cursor.as_ref(),
        )
        .await?;

    Ok(Json(DocumentListBody {
        items: page.items.into_iter().map(to_body).collect(),
        next: page.next.map(|cursor| cursor.encode()),
    }))
}

// -------------------------------------------------------------- authorization

/// Build the principal and check write authority.
///
/// The principal's constructor is where `tenant_id` and `user_id` are validated
/// against `[A-Za-z0-9_-]`. Both become NATS key segments and `tenant_id`
/// becomes a subject token, where `.`, `*`, or `>` would corrupt the space.
/// Rejecting here means no handler can be reached with an unsafe principal.
fn authorize(auth: &AuthContext) -> Result<Principal, ApiFailure> {
    let principal = authorize_read(auth)?;
    if !auth.has_role(WRITE_ROLE) {
        return Err(ApiFailure(DocumentError::Forbidden));
    }
    Ok(principal)
}

fn authorize_read(auth: &AuthContext) -> Result<Principal, ApiFailure> {
    Principal::new(&auth.tenant_id, &auth.user_id).map_err(|error| {
        tracing::warn!(%error, "rejecting a principal whose identifiers are unsafe as NATS keys");
        ApiFailure(DocumentError::Forbidden)
    })
}

// --------------------------------------------------------------------- errors

pub struct ApiFailure(DocumentError);

impl From<DocumentError> for ApiFailure {
    fn from(error: DocumentError) -> Self {
        Self(error)
    }
}

impl IntoResponse for ApiFailure {
    fn into_response(self) -> Response {
        let (status, code, message) = match self.0 {
            DocumentError::NotFound => (
                StatusCode::NOT_FOUND,
                ApiErrorCode::NotFound,
                "not found".to_owned(),
            ),
            DocumentError::Forbidden => (
                StatusCode::FORBIDDEN,
                ApiErrorCode::Forbidden,
                "forbidden".to_owned(),
            ),
            DocumentError::Gone => (
                StatusCode::GONE,
                ApiErrorCode::Gone,
                "this upload has expired; start a new one".to_owned(),
            ),
            DocumentError::Invalid(message) => {
                (StatusCode::BAD_REQUEST, ApiErrorCode::InvalidRequest, message)
            }
            DocumentError::TooLarge(message) => {
                (StatusCode::PAYLOAD_TOO_LARGE, ApiErrorCode::TooLarge, message)
            }
            DocumentError::Conflict { current_version } => (
                StatusCode::CONFLICT,
                ApiErrorCode::Conflict,
                format!("document has moved on to version {current_version}"),
            ),
            DocumentError::Deleted => (
                StatusCode::CONFLICT,
                ApiErrorCode::Conflict,
                "document is deleted".to_owned(),
            ),
            // Internal detail is logged, never returned.
            DocumentError::Internal(detail) => {
                tracing::error!(%detail, "document request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ApiErrorCode::Internal,
                    "internal error".to_owned(),
                )
            }
        };

        (
            status,
            Json(ApiError {
                error: ApiErrorBody { code, message },
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auth(tenant: &str, user: &str, roles: &[&str]) -> AuthContext {
        AuthContext {
            user_id: user.to_owned(),
            tenant_id: tenant.to_owned(),
            email: None,
            roles: roles.iter().map(|role| (*role).to_owned()).collect(),
            bearer_subject: "Bearer test".to_owned(),
        }
    }

    #[test]
    fn writes_require_the_ingester_role_but_reads_do_not() {
        let reader = auth("acme", "user-1", &[]);
        assert!(authorize_read(&reader).is_ok());
        assert!(authorize(&reader).is_err());

        let writer = auth("acme", "user-1", &["ingester"]);
        assert!(authorize(&writer).is_ok());
    }

    #[test]
    fn a_principal_with_subject_metacharacters_never_reaches_a_handler() {
        // A tenant claim containing a `.` would split the event subject.
        let hostile = auth("acme.evil", "user-1", &["ingester"]);
        assert!(authorize_read(&hostile).is_err());
        assert!(authorize(&hostile).is_err());

        // A user id containing a `/` would escape the KV key namespace.
        let hostile = auth("acme", "../other-user", &["ingester"]);
        assert!(authorize_read(&hostile).is_err());
    }

    #[test]
    fn errors_map_to_the_status_codes_clients_branch_on() {
        let cases = [
            (DocumentError::NotFound, StatusCode::NOT_FOUND),
            (DocumentError::Forbidden, StatusCode::FORBIDDEN),
            (DocumentError::Gone, StatusCode::GONE),
            (
                DocumentError::Invalid("bad".to_owned()),
                StatusCode::BAD_REQUEST,
            ),
            (
                DocumentError::TooLarge("big".to_owned()),
                StatusCode::PAYLOAD_TOO_LARGE,
            ),
            (
                DocumentError::Conflict { current_version: 4 },
                StatusCode::CONFLICT,
            ),
            (DocumentError::Deleted, StatusCode::CONFLICT),
            (
                DocumentError::Internal("secret".to_owned()),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ];
        for (error, expected) in cases {
            let response = ApiFailure(error).into_response();
            assert_eq!(response.status(), expected);
        }
    }

    #[tokio::test]
    async fn an_internal_error_never_leaks_its_detail() {
        let response = ApiFailure(DocumentError::Internal(
            "postgres://user:password@host".to_owned(),
        ))
        .into_response();
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body");
        let text = String::from_utf8(body.to_vec()).expect("utf8");
        assert!(!text.contains("password"), "leaked internal detail: {text}");
        assert!(text.contains("internal error"));
    }

    #[test]
    fn the_upload_status_body_carries_the_terminal_outcome() {
        // The wire shape did not change when upload state moved to KV: the
        // client still branches on `state` and reads the same fields.
        let json = serde_json::to_value(UploadStatusBody {
            status: UploadStatus::Accepted {
                version: 7,
                superseded: true,
            },
            document_id: "d1".to_owned(),
        })
        .expect("serialize");
        assert_eq!(json["state"], "accepted");
        assert_eq!(json["version"], 7);
        assert_eq!(json["superseded"], true);
        assert_eq!(json["document_id"], "d1");
    }
}
