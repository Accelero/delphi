//! Per-request authenticated identity.
//!
//! The [`AuthContext`] extractor is mode-agnostic — it just looks at request
//! extensions. Whichever middleware ran (dev injection, OIDC + lazy upsert,
//! …) is responsible for putting the value there. Handlers stay clean.

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use surrealdb::RecordId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthContext {
    pub user_id: RecordId,
    pub tenant_id: RecordId,
    pub email: String,
    pub display_name: Option<String>,
    pub iss: String,
    pub sub: String,
    pub is_dev: bool,
}

impl<S> FromRequestParts<S> for AuthContext
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<AuthContext>()
            .cloned()
            .ok_or((StatusCode::UNAUTHORIZED, "not authenticated"))
    }
}
