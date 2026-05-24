//! Per-request authenticated identity.
//!
//! [`AuthContext`] is what handlers consume. It is built once per request by
//! [`super::identity_middleware`] from a [`super::Claims`], and stashed in
//! request extensions. Whichever [`super::ClaimsExtractor`] produced the
//! claims is invisible to downstream code — that is the point of the
//! abstraction.

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use surrealdb::types::RecordId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthContext {
    pub user_id: RecordId,
    pub tenant_id: RecordId,
    pub email: String,
    pub display_name: Option<String>,
    pub iss: String,
    pub sub: String,
    pub roles: Vec<String>,
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

// Allow `Option<AuthContext>` as a handler argument: handlers like `me` accept
// the absence case (returning 401) instead of failing with the extractor's
// default rejection.
impl<S> axum::extract::OptionalFromRequestParts<S> for AuthContext
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> Result<Option<Self>, Self::Rejection> {
        Ok(parts.extensions.get::<AuthContext>().cloned())
    }
}
