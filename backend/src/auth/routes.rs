//! Auth routes:
//!
//! - `GET  /api/auth/me`     → 200 with `{user, tenant, dev}` or 401.
//! - `GET  /api/auth/login`  → triggers OIDC redirect (in OIDC mode), or
//!                             plain 302 to `/` (in dev mode).
//! - `POST /api/auth/logout` → flush session, 204. Frontend then re-fetches
//!                             `/api/auth/me`, gets 401, and redirects to login.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use axum::Json;
use serde_json::json;
use tower_sessions::Session;

use crate::auth::context::AuthContext;
use crate::state::AppState;

pub async fn me(auth: Option<AuthContext>) -> Response {
    let Some(auth) = auth else {
        return (StatusCode::UNAUTHORIZED, "not authenticated").into_response();
    };
    Json(json!({
        "user": {
            "id": auth.user_id.to_string(),
            "email": auth.email,
            "name": auth.display_name,
        },
        "tenant": {
            "id": auth.tenant_id.to_string(),
        },
        "dev": auth.is_dev,
    }))
    .into_response()
}

/// In OIDC mode this route is gated by `OidcLoginLayer`, so any unauth'd
/// request triggers the IdP redirect chain. After a successful callback the
/// user lands here and we 302 to the configured post-login URL.
///
/// In dev mode it's just a 302 — auth is auto-injected anyway.
pub async fn login(State(state): State<AppState>) -> Response {
    Redirect::to(state.auth.post_login_redirect.as_str()).into_response()
}

pub async fn logout(session: Session) -> Response {
    if let Err(e) = session.flush().await {
        tracing::warn!(error = %e, "session flush failed");
    }
    StatusCode::NO_CONTENT.into_response()
}

// Allow `Option<AuthContext>` as a handler argument: `me` accepts the absence
// case (returning 401) instead of failing with the extractor's default 401.
impl<S> axum::extract::OptionalFromRequestParts<S> for AuthContext
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Option<Self>, Self::Rejection> {
        Ok(parts.extensions.get::<AuthContext>().cloned())
    }
}
