//! The identity middleware: the single point at which a request's [`Claims`]
//! become an [`AuthContext`] downstream code can rely on.
//!
//! Pipeline (per request):
//!
//! 1. Look up the configured [`ClaimsExtractor`] from request extensions.
//! 2. Call `extract(&headers)` — fails fast with 401 on `Missing` / `Invalid`.
//! 3. [`super::ensure_user`] does the SELECT-then-CREATE on `app_user` /
//!    `membership` against the privileged [`SystemDb`], resolving the
//!    tenant by slug.
//! 4. Stash the resulting [`AuthContext`] in request extensions for the
//!    `AuthContext` extractor to pull out in handlers.
//!
//! The privileged [`SystemDb`] handle is plumbed through [`IdentityDeps`]
//! (an axum `Extension` attached at startup). Handlers do not see it —
//! they only get the [`AuthContext`] this middleware produces.

use std::sync::Arc;

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use surrealdb::RecordId;

use super::bootstrap;
use super::claims::{ClaimsError, ClaimsExtractor};
use crate::storage::SystemDb;

/// Bundle the per-request dependencies the middleware needs. Attached to the
/// router as an `Extension` once at startup, then cloned cheaply per request.
#[derive(Clone)]
pub struct IdentityDeps {
    pub system: Arc<SystemDb>,
    pub default_tenant_slug: String,
    pub default_tenant_id: RecordId,
}

pub async fn identity_middleware(
    Extension(extractor): Extension<Arc<dyn ClaimsExtractor>>,
    Extension(deps): Extension<IdentityDeps>,
    mut req: Request,
    next: Next,
) -> Response {
    let claims = match extractor.extract(req.headers()).await {
        Ok(c) => c,
        Err(ClaimsError::Missing) => {
            return (StatusCode::UNAUTHORIZED, "not authenticated").into_response();
        }
        Err(ClaimsError::Invalid(msg)) => {
            tracing::warn!(reason = %msg, "rejecting invalid identity");
            return (StatusCode::UNAUTHORIZED, "invalid identity").into_response();
        }
    };

    let mut auth = match bootstrap::ensure_user(
        &deps.system,
        &claims,
        &deps.default_tenant_slug,
        &deps.default_tenant_id,
    )
    .await
    {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(error = %e, "user upsert failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "auth setup failed").into_response();
        }
    };

    // Mark dev requests so handlers can branch on it (e.g., the `/me` route
    // surfaces it to the frontend for the dev-mode banner).
    auth.is_dev = claims.iss == "dev://local";

    req.extensions_mut().insert(auth);
    next.run(req).await
}
