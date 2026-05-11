//! The identity middleware: the single point at which a request's [`Claims`]
//! become an [`AuthContext`] downstream code can rely on.
//!
//! Pipeline (per request):
//!
//! 1. Look up the configured [`ClaimsExtractor`] from request extensions.
//! 2. Call `extract(&headers)` — fails fast with 401 on `Missing` / `Invalid`.
//! 3. [`super::ensure_user`] does the SELECT-then-CREATE on `app_user` /
//!    `membership` against the privileged [`SystemDb`], resolving the
//!    tenant by slug. **Must run before step 4** — SurrealDB's
//!    AUTHENTICATE clause for `app_session` looks up the user row by
//!    `(iss, sub)` and fails closed if it doesn't exist yet.
//! 4. Pull the bearer JWT from the request, acquire a connection from
//!    [`RequestDbPool`], and call `db.authenticate(bearer)` on it. The
//!    resulting [`AuthedDb`] is a RECORD session — engine-side
//!    `PERMISSIONS` clauses fire on every subsequent query.
//! 5. Stash the [`AuthContext`] and the [`AuthedDb`] in request
//!    extensions for handlers to extract.
//!
//! The privileged [`SystemDb`] handle is plumbed through [`IdentityDeps`]
//! (an axum `Extension` attached at startup). Handlers do not see it —
//! they only get the [`AuthContext`] this middleware produces.

use std::sync::Arc;

use axum::extract::Request;
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use surrealdb::RecordId;

use super::bootstrap;
use super::claims::{ClaimsError, ClaimsExtractor};
use crate::storage::{RequestDbPool, SystemDb};

/// Bundle the per-request dependencies the middleware needs. Attached to the
/// router as an `Extension` once at startup, then cloned cheaply per request.
#[derive(Clone)]
pub struct IdentityDeps {
    pub system: Arc<SystemDb>,
    pub pool: RequestDbPool,
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
            tracing::error!(error = ?e, "user upsert failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "auth setup failed").into_response();
        }
    };

    // Mark dev requests so handlers can branch on it (e.g., the `/me` route
    // surfaces it to the frontend for the dev-mode banner).
    auth.is_dev = claims.iss == "dev://local";

    // Acquire an authenticated SurrealDB handle for the request. The same
    // bearer that authenticated this request to the backend authenticates
    // every DB query that follows; SurrealDB validates it against the
    // `app_session` access method (see `SystemDb::define_jwt_access`).
    let bearer = match bearer_token(req.headers()) {
        Some(b) => b.to_string(),
        None => {
            // `extract` already covered the missing-Authorization case for
            // JWT mode; reaching here means the extractor accepted some
            // non-Bearer scheme. Fail closed — the DB needs a JWT.
            return (StatusCode::UNAUTHORIZED, "missing bearer token").into_response();
        }
    };
    let authed_db = match deps.pool.acquire(&bearer).await {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(error = %e, "db.authenticate failed");
            return (StatusCode::UNAUTHORIZED, "db auth failed").into_response();
        }
    };

    req.extensions_mut().insert(auth);
    req.extensions_mut().insert(Arc::new(authed_db));
    next.run(req).await
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let v = headers.get(axum::http::header::AUTHORIZATION)?.to_str().ok()?;
    v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer "))
}
