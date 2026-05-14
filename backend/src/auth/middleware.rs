//! The identity middleware: the single point at which a request's [`Claims`]
//! become an [`AuthContext`] downstream code can rely on.
//!
//! Pipeline (per request):
//!
//! 1. Look up the configured [`ClaimsExtractor`] from request extensions.
//! 2. Call `extract(&headers)` — fails fast with 401 on `Missing` / `Invalid`.
//! 3. Pull the bearer JWT, acquire a connection from [`RequestDbPool`],
//!    and call `db.authenticate(bearer)` on it. The `app_session`
//!    AUTHENTICATE clause resolves `(iss, sub)` → `app_user.id`; on
//!    success the session transitions into a RECORD scope.
//! 4. **Hot path** — query `$auth` for the resolved row fields
//!    ([`AuthedDb::resolve_auth`]) and build [`AuthContext`]. One DB
//!    roundtrip total per authenticated request.
//! 5. **Cold path** — if `db.authenticate` throws `'unknown user'`
//!    (brand-new identity, no `app_user` row yet),
//!    [`bootstrap::ensure_user`] provisions tenant + user + membership
//!    against the privileged [`SystemDb`], then we retry
//!    `pool.acquire(bearer)` once. The cold path runs at most once per
//!    user-lifetime; concurrent first-requests are tolerated by the
//!    `IndexExists` branch in [`bootstrap`].
//! 6. Stash the [`AuthContext`] and the [`AuthedDb`] in request
//!    extensions for handlers to extract.
//!
//! Any other authenticate failure (bad signature, expired token, wrong
//! issuer/audience) returns 401 without touching the system DB.

use std::sync::Arc;

use axum::extract::Request;
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use surrealdb::RecordId;

use super::bootstrap;
use super::claims::{Claims, ClaimsError, ClaimsExtractor};
use super::context::AuthContext;
use crate::error::Error;
use crate::storage::{AuthRecord, AuthedDb, RequestDbPool, SystemDb};

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

    let bearer = match bearer_token(req.headers()) {
        Some(b) => b.to_string(),
        None => {
            // `extract` already covered the missing-Authorization case for
            // JWT mode; reaching here means the extractor accepted some
            // non-Bearer scheme. Fail closed — the DB needs a JWT.
            return (StatusCode::UNAUTHORIZED, "missing bearer token").into_response();
        }
    };

    // Hot path: try to authenticate the pool connection directly. The
    // `app_session` AUTHENTICATE clause throws `'unknown user'` if no
    // `app_user` row exists yet for `(iss, sub)` — that's the only
    // failure mode we treat as a provisioning trigger; everything else
    // is a 401.
    let (authed_db, auth) = match deps.pool.acquire(&bearer).await {
        Ok(db) => match db.resolve_auth().await {
            Ok(rec) => (db, build_context(&claims, &rec)),
            Err(e) => {
                tracing::error!(error = ?e, "resolve_auth failed");
                return (StatusCode::INTERNAL_SERVER_ERROR, "auth setup failed").into_response();
            }
        },
        Err(e) if is_unknown_user(&e) => {
            match cold_path_provision(&deps, &claims, &bearer).await {
                Ok(pair) => pair,
                Err(resp) => return resp,
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "db.authenticate failed");
            return (StatusCode::UNAUTHORIZED, "db auth failed").into_response();
        }
    };

    req.extensions_mut().insert(auth);
    req.extensions_mut().insert(Arc::new(authed_db));
    next.run(req).await
}

/// Build `AuthContext` from claims + the resolved `$auth` row. Called on
/// every authenticated request once the session is established.
fn build_context(claims: &Claims, rec: &AuthRecord) -> AuthContext {
    AuthContext {
        user_id: rec.id.clone(),
        tenant_id: rec.tenant_id.clone(),
        email: rec.email.clone(),
        display_name: rec.display_name.clone(),
        iss: claims.iss.clone(),
        sub: claims.sub.clone(),
        roles: claims.roles.clone(),
        is_dev: claims.iss == "dev://local",
    }
}

/// Cold path: a fresh identity hit the API before `ensure_user` had ever
/// run for it. Provision the tenant + user + membership rows, then
/// retry `pool.acquire` so the rest of the request runs on the same
/// RECORD-authenticated session as the hot path. Returns either the
/// established `(AuthedDb, AuthContext)` pair or a fully-formed error
/// response.
async fn cold_path_provision(
    deps: &IdentityDeps,
    claims: &Claims,
    bearer: &str,
) -> std::result::Result<(AuthedDb, AuthContext), Response> {
    let mut auth = match bootstrap::ensure_user(
        &deps.system,
        claims,
        &deps.default_tenant_slug,
        &deps.default_tenant_id,
    )
    .await
    {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(error = ?e, "user upsert failed");
            return Err(
                (StatusCode::INTERNAL_SERVER_ERROR, "auth setup failed").into_response()
            );
        }
    };
    auth.is_dev = claims.iss == "dev://local";

    let db = match deps.pool.acquire(bearer).await {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(error = %e, "db.authenticate failed after provisioning");
            return Err((StatusCode::UNAUTHORIZED, "db auth failed").into_response());
        }
    };
    Ok((db, auth))
}

/// True if `err` is the `THROW 'unknown user'` from the `app_session`
/// AUTHENTICATE clause. We deliberately match on the literal string the
/// schema raises (defined in `SystemDb::define_jwt_access`); other
/// authenticate failures (bad sig, expired, wrong iss/aud) carry
/// different messages and must stay 401, not 500.
fn is_unknown_user(err: &Error) -> bool {
    let mut e: Option<&dyn std::error::Error> = Some(err);
    while let Some(cur) = e {
        if cur.to_string().contains("unknown user") {
            return true;
        }
        e = cur.source();
    }
    false
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let v = headers.get(axum::http::header::AUTHORIZATION)?.to_str().ok()?;
    v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer "))
}
