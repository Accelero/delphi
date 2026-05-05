//! OIDC mode wiring.
//!
//! Two layers + one extension:
//!
//! - [`OidcAuthLayer`] (from `axum-oidc`) runs first: reads the session,
//!   verifies/refreshes tokens, populates [`OidcClaims`] / [`OidcAccessToken`]
//!   into request extensions. Doesn't force authentication.
//! - [`OidcLoginLayer`] (from `axum-oidc`) runs second: forces redirect to
//!   the IdP if no [`OidcAccessToken`] is present. After successful callback,
//!   the user lands on the same URL they originally hit.
//! - [`ensure_user_ctx`] runs after both: reads claims, looks up / creates
//!   the `app_user` row, attaches an [`AuthContext`] for downstream handlers.
//!
//! Tenant resolution: we parameterize axum-oidc on a flexible
//! [`ExtraClaims`] so additional JWT claims (whatever the IdP issues) are
//! available; the tenant claim's *name* comes from `OIDC_TENANT_CLAIM`.

use std::sync::Arc;

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use axum_oidc::{AdditionalClaims, OidcClaims};
use serde::{Deserialize, Serialize};
use surrealdb::engine::remote::ws::Client;
use surrealdb::{RecordId, Surreal};

use crate::auth::bootstrap;
use crate::auth::config::OidcConfig;

/// JWT claims passthrough — we read whatever the IdP put in the ID token
/// and look up the configured tenant claim by name at runtime.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExtraClaims {
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}
impl AdditionalClaims for ExtraClaims {}
impl openidconnect::AdditionalClaims for ExtraClaims {}

/// Bundle the OIDC-mode dependencies that `ensure_user_ctx` needs from the
/// router's extension layers.
#[derive(Clone)]
pub struct OidcDeps {
    pub db: Surreal<Client>,
    pub config: Arc<OidcConfig>,
    pub default_tenant_id: Arc<RecordId>,
}

/// After the OIDC middleware has populated request extensions, build our
/// internal [`AuthContext`] (looking up / creating the `app_user`) and stash
/// it for handlers via the [`AuthContext`] extractor.
pub async fn ensure_user_ctx(
    Extension(deps): Extension<OidcDeps>,
    mut req: Request,
    next: Next,
) -> Response {
    let claims = req
        .extensions()
        .get::<OidcClaims<ExtraClaims>>()
        .cloned();
    let Some(claims) = claims else {
        // Not authenticated yet. OidcLoginLayer should have redirected
        // before reaching here for protected routes; if we somehow arrived
        // unauth'd, fall through and let downstream extractors 401.
        return next.run(req).await;
    };

    let claims = claims.0;
    let iss = claims.issuer().to_string();
    let sub = claims.subject().to_string();
    let email = claims
        .email()
        .map(|e| e.to_string())
        .unwrap_or_default();
    let display_name = claims
        .name()
        .and_then(|loc| loc.get(None).map(|n| n.to_string()))
        .or_else(|| {
            claims
                .preferred_username()
                .map(|s| s.to_string())
        });

    let tenant_slug = claims
        .additional_claims()
        .extra
        .get(&deps.config.tenant_claim)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let auth = match bootstrap::ensure_oidc_user(
        &deps.db,
        &iss,
        &sub,
        &email,
        display_name.as_deref(),
        tenant_slug.as_deref(),
        &deps.config.default_tenant_slug,
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

    req.extensions_mut().insert(auth);
    next.run(req).await
}
