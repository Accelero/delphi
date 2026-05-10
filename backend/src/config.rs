//! Construct backend services from environment variables.
//!
//! Keeping these factories centralized means the rest of the codebase
//! never imports a concrete backend or reads env directly.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::storage::{JwtAccessConfig, JwtAccessKind, SystemDb};

/// Construct the privileged [`SystemDb`] from environment. Used by the
/// bin (`api::serve`) and the admin CLI.
pub async fn system_db_from_env() -> Result<Arc<SystemDb>> {
    Ok(Arc::new(SystemDb::from_env().await?))
}

/// Load the runtime config for the `app_session` JWT access method.
///
/// `SURREAL_JWT_MODE=jwks` — production. Requires `SURREAL_JWT_JWKS_URL`
/// pointing at the IdP's JWKS endpoint (e.g.
/// `http://keycloak:8080/realms/delphi/protocol/openid-connect/certs`).
/// SurrealDB fetches public keys from that URL on first auth and
/// caches them.
///
/// `SURREAL_JWT_MODE=hs512` — tier-1 dev and tests. Requires
/// `SURREAL_JWT_SECRET`. The dev-injector middleware (or the test
/// harness) mints JWTs signed with the same secret.
///
/// `SURREAL_JWT_EXPECTED_ISSUER` / `SURREAL_JWT_EXPECTED_AUDIENCE` —
/// optional. When set, the AUTHENTICATE clause throws on mismatching
/// `iss` / `aud`.
pub fn jwt_access_from_env() -> Result<JwtAccessConfig> {
    let mode = std::env::var("SURREAL_JWT_MODE").unwrap_or_else(|_| "hs512".into());
    let kind = match mode.as_str() {
        "jwks" => JwtAccessKind::Jwks {
            url: env_required("SURREAL_JWT_JWKS_URL")?,
        },
        "hs512" => JwtAccessKind::Hs512 {
            secret: env_required("SURREAL_JWT_SECRET")?,
        },
        other => {
            return Err(Error::InvalidConfig(format!(
                "SURREAL_JWT_MODE={other:?}; expected 'jwks' or 'hs512'"
            )))
        }
    };
    Ok(JwtAccessConfig {
        kind,
        expected_issuer: std::env::var("SURREAL_JWT_EXPECTED_ISSUER").ok(),
        expected_audience: std::env::var("SURREAL_JWT_EXPECTED_AUDIENCE").ok(),
        session_duration_secs: std::env::var("SURREAL_JWT_SESSION_SECS")
            .ok()
            .and_then(|s| s.parse().ok()),
    })
}

fn env_required(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| Error::EnvMissing(key.into()))
}
