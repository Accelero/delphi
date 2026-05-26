//! Dev-mode JWT injector (gated by the `dev-auth` cargo feature).
//!
//! In production an upstream BFF (Traefik + oauth2-proxy + Keycloak)
//! forwards the IdP-issued JWT to the backend as
//! `Authorization: Bearer <jwt>`. In dev we don't run the BFF — this
//! middleware mints the same shape of bearer locally, signed with the
//! shared HS512 secret SurrealDB's `app_session` access method
//! validates against (see [`crate::storage::SystemDb::define_jwt_access`]).
//!
//! That keeps the dev path a strict subset of the production path:
//! the exact same [`super::JwtClaimsExtractor`] decodes the bearer,
//! the exact same [`super::ensure_user`] upsert runs, the exact same
//! [`super::AuthedDb`] acquisition and `db.authenticate(jwt)` step
//! follow. The only thing that differs between dev and prod is the
//! source of the JWT.
//!
//! Defence-in-depth: this also strips any inbound `Authorization` from
//! the client. In dev nothing should ever set it externally — but if
//! something did (a misconfigured local proxy, a curl test gone wrong),
//! we don't want to honour it. Strip-then-mint ensures the dev identity
//! is always exactly what `DevConfig` says.

use axum::extract::Request;
use axum::http::{HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;
use axum::Extension;
use chrono::Utc;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::Serialize;

use super::config::DevConfig;

const AUTHORIZATION: HeaderName = HeaderName::from_static("authorization");

/// Claim shape the dev JWT carries. Field names match what
/// [`super::JwtClaimsExtractor`] decodes from real IdP tokens and what
/// SurrealDB's `app_session` access method expects (`ac` / `ns` / `db`
/// for routing, `iss` / `sub` for the AUTHENTICATE clause).
#[derive(Debug, Serialize)]
struct DevClaims<'a> {
    sub: &'a str,
    iss: &'a str,
    email: &'a str,
    preferred_username: &'a str,
    tenant_id: &'a str,
    /// Leaf capabilities the dev user holds. The backend never checks
    /// for hierarchical roles like `owner`; composition (if any) is
    /// configured in Keycloak and flattened into the token there. The
    /// dev injector emits the same leaf-only shape directly.
    roles: &'a [&'a str],
    // SurrealDB session routing.
    ac: &'static str,
    ns: &'a str,
    db: &'a str,
    iat: i64,
    exp: i64,
}

pub async fn dev_inject_middleware(
    Extension(cfg): Extension<DevConfig>,
    mut req: Request,
    next: Next,
) -> Response {
    let h = req.headers_mut();
    h.remove(&AUTHORIZATION);
    let jwt = mint_dev_jwt(&cfg);
    // The token is a base64-url ASCII string with no CR/LF — `from_str`
    // only fails on illegal bytes, which our encoder cannot produce.
    let value = HeaderValue::from_str(&format!("Bearer {jwt}"))
        .expect("dev JWT is valid header content");
    h.insert(&AUTHORIZATION, value);
    next.run(req).await
}

fn mint_dev_jwt(cfg: &DevConfig) -> String {
    let now = Utc::now().timestamp();
    let claims = DevClaims {
        sub: "dev-user",
        iss: "dev://local",
        email: &cfg.user_email,
        preferred_username: &cfg.user_name,
        tenant_id: &cfg.tenant_slug,
        roles: &["ingester"],
        ac: "app_session",
        ns: &cfg.surreal_ns,
        db: &cfg.surreal_db,
        iat: now,
        // 24h. Refreshed every request so a long-running session never
        // expires from under the developer.
        exp: now + 86_400,
    };
    encode(
        &Header::new(Algorithm::HS512),
        &claims,
        &EncodingKey::from_secret(cfg.jwt_secret.as_bytes()),
    )
    .expect("dev JWT encode: claim shape is fixed and known-good")
}

#[cfg(test)]
mod tests {
    //! Equivalence test: the JWT the dev injector mints round-trips
    //! through `JwtClaimsExtractor` to the same `Claims` shape a
    //! production IdP would produce. This is the formal guarantee that
    //! "dev mode is a strict subset of production": same extractor,
    //! same parsing, same `Claims` — only the *source* of the JWT
    //! differs.

    use super::*;
    use crate::auth::{ClaimsExtractor, Hs512Validator, JwtClaimsExtractor, JwtValidator};
    use axum::http::HeaderMap;
    use std::sync::Arc;

    fn dev_cfg() -> DevConfig {
        DevConfig {
            tenant_slug: "dev".into(),
            user_email: "dev@delphi.local".into(),
            user_name: "Dev User".into(),
            jwt_secret: "test-dev-secret".into(),
            surreal_ns: "delphi".into(),
            surreal_db: "main".into(),
        }
    }

    fn headers_with_dev_jwt(cfg: &DevConfig) -> HeaderMap {
        let jwt = mint_dev_jwt(cfg);
        let mut h = HeaderMap::new();
        h.insert(
            &AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {jwt}")).unwrap(),
        );
        h
    }

    #[tokio::test]
    async fn dev_jwt_round_trips_through_extractor() {
        let cfg = dev_cfg();
        let headers = headers_with_dev_jwt(&cfg);

        // Same secret — the backend's defence-in-depth validator
        // (audit N3) accepts the dev injector's signed JWT, just as
        // SurrealDB's `app_session` does engine-side.
        let validator: Arc<dyn JwtValidator> =
            Arc::new(Hs512Validator::new(&cfg.jwt_secret, None, None));
        let claims = JwtClaimsExtractor::new(validator)
            .extract(&headers)
            .await
            .expect("dev JWT must parse cleanly");

        assert_eq!(claims.iss, "dev://local");
        assert_eq!(claims.sub, "dev-user");
        assert_eq!(claims.email, cfg.user_email);
        assert_eq!(claims.display_name.as_deref(), Some(cfg.user_name.as_str()));
        assert_eq!(claims.tenant_slug.as_deref(), Some(cfg.tenant_slug.as_str()));
        assert_eq!(claims.roles, vec!["ingester".to_string()]);
    }

    #[tokio::test]
    async fn dev_jwt_signature_is_hs512_against_shared_secret() {
        // The whole point of the cutover is that SurrealDB validates the
        // signature. We don't re-validate here — JwtClaimsExtractor
        // decodes the payload only — but we do confirm the header
        // declares HS512, so a configuration drift to a different alg
        // would be visible in this test.
        let cfg = dev_cfg();
        let jwt = mint_dev_jwt(&cfg);
        let header = jsonwebtoken::decode_header(&jwt).expect("decode header");
        assert_eq!(header.alg, Algorithm::HS512);
    }
}
