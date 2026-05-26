//! Inbound JWT extraction.
//!
//! [`JwtClaimsExtractor`] reads `Authorization: Bearer <jwt>`,
//! validates the signature + standard claims via the injected
//! [`JwtValidator`], and lifts the verified payload into a [`Claims`]
//! struct for the identity middleware. The bearer is then forwarded
//! unchanged to SurrealDB via `db.authenticate(jwt)`, where the engine
//! re-validates against the matching `app_session` access method
//! (see [`crate::storage::SystemDb::define_jwt_access`]).
//!
//! The backend and SurrealDB validate the same JWT against the same
//! key material, configured from a single `SURREAL_JWT_*` env knob —
//! belt and braces against forged tokens at the backend port and at
//! the engine's permissions boundary.
//!
//! There is no outbound JWT minting in the backend. The IdP is the
//! only issuer; SurrealDB is the only consumer below us.

use std::sync::Arc;

use async_trait::async_trait;
use axum::http::{HeaderMap, HeaderName};
use serde::Deserialize;

use super::claims::{Claims, ClaimsError, ClaimsExtractor};
use super::validator::JwtValidator;

const H_AUTHORIZATION: HeaderName = HeaderName::from_static("authorization");

/// Reads `Authorization: Bearer <jwt>`, validates it, returns [`Claims`].
///
/// 401 on missing or invalid Authorization (signature mismatch, expired,
/// iss/aud mismatch, missing required claim). The single source of
/// identity for the production path.
#[derive(Clone)]
pub struct JwtClaimsExtractor {
    validator: Arc<dyn JwtValidator>,
}

impl JwtClaimsExtractor {
    pub fn new(validator: Arc<dyn JwtValidator>) -> Self {
        Self { validator }
    }
}

#[async_trait]
impl ClaimsExtractor for JwtClaimsExtractor {
    async fn extract(
        &self,
        headers: &HeaderMap,
    ) -> std::result::Result<Claims, ClaimsError> {
        let bearer = bearer_token(headers).ok_or(ClaimsError::Missing)?;

        let payload = self
            .validator
            .validate(bearer)
            .await
            .map_err(|e| ClaimsError::Invalid(e.to_string()))?;

        let inbound: InboundClaims = serde_json::from_value(payload)
            .map_err(|e| ClaimsError::Invalid(format!("decode JWT payload: {e}")))?;

        let sub = inbound
            .sub
            .ok_or_else(|| ClaimsError::Invalid("JWT missing required `sub` claim".into()))?;
        let iss = inbound
            .iss
            .ok_or_else(|| ClaimsError::Invalid("JWT missing required `iss` claim".into()))?;
        let email = inbound
            .email
            .ok_or_else(|| ClaimsError::Invalid("JWT missing required `email` claim".into()))?;

        Ok(Claims {
            sub,
            iss,
            email,
            display_name: inbound.preferred_username.or(inbound.name),
            tenant_slug: inbound.tenant_id,
            roles: inbound.roles.unwrap_or_default(),
        })
    }
}

/// Subset of JWT claims the backend cares about. Anything else in the
/// token is ignored.
#[derive(Debug, Default, Deserialize)]
struct InboundClaims {
    sub: Option<String>,
    iss: Option<String>,
    email: Option<String>,
    preferred_username: Option<String>,
    name: Option<String>,
    /// Custom claim emitted by the IdP (Keycloak: user-attribute mapper).
    tenant_id: Option<String>,
    /// Top-level realm-role array.
    roles: Option<Vec<String>>,
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let v = headers.get(&H_AUTHORIZATION)?.to_str().ok()?;
    v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::validator::Hs512Validator;
    use axum::http::HeaderValue;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde_json::json;

    const SECRET: &str = "jwt-extractor-test-secret";

    fn extractor() -> JwtClaimsExtractor {
        JwtClaimsExtractor::new(Arc::new(Hs512Validator::new(SECRET, None, None)))
    }

    fn sign(payload: serde_json::Value) -> String {
        encode(
            &Header::new(Algorithm::HS512),
            &payload,
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .expect("sign")
    }

    fn now() -> i64 {
        chrono::Utc::now().timestamp()
    }

    fn bearer_headers(jwt: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            &H_AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {jwt}")).unwrap(),
        );
        h
    }

    #[tokio::test]
    async fn extracts_full_claims_from_bearer_jwt() {
        let jwt = sign(json!({
            "sub":  "alice-uuid",
            "iss":  "http://localhost:8088/realms/delphi",
            "email": "alice@delphi.test",
            "preferred_username": "alice",
            "tenant_id": "tenant-a",
            "roles": ["member", "owner"],
            "exp": now() + 60,
        }));

        let claims = extractor().extract(&bearer_headers(&jwt)).await.unwrap();
        assert_eq!(claims.sub, "alice-uuid");
        assert_eq!(claims.iss, "http://localhost:8088/realms/delphi");
        assert_eq!(claims.email, "alice@delphi.test");
        assert_eq!(claims.display_name.as_deref(), Some("alice"));
        assert_eq!(claims.tenant_slug.as_deref(), Some("tenant-a"));
        assert_eq!(claims.roles, vec!["member", "owner"]);
    }

    #[tokio::test]
    async fn missing_authorization_header_is_missing() {
        let h = HeaderMap::new();
        assert!(matches!(
            extractor().extract(&h).await,
            Err(ClaimsError::Missing)
        ));
    }

    #[tokio::test]
    async fn malformed_bearer_is_invalid() {
        let h = bearer_headers("not-a-jwt");
        assert!(matches!(
            extractor().extract(&h).await,
            Err(ClaimsError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn jwt_missing_required_claim_is_invalid() {
        let jwt = sign(json!({
            "iss":  "http://localhost:8088/realms/delphi",
            "email": "alice@delphi.test",
            "exp": now() + 60,
        }));
        assert!(matches!(
            extractor().extract(&bearer_headers(&jwt)).await,
            Err(ClaimsError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn jwt_with_bad_signature_is_invalid() {
        // Signed with the wrong secret — the validator must reject before
        // any claim is read.
        let jwt = encode(
            &Header::new(Algorithm::HS512),
            &json!({
                "sub": "alice", "iss": "i", "email": "a@b",
                "exp": now() + 60,
            }),
            &EncodingKey::from_secret(b"wrong-secret"),
        )
        .unwrap();
        assert!(matches!(
            extractor().extract(&bearer_headers(&jwt)).await,
            Err(ClaimsError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn expired_jwt_is_invalid() {
        let jwt = sign(json!({
            "sub": "alice", "iss": "i", "email": "a@b",
            "exp": now() - 300,
        }));
        assert!(matches!(
            extractor().extract(&bearer_headers(&jwt)).await,
            Err(ClaimsError::Invalid(_))
        ));
    }
}
