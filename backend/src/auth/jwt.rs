//! Inbound JWT extraction.
//!
//! [`JwtClaimsExtractor`] reads `Authorization: Bearer <jwt>`, decodes
//! the payload (no signature check — the BFF validates against the
//! IdP's JWKS upstream), and produces a [`Claims`] struct for the
//! identity middleware. The bearer is then forwarded unchanged to
//! SurrealDB via `db.authenticate(jwt)`, where the engine validates
//! signature + AUTHENTICATE clause against the `app_session` access
//! method (see [`crate::storage::SystemDb::define_jwt_access`]).
//!
//! There is no outbound JWT minting in the backend any more — the IdP
//! is the only issuer, and SurrealDB the only consumer below us. The
//! defence-in-depth slot (backend re-validates the JWT signature
//! against the IdP's JWKS) is open as audit finding N3.

use async_trait::async_trait;
use axum::http::{HeaderMap, HeaderName};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::Deserialize;

use super::claims::{Claims, ClaimsError, ClaimsExtractor};

// ─── inbound: JWT → Claims ────────────────────────────────────────────────

const H_AUTHORIZATION: HeaderName = HeaderName::from_static("authorization");

/// Reads `Authorization: Bearer <jwt>`, decodes the payload (no
/// signature validation — BFF already did it), returns [`Claims`].
///
/// 401 on missing or malformed Authorization. The single source of
/// identity for the production path.
#[derive(Default, Clone)]
pub struct JwtClaimsExtractor;

impl JwtClaimsExtractor {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ClaimsExtractor for JwtClaimsExtractor {
    async fn extract(
        &self,
        headers: &HeaderMap,
    ) -> std::result::Result<Claims, ClaimsError> {
        let bearer = bearer_token(headers).ok_or(ClaimsError::Missing)?;
        let payload = decode_jwt_payload(bearer)
            .ok_or_else(|| ClaimsError::Invalid("malformed JWT in Authorization header".into()))?;

        let sub = payload.sub.ok_or_else(|| {
            ClaimsError::Invalid("JWT missing required `sub` claim".into())
        })?;
        let iss = payload.iss.ok_or_else(|| {
            ClaimsError::Invalid("JWT missing required `iss` claim".into())
        })?;
        let email = payload.email.ok_or_else(|| {
            ClaimsError::Invalid("JWT missing required `email` claim".into())
        })?;

        Ok(Claims {
            sub,
            iss,
            email,
            display_name: payload.preferred_username.or(payload.name),
            tenant_slug: payload.tenant_id,
            roles: payload.roles.unwrap_or_default(),
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

/// Decode the payload segment of a JWT without verifying the signature.
/// The BFF already verified against the IdP's JWKS — we just need the
/// claims.
fn decode_jwt_payload(jwt: &str) -> Option<InboundClaims> {
    let mut parts = jwt.split('.');
    let _header = parts.next()?;
    let payload_b64 = parts.next()?;
    let bytes = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
    serde_json::from_slice::<InboundClaims>(&bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    use serde_json::json;

    /// Construct an unsigned JWT (header.payload.empty-sig). The
    /// extractor doesn't validate signatures, so this is enough for
    /// tests of the inbound path.
    fn craft_unsigned_jwt(payload: serde_json::Value) -> String {
        let header = URL_SAFE_NO_PAD.encode(b"{\"alg\":\"none\"}");
        let body = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
        let sig = URL_SAFE_NO_PAD.encode(b"");
        format!("{header}.{body}.{sig}")
    }

    #[tokio::test]
    async fn extracts_full_claims_from_bearer_jwt() {
        let jwt = craft_unsigned_jwt(json!({
            "sub":  "alice-uuid",
            "iss":  "http://localhost:8088/realms/delphi",
            "email": "alice@delphi.test",
            "preferred_username": "alice",
            "tenant_id": "tenant-a",
            "roles": ["member", "owner"],
        }));
        let mut h = HeaderMap::new();
        h.insert(
            &H_AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {jwt}")).unwrap(),
        );

        let claims = JwtClaimsExtractor::new().extract(&h).await.unwrap();
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
            JwtClaimsExtractor::new().extract(&h).await,
            Err(ClaimsError::Missing)
        ));
    }

    #[tokio::test]
    async fn malformed_bearer_is_invalid() {
        let mut h = HeaderMap::new();
        h.insert(&H_AUTHORIZATION, HeaderValue::from_static("Bearer not-a-jwt"));
        assert!(matches!(
            JwtClaimsExtractor::new().extract(&h).await,
            Err(ClaimsError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn jwt_missing_required_claim_is_invalid() {
        // sub is required; this JWT has only iss and email.
        let jwt = craft_unsigned_jwt(json!({
            "iss":  "http://localhost:8088/realms/delphi",
            "email": "alice@delphi.test",
        }));
        let mut h = HeaderMap::new();
        h.insert(
            &H_AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {jwt}")).unwrap(),
        );
        assert!(matches!(
            JwtClaimsExtractor::new().extract(&h).await,
            Err(ClaimsError::Invalid(_))
        ));
    }

}
