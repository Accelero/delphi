//! JWT plumbing — both directions.
//!
//! Two responsibilities, both about JWTs:
//!
//! 1. **Inbound** — [`JwtClaimsExtractor`]. Reads
//!    `Authorization: Bearer <jwt>` from incoming requests, decodes the
//!    payload, and produces a [`Claims`] struct downstream code can
//!    rely on. The signature is **not** validated by the backend; the
//!    BFF (Traefik + oauth2-proxy + Keycloak's JWKS) already did that.
//!    Defence-in-depth (backend re-validates) is a small drop-in via
//!    `jsonwebtoken::decode`; not load-bearing today.
//!
//! 2. **Outbound** — [`SessionTokenSigner`]. Mints a SurrealDB-scoped
//!    JWT from an [`AuthContext`] for `db.authenticate(jwt)` per
//!    request. Validated by SurrealDB against the matching
//!    `DEFINE ACCESS … TYPE RECORD WITH JWT` (see
//!    [`crate::storage::SystemDb::define_jwt_access`]); engine-side
//!    PERMISSIONS clauses then enforce tenant isolation per query.
//!
//! HS512 only today. The same secret lives on the backend and in the
//! `DEFINE ACCESS` clause SurrealDB validates with.

use async_trait::async_trait;
use axum::http::{HeaderMap, HeaderName};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::Utc;
use jsonwebtoken::{encode, EncodingKey, Header};
use serde::{Deserialize, Serialize};

use super::claims::{Claims, ClaimsError, ClaimsExtractor};
use super::context::AuthContext;
use crate::error::Error;

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

// ─── outbound: AuthContext → SurrealDB JWT ────────────────────────────────

/// Claims the backend embeds in the SurrealDB session token. Field
/// names match what `PERMISSIONS` clauses reference (`$token.tenant_id`,
/// `$token.iss`, etc.).
///
/// `ID` (uppercase) is required by SurrealDB's `RECORD` access method
/// — it's the record id the AUTHENTICATE clause maps to. Lowercase
/// `id` doesn't work; the engine looks for the exact `ID` key.
#[derive(Debug, Serialize)]
struct SessionClaims<'a> {
    #[serde(rename = "ID")]
    id: String,
    #[serde(rename = "ac")]
    access: &'a str,
    #[serde(rename = "ns")]
    namespace: &'a str,
    #[serde(rename = "db")]
    database: &'a str,
    iss: &'a str,
    sub: &'a str,
    tenant_id: String,
    roles: &'a [String],
    iat: i64,
    exp: i64,
}

#[derive(Debug, Clone)]
pub struct SessionTokenSigner {
    secret: Vec<u8>,
    namespace: String,
    database: String,
    /// Name of the matching `DEFINE ACCESS` method. Goes into the `ac`
    /// claim. SurrealDB uses this to route the token to the right
    /// access definition.
    access_method: String,
    /// Per-token lifetime, seconds. Should be ≥ the typical request
    /// duration.
    ttl_secs: i64,
}

impl SessionTokenSigner {
    pub fn new(
        secret: impl Into<Vec<u8>>,
        namespace: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self {
            secret: secret.into(),
            namespace: namespace.into(),
            database: database.into(),
            access_method: "app_session".to_string(),
            ttl_secs: 300, // 5 min — covers any normal request lifetime.
        }
    }

    pub fn with_access_method(mut self, name: impl Into<String>) -> Self {
        self.access_method = name.into();
        self
    }

    pub fn with_ttl_secs(mut self, ttl: i64) -> Self {
        self.ttl_secs = ttl;
        self
    }

    /// Mint a session token for the given [`AuthContext`]. The
    /// returned string is the input to `db.authenticate(jwt)`.
    pub fn sign(&self, auth: &AuthContext) -> crate::error::Result<String> {
        let now = Utc::now().timestamp();
        let claims = SessionClaims {
            id: auth.user_id.to_string(),
            access: &self.access_method,
            namespace: &self.namespace,
            database: &self.database,
            iss: &auth.iss,
            sub: &auth.sub,
            tenant_id: auth.tenant_id.to_string(),
            roles: &auth.roles,
            iat: now,
            exp: now + self.ttl_secs,
        };
        let header = Header::new(jsonwebtoken::Algorithm::HS512);
        encode(&header, &claims, &EncodingKey::from_secret(&self.secret))
            .map_err(|e| Error::InvalidConfig(format!("session-token mint: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use jsonwebtoken::{decode, DecodingKey, Validation};
    use serde_json::json;
    use surrealdb::RecordId;

    /// Construct an unsigned JWT (header.payload.empty-sig). The
    /// extractor doesn't validate signatures, so this is enough for
    /// tests of the inbound path.
    fn craft_unsigned_jwt(payload: serde_json::Value) -> String {
        let header = URL_SAFE_NO_PAD.encode(b"{\"alg\":\"none\"}");
        let body = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
        let sig = URL_SAFE_NO_PAD.encode(b"");
        format!("{header}.{body}.{sig}")
    }

    fn ctx() -> AuthContext {
        AuthContext {
            user_id: RecordId::from(("app_user", "alice")),
            tenant_id: RecordId::from(("tenant", "acme")),
            email: "alice@delphi.test".into(),
            display_name: Some("Alice".into()),
            iss: "https://idp.test/".into(),
            sub: "alice".into(),
            roles: vec!["member".into()],
            is_dev: false,
        }
    }

    // ── inbound extractor ─────────────────────────────────────────────────

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

    // ── outbound signer ───────────────────────────────────────────────────

    #[derive(Debug, Deserialize)]
    struct DecodedSessionClaims {
        #[serde(rename = "ID")]
        id: String,
        #[serde(rename = "ac")]
        access: String,
        tenant_id: String,
        roles: Vec<String>,
        iss: String,
        sub: String,
    }

    #[test]
    fn signer_mint_and_decode_roundtrip() {
        let signer = SessionTokenSigner::new(b"secret-for-test".to_vec(), "delphi", "main");
        let token = signer.sign(&ctx()).unwrap();

        let mut validation = Validation::new(jsonwebtoken::Algorithm::HS512);
        validation.validate_aud = false;
        let decoded = decode::<DecodedSessionClaims>(
            &token,
            &DecodingKey::from_secret(b"secret-for-test"),
            &validation,
        )
        .unwrap();

        assert_eq!(decoded.claims.id, "app_user:alice");
        assert_eq!(decoded.claims.access, "app_session");
        assert_eq!(decoded.claims.tenant_id, "tenant:acme");
        assert_eq!(decoded.claims.iss, "https://idp.test/");
        assert_eq!(decoded.claims.sub, "alice");
        assert_eq!(decoded.claims.roles, vec!["member"]);
    }

    #[test]
    fn signer_rejects_wrong_secret() {
        let signer = SessionTokenSigner::new(b"right".to_vec(), "delphi", "main");
        let token = signer.sign(&ctx()).unwrap();
        let mut validation = Validation::new(jsonwebtoken::Algorithm::HS512);
        validation.validate_aud = false;
        let bad = decode::<DecodedSessionClaims>(
            &token,
            &DecodingKey::from_secret(b"wrong"),
            &validation,
        );
        assert!(bad.is_err());
    }
}
