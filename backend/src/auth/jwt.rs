//! SurrealDB session-token minting.
//!
//! The backend mints a per-request JWT from an [`AuthContext`] and hands
//! it to `db.authenticate(jwt)`. SurrealDB validates the signature
//! against the matching `DEFINE ACCESS … TYPE RECORD WITH JWT` (see
//! [`crate::storage::SystemDb::define_jwt_access`]), resolves
//! `$auth = app_user`, exposes the JWT's other claims as `$token.*`,
//! and applies the table `PERMISSIONS` clauses on every subsequent
//! query.
//!
//! Today: HS512 only. The same `SURREAL_JWT_SECRET` lives on the
//! backend and in the `DEFINE ACCESS` clause SurrealDB validates with.
//! When we want a real OIDC IdP's JWTs to be forwarded straight to
//! SurrealDB (no re-signing — defence in depth), `JwtAccessKind::Jwks`
//! is the corresponding access shape; the backend's minting helper
//! stops being on the hot path for that flow.

use chrono::Utc;
use jsonwebtoken::{encode, EncodingKey, Header};
use serde::Serialize;
use surrealdb::RecordId;

use super::context::AuthContext;
use crate::error::{Error, Result};

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

/// Configuration for the backend's session-token signer.
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
    /// duration. Defaults are bound at construction time.
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
    pub fn sign(&self, auth: &AuthContext) -> Result<String> {
        let now = Utc::now().timestamp();
        let claims = SessionClaims {
            id: record_id_to_string(&auth.user_id),
            access: &self.access_method,
            namespace: &self.namespace,
            database: &self.database,
            iss: &auth.iss,
            sub: &auth.sub,
            tenant_id: record_id_to_string(&auth.tenant_id),
            roles: &auth.roles,
            iat: now,
            exp: now + self.ttl_secs,
        };
        let header = Header::new(jsonwebtoken::Algorithm::HS512);
        encode(&header, &claims, &EncodingKey::from_secret(&self.secret))
            .map_err(|e| Error::InvalidConfig(format!("session-token mint: {e}")))
    }
}

/// `RecordId` Display is `table:key`; that's what SurrealDB wants in
/// the `ID` claim and in `$token.tenant_id` references.
fn record_id_to_string(id: &RecordId) -> String {
    id.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{decode, DecodingKey, Validation};
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct DecodedClaims {
        #[serde(rename = "ID")]
        id: String,
        #[serde(rename = "ac")]
        access: String,
        tenant_id: String,
        roles: Vec<String>,
        iss: String,
        sub: String,
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

    #[test]
    fn mint_and_decode_roundtrip() {
        let signer = SessionTokenSigner::new(b"secret-for-test".to_vec(), "delphi", "main");
        let token = signer.sign(&ctx()).unwrap();

        let mut validation = Validation::new(jsonwebtoken::Algorithm::HS512);
        validation.validate_aud = false;
        let decoded = decode::<DecodedClaims>(
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
    fn token_signature_rejects_wrong_secret() {
        let signer = SessionTokenSigner::new(b"right".to_vec(), "delphi", "main");
        let token = signer.sign(&ctx()).unwrap();

        let mut validation = Validation::new(jsonwebtoken::Algorithm::HS512);
        validation.validate_aud = false;
        let bad = decode::<DecodedClaims>(
            &token,
            &DecodingKey::from_secret(b"wrong"),
            &validation,
        );
        assert!(bad.is_err());
    }
}
