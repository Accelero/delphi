//! JWT signature + standard-claim validation for the inbound bearer token.
//!
//! The IdP signs the JWT, the BFF validates it against the IdP's JWKS,
//! and the backend re-validates as defence-in-depth (audit finding N3).
//! Without this layer, anything that bypasses the BFF and reaches the
//! backend port could mint identities with `alg: none` and forge any
//! `sub` / `tenant_id` it likes.
//!
//! Two implementations:
//!
//! - [`Hs512Validator`]: shared symmetric secret. Used by tier-1 dev
//!   ([`crate::auth::dev::dev_inject_middleware`]) and the integration
//!   test harness, both of which mint their own JWTs with the same
//!   secret SurrealDB validates against.
//! - [`JwksValidator`]: fetches the IdP's public keys from a JWKS URL,
//!   caches by `kid`, and re-fetches on cache miss so a key rotation
//!   costs at most one fetch per worker. Used in tier-2 dev and
//!   production with a real OIDC IdP.
//!
//! Both consume the same [`JwtAccessConfig`] passed to
//! [`crate::storage::SystemDb::define_jwt_access`], so the backend
//! validator and the SurrealDB engine validator stay configured from
//! one knob.
//!
//! Algorithm pinning: the JWKS validator pins the algorithm declared
//! by the JWK (asymmetric only — symmetric algs are filtered out at
//! cache-build time). This blocks the classic alg-confusion attack
//! where an attacker mints an HS256 JWT signed with the RSA public
//! key as the secret and the verifier obligingly checks it as HMAC.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use jsonwebtoken::jwk::{AlgorithmParameters, JwkSet, KeyAlgorithm};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde_json::Value;
use tokio::sync::RwLock;

use crate::storage::{JwtAccessConfig, JwtAccessKind};

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("malformed JWT: {0}")]
    Malformed(String),
    #[error("signature or claim validation failed: {0}")]
    Rejected(String),
    #[error("JWKS fetch failed: {0}")]
    JwksFetch(String),
    #[error("no JWKS key matches kid {0:?}")]
    UnknownKid(String),
}

/// Validates the signature + standard claims (`exp`, optional `iss` /
/// `aud`) of an inbound JWT and returns the verified payload as JSON.
#[async_trait]
pub trait JwtValidator: Send + Sync {
    async fn validate(&self, jwt: &str) -> Result<Value, ValidationError>;
}

/// Build a validator matching the `app_session` JWT access method
/// SurrealDB is configured with — one knob, one validation policy.
pub fn validator_from_jwt_access(cfg: &JwtAccessConfig) -> Arc<dyn JwtValidator> {
    match &cfg.kind {
        JwtAccessKind::Hs512 { secret } => Arc::new(Hs512Validator::new(
            secret,
            cfg.expected_issuer.clone(),
            cfg.expected_audience.clone(),
        )),
        JwtAccessKind::Jwks { url } => Arc::new(JwksValidator::new(
            url.clone(),
            cfg.expected_issuer.clone(),
            cfg.expected_audience.clone(),
        )),
    }
}

// ─── HS512 ────────────────────────────────────────────────────────────────

pub struct Hs512Validator {
    key: DecodingKey,
    validation: Validation,
}

impl Hs512Validator {
    pub fn new(
        secret: &str,
        expected_issuer: Option<String>,
        expected_audience: Option<String>,
    ) -> Self {
        let mut validation = Validation::new(Algorithm::HS512);
        apply_iss_aud(&mut validation, expected_issuer.as_deref(), expected_audience.as_deref());
        Self {
            key: DecodingKey::from_secret(secret.as_bytes()),
            validation,
        }
    }
}

#[async_trait]
impl JwtValidator for Hs512Validator {
    async fn validate(&self, jwt: &str) -> Result<Value, ValidationError> {
        decode::<Value>(jwt, &self.key, &self.validation)
            .map(|d| d.claims)
            .map_err(|e| ValidationError::Rejected(e.to_string()))
    }
}

// ─── JWKS ─────────────────────────────────────────────────────────────────

pub struct JwksValidator {
    url: String,
    http: reqwest::Client,
    cache: RwLock<HashMap<String, CachedKey>>,
    expected_issuer: Option<String>,
    expected_audience: Option<String>,
}

struct CachedKey {
    key: DecodingKey,
    /// Algorithm declared by the JWK. Verification is pinned to this
    /// — the JWT header's `alg` is not consulted for the choice.
    alg: Algorithm,
}

impl JwksValidator {
    pub fn new(
        url: String,
        expected_issuer: Option<String>,
        expected_audience: Option<String>,
    ) -> Self {
        Self {
            url,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("reqwest client builds"),
            cache: RwLock::new(HashMap::new()),
            expected_issuer,
            expected_audience,
        }
    }

    /// Fetch the JWKS from the IdP and replace the cache. Called on
    /// cache miss (kid not found). Tiny payload; one shot, no streaming.
    async fn refresh(&self) -> Result<(), ValidationError> {
        let resp = self
            .http
            .get(&self.url)
            .send()
            .await
            .map_err(|e| ValidationError::JwksFetch(e.to_string()))?
            .error_for_status()
            .map_err(|e| ValidationError::JwksFetch(e.to_string()))?;
        let set: JwkSet = resp
            .json()
            .await
            .map_err(|e| ValidationError::JwksFetch(format!("decode JWKS: {e}")))?;

        let mut next: HashMap<String, CachedKey> = HashMap::new();
        for jwk in set.keys.iter() {
            let Some(kid) = jwk.common.key_id.clone() else {
                continue;
            };
            if !is_asymmetric(&jwk.algorithm) {
                continue;
            }
            let Some(alg) = jwk
                .common
                .key_algorithm
                .and_then(asymmetric_alg_to_alg)
                .or_else(|| infer_alg_from_params(&jwk.algorithm))
            else {
                continue;
            };
            let key = match DecodingKey::from_jwk(jwk) {
                Ok(k) => k,
                Err(e) => {
                    tracing::warn!(kid, error = %e, "skipping unparseable JWK");
                    continue;
                }
            };
            next.insert(kid, CachedKey { key, alg });
        }

        *self.cache.write().await = next;
        Ok(())
    }

    async fn try_cached(&self, jwt: &str, kid: &str) -> Result<Option<Value>, ValidationError> {
        let cache = self.cache.read().await;
        let Some(cached) = cache.get(kid) else {
            return Ok(None);
        };
        let mut validation = Validation::new(cached.alg);
        apply_iss_aud(
            &mut validation,
            self.expected_issuer.as_deref(),
            self.expected_audience.as_deref(),
        );
        let token = decode::<Value>(jwt, &cached.key, &validation)
            .map_err(|e| ValidationError::Rejected(e.to_string()))?;
        Ok(Some(token.claims))
    }
}

#[async_trait]
impl JwtValidator for JwksValidator {
    async fn validate(&self, jwt: &str) -> Result<Value, ValidationError> {
        let header = decode_header(jwt).map_err(|e| ValidationError::Malformed(e.to_string()))?;
        let kid = header
            .kid
            .ok_or_else(|| ValidationError::Malformed("JWT header missing kid".into()))?;

        if let Some(verified) = self.try_cached(jwt, &kid).await? {
            return Ok(verified);
        }
        self.refresh().await?;
        self.try_cached(jwt, &kid)
            .await?
            .ok_or(ValidationError::UnknownKid(kid))
    }
}

// ─── helpers ──────────────────────────────────────────────────────────────

fn apply_iss_aud(
    validation: &mut Validation,
    expected_issuer: Option<&str>,
    expected_audience: Option<&str>,
) {
    if let Some(iss) = expected_issuer {
        validation.set_issuer(&[iss]);
    }
    match expected_audience {
        Some(aud) => validation.set_audience(&[aud]),
        None => validation.validate_aud = false,
    }
}

fn is_asymmetric(p: &AlgorithmParameters) -> bool {
    matches!(
        p,
        AlgorithmParameters::RSA(_)
            | AlgorithmParameters::EllipticCurve(_)
            | AlgorithmParameters::OctetKeyPair(_)
    )
}

fn asymmetric_alg_to_alg(k: KeyAlgorithm) -> Option<Algorithm> {
    use KeyAlgorithm as K;
    Some(match k {
        K::RS256 => Algorithm::RS256,
        K::RS384 => Algorithm::RS384,
        K::RS512 => Algorithm::RS512,
        K::ES256 => Algorithm::ES256,
        K::ES384 => Algorithm::ES384,
        K::PS256 => Algorithm::PS256,
        K::PS384 => Algorithm::PS384,
        K::PS512 => Algorithm::PS512,
        K::EdDSA => Algorithm::EdDSA,
        _ => return None, // HS* and key-management algs (RSA-OAEP, …) excluded
    })
}

/// JWK has no `alg` field? Most IdPs set it; fall back to a reasonable
/// default per key type so a missing `alg` doesn't silently drop the key.
fn infer_alg_from_params(p: &AlgorithmParameters) -> Option<Algorithm> {
    match p {
        AlgorithmParameters::RSA(_) => Some(Algorithm::RS256),
        AlgorithmParameters::EllipticCurve(_) => Some(Algorithm::ES256),
        AlgorithmParameters::OctetKeyPair(_) => Some(Algorithm::EdDSA),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde_json::json;

    const SECRET: &str = "validator-test-hs512-secret";

    fn sign_hs512(payload: serde_json::Value, secret: &str) -> String {
        encode(
            &Header::new(Algorithm::HS512),
            &payload,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .expect("sign")
    }

    fn now() -> i64 {
        chrono::Utc::now().timestamp()
    }

    #[tokio::test]
    async fn hs512_accepts_correctly_signed_token() {
        let v = Hs512Validator::new(SECRET, None, None);
        let jwt = sign_hs512(json!({ "sub": "u", "exp": now() + 60 }), SECRET);
        let claims = v.validate(&jwt).await.expect("valid");
        assert_eq!(claims["sub"], "u");
    }

    #[tokio::test]
    async fn hs512_rejects_bad_signature() {
        let v = Hs512Validator::new(SECRET, None, None);
        let jwt = sign_hs512(json!({ "sub": "u", "exp": now() + 60 }), "different-secret");
        assert!(matches!(
            v.validate(&jwt).await,
            Err(ValidationError::Rejected(_))
        ));
    }

    #[tokio::test]
    async fn hs512_rejects_expired_token() {
        let v = Hs512Validator::new(SECRET, None, None);
        // 5 minutes ago — well past the default 60s leeway.
        let jwt = sign_hs512(json!({ "sub": "u", "exp": now() - 300 }), SECRET);
        assert!(matches!(
            v.validate(&jwt).await,
            Err(ValidationError::Rejected(_))
        ));
    }

    #[tokio::test]
    async fn hs512_rejects_issuer_mismatch_when_configured() {
        let v = Hs512Validator::new(SECRET, Some("https://idp.expected/".into()), None);
        let jwt = sign_hs512(
            json!({ "sub": "u", "iss": "https://idp.other/", "exp": now() + 60 }),
            SECRET,
        );
        assert!(matches!(
            v.validate(&jwt).await,
            Err(ValidationError::Rejected(_))
        ));
    }

    #[tokio::test]
    async fn hs512_accepts_when_no_issuer_configured() {
        let v = Hs512Validator::new(SECRET, None, None);
        let jwt = sign_hs512(
            json!({ "sub": "u", "iss": "https://anything/", "exp": now() + 60 }),
            SECRET,
        );
        v.validate(&jwt).await.expect("iss not validated");
    }

    #[tokio::test]
    async fn hs512_tolerates_missing_aud_when_unconfigured() {
        // Default Validation has validate_aud=true; apply_iss_aud must
        // disable it when no expected audience is configured, otherwise
        // every test JWT (none of which carry aud) would fail.
        let v = Hs512Validator::new(SECRET, None, None);
        let jwt = sign_hs512(json!({ "sub": "u", "exp": now() + 60 }), SECRET);
        v.validate(&jwt).await.expect("aud not validated");
    }
}
