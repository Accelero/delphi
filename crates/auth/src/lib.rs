use async_trait::async_trait;
use axum::extract::{FromRef, FromRequestParts};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use delphi_contracts::AuthUser;
use jsonwebtoken::jwk::{AlgorithmParameters, JwkSet, KeyAlgorithm};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::RwLock;

const H_AUTHORIZATION: HeaderName = HeaderName::from_static("authorization");

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("missing bearer token")]
    Missing,
    #[error("invalid bearer token: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub user_id: String,
    pub tenant_id: String,
    pub email: Option<String>,
    pub roles: Vec<String>,
    pub bearer_subject: String,
}

impl AuthContext {
    pub fn public_user(&self) -> AuthUser {
        AuthUser {
            user_id: self.user_id.clone(),
            tenant_id: self.tenant_id.clone(),
            email: self.email.clone(),
            roles: self.roles.clone(),
        }
    }
}

impl<S> FromRequestParts<S> for AuthContext
where
    S: Send + Sync,
    AuthVerifier: FromRef<S>,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let verifier = AuthVerifier::from_ref(state);
        verifier.verify_headers(&parts.headers).await
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        StatusCode::UNAUTHORIZED.into_response()
    }
}

#[derive(Clone)]
pub struct AuthVerifier {
    validator: Arc<dyn JwtValidator>,
    default_tenant: String,
}

impl AuthVerifier {
    pub fn from_env() -> Result<Self, AuthBuildError> {
        let jwks_url =
            std::env::var("AUTH_JWKS_URL").map_err(|_| AuthBuildError::MissingJwksUrl)?;
        let expected_issuer = std::env::var("AUTH_EXPECTED_ISSUER").ok();
        let expected_audience = std::env::var("AUTH_EXPECTED_AUDIENCE").ok();
        let default_tenant =
            std::env::var("AUTH_DEFAULT_TENANT").unwrap_or_else(|_| "tenant-a".to_owned());

        Ok(Self {
            validator: Arc::new(JwksValidator::new(
                jwks_url,
                expected_issuer,
                expected_audience,
            )),
            default_tenant,
        })
    }

    pub async fn verify_headers(&self, headers: &HeaderMap) -> Result<AuthContext, AuthError> {
        let bearer = bearer_token(headers).ok_or(AuthError::Missing)?;
        let payload = self
            .validator
            .validate(bearer)
            .await
            .map_err(|error| AuthError::Invalid(error.to_string()))?;
        let inbound = serde_json::from_value::<InboundClaims>(payload)
            .map_err(|error| AuthError::Invalid(format!("decode JWT payload: {error}")))?;

        let sub = inbound
            .sub
            .ok_or_else(|| AuthError::Invalid("JWT missing required sub claim".to_owned()))?;
        let email = inbound.email;
        let tenant_id = inbound
            .tenant_id
            .unwrap_or_else(|| self.default_tenant.clone());

        Ok(AuthContext {
            user_id: sub.clone(),
            tenant_id,
            email,
            roles: inbound.roles.unwrap_or_default(),
            bearer_subject: format!("Bearer {bearer}"),
        })
    }
}

#[derive(Debug, Error)]
pub enum AuthBuildError {
    #[error("AUTH_JWKS_URL is required")]
    MissingJwksUrl,
}

#[derive(Debug, Default, Deserialize)]
struct InboundClaims {
    sub: Option<String>,
    email: Option<String>,
    tenant_id: Option<String>,
    roles: Option<Vec<String>>,
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(&H_AUTHORIZATION)?.to_str().ok()?;
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
}

#[derive(Debug, Error)]
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

#[async_trait]
trait JwtValidator: Send + Sync {
    async fn validate(&self, jwt: &str) -> Result<Value, ValidationError>;
}

struct JwksValidator {
    url: String,
    http: reqwest::Client,
    cache: RwLock<HashMap<String, CachedKey>>,
    expected_issuer: Option<String>,
    expected_audience: Option<String>,
}

struct CachedKey {
    key: DecodingKey,
    alg: Algorithm,
}

impl JwksValidator {
    fn new(
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

    async fn refresh(&self) -> Result<(), ValidationError> {
        let response = self
            .http
            .get(&self.url)
            .send()
            .await
            .map_err(|error| ValidationError::JwksFetch(error.to_string()))?
            .error_for_status()
            .map_err(|error| ValidationError::JwksFetch(error.to_string()))?;
        let set = response
            .json::<JwkSet>()
            .await
            .map_err(|error| ValidationError::JwksFetch(format!("decode JWKS: {error}")))?;

        let mut next = HashMap::new();
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
                Ok(key) => key,
                Err(error) => {
                    tracing::warn!(kid, error = %error, "skipping unparseable JWK");
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
        decode::<Value>(jwt, &cached.key, &validation)
            .map(|token| Some(token.claims))
            .map_err(|error| ValidationError::Rejected(error.to_string()))
    }
}

#[async_trait]
impl JwtValidator for JwksValidator {
    async fn validate(&self, jwt: &str) -> Result<Value, ValidationError> {
        let header =
            decode_header(jwt).map_err(|error| ValidationError::Malformed(error.to_string()))?;
        let kid = header
            .kid
            .ok_or_else(|| ValidationError::Malformed("JWT header missing kid".to_owned()))?;

        if let Some(claims) = self.try_cached(jwt, &kid).await? {
            return Ok(claims);
        }
        self.refresh().await?;
        self.try_cached(jwt, &kid)
            .await?
            .ok_or(ValidationError::UnknownKid(kid))
    }
}

fn apply_iss_aud(
    validation: &mut Validation,
    expected_issuer: Option<&str>,
    expected_audience: Option<&str>,
) {
    if let Some(issuer) = expected_issuer {
        validation.set_issuer(&[issuer]);
    }
    match expected_audience {
        Some(audience) => validation.set_audience(&[audience]),
        None => validation.validate_aud = false,
    }
}

fn is_asymmetric(parameters: &AlgorithmParameters) -> bool {
    matches!(
        parameters,
        AlgorithmParameters::RSA(_)
            | AlgorithmParameters::EllipticCurve(_)
            | AlgorithmParameters::OctetKeyPair(_)
    )
}

fn asymmetric_alg_to_alg(algorithm: KeyAlgorithm) -> Option<Algorithm> {
    Some(match algorithm {
        KeyAlgorithm::RS256 => Algorithm::RS256,
        KeyAlgorithm::RS384 => Algorithm::RS384,
        KeyAlgorithm::RS512 => Algorithm::RS512,
        KeyAlgorithm::ES256 => Algorithm::ES256,
        KeyAlgorithm::ES384 => Algorithm::ES384,
        KeyAlgorithm::PS256 => Algorithm::PS256,
        KeyAlgorithm::PS384 => Algorithm::PS384,
        KeyAlgorithm::PS512 => Algorithm::PS512,
        KeyAlgorithm::EdDSA => Algorithm::EdDSA,
        _ => return None,
    })
}

fn infer_alg_from_params(parameters: &AlgorithmParameters) -> Option<Algorithm> {
    match parameters {
        AlgorithmParameters::RSA(_) => Some(Algorithm::RS256),
        AlgorithmParameters::EllipticCurve(_) => Some(Algorithm::ES256),
        AlgorithmParameters::OctetKeyPair(_) => Some(Algorithm::EdDSA),
        _ => None,
    }
}
