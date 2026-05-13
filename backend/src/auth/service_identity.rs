//! Service identities — JWTs minted *for* the backend's own outbound
//! callers (today: the in-process arxiv adapter calling its own
//! `/api/ingestion/documents` endpoint over loopback).
//!
//! There is one trust boundary in this codebase — the
//! [`super::JwtClaimsExtractor`] / [`crate::storage::SystemDb`] pair.
//! User requests carry an IdP JWT through it; in-process adapters now
//! carry their own JWT through the same boundary. The only thing that
//! varies is the JWT *source*:
//!
//! - tier-1 / tests: [`Hs512ServiceIdentity`] mints HS512 locally with
//!   the same `SURREAL_JWT_SECRET` the engine validates against.
//! - tier-2 / prod: [`OAuthClientCredsIdentity`] does an OAuth2
//!   `client_credentials` exchange against the IdP (Keycloak), caches
//!   the access token, and refreshes before expiry.
//!
//! The factory [`service_identity_from_env`] picks one based on
//! `AUTH_MODE`, mirroring [`super::AuthConfig::from_env`].

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[async_trait]
pub trait ServiceIdentity: Send + Sync {
    /// Returns a usable bearer JWT. Implementations cache and refresh
    /// before expiry; callers may invoke this on every request.
    async fn fresh_token(&self) -> Result<String>;
}

// ─── HS512 (tier-1 / tests) ───────────────────────────────────────────────

/// Mints HS512 service JWTs locally. Mirrors the claim shape
/// [`super::dev::dev_inject_middleware`] produces, swapping the user
/// `sub` for `service:{name}` and the role for `ingester`.
pub struct Hs512ServiceIdentity {
    name: String,
    secret: String,
    tenant: String,
    ns: String,
    db: String,
    cache: Mutex<Option<CachedToken>>,
}

struct CachedToken {
    jwt: String,
    /// `Instant` rather than a unix timestamp so refresh comparisons
    /// can use monotonic time without re-parsing the JWT.
    refresh_after: Instant,
}

const HS512_SESSION_SECS: i64 = 3600;
const HS512_REFRESH_HEAD_SECS: u64 = 60;
const OAUTH_REFRESH_HEAD_SECS: u64 = 30;

#[derive(Serialize)]
struct ServiceClaims<'a> {
    sub: String,
    iss: &'a str,
    email: String,
    tenant_id: &'a str,
    roles: [&'a str; 1],
    ac: &'static str,
    ns: &'a str,
    db: &'a str,
    iat: i64,
    exp: i64,
}

impl Hs512ServiceIdentity {
    pub fn new(
        name: impl Into<String>,
        secret: impl Into<String>,
        tenant: impl Into<String>,
        ns: impl Into<String>,
        db: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            secret: secret.into(),
            tenant: tenant.into(),
            ns: ns.into(),
            db: db.into(),
            cache: Mutex::new(None),
        }
    }

    fn mint(&self) -> String {
        let now = Utc::now().timestamp();
        let claims = ServiceClaims {
            sub: format!("service:{}", self.name),
            iss: "dev://local",
            email: format!("{}-adapter@delphi.local", self.name),
            tenant_id: &self.tenant,
            roles: ["ingester"],
            ac: "app_session",
            ns: &self.ns,
            db: &self.db,
            iat: now,
            exp: now + HS512_SESSION_SECS,
        };
        encode(
            &Header::new(Algorithm::HS512),
            &claims,
            &EncodingKey::from_secret(self.secret.as_bytes()),
        )
        .expect("HS512 service JWT encode: claim shape is fixed and known-good")
    }
}

#[async_trait]
impl ServiceIdentity for Hs512ServiceIdentity {
    async fn fresh_token(&self) -> Result<String> {
        let mut slot = self.cache.lock().await;
        if let Some(c) = slot.as_ref() {
            if Instant::now() < c.refresh_after {
                return Ok(c.jwt.clone());
            }
        }
        let jwt = self.mint();
        // Refresh `HS512_REFRESH_HEAD_SECS` before expiry so callers
        // never observe a token that's about to die mid-request.
        let refresh_after = Instant::now()
            + Duration::from_secs(HS512_SESSION_SECS as u64 - HS512_REFRESH_HEAD_SECS);
        *slot = Some(CachedToken {
            jwt: jwt.clone(),
            refresh_after,
        });
        Ok(jwt)
    }
}

// ─── OAuth2 client_credentials (tier-2 / prod) ────────────────────────────

pub struct OAuthClientCredsIdentity {
    token_url: String,
    client_id: String,
    client_secret: String,
    http: reqwest::Client,
    cache: Mutex<Option<CachedToken>>,
}

impl OAuthClientCredsIdentity {
    pub fn new(
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .context("building OAuth2 http client")?;
        Ok(Self {
            token_url: token_url.into(),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            http,
            cache: Mutex::new(None),
        })
    }

    async fn fetch(&self) -> Result<(String, Duration)> {
        #[derive(Deserialize)]
        struct TokenResp {
            access_token: String,
            #[serde(default)]
            expires_in: Option<u64>,
        }
        let resp = self
            .http
            .post(&self.token_url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
            ])
            .send()
            .await
            .with_context(|| format!("POST {}", self.token_url))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("token endpoint returned {status}: {body}"));
        }
        let parsed: TokenResp = resp.json().await.context("decode token response")?;
        // Default to one hour if the IdP omits expires_in (Keycloak
        // always emits it, but other IdPs vary).
        let ttl = Duration::from_secs(parsed.expires_in.unwrap_or(3600));
        Ok((parsed.access_token, ttl))
    }
}

#[async_trait]
impl ServiceIdentity for OAuthClientCredsIdentity {
    async fn fresh_token(&self) -> Result<String> {
        let mut slot = self.cache.lock().await;
        if let Some(c) = slot.as_ref() {
            if Instant::now() < c.refresh_after {
                return Ok(c.jwt.clone());
            }
        }
        let (jwt, ttl) = self.fetch().await?;
        // Refresh `OAUTH_REFRESH_HEAD_SECS` before expiry. Saturating
        // sub guards against an absurdly short TTL (we'd just refetch
        // every call rather than panic).
        let head = Duration::from_secs(OAUTH_REFRESH_HEAD_SECS);
        let lifetime = ttl.checked_sub(head).unwrap_or(Duration::ZERO);
        *slot = Some(CachedToken {
            jwt: jwt.clone(),
            refresh_after: Instant::now() + lifetime,
        });
        Ok(jwt)
    }
}

// ─── factory ──────────────────────────────────────────────────────────────

/// Build the service identity matching the current `AUTH_MODE`. The
/// factory takes a `name` so different adapters can mint identities
/// with distinct `sub` values (and, in tier-2, distinct OAuth clients
/// — set via `${NAME}_OAUTH_*` env vars on the prefix-uppercased name).
///
/// Today the only caller is `arxiv`, which reads `ARXIV_OAUTH_*`.
pub fn service_identity_from_env(name: &str) -> Result<Arc<dyn ServiceIdentity>> {
    let mode = std::env::var("AUTH_MODE").unwrap_or_else(|_| "header".into());
    match mode.as_str() {
        "dev" => {
            let secret = std::env::var("SURREAL_JWT_SECRET").context(
                "AUTH_MODE=dev: SURREAL_JWT_SECRET required for service identity \
                 (mints HS512 with the same key SurrealDB validates against)",
            )?;
            let tenant = std::env::var("SOURCES_DEFAULT_TENANT_SLUG")
                .unwrap_or_else(|_| "default".into());
            let ns = std::env::var("SURREAL_NS").unwrap_or_else(|_| "delphi".into());
            let db = std::env::var("SURREAL_DB").unwrap_or_else(|_| "main".into());
            Ok(Arc::new(Hs512ServiceIdentity::new(name, secret, tenant, ns, db)))
        }
        "header" => {
            let prefix = name.to_uppercase();
            let token_url = required_env(&format!("{prefix}_OAUTH_TOKEN_URL"))?;
            let client_id = required_env(&format!("{prefix}_OAUTH_CLIENT_ID"))?;
            let client_secret = required_env(&format!("{prefix}_OAUTH_CLIENT_SECRET"))?;
            Ok(Arc::new(OAuthClientCredsIdentity::new(
                token_url,
                client_id,
                client_secret,
            )?))
        }
        other => Err(anyhow!("invalid AUTH_MODE for service identity: {other:?}")),
    }
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name).map_err(|_| {
        anyhow!(
            "AUTH_MODE=header: {name} required for service identity \
             (OAuth2 client_credentials against the IdP)"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::validator::{Hs512Validator, JwtValidator};
    use std::net::SocketAddr;

    const TEST_SECRET: &str = "service-identity-test-hs512-secret";

    #[tokio::test]
    async fn hs512_token_round_trips_through_validator() {
        let id = Hs512ServiceIdentity::new(
            "arxiv",
            TEST_SECRET,
            "tenant-a",
            "delphi",
            "main",
        );
        let jwt = id.fresh_token().await.unwrap();
        let validator = Hs512Validator::new(TEST_SECRET, None, None);
        let claims = validator.validate(&jwt).await.expect("validate");
        assert_eq!(claims["sub"].as_str(), Some("service:arxiv"));
        assert_eq!(claims["iss"].as_str(), Some("dev://local"));
        assert_eq!(claims["tenant_id"].as_str(), Some("tenant-a"));
        assert_eq!(claims["ac"].as_str(), Some("app_session"));
        assert_eq!(claims["ns"].as_str(), Some("delphi"));
        assert_eq!(claims["db"].as_str(), Some("main"));
        let roles = claims["roles"].as_array().expect("roles array");
        assert_eq!(roles.len(), 1);
        assert_eq!(roles[0].as_str(), Some("ingester"));
        assert_eq!(
            claims["email"].as_str(),
            Some("arxiv-adapter@delphi.local")
        );
    }

    #[tokio::test]
    async fn hs512_caches_token_within_window() {
        let id = Hs512ServiceIdentity::new("arxiv", TEST_SECRET, "t", "delphi", "main");
        let a = id.fresh_token().await.unwrap();
        let b = id.fresh_token().await.unwrap();
        assert_eq!(a, b, "cached token should be reused within refresh window");
    }

    /// Spin up a tiny axum router that serves the OAuth2 token endpoint
    /// once; assert the identity calls it once across multiple
    /// `fresh_token` invocations (i.e. the cache works).
    #[tokio::test]
    async fn oauth_caches_token_across_calls() {
        use axum::{routing::post, Json, Router};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc as StdArc;

        let calls = StdArc::new(AtomicUsize::new(0));
        let calls_for_handler = calls.clone();

        let app = Router::new().route(
            "/token",
            post(move || {
                let calls = calls_for_handler.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Json(serde_json::json!({
                        "access_token": "fake-jwt-blob",
                        "token_type": "Bearer",
                        "expires_in": 600
                    }))
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let token_url = format!("http://{addr}/token");
        let id = OAuthClientCredsIdentity::new(token_url, "client", "secret").unwrap();

        let a = id.fresh_token().await.expect("first token");
        let b = id.fresh_token().await.expect("second token");
        let c = id.fresh_token().await.expect("third token");

        assert_eq!(a, "fake-jwt-blob");
        assert_eq!(a, b);
        assert_eq!(a, c);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "token endpoint should be hit once; subsequent calls served from cache"
        );
    }
}
