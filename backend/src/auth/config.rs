//! Auth configuration loaded from environment.
//!
//! Picks one of two modes at startup:
//!
//! - [`AuthMode::Oidc`]: production path. Verifies bearer tokens and runs the
//!   OIDC redirect flow against any standard provider (WorkOS, Zitadel, Auth0,
//!   Keycloak, …).
//! - [`AuthMode::Dev`]: local dev convenience. Auto-injects a fixed identity
//!   so the rest of the codebase can rely on `AuthContext` without standing up
//!   an IdP. **Compiled in only when the `dev-auth` cargo feature is on** —
//!   release binaries built with default features don't contain this variant.

use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use tower_sessions::cookie::Key;

#[derive(Debug, Clone)]
pub enum AuthMode {
    Oidc(OidcConfig),
    #[cfg(feature = "dev-auth")]
    Dev(DevConfig),
}

impl AuthMode {
    pub fn label(&self) -> &'static str {
        match self {
            AuthMode::Oidc(_) => "oidc",
            #[cfg(feature = "dev-auth")]
            AuthMode::Dev(_) => "dev",
        }
    }
}

#[derive(Debug, Clone)]
pub struct OidcConfig {
    /// `OIDC_ISSUER` — e.g. `https://your-instance.zitadel.cloud`.
    pub issuer: String,
    pub client_id: String,
    pub client_secret: Option<String>,
    /// `OIDC_APPLICATION_BASE_URL` — used by axum-oidc to construct redirect
    /// URIs dynamically. Register `<base>/*` (or specific subpaths) with the
    /// IdP. e.g. `http://localhost:8081`.
    pub application_base_url: String,
    pub scopes: Vec<String>,
    /// JWT claim name carrying the tenant identifier (e.g. `org_id`).
    pub tenant_claim: String,
    /// Slug of the fallback tenant when no tenant claim is present.
    pub default_tenant_slug: String,
    /// Where to send the browser after a successful login.
    pub post_login_redirect: String,
}

#[cfg(feature = "dev-auth")]
#[derive(Debug, Clone)]
pub struct DevConfig {
    pub tenant_slug: String,
    pub user_email: String,
    pub user_name: String,
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub mode: AuthMode,
    pub session_key: Key,
    pub secure_cookies: bool,
}

impl AuthConfig {
    pub fn from_env() -> Result<Self> {
        let mode_raw = std::env::var("AUTH_MODE").unwrap_or_else(|_| "oidc".into());
        let mode = match mode_raw.as_str() {
            "oidc" => AuthMode::Oidc(load_oidc()?),
            "dev" => load_dev_or_bail()?,
            other => bail!("invalid AUTH_MODE: {other:?}; expected 'oidc' or 'dev'"),
        };

        let session_key = load_session_key(&mode)?;
        let secure_cookies = std::env::var("SECURE_COOKIES")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or_else(|| {
                std::env::var("RUST_ENV").as_deref() == Ok("production")
            });

        Ok(Self {
            mode,
            session_key,
            secure_cookies,
        })
    }
}

fn load_oidc() -> Result<OidcConfig> {
    let issuer = require_env("OIDC_ISSUER")?;
    let client_id = require_env("OIDC_CLIENT_ID")?;
    let client_secret = std::env::var("OIDC_CLIENT_SECRET").ok().filter(|s| !s.is_empty());
    let application_base_url = std::env::var("OIDC_APPLICATION_BASE_URL")
        .unwrap_or_else(|_| "http://localhost:8081".into());
    let scopes = std::env::var("OIDC_SCOPES")
        .unwrap_or_else(|_| "openid profile email".into())
        .split_whitespace()
        .map(|s| s.to_string())
        .collect();
    let tenant_claim = std::env::var("OIDC_TENANT_CLAIM").unwrap_or_else(|_| "org_id".into());
    let default_tenant_slug =
        std::env::var("OIDC_DEFAULT_TENANT_SLUG").unwrap_or_else(|_| "default".into());
    let post_login_redirect =
        std::env::var("OIDC_POST_LOGIN_REDIRECT").unwrap_or_else(|_| "/".into());

    Ok(OidcConfig {
        issuer,
        client_id,
        client_secret,
        application_base_url,
        scopes,
        tenant_claim,
        default_tenant_slug,
        post_login_redirect,
    })
}

#[cfg(feature = "dev-auth")]
fn load_dev_or_bail() -> Result<AuthMode> {
    let tenant_slug = std::env::var("DEV_TENANT_SLUG").unwrap_or_else(|_| "dev".into());
    let user_email = std::env::var("DEV_USER_EMAIL").unwrap_or_else(|_| "dev@delphi.local".into());
    let user_name = std::env::var("DEV_USER_NAME").unwrap_or_else(|_| "Dev User".into());
    Ok(AuthMode::Dev(DevConfig {
        tenant_slug,
        user_email,
        user_name,
    }))
}

#[cfg(not(feature = "dev-auth"))]
fn load_dev_or_bail() -> Result<AuthMode> {
    bail!(
        "AUTH_MODE=dev requires the 'dev-auth' cargo feature. \
         Rebuild with `--features dev-auth` (development only) or set AUTH_MODE=oidc."
    )
}

/// Decode `SESSION_KEY` from the env: base64-encoded, ≥64 bytes.
///
/// In dev mode, we fall back to a freshly-generated random key (with a warning)
/// so iteration doesn't require dotting in a key. In OIDC mode, missing key is
/// a hard error — sessions would silently drop on every restart.
fn load_session_key(mode: &AuthMode) -> Result<Key> {
    if let Ok(raw) = std::env::var("SESSION_KEY") {
        if raw.is_empty() {
            return key_or_err(mode, "SESSION_KEY is empty");
        }
        let bytes = B64
            .decode(raw.trim())
            .context("SESSION_KEY must be base64-encoded")?;
        if bytes.len() < 64 {
            bail!(
                "SESSION_KEY must decode to at least 64 bytes; got {}",
                bytes.len()
            );
        }
        return Key::try_from(bytes.as_slice())
            .map_err(|e| anyhow!("SESSION_KEY rejected: {e}"));
    }
    key_or_err(mode, "SESSION_KEY is not set")
}

fn key_or_err(mode: &AuthMode, why: &str) -> Result<Key> {
    match mode {
        AuthMode::Oidc(_) => bail!(
            "{why}. Generate one with: openssl rand -base64 64"
        ),
        #[cfg(feature = "dev-auth")]
        AuthMode::Dev(_) => {
            tracing::warn!(
                "{why}; using a per-process random session key (sessions reset on restart). \
                 OK for dev — never in production."
            );
            Ok(Key::generate())
        }
    }
}

fn require_env(key: &str) -> Result<String> {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("required environment variable {key} is not set"))
}
