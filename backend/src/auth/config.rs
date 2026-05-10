//! Auth configuration loaded from environment.
//!
//! Two modes:
//!
//! - [`AuthMode::Header`]: production path. Trust an upstream BFF (Traefik +
//!   oauth2-proxy) to project verified JWT claims into `X-Auth-*` headers
//!   on every request. The backend reads those headers via
//!   [`super::HeaderClaimsExtractor`] — no token validation here.
//! - [`AuthMode::Dev`]: local-dev convenience. A small middleware (gated by
//!   the `dev-auth` cargo feature) mints a dev JWT and writes it as
//!   `Authorization: Bearer <jwt>` — the same shape the production IdP
//!   would emit, so the downstream identity middleware runs unchanged.
//!   **Only compiled in when `dev-auth` is on** — release builds literally
//!   don't contain the bypass code.

use anyhow::{bail, Result};

#[derive(Debug, Clone)]
pub enum AuthMode {
    Header(HeaderConfig),
    #[cfg(feature = "dev-auth")]
    Dev(DevConfig),
}

impl AuthMode {
    pub fn label(&self) -> &'static str {
        match self {
            AuthMode::Header(_) => "header",
            #[cfg(feature = "dev-auth")]
            AuthMode::Dev(_) => "dev",
        }
    }
}

/// Settings the backend needs in either mode (the only real one is tenant
/// fallback — header *names* are fixed by the `X-Auth-*` contract).
#[derive(Debug, Clone)]
pub struct HeaderConfig {
    /// Slug of the tenant a request lands in when the proxy doesn't supply
    /// `X-Auth-Tenant-Id` (or supplies one we don't know about).
    pub default_tenant_slug: String,
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
}

impl AuthConfig {
    pub fn from_env() -> Result<Self> {
        let mode_raw = std::env::var("AUTH_MODE").unwrap_or_else(|_| "header".into());
        let mode = match mode_raw.as_str() {
            "header" => AuthMode::Header(load_header()),
            "dev" => load_dev_or_bail()?,
            other => bail!("invalid AUTH_MODE: {other:?}; expected 'header' or 'dev'"),
        };
        Ok(Self { mode })
    }

    /// Slug of the tenant requests fall back to when no tenant claim is
    /// present. Both modes need this — header mode for the fallback path,
    /// dev mode because the dev tenant slug *is* the default.
    pub fn default_tenant_slug(&self) -> &str {
        match &self.mode {
            AuthMode::Header(c) => &c.default_tenant_slug,
            #[cfg(feature = "dev-auth")]
            AuthMode::Dev(c) => &c.tenant_slug,
        }
    }
}

fn load_header() -> HeaderConfig {
    HeaderConfig {
        default_tenant_slug: std::env::var("DEFAULT_TENANT_SLUG")
            .unwrap_or_else(|_| "default".into()),
    }
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
         Rebuild with `--features dev-auth` (development only) or set AUTH_MODE=header."
    )
}
