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
    /// Shared HS512 secret. The dev injector signs with this and
    /// SurrealDB's `app_session` access method validates against it
    /// (registered at startup via `SystemDb::define_jwt_access`).
    /// Loaded from `DELPHI_DB_JWT_SECRET` so dev and storage stay in
    /// sync on a single knob.
    pub jwt_secret: String,
    /// `ns` / `db` claims SurrealDB requires for routing. Default to
    /// the same values `SystemDb` uses so they line up automatically.
    pub surreal_ns: String,
    pub surreal_db: String,
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub mode: AuthMode,
}

impl AuthConfig {
    pub fn from_env() -> Result<Self> {
        let mode_raw = std::env::var("DELPHI_AUTH_MODE").unwrap_or_else(|_| "header".into());
        let mode = match mode_raw.as_str() {
            "header" => AuthMode::Header(load_header()),
            "dev" => load_dev_or_bail()?,
            other => bail!("invalid DELPHI_AUTH_MODE: {other:?}; expected 'header' or 'dev'"),
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
        default_tenant_slug: std::env::var("DELPHI_AUTH_DEFAULT_TENANT")
            .unwrap_or_else(|_| "default".into()),
    }
}

#[cfg(feature = "dev-auth")]
fn load_dev_or_bail() -> Result<AuthMode> {
    let tenant_slug = std::env::var("DELPHI_AUTH_DEV_TENANT").unwrap_or_else(|_| "dev".into());
    let user_email = std::env::var("DELPHI_AUTH_DEV_USER_EMAIL").unwrap_or_else(|_| "dev@delphi.local".into());
    let user_name = std::env::var("DELPHI_AUTH_DEV_USER_NAME").unwrap_or_else(|_| "Dev User".into());
    // Same env var the storage layer reads — keeps the dev injector and
    // SurrealDB's `app_session` access method on a single knob.
    let jwt_secret = std::env::var("DELPHI_DB_JWT_SECRET").map_err(|_| {
        anyhow::anyhow!(
            "DELPHI_AUTH_MODE=dev requires DELPHI_DB_JWT_SECRET (the dev injector signs \
             with this; SurrealDB's app_session access method validates with \
             the same secret)."
        )
    })?;
    let surreal_ns = std::env::var("DELPHI_DB_NAMESPACE").unwrap_or_else(|_| "delphi".into());
    let surreal_db = std::env::var("DELPHI_DB_NAME").unwrap_or_else(|_| "main".into());
    Ok(AuthMode::Dev(DevConfig {
        tenant_slug,
        user_email,
        user_name,
        jwt_secret,
        surreal_ns,
        surreal_db,
    }))
}

#[cfg(not(feature = "dev-auth"))]
fn load_dev_or_bail() -> Result<AuthMode> {
    bail!(
        "DELPHI_AUTH_MODE=dev requires the 'dev-auth' cargo feature. \
         Rebuild with `--features dev-auth` (development only) or set DELPHI_AUTH_MODE=header."
    )
}
