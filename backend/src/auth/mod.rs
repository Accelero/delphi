//! Auth foundation: header-based identity at the request boundary.
//!
//! The browser-facing BFF (Traefik + oauth2-proxy) terminates the OIDC flow
//! and projects verified JWT claims into a fixed set of `X-Auth-*` headers.
//! The backend trusts those headers — it does not see tokens, sessions, or
//! cookies.
//!
//! The layer between "headers on the wire" and "typed [`AuthContext`] in
//! handlers" goes through one trait, [`ClaimsExtractor`], so the trust
//! boundary is a single, swappable abstraction. Today the only impl is
//! [`HeaderClaimsExtractor`]; if we later want defence-in-depth (validate a
//! JWT inside the backend instead of trusting headers), we add a second
//! impl and swap it in via config — no caller code changes.
//!
//! Internals are private. The public surface is the items re-exported below.

mod bootstrap;
mod claims;
mod config;
mod context;
mod guard;
mod headers;
mod jwt;
mod middleware;
mod routes;

#[cfg(feature = "dev-auth")]
mod dev;

// ─── public interface ──────────────────────────────────────────────────────

pub use claims::{Claims, ClaimsError, ClaimsExtractor};
pub use config::{AuthConfig, AuthMode, HeaderConfig};
pub use context::AuthContext;
pub use guard::{enforce_production_guard, print_banner};
pub use headers::HeaderClaimsExtractor;
pub use jwt::SessionTokenSigner;
pub use middleware::{identity_middleware, IdentityDeps};

pub use bootstrap::{ensure_user, resolve_default_tenant};

pub use routes::me;

#[cfg(feature = "dev-auth")]
pub use bootstrap::seed_dev_world;
#[cfg(feature = "dev-auth")]
pub use config::DevConfig;
#[cfg(feature = "dev-auth")]
pub use dev::dev_inject_middleware;
