//! Verified identity claims and the trait that produces them.
//!
//! [`Claims`] is the **representation** of who is making the request — a flat,
//! provider-agnostic struct that downstream code can consume without knowing
//! whether the source was an upstream proxy, a JWT we validated ourselves, or
//! a dev-mode shim.
//!
//! [`ClaimsExtractor`] is the **boundary** at which trust is established. It
//! is the only place in the codebase allowed to decide that a request *has*
//! an identity. Today there is one production implementation,
//! [`super::HeaderClaimsExtractor`], which trusts proxy-injected `X-Auth-*`
//! headers (the BFF terminates the JWT and projects claims into headers).
//! Tomorrow we may add a second implementation that validates a `Bearer` JWT
//! end-to-end inside the backend — same trait, no caller changes.

use async_trait::async_trait;
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Verified identity claims extracted at the request boundary.
///
/// Field semantics intentionally mirror the OIDC vocabulary so that swapping
/// a header-based extractor for a JWT-validating one is a drop-in:
///
/// - `iss`, `sub` — together form the stable user identity (`app_user`'s
///   unique key in the DB). Survives email and display-name changes.
/// - `email` — informational; not part of identity.
/// - `display_name` — best-effort human label; may be absent.
/// - `tenant_slug` — tenant the request belongs to (resolved against the
///   `tenant.slug` index). Absent → fall back to the configured default.
/// - `roles` — application-level role list, used for in-tenant feature gating.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub iss: String,
    pub sub: String,
    pub email: String,
    pub display_name: Option<String>,
    pub tenant_slug: Option<String>,
    pub roles: Vec<String>,
}

/// Reasons a request can fail to produce identity at the trust boundary.
///
/// `Missing` and `Invalid` both translate to `401 Unauthorized` for the
/// caller; the distinction exists so the server can log differently
/// (a missing identity is "the proxy didn't run / the JWT wasn't sent",
/// an invalid one is "something tampered or expired").
#[derive(Debug, Error)]
pub enum ClaimsError {
    #[error("missing identity")]
    Missing,
    #[error("invalid identity: {0}")]
    Invalid(String),
}

/// Establishes a verified [`Claims`] from an incoming request.
///
/// Implementations are the **only** code in the system permitted to
/// decide that an identity is trustworthy. Everything downstream consumes
/// the [`Claims`] value and assumes it has already been verified.
#[async_trait]
pub trait ClaimsExtractor: Send + Sync + 'static {
    async fn extract(&self, headers: &HeaderMap) -> Result<Claims, ClaimsError>;
}
