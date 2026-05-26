//! Auth foundation: JWT-based identity at the request boundary.
//!
//! The browser-facing BFF (Traefik + oauth2-proxy) terminates the OIDC
//! flow with the IdP (Keycloak) and forwards the IdP-issued access
//! token to the backend as `Authorization: Bearer <jwt>`. The backend
//! decodes the payload to lift claims into a typed [`AuthContext`].
//!
//! The trust boundary is the `ClaimsExtractor` trait — satisfied by
//! [`JwtClaimsExtractor`], which delegates signature + standard-claim
//! validation to the injected [`JwtValidator`]. Two validator
//! implementations ship: HS512 (shared secret, used by tier-1 dev and
//! tests) and JWKS (used in tier-2 dev and production with a real
//! OIDC IdP).
//!
//! The same bearer token is then forwarded unchanged to SurrealDB
//! via `db.authenticate(jwt)`. SurrealDB validates the signature
//! against the matching `DEFINE ACCESS … TYPE RECORD WITH JWT` and
//! the AUTHENTICATE clause resolves it to an `app_user` record;
//! `PERMISSIONS` clauses then fire on every query. Backend and engine
//! validate against the same key material, configured from one knob
//! (`SURREAL_JWT_*`). The backend does not mint any JWT of its own.
//!
//! Internals are private. The public surface is the items re-exported below.

mod bootstrap;
mod claims;
mod config;
mod context;
mod guard;
mod jwt;
mod middleware;
mod routes;
mod service_identity;
mod validator;

#[cfg(feature = "dev-auth")]
mod dev;

// ─── public interface ──────────────────────────────────────────────────────

pub use claims::{Claims, ClaimsError, ClaimsExtractor};
pub use config::{AuthConfig, AuthMode, HeaderConfig};
pub use context::AuthContext;
pub use guard::{enforce_production_guard, print_banner};
pub use jwt::JwtClaimsExtractor;
pub use middleware::{identity_middleware, IdentityDeps};
pub use service_identity::{
    service_identity_from_env, Hs512ServiceIdentity, OAuthClientCredsIdentity, ServiceIdentity,
};
pub use validator::{validator_from_jwt_access, Hs512Validator, JwksValidator, JwtValidator};

pub use bootstrap::{ensure_user, resolve_default_tenant};

pub use routes::me;

#[cfg(feature = "dev-auth")]
pub use bootstrap::seed_dev_world;
#[cfg(feature = "dev-auth")]
pub use config::DevConfig;
#[cfg(feature = "dev-auth")]
pub use dev::dev_inject_middleware;
