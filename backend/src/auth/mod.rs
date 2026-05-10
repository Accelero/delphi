//! Auth foundation: JWT-based identity at the request boundary.
//!
//! The browser-facing BFF (Traefik + oauth2-proxy) terminates the OIDC
//! flow with the IdP (Keycloak) and forwards the IdP-issued access
//! token to the backend as `Authorization: Bearer <jwt>`. The backend
//! decodes the payload to lift claims into a typed [`AuthContext`].
//!
//! The trust boundary is the `ClaimsExtractor` trait — currently
//! satisfied by [`JwtClaimsExtractor`]. The backend does not validate
//! the JWT signature (the BFF already did that against Keycloak's
//! JWKS). Defence-in-depth (backend re-validates) is a small drop-in:
//! same trait, different impl.
//!
//! Outbound: per-request the backend mints a SurrealDB-scoped JWT
//! from the [`AuthContext`] via [`SessionTokenSigner`] and hands it
//! to `db.authenticate(jwt)`. SurrealDB validates the signature
//! against the matching `DEFINE ACCESS … TYPE RECORD WITH JWT` and
//! enforces `PERMISSIONS` per query.
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

#[cfg(feature = "dev-auth")]
mod dev;

// ─── public interface ──────────────────────────────────────────────────────

pub use claims::{Claims, ClaimsError, ClaimsExtractor};
pub use config::{AuthConfig, AuthMode, HeaderConfig};
pub use context::AuthContext;
pub use guard::{enforce_production_guard, print_banner};
pub use jwt::{JwtClaimsExtractor, SessionTokenSigner};
pub use middleware::{identity_middleware, IdentityDeps};

pub use bootstrap::{ensure_user, resolve_default_tenant};

pub use routes::me;

#[cfg(feature = "dev-auth")]
pub use bootstrap::seed_dev_world;
#[cfg(feature = "dev-auth")]
pub use config::DevConfig;
#[cfg(feature = "dev-auth")]
pub use dev::dev_inject_middleware;
