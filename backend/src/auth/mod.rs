//! Auth foundation: BFF pattern (backend owns OIDC tokens; browser only sees
//! an HTTP-only signed session cookie). Multi-tenant from day one — identity
//! is `(iss, sub)`, tenant resolved from a configurable JWT claim.

pub mod bootstrap;
pub mod config;
pub mod context;
pub mod guard;
pub mod oidc;
pub mod routes;
pub mod store;

#[cfg(feature = "dev-auth")]
pub mod dev;

pub use context::AuthContext;
