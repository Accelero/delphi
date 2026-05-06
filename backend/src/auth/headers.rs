//! [`ClaimsExtractor`] implementation that trusts proxy-injected headers.
//!
//! This is the production-mode source of identity. The BFF (Traefik +
//! oauth2-proxy in front of the backend) terminates the OIDC flow, owns the
//! session cookie, and projects the verified JWT claims into a small set of
//! `X-Auth-*` headers — which we read here.
//!
//! **Trust model.** This extractor performs *no* cryptographic validation.
//! The backend trusts the proxy. That is only safe when the backend is not
//! reachable from the public internet — only via the proxy. Operationally
//! that means: bind to a private network, never expose the backend port.
//!
//! When/if we want defence-in-depth (the backend re-validates a JWT instead
//! of trusting headers), we add a second `ClaimsExtractor` impl alongside
//! this one and swap it in via config. No caller code changes.

use async_trait::async_trait;
use axum::http::{HeaderMap, HeaderName};

use super::claims::{Claims, ClaimsError, ClaimsExtractor};

// ─── header names ──────────────────────────────────────────────────────────
//
// `X-Auth-*` is the contract documented in `docs/ARCH.md`. The proxy emits
// these regardless of which IdP is upstream, so the backend stays
// IdP-agnostic.

const H_USER_ID: HeaderName = HeaderName::from_static("x-auth-user-id");
const H_ISSUER: HeaderName = HeaderName::from_static("x-auth-issuer");
const H_EMAIL: HeaderName = HeaderName::from_static("x-auth-email");
const H_NAME: HeaderName = HeaderName::from_static("x-auth-name");
const H_TENANT: HeaderName = HeaderName::from_static("x-auth-tenant-id");
const H_ROLES: HeaderName = HeaderName::from_static("x-auth-roles");

#[derive(Default, Clone)]
pub struct HeaderClaimsExtractor;

impl HeaderClaimsExtractor {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ClaimsExtractor for HeaderClaimsExtractor {
    async fn extract(&self, headers: &HeaderMap) -> Result<Claims, ClaimsError> {
        // Required: stable identity. Without these we cannot construct a
        // user record — fail closed.
        let sub = required(headers, &H_USER_ID)?;
        let iss = required(headers, &H_ISSUER)?;
        let email = required(headers, &H_EMAIL)?;

        // Optional: descriptive / scoping fields.
        let display_name = optional(headers, &H_NAME);
        let tenant_slug = optional(headers, &H_TENANT);
        let roles = optional(headers, &H_ROLES)
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        Ok(Claims {
            iss,
            sub,
            email,
            display_name,
            tenant_slug,
            roles,
        })
    }
}

fn required(headers: &HeaderMap, name: &HeaderName) -> Result<String, ClaimsError> {
    let v = headers.get(name).ok_or(ClaimsError::Missing)?;
    let s = v
        .to_str()
        .map_err(|_| ClaimsError::Invalid(format!("{name} is not valid UTF-8")))?;
    if s.is_empty() {
        return Err(ClaimsError::Invalid(format!("{name} is empty")));
    }
    Ok(s.to_string())
}

fn optional(headers: &HeaderMap, name: &HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
}
