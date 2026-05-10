//! Dev-mode header injector (gated by the `dev-auth` cargo feature).
//!
//! In production an upstream BFF (Traefik + oauth2-proxy) writes `X-Auth-*`
//! headers onto every authenticated request. In dev we don't run the BFF —
//! instead this middleware overwrites those same headers with a fixed
//! identity *before* [`super::identity_middleware`] runs.
//!
//! That makes the dev path a strict subset of the production path: the
//! exact same [`super::HeaderClaimsExtractor`] parses the headers, the
//! exact same [`super::ensure_user`] upsert runs, the exact same
//! [`super::AuthContext`] reaches handlers. The only thing that changes
//! is the source of the headers.
//!
//! Defence-in-depth: this also strips any inbound `X-Auth-*` from the
//! client. In dev nothing should ever set them externally — but if
//! something did (a misconfigured local proxy, a curl test gone wrong),
//! we don't want to honour it. Strip-then-set ensures the dev identity is
//! always exactly what `DevConfig` says.

use axum::extract::Request;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;
use axum::Extension;

use super::config::DevConfig;

const DEV_HEADERS: &[&str] = &[
    "x-auth-user-id",
    "x-auth-issuer",
    "x-auth-email",
    "x-auth-name",
    "x-auth-tenant-id",
    "x-auth-roles",
];

pub async fn dev_inject_middleware(
    Extension(cfg): Extension<DevConfig>,
    mut req: Request,
    next: Next,
) -> Response {
    let h = req.headers_mut();
    strip_dev_headers(h);
    set(h, "x-auth-user-id", "dev-user");
    set(h, "x-auth-issuer", "dev://local");
    set(h, "x-auth-email", &cfg.user_email);
    set(h, "x-auth-name", &cfg.user_name);
    set(h, "x-auth-tenant-id", &cfg.tenant_slug);
    set(h, "x-auth-roles", "owner");
    next.run(req).await
}

fn strip_dev_headers(h: &mut HeaderMap) {
    for name in DEV_HEADERS {
        h.remove(*name);
    }
}

fn set(h: &mut HeaderMap, name: &'static str, value: &str) {
    // All values we set here are constants or env-derived strings that have
    // already round-tripped through `String` — they cannot contain CR/LF.
    // The `expect` here would only fire on a programmer error.
    let v = HeaderValue::from_str(value)
        .unwrap_or_else(|_| panic!("dev injector: invalid header value for {name}: {value:?}"));
    h.insert(HeaderName::from_static(name), v);
}

// The previous test module (equivalence with HeaderClaimsExtractor) was
// dropped along with X-Auth-* headers in the JWT cutover. The dev
// injector itself is on the N1 punch list — it'll be rebuilt to mint a
// dev JWT and a fresh test module will be added then.
