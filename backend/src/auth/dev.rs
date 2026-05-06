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

#[cfg(test)]
mod tests {
    //! Equivalence test: the headers the dev injector writes parse
    //! identically through `HeaderClaimsExtractor`. This is the formal
    //! guarantee that "dev mode is a strict subset of production": the
    //! same extractor, the same parsing, the same `Claims` shape — only
    //! the source of headers differs.

    use super::*;
    use crate::auth::claims::ClaimsExtractor;
    use crate::auth::headers::HeaderClaimsExtractor;

    fn dev_cfg() -> DevConfig {
        DevConfig {
            tenant_slug: "dev".into(),
            user_email: "dev@delphi.local".into(),
            user_name: "Dev User".into(),
        }
    }

    /// Apply the same writes the dev middleware performs, against a fresh
    /// HeaderMap. The middleware itself wraps a Request; we mirror its body.
    fn inject_dev_headers(cfg: &DevConfig) -> HeaderMap {
        let mut h = HeaderMap::new();
        set(&mut h, "x-auth-user-id", "dev-user");
        set(&mut h, "x-auth-issuer", "dev://local");
        set(&mut h, "x-auth-email", &cfg.user_email);
        set(&mut h, "x-auth-name", &cfg.user_name);
        set(&mut h, "x-auth-tenant-id", &cfg.tenant_slug);
        set(&mut h, "x-auth-roles", "owner");
        h
    }

    #[tokio::test]
    async fn dev_inject_round_trips_through_header_extractor() {
        let cfg = dev_cfg();
        let headers = inject_dev_headers(&cfg);

        let extractor = HeaderClaimsExtractor::new();
        let claims = extractor
            .extract(&headers)
            .await
            .expect("dev-injected headers must parse cleanly");

        assert_eq!(claims.iss, "dev://local");
        assert_eq!(claims.sub, "dev-user");
        assert_eq!(claims.email, cfg.user_email);
        assert_eq!(claims.display_name.as_deref(), Some(cfg.user_name.as_str()));
        assert_eq!(claims.tenant_slug.as_deref(), Some(cfg.tenant_slug.as_str()));
        assert_eq!(claims.roles, vec!["owner".to_string()]);
    }

    #[tokio::test]
    async fn dev_inject_strips_inbound_attempts() {
        // Adversary attempts to set their own identity by passing X-Auth-*
        // headers to the dev backend. The injector must overwrite (or
        // remove + re-set) them with the dev identity.
        let cfg = dev_cfg();

        // Simulate: build a headers-only "request" with attacker headers,
        // then run the same strip+set sequence the middleware does.
        let mut h = HeaderMap::new();
        h.insert(
            HeaderName::from_static("x-auth-user-id"),
            HeaderValue::from_static("attacker-sub"),
        );
        h.insert(
            HeaderName::from_static("x-auth-issuer"),
            HeaderValue::from_static("https://evil.example/"),
        );
        h.insert(
            HeaderName::from_static("x-auth-roles"),
            HeaderValue::from_static("admin,owner,superuser"),
        );

        strip_dev_headers(&mut h);
        set(&mut h, "x-auth-user-id", "dev-user");
        set(&mut h, "x-auth-issuer", "dev://local");
        set(&mut h, "x-auth-email", &cfg.user_email);
        set(&mut h, "x-auth-name", &cfg.user_name);
        set(&mut h, "x-auth-tenant-id", &cfg.tenant_slug);
        set(&mut h, "x-auth-roles", "owner");

        let extractor = HeaderClaimsExtractor::new();
        let claims = extractor.extract(&h).await.unwrap();

        assert_eq!(claims.iss, "dev://local", "attacker iss must be overwritten");
        assert_eq!(claims.sub, "dev-user", "attacker sub must be overwritten");
        assert_eq!(claims.roles, vec!["owner".to_string()],
                   "attacker roles must be overwritten");
    }
}
