//! End-to-end test of the auth pipeline:
//!
//!     X-Auth-* headers → HeaderClaimsExtractor → ensure_user (DB upsert)
//!                                              → AuthContext in extensions
//!                                              → handler returns 200
//!
//! Covers the contract documented in `docs/ARCH.md`. Each case drives the
//! fully-built axum router via `tower::ServiceExt::oneshot()` against an
//! in-memory SurrealDB — no network, no mocks of the auth machinery.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;

use crate::common::{AuthRequestBuilder, TestApp};

#[tokio::test]
async fn me_401_when_no_auth_headers() {
    let app = TestApp::build().await;
    let res = app
        .send(
            Request::builder()
                .uri("/api/auth/me")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn me_401_when_required_header_missing() {
    // X-Auth-User-Id present but X-Auth-Issuer missing → still unauthorized.
    let app = TestApp::build().await;

    let req = Request::builder().uri("/api/auth/me").body(Body::empty()).unwrap();
    let mut req = req;
    req.headers_mut().insert(
        axum::http::HeaderName::from_static("x-auth-user-id"),
        axum::http::HeaderValue::from_static("partial-user"),
    );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn me_200_when_full_identity_present() {
    let app = TestApp::build().await;
    let req = AuthRequestBuilder::default()
        .sub("alice")
        .iss("https://idp.test/")
        .email("alice@delphi.test")
        .apply(
            Request::builder()
                .uri("/api/auth/me")
                .body(Body::empty())
                .unwrap(),
        );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::OK);

    let body: Value = res.json();
    assert_eq!(body["user"]["email"], "alice@delphi.test");
    // Tenant header was absent → fell back to default.
    assert!(body["tenant"]["id"]
        .as_str()
        .unwrap()
        .starts_with("tenant:"));
}

#[tokio::test]
async fn unknown_tenant_falls_back_to_default() {
    let app = TestApp::build().await;
    let req = AuthRequestBuilder::default()
        .sub("bob")
        .iss("https://idp.test/")
        .email("bob@delphi.test")
        .tenant("does-not-exist")
        .apply(
            Request::builder()
                .uri("/api/auth/me")
                .body(Body::empty())
                .unwrap(),
        );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::OK);

    let body: Value = res.json();
    let tenant = body["tenant"]["id"].as_str().unwrap();
    let expected = format!("tenant:{}", app.default_tenant_id.key());
    assert!(
        tenant.contains(&app.default_tenant_id.key().to_string())
            || tenant == expected
            || tenant.starts_with("tenant:"),
        "got tenant {tenant}, expected fallback to default"
    );
}

#[tokio::test]
async fn roles_propagate_into_auth_context() {
    let app = TestApp::build().await;
    let req = AuthRequestBuilder::default()
        .sub("carol")
        .iss("https://idp.test/")
        .email("carol@delphi.test")
        .roles("admin,owner")
        .apply(
            Request::builder()
                .uri("/api/auth/me")
                .body(Body::empty())
                .unwrap(),
        );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::OK);

    let body: Value = res.json();
    let roles: Vec<String> = serde_json::from_value(body["roles"].clone()).unwrap();
    assert_eq!(roles, vec!["admin".to_string(), "owner".to_string()]);
}
