//! /healthz is public — no `X-Auth-*` headers required.
//!
//! Trivial sanity test: also doubles as proof that the test harness
//! (`build_test_app`, oneshot router) wires up correctly.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};

use crate::common::TestApp;

#[tokio::test]
async fn healthz_is_public() {
    let app = TestApp::build().await;
    let res = app
        .send(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(res.status, StatusCode::OK);
}
