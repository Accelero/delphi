//! Proof the service-identity HTTP loopback path works end-to-end.
//!
//! Mint an HS512 service JWT (`sub: "service:test"`, roles
//! `["ingester"]`) against the test secret, POST to
//! `/api/ingestion/documents`, and verify the document persists and
//! shows up in the per-tenant feed. This is what the in-process
//! scheduler does after the cutover — the only difference at runtime
//! is the source of the request.

mod common;

use axum::body::Body;
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderValue, Request, StatusCode};
use serde_json::{json, Value};

use delphi::auth::{Hs512ServiceIdentity, ServiceIdentity};
use delphi::storage::Storage;

use crate::common::{AuthRequestBuilder, TestApp, TEST_JWT_SECRET};

#[tokio::test]
async fn service_identity_can_ingest_via_http() {
    let app = TestApp::build().await;

    let identity = Hs512ServiceIdentity::new(
        "test",
        TEST_JWT_SECRET,
        app.default_tenant_slug.clone(),
        // Tests run inside an in-memory engine whose namespace is
        // `delphi_test` (see TestApp::build); the JWT's `ns` claim has
        // to match for SurrealDB's app_session AUTHENTICATE clause.
        "delphi_test",
        "main",
    );
    let token = identity.fresh_token().await.expect("mint service token");

    let body = json!({
        "canonical_id": "service-ingest:1",
        "source_type": "test",
        "source_uri": "https://test.example/service-ingest/1",
        "title": "Service ingest",
        "raw_text": "hello from the service identity",
        "metadata": { "x": 1 },
    });

    let mut req = Request::builder()
        .method("POST")
        .uri("/api/ingestion/documents")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    req.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
    );

    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::OK, "service-identity POST should be accepted");
    let outcome: Value = res.json();
    assert_eq!(outcome["outcome"], "created");
    assert_eq!(outcome["version"], 1);

    // Persisted under the test tenant — same tenant the JWT's
    // `tenant_id` claim refers to.
    let doc = app
        .system
        .storage()
        .get_document_by_canonical(&app.default_tenant_id, "service-ingest:1")
        .await
        .unwrap()
        .expect("service-identity ingest persisted document");
    assert_eq!(doc.source_type, "test");

    // Same doc surfaces in the tenant's discovery feed when a user from
    // the same tenant queries — proves the row really is scoped to the
    // service identity's tenant_id, not some side channel.
    let feed_req = AuthRequestBuilder::default()
        .sub("alice")
        .roles("member")
        .apply(
            Request::builder()
                .method("GET")
                .uri("/api/discovery/feed?limit=10")
                .body(Body::empty())
                .unwrap(),
        );
    let feed_res = app.send(feed_req).await;
    assert_eq!(feed_res.status, StatusCode::OK);
    let feed: Value = feed_res.json();
    let items = feed["items"].as_array().expect("items array");
    assert!(
        items
            .iter()
            .any(|it| it["canonical_id"] == "service-ingest:1"),
        "service-ingested doc must appear in same-tenant feed: {items:?}"
    );
}
