//! `POST /api/ingestion/documents` end-to-end:
//!
//!   X-Auth-* + role gate → IngestRequest (JSON body)
//!                      → IngestSink::ingest (Pipeline)
//!                      → SurrealDB upsert
//!
//! The HTTP handler is a thin wrapper around the same `IngestSink` the
//! in-process scheduler uses; this test covers the auth perimeter and
//! verifies persistence.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};

use delphi::storage::Storage;

use crate::common::{AuthRequestBuilder, TestApp};

fn ingest_request(canonical_id: &str, body: &str) -> Body {
    Body::from(
        json!({
            "canonical_id": canonical_id,
            "source_type": "test",
            "source_uri": format!("https://test.example/{canonical_id}"),
            "title": "Test Doc",
            "raw_text": body,
        })
        .to_string(),
    )
}

#[tokio::test]
async fn ingest_401_when_unauthenticated() {
    let app = TestApp::build().await;
    let res = app
        .send(
            Request::builder()
                .method("POST")
                .uri("/api/ingestion/documents")
                .header("content-type", "application/json")
                .body(ingest_request("doc-1", "hello"))
                .unwrap(),
        )
        .await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn ingest_403_when_role_missing() {
    let app = TestApp::build().await;
    let req = AuthRequestBuilder::default()
        .sub("alice")
        // no roles set → not an ingester
        .apply(
            Request::builder()
                .method("POST")
                .uri("/api/ingestion/documents")
                .header("content-type", "application/json")
                .body(ingest_request("doc-1", "hello"))
                .unwrap(),
        );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn ingest_200_creates_then_unchanged_then_versioned() {
    let app = TestApp::build().await;

    let post = |canonical_id: &str, body: &str| {
        AuthRequestBuilder::default()
            .sub("ingestor-1")
            .roles("ingester")
            .apply(
                Request::builder()
                    .method("POST")
                    .uri("/api/ingestion/documents")
                    .header("content-type", "application/json")
                    .body(ingest_request(canonical_id, body))
                    .unwrap(),
            )
    };

    let res = app.send(post("doc-1", "hello")).await;
    assert_eq!(res.status, StatusCode::OK, "first POST");
    let body: Value = res.json();
    assert_eq!(body["outcome"], "created");
    assert_eq!(body["version"], 1);

    let res = app.send(post("doc-1", "hello")).await;
    assert_eq!(res.status, StatusCode::OK, "duplicate POST");
    let body: Value = res.json();
    assert_eq!(body["outcome"], "unchanged");
    assert_eq!(body["version"], 1);

    let res = app.send(post("doc-1", "hello, again")).await;
    assert_eq!(res.status, StatusCode::OK, "modified POST");
    let body: Value = res.json();
    assert_eq!(body["outcome"], "versioned");
    assert_eq!(body["version"], 2);

    // Direct DB inspection: the document persisted and its version is current.
    let doc = app
        .system
        .storage()
        .get_document_by_canonical(&app.default_tenant_id, "doc-1")
        .await
        .unwrap()
        .expect("document persisted");
    assert_eq!(doc.version, 2);
    assert_eq!(doc.source_type, "test");
}

#[tokio::test]
async fn ingest_owner_role_also_works() {
    let app = TestApp::build().await;
    let req = AuthRequestBuilder::default()
        .sub("the-owner")
        .roles("owner")
        .apply(
            Request::builder()
                .method("POST")
                .uri("/api/ingestion/documents")
                .header("content-type", "application/json")
                .body(ingest_request("doc-owned", "owners can ingest"))
                .unwrap(),
        );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::OK);
}
