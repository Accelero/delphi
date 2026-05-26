//! HTTP-direct and scheduler ingest must produce the same persisted
//! state. After the cutover both paths terminate at the same
//! `/api/ingestion/documents` handler, so this test is structurally
//! guaranteed — it stays in the tree as a regression net against a
//! future refactor that re-introduces a parallel ingest codepath.

mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};

use delphi::auth::{Hs512ServiceIdentity, ServiceIdentity};
use delphi::filter::{IngestFilter, NoopFilter};
use delphi::ingestion::IngestRequestBody;
use delphi::sources::{run_scheduler, AdapterRegistry, IngestApiClient};
use delphi::storage::Storage;

use crate::common::fake_source::FakeAdapter;
use crate::common::{AuthRequestBuilder, TestApp, TEST_JWT_SECRET};

fn payload(canonical_id: &str) -> serde_json::Value {
    json!({
        "canonical_id": canonical_id,
        "source_type": "symmetry-test",
        "source_uri": "https://symmetry.example/x",
        "title": "Symmetry",
        "raw_text": "the same body for both paths",
        "metadata": { "x": 1 },
    })
}

fn build_body(canonical_id: &str) -> IngestRequestBody {
    IngestRequestBody {
        canonical_id: canonical_id.into(),
        source_type: "symmetry-test".into(),
        source_uri: "https://symmetry.example/x".into(),
        title: Some("Symmetry".into()),
        authors: vec![],
        published_at: None,
        language: None,
        summary: None,
        raw_text: Some("the same body for both paths".into()),
        storage_uri: None,
        metadata: json!({ "x": 1 }),
    }
}

#[tokio::test]
async fn http_and_scheduler_produce_equal_outcomes() {
    // Path A: HTTP. Use TestApp's full router so the IngestSink call is
    // routed through axum + auth + JSON deserialization under a user
    // identity.
    let app_http = TestApp::build().await;
    let req = AuthRequestBuilder::default()
        .sub("ingestor")
        .roles("ingester")
        .apply(
            Request::builder()
                .method("POST")
                .uri("/api/ingestion/documents")
                .header("content-type", "application/json")
                .body(Body::from(payload("sym-1").to_string()))
                .unwrap(),
        );
    let http_res = app_http.send(req).await;
    assert_eq!(http_res.status, StatusCode::OK);
    let http_outcome: Value = http_res.json();
    assert_eq!(http_outcome["outcome"], "created");
    assert_eq!(http_outcome["version"], 1);
    let http_doc = app_http
        .system
        .storage_for(app_http.default_tenant_id.clone())
        .get_document_by_canonical("sym-1")
        .await
        .unwrap()
        .expect("HTTP path persisted doc");

    // Path B: scheduler, fresh app. The scheduler now goes through HTTP
    // under a service-identity JWT — same endpoint, same handler.
    let app_sched = TestApp::build().await;
    let (base_url, server) = app_sched.serve_local().await;
    let identity: Arc<dyn ServiceIdentity> = Arc::new(Hs512ServiceIdentity::new(
        "test",
        TEST_JWT_SECRET,
        app_sched.default_tenant_slug.clone(),
        "delphi_test",
        "main",
    ));
    let ingest = Arc::new(IngestApiClient::new(base_url, identity));

    let adapter = Arc::new(FakeAdapter::new(
        "symmetry",
        Duration::from_millis(50),
        vec![vec![build_body("sym-1")]],
    ));
    let mut registry = AdapterRegistry::new();
    registry.register(adapter.clone());
    let filter: Arc<dyn IngestFilter> = Arc::new(NoopFilter::new());
    let storage = app_sched.system.storage_for(app_sched.default_tenant_id.clone());
    let handle = run_scheduler(
        ingest,
        filter,
        app_sched.system.clone(),
        app_sched.default_tenant_id.clone(),
        registry,
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    handle.shutdown().await;
    server.abort();

    let sched_doc = storage
        .get_document_by_canonical("sym-1")
        .await
        .unwrap()
        .expect("scheduler path persisted doc");

    // Symmetry: both paths agree on every persisted field except for
    // server-stamped `ingested_at` (different wall-clocks) and
    // `tenant_id` (different DBs — same slug, different RecordIds).
    assert_eq!(http_doc.canonical_id, sched_doc.canonical_id);
    assert_eq!(http_doc.source_type, sched_doc.source_type);
    assert_eq!(http_doc.source_uri, sched_doc.source_uri);
    assert_eq!(http_doc.title, sched_doc.title);
    assert_eq!(http_doc.content_hash, sched_doc.content_hash);
    assert_eq!(http_doc.version, sched_doc.version);
    assert_eq!(http_doc.metadata, sched_doc.metadata);
}
