//! The same `IngestRequest` submitted via the HTTP endpoint and via the
//! in-process scheduler must produce the same `IngestOutcome` and the
//! same persisted state. This is the test that *enforces* the
//! "one unified interface" property of `IngestSink` — if anyone
//! introduces a parallel codepath in either layer, this test catches it.

mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use surrealdb::RecordId;

use delphi::auth::resolve_default_tenant;
use delphi::filter::{IngestFilter, NoopFilter};
use delphi::ingestion::{IngestRequest, IngestSink, Pipeline};
use delphi::sources::{run_scheduler, AdapterRegistry};
use delphi::storage::{Storage, SystemDb};

use crate::common::fake_source::FakeAdapter;
use crate::common::{AuthRequestBuilder, TestApp};

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

fn build_request(tenant: &RecordId, canonical_id: &str) -> IngestRequest {
    IngestRequest {
        tenant_id: tenant.clone(),
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
    // routed through axum + auth + JSON deserialization.
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
        .storage()
        .get_document_by_canonical(&app_http.default_tenant_id, "sym-1")
        .await
        .unwrap()
        .expect("HTTP path persisted doc");

    // Path B: scheduler with the exact same `IngestRequest` shape in a
    // fresh DB.
    let system_b = Arc::new(
        SystemDb::in_memory("symmetry_test_b", "main")
            .await
            .unwrap(),
    );
    system_b.init_schema().await.unwrap();
    let tenant_b = resolve_default_tenant(&system_b, "test")
        .await
        .expect("tenant");
    let trait_storage_b: Arc<dyn Storage> = system_b.storage();
    let sink_b: Arc<dyn IngestSink> = Arc::new(Pipeline::new(trait_storage_b.clone()));

    let item = build_request(&tenant_b, "sym-1");
    let adapter = Arc::new(FakeAdapter::new(
        "symmetry",
        Duration::from_millis(50),
        vec![vec![item]],
    ));
    let mut registry = AdapterRegistry::new();
    registry.register(adapter.clone());
    let filter: Arc<dyn IngestFilter> = Arc::new(NoopFilter::new());
    let handle = run_scheduler(
        sink_b,
        filter,
        trait_storage_b.clone(),
        tenant_b.clone(),
        registry,
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    handle.shutdown().await;

    let sched_doc = trait_storage_b
        .get_document_by_canonical(&tenant_b, "sym-1")
        .await
        .unwrap()
        .expect("scheduler path persisted doc");

    // Symmetry: both paths agree on every persisted field except for
    // server-stamped `ingested_at` (different DBs, different wall-clock)
    // and `tenant_id` (different DBs have different tenant RecordIds).
    assert_eq!(http_doc.canonical_id, sched_doc.canonical_id);
    assert_eq!(http_doc.source_type, sched_doc.source_type);
    assert_eq!(http_doc.source_uri, sched_doc.source_uri);
    assert_eq!(http_doc.title, sched_doc.title);
    assert_eq!(http_doc.content_hash, sched_doc.content_hash);
    assert_eq!(http_doc.version, sched_doc.version);
    assert_eq!(http_doc.metadata, sched_doc.metadata);
}
