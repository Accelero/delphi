//! Scheduler with a fake adapter: tick fires, pipeline persists, cursor
//! advances. Verifies the in-process polling path end-to-end without
//! touching the network.

mod common;

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use surrealdb::RecordId;
use tokio::time::sleep;

use delphi::auth::resolve_default_tenant;
use delphi::filter::{IngestFilter, NoopFilter};
use delphi::ingestion::{IngestRequest, IngestSink, Pipeline};
use delphi::sources::{run_scheduler, AdapterRegistry};
use delphi::storage::{Storage, SystemDb};

use crate::common::fake_source::FakeAdapter;

fn req(canonical_id: &str) -> IngestRequest {
    IngestRequest {
        // Placeholder — scheduler always overwrites via its tenant_id arg.
        tenant_id: RecordId::from(("tenant", "scheduler-placeholder")),
        canonical_id: canonical_id.into(),
        source_type: "fake".into(),
        source_uri: format!("https://fake.example/{canonical_id}"),
        title: Some("Fake".into()),
        authors: vec![],
        published_at: None,
        language: None,
        summary: None,
        raw_text: Some(format!("body of {canonical_id}")),
        storage_uri: None,
        metadata: serde_json::Value::Object(Default::default()),
    }
}

#[tokio::test]
async fn scheduler_persists_and_advances_cursor() {
    let system = Arc::new(
        SystemDb::in_memory("scheduler_test", "main")
            .await
            .expect("connect"),
    );
    system.init_schema().await.expect("init schema");
    let tenant = resolve_default_tenant(&system, "test")
        .await
        .expect("seed tenant");

    // Scheduler runs against the privileged path in production —
    // the test mirrors that wiring rather than the per-request path.
    let trait_storage: Arc<dyn Storage> = system.storage();
    let sink: Arc<dyn IngestSink> = Arc::new(Pipeline::new(trait_storage.clone()));

    let adapter = Arc::new(
        FakeAdapter::new(
            "fake",
            Duration::from_millis(50),
            vec![vec![req("doc-A"), req("doc-B")], vec![req("doc-C")]],
        )
        .with_next_cursor(json!({ "since": "2026-05-06" })),
    );

    let mut registry = AdapterRegistry::new();
    registry.register(adapter.clone());

    let filter: Arc<dyn IngestFilter> = Arc::new(NoopFilter::new());
    let handle = run_scheduler(sink, filter, trait_storage.clone(), tenant.clone(), registry);

    // Wait long enough for at least two ticks to fire (first is immediate).
    sleep(Duration::from_millis(200)).await;
    handle.shutdown().await;

    assert!(adapter.fetch_count() >= 2, "fake adapter should have ticked");

    for canonical_id in ["doc-A", "doc-B", "doc-C"] {
        let doc = trait_storage
            .get_document_by_canonical(&tenant, canonical_id)
            .await
            .unwrap();
        assert!(doc.is_some(), "{canonical_id} not persisted");
    }

    let cursor = trait_storage
        .get_source_cursor(&tenant, "fake")
        .await
        .unwrap();
    assert_eq!(cursor, Some(json!({ "since": "2026-05-06" })));
}
