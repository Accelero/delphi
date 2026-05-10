//! Filter sits between adapter and sink on the scheduler path.
//! `NoopFilter` accepts everything; a hand-rolled `RejectAllFilter`
//! shows that a rejection silently drops the request — `sink.ingest`
//! is never called.

mod common;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use surrealdb::RecordId;
use tokio::time::sleep;

use delphi::auth::resolve_default_tenant;
use delphi::filter::{Decision, IngestFilter, NoopFilter};
use delphi::ingestion::{IngestRequest, IngestSink};
use delphi::sources::{run_scheduler, AdapterRegistry};
use delphi::storage::{Storage, SystemDb};

use crate::common::fake_sink::CountingSink;
use crate::common::fake_source::FakeAdapter;

fn req(canonical_id: &str) -> IngestRequest {
    IngestRequest {
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

struct RejectAllFilter;

#[async_trait]
impl IngestFilter for RejectAllFilter {
    async fn evaluate(&self, _req: &IngestRequest) -> Decision {
        Decision::Reject {
            reason: "test always rejects".into(),
        }
    }
}

async fn fresh_storage() -> (Arc<dyn Storage>, RecordId) {
    let system = Arc::new(
        SystemDb::in_memory("filter_test", "main")
            .await
            .expect("connect"),
    );
    system.init_schema().await.expect("init schema");
    let tenant = resolve_default_tenant(&system, "test").await.expect("tenant");
    // Tests below exercise the application-layer pipeline, not the
    // engine-RBAC path — `SystemStorage` (privileged) is the right
    // surface here. PERMISSIONS clauses don't fire on this handle.
    let storage: Arc<dyn Storage> = system.storage();
    (storage, tenant)
}

#[tokio::test]
async fn noop_filter_lets_everything_through() {
    let (storage, tenant) = fresh_storage().await;
    let counting = Arc::new(CountingSink::new());
    let sink: Arc<dyn IngestSink> = counting.clone();
    let filter: Arc<dyn IngestFilter> = Arc::new(NoopFilter::new());

    let adapter = Arc::new(
        FakeAdapter::new(
            "noop-test",
            Duration::from_millis(50),
            vec![vec![req("a"), req("b"), req("c")]],
        )
        .with_next_cursor(json!({ "x": 1 })),
    );
    let mut registry = AdapterRegistry::new();
    registry.register(adapter);

    let handle = run_scheduler(sink, filter, storage, tenant, registry);
    sleep(Duration::from_millis(150)).await;
    handle.shutdown().await;

    assert_eq!(counting.count(), 3, "all 3 should reach the sink");
    assert_eq!(counting.ids(), vec!["a", "b", "c"]);
}

#[tokio::test]
async fn reject_all_filter_blocks_every_item() {
    let (storage, tenant) = fresh_storage().await;
    let counting = Arc::new(CountingSink::new());
    let sink: Arc<dyn IngestSink> = counting.clone();
    let filter: Arc<dyn IngestFilter> = Arc::new(RejectAllFilter);

    let adapter = Arc::new(FakeAdapter::new(
        "reject-test",
        Duration::from_millis(50),
        vec![vec![req("a"), req("b")]],
    ));
    let mut registry = AdapterRegistry::new();
    registry.register(adapter);

    let handle = run_scheduler(sink, filter, storage, tenant, registry);
    sleep(Duration::from_millis(150)).await;
    handle.shutdown().await;

    assert_eq!(
        counting.count(),
        0,
        "all rejected — sink should never have been called"
    );
}
