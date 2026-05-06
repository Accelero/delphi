//! Filter sits between adapter and sink on the scheduler path.
//! `NoopFilter` accepts everything; a hand-rolled `RejectAllFilter`
//! shows that a rejection silently drops the request — `sink.ingest`
//! is never called.

mod common;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use tokio::time::sleep;

use delphi::filter::{Decision, IngestFilter, NoopFilter};
use delphi::ingestion::{IngestRequest, IngestSink};
use delphi::sources::{run_scheduler, AdapterRegistry};
use delphi::storage::{Storage, SurrealStorage};

use crate::common::fake_sink::CountingSink;
use crate::common::fake_source::FakeAdapter;

fn req(canonical_id: &str) -> IngestRequest {
    IngestRequest {
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

async fn fresh_storage() -> Arc<dyn Storage> {
    let storage = Arc::new(
        SurrealStorage::in_memory("filter_test", "main")
            .await
            .expect("connect"),
    );
    storage.init_schema().await.expect("init schema");
    storage
}

#[tokio::test]
async fn noop_filter_lets_everything_through() {
    let storage = fresh_storage().await;
    let counting = Arc::new(CountingSink::new());
    let sink: Arc<dyn IngestSink> = counting.clone();
    let filter: Arc<dyn IngestFilter> = Arc::new(NoopFilter::new());

    let adapter = Arc::new(FakeAdapter::new(
        "noop-test",
        Duration::from_millis(50),
        vec![vec![req("a"), req("b"), req("c")]],
    )
    .with_next_cursor(json!({ "x": 1 })));
    let mut registry = AdapterRegistry::new();
    registry.register(adapter);

    let handle = run_scheduler(sink, filter, storage, registry);
    sleep(Duration::from_millis(150)).await;
    handle.shutdown().await;

    assert_eq!(counting.count(), 3, "all 3 should reach the sink");
    assert_eq!(counting.ids(), vec!["a", "b", "c"]);
}

#[tokio::test]
async fn reject_all_filter_blocks_every_item() {
    let storage = fresh_storage().await;
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

    let handle = run_scheduler(sink, filter, storage, registry);
    sleep(Duration::from_millis(150)).await;
    handle.shutdown().await;

    assert_eq!(
        counting.count(),
        0,
        "all rejected — sink should never have been called"
    );
}
