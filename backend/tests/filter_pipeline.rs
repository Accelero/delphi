//! Filter sits between adapter and the ingest API call on the scheduler
//! path. `NoopFilter` accepts everything; a hand-rolled `RejectAllFilter`
//! shows that a rejection silently drops the body — `IngestApiClient`
//! is never called.

mod common;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use tokio::time::sleep;

use delphi::auth::{Hs512ServiceIdentity, ServiceIdentity};
use delphi::filter::{Decision, IngestFilter, NoopFilter};
use delphi::ingestion::IngestRequestBody;
use delphi::sources::{run_scheduler, AdapterRegistry, IngestApiClient};
use delphi::storage::Storage;

use crate::common::fake_source::FakeAdapter;
use crate::common::{TestApp, TEST_JWT_SECRET};

fn body(canonical_id: &str) -> IngestRequestBody {
    IngestRequestBody {
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
    async fn evaluate(&self, _req: &IngestRequestBody) -> Decision {
        Decision::Reject {
            reason: "test always rejects".into(),
        }
    }
}

async fn make_ingest(app: &TestApp) -> (Arc<IngestApiClient>, tokio::task::JoinHandle<()>) {
    let (base_url, server) = app.serve_local().await;
    let identity: Arc<dyn ServiceIdentity> = Arc::new(Hs512ServiceIdentity::new(
        "test",
        TEST_JWT_SECRET,
        app.default_tenant_slug.clone(),
        "delphi_test",
        "main",
    ));
    (Arc::new(IngestApiClient::new(base_url, identity)), server)
}

#[tokio::test]
async fn noop_filter_lets_everything_through() {
    let app = TestApp::build().await;
    let (ingest, server) = make_ingest(&app).await;
    let filter: Arc<dyn IngestFilter> = Arc::new(NoopFilter::new());
    let storage: Arc<dyn Storage> = app.system.storage();

    let adapter = Arc::new(
        FakeAdapter::new(
            "noop-test",
            Duration::from_millis(50),
            vec![vec![body("a"), body("b"), body("c")]],
        )
        .with_next_cursor(json!({ "x": 1 })),
    );
    let mut registry = AdapterRegistry::new();
    registry.register(adapter);

    let handle = run_scheduler(
        ingest,
        filter,
        storage.clone(),
        app.default_tenant_id.clone(),
        registry,
    );
    sleep(Duration::from_millis(300)).await;
    handle.shutdown().await;
    server.abort();

    for canonical_id in ["a", "b", "c"] {
        let doc = storage
            .get_document_by_canonical(&app.default_tenant_id, canonical_id)
            .await
            .unwrap();
        assert!(doc.is_some(), "{canonical_id} should have reached the API");
    }
}

#[tokio::test]
async fn reject_all_filter_blocks_every_item() {
    let app = TestApp::build().await;
    let (ingest, server) = make_ingest(&app).await;
    let filter: Arc<dyn IngestFilter> = Arc::new(RejectAllFilter);
    let storage: Arc<dyn Storage> = app.system.storage();

    let adapter = Arc::new(FakeAdapter::new(
        "reject-test",
        Duration::from_millis(50),
        vec![vec![body("a"), body("b")]],
    ));
    let mut registry = AdapterRegistry::new();
    registry.register(adapter);

    let handle = run_scheduler(
        ingest,
        filter,
        storage.clone(),
        app.default_tenant_id.clone(),
        registry,
    );
    sleep(Duration::from_millis(300)).await;
    handle.shutdown().await;
    server.abort();

    for canonical_id in ["a", "b"] {
        let doc = storage
            .get_document_by_canonical(&app.default_tenant_id, canonical_id)
            .await
            .unwrap();
        assert!(
            doc.is_none(),
            "{canonical_id} was filtered out — must not have reached the ingest API"
        );
    }
}
