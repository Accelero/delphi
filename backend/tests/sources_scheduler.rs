//! Scheduler with a fake adapter against a real HTTP loopback: each
//! tick fires, the adapter's `IngestRequestBody`s POST through
//! `/api/ingestion/documents` under an HS512 service-identity JWT, the
//! pipeline persists, and the cursor advances.
//!
//! Verifies the in-process polling path end-to-end without touching
//! the network *outside* the test process.

mod common;

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::time::sleep;

use delphi::auth::{Hs512ServiceIdentity, ServiceIdentity};
use delphi::filter::{IngestFilter, NoopFilter};
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

#[tokio::test]
async fn scheduler_persists_via_http_and_advances_cursor() {
    let app = TestApp::build().await;
    let (base_url, server) = app.serve_local().await;

    // HS512 service identity, signed with the test secret SurrealDB's
    // app_session access method validates against. Roles=["ingester"]
    // so the role gate inside the HTTP handler accepts the call.
    let identity: Arc<dyn ServiceIdentity> = Arc::new(Hs512ServiceIdentity::new(
        "test",
        TEST_JWT_SECRET,
        app.default_tenant_slug.clone(),
        "delphi_test",
        "main",
    ));
    let ingest = Arc::new(IngestApiClient::new(base_url, identity));

    let adapter = Arc::new(
        FakeAdapter::new(
            "fake",
            Duration::from_millis(50),
            vec![vec![body("doc-A"), body("doc-B")], vec![body("doc-C")]],
        )
        .with_next_cursor(json!({ "since": "2026-05-06" })),
    );

    let mut registry = AdapterRegistry::new();
    registry.register(adapter.clone());

    let filter: Arc<dyn IngestFilter> = Arc::new(NoopFilter::new());
    let storage = app.system.storage_for(app.default_tenant_id.clone());
    let handle = run_scheduler(
        ingest,
        filter,
        app.system.clone(),
        app.default_tenant_id.clone(),
        registry,
    );

    // Adapter waits one full poll_interval before its first tick now
    // (loopback ordering, see scheduler doc-comment); allow several
    // ticks to fire.
    sleep(Duration::from_millis(300)).await;
    handle.shutdown().await;
    server.abort();

    assert!(adapter.fetch_count() >= 2, "fake adapter should have ticked");

    for canonical_id in ["doc-A", "doc-B", "doc-C"] {
        let doc = storage
            .get_document_by_canonical(canonical_id)
            .await
            .unwrap();
        assert!(doc.is_some(), "{canonical_id} not persisted");
    }

    let cursor = app
        .system
        .get_source_cursor(&app.default_tenant_id, "fake")
        .await
        .unwrap();
    assert_eq!(cursor, Some(json!({ "since": "2026-05-06" })));
}
