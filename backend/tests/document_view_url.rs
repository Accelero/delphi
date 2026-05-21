//! `GET /api/documents/:key/view-url` — mints a direct-to-storage
//! download URL after a tenant-scoped authz check.
//!
//! The handler looks up the document through the JWT-bound `AuthedDb`
//! (so SurrealDB PERMISSIONS gate the read), then mints a download grant
//! via the `AccessMinter` seam. The test rig wires `MemAccess`, which
//! mints a deterministic `mem-access://<key>?op=download` pseudo-URL, so
//! we can assert the right storage key is minted and that cross-tenant
//! callers are refused *before* any URL is produced.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};

use common::{AuthRequestBuilder, TestApp};

use delphi::storage::{Document, Storage};

fn doc(canonical: &str, storage_uri: Option<&str>) -> Document {
    Document {
        id: None,
        tenant_id: None,
        canonical_id: Some(canonical.into()),
        source_type: "manual".into(),
        source_uri: format!("https://example.test/{canonical}"),
        storage_uri: storage_uri.map(|s| s.to_string()),
        title: Some("Doc Title".into()),
        authors: vec![],
        published_at: None,
        ingested_at: None,
        language: None,
        summary: None,
        paper_embedding: None,
        paper_embedding_model: None,
        content_hash: "deadbeef".into(),
        version: 1,
        metadata: json!({}),
    }
}

#[tokio::test]
async fn mints_download_url_for_in_tenant_doc() {
    let app = TestApp::build().await;
    let storage = app.system.storage_for(app.default_tenant_id.clone());

    let doc_id = storage
        .upsert_document(&doc("vu-ok", Some("s3://delphi/tenants/test/vu-ok")))
        .await
        .expect("upsert doc");
    let key = doc_id.key().to_string();

    let req = AuthRequestBuilder::default().apply(
        Request::builder()
            .method("GET")
            .uri(format!("/api/documents/{key}/view-url"))
            .body(Body::empty())
            .unwrap(),
    );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.text());
    let body: Value = res.json();
    let url = body["url"].as_str().expect("url");
    // MemAccess mints a download grant keyed on the stripped storage key.
    assert!(url.starts_with("mem-access://tenants/test/vu-ok"), "url: {url}");
    assert!(url.contains("op=download"), "url: {url}");
    assert!(body["expires_at"].as_str().is_some(), "missing expires_at");
}

#[tokio::test]
async fn returns_404_when_no_stored_original() {
    let app = TestApp::build().await;
    let storage = app.system.storage_for(app.default_tenant_id.clone());

    let doc_id = storage
        .upsert_document(&doc("vu-nofile", None))
        .await
        .expect("upsert doc");
    let key = doc_id.key().to_string();

    let req = AuthRequestBuilder::default().apply(
        Request::builder()
            .method("GET")
            .uri(format!("/api/documents/{key}/view-url"))
            .body(Body::empty())
            .unwrap(),
    );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND, "{}", res.text());
}

#[tokio::test]
async fn returns_404_for_unknown_doc() {
    let app = TestApp::build().await;
    let req = AuthRequestBuilder::default().apply(
        Request::builder()
            .method("GET")
            .uri("/api/documents/does-not-exist-zzz/view-url")
            .body(Body::empty())
            .unwrap(),
    );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn cross_tenant_doc_cannot_be_minted() {
    let app = TestApp::build().await;

    // Seed a document in a *different* tenant via the privileged path.
    let other_tenant = delphi::auth::resolve_default_tenant(&app.system, "other")
        .await
        .expect("create tenant");
    let other_storage = app.system.storage_for(other_tenant.clone());
    let doc_id = other_storage
        .upsert_document(&doc("vu-other", Some("s3://delphi/tenants/other/vu-other")))
        .await
        .expect("upsert doc");
    let key = doc_id.key().to_string();

    // Default-tenant caller: engine PERMISSIONS refuse the SELECT, the
    // handler sees `Ok(None)` → 404, and no URL is ever minted.
    let req = AuthRequestBuilder::default().apply(
        Request::builder()
            .method("GET")
            .uri(format!("/api/documents/{key}/view-url"))
            .body(Body::empty())
            .unwrap(),
    );
    let res = app.send(req).await;
    assert_eq!(
        res.status,
        StatusCode::NOT_FOUND,
        "cross-tenant view-url must 404; body={}",
        res.text()
    );
}
