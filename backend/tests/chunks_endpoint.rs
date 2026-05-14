//! `GET /api/chunks/:id` — tenant scoping + shape.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;

use common::{AuthRequestBuilder, TestApp};

use delphi::storage::{Bbox, Chunk, Document, Storage};

#[tokio::test]
async fn returns_chunk_payload_when_in_tenant() {
    let app = TestApp::build().await;
    let storage = app.system.storage_for(app.default_tenant_id.clone());

    // Seed a document + chunk directly through the storage layer to
    // skip the embedder dependency.
    let doc_id = storage
        .upsert_document(&Document {
            id: None,
            tenant_id: None,
            canonical_id: "ch-tenant".into(),
            source_type: "manual".into(),
            source_uri: "https://example.test/ch-tenant".into(),
            storage_uri: None,
            title: Some("Title".into()),
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
        })
        .await
        .expect("upsert doc");
    let bboxes = vec![
        Bbox { page: 1, x: 10.0, y: 100.0, w: 200.0, h: 12.0 },
        Bbox { page: 1, x: 10.0, y: 88.0, w: 180.0, h: 12.0 },
    ];
    let chunk = Chunk {
        id: None,
        doc: None,
        ordinal: 0,
        char_start: 0,
        char_end: 100,
        bboxes: Some(bboxes.clone()),
        text: "hello world".into(),
        embedding: vec![0.0; 384],
        embedding_model: "bge-small-en-v1.5".into(),
        chunk_strategy: "v1-fixed-overlap".into(),
    };
    let chunk_ids = storage
        .upsert_chunks(&doc_id, &[chunk])
        .await
        .expect("upsert chunk");
    let chunk_id = chunk_ids.into_iter().next().expect("chunk id");
    let key = chunk_id.key().to_string();

    let req = Request::builder()
        .method("GET")
        .uri(format!("/api/chunks/{}", key))
        .body(Body::empty())
        .unwrap();
    let req = AuthRequestBuilder::default().apply(req);
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.text());
    let body: serde_json::Value = res.json();
    assert!(body["id"].as_str().unwrap().starts_with("chunk:"));
    assert_eq!(body["doc_id"].as_str().unwrap(), doc_id.to_string());
    assert_eq!(body["text"], "hello world");
    let bb = body["bboxes"].as_array().expect("bboxes array");
    assert_eq!(bb.len(), 2);
    assert_eq!(bb[0]["page"], 1);
}

#[tokio::test]
async fn returns_404_for_unknown_id() {
    let app = TestApp::build().await;
    let req = Request::builder()
        .method("GET")
        .uri("/api/chunks/does-not-exist-zzz")
        .body(Body::empty())
        .unwrap();
    let req = AuthRequestBuilder::default().apply(req);
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn returns_404_when_chunk_belongs_to_another_tenant() {
    let app = TestApp::build().await;

    // Bootstrap a second tenant + user. The HTTP path needs an
    // `app_user` row matching the JWT's (iss, sub) for SurrealDB's
    // AUTHENTICATE clause to resolve $auth.
    let other_tenant = delphi::auth::resolve_default_tenant(&app.system, "other")
        .await
        .expect("create tenant");

    // Seed the chunk in tenant "other" via the privileged path.
    let other_storage = app.system.storage_for(other_tenant.clone());
    let doc_id = other_storage
        .upsert_document(&Document {
            id: None,
            tenant_id: None,
            canonical_id: "ct-2".into(),
            source_type: "manual".into(),
            source_uri: "https://example.test/ct-2".into(),
            storage_uri: None,
            title: None,
            authors: vec![],
            published_at: None,
            ingested_at: None,
            language: None,
            summary: None,
            paper_embedding: None,
            paper_embedding_model: None,
            content_hash: "00".into(),
            version: 1,
            metadata: json!({}),
        })
        .await
        .unwrap();
    let chunk_id = other_storage
        .upsert_chunks(
            &doc_id,
            &[Chunk {
                id: None,
                doc: None,
                ordinal: 0,
                char_start: 0,
                char_end: 5,
                bboxes: None,
                text: "hello".into(),
                embedding: vec![0.0; 384],
                embedding_model: "bge-small-en-v1.5".into(),
                chunk_strategy: "v1-fixed-overlap".into(),
            }],
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let key = chunk_id.key().to_string();

    // Now drive the request as the default-tenant user. Engine
    // PERMISSIONS should refuse the SELECT and we get a 404.
    let req = Request::builder()
        .method("GET")
        .uri(format!("/api/chunks/{}", key))
        .body(Body::empty())
        .unwrap();
    let req = AuthRequestBuilder::default().apply(req);
    let res = app.send(req).await;
    assert_eq!(
        res.status,
        StatusCode::NOT_FOUND,
        "cross-tenant chunk must 404 ({}); body={}",
        res.status,
        res.text()
    );
}
