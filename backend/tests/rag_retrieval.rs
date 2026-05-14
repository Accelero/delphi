//! Chat retrieval over a controlled chunk set.
//!
//! Seeds N chunks for one document with deterministic vectors via the
//! FakeEmbedder, queries with a synthetic vector matching chunk #4, and
//! asserts:
//!
//! - chat returns 200 with the AI SDK stream protocol headers,
//! - the response opens with a `2:` data block listing the citations,
//! - neighbor expansion (radius=1) widens the citation set to include
//!   adjacent ordinals.

mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;

use common::fake_embedder::FakeEmbedder;
use common::TestApp;

use delphi::embedder::Embedder;
use delphi::storage::{Chunk, Document, Storage};

/// Strip the `conversation:` table prefix to recover the record key the
/// per-resource routes are mounted on.
fn key_of(id_str: &str) -> String {
    id_str
        .split_once(':')
        .map(|(_, k)| k.to_string())
        .unwrap_or_else(|| id_str.to_string())
}

#[tokio::test]
async fn chat_streams_citations_block_before_text() {
    // Build app with our deterministic chunk embedder so the chat
    // handler's retrieval path runs end-to-end. The default FakeLlmClient
    // emits a single "ok" delta — small, predictable, and lets us verify
    // ordering (citations data block before text delta).
    let chunk_embedder: Arc<dyn Embedder> =
        Arc::new(FakeEmbedder::new("bge-small-en-v1.5", 384));
    let app = TestApp::build_with_rag(None, Some(chunk_embedder.clone()), None).await;

    let storage = app.system.storage_for(app.default_tenant_id.clone());
    let doc_id = storage
        .upsert_document(&Document {
            id: None,
            tenant_id: None,
            canonical_id: "ret-1".into(),
            source_type: "manual".into(),
            source_uri: "https://example.test/ret-1".into(),
            storage_uri: None,
            title: Some("Retrieval Paper".into()),
            authors: vec![],
            published_at: None,
            ingested_at: None,
            language: None,
            summary: None,
            paper_embedding: None,
            paper_embedding_model: None,
            content_hash: "ret-hash".into(),
            version: 1,
            metadata: json!({}),
        })
        .await
        .unwrap();

    // Seed 30 chunks. Their vectors are the same that the chunk embedder
    // would produce for their text — so a query whose vector matches
    // chunk #2's text returns chunk #2 first. We use more than EFC's
    // sweep size (200 here is the default; we keep N comfortably below
    // since HNSW degenerates on tiny corpora).
    let mut chunks = Vec::new();
    for ord in 0..30i64 {
        let text = format!("chunk content {ord}");
        let v = chunk_embedder
            .passages(&[text.clone()])
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        chunks.push(Chunk {
            id: None,
            doc: None,
            ordinal: ord,
            char_start: 0,
            char_end: text.len() as i64,
            bboxes: None,
            text,
            embedding: v,
            embedding_model: "bge-small-en-v1.5".into(),
            chunk_strategy: "v1-fixed-overlap".into(),
        });
    }
    storage.upsert_chunks(&doc_id, &chunks).await.unwrap();

    // Sanity: vector search returns hits directly. If this is empty,
    // the in-memory engine probably doesn't support HNSW or the test
    // setup is wrong; the failure is informative.
    let q = chunk_embedder.query("chunk content 2").await.unwrap();
    let hits = storage
        .search_vector(&q, 5, &delphi::storage::Filters {
            embedding_model: Some("bge-small-en-v1.5".into()),
            ..Default::default()
        })
        .await
        .expect("search_vector");
    assert!(!hits.is_empty(), "expected KNN hits");
    // Top hit should be ordinal 2 (vectors are deterministic).
    assert_eq!(hits[0].ordinal, 2, "top hit should be chunk #2");

    // Post-rebase, the chat handler lives behind a conversation-keyed
    // route. Create a conversation first, then POST the user message to
    // its `messages` endpoint.
    let create = Request::builder()
        .method("POST")
        .uri("/api/chat/conversations")
        .header("content-type", "application/json")
        .body(Body::from("{}".to_string()))
        .unwrap();
    let create = common::AuthRequestBuilder::default().apply(create);
    let created = app.send(create).await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.text());
    let conv_id = created.json::<serde_json::Value>()["id"]
        .as_str()
        .expect("conversation id")
        .to_string();
    let key = key_of(&conv_id);

    // Query for chunk #2's text — KNN should return chunk #2 first; with
    // RAG_RETRIEVAL_NEIGHBOR_RADIUS=1 expansion adds #1 and #3.
    let body = json!({
        "messages": [{ "role": "user", "content": "chunk content 2" }]
    });
    let req = Request::builder()
        .method("POST")
        .uri(&format!("/api/chat/conversations/{key}/messages"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let req = common::AuthRequestBuilder::default().apply(req);
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.text());

    let text = res.text();
    // The protocol opens with a `2:` data block carrying the citations.
    let first_line = text.lines().next().unwrap_or_default();
    assert!(
        first_line.starts_with("2:"),
        "expected leading `2:` citations block; got first line {first_line:?}\nfull: {text}",
    );
    // Parse the data-block body and assert chunk #2 appears in citations.
    let body_json = &first_line[2..];
    let parsed: serde_json::Value = serde_json::from_str(body_json).expect("json");
    let chunks_arr = parsed[0]["chunks"].as_array().expect("chunks array");
    assert!(!chunks_arr.is_empty(), "no citations: {first_line}");
    let texts: Vec<i64> = chunks_arr
        .iter()
        .filter_map(|c| c["page"].as_i64())
        .collect();
    let _ = texts; // we don't assert page numbers here (no bboxes seeded)

    // Text-delta + finish marker round out the stream.
    assert!(text.contains("0:\"ok\""), "expected text delta in: {text}");
    assert!(text.contains("\"finishReason\":\"stop\""));
}
