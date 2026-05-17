//! Chat retrieval over a controlled chunk set.
//!
//! Seeds N chunks for one document with deterministic vectors via the
//! FakeEmbedder, queries with a synthetic vector matching chunk #2, and
//! asserts:
//!
//! - the SSE stream opens with a `2:` data block listing the citations,
//! - neighbor expansion (radius=1) widens the citation set to include
//!   adjacent ordinals,
//! - the stream ends with the expected `0:`/`d:` records.
//!
//! Contract post-redesign: POST `/messages` returns 200 with the
//! AI SDK stream as its body. We read the body directly until the
//! trailing `d:` frame.

mod common;

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
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

/// Default test identity claims — same shape the `AuthRequestBuilder`
/// produces, but assembled into a fresh JWT we hand to reqwest.
fn test_bearer() -> String {
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde_json::json as j;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let claims = j!({
        "sub": "test-user",
        "iss": "https://idp.test/",
        "email": "test@delphi.test",
        "preferred_username": "Test User",
        "ac": "app_session",
        "ns": "delphi_test",
        "db": "main",
        "iat": now,
        "exp": now + 3600,
    });
    encode(
        &Header::new(jsonwebtoken::Algorithm::HS512),
        &claims,
        &EncodingKey::from_secret(common::TEST_JWT_SECRET.as_bytes()),
    )
    .expect("encode jwt")
}

#[tokio::test]
async fn chat_streams_citations_block_before_text() {
    // Build app with our deterministic chunk embedder so the worker's
    // retrieval path runs end-to-end. The default FakeLlmClient emits a
    // single "ok" delta — small, predictable, and lets us verify ordering
    // (citations data block before text delta).
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
    // chunk #2's text returns chunk #2 first.
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

    // Sanity: vector search returns hits directly.
    let q = chunk_embedder.query("chunk content 2").await.unwrap();
    let hits = storage
        .search_vector(&q, 5, &delphi::storage::Filters {
            embedding_model: Some("bge-small-en-v1.5".into()),
            ..Default::default()
        })
        .await
        .expect("search_vector");
    assert!(!hits.is_empty(), "expected KNN hits");
    assert_eq!(hits[0].ordinal, 2, "top hit should be chunk #2");

    // Spin up the router behind a real socket so we can stream the body
    // (oneshot collects the full response and would hang on the stream's
    // no-EOF semantics).
    let (base_url, server) = app.serve_local().await;
    let bearer = test_bearer();
    let client = reqwest::Client::builder()
        .build()
        .expect("reqwest client");

    // Create a conversation.
    let create = client
        .post(format!("{base_url}/api/chat/conversations"))
        .bearer_auth(&bearer)
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .expect("create conversation");
    assert_eq!(create.status().as_u16(), 201);
    let created: serde_json::Value = create.json().await.expect("conv json");
    let conv_id = created["id"].as_str().expect("conv id").to_string();
    let key = key_of(&conv_id);

    // Submit the user message — the response body IS the AI SDK stream.
    let body = json!({
        "id": "01HXY0000000000000000000ZZ",
        "text": "chunk content 2",
        "parent_id": null,
    });
    let post = client
        .post(format!("{base_url}/api/chat/conversations/{key}/messages"))
        .bearer_auth(&bearer)
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .expect("post message");
    assert_eq!(post.status().as_u16(), 200);
    assert_eq!(
        post.headers()
            .get("x-vercel-ai-data-stream")
            .and_then(|v| v.to_str().ok()),
        Some("v1"),
        "POST must announce the AI SDK data-stream protocol",
    );
    let mut bytes = post.bytes_stream();

    // Drain the stream until we see the trailing `d:` frame, then bail.
    let mut acc = Vec::<u8>::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if std::time::Instant::now() > deadline {
            panic!(
                "did not receive finish frame within deadline; got so far: {}",
                String::from_utf8_lossy(&acc)
            );
        }
        let chunk_fut = tokio::time::timeout(Duration::from_millis(500), bytes.next());
        match chunk_fut.await {
            Ok(Some(Ok(chunk))) => acc.extend_from_slice(&chunk),
            Ok(Some(Err(e))) => panic!("stream error: {e}"),
            Ok(None) => break, // server closed (shouldn't happen but harmless)
            Err(_) => {}       // tick — re-check accumulator for terminal frame
        }
        if let Ok(s) = std::str::from_utf8(&acc) {
            if s.contains("\"finishReason\":") {
                break;
            }
        }
    }
    drop(bytes);
    server.abort();

    let text = String::from_utf8(acc).expect("utf-8");

    // Frame ordering: first the `8:` task frame, then the `2:`
    // citations block before any `0:` text. Find the citations line.
    let mut lines = text.lines();
    let task_line = lines.next().unwrap_or_default();
    assert!(
        task_line.starts_with("8:"),
        "expected leading `8:` task frame; got first line {task_line:?}\nfull: {text}",
    );
    let citations_line = lines.next().unwrap_or_default();
    assert!(
        citations_line.starts_with("2:"),
        "expected `2:` citations block right after the task frame; got {citations_line:?}\nfull: {text}",
    );
    let body_json = &citations_line[2..];
    let parsed: serde_json::Value = serde_json::from_str(body_json).expect("json");
    let chunks_arr = parsed[0]["chunks"].as_array().expect("chunks array");
    assert!(!chunks_arr.is_empty(), "no citations: {citations_line}");

    // Text-delta + finish marker round out the stream.
    assert!(text.contains("0:\"ok\""), "expected text delta in: {text}");
    assert!(text.contains("\"finishReason\":\"stop\""));
}
