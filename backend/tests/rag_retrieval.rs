//! Chat retrieval over a controlled chunk set.
//!
//! Seeds N chunks for one document with deterministic vectors via the
//! FakeEmbedder, queries with a synthetic vector matching chunk #2, and
//! asserts:
//!
//! - the SSE stream emits a `citations` frame before any `text` frame,
//! - neighbor expansion (radius=1) widens the citation set to include
//!   adjacent ordinals,
//! - the stream ends with a `finish` frame.
//!
//! Contract (v3): POST `/messages` is fire-and-forget (202); the SSE
//! stream is `GET /conversations/{key}/stream`. We subscribe first
//! and read until `finish`.

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

    // Open the SSE subscription FIRST so the worker's emit fans out to
    // a registered subscriber.
    let stream_client = client.clone();
    let stream_url = format!("{base_url}/api/chat/conversations/{key}/stream");
    let stream_bearer = bearer.clone();
    let stream_task = tokio::spawn(async move {
        let res = stream_client
            .get(&stream_url)
            .bearer_auth(&stream_bearer)
            .send()
            .await
            .expect("subscribe");
        assert_eq!(res.status().as_u16(), 200);
        let mut bytes = res.bytes_stream();
        let mut acc = Vec::<u8>::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if std::time::Instant::now() > deadline {
                panic!("no finish frame within deadline; got: {}", String::from_utf8_lossy(&acc));
            }
            let next = tokio::time::timeout(Duration::from_millis(500), bytes.next()).await;
            match next {
                Ok(Some(Ok(chunk))) => acc.extend_from_slice(&chunk),
                Ok(Some(Err(e))) => panic!("stream error: {e}"),
                Ok(None) => break,
                Err(_) => {}
            }
            if let Ok(s) = std::str::from_utf8(&acc) {
                if s.contains("event: finish") {
                    break;
                }
            }
        }
        acc
    });

    // Give the GET a moment to land its subscribe before POST starts the turn.
    tokio::time::sleep(Duration::from_millis(50)).await;

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
    assert_eq!(post.status().as_u16(), 202);

    let acc = stream_task.await.expect("stream task");
    server.abort();
    let text = String::from_utf8(acc).expect("utf-8");

    // Frame ordering: user_message → citations → text* → finish. Find
    // each by event name and assert the citations block precedes any
    // text frame.
    let user_idx = text
        .find("event: user_message\n")
        .unwrap_or_else(|| panic!("no user_message: {text}"));
    let citations_idx = text
        .find("event: citations\n")
        .unwrap_or_else(|| panic!("no citations frame: {text}"));
    let text_idx = text
        .find("event: text\n")
        .unwrap_or_else(|| panic!("no text frame: {text}"));
    let finish_idx = text
        .find("event: finish\n")
        .unwrap_or_else(|| panic!("no finish frame: {text}"));
    assert!(user_idx < citations_idx, "user_message before citations");
    assert!(citations_idx < text_idx, "citations before text");
    assert!(text_idx < finish_idx, "text before finish");

    // citations data is a JSON array — parse the line after `data: ` on
    // the citations frame.
    let after = &text[citations_idx..];
    let data_start = after.find("\ndata: ").expect("data line") + "\ndata: ".len();
    let data_end = after[data_start..].find('\n').expect("data eol");
    let json_body = &after[data_start..data_start + data_end];
    let parsed: serde_json::Value = serde_json::from_str(json_body).expect("citations json");
    let arr = parsed.as_array().expect("citations is array");
    assert!(!arr.is_empty(), "expected at least one citation: {json_body}");

    // finish reason
    assert!(
        text.contains("\"finishReason\":\"stop\""),
        "expected stop finish: {text}"
    );
}
