//! A subscriber attached mid-turn replays the current frames buffer.
//!
//! We can't simulate a "second SSE GET arriving mid-turn" cleanly via
//! `tower::ServiceExt::oneshot` in the test harness — the test pool is
//! single-slot (see `tests/common/mod.rs` comment) and the in-flight
//! worker holds the slot, so a fresh GET stream request would block in
//! the identity middleware waiting on the same pool.
//!
//! Instead we exercise the replay-on-subscribe invariant directly: POST
//! the turn (worker buffers the user_message and at least one text
//! frame into the per-conversation SessionState), then call
//! `SessionRegistry::lookup` + `SessionState::subscribe` from the test
//! body and drain the channel. This is exactly the same code path the
//! HTTP handler runs after dropping its AuthedDb extension.

mod common;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use serde_json::{json, Value};
use surrealdb::RecordId;

use common::{AuthRequestBuilder, TestApp};
use delphi::error::Result;
use delphi::llm::{DeltaStream, LlmClient, LlmDelta, LlmMessage};

fn auth_post(uri: &str, body: Value) -> Request<Body> {
    AuthRequestBuilder::default().apply(
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
}

fn key_of(id: &str) -> String {
    id.split_once(':').map(|(_, k)| k.to_string()).unwrap_or_default()
}

/// LLM that emits one text delta and then parks indefinitely. Lets the
/// test observe the partial buffer (user_message + first text) before
/// the worker reaches commit.
struct OneShotThenPark;

#[async_trait]
impl LlmClient for OneShotThenPark {
    async fn stream_chat(&self, _messages: Vec<LlmMessage>) -> Result<DeltaStream> {
        let s = futures::stream::unfold(0u32, |state| async move {
            if state == 0 {
                Some((Ok(LlmDelta::Text("partial".into())), state + 1))
            } else {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Some((Ok(LlmDelta::Text("never".into())), state + 1))
            }
        });
        Ok(Box::pin(s))
    }
}

#[tokio::test]
async fn late_subscriber_replays_buffered_frames() {
    let app = TestApp::build_with_llm(Arc::new(OneShotThenPark)).await;
    let res = app
        .send(auth_post("/api/chat/conversations", json!({})))
        .await;
    assert_eq!(res.status, StatusCode::CREATED);
    let body: Value = res.json();
    let key = key_of(body["id"].as_str().expect("id"));
    let conv_id: RecordId = RecordId::from(("conversation", key.as_str()));

    let user_id = "01HXY0000000000000000000AB";
    let res = app
        .send(auth_post(
            &format!("/api/chat/conversations/{key}/messages"),
            json!({"id": user_id, "text": "hi", "parent_id": null}),
        ))
        .await;
    assert_eq!(res.status, StatusCode::ACCEPTED);

    // Wait until the worker has emitted at least user_message + 1 text,
    // then "late-subscribe" via SessionState directly and assert the
    // buffer was replayed in order.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let frames: Vec<Bytes> = loop {
        if let Some(s) = app.sessions.lookup(&conv_id) {
            let mut rx = s.subscribe();
            let mut frames: Vec<Bytes> = Vec::new();
            while let Ok(b) = rx.try_recv() {
                frames.push(b);
            }
            if frames.len() >= 2 {
                break frames;
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for worker to buffer frames");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_replay_ordering(&frames, user_id);
}

fn assert_replay_ordering(frames: &[Bytes], user_id: &str) {
    // Frame 1 is always user_message; frame 2+ is text.
    let to_str = |b: &Bytes| String::from_utf8(b.to_vec()).unwrap();
    let first = to_str(&frames[0]);
    assert!(
        first.starts_with("event: user_message\n"),
        "first replay frame should be user_message: {first:?}"
    );
    assert!(
        first.contains(&format!("\"id\":\"message:{user_id}\"")),
        "user_message must carry the id we POSTed: {first:?}"
    );
    let second = to_str(&frames[1]);
    assert!(
        second.starts_with("event: text\n"),
        "second replay frame should be text: {second:?}"
    );
    assert!(
        second.contains("\"partial\""),
        "expected the OneShotThenPark text delta: {second:?}"
    );
}
