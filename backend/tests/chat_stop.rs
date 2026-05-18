//! `POST /api/chat/conversations/{key}/stop` semantics (v3).
//!
//! Covers the contract from `docs/architecture/chat-streaming.md` § Stop:
//!
//! - 204 when no turn is in flight (idempotent).
//! - 404 when the caller cannot see the conversation.
//! - 204 + clear-frame + no-DB-rows when a turn is in flight.
//!
//! The "in-flight" case can't reasonably go through the HTTP stop
//! endpoint with the single-slot test pool: the worker holds the only
//! pool slot while it's parked inside the LLM stream, so the stop
//! handler's perm-check `db.get_conversation` would deadlock waiting
//! for the slot. Production has a multi-slot pool with physically
//! independent connections and is not subject to this. We exercise
//! the same `SessionState::abort` code path the production handler
//! reaches by calling `app.sessions.lookup(...).abort()` directly,
//! and assert the resulting clear-frame + empty-DB outcome that the
//! HTTP path would produce.

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

fn auth_post_empty(uri: &str) -> Request<Body> {
    AuthRequestBuilder::default().apply(
        Request::builder()
            .method("POST")
            .uri(uri)
            .body(Body::empty())
            .unwrap(),
    )
}

fn auth_get(uri: &str) -> Request<Body> {
    AuthRequestBuilder::default().apply(
        Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .unwrap(),
    )
}

fn key_of(id: &str) -> String {
    id.split_once(':')
        .map(|(_, k)| k.to_string())
        .unwrap_or_default()
}

async fn create_conversation(app: &TestApp) -> String {
    let req = auth_post("/api/chat/conversations", json!({}));
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::CREATED, "{}", res.text());
    let body: Value = res.json();
    let id = body["id"].as_str().expect("id").to_string();
    key_of(&id)
}

/// LLM that emits one text delta and then parks. Lets the test set up
/// a stable "Streaming" phase, observe a subscriber, abort, and assert
/// the clear frame.
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
async fn stop_is_204_when_no_turn_in_flight() {
    let app = TestApp::build().await;
    let key = create_conversation(&app).await;
    let res = app
        .send(auth_post_empty(&format!("/api/chat/conversations/{key}/stop")))
        .await;
    assert_eq!(res.status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn stop_is_404_for_unknown_conversation() {
    let app = TestApp::build().await;
    let res = app
        .send(auth_post_empty(
            "/api/chat/conversations/doesnotexist/stop",
        ))
        .await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn abort_during_in_flight_turn_broadcasts_clear_and_persists_nothing() {
    let app = TestApp::build_with_llm(Arc::new(OneShotThenPark)).await;
    let key = create_conversation(&app).await;
    let conv_id: RecordId = RecordId::from(("conversation", key.as_str()));

    // Fire the turn.
    let res = app
        .send(auth_post(
            &format!("/api/chat/conversations/{key}/messages"),
            json!({
                "id": "01HXY0000000000000000000AA",
                "text": "blocked",
                "parent_id": null,
            }),
        ))
        .await;
    assert_eq!(res.status, StatusCode::ACCEPTED);

    // Wait until the worker has registered with the session (buffered
    // user_message at minimum), then subscribe directly via the
    // SessionState. See the module doc for why we don't subscribe via
    // a second HTTP request in tests.
    let session = {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(s) = app.sessions.lookup(&conv_id) {
                break s;
            }
            if std::time::Instant::now() >= deadline {
                panic!("worker never registered with session");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let mut rx = session.subscribe();

    // Wait until at least user_message + text are buffered.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut got: Vec<Bytes> = Vec::new();
    while got.len() < 2 {
        if let Ok(b) = rx.try_recv() {
            got.push(b);
            continue;
        }
        if std::time::Instant::now() >= deadline {
            panic!("worker never emitted user_message + text; got {got:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Same code path the production /stop handler runs after its perm
    // check. Single-slot test pool deadlocks the HTTP path here (see
    // module doc) — calling SessionState::abort directly exercises
    // exactly the same chat-side machinery.
    session.abort();

    // The clear frame should arrive on our subscriber.
    let frame = rx.recv().await.expect("clear frame");
    let s = String::from_utf8(frame.to_vec()).unwrap();
    assert!(
        s.starts_with("event: clear\n"),
        "expected clear, got {s:?}"
    );

    // Give the worker a moment to notice cancel and bail before
    // anything else can race the read.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let res = app
        .send(auth_get(&format!("/api/chat/conversations/{key}")))
        .await;
    let body: Value = res.json();
    let msgs = body["messages"].as_array().expect("messages");
    assert!(msgs.is_empty(), "aborted turn must not persist: {msgs:?}");
}
