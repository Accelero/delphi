//! `POST /api/chat/conversations/{id}/stop` semantics.
//!
//! Covers the contract from `docs/architecture/chat-streaming.md` § Stop:
//!
//! - 204 when no worker is in flight (idempotent on the empty case).
//! - 404 when the caller cannot see the conversation.
//! - 204 + the per-session `CancellationToken` flips when a worker is
//!   active.
//!
//! The end-to-end "worker actually persists the partial reply on
//! cancel" assertion isn't reachable from a TestApp-driven test: the
//! in-memory engine forces a single-slot `RequestDbPool` (see the
//! comment in `tests/common/mod.rs`), and a worker that parks inside
//! its LLM stream holds that slot for the duration. Any concurrent test
//! request blocks waiting on the slot, so we can't both pin the worker
//! mid-stream *and* observe the world from the same test. The
//! commit-on-partial code path is identical between `StopReason::User`
//! and `StopReason::Eof`, and Eof is already exercised by the
//! conversations `post_message_returns_202…` integration test. What's
//! specific to `/stop` — the endpoint fires the per-session cancel
//! token — is what we pin here.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use common::{AuthRequestBuilder, TestApp};

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

#[tokio::test]
async fn stop_is_204_with_no_worker() {
    let app = TestApp::build().await;
    let key = create_conversation(&app).await;

    let res = app
        .send(auth_post_empty(&format!(
            "/api/chat/conversations/{key}/stop"
        )))
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
async fn stop_cancels_the_active_turn_token() {
    // White-box: stand in for the worker by getting/creating the
    // session state directly and installing a `CancellationToken` via
    // the test-only seam on SessionState. Mirrors the install
    // `chat::worker::run` does just before `drive_turn`. Then POST
    // /stop and assert the token flipped.
    let app = TestApp::build().await;
    let key = create_conversation(&app).await;
    let id = surrealdb::RecordId::from(("conversation", key.as_str()));

    let session = app.session_registry.get_or_create(&id).await;
    let token = CancellationToken::new();
    session.set_current_turn_for_test(token.clone()).await;
    assert!(!token.is_cancelled());

    let res = app
        .send(auth_post_empty(&format!(
            "/api/chat/conversations/{key}/stop"
        )))
        .await;
    assert_eq!(res.status, StatusCode::NO_CONTENT);

    // CancellationToken::cancel is synchronous w.r.t. wakers but
    // `is_cancelled` observes through memory ordering — yield once to
    // keep the assertion deterministic on every scheduler.
    tokio::task::yield_now().await;
    assert!(
        token.is_cancelled(),
        "POST /stop should have cancelled the per-turn token"
    );
}
