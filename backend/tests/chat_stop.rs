//! `POST /api/chat/conversations/{key}/tasks/{task_id}/stop` semantics.
//!
//! Covers the contract from `docs/architecture/chat-streaming.md` § Stop:
//!
//! - 204 when the task id is absent (idempotent — covers
//!   already-finished workers).
//! - 204 when a worker is in flight and the call cancels the per-task
//!   `CancellationToken` (asserted by inspecting the registry).
//! - 404 when the caller cannot see the conversation.
//! - 400 when the `task_id` segment isn't a valid ULID.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use common::{AuthRequestBuilder, TestApp};
use delphi::chat::TaskId;

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
async fn stop_is_204_when_task_unknown() {
    let app = TestApp::build().await;
    let key = create_conversation(&app).await;
    let task_id = TaskId::new();
    let res = app
        .send(auth_post_empty(&format!(
            "/api/chat/conversations/{key}/tasks/{task_id}/stop"
        )))
        .await;
    assert_eq!(res.status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn stop_is_404_for_unknown_conversation() {
    let app = TestApp::build().await;
    let task_id = TaskId::new();
    let res = app
        .send(auth_post_empty(&format!(
            "/api/chat/conversations/doesnotexist/tasks/{task_id}/stop"
        )))
        .await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn stop_is_400_for_garbage_task_id() {
    let app = TestApp::build().await;
    let key = create_conversation(&app).await;
    let res = app
        .send(auth_post_empty(&format!(
            "/api/chat/conversations/{key}/tasks/not-a-ulid/stop"
        )))
        .await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn stop_flips_the_registered_cancel_token() {
    // White-box: register a token under a freshly-minted task id (as
    // the worker would when spawned), then POST /stop and assert the
    // token is flipped and the registry entry is gone.
    let app = TestApp::build().await;
    let key = create_conversation(&app).await;
    let task_id = TaskId::new();
    let token = CancellationToken::new();
    app.tasks.insert(task_id, token.clone());
    assert!(!token.is_cancelled());

    let res = app
        .send(auth_post_empty(&format!(
            "/api/chat/conversations/{key}/tasks/{task_id}/stop"
        )))
        .await;
    assert_eq!(res.status, StatusCode::NO_CONTENT);

    tokio::task::yield_now().await;
    assert!(
        token.is_cancelled(),
        "POST /stop should have cancelled the per-task token"
    );
    assert!(
        app.tasks.is_empty(),
        "registry should be empty after cancel"
    );
}
