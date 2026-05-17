//! Persisted chat conversations: CRUD + the streaming message endpoint.
//!
//! Drives the in-process router via `tower::ServiceExt::oneshot` exactly
//! like `discovery_feed.rs`. The fake LLM emits `"ok"` for any prompt,
//! which is sufficient to exercise both the streaming protocol and the
//! best-effort auto-title path.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};

use crate::common::{AuthRequestBuilder, TestApp};

fn auth_get(uri: &str) -> Request<Body> {
    AuthRequestBuilder::default().sub("alice").apply(
        Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .unwrap(),
    )
}

fn auth_post(uri: &str, body: Value) -> Request<Body> {
    AuthRequestBuilder::default().sub("alice").apply(
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
}

fn auth_patch(uri: &str, body: Value) -> Request<Body> {
    AuthRequestBuilder::default().sub("alice").apply(
        Request::builder()
            .method("PATCH")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
}

fn auth_delete(uri: &str) -> Request<Body> {
    AuthRequestBuilder::default().sub("alice").apply(
        Request::builder()
            .method("DELETE")
            .uri(uri)
            .body(Body::empty())
            .unwrap(),
    )
}

/// Strip the `conversation:` table prefix to recover the record key the
/// per-resource routes are mounted on.
fn key_of(id_str: &str) -> String {
    id_str
        .split_once(':')
        .map(|(_, k)| k.to_string())
        .unwrap_or_else(|| id_str.to_string())
}

async fn create_one(app: &TestApp) -> String {
    let res = app
        .send(auth_post("/api/chat/conversations", json!({})))
        .await;
    assert_eq!(res.status, StatusCode::CREATED, "create");
    let body: Value = res.json();
    body["id"].as_str().expect("id present").to_string()
}

#[tokio::test]
async fn list_starts_empty_then_reflects_create() {
    let app = TestApp::build().await;

    let res = app.send(auth_get("/api/chat/conversations")).await;
    assert_eq!(res.status, StatusCode::OK);
    let items: Value = res.json();
    assert_eq!(items.as_array().unwrap().len(), 0);

    let id = create_one(&app).await;
    assert!(id.starts_with("conversation:"));

    let res = app.send(auth_get("/api/chat/conversations")).await;
    let items: Value = res.json();
    let arr = items.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["id"].as_str().unwrap(), id);
    assert!(arr[0]["title"].is_null(), "title starts null");
}

#[tokio::test]
async fn post_message_returns_202_and_worker_persists_both_messages_and_title() {
    let app = TestApp::build().await;
    let id = create_one(&app).await;
    let key = key_of(&id);

    let uri = format!("/api/chat/conversations/{key}/messages");
    let res = app
        .send(auth_post(
            &uri,
            json!({
                "messages": [
                    {"role": "user", "content": "Hello"}
                ]
            }),
        ))
        .await;
    // POST is fire-and-forget under the new design — the streaming
    // reply arrives on the separate GET /stream subscription.
    assert_eq!(res.status, StatusCode::ACCEPTED, "submit accepted");
    assert!(res.bytes.is_empty(), "body should be empty");

    // The worker runs detached; poll the conversation until both
    // messages + the auto-title land. FakeLlm default emits "ok"
    // synchronously so this usually resolves in the first iteration.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let res = app
            .send(auth_get(&format!("/api/chat/conversations/{key}")))
            .await;
        assert_eq!(res.status, StatusCode::OK);
        let body: Value = res.json();
        let msgs = body["messages"].as_array().cloned().unwrap_or_default();
        let title_ok = body["conversation"]["title"].as_str().is_some();
        if msgs.len() == 2 && title_ok {
            assert_eq!(msgs[0]["role"], "user");
            assert_eq!(msgs[0]["content"], "Hello");
            assert_eq!(msgs[1]["role"], "assistant");
            assert_eq!(msgs[1]["content"], "ok");
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "worker did not persist within deadline; last body = {body:?}"
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn rename_updates_title() {
    let app = TestApp::build().await;
    let id = create_one(&app).await;
    let key = key_of(&id);

    let res = app
        .send(auth_patch(
            &format!("/api/chat/conversations/{key}"),
            json!({"title": "renamed"}),
        ))
        .await;
    assert_eq!(res.status, StatusCode::NO_CONTENT);

    let res = app.send(auth_get("/api/chat/conversations")).await;
    let items: Value = res.json();
    assert_eq!(items.as_array().unwrap()[0]["title"], "renamed");
}

#[tokio::test]
async fn rename_rejects_empty_or_too_long_title() {
    let app = TestApp::build().await;
    let id = create_one(&app).await;
    let key = key_of(&id);

    let uri = format!("/api/chat/conversations/{key}");
    let res = app.send(auth_patch(&uri, json!({"title": "   "}))).await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);

    let long = "a".repeat(201);
    let res = app.send(auth_patch(&uri, json!({"title": long}))).await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn delete_cascades_and_is_idempotent() {
    let app = TestApp::build().await;
    let id = create_one(&app).await;
    let key = key_of(&id);

    // Post a message so we have something to cascade. POST returns
    // 202 immediately; we don't wait for the worker because the user
    // message is persisted synchronously before the 202 is sent.
    let _ = app
        .send(auth_post(
            &format!("/api/chat/conversations/{key}/messages"),
            json!({"messages": [{"role": "user", "content": "hi"}]}),
        ))
        .await;

    let res = app
        .send(auth_delete(&format!("/api/chat/conversations/{key}")))
        .await;
    assert_eq!(res.status, StatusCode::NO_CONTENT);

    // List is now empty.
    let res = app.send(auth_get("/api/chat/conversations")).await;
    let items: Value = res.json();
    assert_eq!(items.as_array().unwrap().len(), 0);

    // Direct GET of the deleted id is 404 — confirms cascade ran.
    let res = app
        .send(auth_get(&format!("/api/chat/conversations/{key}")))
        .await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);

    // Idempotent: deleting again is still 204.
    let res = app
        .send(auth_delete(&format!("/api/chat/conversations/{key}")))
        .await;
    assert_eq!(res.status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn get_404_unknown_id() {
    let app = TestApp::build().await;
    let res = app
        .send(auth_get("/api/chat/conversations/does-not-exist"))
        .await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn patch_404_unknown_id() {
    let app = TestApp::build().await;
    let res = app
        .send(auth_patch(
            "/api/chat/conversations/does-not-exist",
            json!({"title": "x"}),
        ))
        .await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn delete_unknown_id_is_204() {
    let app = TestApp::build().await;
    let res = app
        .send(auth_delete("/api/chat/conversations/does-not-exist"))
        .await;
    assert_eq!(res.status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn post_message_404_unknown_id() {
    let app = TestApp::build().await;
    let res = app
        .send(auth_post(
            "/api/chat/conversations/does-not-exist/messages",
            json!({"messages": [{"role": "user", "content": "hi"}]}),
        ))
        .await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}
