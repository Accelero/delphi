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
async fn post_message_streams_and_persists_pair_with_title() {
    let app = TestApp::build().await;
    let id = create_one(&app).await;
    let key = key_of(&id);

    let uri = format!("/api/chat/conversations/{key}/messages");
    let res = app
        .send(auth_post(
            &uri,
            json!({
                "id": "01HXY0000000000000000000ZZ",
                "text": "Hello",
                "parent_id": null,
            }),
        ))
        .await;
    // POST returns 200 with the AI SDK stream as its body. The
    // FakeLlm default emits "ok" synchronously, then the worker
    // commits and emits the trailing `d:` frame; the response body
    // ends at that point so `app.send` returns the full bytes.
    assert_eq!(res.status, StatusCode::OK, "{}", res.text());
    let body = res.text();
    assert!(body.starts_with("8:"), "first frame should be task: {body}");
    assert!(body.contains("0:\"ok\""), "expected text delta in: {body}");
    assert!(
        body.contains("\"finishReason\":\"stop\""),
        "expected finish frame in: {body}"
    );

    // The worker commits before emitting `d:`, so by the time the
    // response body has ended both messages + auto-title are persisted.
    let res = app
        .send(auth_get(&format!("/api/chat/conversations/{key}")))
        .await;
    assert_eq!(res.status, StatusCode::OK);
    let body: Value = res.json();
    let msgs = body["messages"].as_array().cloned().unwrap_or_default();
    assert_eq!(msgs.len(), 2, "expected exactly two messages: {body:?}");
    assert_eq!(msgs[0]["role"], "user");
    assert_eq!(msgs[0]["content"], "Hello");
    assert_eq!(msgs[0]["id"], "message:01HXY0000000000000000000ZZ");
    assert_eq!(msgs[1]["role"], "assistant");
    assert_eq!(msgs[1]["content"], "ok");
    assert_eq!(msgs[1]["parent_id"], "message:01HXY0000000000000000000ZZ");
    assert!(
        body["conversation"]["title"].as_str().is_some(),
        "auto-title should be set",
    );
}

#[tokio::test]
async fn post_with_stale_parent_returns_409() {
    let app = TestApp::build().await;
    let id = create_one(&app).await;
    let key = key_of(&id);
    // No messages yet — sending a non-null parent_id must 409.
    let res = app
        .send(auth_post(
            &format!("/api/chat/conversations/{key}/messages"),
            json!({
                "id": "01HXY0000000000000000000AA",
                "text": "hi",
                "parent_id": "message:nonexistent",
            }),
        ))
        .await;
    assert_eq!(res.status, StatusCode::CONFLICT);
    let body: Value = res.json();
    assert_eq!(body["reason"], "stale_parent");

    // And the conversation must remain empty — no partial write.
    let res = app
        .send(auth_get(&format!("/api/chat/conversations/{key}")))
        .await;
    let body: Value = res.json();
    let msgs = body["messages"].as_array().cloned().unwrap_or_default();
    assert!(msgs.is_empty(), "stale-parent POST must persist nothing");
}

#[tokio::test]
async fn post_with_garbage_message_id_returns_400() {
    let app = TestApp::build().await;
    let id = create_one(&app).await;
    let key = key_of(&id);
    let res = app
        .send(auth_post(
            &format!("/api/chat/conversations/{key}/messages"),
            json!({"id": "too-short", "text": "hi", "parent_id": null}),
        ))
        .await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn list_messages_round_trips_parent_id_field() {
    // After a turn commits, the assistant message's `parent_id` field
    // should land on the wire as `"message:<key>"`.
    let app = TestApp::build().await;
    let id = create_one(&app).await;
    let key = key_of(&id);
    let res = app
        .send(auth_post(
            &format!("/api/chat/conversations/{key}/messages"),
            json!({
                "id": "01HXY0000000000000000000BB",
                "text": "hi",
                "parent_id": null,
            }),
        ))
        .await;
    assert_eq!(res.status, StatusCode::OK);

    let res = app
        .send(auth_get(&format!("/api/chat/conversations/{key}")))
        .await;
    let body: Value = res.json();
    let msgs = body["messages"].as_array().expect("messages");
    assert_eq!(msgs.len(), 2);
    assert!(msgs[0]["parent_id"].is_null(), "user msg parent is null");
    let asst_parent = msgs[1]["parent_id"]
        .as_str()
        .expect("assistant parent_id is a string");
    assert_eq!(asst_parent, "message:01HXY0000000000000000000BB");
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

    // Post a message so we have something to cascade. The POST body
    // is the worker's stream — `app.send` collects it to completion,
    // by which point `commit_turn` has already persisted both rows.
    let _ = app
        .send(auth_post(
            &format!("/api/chat/conversations/{key}/messages"),
            json!({
                "id": "01HXY0000000000000000000CC",
                "text": "hi",
                "parent_id": null,
            }),
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
            json!({
                "id": "01HXY0000000000000000000DD",
                "text": "hi",
                "parent_id": null,
            }),
        ))
        .await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}
