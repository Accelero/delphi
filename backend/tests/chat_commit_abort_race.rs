//! Stop racing a commit must not produce a ghost-message inconsistency.
//!
//! v4 makes this structural, not a phase machine (§8): the worker is the
//! single writer of the turn's stream, so the terminal frame is `clear`
//! XOR `finish` — never both, and a committed turn always ends in
//! `finish`. A `/stop` that arrives after the worker already broke on EOF
//! is a no-op: the token is never re-checked, the turn commits, and the
//! wire shows `finish`.
//!
//! Asserted here, end to end: after a turn has committed both rows, a
//! late `TurnBus::cancel` (the `/stop` code path) neither drops the rows
//! nor appends a `clear` — the lingering log still ends in `finish`.

mod common;

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures::StreamExt;
use serde_json::{json, Value};
use surrealdb::RecordId;

use common::{AuthRequestBuilder, TestApp};
use delphi::chat::Cursor;

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
    id.split_once(':').map(|(_, k)| k.to_string()).unwrap_or_default()
}

#[tokio::test]
async fn late_stop_after_commit_keeps_rows_and_shows_finish_not_clear() {
    let app = TestApp::build().await; // FakeLlm: one "ok" delta, EOF.
    let res = app
        .send(auth_post("/api/chat/conversations", json!({})))
        .await;
    assert_eq!(res.status, StatusCode::CREATED);
    let body: Value = res.json();
    let key = key_of(body["id"].as_str().expect("id"));
    let conv_id: RecordId = RecordId::from(("conversation", key.as_str()));

    let res = app
        .send(auth_post(
            &format!("/api/chat/conversations/{key}/messages"),
            json!({
                "id": "01HXY0000000000000000000RC",
                "text": "hi",
                "parent_id": null,
            }),
        ))
        .await;
    assert_eq!(res.status, StatusCode::ACCEPTED);

    // Poll until the turn has committed both rows.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let res = app
            .send(auth_get(&format!("/api/chat/conversations/{key}")))
            .await;
        let body: Value = res.json();
        let msgs = body["messages"].as_array().cloned().unwrap_or_default();
        if msgs.len() == 2 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("turn never committed: {body:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Late stop: the turn is done (token already cleared at `terminate`),
    // so `cancel` is a structural no-op. This is the situation a `/stop`
    // arriving a few ms after commit creates.
    app.turn_bus.cancel(&conv_id).await;

    // Resume from the turn's start (cursor 0): the lingering log must end
    // in `finish`, with no `clear` anywhere — clear XOR finish (§8).
    let c0: Cursor = "0".parse().expect("cursor");
    let mut stream = app.turn_bus.subscribe(&conv_id, Some(c0)).await;
    let mut acc = String::new();
    while let Ok(Some(b)) =
        tokio::time::timeout(Duration::from_millis(200), stream.next()).await
    {
        acc.push_str(&String::from_utf8_lossy(&b));
    }
    assert!(
        acc.contains("event: finish"),
        "committed turn must end in finish; got: {acc:?}"
    );
    assert!(
        !acc.contains("event: clear"),
        "a committed turn must never emit clear; got: {acc:?}"
    );

    // Rows survive the late stop.
    let res = app
        .send(auth_get(&format!("/api/chat/conversations/{key}")))
        .await;
    let body: Value = res.json();
    let msgs = body["messages"].as_array().expect("messages");
    assert_eq!(msgs.len(), 2, "late stop must not drop committed rows");
}
