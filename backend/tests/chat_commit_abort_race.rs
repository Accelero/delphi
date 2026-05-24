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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures::StreamExt;
use serde_json::{json, Value};
use surrealdb::types::RecordId;
use tokio::sync::Notify;

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
    let conv_id: RecordId = RecordId::new("conversation", key.as_str());

    // Subscribe BEFORE the turn and drain concurrently. Under refcount an
    // unsubscribed session is freed the instant the worker's handle drops
    // at `terminate`, so a *post-hoc* resume from cursor 0 would resync, not
    // replay the finish (§4.1). A live subscriber — exactly what an open tab
    // is — both holds the session alive and observes the wire as it happens,
    // which is where the clear-XOR-finish guarantee must hold.
    let stream = app.turn_bus.subscribe(&conv_id, None).await;
    let acc = Arc::new(Mutex::new(String::new()));
    let finish_seen = Arc::new(Notify::new());
    let drain = {
        let acc = acc.clone();
        let finish_seen = finish_seen.clone();
        tokio::spawn(async move {
            let mut stream = stream;
            while let Some(b) = stream.next().await {
                let mut g = acc.lock().unwrap();
                g.push_str(&String::from_utf8_lossy(&b));
                let seen = g.contains("event: finish");
                drop(g);
                if seen {
                    finish_seen.notify_one();
                }
            }
        })
    };

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

    // The worker emits `finish` only after committing both rows, so seeing
    // it on the wire means the turn is fully committed.
    tokio::time::timeout(Duration::from_secs(3), finish_seen.notified())
        .await
        .expect("turn must finish (and commit) on the wire");

    // Late stop after commit: the token was cleared at `terminate`, so
    // `cancel` is a structural no-op. This is the situation a `/stop`
    // arriving a few ms after commit creates — it must not append `clear`.
    app.turn_bus.cancel(&conv_id).await;
    // Give any (forbidden) `clear` a window to surface on the live stream.
    tokio::time::sleep(Duration::from_millis(300)).await;
    drain.abort();

    let acc = acc.lock().unwrap().clone();
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
