//! Stop racing a commit must not produce a ghost-message inconsistency.
//!
//! The phase machine in `SessionState` (`enter_committing` flips
//! `Streaming → Committing`, `abort()` only emits `clear` when phase
//! is `Streaming`) closes the race.
//!
//! Integration-level shape asserted here:
//!
//! - If the worker reached commit (DB has both rows), a subsequent
//!   `session.abort()` is a no-op — must not drop the rows.
//!
//! The "Streaming-phase abort emits clear AND clears current" half of
//! the race lives in `chat_stop.rs::abort_during_in_flight_turn_...`
//! and in the session.rs unit test `abort_during_committing_does_not_emit_clear`.
//! Between the two we cover both phase-machine branches.
//!
//! We can't drive the HTTP `/stop` mid-turn on the single-slot test
//! pool — see the `chat_stop.rs` doc — so we invoke
//! `SessionState::abort` directly. That is the exact code path the
//! production handler reaches after its perm-check + drop.

mod common;

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use surrealdb::RecordId;

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
async fn abort_after_commit_does_not_drop_rows() {
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

    // Late abort: turn is done, `current` is None. `SessionState::abort`
    // must be a no-op for both the wire (no spurious `clear`) and the
    // DB (rows stay put). This is exactly the situation a /stop arriving
    // a few ms after commit would create.
    let session = app
        .sessions
        .lookup(&conv_id)
        .expect("session entry exists after a finished turn");
    session.abort();

    let res = app
        .send(auth_get(&format!("/api/chat/conversations/{key}")))
        .await;
    let body: Value = res.json();
    let msgs = body["messages"].as_array().expect("messages");
    assert_eq!(msgs.len(), 2, "late abort must not drop committed rows");
}
