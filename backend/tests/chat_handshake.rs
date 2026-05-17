//! Race a worker commit against a fresh `GET /conversations/{id}` and
//! assert the handshake never observes a duplicate or a dropped message.
//!
//! Background: between the worker's "append `proto::finish`" and "write
//! assistant message to DB + clear the buffer" steps the reply lives in
//! exactly two places successively (buffer, then DB). If the GET response
//! were allowed to run inside that window we could see:
//!
//! - duplicate: assistant message in DB *and* in the next stream replay
//!   (would be ok only if the GET also drained the buffer, which it
//!   doesn't), or
//! - drop: GET sees the buffer cleared but DB write hasn't landed yet —
//!   the reply is invisible.
//!
//! The `finalize_lock` in `conversations::get` serialises the GET against
//! the worker's commit so we always observe one of two consistent points
//! in time:
//!
//!  - before commit ⇒ history has the user message, no assistant yet,
//!  - after commit ⇒ history has both, no duplicates.
//!
//! The test fires the POST (which spawns the worker), then immediately
//! enters a `tokio::join!` racing several concurrent GETs while the
//! worker is in flight. We assert every GET response is one of the two
//! valid states above, then wait for the final state.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use std::time::Duration;

use common::{AuthRequestBuilder, TestApp};

fn auth_get(uri: &str) -> Request<Body> {
    AuthRequestBuilder::default().apply(
        Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .unwrap(),
    )
}

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

/// Strip `conversation:` to recover the routed key.
fn key_of(id: &surrealdb::RecordId) -> String {
    id.to_string()
        .split_once(':')
        .map(|(_, k)| k.to_string())
        .unwrap_or_default()
}

async fn create_one(app: &TestApp) -> surrealdb::RecordId {
    let req = AuthRequestBuilder::default().apply(
        Request::builder()
            .method("POST")
            .uri("/api/chat/conversations")
            .header("content-type", "application/json")
            .body(Body::from("{}".to_string()))
            .unwrap(),
    );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::CREATED, "{}", res.text());
    let body: Value = res.json();
    let id_str = body["id"].as_str().expect("id").to_string();
    let (_, key) = id_str.split_once(':').expect("id has table prefix");
    surrealdb::RecordId::from(("conversation", key))
}

/// Two valid handshake observations:
/// - state A: only the user message persisted (worker still streaming),
/// - state B: both messages persisted (worker committed).
/// Anything else is a bug — duplicate user, duplicate assistant, missing
/// either, or extra rows.
fn assert_consistent(messages: &[Value]) {
    match messages.len() {
        1 => {
            assert_eq!(messages[0]["role"], "user", "lone msg should be user");
            assert_eq!(messages[0]["content"], "ping");
        }
        2 => {
            assert_eq!(messages[0]["role"], "user");
            assert_eq!(messages[0]["content"], "ping");
            assert_eq!(messages[1]["role"], "assistant");
            // Default FakeLlmClient emits "ok".
            assert_eq!(messages[1]["content"], "ok");
        }
        other => panic!(
            "unexpected message count {other}: {:?}",
            messages.iter().collect::<Vec<_>>()
        ),
    }
}

#[tokio::test]
async fn racing_get_during_worker_never_sees_inconsistent_state() {
    let app = TestApp::build().await;
    let id = create_one(&app).await;
    let key = key_of(&id);

    // Submit the user message; POST returns 202 once the worker is
    // dispatched. The worker runs detached.
    let res = app
        .send(auth_post(
            &format!("/api/chat/conversations/{key}/messages"),
            json!({
                "messages": [{ "role": "user", "content": "ping" }],
            }),
        ))
        .await;
    assert_eq!(res.status, StatusCode::ACCEPTED, "{}", res.text());

    // Hammer the GET while the worker is (likely) still in flight.
    // FakeLlmClient yields synchronously so the worker is very fast, but
    // the race window covers the DB-commit step regardless. Running many
    // concurrent GETs maximises the chance of catching the window.
    let path = format!("/api/chat/conversations/{key}");
    let mut handles = Vec::new();
    for _ in 0..32 {
        let app = app.router.clone();
        let path = path.clone();
        handles.push(tokio::spawn(async move {
            use tower::ServiceExt;
            let req = auth_get(&path);
            let res = app.oneshot(req).await.expect("oneshot");
            let status = res.status();
            let bytes = http_body_util::BodyExt::collect(res.into_body())
                .await
                .expect("collect")
                .to_bytes();
            (status, bytes)
        }));
    }

    for h in handles {
        let (status, bytes) = h.await.expect("join");
        assert_eq!(status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&bytes).expect("json");
        let msgs = body["messages"]
            .as_array()
            .cloned()
            .expect("messages array");
        assert_consistent(&msgs);
    }

    // Wait for the worker's commit to land in DB and final state to be the
    // two-message reply. Bounded poll so a real bug shows up as a failure
    // not a hang.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let req = auth_get(&path);
        let res = app.send(req).await;
        let body: Value = res.json();
        let msgs = body["messages"].as_array().cloned().unwrap_or_default();
        if msgs.len() == 2 && msgs[1]["role"] == "assistant" && msgs[1]["content"] == "ok" {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("worker never committed: {body:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn explicit_finalize_lock_blocks_get() {
    // White-box: take the registry's finalize_lock directly and verify a
    // GET sits on it. This is the same invariant the racy test relies on,
    // pinned to a deterministic wait so a regression on the handshake
    // wiring is loud.
    let app = TestApp::build().await;
    let id = create_one(&app).await;
    let key = key_of(&id);

    // Materialise the SessionState directly via the app's registry, then
    // hold finalize_lock for a known duration.
    let session = app
        .session_registry
        .get_or_create(&id)
        .await;
    let started = std::time::Instant::now();
    let hold = Duration::from_millis(150);

    let session_hold = session.clone();
    let lock_task = tokio::spawn(async move {
        let _g = session_hold.lock_finalize().await;
        tokio::time::sleep(hold).await;
    });
    // Give the lock task a head start to actually acquire.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // GET must block until the lock is released.
    let req = auth_get(&format!("/api/chat/conversations/{key}"));
    let res = app.send(req).await;
    let elapsed = started.elapsed();
    assert_eq!(res.status, StatusCode::OK);
    assert!(
        elapsed >= hold,
        "GET returned too fast ({elapsed:?}); finalize_lock wasn't honoured"
    );
    lock_task.await.expect("lock task");
}
