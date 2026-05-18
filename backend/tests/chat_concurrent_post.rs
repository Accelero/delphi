//! Concurrent POSTs against the same conversation: second returns 409.
//!
//! v3 rejects a second submit while a turn is in flight (deliberate
//! regression vs. v2's last-writer-wins, exchanged for a much simpler
//! state machine — see `docs/architecture/chat-streaming.md` §
//! Trade-off vs. v2).
//!
//! We exercise the rejection at the `SessionState::start_turn` layer
//! directly: a second `start_turn` while a turn is in flight returns
//! `AlreadyRunning`, which the HTTP handler maps to 409 with
//! `reason: in_flight`. We can't reasonably drive a second HTTP POST
//! through the single-slot test pool while the worker is parked — see
//! the equivalent comment in `chat_stop.rs`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use surrealdb::RecordId;
use tokio_util::sync::CancellationToken;

use bytes::Bytes;

use common::{AuthRequestBuilder, TestApp};
use delphi::chat::TaskId;
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

struct ParkingLlm;

#[async_trait]
impl LlmClient for ParkingLlm {
    async fn stream_chat(&self, _messages: Vec<LlmMessage>) -> Result<DeltaStream> {
        let s = futures::stream::unfold((), |()| async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Some((Ok(LlmDelta::Text("never".into())), ()))
        });
        Ok(Box::pin(s))
    }
}

#[tokio::test]
async fn second_post_while_in_flight_returns_409() {
    let app = TestApp::build_with_llm(Arc::new(ParkingLlm)).await;
    let res = app
        .send(auth_post("/api/chat/conversations", json!({})))
        .await;
    assert_eq!(res.status, StatusCode::CREATED);
    let body: Value = res.json();
    let key = key_of(body["id"].as_str().expect("id"));

    let res = app
        .send(auth_post(
            &format!("/api/chat/conversations/{key}/messages"),
            json!({
                "id": "01HXY0000000000000000000A1",
                "text": "first",
                "parent_id": null,
            }),
        ))
        .await;
    assert_eq!(res.status, StatusCode::ACCEPTED);

    // Wait for the worker to claim the session.
    let conv_id: RecordId = RecordId::from(("conversation", key.as_str()));
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

    // A second `start_turn` against the same session returns
    // AlreadyRunning. The HTTP POST handler translates that into a 409
    // (with `reason: in_flight`) and never spawns a second worker.
    // Frame content is opaque here — the rejection happens before any
    // fanout so the bytes never leave the buffer.
    let dummy_frame = Bytes::from_static(b"event: user_message\ndata: {}\n\n");
    let err = session.start_turn(TaskId::new(), CancellationToken::new(), dummy_frame);
    assert!(
        err.is_err(),
        "second start_turn while in-flight must be rejected"
    );
}
