//! Concurrent POSTs against the same conversation: second returns 409.
//!
//! v3 rejects a second submit while a turn is in flight (deliberate
//! regression vs. v2's last-writer-wins, exchanged for a much simpler
//! state machine — see `docs/architecture/chat.md` §
//! Trade-off vs. v2).
//!
//! We exercise the rejection at the `TurnBus::try_start` layer directly:
//! a second `try_start` while a turn is in flight returns
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

use bytes::Bytes;

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

    // The POST handler calls `try_start` synchronously before returning
    // 202, so by now the turn is claimed. A second `try_start` against
    // the same conversation returns `AlreadyRunning` — exactly what the
    // HTTP handler maps to 409 `{"reason":"in_flight"}` without spawning a
    // second worker. Frame content is opaque (the claim is rejected before
    // it's buffered).
    let conv_id: RecordId = RecordId::from(("conversation", key.as_str()));
    let dummy_frame = Bytes::from_static(b"event: user_message\ndata: {}\n\n");
    let second = app.turn_bus.try_start(&conv_id, dummy_frame).await;
    assert!(
        second.is_err(),
        "second try_start while in-flight must be AlreadyRunning"
    );
}
