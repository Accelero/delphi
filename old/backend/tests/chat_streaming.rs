//! End-to-end POST /messages + GET /stream: the SSE stream is the
//! single source of truth.
//!
//! Subscribes to the per-conversation SSE stream first, then POSTs the
//! turn, then asserts the frame order on the wire. After `finish` the
//! DB has the committed user+assistant pair.

mod common;

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use futures::StreamExt;
use http_body_util::BodyStream;
use serde_json::{json, Value};
use tower::ServiceExt;

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

fn key_of(id: &str) -> String {
    id.split_once(':').map(|(_, k)| k.to_string()).unwrap_or_default()
}

async fn create_conversation(app: &TestApp) -> String {
    let res = app.send(auth_post("/api/chat/conversations", json!({}))).await;
    assert_eq!(res.status, StatusCode::CREATED);
    let body: Value = res.json();
    key_of(body["id"].as_str().expect("id"))
}

/// One SSE event parsed out of the wire bytes.
#[derive(Debug)]
pub struct SseFrame {
    pub event: String,
    pub data: String,
}

/// Read `text/event-stream` bytes from a router subscription, returning
/// parsed frames until either `until_event` arrives or `timeout` elapses.
pub async fn read_until(
    router: axum::Router,
    uri: &str,
    until_event: &str,
    timeout: Duration,
) -> Vec<SseFrame> {
    let req = AuthRequestBuilder::default().apply(
        Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .unwrap(),
    );
    let res = router.oneshot(req).await.expect("router oneshot");
    assert_eq!(res.status(), StatusCode::OK, "stream open");

    let mut stream = BodyStream::new(res.into_body());
    let mut buf = Vec::<u8>::new();
    let mut frames = Vec::new();

    let read = async {
        while let Some(chunk) = stream.next().await {
            let frame = chunk.expect("chunk");
            let data = frame.data_ref().cloned().unwrap_or_else(Bytes::new);
            if data.is_empty() {
                continue;
            }
            buf.extend_from_slice(&data);
            while let Some(end) = find_event_end(&buf) {
                let block = std::str::from_utf8(&buf[..end])
                    .expect("sse utf-8")
                    .to_string();
                buf.drain(..end + 2); // consume "\n\n"
                if let Some(parsed) = parse_sse_block(&block) {
                    let stop = parsed.event == until_event;
                    frames.push(parsed);
                    if stop {
                        return;
                    }
                }
            }
        }
    };

    let _ = tokio::time::timeout(timeout, read).await;
    frames
}

/// Find the byte index of `\n\n` in `buf` (returns index of the first `\n`).
fn find_event_end(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\n\n")
}

fn parse_sse_block(s: &str) -> Option<SseFrame> {
    let mut event: Option<String> = None;
    let mut data = String::new();
    for line in s.split('\n') {
        if let Some(rest) = line.strip_prefix("event: ") {
            event = Some(rest.to_string());
        } else if let Some(rest) = line.strip_prefix("data: ") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest);
        }
    }
    event.map(|e| SseFrame { event: e, data })
}

#[tokio::test]
async fn sse_emits_user_message_text_finish_and_persists_pair() {
    let app = TestApp::build().await;
    let key = create_conversation(&app).await;
    let user_id = "01HXY0000000000000000000FF";

    // Open the SSE subscription FIRST so the worker's emit fans out to
    // a registered subscriber rather than just into the buffer.
    let stream_router = app.router.clone();
    let stream_uri = format!("/api/chat/conversations/{key}/stream");
    let stream_task = tokio::spawn(async move {
        read_until(stream_router, &stream_uri, "finish", Duration::from_secs(5)).await
    });

    // Give the GET a head start so its subscribe lands before start_turn.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let res = app
        .send(auth_post(
            &format!("/api/chat/conversations/{key}/messages"),
            json!({"id": user_id, "text": "hi", "parent_id": null}),
        ))
        .await;
    assert_eq!(res.status, StatusCode::ACCEPTED, "{}", res.text());

    let frames = stream_task.await.expect("stream task");

    // Frame ordering: user_message → text* → finish.
    assert!(!frames.is_empty(), "expected SSE frames");
    let names: Vec<&str> = frames.iter().map(|f| f.event.as_str()).collect();
    assert_eq!(names.first(), Some(&"user_message"), "first frame: {names:?}");
    assert_eq!(names.last(), Some(&"finish"), "last frame: {names:?}");
    assert!(names.iter().any(|n| *n == "text"), "no text frames: {names:?}");

    // user_message body: { id: "message:<ulid>", content: "hi" }
    let first: Value = serde_json::from_str(&frames[0].data).expect("user_message json");
    assert_eq!(first["id"], format!("message:{user_id}"));
    assert_eq!(first["content"], "hi");

    // finish body: { finishReason: "stop", assistantMessageId: "message:..." }
    let last: Value = serde_json::from_str(&frames.last().unwrap().data).expect("finish json");
    assert_eq!(last["finishReason"], "stop");
    let asst_id = last["assistantMessageId"]
        .as_str()
        .expect("assistantMessageId");
    assert!(asst_id.starts_with("message:"));

    // Persisted pair.
    let res = app
        .send(auth_get(&format!("/api/chat/conversations/{key}")))
        .await;
    let body: Value = res.json();
    let msgs = body["messages"].as_array().expect("messages");
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0]["id"], format!("message:{user_id}"));
    assert_eq!(msgs[1]["id"], asst_id);
    assert_eq!(msgs[1]["parent_id"], format!("message:{user_id}"));
}
