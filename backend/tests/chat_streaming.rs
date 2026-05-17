//! End-to-end POST /messages: the response body IS the AI SDK stream.
//!
//! Drives one turn through the in-process router via
//! `tower::ServiceExt::oneshot`, asserts the frame order on the wire,
//! and asserts the DB has the persisted user+assistant pair.
//!
//! The detailed RAG-citations ordering lives in `rag_retrieval.rs`;
//! this test exercises the bare path with a vanilla FakeLlm so the
//! frame-protocol assertions are stable.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};

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

#[tokio::test]
async fn post_emits_task_then_text_then_finish_and_persists_pair() {
    let app = TestApp::build().await;
    let key = create_conversation(&app).await;
    let user_id = "01HXY0000000000000000000FF";

    let res = app
        .send(auth_post(
            &format!("/api/chat/conversations/{key}/messages"),
            json!({"id": user_id, "text": "hi", "parent_id": null}),
        ))
        .await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.text());

    let body = res.text();
    let mut lines = body.lines();

    // 1) first frame must be `8:` with a 26-char task id.
    let task_line = lines.next().expect("task frame");
    assert!(task_line.starts_with("8:"), "first frame must be task: {task_line}");
    let task_json: Value = serde_json::from_str(&task_line[2..]).expect("task is json");
    let task_id = task_json["taskId"].as_str().expect("taskId field");
    assert_eq!(task_id.len(), 26, "taskId should be a 26-char ULID");

    // 2..n) one or more `0:` text deltas before a single `d:` finish.
    let mut saw_text = false;
    let mut finish_line: Option<&str> = None;
    for line in lines {
        if line.starts_with("0:") {
            saw_text = true;
        } else if line.starts_with("d:") {
            finish_line = Some(line);
            break;
        }
    }
    assert!(saw_text, "expected at least one `0:` text delta: {body}");
    let finish = finish_line.expect("finish frame: {body}");
    let finish_json: Value = serde_json::from_str(&finish[2..]).expect("finish is json");
    assert_eq!(finish_json["finishReason"], "stop");
    let asst_id = finish_json["assistantMessageId"]
        .as_str()
        .expect("assistantMessageId field");
    assert!(asst_id.starts_with("message:"), "asst id is record id: {asst_id}");

    // After the body ends the pair is persisted (commit_turn ran before
    // the `d:` frame).
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
