//! `commit_turn` "last writer wins" semantics.
//!
//! Two concurrent `commit_turn` calls against the same parent end with
//! exactly one user+assistant pair persisted. The transaction's leading
//! `DELETE message WHERE created_at > parent.created_at` step is what
//! provides that — whichever transaction commits second wipes the
//! first's pair before inserting its own.
//!
//! Drives the request-path Storage directly via the pool so the LLM /
//! HTTP layers don't interfere with the timing.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use std::time::Duration;

use common::{AuthRequestBuilder, TestApp};
use delphi::storage::Storage;

fn key_of(id_str: &str) -> String {
    id_str
        .split_once(':')
        .map(|(_, k)| k.to_string())
        .unwrap_or_else(|| id_str.to_string())
}

async fn create_conversation(app: &TestApp) -> surrealdb::RecordId {
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
    surrealdb::RecordId::from(("conversation", key_of(&id_str).as_str()))
}

#[tokio::test]
async fn first_turn_two_concurrent_commits_leave_one_pair() {
    let app = TestApp::build().await;
    let conv_id = create_conversation(&app).await;
    let bearer = AuthRequestBuilder::default().mint_jwt();

    // Single-slot in-memory pool (see common/mod.rs comment) — acquire
    // serialises, so the two "concurrent" commits run sequentially.
    // That's still a valid concurrency exercise: the DELETE step must
    // wipe whatever the first commit left behind. Run them via tasks
    // anyway so the test reads correctly.
    let pool1 = app.request_db_pool.clone();
    let bearer1 = bearer.clone();
    let conv1 = conv_id.clone();
    let pool2 = app.request_db_pool.clone();
    let bearer2 = bearer.clone();
    let conv2 = conv_id.clone();
    let t1 = tokio::spawn(async move {
        let db = pool1.acquire(&bearer1).await.expect("acquire1");
        db.commit_turn(
            &conv1,
            "01abctest1xxxxxxxxxxxxxxxx",
            "hello A",
            None,
            "reply A",
            &[],
        )
        .await
    });
    // Give task 1 a head start; even on a multi-slot pool the in-memory
    // engine serialises transactions, so the order is deterministic.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let t2 = tokio::spawn(async move {
        let db = pool2.acquire(&bearer2).await.expect("acquire2");
        db.commit_turn(
            &conv2,
            "01abctest2xxxxxxxxxxxxxxxx",
            "hello B",
            None,
            "reply B",
            &[],
        )
        .await
    });
    let r1 = t1.await.expect("join1");
    let r2 = t2.await.expect("join2");
    assert!(r1.is_ok(), "t1 commit error: {:?}", r1);
    assert!(r2.is_ok(), "t2 commit error: {:?}", r2);

    let db = app.request_db_pool.acquire(&bearer).await.expect("acquire");
    let msgs = db.list_messages(&conv_id).await.expect("list");
    assert_eq!(
        msgs.len(),
        2,
        "expected exactly one user+assistant pair; got {msgs:?}"
    );
    assert_eq!(msgs[0].role, "user");
    assert_eq!(msgs[1].role, "assistant");
    // The surviving pair is whichever committed second. Both are valid
    // outcomes of "last writer wins"; assert the pair is internally
    // consistent (user+assistant agree on the same turn).
    let user_a = msgs[0].content == "hello A" && msgs[1].content == "reply A";
    let user_b = msgs[0].content == "hello B" && msgs[1].content == "reply B";
    assert!(user_a || user_b, "mixed turn survived: {msgs:?}");

    // The assistant message links to the user message that committed
    // alongside it.
    assert_eq!(msgs[1].parent_id.as_ref(), msgs[0].id.as_ref());
}

#[tokio::test]
async fn second_turn_respects_parent_chain() {
    let app = TestApp::build().await;
    let conv_id = create_conversation(&app).await;
    let bearer = AuthRequestBuilder::default().mint_jwt();

    let db = app.request_db_pool.acquire(&bearer).await.expect("acquire");
    let asst1 = db
        .commit_turn(&conv_id, "01turn1aaaaaaaaaaaaaaaaaaa", "q1", None, "a1", &[])
        .await
        .expect("turn1");
    drop(db);
    // Drop drops the connection back to the pool asynchronously
    // (spawned task in AuthedDb::Drop). Give it a moment so the next
    // acquire doesn't fight for the slot.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let db = app.request_db_pool.acquire(&bearer).await.expect("acquire");
    let asst2 = db
        .commit_turn(
            &conv_id,
            "01turn2bbbbbbbbbbbbbbbbbbb",
            "q2",
            Some(&asst1),
            "a2",
            &[],
        )
        .await
        .expect("turn2");
    drop(db);
    tokio::time::sleep(Duration::from_millis(20)).await;

    let db = app.request_db_pool.acquire(&bearer).await.expect("acquire");
    let msgs = db.list_messages(&conv_id).await.expect("list");
    assert_eq!(msgs.len(), 4, "two turns × two messages: {msgs:?}");
    assert_eq!(msgs[0].content, "q1");
    assert_eq!(msgs[1].content, "a1");
    assert_eq!(msgs[2].content, "q2");
    assert_eq!(msgs[3].content, "a2");
    assert!(asst2.to_string().starts_with("message:"));
    // Parent chain runs user(q1) -> asst(a1) -> user(q2) -> asst(a2).
    assert_eq!(msgs[1].parent_id.as_ref(), msgs[0].id.as_ref());
    assert_eq!(msgs[2].parent_id.as_ref(), Some(&asst1));
    assert_eq!(msgs[3].parent_id.as_ref(), msgs[2].id.as_ref());
}

#[tokio::test]
async fn commit_turn_persists_and_returns_citations() {
    use delphi::storage::Citation;

    let app = TestApp::build().await;
    let conv_id = create_conversation(&app).await;
    let bearer = AuthRequestBuilder::default().mint_jwt();

    let citations = vec![
        Citation {
            n: 1,
            chunk_id: "chunk:abc".into(),
            doc_id: "document:xyz".into(),
            doc_title: Some("A Title".into()),
            page: Some(3),
        },
        Citation {
            n: 2,
            chunk_id: "chunk:def".into(),
            doc_id: "document:xyz".into(),
            doc_title: None,
            page: None,
        },
    ];

    let db = app.request_db_pool.acquire(&bearer).await.expect("acquire");
    db.commit_turn(
        &conv_id,
        "01citetestxxxxxxxxxxxxxxxx",
        "what does it say?",
        None,
        "it says things [1][2]",
        &citations,
    )
    .await
    .expect("commit");

    let msgs = db.list_messages(&conv_id).await.expect("list");
    assert_eq!(msgs.len(), 2, "user+assistant pair: {msgs:?}");
    // User message carries no citations.
    assert_eq!(msgs[0].role, "user");
    assert!(msgs[0].citations.is_none(), "user row must not carry citations");
    // Assistant message round-trips the full citation table.
    assert_eq!(msgs[1].role, "assistant");
    assert_eq!(
        msgs[1].citations.as_deref(),
        Some(citations.as_slice()),
        "assistant citations must round-trip exactly"
    );
}

#[tokio::test]
async fn commit_turn_with_no_citations_reads_back_none() {
    let app = TestApp::build().await;
    let conv_id = create_conversation(&app).await;
    let bearer = AuthRequestBuilder::default().mint_jwt();

    let db = app.request_db_pool.acquire(&bearer).await.expect("acquire");
    db.commit_turn(
        &conv_id,
        "01nocitetestxxxxxxxxxxxxxx",
        "no rag here",
        None,
        "plain answer",
        &[],
    )
    .await
    .expect("commit");

    let msgs = db.list_messages(&conv_id).await.expect("list");
    assert_eq!(msgs.len(), 2);
    // An empty citation slice stores NONE, not [], so it reads back None.
    assert!(
        msgs[1].citations.is_none(),
        "uncited assistant turn must read back as None, got {:?}",
        msgs[1].citations
    );
}
