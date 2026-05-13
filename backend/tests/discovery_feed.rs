//! `GET /api/discovery/feed` + read-state mutations + SSE wiring,
//! end-to-end through the auth middleware.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};

use crate::common::{AuthRequestBuilder, TestApp};

fn ingest(app: &TestApp, canonical_id: &str) -> Body {
    let _ = app; // silence unused warning when called from non-method context
    Body::from(
        json!({
            "canonical_id": canonical_id,
            "source_type": "test",
            "source_uri": format!("https://test.example/{canonical_id}"),
            "title": format!("Title {canonical_id}"),
            "summary": "abstract here",
        })
        .to_string(),
    )
}

async fn seed(app: &TestApp, canonical_id: &str) {
    let req = AuthRequestBuilder::default()
        .sub("seeder")
        .roles("ingester")
        .apply(
            Request::builder()
                .method("POST")
                .uri("/api/ingestion/documents")
                .header("content-type", "application/json")
                .body(ingest(app, canonical_id))
                .unwrap(),
        );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::OK, "seed ingest");
}

/// Pull the bare record key out of a feed item's `id` field. The wire
/// shape for a SurrealDB record id depends on serializer settings; this
/// helper accepts both the string form (`"document:abc"`) and the
/// object form (`{"tb": "document", "id": {"String": "abc"}}` etc.).
fn doc_key(item: &Value) -> String {
    let id = &item["id"];
    if let Some(s) = id.as_str() {
        return s.strip_prefix("document:").unwrap_or(s).to_string();
    }
    // Object form: try common shapes.
    if let Some(inner) = id.get("id") {
        if let Some(s) = inner.as_str() {
            return s.to_string();
        }
        // {"id": {"String": "abc"}} etc.
        if let Some(obj) = inner.as_object() {
            for v in obj.values() {
                if let Some(s) = v.as_str() {
                    return s.to_string();
                }
            }
        }
    }
    panic!("could not extract key from id: {id:?}");
}

fn auth_get(uri: &str) -> Request<Body> {
    AuthRequestBuilder::default()
        .sub("alice")
        .apply(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
}

#[tokio::test]
async fn feed_401_when_unauthenticated() {
    let app = TestApp::build().await;
    let res = app
        .send(
            Request::builder()
                .method("GET")
                .uri("/api/discovery/feed")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn feed_returns_empty_list_when_no_documents() {
    let app = TestApp::build().await;
    let res = app.send(auth_get("/api/discovery/feed")).await;
    assert_eq!(res.status, StatusCode::OK);
    let body: Value = res.json();
    assert_eq!(body["items"].as_array().unwrap().len(), 0);
    assert!(body["next_cursor"].is_null());
}

#[tokio::test]
async fn feed_paginates_with_cursor() {
    let app = TestApp::build().await;
    for i in 0..5 {
        seed(&app, &format!("doc-{i}")).await;
    }

    // Page 1 (limit 2) — should return 2 items + a cursor.
    let res = app.send(auth_get("/api/discovery/feed?limit=2")).await;
    assert_eq!(res.status, StatusCode::OK);
    let p1: Value = res.json();
    assert_eq!(p1["items"].as_array().unwrap().len(), 2);
    let cursor = p1["next_cursor"]
        .as_str()
        .expect("page 1 yields a next_cursor")
        .to_string();

    // Page 2 with the cursor — distinct items, never overlapping page 1.
    let uri = format!("/api/discovery/feed?limit=2&cursor={cursor}");
    let res = app.send(auth_get(&uri)).await;
    let p2: Value = res.json();
    assert_eq!(p2["items"].as_array().unwrap().len(), 2);

    let p1_ids: Vec<&str> = p1["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["canonical_id"].as_str().unwrap())
        .collect();
    for item in p2["items"].as_array().unwrap() {
        let id = item["canonical_id"].as_str().unwrap();
        assert!(!p1_ids.contains(&id), "page 2 must not overlap page 1");
    }

    // Page 3 — final item, no further cursor.
    let cursor2 = p2["next_cursor"].as_str().unwrap();
    let res = app
        .send(auth_get(&format!(
            "/api/discovery/feed?limit=2&cursor={cursor2}"
        )))
        .await;
    let p3: Value = res.json();
    assert_eq!(p3["items"].as_array().unwrap().len(), 1);
    assert!(
        p3["next_cursor"].is_null(),
        "partial page → no next_cursor"
    );
}

#[tokio::test]
async fn feed_400_on_malformed_cursor() {
    let app = TestApp::build().await;
    let res = app
        .send(auth_get("/api/discovery/feed?cursor=garbage!!"))
        .await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn mark_read_then_unread_round_trip() {
    let app = TestApp::build().await;
    seed(&app, "doc-x").await;

    // First read: confirm unread.
    let res = app.send(auth_get("/api/discovery/feed")).await;
    let body: Value = res.json();
    let item = &body["items"][0];
    let key = doc_key(item);
    assert_eq!(item["read"], false);

    // Mark read.
    let req = AuthRequestBuilder::default().sub("alice").apply(
        Request::builder()
            .method("POST")
            .uri(format!("/api/discovery/items/{key}/read"))
            .body(Body::empty())
            .unwrap(),
    );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::NO_CONTENT);

    // Idempotent: second mark-read still 204.
    let req = AuthRequestBuilder::default().sub("alice").apply(
        Request::builder()
            .method("POST")
            .uri(format!("/api/discovery/items/{key}/read"))
            .body(Body::empty())
            .unwrap(),
    );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::NO_CONTENT);

    let res = app.send(auth_get("/api/discovery/feed")).await;
    let body: Value = res.json();
    assert_eq!(body["items"][0]["read"], true);

    // Mark unread.
    let req = AuthRequestBuilder::default().sub("alice").apply(
        Request::builder()
            .method("DELETE")
            .uri(format!("/api/discovery/items/{key}/read"))
            .body(Body::empty())
            .unwrap(),
    );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::NO_CONTENT);

    let res = app.send(auth_get("/api/discovery/feed")).await;
    let body: Value = res.json();
    assert_eq!(body["items"][0]["read"], false);
}

#[tokio::test]
async fn read_state_isolates_per_user() {
    let app = TestApp::build().await;
    seed(&app, "doc-x").await;

    // Alice reads the feed and gets the doc id.
    let res = app
        .send(
            AuthRequestBuilder::default()
                .sub("alice")
                .apply(
                    Request::builder()
                        .method("GET")
                        .uri("/api/discovery/feed")
                        .body(Body::empty())
                        .unwrap(),
                ),
        )
        .await;
    let body: Value = res.json();
    let key = doc_key(&body["items"][0]);
    let key = key.as_str();

    // Alice marks it read.
    let req = AuthRequestBuilder::default().sub("alice").apply(
        Request::builder()
            .method("POST")
            .uri(format!("/api/discovery/items/{key}/read"))
            .body(Body::empty())
            .unwrap(),
    );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::NO_CONTENT);

    // Bob reads the same feed — still unread for him.
    let res = app
        .send(
            AuthRequestBuilder::default()
                .sub("bob")
                .email("bob@delphi.test")
                .apply(
                    Request::builder()
                        .method("GET")
                        .uri("/api/discovery/feed")
                        .body(Body::empty())
                        .unwrap(),
                ),
        )
        .await;
    let body: Value = res.json();
    assert_eq!(body["items"][0]["read"], false, "bob should not see alice's read state");

    // Alice still sees it as read.
    let res = app
        .send(
            AuthRequestBuilder::default()
                .sub("alice")
                .apply(
                    Request::builder()
                        .method("GET")
                        .uri("/api/discovery/feed")
                        .body(Body::empty())
                        .unwrap(),
                ),
        )
        .await;
    let body: Value = res.json();
    assert_eq!(body["items"][0]["read"], true);
}

#[tokio::test]
async fn ingest_broadcasts_new_document_event() {
    // Validates the wiring: NotifyingSink → broadcast::Sender on
    // AppState → the SSE handler subscribes from the same channel.
    // The HTTP SSE response body is a long-lived stream that doesn't
    // play nicely with `oneshot()`, so we subscribe directly to the
    // shared channel and assert the event lands.
    let app = TestApp::build().await;
    let mut rx = app.events.subscribe();

    seed(&app, "broadcast-doc").await;

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("event should arrive within 1s")
        .expect("channel still open");
    assert_eq!(event.item.document.canonical_id, "broadcast-doc");
    assert_eq!(event.item.document.source_type, "test");
    assert!(!event.item.read, "newly created doc cannot be read");
}
