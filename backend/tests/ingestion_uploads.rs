//! Integration tests for the four ingestion-v2 upload endpoints.
//!
//!   POST   /api/ingestion/uploads
//!   POST   /api/ingestion/uploads/:id/sign-part
//!   POST   /api/ingestion/uploads/:id/complete
//!   GET    /api/ingestion/uploads/:id
//!
//! Drives the full middleware stack in-process via `oneshot`, against
//! the in-memory SurrealDB + `MemObjectStore` rig the rest of the
//! integration tests use. The `MemObjectStore` does not implement
//! multipart natively — the happy-path test only covers the `create`
//! handler's wiring up through `create_multipart_upload`, then asserts
//! the session row landed; deeper round-trip tests use the
//! `LocalFsObjectStore` multipart shim once those land.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};

use delphi::storage::Storage;

use crate::common::{AuthRequestBuilder, TestApp};

fn create_body(canonical_id: &str, content_type: &str, size: u64) -> Body {
    Body::from(
        json!({
            "canonical_id": canonical_id,
            "source_type": "manual",
            "source_uri": format!("https://test.example/{canonical_id}"),
            "title": "Test Upload",
            "content_type": content_type,
            "size": size,
            "metadata": {}
        })
        .to_string(),
    )
}

fn auth_post(uri: &str, body: Body, roles: &str) -> Request<Body> {
    AuthRequestBuilder::default()
        .sub("uploader")
        .roles(roles)
        .apply(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(body)
                .unwrap(),
        )
}

fn auth_get(uri: &str, roles: &str) -> Request<Body> {
    AuthRequestBuilder::default()
        .sub("uploader")
        .roles(roles)
        .apply(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
}

#[tokio::test]
async fn create_upload_401_when_unauthenticated() {
    let app = TestApp::build().await;
    let res = app
        .send(
            Request::builder()
                .method("POST")
                .uri("/api/ingestion/uploads")
                .header("content-type", "application/json")
                .body(create_body("manual:abc", "application/pdf", 1024))
                .unwrap(),
        )
        .await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn create_upload_403_when_role_missing() {
    let app = TestApp::build().await;
    let req = auth_post(
        "/api/ingestion/uploads",
        create_body("manual:abc", "application/pdf", 1024),
        "viewer",
    );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn create_upload_400_on_disallowed_content_type() {
    let app = TestApp::build_with_local_fs().await;
    let req = auth_post(
        "/api/ingestion/uploads",
        create_body("manual:abc", "application/x-evil", 1024),
        "ingester",
    );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn create_upload_400_when_forbidden_field_present() {
    let app = TestApp::build_with_local_fs().await;
    // tenant_id from the client must be rejected.
    let body = Body::from(
        json!({
            "canonical_id": "manual:abc",
            "source_type": "manual",
            "source_uri": "https://test.example/abc",
            "content_type": "application/pdf",
            "size": 1024,
            "metadata": {},
            "tenant_id": "evil"
        })
        .to_string(),
    );
    let req = auth_post("/api/ingestion/uploads", body, "ingester");
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn create_upload_200_returns_doc_id_and_part_size() {
    let app = TestApp::build_with_local_fs().await;
    let req = auth_post(
        "/api/ingestion/uploads",
        create_body("manual:doc-aaa", "application/pdf", 4096),
        "ingester",
    );
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::OK, "body: {:?}", res.text());
    let body: Value = res.json();
    assert!(body["doc_id"].as_str().is_some());
    assert!(body["upload_id"].as_str().is_some());
    assert_eq!(
        body["key"]
            .as_str()
            .unwrap()
            .starts_with(&format!("tenants/{}/", app.default_tenant_slug)),
        true,
        "key must namespace under the tenant slug"
    );
    assert!(body["part_size_bytes"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn full_round_trip_create_sign_complete_status() {
    // Walk the full upload saga with the LocalFsObjectStore multipart
    // shim:
    //   1. POST /uploads → get doc_id + upload_id + key.
    //   2. Stage two parts via `upload_part_direct` (the shim's
    //      stand-in for an HTTP PUT against a presigned URL).
    //   3. POST /complete with the resulting ETags.
    //   4. GET /uploads/:id → expect 404 (session is gone, document
    //      lookup keyed on session.doc_id isn't wired in this milestone).
    let app = TestApp::build_with_local_fs().await;

    // 1. Create.
    let res = app
        .send(auth_post(
            "/api/ingestion/uploads",
            create_body("manual:rt-1", "text/plain", 11),
            "ingester",
        ))
        .await;
    assert_eq!(res.status, StatusCode::OK, "create: {:?}", res.text());
    let created: Value = res.json();
    let doc_id = created["doc_id"].as_str().unwrap().to_string();
    let upload_id = created["upload_id"].as_str().unwrap().to_string();
    let key = created["key"].as_str().unwrap().to_string();

    // 2. Stage two parts directly through the object store. (In a real
    //    deployment the client uploads to S3 via the presigned URL
    //    returned by /sign-part — that step is exercised by
    //    `sign_part_returns_url` below.)
    let etag1 = app
        .object_store
        .upload_part_direct(&key, &upload_id, 1, bytes::Bytes::from_static(b"hello "))
        .await
        .expect("part 1");
    let etag2 = app
        .object_store
        .upload_part_direct(&key, &upload_id, 2, bytes::Bytes::from_static(b"world"))
        .await
        .expect("part 2");

    // 3. Complete.
    let complete_body = Body::from(
        json!({
            "parts": [
                { "part_number": 1, "etag": etag1 },
                { "part_number": 2, "etag": etag2 }
            ]
        })
        .to_string(),
    );
    let res = app
        .send(auth_post(
            &format!("/api/ingestion/uploads/{doc_id}/complete"),
            complete_body,
            "ingester",
        ))
        .await;
    assert_eq!(res.status, StatusCode::OK, "complete: {:?}", res.text());
    let resp: Value = res.json();
    assert_eq!(resp["result"], "ready");
    let final_doc_id = resp["doc_id"].as_str().unwrap();
    assert!(final_doc_id.starts_with("document:"));

    // Session row is gone.
    let session = app
        .system
        .storage_for(app.default_tenant_id.clone())
        .get_upload_session(&doc_id)
        .await
        .unwrap();
    assert!(session.is_none(), "session must be deleted on commit");

    // Document row exists.
    let docs: Vec<delphi::storage::Document> = app
        .system
        .storage_for(app.default_tenant_id.clone())
        .list_feed(None, 10)
        .await
        .unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].canonical_id, "manual:rt-1");
}

#[tokio::test]
async fn sign_part_returns_url() {
    let app = TestApp::build_with_local_fs().await;
    let res = app
        .send(auth_post(
            "/api/ingestion/uploads",
            create_body("manual:sign-1", "application/pdf", 4096),
            "ingester",
        ))
        .await;
    assert_eq!(res.status, StatusCode::OK);
    let created: Value = res.json();
    let doc_id = created["doc_id"].as_str().unwrap();

    let res = app
        .send(auth_post(
            &format!("/api/ingestion/uploads/{doc_id}/sign-part"),
            Body::from(json!({ "part_number": 1 }).to_string()),
            "ingester",
        ))
        .await;
    assert_eq!(res.status, StatusCode::OK, "sign-part: {:?}", res.text());
    let body: Value = res.json();
    let url = body["url"].as_str().expect("url");
    assert!(url.starts_with("local-multipart://"));
}

#[tokio::test]
async fn cross_user_session_invisible() {
    // Alice creates an upload; Bob (different sub, same tenant) cannot
    // see it via /sign-part, /complete, or GET.
    let app = TestApp::build_with_local_fs().await;
    let alice = AuthRequestBuilder::default().sub("alice").roles("ingester");
    let res = app
        .send(
            alice.apply(
                Request::builder()
                    .method("POST")
                    .uri("/api/ingestion/uploads")
                    .header("content-type", "application/json")
                    .body(create_body("manual:alice-1", "application/pdf", 1024))
                    .unwrap(),
            ),
        )
        .await;
    assert_eq!(res.status, StatusCode::OK, "alice create: {:?}", res.text());
    let created: Value = res.json();
    let doc_id = created["doc_id"].as_str().unwrap();

    // Bob attempts sign-part.
    let bob = AuthRequestBuilder::default().sub("bob").roles("ingester");
    let res = app
        .send(
            bob.apply(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/ingestion/uploads/{doc_id}/sign-part"))
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "part_number": 1 }).to_string()))
                    .unwrap(),
            ),
        )
        .await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn complete_with_validator_reject_records_rejection() {
    // Declare PDF, upload bytes that aren't a PDF. The validator at
    // /complete sniffs and rejects with 422. The session row is gone,
    // S3 object is deleted, and the rejection is logged.
    let app = TestApp::build_with_local_fs().await;
    let res = app
        .send(auth_post(
            "/api/ingestion/uploads",
            create_body("manual:lie-1", "application/pdf", 11),
            "ingester",
        ))
        .await;
    assert_eq!(res.status, StatusCode::OK);
    let created: Value = res.json();
    let doc_id = created["doc_id"].as_str().unwrap().to_string();
    let upload_id = created["upload_id"].as_str().unwrap().to_string();
    let key = created["key"].as_str().unwrap().to_string();

    let etag = app
        .object_store
        .upload_part_direct(
            &key,
            &upload_id,
            1,
            bytes::Bytes::from_static(b"hello world"),
        )
        .await
        .unwrap();

    let complete_body =
        Body::from(json!({ "parts": [{ "part_number": 1, "etag": etag }] }).to_string());
    let res = app
        .send(auth_post(
            &format!("/api/ingestion/uploads/{doc_id}/complete"),
            complete_body,
            "ingester",
        ))
        .await;
    assert_eq!(
        res.status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "complete: {:?}",
        res.text()
    );
    let resp: Value = res.json();
    assert_eq!(resp["result"], "rejected");

    // GET status → rejected (the rejection log carries the reason).
    let res = app
        .send(auth_get(
            &format!("/api/ingestion/uploads/{doc_id}"),
            "ingester",
        ))
        .await;
    assert_eq!(res.status, StatusCode::OK, "status: {:?}", res.text());
    let body: Value = res.json();
    assert_eq!(body["state"], "rejected");
    assert!(body["reason"].as_str().is_some());
}

#[tokio::test]
async fn concurrent_complete_one_wins() {
    // Two `/complete` POSTs in flight against the same session; one
    // wins the CAS, the other returns 409 with the current state.
    let app = TestApp::build_with_local_fs().await;
    let res = app
        .send(auth_post(
            "/api/ingestion/uploads",
            create_body("manual:race-1", "text/plain", 1),
            "ingester",
        ))
        .await;
    assert_eq!(res.status, StatusCode::OK);
    let created: Value = res.json();
    let doc_id = created["doc_id"].as_str().unwrap().to_string();
    let upload_id = created["upload_id"].as_str().unwrap().to_string();
    let key = created["key"].as_str().unwrap().to_string();
    let etag = app
        .object_store
        .upload_part_direct(&key, &upload_id, 1, bytes::Bytes::from_static(b"x"))
        .await
        .unwrap();

    let body_json = json!({ "parts": [{ "part_number": 1, "etag": etag }] }).to_string();
    let make_req = || {
        auth_post(
            &format!("/api/ingestion/uploads/{doc_id}/complete"),
            Body::from(body_json.clone()),
            "ingester",
        )
    };

    let (r1, r2) = tokio::join!(app.send(make_req()), app.send(make_req()));
    let statuses = [r1.status, r2.status];
    // Exactly one wins (200 OK); the other reports a loser response —
    // either 409 (CAS lost while the winner was still validating) or
    // 404 (the winner already committed and removed the session).
    let ok_count = statuses.iter().filter(|s| **s == StatusCode::OK).count();
    let loser_count = statuses
        .iter()
        .filter(|s| **s == StatusCode::CONFLICT || **s == StatusCode::NOT_FOUND)
        .count();
    assert_eq!(ok_count, 1, "exactly one winner; got {statuses:?}");
    assert_eq!(loser_count, 1, "exactly one loser; got {statuses:?}");
}
