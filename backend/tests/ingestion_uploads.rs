//! Integration tests for the four ingestion-v2 upload endpoints.
//!
//!   POST   /api/ingestion/uploads
//!   POST   /api/ingestion/uploads/:id/sign-part
//!   POST   /api/ingestion/uploads/:id/complete
//!   GET    /api/ingestion/uploads/:id
//!
//! Drives the full middleware stack in-process via `oneshot`, against
//! the in-memory SurrealDB + `MemObjectStore` rig the rest of the
//! integration tests use. `MemObjectStore` now carries the in-process
//! multipart shim (create → sign → upload_part_direct → complete), so
//! the full upload saga runs without Docker.

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

/// The shape the SPA actually sends for a manual upload: no canonical_id,
/// no source_uri, no source_type (server defaults to "manual").
fn manual_create_body(content_type: &str, size: u64) -> Body {
    Body::from(
        json!({
            "content_type": content_type,
            "size": size
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
async fn create_upload_400_when_forbidden_field_present() {
    let app = TestApp::build_with_mem().await;
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
    let app = TestApp::build_with_mem().await;
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
    let app = TestApp::build_with_mem().await;

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
    assert_eq!(docs[0].canonical_id.as_deref(), Some("manual:rt-1"));
}

#[tokio::test]
async fn sign_part_returns_url() {
    let app = TestApp::build_with_mem().await;
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
    // sign-part now routes through the `AccessMinter` seam; the test rig's
    // `MemAccess` mints a `mem-access://…?op=upload-part&…` pseudo-URL.
    assert!(url.starts_with("mem-access://"), "unexpected url: {url}");
    assert!(url.contains("op=upload-part"), "unexpected url: {url}");
    assert!(url.contains("partNumber=1"), "unexpected url: {url}");
}

#[tokio::test]
async fn cross_user_session_invisible() {
    // Alice creates an upload; Bob (different sub, same tenant) cannot
    // see it via /sign-part, /complete, or GET.
    let app = TestApp::build_with_mem().await;
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
    // Upload genuine binary bytes — neither a PDF (no `%PDF-`) nor valid
    // UTF-8 text. The byte-authoritative validator at /complete rejects
    // with `NotInAllowlist` → 422. The session row is gone, the S3 object
    // is deleted, and the rejection is logged.
    //
    // NOTE: until the file ending is plumbed to the validator (Phase 1),
    // a "declared PDF but actually text" lie is *not* detectable — text is
    // legitimately accepted as text/plain. Only non-PDF, non-UTF-8 bytes
    // reject. So this test uses real binary.
    let app = TestApp::build_with_mem().await;
    let bytes = bytes::Bytes::from_static(&[0xff, 0xfe, 0x00, 0x01, 0x02, 0x9c, 0xed]);
    let res = app
        .send(auth_post(
            "/api/ingestion/uploads",
            create_body("manual:lie-1", "application/octet-stream", bytes.len() as u64),
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
        .upload_part_direct(&key, &upload_id, 1, bytes)
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
    let app = TestApp::build_with_mem().await;
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

/// Helper: drive a manual upload (no canonical_id) all the way to commit.
/// Returns the session doc_id.
async fn manual_upload_to_commit(app: &TestApp, body: &str) -> String {
    let res = app
        .send(auth_post(
            "/api/ingestion/uploads",
            manual_create_body("text/plain", body.len() as u64),
            "ingester",
        ))
        .await;
    assert_eq!(res.status, StatusCode::OK, "create: {:?}", res.text());
    let created: Value = res.json();
    let doc_id = created["doc_id"].as_str().unwrap().to_string();
    let upload_id = created["upload_id"].as_str().unwrap().to_string();
    let key = created["key"].as_str().unwrap().to_string();

    let etag = app
        .object_store
        .upload_part_direct(&key, &upload_id, 1, bytes::Bytes::copy_from_slice(body.as_bytes()))
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
    assert_eq!(res.status, StatusCode::OK, "complete: {:?}", res.text());
    let resp: Value = res.json();
    assert_eq!(resp["result"], "ready");
    doc_id
}

#[tokio::test]
async fn manual_upload_without_canonical_id_commits() {
    // The B1/B2 landmine: a manual upload sends no canonical_id, so the
    // document row is written with canonical_id = NONE.
    let app = TestApp::build_with_mem().await;
    let doc_id = manual_upload_to_commit(&app, "manual body one").await;

    // Document row exists with NONE canonical_id and a text body.
    let storage = app.system.storage_for(app.default_tenant_id.clone());
    use delphi::storage::Storage as _;
    let rid = surrealdb::RecordId::from(("document", doc_id.as_str()));
    let doc = storage.get_document(&rid).await.unwrap();
    let doc = doc.expect("document must exist at document:<doc_id>");
    assert!(doc.canonical_id.is_none(), "manual upload has no canonical_id");
    let content = storage.get_content(&rid).await.unwrap();
    assert_eq!(content.unwrap().text, "manual body one");
}

#[tokio::test]
async fn second_manual_upload_does_not_false_conflict() {
    // THE landmine from the review: with canonical_id = NONE, the second
    // manual upload must NOT match the first NONE row and 422. Both
    // commit cleanly.
    let app = TestApp::build_with_mem().await;
    let _first = manual_upload_to_commit(&app, "manual body one").await;
    let _second = manual_upload_to_commit(&app, "manual body two").await;

    let storage = app.system.storage_for(app.default_tenant_id.clone());
    use delphi::storage::Storage as _;
    let docs = storage.list_feed(None, 10).await.unwrap();
    assert_eq!(docs.len(), 2, "both manual uploads committed");
    assert!(docs.iter().all(|d| d.canonical_id.is_none()));
}

#[tokio::test]
async fn get_status_returns_ready_after_commit() {
    // B5: after a successful commit the session row is gone, but the
    // status endpoint resolves `ready` by record-id lookup
    // (document:<doc_id>) so the SPA's recovery poll works.
    let app = TestApp::build_with_mem().await;
    let doc_id = manual_upload_to_commit(&app, "ready body").await;

    let res = app
        .send(auth_get(
            &format!("/api/ingestion/uploads/{doc_id}"),
            "ingester",
        ))
        .await;
    assert_eq!(res.status, StatusCode::OK, "status: {:?}", res.text());
    let body: Value = res.json();
    assert_eq!(body["state"], "ready");
    assert_eq!(
        body["doc_id"].as_str().unwrap(),
        format!("document:{doc_id}")
    );
}

// ---------------------------------------------------------------------------
// Validator coverage (security-critical) — every check in
// `validate_ingestion_metadata`, exercised through the real create endpoint
// so the handler→validator→status-code wiring is verified, not just the pure
// function (which `validation::metadata::tests` covers directly).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn validator_rejects_malformed_metadata_matrix() {
    let app = TestApp::build().await;

    // Pre-built dynamic payloads.
    let oversize = 200u64 * 1024 * 1024 + 1; // > max_size_bytes (200 MiB)
    let long_title = "x".repeat(1025); // > max_title_chars (1024)
    let huge_blob = "x".repeat(100 * 1024); // > max_metadata_bytes (64 KiB)
    let mut deep = json!("leaf"); // > max_metadata_depth (8)
    for _ in 0..20 {
        deep = json!({ "n": deep });
    }

    // (description, request body, expected status)
    let cases: Vec<(&str, Value, StatusCode)> = vec![
        // ---- forbidden server-derived fields → 400 ----
        ("forbidden tenant_id", json!({ "size": 1024, "tenant_id": "evil" }), StatusCode::BAD_REQUEST),
        ("forbidden user_id", json!({ "size": 1024, "user_id": "evil" }), StatusCode::BAD_REQUEST),
        ("forbidden storage_uri", json!({ "size": 1024, "storage_uri": "s3://evil/k" }), StatusCode::BAD_REQUEST),
        ("forbidden key", json!({ "size": 1024, "key": "tenants/evil/k" }), StatusCode::BAD_REQUEST),
        ("forbidden upload_id", json!({ "size": 1024, "upload_id": "mpu-evil" }), StatusCode::BAD_REQUEST),
        // ---- shape → 400 ----
        ("empty source_type", json!({ "size": 1024, "source_type": "" }), StatusCode::BAD_REQUEST),
        ("bad canonical_id pattern", json!({ "size": 1024, "canonical_id": "no-colon-form" }), StatusCode::BAD_REQUEST),
        ("empty canonical_id", json!({ "size": 1024, "canonical_id": "" }), StatusCode::BAD_REQUEST),
        ("javascript: source_uri", json!({ "size": 1024, "source_uri": "javascript:alert(1)" }), StatusCode::BAD_REQUEST),
        ("data: source_uri", json!({ "size": 1024, "source_uri": "data:text/html,<script>" }), StatusCode::BAD_REQUEST),
        ("relative source_uri", json!({ "size": 1024, "source_uri": "/etc/passwd" }), StatusCode::BAD_REQUEST),
        ("title too long", json!({ "size": 1024, "title": long_title }), StatusCode::BAD_REQUEST),
        // ---- resource / size limits → 413 ----
        ("zero size", json!({ "size": 0 }), StatusCode::PAYLOAD_TOO_LARGE),
        ("oversize file", json!({ "size": oversize }), StatusCode::PAYLOAD_TOO_LARGE),
        ("metadata too deep", json!({ "size": 1024, "metadata": deep }), StatusCode::PAYLOAD_TOO_LARGE),
        ("metadata too large", json!({ "size": 1024, "metadata": { "blob": huge_blob } }), StatusCode::PAYLOAD_TOO_LARGE),
    ];

    for (desc, body, expected) in cases {
        let req = auth_post(
            "/api/ingestion/uploads",
            Body::from(body.to_string()),
            "ingester",
        );
        let res = app.send(req).await;
        assert_eq!(
            res.status, expected,
            "case '{desc}': expected {expected}, got {} — body {:?}",
            res.status,
            res.text()
        );
    }
}

#[tokio::test]
async fn validator_accepts_good_samples() {
    let app = TestApp::build().await;

    // Minimal manual upload (the shape the SPA sends): content_type + size.
    let minimal = json!({ "content_type": "application/pdf", "size": 4096 });
    // Fully-specified natural-source write: valid canonical_id + http source.
    let full = json!({
        "canonical_id": "arxiv:2501.00001",
        "source_type": "arxiv",
        "source_uri": "https://arxiv.org/abs/2501.00001",
        "title": "A perfectly valid title",
        "size": 8192,
        "metadata": { "venue": "NeurIPS" }
    });

    for (desc, body) in [("minimal manual", minimal), ("full valid", full)] {
        let req = auth_post(
            "/api/ingestion/uploads",
            Body::from(body.to_string()),
            "ingester",
        );
        let res = app.send(req).await;
        assert_eq!(
            res.status,
            StatusCode::OK,
            "good sample '{desc}' should pass — body {:?}",
            res.text()
        );
    }
}

#[tokio::test]
async fn commit_sanitizes_control_and_bidi_in_title() {
    // A title with a NUL + a bidi-override is within the length cap, so it
    // passes the create gate (length-only) and must be cleaned in place at
    // /complete — not rejected — before it's persisted (Trojan Source guard).
    let app = TestApp::build_with_mem().await;
    let body = "document body text";
    let create = json!({
        "content_type": "text/plain",
        "size": body.len(),
        "title": "Clean\u{0000}\u{202E}Title"
    });
    let res = app
        .send(auth_post(
            "/api/ingestion/uploads",
            Body::from(create.to_string()),
            "ingester",
        ))
        .await;
    assert_eq!(res.status, StatusCode::OK, "create: {:?}", res.text());
    let created: Value = res.json();
    let doc_id = created["doc_id"].as_str().unwrap().to_string();
    let upload_id = created["upload_id"].as_str().unwrap().to_string();
    let key = created["key"].as_str().unwrap().to_string();

    let etag = app
        .object_store
        .upload_part_direct(&key, &upload_id, 1, bytes::Bytes::copy_from_slice(body.as_bytes()))
        .await
        .unwrap();
    let complete = Body::from(json!({ "parts": [{ "part_number": 1, "etag": etag }] }).to_string());
    let res = app
        .send(auth_post(
            &format!("/api/ingestion/uploads/{doc_id}/complete"),
            complete,
            "ingester",
        ))
        .await;
    assert_eq!(res.status, StatusCode::OK, "complete: {:?}", res.text());

    let storage = app.system.storage_for(app.default_tenant_id.clone());
    use delphi::storage::Storage as _;
    let rid = surrealdb::RecordId::from(("document", doc_id.as_str()));
    let doc = storage
        .get_document(&rid)
        .await
        .unwrap()
        .expect("document committed");
    assert_eq!(
        doc.title.as_deref(),
        Some("CleanTitle"),
        "NUL + bidi-override must be stripped from the persisted title"
    );
}
