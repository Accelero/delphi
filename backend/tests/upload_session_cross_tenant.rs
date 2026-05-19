//! Engine-enforced cross-tenant + cross-user isolation on `upload_session`
//! and `ingestion_rejection`.
//!
//! Mirrors `cross_tenant_isolation.rs`: every operation goes through a
//! JWT-authenticated RECORD session, and SurrealDB PERMISSIONS clauses
//! refuse cross-tenant + cross-user access. The handler-side check is the
//! belt; this test exercises the suspenders.

mod common;

use jsonwebtoken::{encode, EncodingKey, Header};
use surrealdb::RecordId;

use delphi::storage::{JwtAccessConfig, JwtAccessKind, SystemDb};

const TEST_SECRET: &str = "test-only-secret-do-not-use-anywhere-real-please";

#[derive(serde::Deserialize)]
struct IdRow {
    id: RecordId,
}

struct World {
    system: SystemDb,
    ns: String,
    tenant_a: RecordId,
    tenant_b: RecordId,
    #[allow(dead_code)]
    alice: RecordId,
    #[allow(dead_code)]
    bob: RecordId,
    #[allow(dead_code)]
    carol: RecordId,
    /// alice's in-progress upload (tenant_a)
    upload_a: RecordId,
    /// bob's in-progress upload (tenant_b)
    upload_b: RecordId,
}

fn mint_jwt(iss: &str, sub: &str, ns: &str) -> String {
    let now = chrono::Utc::now().timestamp();
    let payload = serde_json::json!({
        "iss": iss,
        "sub": sub,
        "ac": "app_session",
        "ns": ns,
        "db": "main",
        "iat": now,
        "exp": now + 60,
    });
    encode(
        &Header::new(jsonwebtoken::Algorithm::HS512),
        &payload,
        &EncodingKey::from_secret(TEST_SECRET.as_bytes()),
    )
    .expect("sign HS512 test JWT")
}

async fn build_world(ns: &str) -> World {
    let system = SystemDb::in_memory(ns, "main").await.expect("connect");
    system.init_schema().await.expect("schema");
    system
        .define_jwt_access(&JwtAccessConfig {
            kind: JwtAccessKind::Hs512 {
                secret: TEST_SECRET.into(),
            },
            expected_issuer: None,
            expected_audience: None,
            session_duration_secs: Some(60),
        })
        .await
        .expect("define jwt access");

    let tenant_a = create_tenant(&system, "tenant-a", "Tenant A").await;
    let tenant_b = create_tenant(&system, "tenant-b", "Tenant B").await;
    let alice = create_user(
        &system,
        "https://idp.test/",
        "alice",
        "alice@a.test",
        &tenant_a,
    )
    .await;
    let bob = create_user(&system, "https://idp.test/", "bob", "bob@b.test", &tenant_b).await;
    // Carol is in tenant_a (same tenant as Alice) but a different user —
    // used to prove session ownership is per-user, not just per-tenant.
    let carol = create_user(
        &system,
        "https://idp.test/",
        "carol",
        "carol@a.test",
        &tenant_a,
    )
    .await;

    let upload_a = create_upload(&system, &tenant_a, &alice, "doc-alice", "k/alice").await;
    let upload_b = create_upload(&system, &tenant_b, &bob, "doc-bob", "k/bob").await;

    World {
        system,
        ns: ns.to_string(),
        tenant_a,
        tenant_b,
        alice,
        bob,
        carol,
        upload_a,
        upload_b,
    }
}

async fn create_tenant(system: &SystemDb, slug: &str, name: &str) -> RecordId {
    let mut r = system
        .raw()
        .query("CREATE tenant CONTENT { slug: $slug, name: $name } RETURN id")
        .bind(("slug", slug.to_string()))
        .bind(("name", name.to_string()))
        .await
        .unwrap();
    let row: Option<IdRow> = r.take(0).unwrap();
    row.unwrap().id
}

async fn create_user(
    system: &SystemDb,
    iss: &str,
    sub: &str,
    email: &str,
    tenant: &RecordId,
) -> RecordId {
    let mut r = system
        .raw()
        .query(
            "CREATE app_user CONTENT \
             { iss: $iss, sub: $sub, email: $email, tenant_id: $tid } \
             RETURN id",
        )
        .bind(("iss", iss.to_string()))
        .bind(("sub", sub.to_string()))
        .bind(("email", email.to_string()))
        .bind(("tid", tenant.clone()))
        .await
        .unwrap();
    let row: Option<IdRow> = r.take(0).unwrap();
    row.unwrap().id
}

async fn create_upload(
    system: &SystemDb,
    tenant: &RecordId,
    user: &RecordId,
    doc_id: &str,
    key: &str,
) -> RecordId {
    let mut r = system
        .raw()
        .query(
            "CREATE upload_session CONTENT { \
                tenant_id: $t, \
                user_id: $u, \
                doc_id: $d, \
                s3_key: $k, \
                s3_upload_id: 'mpu-123', \
                state: 'uploading', \
                canonical_id: $cid, \
                source_type: 'manual', \
                source_uri: $uri, \
                declared_size: 1000, \
                declared_content_type: 'application/pdf' \
             } RETURN id",
        )
        .bind(("t", tenant.clone()))
        .bind(("u", user.clone()))
        .bind(("d", doc_id.to_string()))
        .bind(("k", key.to_string()))
        .bind(("cid", format!("manual:{doc_id}")))
        .bind(("uri", format!("https://test/{doc_id}")))
        .await
        .unwrap();
    let row: Option<IdRow> = r.take(0).unwrap();
    row.unwrap().id
}

#[tokio::test]
async fn alice_cannot_read_bobs_upload_session() {
    let w = build_world("xtenant_upload_read").await;
    let token = mint_jwt("https://idp.test/", "alice", &w.ns);
    w.system
        .raw()
        .authenticate(&token)
        .await
        .expect("auth alice");

    let sessions: Vec<serde_json::Value> = w
        .system
        .raw()
        .query("SELECT doc_id FROM upload_session")
        .await
        .expect("query")
        .take(0)
        .expect("decode");

    assert_eq!(sessions.len(), 1, "alice sees only her own session");
    assert_eq!(sessions[0]["doc_id"], "doc-alice");

    let bob_row: Option<serde_json::Value> = w
        .system
        .raw()
        .query("SELECT * FROM $rid")
        .bind(("rid", w.upload_b.clone()))
        .await
        .expect("query by id")
        .take(0)
        .expect("decode");
    assert!(
        bob_row.is_none(),
        "alice cannot read bob's session by id; got {bob_row:?}"
    );
}

#[tokio::test]
async fn carol_cannot_read_alices_upload_same_tenant() {
    // Carol is in the SAME tenant as Alice — engine PERMISSIONS still
    // refuse the read because `user_id = $auth.id` is part of the rule.
    let w = build_world("xtenant_upload_same_tenant").await;
    let token = mint_jwt("https://idp.test/", "carol", &w.ns);
    w.system
        .raw()
        .authenticate(&token)
        .await
        .expect("auth carol");

    let sessions: Vec<serde_json::Value> = w
        .system
        .raw()
        .query("SELECT doc_id FROM upload_session")
        .await
        .expect("query")
        .take(0)
        .expect("decode");

    assert!(
        sessions.is_empty(),
        "same-tenant other-user cannot see another user's upload sessions; got {sessions:?}"
    );

    let alice_row: Option<serde_json::Value> = w
        .system
        .raw()
        .query("SELECT * FROM $rid")
        .bind(("rid", w.upload_a.clone()))
        .await
        .expect("query")
        .take(0)
        .expect("decode");
    assert!(
        alice_row.is_none(),
        "carol cannot read alice's session by id; got {alice_row:?}"
    );
}

#[tokio::test]
async fn alice_cannot_update_or_delete_bobs_upload_session() {
    let w = build_world("xtenant_upload_mutate").await;
    let token = mint_jwt("https://idp.test/", "alice", &w.ns);
    w.system
        .raw()
        .authenticate(&token)
        .await
        .expect("auth alice");

    // Attempt UPDATE — engine refuses (no-op).
    let _ = w
        .system
        .raw()
        .query("UPDATE $rid SET state = 'validating'")
        .bind(("rid", w.upload_b.clone()))
        .await;

    // Attempt DELETE — also refused.
    let _ = w
        .system
        .raw()
        .query("DELETE $rid")
        .bind(("rid", w.upload_b.clone()))
        .await;

    // Re-authenticate as bob: nothing changed.
    let bob_token = mint_jwt("https://idp.test/", "bob", &w.ns);
    w.system
        .raw()
        .authenticate(&bob_token)
        .await
        .expect("auth bob");

    let row: Option<serde_json::Value> = w
        .system
        .raw()
        .query("SELECT doc_id, state FROM $rid")
        .bind(("rid", w.upload_b.clone()))
        .await
        .expect("query")
        .take(0)
        .expect("decode");
    let v = row.expect("bob still has his session");
    assert_eq!(v["doc_id"], "doc-bob");
    assert_eq!(v["state"], "uploading", "state untouched");
}

#[tokio::test]
async fn alice_cannot_create_upload_session_in_bobs_tenant() {
    // Skip — engine fills tenant_id/user_id from $auth; an explicit
    // cross-tenant CREATE attempt cannot reuse another tenant's id
    // because the PERMISSIONS clause for create fires on the resolved
    // tenant_id, and that always defaults to $auth.tenant_id when the
    // caller omits it. We assert the engine clamps it.
    let w = build_world("xtenant_upload_create").await;
    let token = mint_jwt("https://idp.test/", "alice", &w.ns);
    w.system
        .raw()
        .authenticate(&token)
        .await
        .expect("auth alice");

    let _ = w
        .system
        .raw()
        .query(
            "CREATE upload_session CONTENT { \
                tenant_id: $t, \
                user_id: $u, \
                doc_id: 'sneaky', \
                s3_key: 'tenants/tenant-b/sneaky', \
                s3_upload_id: 'mpu-1', \
                state: 'uploading', \
                canonical_id: 'sneaky', \
                source_type: 'manual', \
                source_uri: 'https://x', \
                declared_size: 1, \
                declared_content_type: 'application/pdf' \
             }",
        )
        .bind(("t", w.tenant_b.clone()))
        .bind(("u", w.bob.clone()))
        .await;

    // Switch to bob: no 'sneaky' row visible.
    let bob_token = mint_jwt("https://idp.test/", "bob", &w.ns);
    w.system
        .raw()
        .authenticate(&bob_token)
        .await
        .expect("auth bob");
    let rows: Vec<serde_json::Value> = w
        .system
        .raw()
        .query("SELECT doc_id FROM upload_session WHERE doc_id = 'sneaky'")
        .await
        .expect("query")
        .take(0)
        .expect("decode");
    assert!(
        rows.is_empty(),
        "no 'sneaky' row should exist in bob's tenant; got {rows:?}"
    );
}

#[tokio::test]
async fn user_cannot_write_ingestion_rejection_directly() {
    // PERMISSIONS FOR create/update/delete WHERE FALSE — only SystemDb
    // (root) can write. A user-session create attempt yields nothing
    // and the row is not present afterwards.
    let w = build_world("xtenant_rejection_write").await;
    let token = mint_jwt("https://idp.test/", "alice", &w.ns);
    w.system
        .raw()
        .authenticate(&token)
        .await
        .expect("auth alice");

    let _ = w
        .system
        .raw()
        .query(
            "CREATE ingestion_rejection CONTENT { \
                tenant_id: $t, \
                user_id: $u, \
                doc_id: 'fake', \
                reason: 'fake-reason' \
             }",
        )
        .bind(("t", w.tenant_a.clone()))
        .bind(("u", w.alice.clone()))
        .await;

    let rows: Vec<serde_json::Value> = w
        .system
        .raw()
        .query("SELECT doc_id FROM ingestion_rejection WHERE doc_id = 'fake'")
        .await
        .expect("query")
        .take(0)
        .expect("decode");
    assert!(
        rows.is_empty(),
        "user-session writes to ingestion_rejection must be refused"
    );
}

#[tokio::test]
async fn alice_can_read_her_own_upload_session() {
    let w = build_world("xtenant_upload_happy").await;
    let token = mint_jwt("https://idp.test/", "alice", &w.ns);
    w.system
        .raw()
        .authenticate(&token)
        .await
        .expect("auth alice");

    let row: Option<serde_json::Value> = w
        .system
        .raw()
        .query("SELECT doc_id, state FROM $rid")
        .bind(("rid", w.upload_a.clone()))
        .await
        .expect("query")
        .take(0)
        .expect("decode");
    let v = row.expect("happy path");
    assert_eq!(v["doc_id"], "doc-alice");
    assert_eq!(v["state"], "uploading");
}
