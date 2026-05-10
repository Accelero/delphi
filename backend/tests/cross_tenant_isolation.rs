//! Engine-enforced cross-tenant isolation.
//!
//! Validates the Phase 2 stack end-to-end at the SurrealDB engine level:
//!
//!   schema PERMISSIONS + DEFINE ACCESS RECORD WITH JWT
//!     + backend mints HS512 JWT from AuthContext
//!     + db.authenticate(jwt) switches the session to a record user
//!     → engine refuses every cross-tenant operation
//!
//! These tests bypass the application-layer Storage trait on purpose —
//! we want to prove the engine itself enforces, not the application
//! discipline (which Phase 1 already covered).

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
    alice: RecordId, // app_user in tenant_a
    #[allow(dead_code)]
    bob: RecordId, // app_user in tenant_b — referenced only by the mint_jwt sub name
    doc_a: RecordId, // document in tenant_a
    doc_b: RecordId, // document in tenant_b
}

/// Mint an HS512-signed JWT carrying the claims the `app_session` access
/// method validates: `iss` / `sub` (resolved by AUTHENTICATE to an
/// `app_user`), plus `ac` / `ns` / `db` so SurrealDB routes the token
/// to the right access definition.
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

    let db = system.raw();

    let tenant_a = create_tenant(&system, "tenant-a", "Tenant A").await;
    let tenant_b = create_tenant(&system, "tenant-b", "Tenant B").await;
    let alice = create_user(&system, "https://idp.test/", "alice", "alice@a.test", &tenant_a).await;
    let bob = create_user(&system, "https://idp.test/", "bob", "bob@b.test", &tenant_b).await;
    create_membership(&system, &alice, &tenant_a, "member").await;
    create_membership(&system, &bob, &tenant_b, "member").await;

    let doc_a = create_document(&system, &tenant_a, "paper-shared", "Alice's paper").await;
    let doc_b = create_document(&system, &tenant_b, "paper-shared", "Bob's paper").await;

    // Re-confirm we're still Root-equivalent by querying both tenants.
    let count: Option<i64> = db
        .query("SELECT count() FROM document GROUP ALL")
        .await
        .unwrap()
        .take((0, "count"))
        .unwrap();
    assert_eq!(count, Some(2), "Root sees both tenants' docs");

    World {
        system,
        ns: ns.to_string(),
        tenant_a,
        tenant_b,
        alice,
        bob,
        doc_a,
        doc_b,
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

async fn create_membership(
    system: &SystemDb,
    user: &RecordId,
    tenant: &RecordId,
    role: &str,
) {
    system
        .raw()
        .query(
            "CREATE membership CONTENT \
             { user: $u, tenant_id: $t, role: $r }",
        )
        .bind(("u", user.clone()))
        .bind(("t", tenant.clone()))
        .bind(("r", role.to_string()))
        .await
        .unwrap()
        .check()
        .unwrap();
}

async fn create_document(
    system: &SystemDb,
    tenant: &RecordId,
    canonical_id: &str,
    title: &str,
) -> RecordId {
    let mut r = system
        .raw()
        .query(
            "CREATE document CONTENT { \
                tenant_id: $t, \
                canonical_id: $cid, \
                source_type: 'test', \
                source_uri: $uri, \
                title: $title, \
                content_hash: $hash, \
                version: 1, \
                metadata: {} \
             } RETURN id",
        )
        .bind(("t", tenant.clone()))
        .bind(("cid", canonical_id.to_string()))
        .bind(("uri", format!("https://test/{canonical_id}")))
        .bind(("title", title.to_string()))
        .bind(("hash", format!("hash-{canonical_id}-{}", tenant.key())))
        .await
        .unwrap();
    let row: Option<IdRow> = r.take(0).unwrap();
    row.unwrap().id
}


#[tokio::test]
async fn alice_cannot_read_bobs_documents() {
    let w = build_world("xtenant_read").await;
    let token = mint_jwt("https://idp.test/", "alice", &w.ns);

    w.system
        .raw()
        .authenticate(&token)
        .await
        .expect("authenticate as alice");

    // Engine rewrites this SELECT under PERMISSIONS clause:
    //   WHERE tenant_id = $token.tenant_id
    // So only Alice's document comes back. Bob's is invisible even
    // though it exists in the same database.
    let docs: Vec<serde_json::Value> = w
        .system
        .raw()
        .query("SELECT canonical_id, title FROM document")
        .await
        .expect("query")
        .take(0)
        .expect("decode");

    assert_eq!(docs.len(), 1, "alice sees exactly one doc; got {docs:?}");
    assert_eq!(docs[0]["title"], "Alice's paper");
}

#[tokio::test]
async fn alice_cannot_read_bobs_document_by_id() {
    let w = build_world("xtenant_read_by_id").await;
    let token = mint_jwt("https://idp.test/", "alice", &w.ns);

    w.system
        .raw()
        .authenticate(&token)
        .await
        .expect("authenticate as alice");

    // Even with the direct record id, the WHERE clause from PERMISSIONS
    // gates the read.
    let result: Option<serde_json::Value> = w
        .system
        .raw()
        .query("SELECT * FROM $rid")
        .bind(("rid", w.doc_b.clone()))
        .await
        .expect("query")
        .take(0)
        .expect("decode");

    assert!(
        result.is_none(),
        "alice should not be able to read bob's doc by id; got {result:?}"
    );
}

#[tokio::test]
async fn alice_cannot_create_doc_in_bobs_tenant() {
    let w = build_world("xtenant_create").await;
    let token = mint_jwt("https://idp.test/", "alice", &w.ns);

    w.system
        .raw()
        .authenticate(&token)
        .await
        .expect("authenticate as alice");

    // PERMISSIONS FOR create WHERE tenant_id = $token.tenant_id —
    // Alice's $token.tenant_id is tenant_a; this CREATE asks for
    // tenant_b. Engine refuses.
    let res = w
        .system
        .raw()
        .query(
            "CREATE document CONTENT { \
                tenant_id: $t, \
                canonical_id: 'sneaky', \
                source_type: 'test', \
                source_uri: 'https://x', \
                content_hash: 'h', \
                version: 1, \
                metadata: {} \
             }",
        )
        .bind(("t", w.tenant_b.clone()))
        .await;

    // Engine refuses with a permissions-violation error OR returns
    // empty. Either is acceptable end-state — what we care about is
    // that no row landed in tenant_b.
    drop(res);

    // Re-authenticate as a tenant_b session to verify nothing was
    // created.
    let bob_token = mint_jwt("https://idp.test/", "bob", &w.ns);
    w.system.raw().authenticate(&bob_token).await.expect("auth bob");

    let docs: Vec<serde_json::Value> = w
        .system
        .raw()
        .query("SELECT canonical_id FROM document WHERE canonical_id = 'sneaky'")
        .await
        .expect("query")
        .take(0)
        .expect("decode");
    assert!(
        docs.is_empty(),
        "no 'sneaky' doc should exist in bob's tenant; got {docs:?}"
    );
}

#[tokio::test]
async fn alice_cannot_mark_bobs_doc_read() {
    let w = build_world("xtenant_feedread").await;
    let token = mint_jwt("https://idp.test/", "alice", &w.ns);

    w.system
        .raw()
        .authenticate(&token)
        .await
        .expect("authenticate as alice");

    // feed_read PERMISSIONS gate creates on both tenant_id match AND
    // user = $auth.id. Alice trying to mark Bob's doc fails on the
    // tenant_id check (and would also fail on user-id if alice's id
    // didn't match).
    let _res = w
        .system
        .raw()
        .query(
            "CREATE feed_read CONTENT { \
                tenant_id: $t, user: $u, document: $d \
             }",
        )
        .bind(("t", w.tenant_b.clone()))
        .bind(("u", w.alice.clone()))
        .bind(("d", w.doc_b.clone()))
        .await;

    // Verify nothing landed.
    let bob_token = mint_jwt("https://idp.test/", "bob", &w.ns);
    w.system.raw().authenticate(&bob_token).await.expect("auth bob");

    let rows: Vec<serde_json::Value> = w
        .system
        .raw()
        .query("SELECT id FROM feed_read")
        .await
        .expect("query")
        .take(0)
        .expect("decode");
    assert!(rows.is_empty(), "no feed_read should have been created");
}

#[tokio::test]
async fn alice_can_read_and_mark_her_own_document() {
    let w = build_world("xtenant_happy").await;
    let token = mint_jwt("https://idp.test/", "alice", &w.ns);

    w.system
        .raw()
        .authenticate(&token)
        .await
        .expect("authenticate as alice");

    // Sanity: positive path works. Alice sees her doc.
    let docs: Vec<serde_json::Value> = w
        .system
        .raw()
        .query("SELECT canonical_id FROM $rid")
        .bind(("rid", w.doc_a.clone()))
        .await
        .expect("query")
        .take(0)
        .expect("decode");
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["canonical_id"], "paper-shared");

    // And can mark-read her own doc.
    w.system
        .raw()
        .query(
            "CREATE feed_read CONTENT { \
                tenant_id: $t, user: $u, document: $d \
             }",
        )
        .bind(("t", w.tenant_a.clone()))
        .bind(("u", w.alice.clone()))
        .bind(("d", w.doc_a.clone()))
        .await
        .expect("create feed_read")
        .check()
        .expect("create succeeded");
}
