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
use surrealdb::types::{RecordId, SurrealValue};

use delphi::storage::{JwtAccessConfig, JwtAccessKind, SystemDb};

const TEST_SECRET: &str = "test-only-secret-do-not-use-anywhere-real-please";

#[derive(SurrealValue)]
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
    create_membership(&system, &alice, &tenant_a).await;
    create_membership(&system, &bob, &tenant_b).await;

    let doc_a = create_document(&system, &tenant_a, "doc-shared", "Alice's document").await;
    let doc_b = create_document(&system, &tenant_b, "doc-shared", "Bob's document").await;

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

async fn create_membership(system: &SystemDb, user: &RecordId, tenant: &RecordId) {
    // No `role` field: the `membership` table is SCHEMAFULL and the schema
    // deliberately omits `role` (capabilities come from the JWT, not the
    // DB). surrealdb 3 rejects writes of undefined fields, where surreal 2
    // silently dropped them.
    system
        .raw()
        .query("CREATE membership CONTENT { user: $u, tenant_id: $t }")
        .bind(("u", user.clone()))
        .bind(("t", tenant.clone()))
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
        .bind(("hash", format!("hash-{canonical_id}-{}", delphi::storage::record_key(tenant))))
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
    assert_eq!(docs[0]["title"], "Alice's document");
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
async fn alice_cannot_see_or_mutate_bobs_conversation() {
    // PERMISSIONS on `conversation` scope by `(tenant_id, user)`, so even
    // within the same tenant, conversations are private to their owner.
    // Cross-tenant the engine refuses every operation — list returns
    // empty, direct id-based reads/updates/deletes are no-ops.
    let w = build_world("xtenant_conv").await;

    // Authenticate as bob and create a conversation in tenant_b.
    let bob_token = mint_jwt("https://idp.test/", "bob", &w.ns);
    w.system
        .raw()
        .authenticate(&bob_token)
        .await
        .expect("authenticate as bob");
    let mut r = w
        .system
        .raw()
        .query("CREATE conversation CONTENT { title: 'bob private' } RETURN id")
        .await
        .expect("bob create");
    let row: Option<IdRow> = r.take(0).expect("decode");
    let bob_conv = row.expect("bob conversation").id;

    // And append a message to it.
    w.system
        .raw()
        .query("CREATE message CONTENT { conversation: $c, role: 'user', content: 'secret' }")
        .bind(("c", bob_conv.clone()))
        .await
        .expect("bob append")
        .check()
        .expect("bob append check");

    // Switch to alice.
    let alice_token = mint_jwt("https://idp.test/", "alice", &w.ns);
    w.system
        .raw()
        .authenticate(&alice_token)
        .await
        .expect("authenticate as alice");

    // SELECT * → empty.
    let conversations: Vec<serde_json::Value> = w
        .system
        .raw()
        .query("SELECT title FROM conversation")
        .await
        .expect("alice list")
        .take(0)
        .expect("decode");
    assert!(
        conversations.is_empty(),
        "alice should not see bob's conversation; got {conversations:?}"
    );

    // Direct read by id → empty.
    let result: Option<serde_json::Value> = w
        .system
        .raw()
        .query("SELECT * FROM $rid")
        .bind(("rid", bob_conv.clone()))
        .await
        .expect("alice direct read")
        .take(0)
        .expect("decode");
    assert!(
        result.is_none(),
        "alice cannot read bob's conversation by id; got {result:?}"
    );

    // Attempt to rename → engine refuses; bob still sees old title.
    let _ = w
        .system
        .raw()
        .query("UPDATE $rid SET title = 'hijacked'")
        .bind(("rid", bob_conv.clone()))
        .await;

    // Attempt to delete → also refused (or no-op due to PERMISSIONS).
    let _ = w
        .system
        .raw()
        .query("DELETE $rid")
        .bind(("rid", bob_conv.clone()))
        .await;

    // Re-authenticate as bob and verify nothing changed.
    w.system
        .raw()
        .authenticate(&bob_token)
        .await
        .expect("re-auth bob");
    let convs: Vec<serde_json::Value> = w
        .system
        .raw()
        .query("SELECT title FROM conversation")
        .await
        .expect("bob list after attack")
        .take(0)
        .expect("decode");
    assert_eq!(convs.len(), 1, "bob still has his conversation");
    assert_eq!(
        convs[0]["title"], "bob private",
        "bob's title is untouched"
    );

    let msgs: Vec<serde_json::Value> = w
        .system
        .raw()
        .query("SELECT content FROM message WHERE conversation = $c")
        .bind(("c", bob_conv.clone()))
        .await
        .expect("bob list msgs")
        .take(0)
        .expect("decode");
    assert_eq!(msgs.len(), 1, "bob still has his message");
    assert_eq!(msgs[0]["content"], "secret");
}

#[tokio::test]
async fn alice_can_read_her_own_document() {
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
    assert_eq!(docs[0]["canonical_id"], "doc-shared");
}
