//! [`SystemDb`] — privileged SurrealDB connection used by boot, admin, and
//! scheduler paths.
//!
//! This is the **only** part of the codebase that holds an "above-RBAC"
//! credential. It's deliberately not in [`crate::state::AppState`] —
//! request handlers physically cannot reach it. The trust surface is
//! limited to:
//!
//! - schema apply on startup
//! - `ensure_user` pre-flight (the per-request JWT path needs the
//!   `app_user` row to already exist before SurrealDB's AUTHENTICATE
//!   clause can resolve it)
//! - tenant bootstrap (`resolve_default_tenant`)
//! - admin CLI (`delphi admin status / wipe`)
//! - in-process scheduler ingest
//!
//! In Phase 1 the per-request `RequestDbPool` borrows the same
//! connection; isolation is application-layer. In Phase 2 the pool gets
//! its own connections authenticated per-request via IdP JWT, and
//! `SystemDb` stays as the small contained escape hatch above.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::Root;
use surrealdb::{RecordId, Surreal};

use crate::error::{Error, Result};

use super::surreal::SurrealStorage;
use super::{
    Chunk, ChunkId, ChunkSearchResult, Content, DocId, Document, FeedCursor, Filters,
    Storage,
};

const SCHEMA_SURQL: &str = include_str!("../../schema.surql");

/// Row counts per table — admin-tier diagnostic.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct Counts {
    pub documents: u64,
    pub document_content: u64,
    pub chunks: u64,
    pub document_versions: u64,
}

/// Runtime config for the `app_session` JWT access method. Passed to
/// [`SystemDb::define_jwt_access`] at startup when `AuthMode::Jwt` is
/// active.
#[derive(Debug, Clone)]
pub struct JwtAccessConfig {
    pub kind: JwtAccessKind,
    /// If `Some(iss)`, the AUTHENTICATE clause throws on JWTs whose
    /// `iss` doesn't match. Defence-in-depth even though the BFF
    /// already validated.
    pub expected_issuer: Option<String>,
    /// If `Some(aud)`, the AUTHENTICATE clause throws on JWTs whose
    /// `aud` doesn't contain this value. Handles both string and array
    /// `aud` claims.
    pub expected_audience: Option<String>,
    /// SurrealDB session lifetime per `db.authenticate`. Defaults to
    /// 1800 (30min).
    pub session_duration_secs: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum JwtAccessKind {
    /// Symmetric — backend mints internal JWTs using this secret. Used in
    /// tests and the tier-1 dev path.
    Hs512 { secret: String },
    /// Asymmetric — SurrealDB fetches the IdP's public keys from JWKS.
    /// Used in tier-2 dev and production with a real OIDC IdP.
    Jwks { url: String },
}

fn escape_surrealql_string(s: &str) -> String {
    // SurrealDB single-quoted string literal: escape backslash and
    // single quotes. The key / URL / issuer / audience strings we
    // accept are operator-controlled, but be defensive anyway.
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Privileged SurrealDB handle. Wraps `Surreal<Any>` and exposes only the
/// system-level operations.
///
/// `shared_engine` is `true` when the underlying connection is an embedded
/// engine (`memory:`, `rocksdb:`, ...) whose session state is shared with
/// the [`RequestDbPool`]'s clones — i.e. test-mode. In that case the
/// system path must reset the session back to privileged baseline before
/// each upsert, because a prior `db.authenticate(jwt)` on a pool clone
/// has transitioned the shared session into RECORD mode.
///
/// In production (`ws://` / `wss://`) the SystemDb owns its own
/// connection that nothing else touches, so this flag is `false` and the
/// reset path is skipped — that avoids the cross-request race where
/// concurrent `invalidate`+`signin` on a single shared handle clobbers
/// each other.
#[derive(Clone)]
pub struct SystemDb {
    db: Surreal<Any>,
    shared_engine: bool,
}

impl SystemDb {
    /// Construct from environment.
    ///
    /// Required env vars: `SURREAL_URL`, `SURREAL_SERVICE_USER`,
    /// `SURREAL_SERVICE_PASS`, `SURREAL_NS`, `SURREAL_DB`.
    pub async fn from_env() -> Result<Self> {
        let url = env_or("SURREAL_URL", "ws://surrealdb:8000/rpc");
        let user = env_required("SURREAL_SERVICE_USER")?;
        let password = env_required("SURREAL_SERVICE_PASS")?;
        let namespace = env_or("SURREAL_NS", "delphi");
        let database = env_or("SURREAL_DB", "main");
        Self::connect(&url, &user, &password, &namespace, &database).await
    }

    pub async fn connect(
        url: &str,
        user: &str,
        password: &str,
        namespace: &str,
        database: &str,
    ) -> Result<Self> {
        let db = surrealdb::engine::any::connect(url).await?;
        let requires_auth = engine_requires_auth(url);
        if requires_auth {
            db.signin(Root {
                username: user,
                password,
            })
            .await?;
        }
        db.use_ns(namespace).use_db(database).await?;
        Ok(Self {
            db,
            shared_engine: !requires_auth,
        })
    }

    /// Spin up an embedded in-memory SurrealDB. **Test-only convenience.**
    pub async fn in_memory(namespace: &str, database: &str) -> Result<Self> {
        Self::connect("memory", "", "", namespace, database).await
    }

    /// True when the underlying engine is shared with pool clones (tests).
    /// `auth/bootstrap.rs` reads this to decide whether to reset the
    /// session to the privileged baseline before each upsert.
    pub fn shared_engine(&self) -> bool {
        self.shared_engine
    }

    /// Borrow the underlying handle. Used by `auth/bootstrap.rs` (which
    /// runs raw SurrealQL upserts on `app_user` / `tenant` / `membership`)
    /// and integration tests that drive the engine directly to verify
    /// PERMISSIONS enforcement.
    ///
    /// Application handlers must not call this — they get the typed
    /// `Storage`-trait surface via [`crate::storage::AuthedDb`].
    pub fn raw(&self) -> &Surreal<Any> {
        &self.db
    }

    /// Privileged [`Storage`] view. PERMISSIONS clauses **do not fire**
    /// on writes/reads through this handle — it runs as the service
    /// user. Reserved for callers that legitimately need cross-tenant
    /// or pre-auth-context access: the in-process scheduler, the ingest
    /// pipeline-from-scheduler, and integration tests.
    ///
    /// Request handlers never reach this; they receive an
    /// [`crate::storage::AuthedDb`] via middleware, which runs under a
    /// RECORD session and therefore is subject to PERMISSIONS.
    pub fn storage(&self) -> Arc<SystemStorage> {
        Arc::new(SystemStorage {
            inner: SurrealStorage::from_handle(self.db.clone()),
        })
    }

    /// Apply the canonical schema. Idempotent — every statement uses
    /// `IF NOT EXISTS` / `IF EXISTS`.
    pub async fn init_schema(&self) -> Result<()> {
        self.db.query(SCHEMA_SURQL).await?.check()?;
        Ok(())
    }

    /// Configure the `app_session` JWT access method at runtime. Required
    /// for the engine-enforced tenant-isolation path — without this,
    /// `db.authenticate(jwt)` has no access definition to validate
    /// against.
    ///
    /// The AUTHENTICATE clause maps the IdP JWT's `(iss, sub)` claims to
    /// the local `app_user` record id, so PERMISSIONS clauses can read
    /// `$auth.tenant_id` etc. The backend's pre-flight `ensure_user`
    /// (run on `SystemDb` *before* `db.authenticate` fires) guarantees
    /// the row exists, otherwise authentication fails closed.
    ///
    /// Optional `expected_issuer` / `expected_audience` checks throw
    /// from inside the clause — defence-in-depth even though the BFF
    /// already validated.
    ///
    /// Re-applies cleanly via `OVERWRITE` so a key/JWKS rotation doesn't
    /// require a schema migration.
    pub async fn define_jwt_access(&self, cfg: &JwtAccessConfig) -> Result<()> {
        let validator = match &cfg.kind {
            JwtAccessKind::Hs512 { secret } => format!(
                "ALGORITHM HS512 KEY '{}'",
                escape_surrealql_string(secret)
            ),
            JwtAccessKind::Jwks { url } => {
                format!("URL '{}'", escape_surrealql_string(url))
            }
        };

        let session_secs = cfg.session_duration_secs.unwrap_or(1800);

        let mut checks = String::new();
        if let Some(iss) = &cfg.expected_issuer {
            checks.push_str(&format!(
                "IF $token.iss != '{}' {{ THROW 'unexpected issuer'; }};",
                escape_surrealql_string(iss)
            ));
        }
        if let Some(aud) = &cfg.expected_audience {
            let aud_esc = escape_surrealql_string(aud);
            // `aud` may be a string OR an array per RFC 7519; handle both.
            checks.push_str(&format!(
                "IF (type::is::array($token.aud) AND !($token.aud CONTAINS '{aud}')) \
                 OR (type::is::string($token.aud) AND $token.aud != '{aud}') \
                 {{ THROW 'unexpected audience'; }};",
                aud = aud_esc
            ));
        }

        // The AUTHENTICATE clause is what gives us a meaningful `$auth`
        // record (resolved from the IdP-claimed identity). Without it,
        // SurrealDB falls back to `$auth = $token.ID` — and IdP tokens
        // don't carry an `ID` claim, so engine-side PERMISSIONS that
        // read `$auth.tenant_id` would all see NONE.
        //
        // The clause must return the **record id** (not the full
        // record); SurrealDB then loads it into `$auth`. The post-load
        // PERMISSIONS check on `app_user` is bypassed by the engine
        // when populating `$auth` from AUTHENTICATE, so the
        // `FOR select WHERE id = $auth.id` clause on app_user is not
        // a chicken-and-egg problem here.
        let stmt = format!(
            "DEFINE ACCESS OVERWRITE app_session ON DATABASE TYPE RECORD \
             WITH JWT {validator} \
             AUTHENTICATE {{ \
                {checks} \
                LET $u = (SELECT VALUE id FROM app_user \
                          WHERE iss = $token.iss AND sub = $token.sub LIMIT 1)[0]; \
                IF $u IS NONE {{ THROW 'unknown user'; }}; \
                RETURN $u; \
             }} \
             DURATION FOR SESSION {session_secs}s;"
        );
        self.db.query(stmt).await?.check()?;
        Ok(())
    }

    /// Row counts. `tenant = Some(...)` scopes per-tenant; `None` is
    /// cross-tenant (admin only).
    pub async fn counts(&self, tenant: Option<&RecordId>) -> Result<Counts> {
        let documents = count_table(&self.db, "document", tenant).await?;
        let document_content = count_table(&self.db, "document_content", tenant).await?;
        let chunks = count_table(&self.db, "chunk", tenant).await?;
        let document_versions = count_table(&self.db, "document_version", tenant).await?;
        Ok(Counts {
            documents,
            document_content,
            chunks,
            document_versions,
        })
    }

    /// Delete data; keep schema. `tenant = Some(...)` scopes per-tenant;
    /// `None` is cross-tenant (admin only).
    pub async fn wipe(&self, tenant: Option<&RecordId>) -> Result<()> {
        for table in ["chunk", "document_content", "document_version", "document"] {
            match tenant {
                Some(t) => {
                    self.db
                        .query(format!("DELETE {table} WHERE tenant_id = $t"))
                        .bind(("t", t.clone()))
                        .await?
                        .check()?;
                }
                None => {
                    self.db.query(format!("DELETE {table}")).await?.check()?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct CountRow {
    n: u64,
}

async fn count_table(
    db: &Surreal<Any>,
    table: &str,
    tenant: Option<&RecordId>,
) -> Result<u64> {
    let row: Option<CountRow> = match tenant {
        Some(t) => db
            .query(format!(
                "SELECT count() AS n FROM {table} WHERE tenant_id = $t GROUP ALL"
            ))
            .bind(("t", t.clone()))
            .await?
            .take(0)?,
        None => db
            .query(format!("SELECT count() AS n FROM {table} GROUP ALL"))
            .await?
            .take(0)?,
    };
    Ok(row.map(|r| r.n).unwrap_or(0))
}

/// `memory` / `rocksdb:` are local engines that don't gate on credentials.
/// Anything else (ws/wss/http/https/tcp/…) does.
pub(crate) fn engine_requires_auth(url: &str) -> bool {
    !(url == "memory"
        || url.starts_with("mem://")
        || url.starts_with("rocksdb:")
        || url.starts_with("surrealkv:")
        || url.starts_with("file:"))
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

fn env_required(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| Error::EnvMissing(key.into()))
}

/// Privileged [`Storage`] view obtained via [`SystemDb::storage`]. Wraps
/// the service-user [`SurrealStorage`]; **PERMISSIONS clauses do not
/// fire on calls through this handle**.
///
/// Used by:
/// - the in-process source-adapter scheduler (cross-tenant by design),
/// - the ingest pipeline when driven by the scheduler,
/// - integration tests that need above-RBAC seeding.
pub struct SystemStorage {
    inner: SurrealStorage,
}

#[async_trait]
impl Storage for SystemStorage {
    async fn upsert_document(&self, tenant: &RecordId, doc: &Document) -> Result<DocId> {
        self.inner.upsert_document(tenant, doc).await
    }
    async fn get_document(&self, tenant: &RecordId, id: &DocId) -> Result<Option<Document>> {
        self.inner.get_document(tenant, id).await
    }
    async fn get_document_by_canonical(
        &self,
        tenant: &RecordId,
        canonical_id: &str,
    ) -> Result<Option<Document>> {
        self.inner.get_document_by_canonical(tenant, canonical_id).await
    }
    async fn delete_document(&self, tenant: &RecordId, id: &DocId) -> Result<()> {
        self.inner.delete_document(tenant, id).await
    }
    async fn upsert_content(
        &self,
        tenant: &RecordId,
        doc_id: &DocId,
        content: &Content,
    ) -> Result<()> {
        self.inner.upsert_content(tenant, doc_id, content).await
    }
    async fn get_content(&self, tenant: &RecordId, doc_id: &DocId) -> Result<Option<Content>> {
        self.inner.get_content(tenant, doc_id).await
    }
    async fn upsert_chunks(
        &self,
        tenant: &RecordId,
        doc_id: &DocId,
        chunks: &[Chunk],
    ) -> Result<Vec<ChunkId>> {
        self.inner.upsert_chunks(tenant, doc_id, chunks).await
    }
    async fn list_chunks(&self, tenant: &RecordId, doc_id: &DocId) -> Result<Vec<Chunk>> {
        self.inner.list_chunks(tenant, doc_id).await
    }
    async fn delete_chunks(&self, tenant: &RecordId, doc_id: &DocId) -> Result<()> {
        self.inner.delete_chunks(tenant, doc_id).await
    }
    async fn search_vector(
        &self,
        tenant: &RecordId,
        query: &[f32],
        top_k: usize,
        filters: &Filters,
    ) -> Result<Vec<ChunkSearchResult>> {
        self.inner.search_vector(tenant, query, top_k, filters).await
    }
    async fn search_keyword(
        &self,
        tenant: &RecordId,
        query: &str,
        top_k: usize,
        filters: &Filters,
    ) -> Result<Vec<ChunkSearchResult>> {
        self.inner.search_keyword(tenant, query, top_k, filters).await
    }
    async fn get_source_cursor(
        &self,
        tenant: &RecordId,
        adapter: &str,
    ) -> Result<Option<serde_json::Value>> {
        self.inner.get_source_cursor(tenant, adapter).await
    }
    async fn put_source_cursor(
        &self,
        tenant: &RecordId,
        adapter: &str,
        cursor: &serde_json::Value,
    ) -> Result<()> {
        self.inner.put_source_cursor(tenant, adapter, cursor).await
    }
    async fn list_feed(
        &self,
        tenant: &RecordId,
        cursor: Option<FeedCursor>,
        limit: usize,
    ) -> Result<Vec<Document>> {
        self.inner.list_feed(tenant, cursor, limit).await
    }
}

#[cfg(test)]
mod endpoint_tests {
    use super::engine_requires_auth;

    #[test]
    fn auth_required_for_remote_engines() {
        assert!(engine_requires_auth("ws://surrealdb:8000/rpc"));
        assert!(engine_requires_auth("wss://x:8000"));
        assert!(engine_requires_auth("http://x:8000"));
        assert!(engine_requires_auth("https://x:8000"));
    }

    #[test]
    fn auth_skipped_for_local_engines() {
        assert!(!engine_requires_auth("memory"));
        assert!(!engine_requires_auth("mem://"));
        assert!(!engine_requires_auth("rocksdb:/data/db"));
    }
}
