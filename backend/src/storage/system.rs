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
//! - source-adapter scheduler cursor persistence (cross-tenant by design)
//!
//! Application/request code goes through [`super::AuthedDb`] instead —
//! that handle is JWT-authenticated and `PERMISSIONS` clauses fire on
//! every query.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::Root;
use surrealdb::{Datetime, RecordId, Surreal};

use crate::error::{Error, Result};

use super::{
    Bbox, ChatMessage, Chunk, ChunkId, ChunkSearchResult, Citation, Content, Conversation,
    ConversationId, CreateUploadSessionParams, DocId, Document, FeedCursor, Filters,
    IngestionRejection, MessageId, Storage, UploadSession,
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
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Privileged SurrealDB handle. Wraps `Surreal<Any>` and exposes only the
/// system-level operations.
///
/// `shared_engine` is `true` when the underlying connection is an embedded
/// engine (`memory:`, `rocksdb:`, ...) whose session state is shared with
/// the [`super::RequestDbPool`]'s clones — i.e. test-mode. In that case the
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
    /// Required env vars: `DELPHI_DB_URL`, `DELPHI_DB_USER`,
    /// `DELPHI_DB_PASSWORD`, `DELPHI_DB_NAMESPACE`, `DELPHI_DB_NAME`.
    pub async fn from_env() -> Result<Self> {
        let url = env_or("DELPHI_DB_URL", "ws://surrealdb:8000/rpc");
        let user = env_required("DELPHI_DB_USER")?;
        let password = env_required("DELPHI_DB_PASSWORD")?;
        let namespace = env_or("DELPHI_DB_NAMESPACE", "delphi");
        let database = env_or("DELPHI_DB_NAME", "main");
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

    /// Reset the shared in-memory engine's session to the privileged
    /// baseline. Called by request-path code that must run a system
    /// write inline (e.g. the validator-reject path writing to
    /// `ingestion_rejection`, whose PERMISSIONS deny user-session
    /// writes). No-op against a remote engine, where the SystemDb owns
    /// its own connection and the session is permanently Root.
    pub async fn reset_session_if_shared(&self) {
        if !self.shared_engine {
            return;
        }
        let _ = self.db.invalidate().await;
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

    /// Apply the canonical schema. Idempotent.
    pub async fn init_schema(&self) -> Result<()> {
        self.db.query(SCHEMA_SURQL).await?.check()?;
        Ok(())
    }

    /// Configure the `app_session` JWT access method at runtime.
    pub async fn define_jwt_access(&self, cfg: &JwtAccessConfig) -> Result<()> {
        let validator = match &cfg.kind {
            JwtAccessKind::Hs512 { secret } => {
                format!("ALGORITHM HS512 KEY '{}'", escape_surrealql_string(secret))
            }
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
            checks.push_str(&format!(
                "IF (type::is::array($token.aud) AND !($token.aud CONTAINS '{aud}')) \
                 OR (type::is::string($token.aud) AND $token.aud != '{aud}') \
                 {{ THROW 'unexpected audience'; }};",
                aud = aud_esc
            ));
        }

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

    // ---- source-adapter cursors -------------------------------------------
    //
    // Cross-tenant, system-path operation: the scheduler runs above-RBAC
    // and writes one cursor row per (tenant, adapter). Lives on
    // SystemDb (not the Storage trait) because it explicitly bypasses
    // the JWT-authenticated request path.

    pub async fn get_source_cursor(
        &self,
        tenant: &RecordId,
        adapter: &str,
    ) -> Result<Option<serde_json::Value>> {
        let mut response = self
            .db
            .query(
                "SELECT cursor FROM source_state \
                 WHERE tenant_id = $t AND adapter = $name LIMIT 1",
            )
            .bind(("t", tenant.clone()))
            .bind(("name", adapter.to_string()))
            .await?;
        let row: Option<CursorRow> = response.take(0)?;
        Ok(row.map(|r| r.cursor))
    }

    /// Tenant-bound [`Storage`] view for system-path callers (admin CLI,
    /// integration tests) that need to read/write domain data without
    /// going through a JWT-authenticated session. Implements the same
    /// tenant-free [`Storage`] surface as [`super::AuthedDb`]; internally
    /// scopes every query to `tenant` via explicit filters, since root
    /// sessions have no `$auth` for PERMISSIONS / DEFAULT to consult.
    ///
    /// **PERMISSIONS clauses do not fire** on calls through this view —
    /// it runs as the service user. Reserved for callers that
    /// legitimately need above-RBAC access.
    pub fn storage_for(&self, tenant: RecordId) -> SystemStorage {
        SystemStorage {
            db: self.db.clone(),
            tenant,
        }
    }

    pub async fn put_source_cursor(
        &self,
        tenant: &RecordId,
        adapter: &str,
        cursor: &serde_json::Value,
    ) -> Result<()> {
        let mut response = self
            .db
            .query(
                "SELECT id FROM source_state \
                 WHERE tenant_id = $t AND adapter = $name LIMIT 1",
            )
            .bind(("t", tenant.clone()))
            .bind(("name", adapter.to_string()))
            .await?;
        let existing: Option<IdRow> = response.take(0)?;

        if let Some(IdRow { id }) = existing {
            self.db
                .query("UPDATE $rid MERGE { cursor: $cursor, updated_at: time::now() }")
                .bind(("rid", id))
                .bind(("cursor", cursor.clone()))
                .await?
                .check()?;
        } else {
            self.db
                .query(
                    "CREATE source_state CONTENT \
                     { tenant_id: $t, adapter: $name, cursor: $cursor }",
                )
                .bind(("t", tenant.clone()))
                .bind(("name", adapter.to_string()))
                .bind(("cursor", cursor.clone()))
                .await?
                .check()?;
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct CountRow {
    n: u64,
}

#[derive(Debug, Deserialize)]
struct IdRow {
    id: RecordId,
}

#[derive(Debug, Deserialize)]
struct CursorRow {
    cursor: serde_json::Value,
}

async fn count_table(db: &Surreal<Any>, table: &str, tenant: Option<&RecordId>) -> Result<u64> {
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

// ============================================================================
// SystemStorage — tenant-bound, root-authed Storage view.
// ============================================================================

/// Bound to a specific `tenant`. Implements the tenant-free [`Storage`]
/// trait by injecting `tenant_id = $t` filters into every query (since
/// root sessions have no `$auth` for engine-side PERMISSIONS / DEFAULT
/// to use).
///
/// Used by admin CLI and integration tests. Not reachable from request
/// handlers.
pub struct SystemStorage {
    db: Surreal<Any>,
    tenant: RecordId,
}

#[derive(Debug, Serialize, Deserialize)]
struct DocumentWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<RecordId>,
    tenant_id: RecordId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    canonical_id: Option<String>,
    source_type: String,
    source_uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    storage_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(default)]
    authors: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    published_at: Option<Datetime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ingested_at: Option<Datetime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    paper_embedding: Option<Vec<f32>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    paper_embedding_model: Option<String>,
    content_hash: String,
    #[serde(default = "default_version")]
    version: i64,
    #[serde(default)]
    metadata: serde_json::Value,
}

fn default_version() -> i64 {
    1
}

impl SystemStorage {
    fn into_wire(&self, d: &Document) -> DocumentWire {
        DocumentWire {
            id: d.id.clone(),
            tenant_id: self.tenant.clone(),
            canonical_id: d.canonical_id.clone(),
            source_type: d.source_type.clone(),
            source_uri: d.source_uri.clone(),
            storage_uri: d.storage_uri.clone(),
            title: d.title.clone(),
            authors: d.authors.clone(),
            published_at: d.published_at.map(Datetime::from),
            ingested_at: d.ingested_at.map(Datetime::from),
            language: d.language.clone(),
            summary: d.summary.clone(),
            paper_embedding: d.paper_embedding.clone(),
            paper_embedding_model: d.paper_embedding_model.clone(),
            content_hash: d.content_hash.clone(),
            version: d.version,
            metadata: d.metadata.clone(),
        }
    }

    fn from_wire(w: DocumentWire) -> Document {
        Document {
            id: w.id,
            tenant_id: Some(w.tenant_id),
            canonical_id: w.canonical_id,
            source_type: w.source_type,
            source_uri: w.source_uri,
            storage_uri: w.storage_uri,
            title: w.title,
            authors: w.authors,
            published_at: w
                .published_at
                .map(|d| -> DateTime<Utc> { d.into_inner().into() }),
            ingested_at: w
                .ingested_at
                .map(|d| -> DateTime<Utc> { d.into_inner().into() }),
            language: w.language,
            summary: w.summary,
            paper_embedding: w.paper_embedding,
            paper_embedding_model: w.paper_embedding_model,
            content_hash: w.content_hash,
            version: w.version,
            metadata: w.metadata,
        }
    }
}

#[derive(Debug, Serialize)]
struct ContentData {
    tenant_id: RecordId,
    doc: RecordId,
    format: String,
    text: String,
    extractor: String,
}

#[derive(Debug, Serialize)]
struct ChunkData {
    tenant_id: RecordId,
    doc: RecordId,
    ordinal: i64,
    char_start: i64,
    char_end: i64,
    bboxes: Option<Vec<Bbox>>,
    text: String,
    embedding: Vec<f32>,
    embedding_model: String,
    chunk_strategy: String,
}

#[async_trait]
impl Storage for SystemStorage {
    async fn upsert_document(&self, doc: &Document) -> Result<DocId> {
        // Skip the dedup pre-check when canonical_id is unset (manual
        // uploads). Matching on `canonical_id = NONE` would false-match
        // prior unset rows.
        let existing: Option<IdRow> = match &doc.canonical_id {
            Some(cid) => {
                let mut response = self
                    .db
                    .query(
                        "SELECT id FROM document \
                         WHERE tenant_id = $t AND canonical_id = $cid LIMIT 1",
                    )
                    .bind(("t", self.tenant.clone()))
                    .bind(("cid", cid.clone()))
                    .await?;
                response.take(0)?
            }
            None => None,
        };

        let wire = self.into_wire(doc);

        if let Some(IdRow { id }) = existing {
            self.db
                .query("UPDATE $rid MERGE $data")
                .bind(("rid", id.clone()))
                .bind(("data", wire))
                .await?
                .check()?;
            Ok(id)
        } else {
            let mut response = self
                .db
                .query("CREATE document CONTENT $data RETURN id")
                .bind(("data", wire))
                .await?;
            let row: Option<IdRow> = response.take(0)?;
            row.map(|r| r.id).ok_or(Error::EmptyResult)
        }
    }

    async fn get_document(&self, id: &DocId) -> Result<Option<Document>> {
        let mut response = self
            .db
            .query("SELECT * FROM $rid WHERE tenant_id = $t LIMIT 1")
            .bind(("rid", id.clone()))
            .bind(("t", self.tenant.clone()))
            .await?;
        let row: Option<DocumentWire> = response.take(0)?;
        Ok(row.map(Self::from_wire))
    }

    async fn get_document_by_canonical(&self, canonical_id: &str) -> Result<Option<Document>> {
        let mut response = self
            .db
            .query(
                "SELECT * FROM document \
                 WHERE tenant_id = $t AND canonical_id = $cid LIMIT 1",
            )
            .bind(("t", self.tenant.clone()))
            .bind(("cid", canonical_id.to_string()))
            .await?;
        let row: Option<DocumentWire> = response.take(0)?;
        Ok(row.map(Self::from_wire))
    }

    async fn delete_document(&self, id: &DocId) -> Result<()> {
        for table in ["document_content", "chunk", "document_version"] {
            self.db
                .query(format!(
                    "DELETE {table} WHERE doc = $rid AND tenant_id = $t"
                ))
                .bind(("rid", id.clone()))
                .bind(("t", self.tenant.clone()))
                .await?
                .check()?;
        }
        self.db
            .query("DELETE $rid WHERE tenant_id = $t")
            .bind(("rid", id.clone()))
            .bind(("t", self.tenant.clone()))
            .await?
            .check()?;
        Ok(())
    }

    async fn upsert_content(&self, doc_id: &DocId, content: &Content) -> Result<()> {
        let mut response = self
            .db
            .query(
                "SELECT id FROM document_content \
                 WHERE doc = $rid AND tenant_id = $t LIMIT 1",
            )
            .bind(("rid", doc_id.clone()))
            .bind(("t", self.tenant.clone()))
            .await?;
        let existing: Option<IdRow> = response.take(0)?;

        let data = ContentData {
            tenant_id: self.tenant.clone(),
            doc: doc_id.clone(),
            format: content.format.clone(),
            text: content.text.clone(),
            extractor: content.extractor.clone(),
        };

        if let Some(IdRow { id }) = existing {
            self.db
                .query("UPDATE $rid MERGE $data")
                .bind(("rid", id))
                .bind(("data", data))
                .await?
                .check()?;
        } else {
            self.db
                .query("CREATE document_content CONTENT $data")
                .bind(("data", data))
                .await?
                .check()?;
        }
        Ok(())
    }

    async fn get_content(&self, doc_id: &DocId) -> Result<Option<Content>> {
        let mut response = self
            .db
            .query(
                "SELECT format, text, extractor FROM document_content \
                 WHERE doc = $rid AND tenant_id = $t LIMIT 1",
            )
            .bind(("rid", doc_id.clone()))
            .bind(("t", self.tenant.clone()))
            .await?;
        Ok(response.take(0)?)
    }

    async fn upsert_chunks(&self, doc_id: &DocId, chunks: &[Chunk]) -> Result<Vec<ChunkId>> {
        let mut ids = Vec::with_capacity(chunks.len());
        for c in chunks {
            let data = ChunkData {
                tenant_id: self.tenant.clone(),
                doc: doc_id.clone(),
                ordinal: c.ordinal,
                char_start: c.char_start,
                char_end: c.char_end,
                bboxes: c.bboxes.clone(),
                text: c.text.clone(),
                embedding: c.embedding.clone(),
                embedding_model: c.embedding_model.clone(),
                chunk_strategy: c.chunk_strategy.clone(),
            };

            let mut response = self
                .db
                .query(
                    "SELECT id FROM chunk \
                     WHERE doc = $rid AND tenant_id = $t \
                       AND ordinal = $ord \
                       AND embedding_model = $model \
                       AND chunk_strategy = $strategy LIMIT 1",
                )
                .bind(("rid", doc_id.clone()))
                .bind(("t", self.tenant.clone()))
                .bind(("ord", c.ordinal))
                .bind(("model", c.embedding_model.clone()))
                .bind(("strategy", c.chunk_strategy.clone()))
                .await?;
            let existing: Option<IdRow> = response.take(0)?;

            if let Some(IdRow { id }) = existing {
                self.db
                    .query("UPDATE $rid MERGE $data")
                    .bind(("rid", id.clone()))
                    .bind(("data", data))
                    .await?
                    .check()?;
                ids.push(id);
            } else {
                let mut response = self
                    .db
                    .query("CREATE chunk CONTENT $data RETURN id")
                    .bind(("data", data))
                    .await?;
                let row: Option<IdRow> = response.take(0)?;
                ids.push(row.map(|r| r.id).ok_or(Error::EmptyResult)?);
            }
        }
        Ok(ids)
    }

    async fn list_chunks(&self, doc_id: &DocId) -> Result<Vec<Chunk>> {
        let mut response = self
            .db
            .query(
                "SELECT * FROM chunk \
                 WHERE doc = $rid AND tenant_id = $t \
                 ORDER BY ordinal ASC",
            )
            .bind(("rid", doc_id.clone()))
            .bind(("t", self.tenant.clone()))
            .await?;
        Ok(response.take(0)?)
    }

    async fn delete_chunks(&self, doc_id: &DocId) -> Result<()> {
        self.db
            .query("DELETE chunk WHERE doc = $rid AND tenant_id = $t")
            .bind(("rid", doc_id.clone()))
            .bind(("t", self.tenant.clone()))
            .await?
            .check()?;
        Ok(())
    }

    async fn get_chunk(&self, id: &ChunkId) -> Result<Option<Chunk>> {
        let mut response = self
            .db
            .query("SELECT * FROM $rid WHERE tenant_id = $t LIMIT 1")
            .bind(("rid", id.clone()))
            .bind(("t", self.tenant.clone()))
            .await?;
        Ok(response.take(0)?)
    }

    async fn list_chunks_in_range(
        &self,
        doc_id: &DocId,
        ord_lo: i64,
        ord_hi: i64,
    ) -> Result<Vec<Chunk>> {
        let mut response = self
            .db
            .query(
                "SELECT * FROM chunk \
                 WHERE doc = $rid AND tenant_id = $t \
                   AND ordinal >= $lo AND ordinal <= $hi \
                 ORDER BY ordinal ASC",
            )
            .bind(("rid", doc_id.clone()))
            .bind(("t", self.tenant.clone()))
            .bind(("lo", ord_lo))
            .bind(("hi", ord_hi))
            .await?;
        Ok(response.take(0)?)
    }

    async fn search_vector(
        &self,
        query: &[f32],
        top_k: usize,
        filters: &Filters,
    ) -> Result<Vec<ChunkSearchResult>> {
        // Brute-force cosine; see surreal.rs::search_vector for the
        // reasoning (HNSW is silent on small corpora / kv-mem).
        let k_lit = top_k.clamp(1, 1000);
        let mut clause = String::from("WHERE tenant_id = $t");
        if filters.embedding_model.is_some() {
            clause.push_str(" AND embedding_model = $f_model");
        }
        if filters.chunk_strategy.is_some() {
            clause.push_str(" AND chunk_strategy = $f_strategy");
        }
        if filters.source_type.is_some() {
            clause.push_str(" AND doc.source_type = $f_source_type");
        }
        let sql = format!(
            "SELECT id AS chunk_id, doc AS doc_id, ordinal, char_start, char_end, \
             text, (1 - vector::similarity::cosine(embedding, $q)) AS score \
             FROM chunk {clause} ORDER BY score ASC LIMIT {k_lit}"
        );
        let mut q = self
            .db
            .query(sql)
            .bind(("t", self.tenant.clone()))
            .bind(("q", query.to_vec()));
        if let Some(v) = &filters.embedding_model {
            q = q.bind(("f_model", v.clone()));
        }
        if let Some(v) = &filters.chunk_strategy {
            q = q.bind(("f_strategy", v.clone()));
        }
        if let Some(v) = &filters.source_type {
            q = q.bind(("f_source_type", v.clone()));
        }
        let mut response = q.await?;
        Ok(response.take(0)?)
    }

    async fn search_keyword(
        &self,
        query: &str,
        top_k: usize,
        filters: &Filters,
    ) -> Result<Vec<ChunkSearchResult>> {
        let mut clause = String::from("WHERE tenant_id = $t AND text @0@ $q");
        if filters.embedding_model.is_some() {
            clause.push_str(" AND embedding_model = $f_model");
        }
        if filters.chunk_strategy.is_some() {
            clause.push_str(" AND chunk_strategy = $f_strategy");
        }
        if filters.source_type.is_some() {
            clause.push_str(" AND doc.source_type = $f_source_type");
        }
        let sql = format!(
            "SELECT id AS chunk_id, doc AS doc_id, ordinal, char_start, char_end, \
             text, search::score(0) AS score \
             FROM chunk {clause} ORDER BY score DESC LIMIT $k"
        );
        let mut q = self
            .db
            .query(sql)
            .bind(("t", self.tenant.clone()))
            .bind(("k", top_k as i64))
            .bind(("q", query.to_string()));
        if let Some(v) = &filters.embedding_model {
            q = q.bind(("f_model", v.clone()));
        }
        if let Some(v) = &filters.chunk_strategy {
            q = q.bind(("f_strategy", v.clone()));
        }
        if let Some(v) = &filters.source_type {
            q = q.bind(("f_source_type", v.clone()));
        }
        let mut response = q.await?;
        Ok(response.take(0)?)
    }

    async fn list_feed(&self, cursor: Option<FeedCursor>, limit: usize) -> Result<Vec<Document>> {
        let where_cursor = if cursor.is_some() {
            "AND (ingested_at < $cursor_ts \
                  OR (ingested_at = $cursor_ts AND id < $cursor_id))"
        } else {
            ""
        };
        let sql = format!(
            "SELECT * FROM document \
             WHERE tenant_id = $t {where_cursor} \
             ORDER BY ingested_at DESC, id DESC \
             LIMIT $limit"
        );
        let mut q = self
            .db
            .query(sql)
            .bind(("t", self.tenant.clone()))
            .bind(("limit", limit as i64));
        if let Some(c) = cursor {
            q = q
                .bind(("cursor_ts", Datetime::from(c.ingested_at)))
                .bind(("cursor_id", c.id));
        }
        let mut response = q.await?;
        let wires: Vec<DocumentWire> = response.take(0)?;
        Ok(wires.into_iter().map(Self::from_wire).collect())
    }

    // ---- conversations -----------------------------------------------------
    //
    // Conversations are a request-path concept (per-user chat history).
    // The system path has no use case for them today — no scheduler / admin
    // CLI operates on chat history — so these are explicit `NotImplemented`
    // stubs. If a future system-path consumer appears (e.g. a scheduled
    // cleanup job), wire them up against `$auth = NONE` queries with an
    // explicit `tenant_id` bind, matching the pattern of the document
    // methods above.

    async fn create_conversation(&self, _title: Option<&str>) -> Result<ConversationId> {
        Err(Error::NotImplemented(
            "SystemStorage does not create conversations".into(),
        ))
    }
    async fn list_conversations(&self) -> Result<Vec<Conversation>> {
        Err(Error::NotImplemented(
            "SystemStorage does not list conversations".into(),
        ))
    }
    async fn get_conversation(&self, _id: &ConversationId) -> Result<Option<Conversation>> {
        Err(Error::NotImplemented(
            "SystemStorage does not read conversations".into(),
        ))
    }
    async fn list_messages(&self, _conv: &ConversationId) -> Result<Vec<ChatMessage>> {
        Err(Error::NotImplemented(
            "SystemStorage does not read messages".into(),
        ))
    }
    async fn append_message(
        &self,
        _conv: &ConversationId,
        _role: &str,
        _content: &str,
    ) -> Result<MessageId> {
        Err(Error::NotImplemented(
            "SystemStorage does not append messages".into(),
        ))
    }
    async fn commit_turn(
        &self,
        _conv: &ConversationId,
        _user_message_id: &str,
        _user_text: &str,
        _parent_id: Option<&MessageId>,
        _assistant_text: &str,
        _citations: &[Citation],
    ) -> Result<MessageId> {
        Err(Error::NotImplemented(
            "SystemStorage does not commit turns".into(),
        ))
    }
    async fn rename_conversation(&self, _id: &ConversationId, _title: &str) -> Result<()> {
        Err(Error::NotImplemented(
            "SystemStorage does not rename conversations".into(),
        ))
    }
    async fn delete_conversation(&self, _id: &ConversationId) -> Result<()> {
        Err(Error::NotImplemented(
            "SystemStorage does not delete conversations".into(),
        ))
    }

    // ---- ingestion v2: upload sessions -------------------------------------
    //
    // SystemStorage is the cleaner's surface — only `record_ingestion_rejection`
    // is implemented here today (the only write that must bypass PERMISSIONS).
    // Listing + reaping helpers live as inherent methods on `SystemDb`
    // proper rather than the `Storage` trait, since they're system-only
    // operations the request path has no use for.

    async fn create_upload_session(
        &self,
        _params: &CreateUploadSessionParams,
    ) -> Result<UploadSession> {
        Err(Error::NotImplemented(
            "SystemStorage::create_upload_session: use AuthedDb instead".into(),
        ))
    }
    async fn get_upload_session(&self, doc_id: &str) -> Result<Option<UploadSession>> {
        let mut response = self
            .db
            .query("SELECT * FROM upload_session WHERE tenant_id = $t AND doc_id = $d LIMIT 1")
            .bind(("t", self.tenant.clone()))
            .bind(("d", doc_id.to_string()))
            .await?;
        Ok(response.take(0)?)
    }
    async fn cas_upload_session_state(
        &self,
        _doc_id: &str,
        _from: &str,
        _to: &str,
    ) -> Result<bool> {
        Err(Error::NotImplemented(
            "SystemStorage::cas_upload_session_state: use AuthedDb instead".into(),
        ))
    }
    async fn commit_upload(
        &self,
        _doc_id: &str,
        _doc: &Document,
        _content: &Content,
        _dedup_key: Option<&str>,
    ) -> Result<DocId> {
        Err(Error::NotImplemented(
            "SystemStorage::commit_upload: use AuthedDb instead".into(),
        ))
    }
    async fn delete_upload_session(&self, doc_id: &str) -> Result<()> {
        self.db
            .query("DELETE upload_session WHERE tenant_id = $t AND doc_id = $d")
            .bind(("t", self.tenant.clone()))
            .bind(("d", doc_id.to_string()))
            .await?
            .check()?;
        Ok(())
    }
    async fn record_ingestion_rejection(&self, rec: &IngestionRejection) -> Result<()> {
        // Root session: PERMISSIONS don't fire. We must set tenant_id /
        // user_id explicitly. Caller is the validator-reject path, which
        // has the values to hand.
        //
        // In the in-memory test rig the engine is shared with the
        // RequestDbPool, so the active session may be RECORD (the
        // caller's authenticated session). `record_ingestion_rejection`
        // must run as root to bypass the `FOR create … WHERE FALSE`
        // PERMISSIONS clause; drop the RECORD session first. No-op
        // against a remote engine (production), where the SystemDb owns
        // its own connection.
        let _ = self.db.invalidate().await;

        let tenant_id = rec.tenant_id.clone().unwrap_or_else(|| self.tenant.clone());
        let user_id = rec.user_id.clone().ok_or_else(|| {
            Error::InvalidConfig("record_ingestion_rejection requires user_id".into())
        })?;
        self.db
            .query(
                "CREATE ingestion_rejection CONTENT { \
                    tenant_id: $t, \
                    user_id: $u, \
                    doc_id: $d, \
                    reason: $r, \
                    sniffed_type: $s \
                 }",
            )
            .bind(("t", tenant_id))
            .bind(("u", user_id))
            .bind(("d", rec.doc_id.clone()))
            .bind(("r", rec.reason.clone()))
            .bind(("s", rec.sniffed_type.clone()))
            .await?
            .check()?;
        Ok(())
    }
    async fn get_ingestion_rejection(&self, doc_id: &str) -> Result<Option<IngestionRejection>> {
        let mut response = self
            .db
            .query(
                "SELECT * FROM ingestion_rejection \
                 WHERE tenant_id = $t AND doc_id = $d \
                 ORDER BY rejected_at DESC LIMIT 1",
            )
            .bind(("t", self.tenant.clone()))
            .bind(("d", doc_id.to_string()))
            .await?;
        Ok(response.take(0)?)
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
