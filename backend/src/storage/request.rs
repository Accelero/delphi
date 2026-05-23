//! Per-request, JWT-authenticated SurrealDB handles.
//!
//! The pool holds a fixed set of pre-connected SurrealDB clients. On every
//! protected request the identity middleware acquires one from the pool,
//! calls `db.authenticate(<idp-jwt>)` so the session becomes a RECORD
//! session under `app_session` (see
//! [`crate::storage::SystemDb::define_jwt_access`]), and attaches the
//! resulting [`AuthedDb`] to the request via `Extension`.
//!
//! From that point engine-side `PERMISSIONS` clauses enforce tenant
//! isolation on every query: a handler that builds the wrong `WHERE`
//! clause cannot leak across tenants because SurrealDB itself refuses.
//!
//! The pool is bounded — `acquire()` waits when all connections are in
//! flight. Dropping [`AuthedDb`] **automatically logs the session out**
//! (`db.invalidate()`) before returning the connection to the pool, so
//! a connection idle in the channel is never in a leftover RECORD
//! session. There is no public release/logout method — the scope guard
//! is the API.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use surrealdb::engine::any::{self, Any};
use surrealdb::opt::auth::Root;
use surrealdb::{RecordId, Surreal};
use tokio::sync::{mpsc, Mutex};

use crate::error::{Error, Result};

use super::surreal::SurrealStorage;
use super::system::engine_requires_auth;
use super::{
    ChatMessage, Chunk, ChunkId, ChunkSearchResult, Citation, Content, Conversation,
    ConversationId, CreateUploadSessionParams, DocId, Document, FeedCursor, Filters,
    IngestionRejection, MessageId, Storage, UploadSession,
};

/// Default pool size when `DELPHI_DB_POOL_SIZE` is unset. Sized to
/// cover typical inbound concurrency without holding many idle
/// WebSocket sessions against SurrealDB. Override via env for
/// deployments that need more (or fewer) physical connections.
const DEFAULT_POOL_SIZE: usize = 8;

/// Pool of pre-connected SurrealDB clients. Cloneable cheaply — internals
/// are `Arc`-counted. One pool per backend process.
#[derive(Clone)]
pub struct RequestDbPool {
    inner: Arc<RequestDbPoolInner>,
}

/// Implementation note: `mpsc` is *multi-producer, single-consumer*, but a
/// pool naturally wants multi-consumer (every concurrent request pulls a
/// connection out). We work around that by wrapping the receiver in a
/// `Mutex` — acquirers lock, `recv().await`, unlock. Functionally fine
/// at our scale (small N, modest concurrency), but it serialises the
/// "wait for a free connection" path through a mutex rather than letting
/// many consumers `recv` in parallel.
///
/// **Future upgrade path** if contention ever shows up: replace this with
/// a real connection-pool crate (`deadpool`, `bb8`, `mobc`) for
/// production-grade features (health checks, max-lifetime, broken-
/// connection eviction, acquire timeouts), or switch to `async-channel`
/// for a drop-in multi-consumer queue without the `Mutex`. The
/// surrounding `AuthedDb` / Drop semantics would carry over unchanged.
struct RequestDbPoolInner {
    /// Receiver side of the available-connections queue. `acquire` does
    /// `recv().await`, which yields the next free connection (or waits
    /// if all are in flight).
    rx: Mutex<mpsc::Receiver<Surreal<Any>>>,
    /// Sender side — `AuthedDb`'s `Drop` impl posts back to this so the
    /// connection returns to the pool.
    tx: mpsc::Sender<Surreal<Any>>,
}

impl RequestDbPool {
    /// Construct a pool by opening `size` independent SurrealDB
    /// connections. Each is signed in as the service user up-front so
    /// `use_ns` / `use_db` are set; per-request `authenticate(jwt)`
    /// then transitions the session into a RECORD scope.
    ///
    /// The service-user credential is loaded from the same env vars
    /// `SystemDb` uses (`DELPHI_DB_USER` / `DELPHI_DB_PASSWORD`)
    /// so we don't introduce a second credential surface.
    pub async fn from_env(size: usize) -> Result<Self> {
        let url = env_or("DELPHI_DB_URL", "ws://surrealdb:8000/rpc");
        let user = env_required("DELPHI_DB_USER")?;
        let password = env_required("DELPHI_DB_PASSWORD")?;
        let namespace = env_or("DELPHI_DB_NAMESPACE", "delphi");
        let database = env_or("DELPHI_DB_NAME", "main");

        let (tx, rx) = mpsc::channel(size);
        for _ in 0..size {
            let db = any::connect(&url).await?;
            if engine_requires_auth(&url) {
                db.signin(Root {
                    username: &user,
                    password: &password,
                })
                .await?;
            }
            db.use_ns(&namespace).use_db(&database).await?;
            tx.send(db)
                .await
                .map_err(|_| Error::InvalidConfig("pool init: send to own channel".into()))?;
        }

        Ok(Self {
            inner: Arc::new(RequestDbPoolInner {
                rx: Mutex::new(rx),
                tx,
            }),
        })
    }

    /// Pool size taken from `DELPHI_DB_POOL_SIZE` (default
    /// [`DEFAULT_POOL_SIZE`]). Most callers want this.
    pub async fn from_env_default() -> Result<Self> {
        let size = match std::env::var("DELPHI_DB_POOL_SIZE") {
            Ok(s) => s.parse::<usize>().map_err(|_| {
                Error::InvalidConfig(format!(
                    "DELPHI_DB_POOL_SIZE must be a positive integer; got {s:?}"
                ))
            })?,
            Err(_) => DEFAULT_POOL_SIZE,
        };
        if size == 0 {
            return Err(Error::InvalidConfig(
                "DELPHI_DB_POOL_SIZE must be > 0".into(),
            ));
        }
        Self::from_env(size).await
    }

    /// Test-only convenience: build a pool sharing a pre-connected
    /// in-memory `Surreal<Any>`. All slots reference the same engine
    /// instance (SurrealDB clones are internally `Arc`-shared), so
    /// tests see consistent state regardless of which slot was
    /// acquired.
    #[doc(hidden)]
    pub async fn in_memory(seed: &Surreal<Any>, size: usize) -> Result<Self> {
        let (tx, rx) = mpsc::channel(size);
        for _ in 0..size {
            tx.send(seed.clone())
                .await
                .map_err(|_| Error::InvalidConfig("test pool init".into()))?;
        }
        Ok(Self {
            inner: Arc::new(RequestDbPoolInner {
                rx: Mutex::new(rx),
                tx,
            }),
        })
    }

    /// Acquire a connection, authenticate it with `bearer`, and return
    /// an [`AuthedDb`] guard. The guard releases on drop.
    pub async fn acquire(&self, bearer: &str) -> Result<AuthedDb> {
        let db = {
            let mut rx = self.inner.rx.lock().await;
            rx.recv()
                .await
                .ok_or_else(|| Error::InvalidConfig("request DB pool closed".into()))?
        };
        // SurrealDB's `authenticate` validates against the `app_session`
        // RECORD access method (its `WITH JWT URL '<jwks>'` form fetches
        // the IdP's public keys; the `KEY '<secret>' ALGORITHM HS512`
        // form validates symmetrically). On success the session
        // transitions into a RECORD scope and PERMISSIONS clauses fire.
        if let Err(e) = db.authenticate(bearer).await {
            // Return the connection to the pool even though we couldn't
            // use it — otherwise a burst of bad tokens drains the pool.
            let _ = self.inner.tx.send(db).await;
            return Err(Error::Surreal(e));
        }
        Ok(AuthedDb {
            inner: Some(AuthedDbInner {
                db: db.clone(),
                storage: SurrealStorage::from_handle(db),
            }),
            tx: self.inner.tx.clone(),
        })
    }
}

/// A pool-borrowed, JWT-authenticated SurrealDB handle. Implements
/// [`Storage`]; on drop the underlying connection returns to the pool.
///
/// `Clone` is intentionally not implemented — that would split the
/// release path across copies. Pass `&AuthedDb` around, or use
/// [`AuthedDb::as_storage`] to hand a shared `Arc<dyn Storage>` to
/// short-lived consumers (e.g. an ingest pipeline within one request).
pub struct AuthedDb {
    /// `Option` so `Drop` can take ownership of the connection without
    /// requiring `AuthedDb: !Sized`. Always `Some(_)` while alive.
    inner: Option<AuthedDbInner>,
    tx: mpsc::Sender<Surreal<Any>>,
}

struct AuthedDbInner {
    db: Surreal<Any>,
    storage: SurrealStorage,
}

impl AuthedDb {
    fn storage(&self) -> &SurrealStorage {
        &self
            .inner
            .as_ref()
            .expect("AuthedDb used after drop")
            .storage
    }

    /// Share this connection with a short-lived consumer (typically
    /// the ingest pipeline). The returned `Arc<dyn Storage>` references
    /// the same authenticated session but does not participate in the
    /// pool-release path — this [`AuthedDb`] retains ownership.
    pub fn as_storage(&self) -> Arc<dyn Storage> {
        let db = self
            .inner
            .as_ref()
            .expect("AuthedDb used after drop")
            .db
            .clone();
        Arc::new(SurrealStorage::from_handle(db))
    }

    /// Resolve the slug of the caller's tenant via `$auth`. Used by
    /// the upload endpoint to construct the object-store key
    /// `tenants/<slug>/<doc_id>` from inside a JWT-authenticated
    /// session, without leaking the raw `tenant:<key>` record id.
    pub async fn resolve_tenant_slug(&self) -> Result<String> {
        let inner = self.inner.as_ref().expect("AuthedDb used after drop");
        let mut r = inner
            .db
            .query("SELECT VALUE slug FROM ONLY $auth.tenant_id;")
            .await?;
        let slug: Option<String> = r.take(0)?;
        slug.ok_or_else(|| {
            Error::InvalidConfig("resolve_tenant_slug: $auth.tenant_id missing".into())
        })
    }

    /// One-shot resolution of the authenticated user's row fields via
    /// `$auth`. Used by the identity middleware to populate
    /// [`crate::auth::AuthContext`] without an upsert. The
    /// `app_session` AUTHENTICATE clause has already resolved
    /// `(iss, sub)` to an `app_user` record; this query just surfaces
    /// the fields the Rust side needs (logging, `/me` response, the
    /// SSE broadcast filter that compares `RecordId`s).
    pub async fn resolve_auth(&self) -> Result<AuthRecord> {
        let inner = self.inner.as_ref().expect("AuthedDb used after drop");
        // `$auth` is the `app_user` record link the `app_session`
        // AUTHENTICATE clause returns. Field access on a record link
        // auto-dereferences, so we project the four fields the
        // identity middleware needs as an inline object — bypassing
        // table-level PERMISSIONS in the process (no SELECT against
        // a table).
        // `$auth` is the `app_user` record link returned by the
        // `app_session` AUTHENTICATE clause. `FROM ONLY $auth`
        // dereferences it; PERMISSIONS on `app_user`
        // (`FOR select WHERE id = $auth.id`) admit this row (the
        // caller's own).
        let mut r = inner
            .db
            .query("SELECT id, tenant_id, email, display_name FROM ONLY $auth;")
            .await?;
        let row: Option<AuthRecord> = r.take(0)?;
        row.ok_or_else(|| Error::InvalidConfig("resolve_auth: $auth returned no row".into()))
    }
}

/// Subset of `app_user` fields the identity middleware reads after
/// authenticate. All values come from `$auth`, which is bound by the
/// `app_session` access method's AUTHENTICATE clause.
#[derive(Debug, Clone, Deserialize)]
pub struct AuthRecord {
    pub id: RecordId,
    pub tenant_id: RecordId,
    pub email: String,
    pub display_name: Option<String>,
}

impl Drop for AuthedDb {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            // Two-step scope cleanup, fired automatically:
            //   1. `invalidate()` — log the RECORD session out, so the
            //      connection returns to the pool in a known clean
            //      state (no leftover user identity on the wire).
            //   2. `send(db)` — release back into the channel for the
            //      next acquirer.
            // Both are best-effort: if invalidate fails the next
            // acquirer's `authenticate(jwt)` overwrites the session
            // anyway, and if the receiver is gone (process shutdown)
            // we drop the connection — fine.
            let tx = self.tx.clone();
            tokio::spawn(async move {
                let _ = inner.db.invalidate().await;
                let _ = tx.send(inner.db).await;
            });
        }
    }
}

#[async_trait]
impl Storage for AuthedDb {
    async fn upsert_document(&self, doc: &Document) -> Result<DocId> {
        self.storage().upsert_document(doc).await
    }
    async fn get_document(&self, id: &DocId) -> Result<Option<Document>> {
        self.storage().get_document(id).await
    }
    async fn get_document_by_canonical(&self, canonical_id: &str) -> Result<Option<Document>> {
        self.storage().get_document_by_canonical(canonical_id).await
    }
    async fn delete_document(&self, id: &DocId) -> Result<()> {
        self.storage().delete_document(id).await
    }
    async fn upsert_content(&self, doc_id: &DocId, content: &Content) -> Result<()> {
        self.storage().upsert_content(doc_id, content).await
    }
    async fn get_content(&self, doc_id: &DocId) -> Result<Option<Content>> {
        self.storage().get_content(doc_id).await
    }
    async fn upsert_chunks(&self, doc_id: &DocId, chunks: &[Chunk]) -> Result<Vec<ChunkId>> {
        self.storage().upsert_chunks(doc_id, chunks).await
    }
    async fn list_chunks(&self, doc_id: &DocId) -> Result<Vec<Chunk>> {
        self.storage().list_chunks(doc_id).await
    }
    async fn delete_chunks(&self, doc_id: &DocId) -> Result<()> {
        self.storage().delete_chunks(doc_id).await
    }
    async fn get_chunk(&self, id: &ChunkId) -> Result<Option<Chunk>> {
        self.storage().get_chunk(id).await
    }
    async fn list_chunks_in_range(
        &self,
        doc_id: &DocId,
        ord_lo: i64,
        ord_hi: i64,
    ) -> Result<Vec<Chunk>> {
        self.storage()
            .list_chunks_in_range(doc_id, ord_lo, ord_hi)
            .await
    }
    async fn search_vector(
        &self,
        query: &[f32],
        top_k: usize,
        filters: &Filters,
    ) -> Result<Vec<ChunkSearchResult>> {
        self.storage().search_vector(query, top_k, filters).await
    }
    async fn search_keyword(
        &self,
        query: &str,
        top_k: usize,
        filters: &Filters,
    ) -> Result<Vec<ChunkSearchResult>> {
        self.storage().search_keyword(query, top_k, filters).await
    }
    async fn list_feed(&self, cursor: Option<FeedCursor>, limit: usize) -> Result<Vec<Document>> {
        self.storage().list_feed(cursor, limit).await
    }
    async fn create_conversation(&self, title: Option<&str>) -> Result<ConversationId> {
        self.storage().create_conversation(title).await
    }
    async fn list_conversations(&self) -> Result<Vec<Conversation>> {
        self.storage().list_conversations().await
    }
    async fn get_conversation(&self, id: &ConversationId) -> Result<Option<Conversation>> {
        self.storage().get_conversation(id).await
    }
    async fn list_messages(&self, conv: &ConversationId) -> Result<Vec<ChatMessage>> {
        self.storage().list_messages(conv).await
    }
    async fn append_message(
        &self,
        conv: &ConversationId,
        role: &str,
        content: &str,
    ) -> Result<MessageId> {
        self.storage().append_message(conv, role, content).await
    }
    async fn commit_turn(
        &self,
        conv: &ConversationId,
        user_message_id: &str,
        user_text: &str,
        parent_id: Option<&MessageId>,
        assistant_text: &str,
        citations: &[Citation],
    ) -> Result<MessageId> {
        self.storage()
            .commit_turn(
                conv,
                user_message_id,
                user_text,
                parent_id,
                assistant_text,
                citations,
            )
            .await
    }
    async fn rename_conversation(&self, id: &ConversationId, title: &str) -> Result<()> {
        self.storage().rename_conversation(id, title).await
    }
    async fn delete_conversation(&self, id: &ConversationId) -> Result<()> {
        self.storage().delete_conversation(id).await
    }
    async fn create_upload_session(
        &self,
        params: &CreateUploadSessionParams,
    ) -> Result<UploadSession> {
        self.storage().create_upload_session(params).await
    }
    async fn get_upload_session(&self, doc_id: &str) -> Result<Option<UploadSession>> {
        self.storage().get_upload_session(doc_id).await
    }
    async fn cas_upload_session_state(&self, doc_id: &str, from: &str, to: &str) -> Result<bool> {
        self.storage()
            .cas_upload_session_state(doc_id, from, to)
            .await
    }
    async fn commit_upload(
        &self,
        doc_id: &str,
        doc: &Document,
        content: &Content,
        dedup_key: Option<&str>,
    ) -> Result<DocId> {
        self.storage()
            .commit_upload(doc_id, doc, content, dedup_key)
            .await
    }
    async fn delete_upload_session(&self, doc_id: &str) -> Result<()> {
        self.storage().delete_upload_session(doc_id).await
    }
    async fn record_ingestion_rejection(&self, rec: &IngestionRejection) -> Result<()> {
        self.storage().record_ingestion_rejection(rec).await
    }
    async fn get_ingestion_rejection(&self, doc_id: &str) -> Result<Option<IngestionRejection>> {
        self.storage().get_ingestion_rejection(doc_id).await
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

fn env_required(key: &str) -> std::result::Result<String, Error> {
    std::env::var(key).map_err(|_| Error::EnvMissing(key.into()))
}
