//! Storage layer.
//!
//! Two distinct entry points, on purpose:
//!
//! - [`SystemDb`] — privileged singleton signed in as the service user.
//!   Used only by the composition root (`api::serve`), bootstrap (tenant
//!   + user upserts), the scheduler, and the admin CLI. Holds the
//!   "above-RBAC" credential. Not in [`crate::state::AppState`] — request
//!   handlers physically cannot reach it.
//!
//! - [`RequestDbPool`] — what request handlers receive (via
//!   [`crate::state::AppState`]). Phase 1: a thin wrapper around the
//!   shared client that implements the [`Storage`] trait; tenant
//!   isolation is application-layer (every method takes `tenant: &RecordId`
//!   and the impl writes / filters by it). Phase 2: a pool of N
//!   connections, each authenticated per-request via the IdP-issued JWT
//!   so SurrealDB record-level rules enforce isolation engine-side.
//!
//! Application code depends only on the [`Storage`] trait — never on a
//! concrete backend.

mod models;
mod request;
mod surreal;
mod system;

pub use models::{
    Chunk, ChunkId, ChunkSearchResult, Content, DocId, Document, FeedCursor, Filters,
};
pub use request::{AuthedDb, RequestDbPool};
pub use system::{Counts, JwtAccessConfig, JwtAccessKind, SystemDb, SystemStorage};

use async_trait::async_trait;
use surrealdb::RecordId;

use crate::error::Result;

/// Per-request storage operations. Every method takes `tenant: &RecordId`
/// to make tenant scoping a structural property of the API surface
/// (Phase 1: enforced in-handler; Phase 2: backed by engine-level
/// PERMISSIONS clauses).
///
/// Schema apply, cross-tenant counts, and wipe live on [`SystemDb`] —
/// not in this trait — because they run with elevated privilege and
/// shouldn't be reachable from request handlers.
#[async_trait]
pub trait Storage: Send + Sync {
    // ---- documents ---------------------------------------------------------

    /// Insert or update a document by `(tenant_id, canonical_id)`.
    async fn upsert_document(&self, tenant: &RecordId, doc: &Document) -> Result<DocId>;

    async fn get_document(&self, tenant: &RecordId, id: &DocId) -> Result<Option<Document>>;

    async fn get_document_by_canonical(
        &self,
        tenant: &RecordId,
        canonical_id: &str,
    ) -> Result<Option<Document>>;

    /// Cascade-deletes content, chunks, and version history.
    async fn delete_document(&self, tenant: &RecordId, id: &DocId) -> Result<()>;

    // ---- content -----------------------------------------------------------

    async fn upsert_content(
        &self,
        tenant: &RecordId,
        doc_id: &DocId,
        content: &Content,
    ) -> Result<()>;

    async fn get_content(&self, tenant: &RecordId, doc_id: &DocId) -> Result<Option<Content>>;

    // ---- chunks ------------------------------------------------------------

    /// Bulk upsert. Returns chunk ids in input order.
    async fn upsert_chunks(
        &self,
        tenant: &RecordId,
        doc_id: &DocId,
        chunks: &[Chunk],
    ) -> Result<Vec<ChunkId>>;

    async fn list_chunks(&self, tenant: &RecordId, doc_id: &DocId) -> Result<Vec<Chunk>>;

    async fn delete_chunks(&self, tenant: &RecordId, doc_id: &DocId) -> Result<()>;

    // ---- search ------------------------------------------------------------

    /// KNN search over chunk embeddings, scoped to the caller's tenant.
    async fn search_vector(
        &self,
        tenant: &RecordId,
        query: &[f32],
        top_k: usize,
        filters: &Filters,
    ) -> Result<Vec<ChunkSearchResult>>;

    /// Full-text BM25 search over chunk text, scoped to the caller's tenant.
    async fn search_keyword(
        &self,
        tenant: &RecordId,
        query: &str,
        top_k: usize,
        filters: &Filters,
    ) -> Result<Vec<ChunkSearchResult>>;

    // ---- source state ------------------------------------------------------

    /// Read the persisted cursor for a (tenant, adapter) pair.
    async fn get_source_cursor(
        &self,
        tenant: &RecordId,
        adapter: &str,
    ) -> Result<Option<serde_json::Value>>;

    async fn put_source_cursor(
        &self,
        tenant: &RecordId,
        adapter: &str,
        cursor: &serde_json::Value,
    ) -> Result<()>;

    // ---- discovery feed ----------------------------------------------------

    /// Cursor-paginated list of documents. Sorted newest-first by
    /// `(ingested_at, id)`. Scoped to tenant.
    async fn list_feed(
        &self,
        tenant: &RecordId,
        cursor: Option<FeedCursor>,
        limit: usize,
    ) -> Result<Vec<Document>>;
}
