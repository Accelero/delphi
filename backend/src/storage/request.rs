//! [`RequestDbPool`] — the per-request storage handle handlers receive.
//!
//! Phase 1 (this file): a thin wrapper around the shared SurrealDB
//! connection that implements [`Storage`] by delegation. Tenant
//! isolation is application-layer — every method takes
//! `tenant: &RecordId` and the implementation writes / filters by it.
//!
//! Phase 2 (future): a pool of N WebSocket connections to SurrealDB,
//! each authenticated per-request via the IdP-issued JWT so SurrealDB
//! record-level rules enforce isolation engine-side. The trait surface
//! handlers see does not change between phases — only the internals of
//! how queries reach the engine do.

use std::sync::Arc;

use async_trait::async_trait;
use surrealdb::RecordId;

use crate::error::Result;

use super::surreal::SurrealStorage;
use super::system::SystemDb;
use super::{
    Chunk, ChunkId, ChunkSearchResult, Content, DocId, Document, FeedCursor, FeedItem, Filters,
    Storage,
};

/// Per-request storage handle. Cloneable cheaply — internals are
/// `Arc`-counted.
#[derive(Clone)]
pub struct RequestDbPool {
    inner: Arc<SurrealStorage>,
}

impl RequestDbPool {
    /// Phase 1: borrow the same connection [`SystemDb`] uses. Phase 2:
    /// initialise an independent pool of authenticated connections.
    pub fn from_system(system: &SystemDb) -> Self {
        Self {
            inner: Arc::new(SurrealStorage::from_handle(system.raw().clone())),
        }
    }
}

/// Reserved for Phase 2: a per-request authenticated handle returned by a
/// pool acquire. In Phase 1 this type does not yet exist as a distinct
/// runtime construct; handlers use [`RequestDbPool`] directly via the
/// [`Storage`] trait. The type alias is kept here so that callers writing
/// code that anticipates Phase 2 (e.g., test helpers) have a stable name
/// to reach for.
pub type AuthenticatedDb = RequestDbPool;

#[async_trait]
impl Storage for RequestDbPool {
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
        user_id: &RecordId,
        cursor: Option<FeedCursor>,
        limit: usize,
    ) -> Result<Vec<FeedItem>> {
        self.inner.list_feed(tenant, user_id, cursor, limit).await
    }

    async fn mark_read(
        &self,
        tenant: &RecordId,
        user_id: &RecordId,
        doc_id: &DocId,
    ) -> Result<()> {
        self.inner.mark_read(tenant, user_id, doc_id).await
    }

    async fn mark_unread(
        &self,
        tenant: &RecordId,
        user_id: &RecordId,
        doc_id: &DocId,
    ) -> Result<()> {
        self.inner.mark_unread(tenant, user_id, doc_id).await
    }
}
