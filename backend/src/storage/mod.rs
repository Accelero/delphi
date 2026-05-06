//! Storage backend trait. The single contract every implementation satisfies.
//!
//! Application code must depend only on this trait, not on a concrete
//! implementation. The choice of backend is made in [`crate::config`].

mod models;
mod surreal;

pub use models::{Chunk, ChunkId, ChunkSearchResult, Content, DocId, Document, Filters};

/// Concrete-Surreal escape hatch. The bin and integration tests both need
/// the underlying `Surreal<Any>` for auth bootstrap upserts; tests also use
/// [`SurrealStorage::in_memory`] to spin up a fresh DB per case. Other
/// callers go through [`crate::config::storage_from_env`].
pub use surreal::SurrealStorage;
pub(crate) use surreal::surreal_from_env;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;

#[async_trait]
pub trait Storage: Send + Sync {
    // ---- lifecycle ---------------------------------------------------------

    async fn init_schema(&self) -> Result<()>;

    // ---- documents ---------------------------------------------------------

    /// Insert or update a document by `canonical_id`. Returns the backend id.
    async fn upsert_document(&self, doc: &Document) -> Result<DocId>;

    async fn get_document(&self, id: &DocId) -> Result<Option<Document>>;

    async fn get_document_by_canonical(
        &self,
        canonical_id: &str,
    ) -> Result<Option<Document>>;

    /// Cascade-deletes content, chunks, and version history.
    async fn delete_document(&self, id: &DocId) -> Result<()>;

    // ---- content -----------------------------------------------------------

    async fn upsert_content(&self, doc_id: &DocId, content: &Content) -> Result<()>;

    async fn get_content(&self, doc_id: &DocId) -> Result<Option<Content>>;

    // ---- chunks ------------------------------------------------------------

    /// Bulk upsert. Returns chunk ids in input order.
    async fn upsert_chunks(
        &self,
        doc_id: &DocId,
        chunks: &[Chunk],
    ) -> Result<Vec<ChunkId>>;

    async fn list_chunks(&self, doc_id: &DocId) -> Result<Vec<Chunk>>;

    async fn delete_chunks(&self, doc_id: &DocId) -> Result<()>;

    // ---- search ------------------------------------------------------------

    /// KNN search over chunk embeddings.
    async fn search_vector(
        &self,
        query: &[f32],
        top_k: usize,
        filters: &Filters,
    ) -> Result<Vec<ChunkSearchResult>>;

    /// Full-text BM25 search over chunk text.
    async fn search_keyword(
        &self,
        query: &str,
        top_k: usize,
        filters: &Filters,
    ) -> Result<Vec<ChunkSearchResult>>;

    // ---- ops ---------------------------------------------------------------

    async fn counts(&self) -> Result<Counts>;

    /// Delete all data; keep schema.
    async fn wipe(&self) -> Result<()>;
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Counts {
    pub documents: u64,
    pub document_content: u64,
    pub chunks: u64,
    pub document_versions: u64,
}
