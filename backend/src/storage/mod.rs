//! Storage layer.
//!
//! Two distinct entry points:
//!
//! - [`SystemDb`] — privileged singleton signed in as the service user.
//!   Used only by the composition root (`api::serve`), bootstrap (tenant
//!   + user upserts), the scheduler (cursor persistence), and the admin
//!   CLI. Holds the "above-RBAC" credential. Not in
//!   [`crate::state::AppState`] — request handlers physically cannot
//!   reach it.
//!
//! - [`RequestDbPool`] → [`AuthedDb`] — what request handlers receive
//!   (via `Extension`). Each handler gets a SurrealDB session
//!   authenticated by the request's JWT (RECORD session under the
//!   `app_session` access method). Engine-side `PERMISSIONS` clauses
//!   fire on every query, so a handler that builds the wrong query
//!   cannot leak across tenants — SurrealDB refuses.
//!
//! The [`Storage`] trait the request path consumes carries **no notion
//! of tenancy**: methods take no `tenant: &RecordId` parameter, and
//! queries inside the impl rely on `$auth` + PERMISSIONS for scoping.
//! Tenant-explicit operations (scheduler cursor persistence, admin
//! cross-tenant wipe) live on typed methods on [`SystemDb`].

mod models;
mod request;
mod surreal;
mod system;

pub use models::{
    ChatMessage, Chunk, ChunkId, ChunkSearchResult, Content, Conversation, ConversationId, DocId,
    Document, FeedCursor, Filters, MessageId,
};
pub use request::{AuthedDb, RequestDbPool};
pub use surreal::SurrealStorage;
pub use system::{Counts, JwtAccessConfig, JwtAccessKind, SystemDb, SystemStorage};

use async_trait::async_trait;

use crate::error::Result;

/// Per-request storage operations. **No tenant parameter on any
/// method** — tenant scoping comes from the JWT-authenticated session
/// the request is running under, via engine-side `PERMISSIONS` and
/// the schema's `DEFAULT $auth.tenant_id` clauses on write.
///
/// Schema apply, cross-tenant counts/wipe, source-adapter cursor
/// persistence, and any other system-path operation live on
/// [`SystemDb`] — they run with elevated privilege and shouldn't be
/// reachable from request handlers.
#[async_trait]
pub trait Storage: Send + Sync {
    // ---- documents ---------------------------------------------------------

    /// Insert or update a document by `canonical_id`. The engine fills
    /// `tenant_id` from `$auth.tenant_id` via the schema's DEFAULT clause.
    async fn upsert_document(&self, doc: &Document) -> Result<DocId>;

    async fn get_document(&self, id: &DocId) -> Result<Option<Document>>;

    async fn get_document_by_canonical(&self, canonical_id: &str) -> Result<Option<Document>>;

    /// Cascade-deletes content, chunks, and version history.
    async fn delete_document(&self, id: &DocId) -> Result<()>;

    // ---- content -----------------------------------------------------------

    async fn upsert_content(&self, doc_id: &DocId, content: &Content) -> Result<()>;

    async fn get_content(&self, doc_id: &DocId) -> Result<Option<Content>>;

    // ---- chunks ------------------------------------------------------------

    /// Bulk upsert. Returns chunk ids in input order.
    async fn upsert_chunks(&self, doc_id: &DocId, chunks: &[Chunk]) -> Result<Vec<ChunkId>>;

    async fn list_chunks(&self, doc_id: &DocId) -> Result<Vec<Chunk>>;

    async fn delete_chunks(&self, doc_id: &DocId) -> Result<()>;

    // ---- search ------------------------------------------------------------

    /// KNN search over chunk embeddings. Engine scopes by tenant via
    /// PERMISSIONS — no application-side filter needed.
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

    // ---- discovery feed ----------------------------------------------------

    /// Cursor-paginated list of documents, newest-first by
    /// `(ingested_at, id)`. Engine scopes by tenant.
    async fn list_feed(&self, cursor: Option<FeedCursor>, limit: usize) -> Result<Vec<Document>>;

    // ---- conversations -----------------------------------------------------

    /// Create a new conversation owned by the caller. Engine fills
    /// `tenant_id` and `user` from `$auth` via DEFAULT clauses.
    async fn create_conversation(&self, title: Option<&str>) -> Result<ConversationId>;

    /// All conversations visible to the caller, most-recent-first by
    /// `updated_at`.
    async fn list_conversations(&self) -> Result<Vec<Conversation>>;

    /// Fetch one conversation by id. Returns `None` if absent or
    /// engine-side PERMISSIONS hide it.
    async fn get_conversation(&self, id: &ConversationId) -> Result<Option<Conversation>>;

    /// Messages in a conversation, oldest-first.
    async fn list_messages(&self, conv: &ConversationId) -> Result<Vec<ChatMessage>>;

    /// Append a message and bump the parent conversation's `updated_at`.
    async fn append_message(
        &self,
        conv: &ConversationId,
        role: &str,
        content: &str,
    ) -> Result<MessageId>;

    /// Update the title. Engine PERMISSIONS refuse cross-user / cross-
    /// tenant writes.
    async fn rename_conversation(&self, id: &ConversationId, title: &str) -> Result<()>;

    /// Cascade-delete a conversation and all of its messages.
    /// Idempotent: deleting a missing id is a no-op.
    async fn delete_conversation(&self, id: &ConversationId) -> Result<()>;
}
