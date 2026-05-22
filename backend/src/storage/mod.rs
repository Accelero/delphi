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
    dedup_key, Bbox, CanonicalIdConflict, ChatMessage, Chunk, ChunkId, ChunkSearchResult, Citation,
    Content, Conversation, ConversationId, CreateUploadSessionParams, DocId, Document, FeedCursor,
    Filters, IngestionRejection, MessageId, UploadSession,
};
pub use request::{AuthRecord, AuthedDb, RequestDbPool};
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

    /// Fetch a single chunk by id (tenant-scoped).
    async fn get_chunk(&self, id: &ChunkId) -> Result<Option<Chunk>>;

    /// Load a window of chunks for the same document by ordinal range
    /// (inclusive). Used by chat retrieval to expand a KNN hit with its
    /// adjacent neighbors.
    async fn list_chunks_in_range(
        &self,
        doc_id: &DocId,
        ord_lo: i64,
        ord_hi: i64,
    ) -> Result<Vec<Chunk>>;

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
    ///
    /// Kept for tests and ad-hoc inserts. Production chat writes go
    /// through [`Storage::commit_turn`], which writes the
    /// user+assistant pair atomically with "last writer wins"
    /// semantics against a `parent_id`.
    async fn append_message(
        &self,
        conv: &ConversationId,
        role: &str,
        content: &str,
    ) -> Result<MessageId>;

    /// Atomically commit one chat turn: delete any messages created
    /// after `parent_id` (the "last writer wins" step), insert the
    /// user message with `user_message_id` as its record key, insert
    /// the assistant reply linked to it, and bump the conversation's
    /// `updated_at`. Returns the assistant message id.
    ///
    /// `user_message_id` is a client-provided ULID (no `message:`
    /// prefix); the storage layer fabricates the record id internally.
    /// `parent_id == None` declares "this is the first turn" and
    /// causes the DELETE step to scrub everything in the conversation
    /// — that is correct: a first-turn submit is asserting the chat
    /// was empty.
    ///
    /// `citations` are written onto the assistant row so a reloaded
    /// conversation renders its `[N]` markers; pass an empty slice when
    /// the turn cited nothing (stored as `NONE`).
    async fn commit_turn(
        &self,
        conv: &ConversationId,
        user_message_id: &str,
        user_text: &str,
        parent_id: Option<&MessageId>,
        assistant_text: &str,
        citations: &[Citation],
    ) -> Result<MessageId>;

    /// Update the title. Engine PERMISSIONS refuse cross-user / cross-
    /// tenant writes.
    async fn rename_conversation(&self, id: &ConversationId, title: &str) -> Result<()>;

    /// Cascade-delete a conversation and all of its messages.
    /// Idempotent: deleting a missing id is a no-op.
    async fn delete_conversation(&self, id: &ConversationId) -> Result<()>;

    // ---- ingestion v2: upload sessions -------------------------------------
    //
    // Each method below is the typed surface that the four upload
    // endpoints depend on. Raw SurrealQL never escapes this module.
    //
    // Engine PERMISSIONS on `upload_session` scope by `(tenant_id,
    // user_id)`, both populated from `$auth` via the schema's DEFAULT
    // clauses; the handler additionally double-checks the loaded row's
    // identity against `AuthContext` (belt-and-suspenders).

    async fn create_upload_session(
        &self,
        params: &CreateUploadSessionParams,
    ) -> Result<UploadSession>;

    async fn get_upload_session(&self, doc_id: &str) -> Result<Option<UploadSession>>;

    /// Compare-and-swap state transition. Returns `Ok(true)` if the row
    /// was updated (caller proceeds), `Ok(false)` if it wasn't (another
    /// caller has the session, it doesn't exist, or the state was
    /// something other than `from`). Implementation:
    /// `UPDATE upload_session WHERE doc_id = $d AND state = $from
    ///  SET state = $to RETURN BEFORE`.
    async fn cas_upload_session_state(
        &self,
        doc_id: &str,
        from: &str,
        to: &str,
    ) -> Result<bool>;

    /// Atomic transaction: `CREATE document:<doc_id>` (deterministic
    /// record id) + UPSERT `document_content` (extracted text) + DELETE
    /// `upload_session`, all in one Surreal transaction. Returns the new
    /// document id (`document:<doc_id>`).
    ///
    /// Errors with [`Error::CanonicalIdConflict`] when the document's
    /// `canonical_id` is set and a row with the same
    /// `(tenant_id, canonical_id)` already exists (the conflict pre-check
    /// is skipped when `canonical_id` is `None`). The handler turns the
    /// conflict into a 422.
    async fn commit_upload(
        &self,
        doc_id: &str,
        doc: &Document,
        content: &Content,
        dedup_key: Option<&str>,
    ) -> Result<DocId>;

    /// Idempotent: deleting a missing session is a no-op.
    async fn delete_upload_session(&self, doc_id: &str) -> Result<()>;

    async fn record_ingestion_rejection(
        &self,
        rec: &IngestionRejection,
    ) -> Result<()>;

    async fn get_ingestion_rejection(
        &self,
        doc_id: &str,
    ) -> Result<Option<IngestionRejection>>;
}
