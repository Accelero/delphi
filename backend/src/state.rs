//! Shared application state injected into axum handlers.
//!
//! Per-request storage is **not** here — handlers receive an
//! [`Extension<Arc<crate::storage::AuthedDb>>`] from the identity
//! middleware, which holds a JWT-authenticated SurrealDB session
//! scoped to the caller. PERMISSIONS clauses fire on every query
//! through that handle.
//!
//! `AppState` carries only state that is genuinely process-global: the
//! LLM client, object store, and the SSE broadcast channel that
//! ingestion publishes new-document events on.

use std::sync::Arc;

use tokio::sync::broadcast;

use crate::chat::SessionRegistry;
use crate::embedder::Embedder;
use crate::ingestion::FeedItemEvent;
use crate::llm::LlmClient;
use crate::object_store::ObjectStore;
use crate::storage::RequestDbPool;
use crate::text_extractor::TextExtractor;

#[derive(Clone)]
pub struct AppState {
    pub llm: Arc<dyn LlmClient>,
    /// Per-conversation chat sessions. Each entry buffers the in-flight
    /// turn's SSE frames and tracks live subscribers, so every tab on
    /// the same conversation sees the same byte stream. The `/stop`
    /// endpoint cancels by conversation id (no task id in the public
    /// API in v3).
    pub sessions: Arc<SessionRegistry>,
    /// Per-request DB pool, shared with the identity middleware. The
    /// chat worker checks out its own `AuthedDb` for the commit step
    /// because the request that spawned it has already released its
    /// connection by the time the worker finishes.
    pub request_db_pool: RequestDbPool,
    /// Where original artefacts (PDFs, …) are stashed. Adapters use it
    /// directly; HTTP handlers can dereference `Document.storage_uri`
    /// through it for "show original" features.
    pub object_store: Arc<dyn ObjectStore>,
    /// Fan-out channel for "new document accepted" events. The Discovery
    /// SSE handler subscribes per request; the ingestion HTTP handler
    /// publishes via a per-request `NotifyingSink` on the `Created`
    /// outcome.
    pub events: broadcast::Sender<FeedItemEvent>,
    /// PDF → `Vec<Word>` extractor used at ingest by the RAG pipeline.
    /// `None` when chunking/embedding are disabled — the ingest path then
    /// runs the old metadata-only flow.
    pub text_extractor: Option<Arc<dyn TextExtractor>>,
    /// Chunk-level embedder (BGE-small in v1). `None` ⇒ chunking
    /// pipeline is skipped at ingest. Same instance also drives the
    /// chat retrieval path's `query()` call.
    pub chunk_embedder: Option<Arc<dyn Embedder>>,
    /// Document-level embedder (SPECTER2 in v1). `None` ⇒
    /// `document.paper_embedding` is not populated.
    pub document_embedder: Option<Arc<dyn Embedder>>,
}
