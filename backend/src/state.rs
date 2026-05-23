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

use crate::chat::TurnBus;
use crate::embedder::Embedder;
use crate::ingestion::{FeedItemEvent, MetadataExtractor};
use crate::llm::LlmClient;
use crate::object_store::{AccessMinter, ObjectStore};
use crate::storage::{RequestDbPool, SystemDb};
use crate::text_extractor::TextExtractor;

#[derive(Clone)]
pub struct AppState {
    pub llm: Arc<dyn LlmClient>,
    /// Cheap, usually-local client for first-turn title generation.
    /// Defaults to the bundled title sidecar; equals `llm` when titles are
    /// configured to reuse the chat model (`DELPHI_TITLE_ENABLED=false`).
    /// See `docs/architecture/title-llm.md`.
    pub title_llm: Arc<dyn LlmClient>,
    /// Per-conversation turn transport (v4). Owns the single-flight slot,
    /// the ordered SSE delta log (replay + live fan-out), and cancel
    /// delivery, behind one trait so the in-memory impl swaps for a
    /// Redis-backed one without touching callers. `/stop` cancels by
    /// conversation id (no task id in the public API). `Arc<dyn TurnBus>`
    /// = `InProcessBus` in Phase 1.
    pub turn_bus: Arc<dyn TurnBus>,
    /// Per-request DB pool, shared with the identity middleware. The
    /// chat worker checks out its own `AuthedDb` for the commit step
    /// because the request that spawned it has already released its
    /// connection by the time the worker finishes.
    pub request_db_pool: RequestDbPool,
    /// Where original artefacts (PDFs, …) are stashed. Adapters use it
    /// directly; HTTP handlers can dereference `Document.storage_uri`
    /// through it for "show original" features.
    pub object_store: Arc<dyn ObjectStore>,
    /// Client-facing object-access minter — the swappable seam for
    /// direct-to-storage reads/writes. Handlers run the authz decision,
    /// then mint a short-lived scoped URL (presigned today via
    /// `S3PresignAccess`; CDN/STS/proxy drop in here later without caller
    /// changes). See `docs/architecture/object-access.md`.
    pub access: Arc<dyn AccessMinter>,
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
    /// Privileged DB handle. Reserved for the **handler-side** paths
    /// that legitimately need to bypass `PERMISSIONS` — currently only
    /// the ingestion v2 `/complete` validator-reject write to
    /// `ingestion_rejection` (PERMISSIONS deny user-session writes by
    /// design). Other handlers must continue using `AuthedDb`.
    pub system_db: Arc<SystemDb>,
    /// Ingestion v2 runtime knobs (part size, TTLs, policies). Built
    /// once at boot from env; handlers read this for per-request
    /// decisions without re-parsing env on every call.
    pub uploads_config: Arc<crate::ingestion::UploadsConfig>,
    /// Metadata autofill seam. Ships as `NoopExtractor` today; the
    /// Phase-3 `LlmExtractor` swaps in here without touching callers.
    pub metadata_extractor: Arc<dyn MetadataExtractor>,
}
