//! Shared application state injected into axum handlers.
//!
//! Per-request storage is **not** here — handlers receive an
//! [`Extension<Arc<crate::storage::AuthedDb>>`] from the identity
//! middleware, which holds a JWT-authenticated SurrealDB session
//! scoped to the caller. PERMISSIONS clauses fire on every query
//! through that handle.
//!
//! `AppState` carries only state that is genuinely process-global:
//! the LLM client, object store, ingestion sink (used by the
//! scheduler — handlers ingest via their own per-request pipeline),
//! and the SSE broadcast channel.

use std::sync::Arc;

use tokio::sync::broadcast;

use crate::ingestion::{FeedItemEvent, IngestSink};
use crate::llm::LlmClient;
use crate::object_store::ObjectStore;

#[derive(Clone)]
pub struct AppState {
    pub llm: Arc<dyn LlmClient>,
    /// The single contract every ingestion path (HTTP endpoint and
    /// in-process scheduler alike) calls. See [`crate::ingestion`].
    /// Backed by `SystemStorage` — handlers needing to ingest under
    /// the caller's identity construct their own per-request pipeline
    /// off `AuthedDb` instead.
    pub sink: Arc<dyn IngestSink>,
    /// Where original artefacts (PDFs, …) are stashed. Adapters use it
    /// directly; HTTP handlers can dereference `Document.storage_uri`
    /// through it for "show original" features.
    pub object_store: Arc<dyn ObjectStore>,
    /// Fan-out channel for "new document accepted" events. The Discovery
    /// SSE handler subscribes per request; `NotifyingSink` (wrapping the
    /// canonical `Pipeline`) publishes on the `Created` outcome.
    pub events: broadcast::Sender<FeedItemEvent>,
}
