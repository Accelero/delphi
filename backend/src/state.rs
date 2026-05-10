//! Shared application state injected into axum handlers.
//!
//! Note: `AppState` carries [`RequestDbPool`] — the per-request storage
//! handle. It deliberately does **not** carry [`crate::storage::SystemDb`]
//! (the privileged singleton used for boot, admin, and scheduler paths).
//! Handlers physically cannot reach the system handle from here.

use std::sync::Arc;

use tokio::sync::broadcast;

use crate::ingestion::{IngestSink, NewDocumentEvent};
use crate::llm::LlmClient;
use crate::object_store::ObjectStore;
use crate::storage::RequestDbPool;

#[derive(Clone)]
pub struct AppState {
    /// Per-request storage handle. Phase 1: thin wrapper over the shared
    /// service-user connection. Phase 2: pool of per-request
    /// JWT-authenticated connections.
    pub db: Arc<RequestDbPool>,
    pub llm: Arc<dyn LlmClient>,
    /// The single contract every ingestion path (HTTP endpoint and
    /// in-process scheduler alike) calls. See [`crate::ingestion`].
    pub sink: Arc<dyn IngestSink>,
    /// Where original artefacts (PDFs, …) are stashed. Adapters use it
    /// directly; HTTP handlers can dereference `Document.storage_uri`
    /// through it for "show original" features.
    pub object_store: Arc<dyn ObjectStore>,
    /// Fan-out channel for "new document accepted" events. The Discovery
    /// SSE handler subscribes per request; `NotifyingSink` (wrapping the
    /// canonical `Pipeline`) publishes on the `Created` outcome.
    pub events: broadcast::Sender<NewDocumentEvent>,
}
