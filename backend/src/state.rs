//! Shared application state injected into axum handlers.

use std::sync::Arc;

use tokio::sync::broadcast;

use crate::ingestion::{IngestSink, NewDocumentEvent};
use crate::llm::LlmClient;
use crate::object_store::ObjectStore;
use crate::storage::Storage;

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<dyn Storage>,
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
