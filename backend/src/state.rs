//! Shared application state injected into axum handlers.

use std::sync::Arc;

use crate::llm::LlmClient;
use crate::storage::Storage;

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<dyn Storage>,
    pub llm: Arc<dyn LlmClient>,
}
