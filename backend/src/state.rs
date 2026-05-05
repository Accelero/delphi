//! Shared application state injected into axum handlers.

use std::sync::Arc;

use surrealdb::RecordId;

use crate::llm::LlmClient;
use crate::storage::Storage;

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<dyn Storage>,
    pub llm: Arc<dyn LlmClient>,
    pub auth: Arc<AuthAppState>,
}

/// Auth-related state that handlers may want to read at request time.
/// Layers and middleware are *not* in here — they're attached to the router
/// in `serve()`. This keeps `AppState` small.
#[derive(Clone, Debug)]
pub struct AuthAppState {
    pub mode_label: &'static str,
    pub default_tenant_id: Option<RecordId>,
    pub post_login_redirect: String,
}
