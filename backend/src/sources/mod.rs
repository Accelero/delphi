//! Source adapters: in-process pollers that fetch documents from external
//! catalogues on a schedule and funnel them through the ingestion pipeline.
//!
//! Adapters do not persist; they produce [`IngestRequest`]s that the
//! scheduler hands to an [`IngestSink`]. This is the same
//! `IngestSink::ingest` call the HTTP endpoint at
//! `POST /api/ingestion/documents` makes — internal and external
//! ingestion paths converge on one method.
//!
//! ## Tenancy
//!
//! Slice 1 is single-tenant. The scheduler has no `AuthContext`; it
//! operates implicitly against `AUTH_DEFAULT_TENANT_SLUG` (resolved at
//! startup in `crate::api::serve`). When SaaS lands, the scheduler will
//! gain per-tenant context and `IngestRequest` will carry `tenant_id`.
//!
//! ## Installing adapters
//!
//! - **Bundled (Rust):** drop a `SourceAdapter` impl alongside the
//!   existing ones and wire it into [`default_registry`]. Each adapter
//!   exposes `try_from_env() -> Option<…>`, returning `None` when its
//!   required env vars are unset, so adapters are opt-in by configuration.
//! - **External (any language):** call `POST /api/ingestion/documents`
//!   with a service-account identity that has the `ingester` role. The
//!   request shape is the same `IngestRequest` in-tree adapters produce.
//! - **Hot-loadable (WASM/dlopen):** explicitly out of scope for slice 1.
//!
//! [`IngestRequest`]: crate::ingestion::IngestRequest
//! [`IngestSink`]: crate::ingestion::IngestSink

mod arxiv;
mod registry;
mod scheduler;

use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use surrealdb::RecordId;

use crate::error::Result;
use crate::ingestion::IngestRequest;

pub use registry::{default_registry, AdapterRegistry};
pub use scheduler::{run_scheduler, SchedulerHandle};

/// Placeholder tenant id adapters use when constructing an
/// `IngestRequest`. The scheduler overwrites it before the request
/// reaches the sink — adapters are tenant-agnostic.
pub(crate) fn placeholder_tenant_id() -> RecordId {
    RecordId::from(("tenant", "scheduler-placeholder"))
}

/// One adapter's output for a single fetch cycle.
pub struct Fetched {
    pub items: Vec<IngestRequest>,
    /// Optional new cursor to persist before the next poll. `None` means
    /// "leave the cursor alone" (e.g., empty fetch with nothing new).
    pub next_cursor: Option<Value>,
}

#[async_trait]
pub trait SourceAdapter: Send + Sync {
    /// Stable key used as the `source_state.adapter` row id. Must not
    /// change across versions — it's how we resume after restart.
    fn name(&self) -> &str;

    fn poll_interval(&self) -> Duration;

    /// Fetch one batch. `cursor` is whatever was returned as
    /// `next_cursor` last cycle, or `None` on first run.
    async fn fetch(&self, cursor: Option<Value>) -> Result<Fetched>;
}
