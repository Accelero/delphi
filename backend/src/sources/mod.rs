//! Source adapters: in-process pollers that fetch documents from external
//! catalogues on a schedule and funnel them through the ingestion pipeline.
//!
//! Adapters do not persist; they produce [`IngestRequestBody`]s that the
//! scheduler POSTs to its own `/api/ingestion/documents` endpoint over
//! loopback, authenticated as a service identity. There is exactly one
//! ingestion API — internal adapters and external callers converge on
//! it, validated by the same JWT pipeline.
//!
//! ## Tenancy
//!
//! Adapters are tenant-agnostic — they construct
//! [`IngestRequestBody`]s (which carry no `tenant_id` field). The
//! tenant is stamped server-side by the `/api/ingestion/documents`
//! handler from the service-identity JWT's `tenant_id` claim. v1 mints
//! a single service identity per scheduler, pinning it to
//! `SOURCES_DEFAULT_TENANT_SLUG`. SaaS will mint one identity per
//! tenant.
//!
//! ## Installing adapters
//!
//! - **Bundled (Rust):** drop a `SourceAdapter` impl alongside the
//!   existing ones and wire it into [`default_registry`]. Each adapter
//!   exposes `try_from_env() -> Option<…>`, returning `None` when its
//!   required env vars are unset, so adapters are opt-in by configuration.
//! - **External (any language):** call `POST /api/ingestion/documents`
//!   with a service-account identity that has the `ingester` role. The
//!   request shape is the same `IngestRequestBody` in-tree adapters
//!   produce — this is literally the same endpoint the in-process
//!   scheduler calls.
//! - **Hot-loadable (WASM/dlopen):** explicitly out of scope for slice 1.
//!
//! [`IngestRequestBody`]: crate::ingestion::IngestRequestBody

mod arxiv;
mod ingest_client;
mod registry;
mod scheduler;

use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use crate::error::Result;
use crate::ingestion::IngestRequestBody;

pub use ingest_client::IngestApiClient;
pub use registry::{default_registry, AdapterRegistry};
pub use scheduler::{run_scheduler, SchedulerHandle};

/// One adapter's output for a single fetch cycle.
pub struct Fetched {
    pub items: Vec<IngestRequestBody>,
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
