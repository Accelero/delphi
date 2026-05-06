//! Ingestion filter — the gate between adapter output and the
//! ingestion pipeline.
//!
//! The SPEC's "Discovery" pillar requires a user-defined semantic
//! filter (research questions, topic relevance) that decides which
//! incoming documents earn a place in the corpus. Slice 2 ships the
//! interface plus a [`NoopFilter`] (always accepts); the real
//! LLM-driven filter slots in later as a second [`IngestFilter`] impl
//! with no caller changes.
//!
//! ## Where it sits
//!
//! Filtering applies to the **scheduler path only**:
//!
//! ```text
//! adapter.fetch → filter.evaluate → sink.ingest
//! ```
//!
//! The HTTP `POST /api/ingestion/documents` endpoint deliberately
//! bypasses filtering. Manual pushes are owner-driven and authoritative
//! (e.g. "I want this paper in my corpus, regardless of my standing
//! filters").

mod noop;

use async_trait::async_trait;

use crate::ingestion::IngestRequest;

pub use noop::NoopFilter;

#[async_trait]
pub trait IngestFilter: Send + Sync {
    async fn evaluate(&self, req: &IngestRequest) -> Decision;
}

#[derive(Debug, Clone)]
pub enum Decision {
    Accept,
    Reject { reason: String },
}
