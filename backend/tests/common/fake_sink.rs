//! Counting `IngestSink` impl. Used by tests that care about whether
//! requests reach the sink at all (filter tests in particular).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use surrealdb::types::RecordId;

use delphi::error::Result;
use delphi::ingestion::{IngestOutcome, IngestRequest, IngestSink};

#[derive(Default)]
pub struct CountingSink {
    received: AtomicUsize,
    canonical_ids: Mutex<Vec<String>>,
}

impl CountingSink {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn count(&self) -> usize {
        self.received.load(Ordering::Relaxed)
    }
    pub fn ids(&self) -> Vec<String> {
        self.canonical_ids.lock().unwrap().clone()
    }
}

#[async_trait]
impl IngestSink for CountingSink {
    async fn ingest(&self, req: IngestRequest) -> Result<IngestOutcome> {
        self.received.fetch_add(1, Ordering::Relaxed);
        self.canonical_ids
            .lock()
            .unwrap()
            .push(req.canonical_id.clone());
        // Synthetic id — tests don't dereference it.
        let id = RecordId::new("document", req.canonical_id.as_str());
        Ok(IngestOutcome::Created { id, version: 1 })
    }
}
