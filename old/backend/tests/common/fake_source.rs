//! `SourceAdapter` impl that returns a scripted batch without touching
//! the network. Mirrors `fake_llm.rs`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use delphi::error::Result;
use delphi::ingestion::IngestRequestBody;
use delphi::sources::{Fetched, SourceAdapter};

pub struct FakeAdapter {
    name: String,
    poll_interval: Duration,
    /// Each tick pops one batch from the front. When exhausted, returns
    /// an empty batch — the scheduler keeps ticking but does no work.
    batches: Arc<Mutex<Vec<Vec<IngestRequestBody>>>>,
    next_cursor: Mutex<Option<Value>>,
    fetch_count: AtomicUsize,
}

impl FakeAdapter {
    pub fn new(name: &str, poll_interval: Duration, batches: Vec<Vec<IngestRequestBody>>) -> Self {
        Self {
            name: name.into(),
            poll_interval,
            batches: Arc::new(Mutex::new(batches)),
            next_cursor: Mutex::new(None),
            fetch_count: AtomicUsize::new(0),
        }
    }

    pub fn with_next_cursor(self, cursor: Value) -> Self {
        *self.next_cursor.lock().unwrap() = Some(cursor);
        self
    }

    pub fn fetch_count(&self) -> usize {
        self.fetch_count.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl SourceAdapter for FakeAdapter {
    fn name(&self) -> &str {
        &self.name
    }
    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }
    async fn fetch(&self, _cursor: Option<Value>) -> Result<Fetched> {
        self.fetch_count.fetch_add(1, Ordering::Relaxed);
        let mut batches = self.batches.lock().unwrap();
        let items = if batches.is_empty() {
            Vec::new()
        } else {
            batches.remove(0)
        };
        let next_cursor = self.next_cursor.lock().unwrap().clone();
        Ok(Fetched { items, next_cursor })
    }
}
