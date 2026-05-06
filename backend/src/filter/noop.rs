use async_trait::async_trait;

use crate::ingestion::IngestRequest;

use super::{Decision, IngestFilter};

/// Always accepts. Slice-2 default; lets the architecture be in place
/// while the real semantic filter is designed.
#[derive(Default, Clone)]
pub struct NoopFilter;

impl NoopFilter {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl IngestFilter for NoopFilter {
    async fn evaluate(&self, _req: &IngestRequest) -> Decision {
        Decision::Accept
    }
}
