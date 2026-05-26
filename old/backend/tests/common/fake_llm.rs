//! `LlmClient` impl that returns scripted deltas without touching the network.
//!
//! Default behaviour: emit a single `Text("ok")` delta. Override with
//! [`FakeLlmClient::with_script`] for tests that care about specific output
//! (e.g., the chat-streaming protocol test).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream;

use delphi::error::Result;
use delphi::llm::{DeltaStream, LlmClient, LlmDelta, LlmMessage};

#[derive(Default)]
pub struct FakeLlmClient {
    script: Arc<Mutex<Vec<LlmDelta>>>,
}

impl FakeLlmClient {
    pub fn with_script(deltas: Vec<LlmDelta>) -> Self {
        Self {
            script: Arc::new(Mutex::new(deltas)),
        }
    }
}

#[async_trait]
impl LlmClient for FakeLlmClient {
    async fn stream_chat(&self, _messages: Vec<LlmMessage>) -> Result<DeltaStream> {
        let deltas = {
            let g = self.script.lock().expect("fake llm script mutex poisoned");
            if g.is_empty() {
                vec![LlmDelta::Text("ok".into())]
            } else {
                g.clone()
            }
        };
        let s = stream::iter(deltas.into_iter().map(Ok));
        Ok(Box::pin(s))
    }
}
