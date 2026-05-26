//! LLM abstraction layer.
//!
//! `LlmClient` is the trait the rest of the codebase depends on. The
//! `rig`-backed implementation lives in [`rig_impl`]. Add a new provider
//! family by writing another `impl LlmClient` and registering it in
//! [`llm_from_env`].
//!
//! v1 streams plain text deltas. Citations / tool-call deltas can be
//! threaded through later by extending [`LlmDelta`].

mod rig_impl;

use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;

use crate::error::Result;

pub use rig_impl::{extractor_llm_from_env, llm_from_env, title_llm_from_env};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    System,
    User,
    Assistant,
}

#[derive(Debug, Clone)]
pub struct LlmMessage {
    pub role: Role,
    pub content: String,
}

/// Token-level delta yielded by streaming chat.
#[derive(Debug, Clone)]
pub enum LlmDelta {
    /// Incremental piece of assistant text.
    Text(String),
}

pub type DeltaStream = Pin<Box<dyn Stream<Item = Result<LlmDelta>> + Send>>;

#[async_trait]
pub trait LlmClient: Send + Sync {
    /// Stream a chat completion. The last message is treated as the prompt;
    /// preceding messages are history. System messages are folded into the
    /// agent preamble by the implementation.
    async fn stream_chat(&self, messages: Vec<LlmMessage>) -> Result<DeltaStream>;
}
