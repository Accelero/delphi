//! `rig`-backed implementation of [`LlmClient`].
//!
//! One concrete impl per provider family (separate types because rig's
//! `Agent<M>` is parameterized over the provider's completion model and the
//! types differ). Selection happens in [`llm_from_env`].

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use rig::agent::{Agent, MultiTurnStreamItem};
use rig::client::CompletionClient;
use rig::completion::Message;
use rig::providers::openai::completion::CompletionModel as OpenAiChatCompletionModel;
use rig::providers::openai::responses_api::ResponsesCompletionModel;
use rig::providers::openai::CompletionsClient;
use rig::providers::{anthropic, gemini, openai};
use rig::streaming::{StreamedAssistantContent, StreamingChat};
use tracing::{debug, warn};

use super::{DeltaStream, LlmClient, LlmDelta, LlmMessage, Role};
use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn split_history(messages: Vec<LlmMessage>) -> Result<(String, Vec<Message>)> {
    let mut history: Vec<Message> = Vec::new();
    let mut last_user: Option<String> = None;

    for m in messages {
        match m.role {
            Role::System => {
                // Fold any system message into history as a user note. v1
                // doesn't override the agent preamble per request.
                history.push(Message::user(format!("[system] {}", m.content)));
            }
            Role::User => {
                if let Some(prev) = last_user.take() {
                    history.push(Message::user(prev));
                }
                last_user = Some(m.content);
            }
            Role::Assistant => {
                if let Some(prev) = last_user.take() {
                    history.push(Message::user(prev));
                }
                history.push(Message::assistant(m.content));
            }
        }
    }
    let prompt = last_user.ok_or_else(|| {
        Error::InvalidConfig("chat request had no trailing user message".into())
    })?;
    Ok((prompt, history))
}

/// Generic delta extractor for both providers — we only care about Text;
/// everything else is dropped for v1.
fn map_assistant<R>(content: StreamedAssistantContent<R>) -> Option<LlmDelta> {
    match content {
        StreamedAssistantContent::Text(t) => Some(LlmDelta::Text(t.text)),
        StreamedAssistantContent::ToolCall { .. }
        | StreamedAssistantContent::ToolCallDelta { .. } => {
            debug!("tool-call event dropped (v1 has no tools)");
            None
        }
        StreamedAssistantContent::Reasoning(_)
        | StreamedAssistantContent::ReasoningDelta { .. } => None,
        StreamedAssistantContent::Final(_) => None,
    }
}

fn map_multi_turn<R>(item: MultiTurnStreamItem<R>) -> Option<LlmDelta> {
    match item {
        MultiTurnStreamItem::StreamAssistantItem(c) => map_assistant(c),
        MultiTurnStreamItem::StreamUserItem(_) => None,
        MultiTurnStreamItem::FinalResponse(_) => None,
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Provider impls
// ---------------------------------------------------------------------------

struct AnthropicLlm {
    agent: Agent<anthropic::completion::CompletionModel>,
}

impl AnthropicLlm {
    fn from_env(model: &str) -> Result<Self> {
        let client = anthropic::Client::new(&require_env("DELPHI_PROVIDER_ANTHROPIC_API_KEY")?)
            .map_err(|e| Error::InvalidConfig(format!("anthropic client: {e}")))?;
        let agent = client
            .agent(model)
            .preamble("You are delphi, a research assistant.")
            .build();
        Ok(Self { agent })
    }
}

#[async_trait]
impl LlmClient for AnthropicLlm {
    async fn stream_chat(&self, messages: Vec<LlmMessage>) -> Result<DeltaStream> {
        let (prompt, history) = split_history(messages)?;
        let stream = self.agent.stream_chat(prompt, history).await;

        let mapped = stream.filter_map(|chunk| async move {
            match chunk {
                Ok(item) => map_multi_turn(item).map(Ok),
                Err(e) => {
                    warn!("rig stream error: {e}");
                    Some(Err(Error::InvalidConfig(format!("stream: {e}"))))
                }
            }
        });
        Ok(Box::pin(mapped))
    }
}

struct OpenAiLlm {
    agent: Agent<ResponsesCompletionModel>,
}

impl OpenAiLlm {
    fn from_env(model: &str) -> Result<Self> {
        let client = openai::Client::new(&require_env("DELPHI_PROVIDER_OPENAI_API_KEY")?)
            .map_err(|e| Error::InvalidConfig(format!("openai client: {e}")))?;
        let agent = client
            .agent(model)
            .preamble("You are delphi, a research assistant.")
            .build();
        Ok(Self { agent })
    }
}

#[async_trait]
impl LlmClient for OpenAiLlm {
    async fn stream_chat(&self, messages: Vec<LlmMessage>) -> Result<DeltaStream> {
        let (prompt, history) = split_history(messages)?;
        let stream = self.agent.stream_chat(prompt, history).await;

        let mapped = stream.filter_map(|chunk| async move {
            match chunk {
                Ok(item) => map_multi_turn(item).map(Ok),
                Err(e) => {
                    warn!("rig stream error: {e}");
                    Some(Err(Error::InvalidConfig(format!("stream: {e}"))))
                }
            }
        });
        Ok(Box::pin(mapped))
    }
}

/// MiniMax exposes an OpenAI-compatible Chat Completions endpoint at
/// `https://api.minimax.io/v1`. We build a [`CompletionsClient`] (legacy
/// chat-completions flavor) with that base URL plus the user's MiniMax
/// API key. The model id comes from `DELPHI_PROVIDER_MODEL` (e.g. `MiniMax-M2.7`).
struct MinimaxLlm {
    agent: Agent<OpenAiChatCompletionModel>,
}

impl MinimaxLlm {
    fn from_env(model: &str) -> Result<Self> {
        let api_key = std::env::var("DELPHI_PROVIDER_MINIMAX_API_KEY")
            .map_err(|_| Error::EnvMissing("DELPHI_PROVIDER_MINIMAX_API_KEY".into()))?;
        let base_url = std::env::var("DELPHI_PROVIDER_MINIMAX_BASE_URL")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "https://api.minimax.io/v1".into());

        let client = CompletionsClient::builder()
            .api_key(api_key)
            .base_url(base_url)
            .build()
            .map_err(|e| Error::InvalidConfig(format!("minimax client: {e}")))?;

        let agent = client
            .agent(model)
            .preamble("You are delphi, a research assistant.")
            .build();
        Ok(Self { agent })
    }
}

#[async_trait]
impl LlmClient for MinimaxLlm {
    async fn stream_chat(&self, messages: Vec<LlmMessage>) -> Result<DeltaStream> {
        let (prompt, history) = split_history(messages)?;
        let stream = self.agent.stream_chat(prompt, history).await;

        let mapped = stream.filter_map(|chunk| async move {
            match chunk {
                Ok(item) => map_multi_turn(item).map(Ok),
                Err(e) => {
                    warn!("rig stream error: {e}");
                    Some(Err(Error::InvalidConfig(format!("stream: {e}"))))
                }
            }
        });
        Ok(Box::pin(mapped))
    }
}

/// Google Gemini via the Generative Language API. The API key comes from
/// `DELPHI_PROVIDER_GOOGLE_API_KEY`, read explicitly and passed to
/// `Client::new` rather than via `rig`'s `from_env` (which would read the
/// library's own `DELPHI_PROVIDER_GOOGLE_API_KEY`). The model id comes from
/// `DELPHI_PROVIDER_MODEL` (e.g. `gemini-3.5-flash`, `gemini-2.5-pro`).
struct GeminiLlm {
    agent: Agent<gemini::completion::CompletionModel>,
}

impl GeminiLlm {
    fn from_env(model: &str) -> Result<Self> {
        let client = gemini::Client::new(&require_env("DELPHI_PROVIDER_GOOGLE_API_KEY")?)
            .map_err(|e| Error::InvalidConfig(format!("gemini client: {e}")))?;
        let agent = client
            .agent(model)
            .preamble("You are delphi, a research assistant.")
            .build();
        Ok(Self { agent })
    }
}

#[async_trait]
impl LlmClient for GeminiLlm {
    async fn stream_chat(&self, messages: Vec<LlmMessage>) -> Result<DeltaStream> {
        let (prompt, history) = split_history(messages)?;
        let stream = self.agent.stream_chat(prompt, history).await;

        let mapped = stream.filter_map(|chunk| async move {
            match chunk {
                Ok(item) => map_multi_turn(item).map(Ok),
                Err(e) => {
                    warn!("rig stream error: {e}");
                    Some(Err(Error::InvalidConfig(format!("stream: {e}"))))
                }
            }
        });
        Ok(Box::pin(mapped))
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Read a required, non-blank environment variable, or fail with a clean
/// [`Error::EnvMissing`] so the misconfiguration surfaces at startup
/// rather than as a guessed default or a downstream runtime error.
fn require_env(key: &str) -> Result<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Error::EnvMissing(key.into()))
}

pub fn llm_from_env() -> Result<Arc<dyn LlmClient>> {
    // No hardcoded provider/model fallbacks: both are deployment
    // configuration, not code defaults. A missing or blank value is a
    // misconfiguration we surface at startup rather than papering over
    // with a guessed provider or model. Per-provider API keys are read
    // inside each provider's `from_env` (see `require_env` calls there).
    let provider = require_env("DELPHI_PROVIDER")?.to_lowercase();
    let model = require_env("DELPHI_PROVIDER_MODEL")?;

    match provider.as_str() {
        "anthropic" => Ok(Arc::new(AnthropicLlm::from_env(&model)?)),
        "openai" => Ok(Arc::new(OpenAiLlm::from_env(&model)?)),
        "minimax" => Ok(Arc::new(MinimaxLlm::from_env(&model)?)),
        "gemini" | "google" => Ok(Arc::new(GeminiLlm::from_env(&model)?)),
        other => Err(Error::UnknownBackend(format!("DELPHI_PROVIDER={other}"))),
    }
}
