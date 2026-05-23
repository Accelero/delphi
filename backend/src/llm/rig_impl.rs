//! `rig`-backed implementation of [`LlmClient`].
//!
//! One concrete impl per provider family (separate types because rig's
//! `Agent<M>` is parameterized over the provider's completion model and the
//! types differ). Selection happens in [`llm_from_env`].

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use rig::agent::{Agent, MultiTurnStreamItem};
use rig::client::{CompletionClient, ProviderClient};
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
        let client = anthropic::Client::from_env();
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
        let client = openai::Client::from_env();
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
/// API key. Default model is `MiniMax-M2.7` (their featured coding model).
struct MinimaxLlm {
    agent: Agent<OpenAiChatCompletionModel>,
}

impl MinimaxLlm {
    fn from_env(model: &str) -> Result<Self> {
        let api_key = std::env::var("MINIMAX_API_KEY")
            .map_err(|_| Error::EnvMissing("MINIMAX_API_KEY".into()))?;
        let base_url = std::env::var("MINIMAX_BASE_URL")
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

/// Google Gemini via the Generative Language API. `rig`'s
/// [`gemini::Client::from_env`] reads `GEMINI_API_KEY`; we check it
/// ourselves first so a missing key is a clean [`Error::EnvMissing`]
/// rather than the panic `from_env` would otherwise raise. Default model
/// is `gemini-2.5-flash` (fast + cheap); override with `LLM_MODEL`
/// (e.g. `gemini-2.5-pro`).
struct GeminiLlm {
    agent: Agent<gemini::completion::CompletionModel>,
}

impl GeminiLlm {
    fn from_env(model: &str) -> Result<Self> {
        if std::env::var("GEMINI_API_KEY")
            .map(|v| v.trim().is_empty())
            .unwrap_or(true)
        {
            return Err(Error::EnvMissing("GEMINI_API_KEY".into()));
        }
        let client = gemini::Client::from_env();
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

pub fn llm_from_env() -> Result<Arc<dyn LlmClient>> {
    let provider = std::env::var("LLM_PROVIDER")
        .unwrap_or_else(|_| "anthropic".into())
        .to_lowercase();
    let model = std::env::var("LLM_MODEL").unwrap_or_else(|_| match provider.as_str() {
        "openai" => "gpt-4o-mini".into(),
        "minimax" => "MiniMax-M2.7".into(),
        "gemini" | "google" => "gemini-2.5-flash".into(),
        _ => "claude-sonnet-4-5".into(),
    });

    match provider.as_str() {
        "anthropic" => Ok(Arc::new(AnthropicLlm::from_env(&model)?)),
        "openai" => Ok(Arc::new(OpenAiLlm::from_env(&model)?)),
        "minimax" => Ok(Arc::new(MinimaxLlm::from_env(&model)?)),
        "gemini" | "google" => Ok(Arc::new(GeminiLlm::from_env(&model)?)),
        other => Err(Error::UnknownBackend(format!("LLM_PROVIDER={other}"))),
    }
}
