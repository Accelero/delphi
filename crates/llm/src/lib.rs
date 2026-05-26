use std::pin::Pin;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use rig::agent::{Agent, MultiTurnStreamItem};
use rig::client::CompletionClient;
use rig::completion::Message;
use rig::providers::openai::completion::CompletionModel as OpenAiChatCompletionModel;
use rig::providers::openai::responses_api::ResponsesCompletionModel;
use rig::providers::openai::CompletionsClient;
use rig::providers::{anthropic, gemini, openai};
use rig::streaming::{StreamedAssistantContent, StreamingChat};
use tracing::{debug, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    System,
    User,
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmMessage {
    pub role: Role,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LlmDelta {
    Text(String),
}

pub type DeltaStream = Pin<Box<dyn Stream<Item = Result<LlmDelta>> + Send>>;

#[async_trait]
pub trait LlmClient: Send + Sync {
    async fn stream_chat(&self, messages: Vec<LlmMessage>) -> Result<DeltaStream>;
}

fn split_history(messages: Vec<LlmMessage>) -> Result<(String, Vec<Message>)> {
    let mut history = Vec::new();
    let mut last_user = None;

    for message in messages {
        match message.role {
            Role::System => {
                if let Some(previous) = last_user.take() {
                    history.push(Message::user(previous));
                }
                history.push(Message::system(message.content));
            }
            Role::User => {
                if let Some(previous) = last_user.take() {
                    history.push(Message::user(previous));
                }
                last_user = Some(message.content);
            }
            Role::Assistant => {
                if let Some(previous) = last_user.take() {
                    history.push(Message::user(previous));
                }
                history.push(Message::assistant(message.content));
            }
        }
    }

    let prompt = last_user.ok_or_else(|| anyhow!("chat request had no trailing user message"))?;
    Ok((prompt, history))
}

fn map_assistant<R>(content: StreamedAssistantContent<R>) -> Option<LlmDelta> {
    match content {
        StreamedAssistantContent::Text(text) => Some(LlmDelta::Text(text.text)),
        StreamedAssistantContent::ToolCall { .. }
        | StreamedAssistantContent::ToolCallDelta { .. } => {
            debug!("dropping tool-call event; chat v1 has no tool execution");
            None
        }
        StreamedAssistantContent::Reasoning(_)
        | StreamedAssistantContent::ReasoningDelta { .. }
        | StreamedAssistantContent::Final(_) => None,
    }
}

fn map_multi_turn<R>(item: MultiTurnStreamItem<R>) -> Option<LlmDelta> {
    match item {
        MultiTurnStreamItem::StreamAssistantItem(content) => map_assistant(content),
        MultiTurnStreamItem::StreamUserItem(_) | MultiTurnStreamItem::FinalResponse(_) => None,
        _ => None,
    }
}

struct AnthropicLlm {
    agent: Agent<anthropic::completion::CompletionModel>,
}

impl AnthropicLlm {
    fn from_env(model: &str) -> Result<Self> {
        let client = anthropic::Client::new(&require_env("DELPHI_PROVIDER_ANTHROPIC_API_KEY")?)
            .map_err(|error| anyhow!("anthropic client: {error}"))?;
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
        Ok(Box::pin(map_provider_stream(stream)))
    }
}

struct OpenAiLlm {
    agent: Agent<ResponsesCompletionModel>,
}

impl OpenAiLlm {
    fn from_env(model: &str) -> Result<Self> {
        let client = openai::Client::new(&require_env("DELPHI_PROVIDER_OPENAI_API_KEY")?)
            .map_err(|error| anyhow!("openai client: {error}"))?;
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
        Ok(Box::pin(map_provider_stream(stream)))
    }
}

struct OpenAiCompatLlm {
    agent: Agent<OpenAiChatCompletionModel>,
}

impl OpenAiCompatLlm {
    fn new(model: &str, api_key: String, base_url: String) -> Result<Self> {
        let client = CompletionsClient::builder()
            .api_key(api_key)
            .base_url(base_url)
            .build()
            .map_err(|error| anyhow!("openai-compatible client: {error}"))?;
        let agent = client
            .agent(model)
            .preamble("You are delphi, a research assistant.")
            .build();
        Ok(Self { agent })
    }
}

#[async_trait]
impl LlmClient for OpenAiCompatLlm {
    async fn stream_chat(&self, messages: Vec<LlmMessage>) -> Result<DeltaStream> {
        let (prompt, history) = split_history(messages)?;
        let stream = self.agent.stream_chat(prompt, history).await;
        Ok(Box::pin(map_provider_stream(stream)))
    }
}

fn minimax_from_env(model: &str) -> Result<OpenAiCompatLlm> {
    let api_key = require_env("DELPHI_PROVIDER_MINIMAX_API_KEY")?;
    let base_url = env_or(
        "DELPHI_PROVIDER_MINIMAX_BASE_URL",
        "https://api.minimax.io/v1",
    );
    OpenAiCompatLlm::new(model, api_key, base_url)
}

struct GeminiLlm {
    agent: Agent<gemini::completion::CompletionModel>,
}

impl GeminiLlm {
    fn from_env(model: &str) -> Result<Self> {
        let client = gemini::Client::new(&require_env("DELPHI_PROVIDER_GOOGLE_API_KEY")?)
            .map_err(|error| anyhow!("gemini client: {error}"))?;
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
        Ok(Box::pin(map_provider_stream(stream)))
    }
}

fn map_provider_stream<R, E>(
    stream: impl Stream<Item = std::result::Result<MultiTurnStreamItem<R>, E>> + Send + 'static,
) -> impl Stream<Item = Result<LlmDelta>> + Send
where
    R: Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    stream.filter_map(|chunk| async move {
        match chunk {
            Ok(item) => map_multi_turn(item).map(Ok),
            Err(error) => {
                warn!("llm stream error: {error}");
                Some(Err(anyhow!("llm stream: {error}")))
            }
        }
    })
}

pub fn llm_from_env() -> Result<Arc<dyn LlmClient>> {
    let provider = require_env("DELPHI_PROVIDER")?.to_lowercase();
    let model = require_env("DELPHI_PROVIDER_MODEL")?;

    match provider.as_str() {
        "anthropic" => Ok(Arc::new(AnthropicLlm::from_env(&model)?)),
        "openai" => Ok(Arc::new(OpenAiLlm::from_env(&model)?)),
        "minimax" => Ok(Arc::new(minimax_from_env(&model)?)),
        "gemini" | "google" => Ok(Arc::new(GeminiLlm::from_env(&model)?)),
        other => bail!("unknown DELPHI_PROVIDER={other}"),
    }
}

fn require_env(key: &str) -> Result<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing required env var {key}"))
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_history_uses_trailing_user_as_prompt() {
        let (prompt, history) = split_history(vec![
            LlmMessage {
                role: Role::System,
                content: "be precise".into(),
            },
            LlmMessage {
                role: Role::User,
                content: "hello".into(),
            },
            LlmMessage {
                role: Role::Assistant,
                content: "hi".into(),
            },
            LlmMessage {
                role: Role::User,
                content: "next".into(),
            },
        ])
        .unwrap();

        assert_eq!(prompt, "next");
        assert_eq!(history.len(), 3);
        assert!(matches!(history[0], Message::System { .. }));
    }

    #[test]
    fn split_history_flushes_deferred_user_before_system_message() {
        let (prompt, history) = split_history(vec![
            LlmMessage {
                role: Role::User,
                content: "first".into(),
            },
            LlmMessage {
                role: Role::System,
                content: "side instruction".into(),
            },
            LlmMessage {
                role: Role::User,
                content: "second".into(),
            },
        ])
        .unwrap();

        assert_eq!(prompt, "second");
        assert_eq!(history.len(), 2);
        assert!(matches!(history[0], Message::User { .. }));
        assert!(matches!(history[1], Message::System { .. }));
    }
}
