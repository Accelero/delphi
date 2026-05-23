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

/// Generic client for any endpoint that speaks the OpenAI Chat Completions
/// wire format at `<base_url>/chat/completions`. Built from an explicit
/// `(model, api_key, base_url)` so callers own where those come from. Two
/// consumers today: MiniMax (a cloud OpenAI-compatible provider) and the
/// local title-generation sidecar (`title-llm.md`). Adding another is just
/// another caller of [`OpenAiCompatLlm::new`].
struct OpenAiCompatLlm {
    agent: Agent<OpenAiChatCompletionModel>,
}

impl OpenAiCompatLlm {
    fn new(model: &str, api_key: String, base_url: String) -> Result<Self> {
        let client = CompletionsClient::builder()
            .api_key(api_key)
            .base_url(base_url)
            .build()
            .map_err(|e| Error::InvalidConfig(format!("openai-compatible client: {e}")))?;

        let agent = client
            .agent(model)
            .preamble("You are delphi, a research assistant.")
            .build();
        Ok(Self { agent })
    }
}

/// MiniMax exposes an OpenAI-compatible Chat Completions endpoint at
/// `https://api.minimax.io/v1`. Thin wrapper over [`OpenAiCompatLlm`] that
/// sources the key/base-url from `DELPHI_PROVIDER_MINIMAX_*`. The model id
/// comes from `DELPHI_PROVIDER_MODEL` (e.g. `MiniMax-M2.7`).
fn minimax_from_env(model: &str) -> Result<OpenAiCompatLlm> {
    let api_key = require_env("DELPHI_PROVIDER_MINIMAX_API_KEY")?;
    let base_url = env_or("DELPHI_PROVIDER_MINIMAX_BASE_URL", "https://api.minimax.io/v1");
    OpenAiCompatLlm::new(model, api_key, base_url)
}

#[async_trait]
impl LlmClient for OpenAiCompatLlm {
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

/// Read an optional environment variable, falling back to `default` when it
/// is unset or blank. The mirror of [`require_env`] for config that has a
/// legitimately correct default — used by the title block, whose defaults
/// point at the bundled sidecar (see title-llm.md §4).
fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// Read a boolean flag. Unset or blank ⇒ `default`; otherwise `true` iff the
/// value is `"true"` (case-insensitive), matching the `== "true"` convention
/// used elsewhere in the codebase (e.g. `DELPHI_SOURCES_ENABLED`).
fn env_flag(key: &str, default: bool) -> bool {
    match std::env::var(key).ok().map(|v| v.trim().to_lowercase()) {
        Some(v) if !v.is_empty() => v == "true",
        _ => default,
    }
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
        "minimax" => Ok(Arc::new(minimax_from_env(&model)?)),
        "gemini" | "google" => Ok(Arc::new(GeminiLlm::from_env(&model)?)),
        other => Err(Error::UnknownBackend(format!("DELPHI_PROVIDER={other}"))),
    }
}

/// Build the first-turn title-generation client. Unlike [`llm_from_env`],
/// the `DELPHI_TITLE_*` block ships working defaults (applied here, not via
/// `require_env`) that point at the bundled OpenAI-compatible sidecar — the
/// title model has one correct default, so the feature is "default to the
/// sidecar." See title-llm.md §4 for why this is a deliberate exception to
/// the no-hardcoded-defaults rule.
///
/// `DELPHI_TITLE_ENABLED=false` returns the chat client unchanged, so
/// deployments without the sidecar (e.g. a bare `cargo run`) fall back to
/// titling with the chat model instead of silently skipping.
pub fn title_llm_from_env(chat_llm: &Arc<dyn LlmClient>) -> Result<Arc<dyn LlmClient>> {
    if !env_flag("DELPHI_TITLE_ENABLED", true) {
        return Ok(chat_llm.clone());
    }
    match env_or("DELPHI_TITLE_PROVIDER", "openai").to_lowercase().as_str() {
        "openai" => {
            let base_url = env_or("DELPHI_TITLE_BASE_URL", "http://title-llm:80/v1");
            let model = env_or("DELPHI_TITLE_MODEL", "Qwen2.5-0.5B-Instruct");
            let api_key = env_or("DELPHI_TITLE_API_KEY", "sk-noauth");
            Ok(Arc::new(OpenAiCompatLlm::new(&model, api_key, base_url)?))
        }
        other => Err(Error::UnknownBackend(format!("DELPHI_TITLE_PROVIDER={other}"))),
    }
}

/// Build the metadata-autofill client (ingestion `LlmExtractor`). Defaults
/// to the chat client; an explicit `DELPHI_EXTRACT_BASE_URL` redirects to
/// any OpenAI-compatible endpoint (a different cloud model or a local
/// extraction sidecar) without disturbing the chat provider. See
/// docs/architecture/metadata-extractor.md §3.
pub fn extractor_llm_from_env(chat_llm: &Arc<dyn LlmClient>) -> Result<Arc<dyn LlmClient>> {
    match std::env::var("DELPHI_EXTRACT_BASE_URL")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
    {
        Some(base_url) => {
            let model = require_env("DELPHI_EXTRACT_MODEL")?;
            let api_key = env_or("DELPHI_EXTRACT_API_KEY", "sk-noauth");
            Ok(Arc::new(OpenAiCompatLlm::new(&model, api_key, base_url)?))
        }
        None => Ok(chat_llm.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StubLlm;

    #[async_trait]
    impl LlmClient for StubLlm {
        async fn stream_chat(&self, _messages: Vec<LlmMessage>) -> Result<DeltaStream> {
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    #[test]
    fn env_or_uses_default_when_unset_or_blank() {
        std::env::remove_var("DELPHI_TEST_ENVOR");
        assert_eq!(env_or("DELPHI_TEST_ENVOR", "fallback"), "fallback");
        std::env::set_var("DELPHI_TEST_ENVOR", "   ");
        assert_eq!(env_or("DELPHI_TEST_ENVOR", "fallback"), "fallback");
        std::env::set_var("DELPHI_TEST_ENVOR", "  value  ");
        assert_eq!(env_or("DELPHI_TEST_ENVOR", "fallback"), "value");
        std::env::remove_var("DELPHI_TEST_ENVOR");
    }

    #[test]
    fn env_flag_parses_true_else_default() {
        std::env::remove_var("DELPHI_TEST_FLAG");
        assert!(env_flag("DELPHI_TEST_FLAG", true));
        assert!(!env_flag("DELPHI_TEST_FLAG", false));
        std::env::set_var("DELPHI_TEST_FLAG", "false");
        assert!(!env_flag("DELPHI_TEST_FLAG", true));
        std::env::set_var("DELPHI_TEST_FLAG", "TRUE");
        assert!(env_flag("DELPHI_TEST_FLAG", false));
        std::env::remove_var("DELPHI_TEST_FLAG");
    }

    /// The `DELPHI_TITLE_ENABLED=false` escape hatch must reuse the chat
    /// client itself (same allocation), not build a sidecar client.
    #[test]
    fn title_llm_disabled_reuses_chat_client() {
        std::env::set_var("DELPHI_TITLE_ENABLED", "false");
        let chat: Arc<dyn LlmClient> = Arc::new(StubLlm);
        let title = title_llm_from_env(&chat).expect("disabled path is infallible");
        assert!(
            Arc::ptr_eq(&chat, &title),
            "DELPHI_TITLE_ENABLED=false must return the same Arc as the chat client"
        );
        std::env::remove_var("DELPHI_TITLE_ENABLED");
    }
}
