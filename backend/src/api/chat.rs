//! POST /api/chat — streaming chat completion in Vercel AI SDK Data Stream
//! Protocol. Consumed by `useChat()` in the React frontend.

use std::convert::Infallible;

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::stream::{self, StreamExt};
use serde::Deserialize;
use tracing::{error, info};

use crate::api::stream as proto;
use crate::auth::AuthContext;
use crate::llm::{LlmDelta, LlmMessage, Role};
use crate::state::AppState;

/// Body sent by `@ai-sdk/react`'s `useChat`. Extra fields (id, parts, etc.)
/// are ignored.
#[derive(Debug, Deserialize)]
pub struct ChatRequest {
    #[serde(default)]
    pub messages: Vec<ChatRequestMessage>,
}

#[derive(Debug, Deserialize)]
pub struct ChatRequestMessage {
    pub role: String,
    #[serde(default)]
    pub content: String,
    /// `useChat` v3+ sends a `parts` array; flatten any text parts into
    /// `content` if `content` is empty.
    #[serde(default)]
    pub parts: Vec<MessagePart>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum MessagePart {
    Text {
        #[serde(default)]
        text: String,
    },
    #[serde(other)]
    Other,
}

impl ChatRequestMessage {
    fn collapse_text(&self) -> String {
        if !self.content.is_empty() {
            return self.content.clone();
        }
        self.parts
            .iter()
            .filter_map(|p| match p {
                MessagePart::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("")
    }

    fn to_llm(&self) -> Option<LlmMessage> {
        let role = match self.role.as_str() {
            "system" => Role::System,
            "user" => Role::User,
            "assistant" => Role::Assistant,
            _ => return None,
        };
        Some(LlmMessage {
            role,
            content: self.collapse_text(),
        })
    }
}

pub async fn chat(
    State(state): State<AppState>,
    auth: AuthContext,
    Json(req): Json<ChatRequest>,
) -> Response {
    let messages: Vec<LlmMessage> = req.messages.iter().filter_map(|m| m.to_llm()).collect();
    if messages.is_empty() {
        return (StatusCode::BAD_REQUEST, "no messages").into_response();
    }
    info!(
        user_id = %auth.user_id,
        tenant_id = %auth.tenant_id,
        count = messages.len(),
        "chat request received"
    );

    let llm = state.llm.clone();
    let upstream = match llm.stream_chat(messages).await {
        Ok(s) => s,
        Err(e) => {
            error!("stream_chat init failed: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("llm error: {e}"),
            )
                .into_response();
        }
    };

    // Translate LlmDelta stream → AI SDK Data Stream Protocol records.
    let body_stream = upstream
        .map(|item| {
            let line = match item {
                Ok(LlmDelta::Text(t)) => proto::text(&t),
                Err(e) => proto::error(&e.to_string()),
            };
            Ok::<_, Infallible>(line.into_bytes())
        })
        .chain(stream::once(async {
            Ok::<_, Infallible>(proto::finish("stop").into_bytes())
        }));

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .header("x-vercel-ai-data-stream", "v1")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(body_stream))
        .unwrap()
}
