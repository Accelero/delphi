use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const CONTRACT_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthUser {
    pub user_id: String,
    pub tenant_id: String,
    pub email: Option<String>,
    pub roles: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConversationSummary {
    pub id: String,
    pub title: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConversationDetail {
    pub id: String,
    pub title: String,
    pub messages: Vec<MessageDto>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    User,
    Assistant,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageDto {
    pub id: String,
    pub role: MessageRole,
    pub content: String,
    pub parent_message_id: Option<String>,
    pub citations: Vec<CitationEntry>,
    pub turn_id: Option<String>,
    #[serde(default)]
    pub interrupted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CitationEntry {
    pub index: u32,
    pub label: String,
    pub url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateConversationRequest {
    pub title: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateConversationRequest {
    pub title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubmitTurnRequest {
    pub user_message_id: String,
    pub turn_id: String,
    pub text: String,
    pub parent_message_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubmitChatMessageRequest {
    pub text: String,
    pub parent_message_id: Option<String>,
    pub user_message_id: Option<String>,
    pub turn_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TurnAccepted {
    pub turn_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApiError {
    pub error: ApiErrorBody,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApiErrorBody {
    pub code: ApiErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApiErrorCode {
    Unauthorized,
    Forbidden,
    NotFound,
    InvalidRequest,
    StaleParent,
    InFlight,
    /// The caller acted on a version that is no longer current.
    Conflict,
    /// The resource existed but its window has closed — an expired multipart,
    /// for instance. Distinct from `NotFound` so a client knows retrying the
    /// same identifier is pointless.
    Gone,
    TooLarge,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TurnRequested {
    pub v: u16,
    pub command_id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub conversation_id: String,
    pub turn_id: String,
    pub ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatEventEnvelope {
    pub v: u16,
    pub tenant_id: String,
    pub user_id: String,
    pub conversation_id: String,
    pub turn_id: String,
    pub ts: DateTime<Utc>,
    pub event: ChatEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatEvent {
    TurnStarted {
        turn_id: String,
    },
    UserMessage {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        turn_id: Option<String>,
        content: String,
    },
    Citations {
        citations: Vec<CitationEntry>,
    },
    TextDelta {
        delta: String,
    },
    Finish {
        assistant_message_id: String,
        finish_reason: FinishReason,
    },
    Interrupted {
        assistant_message_id: String,
        content: String,
        finish_reason: InterruptReason,
    },
    Clear {
        reason: ClearReason,
    },
    Error {
        message: String,
    },
    TitleUpdated {
        title: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    Stop,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InterruptReason {
    UserInterrupted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClearReason {
    Cancelled,
    WorkerLost,
    FailedBeforeCommit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientWsMessage {
    SubscribeConversation {
        conversation_id: String,
        last_event_id: Option<String>,
    },
    UnsubscribeConversation {
        conversation_id: String,
    },
    Ping {
        nonce: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerWsMessage {
    Subscribed {
        conversation_id: String,
    },
    Event {
        conversation_id: String,
        event_id: String,
        event: ChatEvent,
    },
    ResyncRequired {
        conversation_id: String,
    },
    Error {
        code: String,
        message: String,
    },
    Pong {
        nonce: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn websocket_event_uses_structured_json_not_sse_frames() {
        let msg = ServerWsMessage::Event {
            conversation_id: "c1".into(),
            event_id: "42".into(),
            event: ChatEvent::TextDelta {
                delta: "hello".into(),
            },
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"event\""));
        assert!(json.contains("\"event_id\":\"42\""));
        assert!(!json.starts_with("event:"));
    }
}
