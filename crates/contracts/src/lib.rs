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
#[serde(rename_all = "snake_case")]
pub enum DocumentState {
    Staging,
    Validating,
    Indexing,
    Ready,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IngestionStage {
    Validate,
    Extract,
    Chunk,
    Embed,
    Publish,
    Reconcile,
}

impl IngestionStage {
    pub fn as_subject_token(&self) -> &'static str {
        match self {
            Self::Validate => "validate",
            Self::Extract => "extract",
            Self::Chunk => "chunk",
            Self::Embed => "embed",
            Self::Publish => "publish",
            Self::Reconcile => "reconcile",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IngestionJobStatus {
    Validating,
    Extracting,
    Chunking,
    Embedding,
    Publishing,
    Ready,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateIngestionDocument {
    pub document_id: Option<String>,
    pub job_id: Option<String>,
    pub title: Option<String>,
    pub source_type: String,
    pub source_uri: Option<String>,
    pub storage_key: String,
    pub filename: Option<String>,
    pub content_type: Option<String>,
    pub declared_size: u64,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestionJobDto {
    pub id: String,
    pub document_id: String,
    pub state: DocumentState,
    pub status: IngestionJobStatus,
    pub current_stage: Option<IngestionStage>,
    pub pipeline_version: u32,
    pub attempt: u32,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StartIngestionResponse {
    pub document_id: String,
    pub job_id: String,
    pub state: DocumentState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateUploadRequest {
    pub filename: String,
    pub size: u64,
    pub content_type: Option<String>,
    pub title: Option<String>,
    pub source_uri: Option<String>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateUploadResponse {
    pub upload_id: String,
    pub key: String,
    pub multipart_upload_id: String,
    pub part_size_bytes: u64,
    pub part_url_ttl_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignUploadPartRequest {
    pub part_number: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignUploadPartResponse {
    pub url: String,
    pub method: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompletedUploadPart {
    pub part_number: u16,
    pub etag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompleteUploadRequest {
    pub parts: Vec<CompletedUploadPart>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum CompleteUploadResponse {
    Accepted { document_id: String, job_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum UploadStatusResponse {
    Uploading,
    Accepted { document_id: String, job_id: String },
    Failed { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestStageRequested {
    pub v: u16,
    pub command_id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub job_id: String,
    pub document_id: String,
    pub stage: IngestionStage,
    pub pipeline_version: u32,
    pub attempt: u32,
    pub causation_id: String,
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
