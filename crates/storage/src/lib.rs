use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use delphi_contracts::{
    CitationEntry, ConversationDetail, ConversationSummary, MessageDto, MessageRole,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use surrealdb::engine::any::{self, Any};
use surrealdb::opt::auth::Root;
use surrealdb::types::SurrealValue;
use surrealdb::Surreal;
use thiserror::Error;
use tokio::sync::Mutex;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("not found")]
    NotFound,
    #[error("forbidden")]
    Forbidden,
    #[error("stale parent")]
    StaleParent,
    #[error("storage error: {0}")]
    Internal(String),
}

#[async_trait]
pub trait ChatRepository: Clone + Send + Sync + 'static {
    async fn list_conversations(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Vec<ConversationSummary>, StorageError>;

    async fn create_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        title: Option<String>,
    ) -> Result<ConversationDetail, StorageError>;

    async fn get_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<ConversationDetail, StorageError>;

    async fn rename_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        title: String,
    ) -> Result<ConversationDetail, StorageError>;

    async fn delete_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<(), StorageError>;

    async fn assert_parent_tail(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        parent_message_id: Option<&str>,
    ) -> Result<(), StorageError>;

    async fn commit_turn(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        user_message_id: &str,
        user_text: &str,
        parent_message_id: Option<&str>,
        assistant_message_id: &str,
        assistant_text: &str,
        citations: Vec<CitationEntry>,
    ) -> Result<ConversationDetail, StorageError>;

    async fn commit_interrupted_turn(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        user_message_id: &str,
        user_text: &str,
        parent_message_id: Option<&str>,
        assistant_message_id: &str,
        assistant_text: &str,
        citations: Vec<CitationEntry>,
    ) -> Result<ConversationDetail, StorageError>;
}

#[derive(Debug, Clone)]
pub struct SurrealChatRepository {
    db: Surreal<Any>,
}

impl SurrealChatRepository {
    pub async fn connect(
        url: &str,
        namespace: &str,
        database: &str,
        username: &str,
        password: &str,
    ) -> Result<Self, StorageError> {
        let db = any::connect(url)
            .await
            .map_err(|error| StorageError::Internal(format!("connect to SurrealDB: {error}")))?;
        db.signin(Root {
            username: username.to_owned(),
            password: password.to_owned(),
        })
        .await
        .map_err(|error| StorageError::Internal(format!("sign in to SurrealDB: {error}")))?;
        db.use_ns(namespace)
            .use_db(database)
            .await
            .map_err(|error| {
                StorageError::Internal(format!("select SurrealDB namespace: {error}"))
            })?;

        let repo = Self { db };
        repo.bootstrap_with_retry().await?;
        Ok(repo)
    }

    async fn bootstrap_with_retry(&self) -> Result<(), StorageError> {
        let mut attempt = 0usize;
        loop {
            match self.bootstrap().await {
                Ok(()) => return Ok(()),
                Err(error) if is_transient_conflict(&error) && attempt < 4 => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(25 * attempt as u64)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn bootstrap(&self) -> Result<(), StorageError> {
        self.db
            .query(
                "
                DEFINE TABLE IF NOT EXISTS chat_conversation SCHEMALESS;
                DEFINE TABLE IF NOT EXISTS chat_message SCHEMALESS;
                DEFINE INDEX IF NOT EXISTS chat_conversation_owner
                    ON chat_conversation FIELDS tenant_id, user_id, conversation_id UNIQUE;
                DEFINE INDEX IF NOT EXISTS chat_conversation_updated
                    ON chat_conversation FIELDS tenant_id, user_id, updated_at;
                DEFINE INDEX IF NOT EXISTS chat_message_order
                    ON chat_message FIELDS tenant_id, conversation_id, created_at;
                DEFINE INDEX IF NOT EXISTS chat_message_id
                    ON chat_message FIELDS tenant_id, message_id UNIQUE;
                DEFINE INDEX IF NOT EXISTS chat_message_parent
                    ON chat_message FIELDS tenant_id, conversation_id, parent_message_id;
                ",
            )
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct DbConversation {
    #[surreal(default)]
    conversation_id: String,
    #[surreal(default)]
    tenant_id: String,
    #[surreal(default)]
    user_id: String,
    #[surreal(default)]
    title: String,
    #[surreal(default)]
    updated_at: DateTime<Utc>,
    #[surreal(default)]
    deleted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct DbMessage {
    #[surreal(default)]
    message_id: String,
    #[surreal(default)]
    tenant_id: String,
    #[surreal(default)]
    user_id: String,
    #[surreal(default)]
    conversation_id: String,
    #[surreal(default)]
    role: String,
    #[surreal(default)]
    content: String,
    #[surreal(default)]
    parent_message_id: Option<String>,
    #[surreal(default)]
    citations: Vec<CitationRow>,
    #[surreal(default)]
    turn_id: Option<String>,
    #[surreal(default)]
    interrupted: bool,
    #[surreal(default)]
    finish_reason: Option<String>,
    #[surreal(default)]
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct CitationRow {
    #[surreal(default)]
    index: u32,
    #[surreal(default)]
    label: String,
    #[surreal(default)]
    url: Option<String>,
}

#[async_trait]
impl ChatRepository for SurrealChatRepository {
    async fn list_conversations(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Vec<ConversationSummary>, StorageError> {
        let mut response = self
            .db
            .query(
                "
                SELECT conversation_id, title, updated_at
                FROM chat_conversation
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND deleted_at = NONE
                ORDER BY updated_at DESC
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        let rows: Vec<ConversationSummaryRow> = response.take(0).map_err(storage_internal)?;
        Ok(rows
            .into_iter()
            .map(|row| ConversationSummary {
                id: row.conversation_id,
                title: row.title,
                updated_at: row.updated_at,
            })
            .collect())
    }

    async fn create_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        title: Option<String>,
    ) -> Result<ConversationDetail, StorageError> {
        let row = DbConversation {
            conversation_id: ulid::Ulid::new().to_string(),
            tenant_id: tenant_id.to_owned(),
            user_id: user_id.to_owned(),
            title: title.unwrap_or_else(|| "New chat".to_owned()),
            updated_at: Utc::now(),
            deleted_at: None,
        };

        self.db
            .query("CREATE chat_conversation CONTENT $data")
            .bind(("data", row.clone()))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        Ok(to_surreal_detail(row, Vec::new()))
    }

    async fn get_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<ConversationDetail, StorageError> {
        let conversation = self
            .get_visible_conversation(tenant_id, user_id, conversation_id)
            .await?;
        let messages = self
            .list_messages(tenant_id, conversation_id)
            .await?
            .into_iter()
            .map(db_message_to_dto)
            .collect();
        Ok(to_surreal_detail(conversation, messages))
    }

    async fn rename_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        title: String,
    ) -> Result<ConversationDetail, StorageError> {
        let now = Utc::now();
        let mut response = self
            .db
            .query(
                "
                UPDATE chat_conversation
                SET title = $title, updated_at = $updated_at
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND conversation_id = $conversation_id
                  AND deleted_at = NONE
                RETURN conversation_id, tenant_id, user_id, title, updated_at, deleted_at
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
            .bind(("conversation_id", conversation_id.to_owned()))
            .bind(("title", title))
            .bind(("updated_at", now))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        let rows: Vec<DbConversation> = response.take(0).map_err(storage_internal)?;
        let conversation = rows.into_iter().next().ok_or(StorageError::NotFound)?;
        let messages = self
            .list_messages(tenant_id, conversation_id)
            .await?
            .into_iter()
            .map(db_message_to_dto)
            .collect();
        Ok(to_surreal_detail(conversation, messages))
    }

    async fn delete_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<(), StorageError> {
        let now = Utc::now();
        let mut response = self
            .db
            .query(
                "
                UPDATE chat_conversation
                SET deleted_at = $deleted_at, updated_at = $deleted_at
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND conversation_id = $conversation_id
                  AND deleted_at = NONE
                RETURN conversation_id
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
            .bind(("conversation_id", conversation_id.to_owned()))
            .bind(("deleted_at", now))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        let rows: Vec<ConversationIdRow> = response.take(0).map_err(storage_internal)?;
        if rows.is_empty() {
            Err(StorageError::NotFound)
        } else {
            Ok(())
        }
    }

    async fn assert_parent_tail(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        parent_message_id: Option<&str>,
    ) -> Result<(), StorageError> {
        self.get_visible_conversation(tenant_id, user_id, conversation_id)
            .await?;
        let mut response = self
            .db
            .query(
                "
                SELECT message_id, created_at
                FROM chat_message
                WHERE tenant_id = $tenant_id
                  AND conversation_id = $conversation_id
                ORDER BY created_at DESC
                LIMIT 1
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("conversation_id", conversation_id.to_owned()))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        let rows: Vec<MessageIdRow> = response.take(0).map_err(storage_internal)?;
        let tail = rows.first().map(|row| row.message_id.as_str());
        if tail == parent_message_id {
            Ok(())
        } else {
            Err(StorageError::StaleParent)
        }
    }

    async fn commit_turn(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        user_message_id: &str,
        user_text: &str,
        parent_message_id: Option<&str>,
        assistant_message_id: &str,
        assistant_text: &str,
        citations: Vec<CitationEntry>,
    ) -> Result<ConversationDetail, StorageError> {
        self.commit_turn_with_metadata(
            tenant_id,
            user_id,
            conversation_id,
            turn_id,
            user_message_id,
            user_text,
            parent_message_id,
            assistant_message_id,
            assistant_text,
            citations,
            false,
            None,
        )
        .await
    }

    async fn commit_interrupted_turn(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        user_message_id: &str,
        user_text: &str,
        parent_message_id: Option<&str>,
        assistant_message_id: &str,
        assistant_text: &str,
        citations: Vec<CitationEntry>,
    ) -> Result<ConversationDetail, StorageError> {
        self.commit_turn_with_metadata(
            tenant_id,
            user_id,
            conversation_id,
            turn_id,
            user_message_id,
            user_text,
            parent_message_id,
            assistant_message_id,
            assistant_text,
            citations,
            true,
            Some("user_interrupted".to_owned()),
        )
        .await
    }
}

impl SurrealChatRepository {
    async fn commit_turn_with_metadata(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        user_message_id: &str,
        user_text: &str,
        parent_message_id: Option<&str>,
        assistant_message_id: &str,
        assistant_text: &str,
        citations: Vec<CitationEntry>,
        interrupted: bool,
        finish_reason: Option<String>,
    ) -> Result<ConversationDetail, StorageError> {
        self.get_visible_conversation(tenant_id, user_id, conversation_id)
            .await?;

        let now = Utc::now();
        let assistant_created_at = now + Duration::milliseconds(1);
        let user = DbMessage {
            message_id: user_message_id.to_owned(),
            tenant_id: tenant_id.to_owned(),
            user_id: user_id.to_owned(),
            conversation_id: conversation_id.to_owned(),
            role: "user".to_owned(),
            content: user_text.to_owned(),
            parent_message_id: parent_message_id.map(str::to_owned),
            citations: Vec::new(),
            turn_id: Some(turn_id.to_owned()),
            interrupted: false,
            finish_reason: None,
            created_at: now,
        };
        let assistant = DbMessage {
            message_id: assistant_message_id.to_owned(),
            tenant_id: tenant_id.to_owned(),
            user_id: user_id.to_owned(),
            conversation_id: conversation_id.to_owned(),
            role: "assistant".to_owned(),
            content: assistant_text.to_owned(),
            parent_message_id: Some(user_message_id.to_owned()),
            citations: citations.into_iter().map(CitationRow::from).collect(),
            turn_id: Some(turn_id.to_owned()),
            interrupted,
            finish_reason,
            created_at: assistant_created_at,
        };
        let title: String = user_text.chars().take(48).collect();

        self.db
            .query(
                "
                BEGIN;
                LET $parent_rows = SELECT created_at
                    FROM chat_message
                    WHERE tenant_id = $tenant_id
                      AND conversation_id = $conversation_id
                      AND message_id = $parent_message_id
                    LIMIT 1;
                LET $parent_created_at = IF $parent_message_id != NONE
                    THEN $parent_rows[0].created_at
                    ELSE time::EPOCH
                    END;
                DELETE chat_message
                WHERE tenant_id = $tenant_id
                  AND conversation_id = $conversation_id
                  AND created_at > $parent_created_at;
                CREATE chat_message CONTENT $user_message;
                CREATE chat_message CONTENT $assistant_message;
                UPDATE chat_conversation
                SET
                    updated_at = $updated_at,
                    title = IF title = 'New chat' THEN $title ELSE title END
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND conversation_id = $conversation_id
                  AND deleted_at = NONE;
                COMMIT;
                ",
            )
            .bind(("user_message", user))
            .bind(("assistant_message", assistant))
            .bind(("updated_at", now))
            .bind(("title", title))
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
            .bind(("conversation_id", conversation_id.to_owned()))
            .bind(("parent_message_id", parent_message_id.map(str::to_owned)))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;

        self.get_conversation(tenant_id, user_id, conversation_id)
            .await
    }
}

#[derive(Debug, Deserialize, SurrealValue)]
struct ConversationSummaryRow {
    #[surreal(default)]
    conversation_id: String,
    #[surreal(default)]
    title: String,
    #[surreal(default)]
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, SurrealValue)]
struct ConversationIdRow {
    #[surreal(default)]
    conversation_id: String,
}

#[derive(Debug, Deserialize, SurrealValue)]
struct MessageIdRow {
    #[surreal(default)]
    message_id: String,
}

impl SurrealChatRepository {
    async fn get_visible_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<DbConversation, StorageError> {
        let mut response = self
            .db
            .query(
                "
                SELECT conversation_id, tenant_id, user_id, title, updated_at, deleted_at
                FROM chat_conversation
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND conversation_id = $conversation_id
                  AND deleted_at = NONE
                LIMIT 1
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
            .bind(("conversation_id", conversation_id.to_owned()))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        let rows: Vec<DbConversation> = response.take(0).map_err(storage_internal)?;
        rows.into_iter().next().ok_or(StorageError::NotFound)
    }

    async fn list_messages(
        &self,
        tenant_id: &str,
        conversation_id: &str,
    ) -> Result<Vec<DbMessage>, StorageError> {
        let mut response = self
            .db
            .query(
                "
                SELECT message_id, tenant_id, user_id, conversation_id, role, content, parent_message_id, citations, turn_id, interrupted, finish_reason, created_at
                FROM chat_message
                WHERE tenant_id = $tenant_id
                  AND conversation_id = $conversation_id
                ORDER BY created_at ASC
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("conversation_id", conversation_id.to_owned()))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        response.take(0).map_err(storage_internal)
    }
}

fn to_surreal_detail(row: DbConversation, messages: Vec<MessageDto>) -> ConversationDetail {
    ConversationDetail {
        id: row.conversation_id,
        title: row.title,
        messages,
        updated_at: row.updated_at,
    }
}

fn db_message_to_dto(row: DbMessage) -> MessageDto {
    MessageDto {
        id: row.message_id,
        role: match row.role.as_str() {
            "assistant" => MessageRole::Assistant,
            "system" => MessageRole::System,
            _ => MessageRole::User,
        },
        content: row.content,
        parent_message_id: row.parent_message_id,
        citations: row.citations.into_iter().map(CitationEntry::from).collect(),
        turn_id: row.turn_id,
        interrupted: row.interrupted,
        finish_reason: row.finish_reason,
        created_at: row.created_at,
    }
}

impl From<CitationEntry> for CitationRow {
    fn from(value: CitationEntry) -> Self {
        Self {
            index: value.index,
            label: value.label,
            url: value.url,
        }
    }
}

impl From<CitationRow> for CitationEntry {
    fn from(value: CitationRow) -> Self {
        Self {
            index: value.index,
            label: value.label,
            url: value.url,
        }
    }
}

fn storage_internal(error: impl std::fmt::Display) -> StorageError {
    StorageError::Internal(error.to_string())
}

fn is_transient_conflict(error: &StorageError) -> bool {
    let StorageError::Internal(message) = error else {
        return false;
    };
    message.contains("Transaction conflict")
        || message.contains("Write conflict")
        || message.contains("Resource busy")
}

#[derive(Debug, Clone, Default)]
pub struct MemoryChatRepository {
    inner: Arc<Mutex<MemoryState>>,
}

#[derive(Debug, Default)]
struct MemoryState {
    conversations: HashMap<String, StoredConversation>,
}

#[derive(Debug, Clone)]
struct StoredConversation {
    id: String,
    tenant_id: String,
    user_id: String,
    title: String,
    updated_at: DateTime<Utc>,
    deleted_at: Option<DateTime<Utc>>,
    messages: Vec<MessageDto>,
}

#[async_trait]
impl ChatRepository for MemoryChatRepository {
    async fn list_conversations(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Vec<ConversationSummary>, StorageError> {
        let state = self.inner.lock().await;
        let mut rows = state
            .conversations
            .values()
            .filter(|row| visible_to(row, tenant_id, user_id))
            .map(|row| ConversationSummary {
                id: row.id.clone(),
                title: row.title.clone(),
                updated_at: row.updated_at,
            })
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| std::cmp::Reverse(row.updated_at));
        Ok(rows)
    }

    async fn create_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        title: Option<String>,
    ) -> Result<ConversationDetail, StorageError> {
        let now = Utc::now();
        let id = ulid::Ulid::new().to_string();
        let row = StoredConversation {
            id: id.clone(),
            tenant_id: tenant_id.to_owned(),
            user_id: user_id.to_owned(),
            title: title.unwrap_or_else(|| "New chat".to_owned()),
            updated_at: now,
            deleted_at: None,
            messages: Vec::new(),
        };

        let detail = to_detail(&row);
        self.inner.lock().await.conversations.insert(id, row);
        Ok(detail)
    }

    async fn get_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<ConversationDetail, StorageError> {
        let state = self.inner.lock().await;
        let row = state
            .conversations
            .get(conversation_id)
            .ok_or(StorageError::NotFound)?;
        ensure_visible(row, tenant_id, user_id)?;
        Ok(to_detail(row))
    }

    async fn rename_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        title: String,
    ) -> Result<ConversationDetail, StorageError> {
        let mut state = self.inner.lock().await;
        let row = state
            .conversations
            .get_mut(conversation_id)
            .ok_or(StorageError::NotFound)?;
        ensure_visible(row, tenant_id, user_id)?;
        row.title = title;
        row.updated_at = Utc::now();
        Ok(to_detail(row))
    }

    async fn delete_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<(), StorageError> {
        let mut state = self.inner.lock().await;
        let row = state
            .conversations
            .get_mut(conversation_id)
            .ok_or(StorageError::NotFound)?;
        ensure_visible(row, tenant_id, user_id)?;
        row.deleted_at = Some(Utc::now());
        Ok(())
    }

    async fn assert_parent_tail(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        parent_message_id: Option<&str>,
    ) -> Result<(), StorageError> {
        let state = self.inner.lock().await;
        let row = state
            .conversations
            .get(conversation_id)
            .ok_or(StorageError::NotFound)?;
        ensure_visible(row, tenant_id, user_id)?;
        let tail = row.messages.last().map(|message| message.id.as_str());
        if tail == parent_message_id {
            Ok(())
        } else {
            Err(StorageError::StaleParent)
        }
    }

    async fn commit_turn(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        user_message_id: &str,
        user_text: &str,
        parent_message_id: Option<&str>,
        assistant_message_id: &str,
        assistant_text: &str,
        citations: Vec<CitationEntry>,
    ) -> Result<ConversationDetail, StorageError> {
        self.commit_turn_with_metadata(
            tenant_id,
            user_id,
            conversation_id,
            turn_id,
            user_message_id,
            user_text,
            parent_message_id,
            assistant_message_id,
            assistant_text,
            citations,
            false,
            None,
        )
        .await
    }

    async fn commit_interrupted_turn(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        user_message_id: &str,
        user_text: &str,
        parent_message_id: Option<&str>,
        assistant_message_id: &str,
        assistant_text: &str,
        citations: Vec<CitationEntry>,
    ) -> Result<ConversationDetail, StorageError> {
        self.commit_turn_with_metadata(
            tenant_id,
            user_id,
            conversation_id,
            turn_id,
            user_message_id,
            user_text,
            parent_message_id,
            assistant_message_id,
            assistant_text,
            citations,
            true,
            Some("user_interrupted".to_owned()),
        )
        .await
    }
}

impl MemoryChatRepository {
    async fn commit_turn_with_metadata(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        user_message_id: &str,
        user_text: &str,
        parent_message_id: Option<&str>,
        assistant_message_id: &str,
        assistant_text: &str,
        citations: Vec<CitationEntry>,
        interrupted: bool,
        finish_reason: Option<String>,
    ) -> Result<ConversationDetail, StorageError> {
        let mut state = self.inner.lock().await;
        let row = state
            .conversations
            .get_mut(conversation_id)
            .ok_or(StorageError::NotFound)?;
        ensure_visible(row, tenant_id, user_id)?;

        let now = Utc::now();
        let parent_created_at = match parent_message_id {
            Some(parent_id) => row
                .messages
                .iter()
                .find(|message| message.id == parent_id)
                .map(|message| message.created_at)
                .ok_or(StorageError::StaleParent)?,
            None => DateTime::<Utc>::MIN_UTC,
        };
        row.messages
            .retain(|message| message.created_at <= parent_created_at);
        row.messages.push(MessageDto {
            id: user_message_id.to_owned(),
            role: MessageRole::User,
            content: user_text.to_owned(),
            parent_message_id: parent_message_id.map(str::to_owned),
            citations: Vec::new(),
            turn_id: Some(turn_id.to_owned()),
            interrupted: false,
            finish_reason: None,
            created_at: now,
        });
        row.messages.push(MessageDto {
            id: assistant_message_id.to_owned(),
            role: MessageRole::Assistant,
            content: assistant_text.to_owned(),
            parent_message_id: Some(user_message_id.to_owned()),
            citations,
            turn_id: Some(turn_id.to_owned()),
            interrupted,
            finish_reason,
            created_at: now,
        });
        row.updated_at = now;
        if row.title == "New chat" {
            row.title = user_text.chars().take(48).collect();
        }

        Ok(to_detail(row))
    }
}

fn visible_to(row: &StoredConversation, tenant_id: &str, user_id: &str) -> bool {
    row.deleted_at.is_none() && row.tenant_id == tenant_id && row.user_id == user_id
}

fn ensure_visible(
    row: &StoredConversation,
    tenant_id: &str,
    user_id: &str,
) -> Result<(), StorageError> {
    if visible_to(row, tenant_id, user_id) {
        Ok(())
    } else if row.tenant_id == tenant_id {
        Err(StorageError::Forbidden)
    } else {
        Err(StorageError::NotFound)
    }
}

fn to_detail(row: &StoredConversation) -> ConversationDetail {
    ConversationDetail {
        id: row.id.clone(),
        title: row.title.clone(),
        messages: row.messages.clone(),
        updated_at: row.updated_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TENANT: &str = "tenant-a";
    const USER: &str = "user-a";

    #[tokio::test]
    async fn memory_commit_turn_prunes_messages_newer_than_parent() {
        let repo = MemoryChatRepository::default();
        let conversation = repo
            .create_conversation(TENANT, USER, Some("Branch test".to_owned()))
            .await
            .unwrap();

        repo.commit_turn(
            TENANT,
            USER,
            &conversation.id,
            "turn-1",
            "user-1",
            "first",
            None,
            "assistant-1",
            "first answer",
            Vec::new(),
        )
        .await
        .unwrap();
        repo.commit_turn(
            TENANT,
            USER,
            &conversation.id,
            "turn-2",
            "user-2",
            "second",
            Some("assistant-1"),
            "assistant-2",
            "second answer",
            Vec::new(),
        )
        .await
        .unwrap();

        let branched = repo
            .commit_turn(
                TENANT,
                USER,
                &conversation.id,
                "turn-3",
                "user-3",
                "replacement second",
                Some("assistant-1"),
                "assistant-3",
                "replacement answer",
                Vec::new(),
            )
            .await
            .unwrap();

        let ids = branched
            .messages
            .iter()
            .map(|message| message.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, ["user-1", "assistant-1", "user-3", "assistant-3"]);
        assert_eq!(
            branched.messages[2].parent_message_id.as_deref(),
            Some("assistant-1")
        );
        assert_eq!(
            branched.messages[3].parent_message_id.as_deref(),
            Some("user-3")
        );
    }

    #[tokio::test]
    async fn memory_first_turn_commit_prunes_existing_history() {
        let repo = MemoryChatRepository::default();
        let conversation = repo
            .create_conversation(TENANT, USER, Some("First turn reset".to_owned()))
            .await
            .unwrap();

        repo.commit_turn(
            TENANT,
            USER,
            &conversation.id,
            "turn-1",
            "user-1",
            "stale first",
            None,
            "assistant-1",
            "stale answer",
            Vec::new(),
        )
        .await
        .unwrap();

        let replaced = repo
            .commit_turn(
                TENANT,
                USER,
                &conversation.id,
                "turn-2",
                "user-2",
                "real first",
                None,
                "assistant-2",
                "real answer",
                Vec::new(),
            )
            .await
            .unwrap();

        let ids = replaced
            .messages
            .iter()
            .map(|message| message.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, ["user-2", "assistant-2"]);
        assert_eq!(replaced.messages[0].parent_message_id, None);
        assert_eq!(
            replaced.messages[1].parent_message_id.as_deref(),
            Some("user-2")
        );
    }

    #[tokio::test]
    async fn memory_interrupted_turn_commits_partial_assistant_metadata() {
        let repo = MemoryChatRepository::default();
        let conversation = repo
            .create_conversation(TENANT, USER, Some("Interrupted".to_owned()))
            .await
            .unwrap();

        let detail = repo
            .commit_interrupted_turn(
                TENANT,
                USER,
                &conversation.id,
                "turn-1",
                "user-1",
                "tell me more",
                None,
                "assistant-1",
                "partial answer",
                Vec::new(),
            )
            .await
            .unwrap();

        assert_eq!(detail.messages.len(), 2);
        assert!(!detail.messages[0].interrupted);
        assert_eq!(detail.messages[1].content, "partial answer");
        assert!(detail.messages[1].interrupted);
        assert_eq!(
            detail.messages[1].finish_reason.as_deref(),
            Some("user_interrupted")
        );
    }
}
