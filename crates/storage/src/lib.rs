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

    async fn record_turn_requested(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        user_message_id: &str,
        parent_message_id: Option<&str>,
    ) -> Result<(), StorageError>;

    async fn record_turn_running(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        worker_id: &str,
    ) -> Result<(), StorageError>;

    async fn record_turn_failed(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        error: &str,
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
        if should_bootstrap_schema() {
            repo.bootstrap_with_retry().await?;
        }
        Ok(repo)
    }

    async fn bootstrap_with_retry(&self) -> Result<(), StorageError> {
        let mut attempt = 0usize;
        loop {
            match self.bootstrap().await {
                Ok(()) => return Ok(()),
                Err(error) if is_transient_conflict(&error) && attempt < 20 => {
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
                DEFINE TABLE OVERWRITE tenant SCHEMAFULL;
                DEFINE FIELD OVERWRITE tenant_id ON tenant TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE name ON tenant TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE created_at ON tenant TYPE datetime DEFAULT time::now();
                DEFINE FIELD OVERWRITE metadata ON tenant TYPE object FLEXIBLE DEFAULT {};
                REMOVE INDEX IF EXISTS tenant_id ON tenant;
                DEFINE INDEX IF NOT EXISTS tenant_id ON tenant FIELDS tenant_id UNIQUE;

                DEFINE TABLE OVERWRITE app_user SCHEMAFULL;
                DEFINE FIELD OVERWRITE tenant_id ON app_user TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE user_id ON app_user TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE email ON app_user TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE display_name ON app_user TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE created_at ON app_user TYPE datetime DEFAULT time::now();
                DEFINE FIELD OVERWRITE last_seen_at ON app_user TYPE option<datetime> DEFAULT NONE;
                REMOVE INDEX IF EXISTS app_user_identity ON app_user;
                REMOVE INDEX IF EXISTS app_user_tenant ON app_user;
                DEFINE INDEX IF NOT EXISTS app_user_identity ON app_user FIELDS tenant_id, user_id UNIQUE;
                DEFINE INDEX IF NOT EXISTS app_user_tenant ON app_user FIELDS tenant_id;

                DEFINE TABLE OVERWRITE chat_conversation SCHEMAFULL;
                DEFINE FIELD OVERWRITE conversation_id ON chat_conversation TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE tenant_id ON chat_conversation TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE user_id ON chat_conversation TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE title ON chat_conversation TYPE string DEFAULT 'New chat';
                DEFINE FIELD OVERWRITE created_at ON chat_conversation TYPE datetime DEFAULT time::now();
                DEFINE FIELD OVERWRITE updated_at ON chat_conversation TYPE datetime DEFAULT time::now();
                DEFINE FIELD OVERWRITE deleted_at ON chat_conversation TYPE option<datetime> DEFAULT NONE;
                DEFINE FIELD OVERWRITE next_message_ordinal ON chat_conversation TYPE int DEFAULT 1;
                REMOVE INDEX IF EXISTS chat_conversation_id ON chat_conversation;
                REMOVE INDEX IF EXISTS chat_conversation_owner ON chat_conversation;
                REMOVE INDEX IF EXISTS chat_conversation_updated ON chat_conversation;
                DEFINE INDEX IF NOT EXISTS chat_conversation_id
                    ON chat_conversation FIELDS conversation_id UNIQUE;
                DEFINE INDEX IF NOT EXISTS chat_conversation_owner
                    ON chat_conversation FIELDS tenant_id, user_id, conversation_id UNIQUE;
                DEFINE INDEX IF NOT EXISTS chat_conversation_updated
                    ON chat_conversation FIELDS tenant_id, user_id, updated_at;

                DEFINE TABLE OVERWRITE chat_message SCHEMAFULL;
                DEFINE FIELD OVERWRITE message_id ON chat_message TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE tenant_id ON chat_message TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE user_id ON chat_message TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE conversation_id ON chat_message TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE role ON chat_message TYPE string ASSERT $value INSIDE ['system', 'user', 'assistant', 'tool'];
                DEFINE FIELD OVERWRITE content ON chat_message TYPE string DEFAULT '';
                DEFINE FIELD OVERWRITE parent_message_id ON chat_message TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE citations ON chat_message TYPE array DEFAULT [];
                DEFINE FIELD OVERWRITE turn_id ON chat_message TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE interrupted ON chat_message TYPE bool DEFAULT false;
                DEFINE FIELD OVERWRITE finish_reason ON chat_message TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE ordinal ON chat_message TYPE int ASSERT $value > 0;
                DEFINE FIELD OVERWRITE created_at ON chat_message TYPE datetime DEFAULT time::now();
                REMOVE INDEX IF EXISTS chat_message_order ON chat_message;
                REMOVE INDEX IF EXISTS chat_message_id ON chat_message;
                REMOVE INDEX IF EXISTS chat_message_parent ON chat_message;
                REMOVE INDEX IF EXISTS chat_message_turn ON chat_message;
                REMOVE INDEX IF EXISTS chat_message_ordinal_unique ON chat_message;
                DEFINE INDEX IF NOT EXISTS chat_message_order
                    ON chat_message FIELDS tenant_id, user_id, conversation_id, ordinal;
                DEFINE INDEX IF NOT EXISTS chat_message_id
                    ON chat_message FIELDS tenant_id, message_id UNIQUE;
                DEFINE INDEX IF NOT EXISTS chat_message_ordinal_unique
                    ON chat_message FIELDS tenant_id, conversation_id, ordinal UNIQUE;
                DEFINE INDEX IF NOT EXISTS chat_message_parent
                    ON chat_message FIELDS tenant_id, conversation_id, parent_message_id;
                DEFINE INDEX IF NOT EXISTS chat_message_turn
                    ON chat_message FIELDS tenant_id, conversation_id, turn_id;

                DEFINE TABLE OVERWRITE chat_turn SCHEMAFULL;
                DEFINE FIELD OVERWRITE turn_id ON chat_turn TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE tenant_id ON chat_turn TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE user_id ON chat_turn TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE conversation_id ON chat_turn TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE user_message_id ON chat_turn TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE assistant_message_id ON chat_turn TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE parent_message_id ON chat_turn TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE status ON chat_turn TYPE string ASSERT $value INSIDE ['requested', 'running', 'committed', 'interrupted', 'failed'];
                DEFINE FIELD OVERWRITE worker_id ON chat_turn TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE error ON chat_turn TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE created_at ON chat_turn TYPE datetime DEFAULT time::now();
                DEFINE FIELD OVERWRITE updated_at ON chat_turn TYPE datetime DEFAULT time::now();
                REMOVE INDEX IF EXISTS chat_turn_id ON chat_turn;
                REMOVE INDEX IF EXISTS chat_turn_conversation ON chat_turn;
                REMOVE INDEX IF EXISTS chat_turn_status ON chat_turn;
                DEFINE INDEX IF NOT EXISTS chat_turn_id ON chat_turn FIELDS tenant_id, turn_id UNIQUE;
                DEFINE INDEX IF NOT EXISTS chat_turn_conversation
                    ON chat_turn FIELDS tenant_id, user_id, conversation_id, created_at;
                DEFINE INDEX IF NOT EXISTS chat_turn_status
                    ON chat_turn FIELDS tenant_id, conversation_id, status;
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
    created_at: DateTime<Utc>,
    #[surreal(default)]
    updated_at: DateTime<Utc>,
    #[surreal(default)]
    deleted_at: Option<DateTime<Utc>>,
    #[surreal(default)]
    next_message_ordinal: i64,
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
    ordinal: i64,
    #[surreal(default)]
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct DbChatTurn {
    #[surreal(default)]
    turn_id: String,
    #[surreal(default)]
    tenant_id: String,
    #[surreal(default)]
    user_id: String,
    #[surreal(default)]
    conversation_id: String,
    #[surreal(default)]
    user_message_id: Option<String>,
    #[surreal(default)]
    assistant_message_id: Option<String>,
    #[surreal(default)]
    parent_message_id: Option<String>,
    #[surreal(default)]
    status: String,
    #[surreal(default)]
    worker_id: Option<String>,
    #[surreal(default)]
    error: Option<String>,
    #[surreal(default)]
    created_at: DateTime<Utc>,
    #[surreal(default)]
    updated_at: DateTime<Utc>,
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
        self.ensure_principal(tenant_id, user_id).await?;
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
        self.ensure_principal(tenant_id, user_id).await?;
        let now = Utc::now();
        let row = DbConversation {
            conversation_id: ulid::Ulid::new().to_string(),
            tenant_id: tenant_id.to_owned(),
            user_id: user_id.to_owned(),
            title: title.unwrap_or_else(|| "New chat".to_owned()),
            created_at: now,
            updated_at: now,
            deleted_at: None,
            next_message_ordinal: 1,
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
            .list_messages(tenant_id, user_id, conversation_id)
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
            .list_messages(tenant_id, user_id, conversation_id)
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
                SELECT message_id, ordinal
                FROM chat_message
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND conversation_id = $conversation_id
                ORDER BY ordinal DESC
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
        let rows: Vec<MessageIdRow> = response.take(0).map_err(storage_internal)?;
        let tail = rows.first().map(|row| row.message_id.as_str());
        if tail == parent_message_id {
            Ok(())
        } else {
            Err(StorageError::StaleParent)
        }
    }

    async fn record_turn_requested(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        user_message_id: &str,
        parent_message_id: Option<&str>,
    ) -> Result<(), StorageError> {
        self.get_visible_conversation(tenant_id, user_id, conversation_id)
            .await?;
        self.upsert_turn(DbChatTurn {
            turn_id: turn_id.to_owned(),
            tenant_id: tenant_id.to_owned(),
            user_id: user_id.to_owned(),
            conversation_id: conversation_id.to_owned(),
            user_message_id: Some(user_message_id.to_owned()),
            assistant_message_id: None,
            parent_message_id: parent_message_id.map(str::to_owned),
            status: "requested".to_owned(),
            worker_id: None,
            error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
        .await
    }

    async fn record_turn_running(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        worker_id: &str,
    ) -> Result<(), StorageError> {
        self.get_visible_conversation(tenant_id, user_id, conversation_id)
            .await?;
        let now = Utc::now();
        self.db
            .query(
                "
                UPDATE chat_turn
                SET status = 'running', worker_id = $worker_id, error = NONE, updated_at = $updated_at
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND conversation_id = $conversation_id
                  AND turn_id = $turn_id
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
            .bind(("conversation_id", conversation_id.to_owned()))
            .bind(("turn_id", turn_id.to_owned()))
            .bind(("worker_id", worker_id.to_owned()))
            .bind(("updated_at", now))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        Ok(())
    }

    async fn record_turn_failed(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        error: &str,
    ) -> Result<(), StorageError> {
        self.get_visible_conversation(tenant_id, user_id, conversation_id)
            .await?;
        let now = Utc::now();
        self.db
            .query(
                "
                UPDATE chat_turn
                SET status = 'failed', error = $error, updated_at = $updated_at
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND conversation_id = $conversation_id
                  AND turn_id = $turn_id
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
            .bind(("conversation_id", conversation_id.to_owned()))
            .bind(("turn_id", turn_id.to_owned()))
            .bind(("error", error.to_owned()))
            .bind(("updated_at", now))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        Ok(())
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
        let parent_ordinal = self
            .parent_ordinal(tenant_id, user_id, conversation_id, parent_message_id)
            .await?;
        let user_ordinal = parent_ordinal + 1;
        let assistant_ordinal = parent_ordinal + 2;
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
            ordinal: user_ordinal,
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
            ordinal: assistant_ordinal,
            created_at: assistant_created_at,
        };
        let title: String = user_text.chars().take(48).collect();
        let turn_status = if interrupted {
            "interrupted"
        } else {
            "committed"
        };

        self.db
            .query(
                "
                BEGIN;
                DELETE chat_message
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND conversation_id = $conversation_id
                  AND ordinal > $parent_ordinal;
                CREATE chat_message CONTENT $user_message;
                CREATE chat_message CONTENT $assistant_message;
                UPDATE chat_conversation
                SET
                    updated_at = $updated_at,
                    title = IF title = 'New chat' THEN $title ELSE title END,
                    next_message_ordinal = $next_message_ordinal
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND conversation_id = $conversation_id
                  AND deleted_at = NONE;
                UPDATE chat_turn
                SET
                    status = $turn_status,
                    user_message_id = $user_message_id,
                    assistant_message_id = $assistant_message_id,
                    parent_message_id = $parent_message_id,
                    error = NONE,
                    updated_at = $updated_at
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND conversation_id = $conversation_id
                  AND turn_id = $turn_id;
                COMMIT;
                ",
            )
            .bind(("user_message", user))
            .bind(("assistant_message", assistant))
            .bind(("updated_at", now))
            .bind(("title", title))
            .bind(("next_message_ordinal", assistant_ordinal + 1))
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
            .bind(("conversation_id", conversation_id.to_owned()))
            .bind(("parent_message_id", parent_message_id.map(str::to_owned)))
            .bind(("parent_ordinal", parent_ordinal))
            .bind(("turn_status", turn_status.to_owned()))
            .bind(("turn_id", turn_id.to_owned()))
            .bind(("user_message_id", user_message_id.to_owned()))
            .bind(("assistant_message_id", assistant_message_id.to_owned()))
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
    #[surreal(default)]
    ordinal: i64,
}

#[derive(Debug, Deserialize, SurrealValue)]
struct TenantIdRow {
    #[surreal(default)]
    tenant_id: String,
}

#[derive(Debug, Deserialize, SurrealValue)]
struct UserIdRow {
    #[surreal(default)]
    user_id: String,
}

#[derive(Debug, Deserialize, SurrealValue)]
struct TurnIdRow {
    #[surreal(default)]
    turn_id: String,
}

impl SurrealChatRepository {
    async fn ensure_principal(&self, tenant_id: &str, user_id: &str) -> Result<(), StorageError> {
        let now = Utc::now();
        let mut tenant_response = self
            .db
            .query(
                "
                SELECT tenant_id
                FROM tenant
                WHERE tenant_id = $tenant_id
                LIMIT 1
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        let tenants: Vec<TenantIdRow> = tenant_response.take(0).map_err(storage_internal)?;
        if tenants.is_empty() {
            self.db
                .query(
                    "
                    CREATE tenant CONTENT {
                        tenant_id: $tenant_id,
                        name: $tenant_id,
                        created_at: $created_at,
                        metadata: {}
                    }
                    ",
                )
                .bind(("tenant_id", tenant_id.to_owned()))
                .bind(("created_at", now))
                .await
                .map_err(storage_internal)?
                .check()
                .map_err(storage_internal)?;
        }

        let mut user_response = self
            .db
            .query(
                "
                SELECT user_id
                FROM app_user
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                LIMIT 1
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        let users: Vec<UserIdRow> = user_response.take(0).map_err(storage_internal)?;
        if users.is_empty() {
            self.db
                .query(
                    "
                    CREATE app_user CONTENT {
                        tenant_id: $tenant_id,
                        user_id: $user_id,
                        email: NONE,
                        display_name: NONE,
                        created_at: $created_at,
                        last_seen_at: $created_at
                    }
                    ",
                )
                .bind(("tenant_id", tenant_id.to_owned()))
                .bind(("user_id", user_id.to_owned()))
                .bind(("created_at", now))
                .await
                .map_err(storage_internal)?
                .check()
                .map_err(storage_internal)?;
        } else {
            self.db
                .query(
                    "
                    UPDATE app_user
                    SET last_seen_at = $last_seen_at
                    WHERE tenant_id = $tenant_id
                      AND user_id = $user_id
                    ",
                )
                .bind(("tenant_id", tenant_id.to_owned()))
                .bind(("user_id", user_id.to_owned()))
                .bind(("last_seen_at", now))
                .await
                .map_err(storage_internal)?
                .check()
                .map_err(storage_internal)?;
        }
        Ok(())
    }

    async fn upsert_turn(&self, turn: DbChatTurn) -> Result<(), StorageError> {
        let mut response = self
            .db
            .query(
                "
                SELECT turn_id
                FROM chat_turn
                WHERE tenant_id = $tenant_id
                  AND turn_id = $turn_id
                LIMIT 1
                ",
            )
            .bind(("tenant_id", turn.tenant_id.clone()))
            .bind(("turn_id", turn.turn_id.clone()))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        let rows: Vec<TurnIdRow> = response.take(0).map_err(storage_internal)?;
        if rows.is_empty() {
            self.db
                .query("CREATE chat_turn CONTENT $data")
                .bind(("data", turn))
                .await
                .map_err(storage_internal)?
                .check()
                .map_err(storage_internal)?;
        } else {
            self.db
                .query(
                    "
                    UPDATE chat_turn
                    SET
                        user_id = $user_id,
                        conversation_id = $conversation_id,
                        user_message_id = $user_message_id,
                        assistant_message_id = $assistant_message_id,
                        parent_message_id = $parent_message_id,
                        status = $status,
                        worker_id = $worker_id,
                        error = $error,
                        updated_at = $updated_at
                    WHERE tenant_id = $tenant_id
                      AND turn_id = $turn_id
                    ",
                )
                .bind(("tenant_id", turn.tenant_id))
                .bind(("turn_id", turn.turn_id))
                .bind(("user_id", turn.user_id))
                .bind(("conversation_id", turn.conversation_id))
                .bind(("user_message_id", turn.user_message_id))
                .bind(("assistant_message_id", turn.assistant_message_id))
                .bind(("parent_message_id", turn.parent_message_id))
                .bind(("status", turn.status))
                .bind(("worker_id", turn.worker_id))
                .bind(("error", turn.error))
                .bind(("updated_at", turn.updated_at))
                .await
                .map_err(storage_internal)?
                .check()
                .map_err(storage_internal)?;
        }
        Ok(())
    }

    async fn parent_ordinal(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        parent_message_id: Option<&str>,
    ) -> Result<i64, StorageError> {
        let Some(parent_message_id) = parent_message_id else {
            return Ok(0);
        };
        let mut response = self
            .db
            .query(
                "
                SELECT ordinal
                FROM chat_message
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND conversation_id = $conversation_id
                  AND message_id = $parent_message_id
                LIMIT 1
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
            .bind(("conversation_id", conversation_id.to_owned()))
            .bind(("parent_message_id", parent_message_id.to_owned()))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        let rows: Vec<MessageIdRow> = response.take(0).map_err(storage_internal)?;
        rows.first()
            .map(|row| row.ordinal)
            .ok_or(StorageError::StaleParent)
    }

    async fn get_visible_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<DbConversation, StorageError> {
        self.ensure_principal(tenant_id, user_id).await?;
        let mut response = self
            .db
            .query(
                "
                SELECT conversation_id, tenant_id, user_id, title, created_at, updated_at, deleted_at, next_message_ordinal
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
        user_id: &str,
        conversation_id: &str,
    ) -> Result<Vec<DbMessage>, StorageError> {
        let mut response = self
            .db
            .query(
                "
                SELECT message_id, tenant_id, user_id, conversation_id, role, content, parent_message_id, citations, turn_id, interrupted, finish_reason, ordinal, created_at
                FROM chat_message
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND conversation_id = $conversation_id
                ORDER BY ordinal ASC
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
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

fn should_bootstrap_schema() -> bool {
    std::env::var("SURREAL_BOOTSTRAP_SCHEMA")
        .map(|value| {
            let value = value.trim().to_ascii_lowercase();
            value != "0" && value != "false" && value != "no"
        })
        .unwrap_or(true)
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
    next_message_ordinal: i64,
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
            next_message_ordinal: 1,
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

    async fn record_turn_requested(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        _turn_id: &str,
        _user_message_id: &str,
        _parent_message_id: Option<&str>,
    ) -> Result<(), StorageError> {
        let state = self.inner.lock().await;
        let row = state
            .conversations
            .get(conversation_id)
            .ok_or(StorageError::NotFound)?;
        ensure_visible(row, tenant_id, user_id)
    }

    async fn record_turn_running(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        _turn_id: &str,
        _worker_id: &str,
    ) -> Result<(), StorageError> {
        let state = self.inner.lock().await;
        let row = state
            .conversations
            .get(conversation_id)
            .ok_or(StorageError::NotFound)?;
        ensure_visible(row, tenant_id, user_id)
    }

    async fn record_turn_failed(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        _turn_id: &str,
        _error: &str,
    ) -> Result<(), StorageError> {
        let state = self.inner.lock().await;
        let row = state
            .conversations
            .get(conversation_id)
            .ok_or(StorageError::NotFound)?;
        ensure_visible(row, tenant_id, user_id)
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
        let retain_len = match parent_message_id {
            Some(parent_id) => row
                .messages
                .iter()
                .position(|message| message.id == parent_id)
                .map(|position| position + 1)
                .ok_or(StorageError::StaleParent)?,
            None => 0,
        };
        row.messages.truncate(retain_len);
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
        row.next_message_ordinal = row.messages.len() as i64 + 1;

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
