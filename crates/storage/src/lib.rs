//! Chat persistence. The document path moved to `delphi-document-adapters`;
//! this crate is slated for the same event-sourced rewrite.
//!
//! Wide argument lists below are pre-existing chat code: the turn commit
//! takes the whole turn positionally. They go away with that rewrite.
#![allow(clippy::too_many_arguments)]

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use delphi_contracts::{
    CitationEntry, ConversationDetail, ConversationSummary, MessageDto, MessageRole,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
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

    async fn rename_conversation_if_default(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        title: String,
    ) -> Result<Option<ConversationDetail>, StorageError>;

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
pub struct PgRepository {
    pool: PgPool,
}

impl PgRepository {
    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self, StorageError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections.max(1))
            .connect(database_url)
            .await
            .map_err(|error| StorageError::Internal(format!("connect to Postgres: {error}")))?;
        Ok(Self { pool })
    }

    async fn ensure_principal(&self, tenant_id: &str, user_id: &str) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await.map_err(storage_internal)?;
        sqlx::query(
            "
            INSERT INTO tenant (tenant_id, name)
            VALUES ($1, $1)
            ON CONFLICT (tenant_id) DO NOTHING
            ",
        )
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(storage_internal)?;
        sqlx::query(
            "
            INSERT INTO app_user (tenant_id, user_id, last_seen_at)
            VALUES ($1, $2, now())
            ON CONFLICT (tenant_id, user_id)
            DO UPDATE SET last_seen_at = excluded.last_seen_at
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .execute(&mut *tx)
        .await
        .map_err(storage_internal)?;
        tx.commit().await.map_err(storage_internal)
    }

    async fn visible_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<PgConversation, StorageError> {
        self.ensure_principal(tenant_id, user_id).await?;
        sqlx::query_as::<_, PgConversation>(
            "
            SELECT tenant_id, user_id, conversation_id, title, updated_at
            FROM chat_conversation
            WHERE tenant_id = $1
              AND user_id = $2
              AND conversation_id = $3
              AND deleted_at IS NULL
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(conversation_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_internal)?
        .ok_or(StorageError::NotFound)
    }

    async fn list_pg_messages(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<Vec<MessageDto>, StorageError> {
        let rows = sqlx::query_as::<_, PgMessage>(
            "
            SELECT tenant_id, user_id, conversation_id, message_id, role, content,
                   parent_message_id, citations, turn_id, interrupted, finish_reason,
                   ordinal, created_at
            FROM chat_message
            WHERE tenant_id = $1
              AND user_id = $2
              AND conversation_id = $3
            ORDER BY ordinal ASC
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(conversation_id)
        .fetch_all(&self.pool)
        .await
        .map_err(storage_internal)?;
        rows.into_iter().map(pg_message_to_dto).collect()
    }

    async fn pg_conversation_detail(
        &self,
        conversation: PgConversation,
    ) -> Result<ConversationDetail, StorageError> {
        let messages = self
            .list_pg_messages(
                &conversation.tenant_id,
                &conversation.user_id,
                &conversation.conversation_id,
            )
            .await?;
        Ok(ConversationDetail {
            id: conversation.conversation_id,
            title: conversation.title,
            messages,
            updated_at: conversation.updated_at,
        })
    }

    async fn commit_pg_turn(
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
        finish_reason: Option<&str>,
    ) -> Result<ConversationDetail, StorageError> {
        let mut tx = self.pool.begin().await.map_err(storage_internal)?;
        let conversation = sqlx::query_as::<_, PgConversation>(
            "
            SELECT tenant_id, user_id, conversation_id, title, updated_at
            FROM chat_conversation
            WHERE tenant_id = $1
              AND user_id = $2
              AND conversation_id = $3
              AND deleted_at IS NULL
            FOR UPDATE
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(conversation_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_internal)?
        .ok_or(StorageError::NotFound)?;

        let parent_ordinal = match parent_message_id {
            Some(parent_message_id) => sqlx::query_as::<_, PgOrdinal>(
                "
                SELECT ordinal
                FROM chat_message
                WHERE tenant_id = $1
                  AND user_id = $2
                  AND conversation_id = $3
                  AND message_id = $4
                ",
            )
            .bind(tenant_id)
            .bind(user_id)
            .bind(conversation_id)
            .bind(parent_message_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_internal)?
            .map(|row| row.ordinal)
            .ok_or(StorageError::StaleParent)?,
            None => 0,
        };
        let now = Utc::now();
        let user_ordinal = parent_ordinal + 1;
        let assistant_ordinal = parent_ordinal + 2;
        let citations = serde_json::to_value(citations)
            .map_err(|error| StorageError::Internal(format!("serialize citations: {error}")))?;
        sqlx::query(
            "
            DELETE FROM chat_message
            WHERE tenant_id = $1
              AND user_id = $2
              AND conversation_id = $3
              AND ordinal > $4
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(conversation_id)
        .bind(parent_ordinal)
        .execute(&mut *tx)
        .await
        .map_err(storage_internal)?;
        sqlx::query(
            "
            INSERT INTO chat_message (
              tenant_id, user_id, conversation_id, message_id, role, content,
              parent_message_id, citations, turn_id, interrupted, finish_reason, ordinal, created_at
            )
            VALUES ($1, $2, $3, $4, 'user', $5, $6, '[]', $7, false, NULL, $8, $9)
            ON CONFLICT (tenant_id, message_id) DO UPDATE SET
              content = excluded.content,
              parent_message_id = excluded.parent_message_id,
              turn_id = excluded.turn_id,
              ordinal = excluded.ordinal,
              created_at = excluded.created_at
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(conversation_id)
        .bind(user_message_id)
        .bind(user_text)
        .bind(parent_message_id)
        .bind(turn_id)
        .bind(user_ordinal)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_internal)?;
        sqlx::query(
            "
            INSERT INTO chat_message (
              tenant_id, user_id, conversation_id, message_id, role, content,
              parent_message_id, citations, turn_id, interrupted, finish_reason, ordinal, created_at
            )
            VALUES ($1, $2, $3, $4, 'assistant', $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (tenant_id, message_id) DO UPDATE SET
              content = excluded.content,
              parent_message_id = excluded.parent_message_id,
              citations = excluded.citations,
              turn_id = excluded.turn_id,
              interrupted = excluded.interrupted,
              finish_reason = excluded.finish_reason,
              ordinal = excluded.ordinal,
              created_at = excluded.created_at
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(conversation_id)
        .bind(assistant_message_id)
        .bind(assistant_text)
        .bind(user_message_id)
        .bind(citations)
        .bind(turn_id)
        .bind(interrupted)
        .bind(finish_reason)
        .bind(assistant_ordinal)
        .bind(now + Duration::milliseconds(1))
        .execute(&mut *tx)
        .await
        .map_err(storage_internal)?;
        let status = if interrupted {
            "interrupted"
        } else {
            "committed"
        };
        sqlx::query(
            "
            INSERT INTO chat_turn (
              tenant_id, turn_id, user_id, conversation_id, user_message_id,
              assistant_message_id, parent_message_id, status, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (tenant_id, turn_id) DO UPDATE SET
              user_id = excluded.user_id,
              conversation_id = excluded.conversation_id,
              user_message_id = excluded.user_message_id,
              assistant_message_id = excluded.assistant_message_id,
              parent_message_id = excluded.parent_message_id,
              status = excluded.status,
              error = NULL,
              updated_at = excluded.updated_at
            ",
        )
        .bind(tenant_id)
        .bind(turn_id)
        .bind(user_id)
        .bind(conversation_id)
        .bind(user_message_id)
        .bind(assistant_message_id)
        .bind(parent_message_id)
        .bind(status)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_internal)?;
        sqlx::query(
            "
            UPDATE chat_conversation
            SET updated_at = $1, next_message_ordinal = $2
            WHERE tenant_id = $3
              AND user_id = $4
              AND conversation_id = $5
              AND deleted_at IS NULL
            ",
        )
        .bind(now)
        .bind(assistant_ordinal + 1)
        .bind(tenant_id)
        .bind(user_id)
        .bind(conversation_id)
        .execute(&mut *tx)
        .await
        .map_err(storage_internal)?;
        tx.commit().await.map_err(storage_internal)?;

        self.get_conversation(
            &conversation.tenant_id,
            &conversation.user_id,
            &conversation.conversation_id,
        )
        .await
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct PgConversation {
    tenant_id: String,
    user_id: String,
    conversation_id: String,
    title: String,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct PgMessage {
    message_id: String,
    role: String,
    content: String,
    parent_message_id: Option<String>,
    citations: serde_json::Value,
    turn_id: Option<String>,
    interrupted: bool,
    finish_reason: Option<String>,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct PgMessageTail {
    message_id: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct PgOrdinal {
    ordinal: i64,
}

#[async_trait]
impl ChatRepository for PgRepository {
    async fn list_conversations(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Vec<ConversationSummary>, StorageError> {
        self.ensure_principal(tenant_id, user_id).await?;
        let rows = sqlx::query_as::<_, PgConversation>(
            "
            SELECT tenant_id, user_id, conversation_id, title, updated_at
            FROM chat_conversation
            WHERE tenant_id = $1
              AND user_id = $2
              AND deleted_at IS NULL
            ORDER BY updated_at DESC
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
        .map_err(storage_internal)?;
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
        let id = ulid::Ulid::new().to_string();
        let row = sqlx::query_as::<_, PgConversation>(
            "
            INSERT INTO chat_conversation (tenant_id, user_id, conversation_id, title)
            VALUES ($1, $2, $3, $4)
            RETURNING tenant_id, user_id, conversation_id, title, updated_at
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(&id)
        .bind(title.unwrap_or_else(|| "New chat".to_owned()))
        .fetch_one(&self.pool)
        .await
        .map_err(storage_internal)?;
        Ok(ConversationDetail {
            id: row.conversation_id,
            title: row.title,
            messages: Vec::new(),
            updated_at: row.updated_at,
        })
    }

    async fn get_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<ConversationDetail, StorageError> {
        let conversation = self
            .visible_conversation(tenant_id, user_id, conversation_id)
            .await?;
        self.pg_conversation_detail(conversation).await
    }

    async fn rename_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        title: String,
    ) -> Result<ConversationDetail, StorageError> {
        let row = sqlx::query_as::<_, PgConversation>(
            "
            UPDATE chat_conversation
            SET title = $4, updated_at = now()
            WHERE tenant_id = $1
              AND user_id = $2
              AND conversation_id = $3
              AND deleted_at IS NULL
            RETURNING tenant_id, user_id, conversation_id, title, updated_at
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(conversation_id)
        .bind(title)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_internal)?
        .ok_or(StorageError::NotFound)?;
        self.pg_conversation_detail(row).await
    }

    async fn rename_conversation_if_default(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        title: String,
    ) -> Result<Option<ConversationDetail>, StorageError> {
        let row = sqlx::query_as::<_, PgConversation>(
            "
            UPDATE chat_conversation
            SET title = $4, updated_at = now()
            WHERE tenant_id = $1
              AND user_id = $2
              AND conversation_id = $3
              AND deleted_at IS NULL
              AND title = 'New chat'
            RETURNING tenant_id, user_id, conversation_id, title, updated_at
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(conversation_id)
        .bind(title)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_internal)?;
        let Some(row) = row else {
            self.visible_conversation(tenant_id, user_id, conversation_id)
                .await?;
            return Ok(None);
        };
        Ok(Some(self.pg_conversation_detail(row).await?))
    }

    async fn delete_conversation(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "
            UPDATE chat_conversation
            SET deleted_at = now(), updated_at = now()
            WHERE tenant_id = $1
              AND user_id = $2
              AND conversation_id = $3
              AND deleted_at IS NULL
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(conversation_id)
        .execute(&self.pool)
        .await
        .map_err(storage_internal)?;
        if result.rows_affected() == 0 {
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
        self.visible_conversation(tenant_id, user_id, conversation_id)
            .await?;
        let tail = sqlx::query_as::<_, PgMessageTail>(
            "
            SELECT message_id
            FROM chat_message
            WHERE tenant_id = $1
              AND user_id = $2
              AND conversation_id = $3
            ORDER BY ordinal DESC
            LIMIT 1
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(conversation_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_internal)?;
        if tail.as_ref().map(|row| row.message_id.as_str()) == parent_message_id {
            Ok(())
        } else {
            Err(StorageError::StaleParent)
        }
    }

    async fn record_turn_failed(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        error: &str,
    ) -> Result<(), StorageError> {
        self.visible_conversation(tenant_id, user_id, conversation_id)
            .await?;
        sqlx::query(
            "
            INSERT INTO chat_turn (tenant_id, turn_id, user_id, conversation_id, status, error, updated_at)
            VALUES ($1, $2, $3, $4, 'failed', $5, now())
            ON CONFLICT (tenant_id, turn_id) DO UPDATE SET
              status = 'failed',
              error = excluded.error,
              updated_at = excluded.updated_at
            ",
        )
        .bind(tenant_id)
        .bind(turn_id)
        .bind(user_id)
        .bind(conversation_id)
        .bind(error)
        .execute(&self.pool)
        .await
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
        self.commit_pg_turn(
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
        self.commit_pg_turn(
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
            Some("user_interrupted"),
        )
        .await
    }
}


fn pg_message_to_dto(row: PgMessage) -> Result<MessageDto, StorageError> {
    let citations = serde_json::from_value::<Vec<CitationEntry>>(row.citations)
        .map_err(|error| StorageError::Internal(format!("decode citations: {error}")))?;
    Ok(MessageDto {
        id: row.message_id,
        role: match row.role.as_str() {
            "assistant" => MessageRole::Assistant,
            "system" => MessageRole::System,
            _ => MessageRole::User,
        },
        content: row.content,
        parent_message_id: row.parent_message_id,
        citations,
        turn_id: row.turn_id,
        interrupted: row.interrupted,
        finish_reason: row.finish_reason,
        created_at: row.created_at,
    })
}

fn storage_internal(error: impl std::fmt::Display) -> StorageError {
    StorageError::Internal(error.to_string())
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

    async fn rename_conversation_if_default(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        title: String,
    ) -> Result<Option<ConversationDetail>, StorageError> {
        let mut state = self.inner.lock().await;
        let row = state
            .conversations
            .get_mut(conversation_id)
            .ok_or(StorageError::NotFound)?;
        ensure_visible(row, tenant_id, user_id)?;
        if row.title != "New chat" {
            return Ok(None);
        }
        row.title = title;
        row.updated_at = Utc::now();
        Ok(Some(to_detail(row)))
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
