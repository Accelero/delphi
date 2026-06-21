use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use delphi_contracts::{
    CitationEntry, ConversationDetail, ConversationSummary, CreateIngestionDocument, DocumentState,
    IngestionJobDto, IngestionJobStatus, IngestionStage, MessageDto, MessageRole,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, PgPool};
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

#[async_trait]
pub trait IngestionRepository: Clone + Send + Sync + 'static {
    async fn create_upload_session(
        &self,
        tenant_id: &str,
        user_id: &str,
        input: CreateUploadSession,
    ) -> Result<UploadSessionDto, StorageError>;

    async fn get_upload_session(
        &self,
        tenant_id: &str,
        user_id: &str,
        upload_id: &str,
    ) -> Result<UploadSessionDto, StorageError>;

    async fn mark_upload_accepted(
        &self,
        tenant_id: &str,
        user_id: &str,
        upload_id: &str,
        document_id: &str,
        job_id: &str,
    ) -> Result<(), StorageError>;
    async fn mark_upload_failed(
        &self,
        tenant_id: &str,
        user_id: &str,
        upload_id: &str,
        error: &str,
    ) -> Result<(), StorageError>;

    async fn create_ingestion_job(
        &self,
        tenant_id: &str,
        user_id: &str,
        input: CreateIngestionDocument,
        pipeline_version: u32,
    ) -> Result<IngestionJobDto, StorageError>;

    async fn get_ingestion_job(
        &self,
        tenant_id: &str,
        user_id: &str,
        job_id: &str,
    ) -> Result<IngestionJobDto, StorageError>;
}

#[derive(Debug, Clone)]
pub struct CreateUploadSession {
    pub upload_id: String,
    pub storage_key: String,
    pub multipart_upload_id: String,
    pub filename: String,
    pub content_type: Option<String>,
    pub declared_size: u64,
    pub title: Option<String>,
    pub source_uri: Option<String>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UploadSessionDto {
    pub id: String,
    pub storage_key: String,
    pub multipart_upload_id: String,
    pub filename: String,
    pub content_type: Option<String>,
    pub declared_size: u64,
    pub title: Option<String>,
    pub source_uri: Option<String>,
    pub metadata: serde_json::Value,
    pub state: String,
    pub document_id: Option<String>,
    pub job_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
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
        pool.execute(include_str!("../../../migrations/0001_pg_cutover.sql"))
            .await
            .map_err(|error| StorageError::Internal(format!("run Postgres migrations: {error}")))?;
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

    async fn find_pg_ingestion_job(
        &self,
        tenant_id: &str,
        user_id: &str,
        document_id: &str,
        pipeline_version: u32,
    ) -> Result<Option<IngestionJobDto>, StorageError> {
        let Some(job) = sqlx::query_as::<_, PgIngestionJob>(
            "
            SELECT tenant_id, user_id, job_id, document_id, status, current_stage,
                   pipeline_version, attempt, error, created_at, updated_at
            FROM ingestion_job
            WHERE tenant_id = $1
              AND user_id = $2
              AND document_id = $3
              AND pipeline_version = $4
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(document_id)
        .bind(i64::from(pipeline_version))
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_internal)?
        else {
            return Ok(None);
        };
        let state = self
            .get_pg_document_state(tenant_id, user_id, &job.document_id)
            .await?;
        Ok(Some(pg_ingestion_job_to_dto(job, state)?))
    }

    async fn get_pg_document_state(
        &self,
        tenant_id: &str,
        user_id: &str,
        document_id: &str,
    ) -> Result<DocumentState, StorageError> {
        let row = sqlx::query_as::<_, PgDocumentState>(
            "
            SELECT state
            FROM document
            WHERE tenant_id = $1
              AND owner_user_id = $2
              AND document_id = $3
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(document_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_internal)?
        .ok_or(StorageError::NotFound)?;
        parse_document_state(&row.state)
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
        let status = if interrupted { "interrupted" } else { "committed" };
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

#[derive(Debug, Clone, sqlx::FromRow)]
struct PgUploadSession {
    upload_id: String,
    storage_key: String,
    multipart_upload_id: String,
    filename: String,
    content_type: Option<String>,
    declared_size: i64,
    title: Option<String>,
    source_uri: Option<String>,
    metadata: serde_json::Value,
    state: String,
    document_id: Option<String>,
    job_id: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct PgIngestionJob {
    job_id: String,
    document_id: String,
    status: String,
    current_stage: Option<String>,
    pipeline_version: i64,
    attempt: i64,
    error: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct PgDocumentState {
    state: String,
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

#[async_trait]
impl IngestionRepository for PgRepository {
    async fn create_upload_session(
        &self,
        tenant_id: &str,
        user_id: &str,
        input: CreateUploadSession,
    ) -> Result<UploadSessionDto, StorageError> {
        self.ensure_principal(tenant_id, user_id).await?;
        let declared_size = i64::try_from(input.declared_size)
            .map_err(|_| StorageError::Internal("declared_size exceeds i64".to_owned()))?;
        let row = sqlx::query_as::<_, PgUploadSession>(
            "
            INSERT INTO upload_session (
              tenant_id, user_id, upload_id, storage_key, multipart_upload_id,
              filename, content_type, declared_size, title, source_uri, metadata, state
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'uploading')
            RETURNING upload_id, storage_key, multipart_upload_id, filename, content_type,
                      declared_size, title, source_uri, metadata, state, document_id, job_id,
                      created_at, updated_at
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(input.upload_id)
        .bind(input.storage_key)
        .bind(input.multipart_upload_id)
        .bind(input.filename)
        .bind(input.content_type)
        .bind(declared_size)
        .bind(input.title)
        .bind(input.source_uri)
        .bind(object_metadata(input.metadata))
        .fetch_one(&self.pool)
        .await
        .map_err(storage_internal)?;
        Ok(pg_upload_session_to_dto(row))
    }

    async fn get_upload_session(
        &self,
        tenant_id: &str,
        user_id: &str,
        upload_id: &str,
    ) -> Result<UploadSessionDto, StorageError> {
        let row = sqlx::query_as::<_, PgUploadSession>(
            "
            SELECT upload_id, storage_key, multipart_upload_id, filename, content_type,
                   declared_size, title, source_uri, metadata, state, document_id, job_id,
                   created_at, updated_at
            FROM upload_session
            WHERE tenant_id = $1
              AND user_id = $2
              AND upload_id = $3
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(upload_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_internal)?
        .ok_or(StorageError::NotFound)?;
        Ok(pg_upload_session_to_dto(row))
    }

    async fn mark_upload_accepted(
        &self,
        tenant_id: &str,
        user_id: &str,
        upload_id: &str,
        document_id: &str,
        job_id: &str,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "
            UPDATE upload_session
            SET state = 'accepted', document_id = $4, job_id = $5, updated_at = now()
            WHERE tenant_id = $1
              AND user_id = $2
              AND upload_id = $3
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(upload_id)
        .bind(document_id)
        .bind(job_id)
        .execute(&self.pool)
        .await
        .map_err(storage_internal)?;
        if result.rows_affected() == 0 {
            Err(StorageError::NotFound)
        } else {
            Ok(())
        }
    }

    async fn mark_upload_failed(
        &self,
        tenant_id: &str,
        user_id: &str,
        upload_id: &str,
        error: &str,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "
            UPDATE upload_session
            SET state = 'failed', error = $4, updated_at = now()
            WHERE tenant_id = $1
              AND user_id = $2
              AND upload_id = $3
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(upload_id)
        .bind(error)
        .execute(&self.pool)
        .await
        .map_err(storage_internal)?;
        if result.rows_affected() == 0 {
            Err(StorageError::NotFound)
        } else {
            Ok(())
        }
    }

    async fn create_ingestion_job(
        &self,
        tenant_id: &str,
        user_id: &str,
        input: CreateIngestionDocument,
        pipeline_version: u32,
    ) -> Result<IngestionJobDto, StorageError> {
        self.ensure_principal(tenant_id, user_id).await?;
        if input.storage_key.trim().is_empty() {
            return Err(StorageError::Internal(
                "storage_key cannot be empty".to_owned(),
            ));
        }
        if input.source_type.trim().is_empty() {
            return Err(StorageError::Internal(
                "source_type cannot be empty".to_owned(),
            ));
        }
        let declared_size = i64::try_from(input.declared_size)
            .map_err(|_| StorageError::Internal("declared_size exceeds i64".to_owned()))?;
        let document_id = input
            .document_id
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| ulid::Ulid::new().to_string());
        let job_id = input
            .job_id
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| ulid::Ulid::new().to_string());
        if let Some(existing) = self
            .find_pg_ingestion_job(tenant_id, user_id, &document_id, pipeline_version)
            .await?
        {
            return Ok(existing);
        }
        let storage_key = input.storage_key;
        let mut tx = self.pool.begin().await.map_err(storage_internal)?;
        sqlx::query(
            "
            INSERT INTO document (
              tenant_id, document_id, owner_user_id, document_version, state, title, metadata,
              object_key, object_size_bytes, content_type, filename, source_type, source_uri,
              storage_key, declared_size
            )
            VALUES ($1, $2, $3, 1, 'staging', $4, $5, $6, $7, $8, $9, $10, $11, $6, $7)
            ON CONFLICT (tenant_id, document_id) DO NOTHING
            ",
        )
        .bind(tenant_id)
        .bind(&document_id)
        .bind(user_id)
        .bind(input.title.filter(|title| !title.trim().is_empty()))
        .bind(object_metadata(input.metadata))
        .bind(&storage_key)
        .bind(declared_size)
        .bind(input.content_type)
        .bind(input.filename)
        .bind(input.source_type)
        .bind(input.source_uri)
        .execute(&mut *tx)
        .await
        .map_err(storage_internal)?;
        let job = sqlx::query_as::<_, PgIngestionJob>(
            "
            INSERT INTO ingestion_job (
              tenant_id, user_id, job_id, document_id, status, current_stage,
              pipeline_version, attempt
            )
            VALUES ($1, $2, $3, $4, 'validating', 'validate', $5, 1)
            ON CONFLICT (tenant_id, document_id, pipeline_version) DO UPDATE SET
              updated_at = ingestion_job.updated_at
            RETURNING tenant_id, user_id, job_id, document_id, status, current_stage,
                      pipeline_version, attempt, error, created_at, updated_at
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(&job_id)
        .bind(&document_id)
        .bind(i64::from(pipeline_version))
        .fetch_one(&mut *tx)
        .await
        .map_err(storage_internal)?;
        let outbox_payload = serde_json::json!({
            "tenant_id": tenant_id,
            "document_id": document_id,
            "document_version": 1,
            "owner_user_id": user_id,
            "state": "staging",
            "storage_key": storage_key,
            "job_id": job.job_id,
            "pipeline_version": pipeline_version,
        });
        sqlx::query(
            "
            INSERT INTO outbox_event (
              event_id, subject, event_type, tenant_id, aggregate_id, aggregate_version, payload
            )
            VALUES ($1, 'documents.snapshots.v1.upserted', 'document_snapshot_created', $2, $3, 1, $4)
            ON CONFLICT (event_id) DO NOTHING
            ",
        )
        .bind(format!("{tenant_id}:{document_id}:v1:document_snapshot_created"))
        .bind(tenant_id)
        .bind(&document_id)
        .bind(outbox_payload)
        .execute(&mut *tx)
        .await
        .map_err(storage_internal)?;
        tx.commit().await.map_err(storage_internal)?;
        pg_ingestion_job_to_dto(job, DocumentState::Staging)
    }

    async fn get_ingestion_job(
        &self,
        tenant_id: &str,
        user_id: &str,
        job_id: &str,
    ) -> Result<IngestionJobDto, StorageError> {
        let job = sqlx::query_as::<_, PgIngestionJob>(
            "
            SELECT tenant_id, user_id, job_id, document_id, status, current_stage,
                   pipeline_version, attempt, error, created_at, updated_at
            FROM ingestion_job
            WHERE tenant_id = $1
              AND user_id = $2
              AND job_id = $3
            ",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_internal)?
        .ok_or(StorageError::NotFound)?;
        let state = self
            .get_pg_document_state(tenant_id, user_id, &job.document_id)
            .await?;
        pg_ingestion_job_to_dto(job, state)
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

fn pg_upload_session_to_dto(row: PgUploadSession) -> UploadSessionDto {
    UploadSessionDto {
        id: row.upload_id,
        storage_key: row.storage_key,
        multipart_upload_id: row.multipart_upload_id,
        filename: row.filename,
        content_type: row.content_type,
        declared_size: row.declared_size.max(0) as u64,
        title: row.title,
        source_uri: row.source_uri,
        metadata: row.metadata,
        state: row.state,
        document_id: row.document_id,
        job_id: row.job_id,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn pg_ingestion_job_to_dto(
    row: PgIngestionJob,
    state: DocumentState,
) -> Result<IngestionJobDto, StorageError> {
    let status = parse_ingestion_status(&row.status)?;
    let current_stage = row
        .current_stage
        .as_deref()
        .map(parse_ingestion_stage)
        .transpose()?;
    Ok(IngestionJobDto {
        id: row.job_id,
        document_id: row.document_id,
        state,
        status,
        current_stage,
        pipeline_version: row.pipeline_version.max(0) as u32,
        attempt: row.attempt.max(0) as u32,
        error: row.error,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
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
                DEFINE FIELD OVERWRITE status ON chat_turn TYPE string ASSERT $value INSIDE ['committed', 'interrupted', 'failed'];
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

                DEFINE TABLE OVERWRITE upload_session SCHEMAFULL;
                DEFINE FIELD OVERWRITE upload_id ON upload_session TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE tenant_id ON upload_session TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE user_id ON upload_session TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE storage_key ON upload_session TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE multipart_upload_id ON upload_session TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE filename ON upload_session TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE content_type ON upload_session TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE declared_size ON upload_session TYPE int ASSERT $value >= 0;
                DEFINE FIELD OVERWRITE title ON upload_session TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE source_uri ON upload_session TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE metadata ON upload_session TYPE object FLEXIBLE DEFAULT {};
                DEFINE FIELD OVERWRITE state ON upload_session TYPE string ASSERT $value INSIDE ['uploading', 'accepted', 'failed'];
                DEFINE FIELD OVERWRITE document_id ON upload_session TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE job_id ON upload_session TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE error ON upload_session TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE created_at ON upload_session TYPE datetime DEFAULT time::now();
                DEFINE FIELD OVERWRITE updated_at ON upload_session TYPE datetime DEFAULT time::now();
                REMOVE INDEX IF EXISTS upload_session_id ON upload_session;
                REMOVE INDEX IF EXISTS upload_session_owner ON upload_session;
                DEFINE INDEX IF NOT EXISTS upload_session_id
                    ON upload_session FIELDS tenant_id, upload_id UNIQUE;
                DEFINE INDEX IF NOT EXISTS upload_session_owner
                    ON upload_session FIELDS tenant_id, user_id, updated_at;

                DEFINE TABLE OVERWRITE document SCHEMAFULL;
                DEFINE FIELD OVERWRITE document_id ON document TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE tenant_id ON document TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE user_id ON document TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE title ON document TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE source_type ON document TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE source_uri ON document TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE storage_key ON document TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE filename ON document TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE content_type ON document TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE declared_size ON document TYPE int ASSERT $value >= 0;
                DEFINE FIELD OVERWRITE state ON document TYPE string ASSERT $value INSIDE ['staging', 'validating', 'indexing', 'ready', 'failed'];
                DEFINE FIELD OVERWRITE created_at ON document TYPE datetime DEFAULT time::now();
                DEFINE FIELD OVERWRITE updated_at ON document TYPE datetime DEFAULT time::now();
                DEFINE FIELD OVERWRITE ready_at ON document TYPE option<datetime> DEFAULT NONE;
                DEFINE FIELD OVERWRITE failed_at ON document TYPE option<datetime> DEFAULT NONE;
                DEFINE FIELD OVERWRITE failed_reason ON document TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE metadata ON document TYPE object FLEXIBLE DEFAULT {};
                REMOVE INDEX IF EXISTS document_id ON document;
                REMOVE INDEX IF EXISTS document_owner ON document;
                REMOVE INDEX IF EXISTS document_ready ON document;
                DEFINE INDEX IF NOT EXISTS document_id ON document FIELDS tenant_id, document_id UNIQUE;
                DEFINE INDEX IF NOT EXISTS document_owner
                    ON document FIELDS tenant_id, user_id, updated_at;
                DEFINE INDEX IF NOT EXISTS document_ready
                    ON document FIELDS tenant_id, state, updated_at;

                DEFINE TABLE OVERWRITE document_content SCHEMAFULL;
                DEFINE FIELD OVERWRITE document_id ON document_content TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE tenant_id ON document_content TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE extractor ON document_content TYPE string DEFAULT 'none';
                DEFINE FIELD OVERWRITE text ON document_content TYPE string DEFAULT '';
                DEFINE FIELD OVERWRITE metadata ON document_content TYPE object FLEXIBLE DEFAULT {};
                DEFINE FIELD OVERWRITE created_at ON document_content TYPE datetime DEFAULT time::now();
                DEFINE FIELD OVERWRITE updated_at ON document_content TYPE datetime DEFAULT time::now();
                REMOVE INDEX IF EXISTS document_content_id ON document_content;
                DEFINE INDEX IF NOT EXISTS document_content_id
                    ON document_content FIELDS tenant_id, document_id UNIQUE;

                DEFINE TABLE OVERWRITE document_chunk SCHEMAFULL;
                DEFINE FIELD OVERWRITE chunk_id ON document_chunk TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE document_id ON document_chunk TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE tenant_id ON document_chunk TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE ordinal ON document_chunk TYPE int ASSERT $value >= 0;
                DEFINE FIELD OVERWRITE pipeline_version ON document_chunk TYPE int ASSERT $value > 0;
                DEFINE FIELD OVERWRITE strategy ON document_chunk TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE text ON document_chunk TYPE string DEFAULT '';
                DEFINE FIELD OVERWRITE bboxes ON document_chunk TYPE array DEFAULT [];
                DEFINE FIELD OVERWRITE created_at ON document_chunk TYPE datetime DEFAULT time::now();
                REMOVE INDEX IF EXISTS document_chunk_id ON document_chunk;
                REMOVE INDEX IF EXISTS document_chunk_unique ON document_chunk;
                DEFINE INDEX IF NOT EXISTS document_chunk_id
                    ON document_chunk FIELDS tenant_id, chunk_id UNIQUE;
                DEFINE INDEX IF NOT EXISTS document_chunk_unique
                    ON document_chunk FIELDS tenant_id, document_id, pipeline_version, strategy, ordinal UNIQUE;

                DEFINE TABLE OVERWRITE document_embedding SCHEMAFULL;
                DEFINE FIELD OVERWRITE embedding_id ON document_embedding TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE tenant_id ON document_embedding TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE document_id ON document_embedding TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE chunk_id ON document_embedding TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE model ON document_embedding TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE pipeline_version ON document_embedding TYPE int ASSERT $value > 0;
                DEFINE FIELD OVERWRITE vector ON document_embedding TYPE array DEFAULT [];
                DEFINE FIELD OVERWRITE created_at ON document_embedding TYPE datetime DEFAULT time::now();
                REMOVE INDEX IF EXISTS document_embedding_id ON document_embedding;
                REMOVE INDEX IF EXISTS document_embedding_unique ON document_embedding;
                DEFINE INDEX IF NOT EXISTS document_embedding_id
                    ON document_embedding FIELDS tenant_id, embedding_id UNIQUE;
                DEFINE INDEX IF NOT EXISTS document_embedding_unique
                    ON document_embedding FIELDS tenant_id, document_id, chunk_id, model, pipeline_version UNIQUE;

                DEFINE TABLE OVERWRITE ingestion_job SCHEMAFULL;
                DEFINE FIELD OVERWRITE job_id ON ingestion_job TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE tenant_id ON ingestion_job TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE user_id ON ingestion_job TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE document_id ON ingestion_job TYPE string ASSERT $value != '';
                DEFINE FIELD OVERWRITE status ON ingestion_job TYPE string ASSERT $value INSIDE ['validating', 'extracting', 'chunking', 'embedding', 'publishing', 'ready', 'failed'];
                DEFINE FIELD OVERWRITE current_stage ON ingestion_job TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE pipeline_version ON ingestion_job TYPE int ASSERT $value > 0;
                DEFINE FIELD OVERWRITE attempt ON ingestion_job TYPE int ASSERT $value >= 1;
                DEFINE FIELD OVERWRITE error ON ingestion_job TYPE option<string> DEFAULT NONE;
                DEFINE FIELD OVERWRITE created_at ON ingestion_job TYPE datetime DEFAULT time::now();
                DEFINE FIELD OVERWRITE updated_at ON ingestion_job TYPE datetime DEFAULT time::now();
                REMOVE INDEX IF EXISTS ingestion_job_id ON ingestion_job;
                REMOVE INDEX IF EXISTS ingestion_job_document ON ingestion_job;
                REMOVE INDEX IF EXISTS ingestion_job_status ON ingestion_job;
                DEFINE INDEX IF NOT EXISTS ingestion_job_id
                    ON ingestion_job FIELDS tenant_id, job_id UNIQUE;
                DEFINE INDEX IF NOT EXISTS ingestion_job_document
                    ON ingestion_job FIELDS tenant_id, document_id, pipeline_version UNIQUE;
                DEFINE INDEX IF NOT EXISTS ingestion_job_status
                    ON ingestion_job FIELDS tenant_id, status, updated_at;
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
struct DbDocument {
    #[surreal(default)]
    document_id: String,
    #[surreal(default)]
    tenant_id: String,
    #[surreal(default)]
    user_id: String,
    #[surreal(default)]
    title: Option<String>,
    #[surreal(default)]
    source_type: String,
    #[surreal(default)]
    source_uri: Option<String>,
    #[surreal(default)]
    storage_key: String,
    #[surreal(default)]
    filename: Option<String>,
    #[surreal(default)]
    content_type: Option<String>,
    #[surreal(default)]
    declared_size: i64,
    #[surreal(default)]
    state: String,
    #[surreal(default)]
    created_at: DateTime<Utc>,
    #[surreal(default)]
    updated_at: DateTime<Utc>,
    #[surreal(default)]
    ready_at: Option<DateTime<Utc>>,
    #[surreal(default)]
    failed_at: Option<DateTime<Utc>>,
    #[surreal(default)]
    failed_reason: Option<String>,
    #[surreal(default)]
    metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct DbUploadSession {
    #[surreal(default)]
    upload_id: String,
    #[surreal(default)]
    tenant_id: String,
    #[surreal(default)]
    user_id: String,
    #[surreal(default)]
    storage_key: String,
    #[surreal(default)]
    multipart_upload_id: String,
    #[surreal(default)]
    filename: String,
    #[surreal(default)]
    content_type: Option<String>,
    #[surreal(default)]
    declared_size: i64,
    #[surreal(default)]
    title: Option<String>,
    #[surreal(default)]
    source_uri: Option<String>,
    #[surreal(default)]
    metadata: serde_json::Value,
    #[surreal(default)]
    state: String,
    #[surreal(default)]
    document_id: Option<String>,
    #[surreal(default)]
    job_id: Option<String>,
    #[surreal(default)]
    error: Option<String>,
    #[surreal(default)]
    created_at: DateTime<Utc>,
    #[surreal(default)]
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct DbIngestionJob {
    #[surreal(default)]
    job_id: String,
    #[surreal(default)]
    tenant_id: String,
    #[surreal(default)]
    user_id: String,
    #[surreal(default)]
    document_id: String,
    #[surreal(default)]
    status: String,
    #[surreal(default)]
    current_stage: Option<String>,
    #[surreal(default)]
    pipeline_version: i64,
    #[surreal(default)]
    attempt: i64,
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

    async fn rename_conversation_if_default(
        &self,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        title: String,
    ) -> Result<Option<ConversationDetail>, StorageError> {
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
                  AND title = 'New chat'
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
        let Some(conversation) = rows.into_iter().next() else {
            self.get_visible_conversation(tenant_id, user_id, conversation_id)
                .await?;
            return Ok(None);
        };
        let messages = self
            .list_messages(tenant_id, user_id, conversation_id)
            .await?
            .into_iter()
            .map(db_message_to_dto)
            .collect();
        Ok(Some(to_surreal_detail(conversation, messages)))
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
        self.upsert_turn(DbChatTurn {
            turn_id: turn_id.to_owned(),
            tenant_id: tenant_id.to_owned(),
            user_id: user_id.to_owned(),
            conversation_id: conversation_id.to_owned(),
            user_message_id: None,
            assistant_message_id: None,
            parent_message_id: None,
            status: "failed".to_owned(),
            worker_id: None,
            error: Some(error.to_owned()),
            created_at: now,
            updated_at: now,
        })
        .await
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

#[async_trait]
impl IngestionRepository for SurrealChatRepository {
    async fn create_upload_session(
        &self,
        tenant_id: &str,
        user_id: &str,
        input: CreateUploadSession,
    ) -> Result<UploadSessionDto, StorageError> {
        self.ensure_principal(tenant_id, user_id).await?;
        let declared_size = i64::try_from(input.declared_size)
            .map_err(|_| StorageError::Internal("declared_size exceeds i64".to_owned()))?;
        let now = Utc::now();
        let row = DbUploadSession {
            upload_id: input.upload_id,
            tenant_id: tenant_id.to_owned(),
            user_id: user_id.to_owned(),
            storage_key: input.storage_key,
            multipart_upload_id: input.multipart_upload_id,
            filename: input.filename,
            content_type: input.content_type,
            declared_size,
            title: input.title,
            source_uri: input.source_uri,
            metadata: object_metadata(input.metadata),
            state: "uploading".to_owned(),
            document_id: None,
            job_id: None,
            error: None,
            created_at: now,
            updated_at: now,
        };
        self.db
            .query("CREATE upload_session CONTENT $data")
            .bind(("data", row.clone()))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        Ok(upload_session_to_dto(row))
    }

    async fn get_upload_session(
        &self,
        tenant_id: &str,
        user_id: &str,
        upload_id: &str,
    ) -> Result<UploadSessionDto, StorageError> {
        let mut response = self
            .db
            .query(
                "
                SELECT upload_id, tenant_id, user_id, storage_key, multipart_upload_id,
                       filename, content_type, declared_size, title, source_uri, metadata, state,
                       document_id, job_id, error, created_at, updated_at
                FROM upload_session
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND upload_id = $upload_id
                LIMIT 1
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
            .bind(("upload_id", upload_id.to_owned()))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        let rows: Vec<DbUploadSession> = response.take(0).map_err(storage_internal)?;
        rows.into_iter()
            .next()
            .map(upload_session_to_dto)
            .ok_or(StorageError::NotFound)
    }

    async fn mark_upload_accepted(
        &self,
        tenant_id: &str,
        user_id: &str,
        upload_id: &str,
        document_id: &str,
        job_id: &str,
    ) -> Result<(), StorageError> {
        let mut response = self
            .db
            .query(
                "
                UPDATE upload_session
                SET state = 'accepted',
                    document_id = $document_id,
                    job_id = $job_id,
                    updated_at = $updated_at
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND upload_id = $upload_id
                RETURN upload_id
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
            .bind(("upload_id", upload_id.to_owned()))
            .bind(("document_id", document_id.to_owned()))
            .bind(("job_id", job_id.to_owned()))
            .bind(("updated_at", Utc::now()))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        let rows: Vec<UploadIdRow> = response.take(0).map_err(storage_internal)?;
        if rows.is_empty() {
            Err(StorageError::NotFound)
        } else {
            Ok(())
        }
    }

    async fn mark_upload_failed(
        &self,
        tenant_id: &str,
        user_id: &str,
        upload_id: &str,
        error: &str,
    ) -> Result<(), StorageError> {
        let mut response = self
            .db
            .query(
                "
                UPDATE upload_session
                SET state = 'failed',
                    error = $error,
                    updated_at = $updated_at
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND upload_id = $upload_id
                RETURN upload_id
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
            .bind(("upload_id", upload_id.to_owned()))
            .bind(("error", error.to_owned()))
            .bind(("updated_at", Utc::now()))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        let rows: Vec<UploadIdRow> = response.take(0).map_err(storage_internal)?;
        if rows.is_empty() {
            Err(StorageError::NotFound)
        } else {
            Ok(())
        }
    }

    async fn create_ingestion_job(
        &self,
        tenant_id: &str,
        user_id: &str,
        input: CreateIngestionDocument,
        pipeline_version: u32,
    ) -> Result<IngestionJobDto, StorageError> {
        self.ensure_principal(tenant_id, user_id).await?;
        if input.storage_key.trim().is_empty() {
            return Err(StorageError::Internal(
                "storage_key cannot be empty".to_owned(),
            ));
        }
        if input.source_type.trim().is_empty() {
            return Err(StorageError::Internal(
                "source_type cannot be empty".to_owned(),
            ));
        }
        let declared_size = i64::try_from(input.declared_size)
            .map_err(|_| StorageError::Internal("declared_size exceeds i64".to_owned()))?;
        let now = Utc::now();
        let document_id = input
            .document_id
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| ulid::Ulid::new().to_string());
        let job_id = input
            .job_id
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| ulid::Ulid::new().to_string());
        if let Some(existing) = self
            .find_ingestion_job(tenant_id, user_id, &document_id, pipeline_version)
            .await?
        {
            return Ok(existing);
        }
        let document = DbDocument {
            document_id: document_id.clone(),
            tenant_id: tenant_id.to_owned(),
            user_id: user_id.to_owned(),
            title: input.title.filter(|title| !title.trim().is_empty()),
            source_type: input.source_type,
            source_uri: input.source_uri,
            storage_key: input.storage_key,
            filename: input.filename,
            content_type: input.content_type,
            declared_size,
            state: document_state_token(DocumentState::Staging).to_owned(),
            created_at: now,
            updated_at: now,
            ready_at: None,
            failed_at: None,
            failed_reason: None,
            metadata: object_metadata(input.metadata),
        };
        let job = DbIngestionJob {
            job_id: job_id.clone(),
            tenant_id: tenant_id.to_owned(),
            user_id: user_id.to_owned(),
            document_id,
            status: ingestion_status_token(IngestionJobStatus::Validating).to_owned(),
            current_stage: Some(ingestion_stage_token(IngestionStage::Validate).to_owned()),
            pipeline_version: i64::from(pipeline_version),
            attempt: 1,
            error: None,
            created_at: now,
            updated_at: now,
        };

        self.db
            .query(
                "
                BEGIN TRANSACTION;
                CREATE document CONTENT $document;
                CREATE ingestion_job CONTENT $job;
                COMMIT TRANSACTION;
                ",
            )
            .bind(("document", document))
            .bind(("job", job.clone()))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;

        Ok(ingestion_job_to_dto(
            job,
            DocumentState::Staging,
            IngestionJobStatus::Validating,
            Some(IngestionStage::Validate),
        ))
    }

    async fn get_ingestion_job(
        &self,
        tenant_id: &str,
        user_id: &str,
        job_id: &str,
    ) -> Result<IngestionJobDto, StorageError> {
        let mut response = self
            .db
            .query(
                "
                SELECT job_id, tenant_id, user_id, document_id, status, current_stage,
                       pipeline_version, attempt, error, created_at, updated_at
                FROM ingestion_job
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND job_id = $job_id
                LIMIT 1
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
            .bind(("job_id", job_id.to_owned()))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        let rows: Vec<DbIngestionJob> = response.take(0).map_err(storage_internal)?;
        let job = rows.into_iter().next().ok_or(StorageError::NotFound)?;
        let state = self
            .get_document_state(tenant_id, user_id, &job.document_id)
            .await?;
        let status = parse_ingestion_status(&job.status)?;
        let current_stage = job
            .current_stage
            .as_deref()
            .map(parse_ingestion_stage)
            .transpose()?;
        Ok(ingestion_job_to_dto(job, state, status, current_stage))
    }
}

impl SurrealChatRepository {
    async fn find_ingestion_job(
        &self,
        tenant_id: &str,
        user_id: &str,
        document_id: &str,
        pipeline_version: u32,
    ) -> Result<Option<IngestionJobDto>, StorageError> {
        let mut response = self
            .db
            .query(
                "
                SELECT job_id, tenant_id, user_id, document_id, status, current_stage,
                       pipeline_version, attempt, error, created_at, updated_at
                FROM ingestion_job
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND document_id = $document_id
                  AND pipeline_version = $pipeline_version
                LIMIT 1
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
            .bind(("document_id", document_id.to_owned()))
            .bind(("pipeline_version", i64::from(pipeline_version)))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        let rows: Vec<DbIngestionJob> = response.take(0).map_err(storage_internal)?;
        let Some(job) = rows.into_iter().next() else {
            return Ok(None);
        };
        let state = self
            .get_document_state(tenant_id, user_id, &job.document_id)
            .await?;
        let status = parse_ingestion_status(&job.status)?;
        let current_stage = job
            .current_stage
            .as_deref()
            .map(parse_ingestion_stage)
            .transpose()?;
        Ok(Some(ingestion_job_to_dto(
            job,
            state,
            status,
            current_stage,
        )))
    }

    async fn get_document_state(
        &self,
        tenant_id: &str,
        user_id: &str,
        document_id: &str,
    ) -> Result<DocumentState, StorageError> {
        let mut response = self
            .db
            .query(
                "
                SELECT state
                FROM document
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND document_id = $document_id
                LIMIT 1
                ",
            )
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
            .bind(("document_id", document_id.to_owned()))
            .await
            .map_err(storage_internal)?
            .check()
            .map_err(storage_internal)?;
        let rows: Vec<DocumentStateRow> = response.take(0).map_err(storage_internal)?;
        let row = rows.into_iter().next().ok_or(StorageError::NotFound)?;
        parse_document_state(&row.state)
    }

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
        let turn_status = if interrupted {
            "interrupted"
        } else {
            "committed"
        };
        let terminal_turn = DbChatTurn {
            turn_id: turn_id.to_owned(),
            tenant_id: tenant_id.to_owned(),
            user_id: user_id.to_owned(),
            conversation_id: conversation_id.to_owned(),
            user_message_id: Some(user_message_id.to_owned()),
            assistant_message_id: Some(assistant_message_id.to_owned()),
            parent_message_id: parent_message_id.map(str::to_owned),
            status: turn_status.to_owned(),
            worker_id: None,
            error: None,
            created_at: now,
            updated_at: now,
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
                    next_message_ordinal = $next_message_ordinal
                WHERE tenant_id = $tenant_id
                  AND user_id = $user_id
                  AND conversation_id = $conversation_id
                  AND deleted_at = NONE;
                UPSERT chat_turn CONTENT $terminal_turn
                WHERE tenant_id = $tenant_id
                  AND turn_id = $turn_id;
                COMMIT;
                ",
            )
            .bind(("user_message", user))
            .bind(("assistant_message", assistant))
            .bind(("updated_at", now))
            .bind(("next_message_ordinal", assistant_ordinal + 1))
            .bind(("tenant_id", tenant_id.to_owned()))
            .bind(("user_id", user_id.to_owned()))
            .bind(("conversation_id", conversation_id.to_owned()))
            .bind(("parent_ordinal", parent_ordinal))
            .bind(("terminal_turn", terminal_turn))
            .bind(("turn_id", turn_id.to_owned()))
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

#[derive(Debug, Deserialize, SurrealValue)]
struct UploadIdRow {
    #[surreal(default)]
    upload_id: String,
}

#[derive(Debug, Deserialize, SurrealValue)]
struct DocumentStateRow {
    #[surreal(default)]
    state: String,
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

fn upload_session_to_dto(row: DbUploadSession) -> UploadSessionDto {
    UploadSessionDto {
        id: row.upload_id,
        storage_key: row.storage_key,
        multipart_upload_id: row.multipart_upload_id,
        filename: row.filename,
        content_type: row.content_type,
        declared_size: row.declared_size.max(0) as u64,
        title: row.title,
        source_uri: row.source_uri,
        metadata: row.metadata,
        state: row.state,
        document_id: row.document_id,
        job_id: row.job_id,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn object_metadata(value: serde_json::Value) -> serde_json::Value {
    if value.is_object() {
        value
    } else {
        serde_json::json!({})
    }
}

fn ingestion_job_to_dto(
    row: DbIngestionJob,
    state: DocumentState,
    status: IngestionJobStatus,
    current_stage: Option<IngestionStage>,
) -> IngestionJobDto {
    IngestionJobDto {
        id: row.job_id,
        document_id: row.document_id,
        state,
        status,
        current_stage,
        pipeline_version: row.pipeline_version.max(0) as u32,
        attempt: row.attempt.max(0) as u32,
        error: row.error,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn document_state_token(state: DocumentState) -> &'static str {
    match state {
        DocumentState::Staging => "staging",
        DocumentState::Validating => "validating",
        DocumentState::Indexing => "indexing",
        DocumentState::Ready => "ready",
        DocumentState::Failed => "failed",
    }
}

fn ingestion_stage_token(stage: IngestionStage) -> &'static str {
    match stage {
        IngestionStage::Validate => "validate",
        IngestionStage::Extract => "extract",
        IngestionStage::Chunk => "chunk",
        IngestionStage::Embed => "embed",
        IngestionStage::Publish => "publish",
        IngestionStage::Reconcile => "reconcile",
    }
}

fn ingestion_status_token(status: IngestionJobStatus) -> &'static str {
    match status {
        IngestionJobStatus::Validating => "validating",
        IngestionJobStatus::Extracting => "extracting",
        IngestionJobStatus::Chunking => "chunking",
        IngestionJobStatus::Embedding => "embedding",
        IngestionJobStatus::Publishing => "publishing",
        IngestionJobStatus::Ready => "ready",
        IngestionJobStatus::Failed => "failed",
    }
}

fn parse_document_state(value: &str) -> Result<DocumentState, StorageError> {
    match value {
        "staging" => Ok(DocumentState::Staging),
        "validating" => Ok(DocumentState::Validating),
        "indexing" => Ok(DocumentState::Indexing),
        "ready" => Ok(DocumentState::Ready),
        "failed" => Ok(DocumentState::Failed),
        other => Err(StorageError::Internal(format!(
            "unknown document state: {other}"
        ))),
    }
}

fn parse_ingestion_stage(value: &str) -> Result<IngestionStage, StorageError> {
    match value {
        "validate" => Ok(IngestionStage::Validate),
        "extract" => Ok(IngestionStage::Extract),
        "chunk" => Ok(IngestionStage::Chunk),
        "embed" => Ok(IngestionStage::Embed),
        "publish" => Ok(IngestionStage::Publish),
        "reconcile" => Ok(IngestionStage::Reconcile),
        other => Err(StorageError::Internal(format!(
            "unknown ingestion stage: {other}"
        ))),
    }
}

fn parse_ingestion_status(value: &str) -> Result<IngestionJobStatus, StorageError> {
    match value {
        "validating" => Ok(IngestionJobStatus::Validating),
        "extracting" => Ok(IngestionJobStatus::Extracting),
        "chunking" => Ok(IngestionJobStatus::Chunking),
        "embedding" => Ok(IngestionJobStatus::Embedding),
        "publishing" => Ok(IngestionJobStatus::Publishing),
        "ready" => Ok(IngestionJobStatus::Ready),
        "failed" => Ok(IngestionJobStatus::Failed),
        other => Err(StorageError::Internal(format!(
            "unknown ingestion job status: {other}"
        ))),
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
