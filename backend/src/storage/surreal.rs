//! SurrealDB implementation of the [`Storage`] trait.
//!
//! Queries are intentionally bare — no `WHERE tenant_id = …` clauses,
//! no `tenant_id` in CREATE/UPDATE payloads. The connection backing
//! this struct is expected to be a JWT-authenticated RECORD session
//! (constructed by [`super::RequestDbPool::acquire`]); engine-side
//! `PERMISSIONS` clauses and the schema's `DEFAULT $auth.tenant_id`
//! handle tenant scoping. A misbuilt query cannot escape its tenant —
//! SurrealDB refuses.
//!
//! Wire-shape concern: SurrealDB rejects raw RFC3339 strings on `TYPE
//! datetime` columns, so the public `Document` model uses
//! `chrono::DateTime<Utc>` and a private `DocumentWire` struct converts
//! to/from `surrealdb::Datetime` at the (de)serialize boundary.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::engine::any::Any;
use surrealdb::{Datetime, RecordId, Surreal};

use crate::error::{Error, Result};
use crate::storage::{
    ChatMessage, Chunk, ChunkId, ChunkSearchResult, Content, Conversation, ConversationId, DocId,
    Document, FeedCursor, Filters, MessageId, Storage,
};

/// Storage trait implementation against a SurrealDB connection.
///
/// Constructed by [`super::RequestDbPool::acquire`] wrapping a
/// JWT-authenticated session. Also instantiable for tests that drive
/// the engine directly.
pub struct SurrealStorage {
    db: Surreal<Any>,
}

impl SurrealStorage {
    /// Wrap an existing connection. The connection must already be
    /// signed in (root for system path, or authenticated via JWT for
    /// the request path) and have `use_ns` / `use_db` configured.
    pub fn from_handle(db: Surreal<Any>) -> Self {
        Self { db }
    }
}

// ─── wire structs ─────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
struct DocumentWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<RecordId>,
    /// Engine fills this on CREATE via DEFAULT $auth.tenant_id; we skip
    /// it on serialize so application code never accidentally sets it.
    /// Populated on read so SSE filtering can see the tenant the row
    /// belongs to.
    #[serde(default, skip_serializing)]
    tenant_id: Option<RecordId>,
    canonical_id: String,
    source_type: String,
    source_uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    storage_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(default)]
    authors: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    published_at: Option<Datetime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ingested_at: Option<Datetime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    summary: Option<String>,
    content_hash: String,
    #[serde(default = "default_version")]
    version: i64,
    #[serde(default)]
    metadata: serde_json::Value,
}

fn default_version() -> i64 {
    1
}

impl From<&Document> for DocumentWire {
    fn from(d: &Document) -> Self {
        Self {
            id: d.id.clone(),
            tenant_id: None,
            canonical_id: d.canonical_id.clone(),
            source_type: d.source_type.clone(),
            source_uri: d.source_uri.clone(),
            storage_uri: d.storage_uri.clone(),
            title: d.title.clone(),
            authors: d.authors.clone(),
            published_at: d.published_at.map(Datetime::from),
            ingested_at: d.ingested_at.map(Datetime::from),
            language: d.language.clone(),
            summary: d.summary.clone(),
            content_hash: d.content_hash.clone(),
            version: d.version,
            metadata: d.metadata.clone(),
        }
    }
}

impl From<DocumentWire> for Document {
    fn from(w: DocumentWire) -> Self {
        Self {
            id: w.id,
            tenant_id: w.tenant_id,
            canonical_id: w.canonical_id,
            source_type: w.source_type,
            source_uri: w.source_uri,
            storage_uri: w.storage_uri,
            title: w.title,
            authors: w.authors,
            published_at: w.published_at.map(datetime_to_chrono),
            ingested_at: w.ingested_at.map(datetime_to_chrono),
            language: w.language,
            summary: w.summary,
            content_hash: w.content_hash,
            version: w.version,
            metadata: w.metadata,
        }
    }
}

fn datetime_to_chrono(d: Datetime) -> DateTime<Utc> {
    d.into_inner().into()
}

#[derive(Debug, Deserialize)]
struct ConversationWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<RecordId>,
    #[serde(default)]
    tenant_id: Option<RecordId>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    created_at: Option<Datetime>,
    #[serde(default)]
    updated_at: Option<Datetime>,
}

impl From<ConversationWire> for Conversation {
    fn from(w: ConversationWire) -> Self {
        Self {
            id: w.id,
            tenant_id: w.tenant_id,
            title: w.title,
            created_at: w.created_at.map(datetime_to_chrono),
            updated_at: w.updated_at.map(datetime_to_chrono),
        }
    }
}

#[derive(Debug, Deserialize)]
struct ChatMessageWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<RecordId>,
    role: String,
    content: String,
    #[serde(default)]
    created_at: Option<Datetime>,
}

impl From<ChatMessageWire> for ChatMessage {
    fn from(w: ChatMessageWire) -> Self {
        Self {
            id: w.id,
            role: w.role,
            content: w.content,
            created_at: w.created_at.map(datetime_to_chrono),
        }
    }
}

#[derive(Debug, Deserialize)]
struct IdRow {
    id: RecordId,
}

#[derive(Debug, Serialize)]
struct ContentData {
    doc: RecordId,
    format: String,
    text: String,
    extractor: String,
}

#[derive(Debug, Serialize)]
struct ChunkData {
    doc: RecordId,
    ordinal: i64,
    char_start: i64,
    char_end: i64,
    page: Option<i64>,
    bbox: Option<serde_json::Value>,
    text: String,
    embedding: Vec<f32>,
    embedding_model: String,
    chunk_strategy: String,
}

#[async_trait]
impl Storage for SurrealStorage {
    // ---- documents ---------------------------------------------------------

    async fn upsert_document(&self, doc: &Document) -> Result<DocId> {
        // Look up by canonical_id alone — PERMISSIONS already scope to
        // the caller's tenant.
        let mut response = self
            .db
            .query("SELECT id FROM document WHERE canonical_id = $cid LIMIT 1")
            .bind(("cid", doc.canonical_id.clone()))
            .await?;
        let existing: Option<IdRow> = response.take(0)?;

        let wire = DocumentWire::from(doc);

        if let Some(IdRow { id }) = existing {
            self.db
                .query("UPDATE $rid MERGE $data")
                .bind(("rid", id.clone()))
                .bind(("data", wire))
                .await?
                .check()?;
            Ok(id)
        } else {
            let mut response = self
                .db
                .query("CREATE document CONTENT $data RETURN id")
                .bind(("data", wire))
                .await?;
            let row: Option<IdRow> = response.take(0)?;
            row.map(|r| r.id).ok_or(Error::EmptyResult)
        }
    }

    async fn get_document(&self, id: &DocId) -> Result<Option<Document>> {
        let mut response = self
            .db
            .query("SELECT * FROM $rid LIMIT 1")
            .bind(("rid", id.clone()))
            .await?;
        let row: Option<DocumentWire> = response.take(0)?;
        Ok(row.map(Document::from))
    }

    async fn get_document_by_canonical(&self, canonical_id: &str) -> Result<Option<Document>> {
        let mut response = self
            .db
            .query("SELECT * FROM document WHERE canonical_id = $cid LIMIT 1")
            .bind(("cid", canonical_id.to_string()))
            .await?;
        let row: Option<DocumentWire> = response.take(0)?;
        Ok(row.map(Document::from))
    }

    async fn delete_document(&self, id: &DocId) -> Result<()> {
        // Cascade manually — SurrealDB has no ON DELETE CASCADE.
        // PERMISSIONS on every child table will refuse cross-tenant
        // rows, so no application-side guard needed.
        self.db
            .query("DELETE document_content WHERE doc = $rid")
            .bind(("rid", id.clone()))
            .await?
            .check()?;
        self.db
            .query("DELETE chunk WHERE doc = $rid")
            .bind(("rid", id.clone()))
            .await?
            .check()?;
        self.db
            .query("DELETE document_version WHERE doc = $rid")
            .bind(("rid", id.clone()))
            .await?
            .check()?;
        self.db
            .query("DELETE $rid")
            .bind(("rid", id.clone()))
            .await?
            .check()?;
        Ok(())
    }

    // ---- content -----------------------------------------------------------

    async fn upsert_content(&self, doc_id: &DocId, content: &Content) -> Result<()> {
        let mut response = self
            .db
            .query("SELECT id FROM document_content WHERE doc = $rid LIMIT 1")
            .bind(("rid", doc_id.clone()))
            .await?;
        let existing: Option<IdRow> = response.take(0)?;

        let data = ContentData {
            doc: doc_id.clone(),
            format: content.format.clone(),
            text: content.text.clone(),
            extractor: content.extractor.clone(),
        };

        if let Some(IdRow { id }) = existing {
            self.db
                .query("UPDATE $rid MERGE $data")
                .bind(("rid", id))
                .bind(("data", data))
                .await?
                .check()?;
        } else {
            self.db
                .query("CREATE document_content CONTENT $data")
                .bind(("data", data))
                .await?
                .check()?;
        }
        Ok(())
    }

    async fn get_content(&self, doc_id: &DocId) -> Result<Option<Content>> {
        let mut response = self
            .db
            .query(
                "SELECT format, text, extractor FROM document_content \
                 WHERE doc = $rid LIMIT 1",
            )
            .bind(("rid", doc_id.clone()))
            .await?;
        Ok(response.take(0)?)
    }

    // ---- chunks ------------------------------------------------------------

    async fn upsert_chunks(&self, doc_id: &DocId, chunks: &[Chunk]) -> Result<Vec<ChunkId>> {
        let mut ids = Vec::with_capacity(chunks.len());
        for c in chunks {
            let data = ChunkData {
                doc: doc_id.clone(),
                ordinal: c.ordinal,
                char_start: c.char_start,
                char_end: c.char_end,
                page: c.page,
                bbox: c.bbox.clone(),
                text: c.text.clone(),
                embedding: c.embedding.clone(),
                embedding_model: c.embedding_model.clone(),
                chunk_strategy: c.chunk_strategy.clone(),
            };

            let mut response = self
                .db
                .query(
                    "SELECT id FROM chunk \
                     WHERE doc = $rid \
                       AND ordinal = $ord \
                       AND embedding_model = $model \
                       AND chunk_strategy = $strategy \
                     LIMIT 1",
                )
                .bind(("rid", doc_id.clone()))
                .bind(("ord", c.ordinal))
                .bind(("model", c.embedding_model.clone()))
                .bind(("strategy", c.chunk_strategy.clone()))
                .await?;
            let existing: Option<IdRow> = response.take(0)?;

            if let Some(IdRow { id }) = existing {
                self.db
                    .query("UPDATE $rid MERGE $data")
                    .bind(("rid", id.clone()))
                    .bind(("data", data))
                    .await?
                    .check()?;
                ids.push(id);
            } else {
                let mut response = self
                    .db
                    .query("CREATE chunk CONTENT $data RETURN id")
                    .bind(("data", data))
                    .await?;
                let row: Option<IdRow> = response.take(0)?;
                ids.push(row.map(|r| r.id).ok_or(Error::EmptyResult)?);
            }
        }
        Ok(ids)
    }

    async fn list_chunks(&self, doc_id: &DocId) -> Result<Vec<Chunk>> {
        let mut response = self
            .db
            .query("SELECT * FROM chunk WHERE doc = $rid ORDER BY ordinal ASC")
            .bind(("rid", doc_id.clone()))
            .await?;
        Ok(response.take(0)?)
    }

    async fn delete_chunks(&self, doc_id: &DocId) -> Result<()> {
        self.db
            .query("DELETE chunk WHERE doc = $rid")
            .bind(("rid", doc_id.clone()))
            .await?
            .check()?;
        Ok(())
    }

    // ---- search ------------------------------------------------------------

    async fn search_vector(
        &self,
        query: &[f32],
        top_k: usize,
        filters: &Filters,
    ) -> Result<Vec<ChunkSearchResult>> {
        let where_clause = build_filter_clause(filters);
        let sql = format!(
            "SELECT \
                id AS chunk_id, \
                doc AS doc_id, \
                ordinal, char_start, char_end, page, text, \
                vector::distance::knn() AS score \
             FROM chunk \
             WHERE embedding <|$k|> $q {where_clause} \
             ORDER BY score ASC \
             LIMIT $k"
        );

        let mut q = self
            .db
            .query(sql)
            .bind(("k", top_k as i64))
            .bind(("q", query.to_vec()));
        if let Some(v) = &filters.embedding_model {
            q = q.bind(("f_model", v.clone()));
        }
        if let Some(v) = &filters.chunk_strategy {
            q = q.bind(("f_strategy", v.clone()));
        }
        if let Some(v) = &filters.source_type {
            q = q.bind(("f_source_type", v.clone()));
        }
        let mut response = q.await?;
        Ok(response.take(0)?)
    }

    async fn search_keyword(
        &self,
        query: &str,
        top_k: usize,
        filters: &Filters,
    ) -> Result<Vec<ChunkSearchResult>> {
        let where_clause = build_filter_clause(filters);
        let sql = format!(
            "SELECT \
                id AS chunk_id, \
                doc AS doc_id, \
                ordinal, char_start, char_end, page, text, \
                search::score(0) AS score \
             FROM chunk \
             WHERE text @0@ $q {where_clause} \
             ORDER BY score DESC \
             LIMIT $k"
        );

        let mut q = self
            .db
            .query(sql)
            .bind(("k", top_k as i64))
            .bind(("q", query.to_string()));
        if let Some(v) = &filters.embedding_model {
            q = q.bind(("f_model", v.clone()));
        }
        if let Some(v) = &filters.chunk_strategy {
            q = q.bind(("f_strategy", v.clone()));
        }
        if let Some(v) = &filters.source_type {
            q = q.bind(("f_source_type", v.clone()));
        }
        let mut response = q.await?;
        Ok(response.take(0)?)
    }

    // ---- conversations -----------------------------------------------------

    async fn create_conversation(&self, title: Option<&str>) -> Result<ConversationId> {
        // CREATE … RETURN id. tenant_id/user fill in from $auth via DEFAULT.
        let mut response = self
            .db
            .query("CREATE conversation CONTENT { title: $title } RETURN id")
            .bind(("title", title.map(|s| s.to_string())))
            .await?;
        let row: Option<IdRow> = response.take(0)?;
        row.map(|r| r.id).ok_or(Error::EmptyResult)
    }

    async fn list_conversations(&self) -> Result<Vec<Conversation>> {
        let mut response = self
            .db
            .query(
                "SELECT id, tenant_id, title, created_at, updated_at \
                 FROM conversation ORDER BY updated_at DESC",
            )
            .await?;
        let wires: Vec<ConversationWire> = response.take(0)?;
        Ok(wires.into_iter().map(Conversation::from).collect())
    }

    async fn get_conversation(&self, id: &ConversationId) -> Result<Option<Conversation>> {
        let mut response = self
            .db
            .query(
                "SELECT id, tenant_id, title, created_at, updated_at \
                 FROM $rid LIMIT 1",
            )
            .bind(("rid", id.clone()))
            .await?;
        let row: Option<ConversationWire> = response.take(0)?;
        Ok(row.map(Conversation::from))
    }

    async fn list_messages(&self, conv: &ConversationId) -> Result<Vec<ChatMessage>> {
        let mut response = self
            .db
            .query(
                "SELECT id, role, content, created_at \
                 FROM message WHERE conversation = $conv \
                 ORDER BY created_at ASC",
            )
            .bind(("conv", conv.clone()))
            .await?;
        let wires: Vec<ChatMessageWire> = response.take(0)?;
        Ok(wires.into_iter().map(ChatMessage::from).collect())
    }

    async fn append_message(
        &self,
        conv: &ConversationId,
        role: &str,
        content: &str,
    ) -> Result<MessageId> {
        // Two statements in one round-trip: create the message, bump the
        // parent's updated_at. Engine PERMISSIONS on both tables refuse
        // the write if the caller doesn't own the conversation.
        let mut response = self
            .db
            .query(
                "CREATE message CONTENT { \
                    conversation: $conv, \
                    role: $role, \
                    content: $content \
                 } RETURN id; \
                 UPDATE $conv SET updated_at = time::now()",
            )
            .bind(("conv", conv.clone()))
            .bind(("role", role.to_string()))
            .bind(("content", content.to_string()))
            .await?;
        let row: Option<IdRow> = response.take(0)?;
        row.map(|r| r.id).ok_or(Error::EmptyResult)
    }

    async fn rename_conversation(&self, id: &ConversationId, title: &str) -> Result<()> {
        self.db
            .query("UPDATE $rid SET title = $title, updated_at = time::now()")
            .bind(("rid", id.clone()))
            .bind(("title", title.to_string()))
            .await?
            .check()?;
        Ok(())
    }

    async fn delete_conversation(&self, id: &ConversationId) -> Result<()> {
        // Cascade: messages first, then the conversation itself. Engine
        // PERMISSIONS will refuse cross-tenant rows, so no application
        // guard is needed; if the row doesn't exist, both DELETEs are
        // no-ops — idempotent.
        self.db
            .query(
                "DELETE message WHERE conversation = $rid; \
                 DELETE $rid",
            )
            .bind(("rid", id.clone()))
            .await?
            .check()?;
        Ok(())
    }

    // ---- discovery feed ----------------------------------------------------

    async fn list_feed(&self, cursor: Option<FeedCursor>, limit: usize) -> Result<Vec<Document>> {
        let where_cursor = if cursor.is_some() {
            "WHERE (ingested_at < $cursor_ts \
                    OR (ingested_at = $cursor_ts AND id < $cursor_id))"
        } else {
            ""
        };
        let sql = format!(
            "SELECT * FROM document {where_cursor} \
             ORDER BY ingested_at DESC, id DESC \
             LIMIT $limit"
        );
        let mut q = self.db.query(sql).bind(("limit", limit as i64));
        if let Some(c) = cursor {
            q = q
                .bind(("cursor_ts", Datetime::from(c.ingested_at)))
                .bind(("cursor_id", c.id));
        }
        let mut response = q.await?;
        let wires: Vec<DocumentWire> = response.take(0)?;
        Ok(wires.into_iter().map(Document::from).collect())
    }
}

fn build_filter_clause(f: &Filters) -> String {
    let mut parts = Vec::new();
    if f.embedding_model.is_some() {
        parts.push("embedding_model = $f_model");
    }
    if f.chunk_strategy.is_some() {
        parts.push("chunk_strategy = $f_strategy");
    }
    if f.source_type.is_some() {
        parts.push("doc.source_type = $f_source_type");
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("AND {}", parts.join(" AND "))
    }
}
