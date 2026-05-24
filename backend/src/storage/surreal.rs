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
use surrealdb::engine::any::Any;
use surrealdb::types::{Datetime, RecordId, SurrealValue, ToSql};
use surrealdb::Surreal;

use crate::error::{Error, Result};
use crate::storage::models::{content_without_none, IngestionRejectionWire, UploadSessionWire};
use crate::storage::{
    Bbox, ChatMessage, Chunk, ChunkId, ChunkSearchResult, Citation, Content, Conversation,
    ConversationId, CreateUploadSessionParams, DocId, Document, FeedCursor, Filters,
    IngestionRejection, MessageId, Storage, UploadSession,
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
//
// surrealdb 3 binds/extracts via `SurrealValue`, not serde — and the derive
// has no `skip`/`skip_serializing` equivalent: every field is emitted on
// write. So the write payloads (`*Create`/`*Data`) physically omit `id` and
// `tenant_id`; the engine fills `tenant_id` from `DEFAULT $auth.tenant_id`
// and would be *overwritten with NONE* (failing the `ASSERT $value != NONE`,
// or — worse, via `UPDATE … MERGE` — nulling a good value) if we sent them.
// Reads use separate `*Read` structs that carry `id`/`tenant_id` back.

/// Write payload for `document` (CREATE CONTENT / UPDATE MERGE). No `id`,
/// no `tenant_id` — see the module note above.
#[derive(Debug, SurrealValue)]
struct DocumentCreate {
    #[surreal(default)]
    canonical_id: Option<String>,
    source_type: String,
    source_uri: String,
    #[surreal(default)]
    storage_uri: Option<String>,
    #[surreal(default)]
    title: Option<String>,
    authors: Vec<String>,
    #[surreal(default)]
    published_at: Option<Datetime>,
    #[surreal(default)]
    ingested_at: Option<Datetime>,
    #[surreal(default)]
    language: Option<String>,
    #[surreal(default)]
    summary: Option<String>,
    #[surreal(default)]
    paper_embedding: Option<Vec<f32>>,
    #[surreal(default)]
    paper_embedding_model: Option<String>,
    content_hash: String,
    version: i64,
    metadata: serde_json::Value,
}

impl From<&Document> for DocumentCreate {
    fn from(d: &Document) -> Self {
        Self {
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
            paper_embedding: d.paper_embedding.clone(),
            paper_embedding_model: d.paper_embedding_model.clone(),
            content_hash: d.content_hash.clone(),
            version: d.version,
            metadata: d.metadata.clone(),
        }
    }
}

/// Read projection for `document`. Carries `id`/`tenant_id` (engine-set)
/// back so SSE filtering and deep-links can see them.
#[derive(Debug, SurrealValue)]
struct DocumentRead {
    #[surreal(default)]
    id: Option<RecordId>,
    /// Populated on read so SSE filtering can see the tenant the row
    /// belongs to.
    #[surreal(default)]
    tenant_id: Option<RecordId>,
    #[surreal(default)]
    canonical_id: Option<String>,
    source_type: String,
    source_uri: String,
    #[surreal(default)]
    storage_uri: Option<String>,
    #[surreal(default)]
    title: Option<String>,
    #[surreal(default)]
    authors: Vec<String>,
    #[surreal(default)]
    published_at: Option<Datetime>,
    #[surreal(default)]
    ingested_at: Option<Datetime>,
    #[surreal(default)]
    language: Option<String>,
    #[surreal(default)]
    summary: Option<String>,
    #[surreal(default)]
    paper_embedding: Option<Vec<f32>>,
    #[surreal(default)]
    paper_embedding_model: Option<String>,
    content_hash: String,
    #[surreal(default = "default_version")]
    version: i64,
    #[surreal(default)]
    metadata: serde_json::Value,
}

fn default_version() -> i64 {
    1
}

impl From<DocumentRead> for Document {
    fn from(w: DocumentRead) -> Self {
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
            paper_embedding: w.paper_embedding,
            paper_embedding_model: w.paper_embedding_model,
            content_hash: w.content_hash,
            version: w.version,
            metadata: w.metadata,
        }
    }
}

fn datetime_to_chrono(d: Datetime) -> DateTime<Utc> {
    d.into_inner()
}

#[derive(Debug, SurrealValue)]
struct ConversationWire {
    #[surreal(default)]
    id: Option<RecordId>,
    #[surreal(default)]
    tenant_id: Option<RecordId>,
    #[surreal(default)]
    title: Option<String>,
    #[surreal(default)]
    created_at: Option<Datetime>,
    #[surreal(default)]
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

#[derive(Debug, SurrealValue)]
struct ChatMessageWire {
    #[surreal(default)]
    id: Option<RecordId>,
    role: String,
    content: String,
    #[surreal(default)]
    parent_id: Option<RecordId>,
    #[surreal(default)]
    citations: Option<Vec<Citation>>,
    #[surreal(default)]
    created_at: Option<Datetime>,
}

impl From<ChatMessageWire> for ChatMessage {
    fn from(w: ChatMessageWire) -> Self {
        Self {
            id: w.id,
            role: w.role,
            content: w.content,
            parent_id: w.parent_id,
            citations: w.citations,
            created_at: w.created_at.map(datetime_to_chrono),
        }
    }
}

#[derive(Debug, SurrealValue)]
struct IdRow {
    id: RecordId,
}

#[derive(Debug, SurrealValue)]
struct ContentData {
    doc: RecordId,
    format: String,
    text: String,
    extractor: String,
}

#[derive(Debug, SurrealValue)]
struct ChunkData {
    doc: RecordId,
    ordinal: i64,
    char_start: i64,
    char_end: i64,
    bboxes: Option<Vec<Bbox>>,
    text: String,
    embedding: Vec<f32>,
    embedding_model: String,
    chunk_strategy: String,
}

/// Assistant-message write payload for `commit_turn`. Bound as a single
/// `CONTENT $struct` value: surrealdb 3 only honours the `FLEXIBLE` rule
/// for the nested `citations` objects on a SCHEMAFULL table when the row
/// is written this way — an inline `{ citations: $x }` literal trips
/// "no such field exists" instead.
#[derive(Debug, SurrealValue)]
struct AssistantContent {
    conversation: RecordId,
    role: String,
    content: String,
    parent_id: RecordId,
    #[surreal(default)]
    citations: Option<Vec<Citation>>,
}

#[async_trait]
impl Storage for SurrealStorage {
    // ---- documents ---------------------------------------------------------

    async fn upsert_document(&self, doc: &Document) -> Result<DocId> {
        // Look up by canonical_id alone — PERMISSIONS already scope to
        // the caller's tenant. Skip entirely when canonical_id is unset:
        // such rows (manual uploads) are never deduped, and matching on
        // `canonical_id = NONE` would false-match every prior NONE row.
        let existing: Option<IdRow> = match &doc.canonical_id {
            Some(cid) => {
                let mut response = self
                    .db
                    .query("SELECT id FROM document WHERE canonical_id = $cid LIMIT 1")
                    .bind(("cid", cid.clone()))
                    .await?;
                response.take(0)?
            }
            None => None,
        };

        let data = content_without_none(DocumentCreate::from(doc));

        if let Some(IdRow { id }) = existing {
            self.db
                .query("UPDATE $rid MERGE $data")
                .bind(("rid", id.clone()))
                .bind(("data", data))
                .await?
                .check()?;
            Ok(id)
        } else {
            let mut response = self
                .db
                .query("CREATE document CONTENT $data RETURN id")
                .bind(("data", data))
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
        let row: Option<DocumentRead> = response.take(0)?;
        Ok(row.map(Document::from))
    }

    async fn get_document_by_canonical(&self, canonical_id: &str) -> Result<Option<Document>> {
        let mut response = self
            .db
            .query("SELECT * FROM document WHERE canonical_id = $cid LIMIT 1")
            .bind(("cid", canonical_id.to_string()))
            .await?;
        let row: Option<DocumentRead> = response.take(0)?;
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
                bboxes: c.bboxes.clone(),
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

    async fn get_chunk(&self, id: &ChunkId) -> Result<Option<Chunk>> {
        let mut response = self
            .db
            .query("SELECT * FROM $rid LIMIT 1")
            .bind(("rid", id.clone()))
            .await?;
        Ok(response.take(0)?)
    }

    async fn list_chunks_in_range(
        &self,
        doc_id: &DocId,
        ord_lo: i64,
        ord_hi: i64,
    ) -> Result<Vec<Chunk>> {
        let mut response = self
            .db
            .query(
                "SELECT * FROM chunk \
                 WHERE doc = $rid AND ordinal >= $lo AND ordinal <= $hi \
                 ORDER BY ordinal ASC",
            )
            .bind(("rid", doc_id.clone()))
            .bind(("lo", ord_lo))
            .bind(("hi", ord_hi))
            .await?;
        Ok(response.take(0)?)
    }

    // ---- search ------------------------------------------------------------

    async fn search_vector(
        &self,
        query: &[f32],
        top_k: usize,
        filters: &Filters,
    ) -> Result<Vec<ChunkSearchResult>> {
        let where_filters = build_filter_clause_no_and(filters);
        let where_clause = if where_filters.is_empty() {
            String::new()
        } else {
            format!("WHERE {where_filters}")
        };
        // We use brute-force cosine similarity rather than the HNSW
        // `<|N|>` operator. Reasoning: the HNSW operator silently
        // returns empty when the index hasn't built sufficient layers
        // (small corpora / certain engine builds — including the
        // in-memory engine the integration tests run against). Brute
        // force is O(N) but our corpora are well under the threshold
        // where that matters. The schema's HNSW index is kept for
        // forward-compatibility; switching to it once we hit a
        // performance ceiling is one query-string change.
        let k_lit = sanitize_top_k(top_k);
        let sql = format!(
            "SELECT \
                id AS chunk_id, \
                doc AS doc_id, \
                ordinal, char_start, char_end, text, \
                (1 - vector::similarity::cosine(embedding, $q)) AS score \
             FROM chunk \
             {where_clause} \
             ORDER BY score ASC \
             LIMIT {k_lit}"
        );

        let mut q = self.db.query(sql).bind(("q", query.to_vec()));
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
                ordinal, char_start, char_end, text, \
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
                "SELECT id, role, content, parent_id, citations, created_at \
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
        //
        // Production chat writes go through `commit_turn` so the
        // user+assistant pair is atomic with "last writer wins"
        // semantics. `append_message` is kept for tests and ad-hoc
        // single-message inserts.
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

    async fn commit_turn(
        &self,
        conv: &ConversationId,
        user_message_id: &str,
        user_text: &str,
        parent_id: Option<&MessageId>,
        assistant_text: &str,
        citations: &[Citation],
    ) -> Result<MessageId> {
        // Atomic transaction:
        //   1. DELETE any messages created after our parent (the
        //      "last writer wins" step — drops a competing turn that
        //      committed first against the same parent).
        //   2. CREATE the user message with the client-provided ULID.
        //   3. CREATE the assistant message linked to the user message.
        //   4. Bump the conversation's updated_at.
        //
        // The first statement is parametrised on the parent's
        // `created_at`. When there is no parent (first turn) we use
        // `time::EPOCH` so the WHERE clause is `created_at > epoch`,
        // which matches every prior row in the conversation — that's
        // correct: a "first turn" submit declares the conversation was
        // empty, so any rows lying around belonged to a competing first
        // turn and should be wiped.
        //
        // The user record id is `message:<ulid>`. We hand the key into
        // a `type::thing('message', $key)` builder so SurrealDB's
        // parser receives a record literal, not a string.
        let user_rid = RecordId::new("message", user_message_id);
        // The transaction does (in order):
        //   1. LET $parent_ts — bind the parent's created_at (or EPOCH).
        //   2. DELETE any messages newer than that — "last writer wins".
        //   3. CREATE the user message with the client-provided record id.
        //   4. CREATE the assistant message linked to the user message,
        //      with `RETURN id` so we can read the new id back.
        //   5. UPDATE conversation.updated_at.
        //
        // The assistant row is written via `CONTENT $asst_data` (a bound
        // SurrealValue struct) rather than an inline object literal: on a
        // SCHEMAFULL table, surrealdb 3 only applies the `citations` field's
        // FLEXIBLE rule to the nested objects when they arrive as a bound
        // payload (inline `{ citations: $x }` raises "no such field").
        let sql = "
            BEGIN;
            LET $parent_ts = IF $parent_id != NONE
                THEN (SELECT VALUE created_at FROM ONLY $parent_id)
                ELSE time::EPOCH
                END;
            DELETE message
                WHERE conversation = $conv
                  AND created_at > $parent_ts;
            CREATE $user_rid CONTENT {
                conversation: $conv,
                role: 'user',
                content: $user_text,
                parent_id: $parent_id
            };
            CREATE ONLY message CONTENT $asst_data RETURN id;
            UPDATE $conv SET updated_at = time::now();
            COMMIT;
        ";
        // Store `NONE` (not `[]`) for an uncited turn, so user rows and
        // citationless assistant rows read back as `citations: None`.
        let citations_bind: Option<Vec<Citation>> = if citations.is_empty() {
            None
        } else {
            Some(citations.to_vec())
        };
        let asst_data = AssistantContent {
            conversation: conv.clone(),
            role: "assistant".to_string(),
            content: assistant_text.to_string(),
            parent_id: user_rid.clone(),
            citations: citations_bind,
        };
        let mut response = self
            .db
            .query(sql)
            .bind(("conv", conv.clone()))
            .bind(("user_rid", user_rid))
            .bind(("user_text", user_text.to_string()))
            .bind(("parent_id", parent_id.cloned()))
            .bind(("asst_data", asst_data))
            .await?
            .check()?;
        // Statement-slot map (surrealdb 3): `BEGIN` consumes slot 0, then
        // every statement gets its own slot, so the data statements are
        // shifted one past their position in the SQL text:
        //   0: BEGIN
        //   1: LET $parent_ts
        //   2: DELETE
        //   3: CREATE user
        //   4: CREATE ONLY assistant RETURN id  ← what we want
        //   5: UPDATE
        let row: Option<IdRow> = response.take(4)?;
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
        let wires: Vec<DocumentRead> = response.take(0)?;
        Ok(wires.into_iter().map(Document::from).collect())
    }

    // ---- ingestion v2: upload sessions -------------------------------------

    async fn create_upload_session(
        &self,
        params: &CreateUploadSessionParams,
    ) -> Result<UploadSession> {
        // Engine fills tenant_id / user_id from $auth via DEFAULT.
        // PERMISSIONS gate the row to the caller; a tenant-mismatched
        // canonical_id triggers the UNIQUE index → SurrealDB error,
        // propagated to the handler as a 409.
        // surrealdb 3 distinguishes NULL from NONE: a JSON `null`
        // (`serde_json::Value::Null`) binds as SurrealDB `NULL`, which
        // fails to coerce into the non-optional `declared_metadata object
        // DEFAULT {}` column (DEFAULT only fills NONE/absent). Manual
        // uploads carry no metadata, so normalise `null` → `{}`.
        let declared_metadata = if params.declared_metadata.is_null() {
            serde_json::json!({})
        } else {
            params.declared_metadata.clone()
        };
        let mut response = self
            .db
            .query(
                "CREATE upload_session CONTENT { \
                    doc_id: $doc_id, \
                    s3_key: $s3_key, \
                    s3_upload_id: $s3_upload_id, \
                    state: 'uploading', \
                    canonical_id: $canonical_id, \
                    dedup_key: $dedup_key, \
                    source_type: $source_type, \
                    source_uri: $source_uri, \
                    title: $title, \
                    filename: $filename, \
                    declared_size: $declared_size, \
                    declared_content_type: $declared_content_type, \
                    declared_metadata: $declared_metadata \
                 }",
            )
            .bind(("doc_id", params.doc_id.clone()))
            .bind(("s3_key", params.s3_key.clone()))
            .bind(("s3_upload_id", params.s3_upload_id.clone()))
            .bind(("canonical_id", params.canonical_id.clone()))
            .bind(("dedup_key", params.dedup_key.clone()))
            .bind(("source_type", params.source_type.clone()))
            .bind(("source_uri", params.source_uri.clone()))
            .bind(("title", params.title.clone()))
            .bind(("filename", params.filename.clone()))
            .bind(("declared_size", params.declared_size as i64))
            .bind((
                "declared_content_type",
                params.declared_content_type.clone(),
            ))
            .bind(("declared_metadata", declared_metadata))
            .await?
            .check()?;
        let row: Option<UploadSessionWire> = response.take(0)?;
        row.map(UploadSession::from).ok_or(Error::EmptyResult)
    }

    async fn get_upload_session(&self, doc_id: &str) -> Result<Option<UploadSession>> {
        let mut response = self
            .db
            .query("SELECT * FROM upload_session WHERE doc_id = $d LIMIT 1")
            .bind(("d", doc_id.to_string()))
            .await?;
        let row: Option<UploadSessionWire> = response.take(0)?;
        Ok(row.map(UploadSession::from))
    }

    async fn cas_upload_session_state(&self, doc_id: &str, from: &str, to: &str) -> Result<bool> {
        // Two-statement transaction to read the affected-row count
        // without serializing the row itself (`UPDATE … RETURN AFTER`
        // can return a SurrealDB record-id type that doesn't round-trip
        // through `serde_json::Value`). `RETURN id` keeps the response
        // shape narrow.
        let mut response = self
            .db
            .query(
                "UPDATE upload_session SET state = $to \
                 WHERE doc_id = $d AND state = $from \
                 RETURN id",
            )
            .bind(("d", doc_id.to_string()))
            .bind(("from", from.to_string()))
            .bind(("to", to.to_string()))
            .await?
            .check()?;
        let rows: Vec<IdRow> = response.take(0)?;
        Ok(!rows.is_empty())
    }

    async fn commit_upload(
        &self,
        doc_id: &str,
        doc: &Document,
        content: &Content,
        dedup_key: Option<&str>,
    ) -> Result<DocId> {
        // Pre-check for canonical_id conflict so we can return the
        // existing doc id to the SPA (rather than a UNIQUE-constraint
        // error from inside the transaction). Engine PERMISSIONS scope
        // both queries to the caller's tenant.
        //
        // CRITICAL: skip the pre-check entirely when canonical_id is
        // unset. Manual uploads carry no canonical_id (identity is the
        // record id) and are never deduped; `WHERE canonical_id = NONE`
        // would false-match every prior manual upload and 422 every
        // upload after the first.
        if let Some(cid) = &doc.canonical_id {
            let mut conflict = self
                .db
                .query("SELECT id FROM document WHERE canonical_id = $cid LIMIT 1")
                .bind(("cid", cid.clone()))
                .await?;
            let existing: Option<IdRow> = conflict.take(0)?;
            if let Some(IdRow { id }) = existing {
                return Err(Error::CanonicalIdConflict {
                    existing_doc_id: id.to_sql(),
                });
            }
        }

        // One Surreal transaction:
        //   1. CREATE document:<doc_id> — deterministic record id (doc_id
        //      is a lowercased ULID, a legal record-id key: [0-9a-z]).
        //      This is what lets `GET /uploads/:id` resolve `ready` by
        //      record-id lookup after the session row is gone (B5).
        //   2. UPSERT the extracted text into document_content
        //      (document_content_doc is UNIQUE, so a retried commit must
        //      not double-insert — UPSERT keys on `doc`).
        //   3. DELETE the upload_session row.
        // dedup_key is overlaid by a follow-up UPDATE (CONTENT can't be
        // combined with SET in one CREATE). The engine can't compute it
        // (it'd see tenant_id as NONE before DEFAULT runs; see schema
        // comment), so the app supplies it. A colliding key trips the
        // UNIQUE index → a non-transient error the handler maps (the
        // app-level pre-check above already caught the common set-cid case).
        let sql = "
            BEGIN;
            CREATE ONLY $rid CONTENT $data RETURN id;
            UPDATE $rid SET dedup_key = $dedup_key;
            UPSERT document_content CONTENT $content WHERE doc = $rid;
            DELETE upload_session WHERE doc_id = $d;
            COMMIT;
        ";
        // Retry on a transient write-write conflict: SurrealDB uses
        // optimistic concurrency and returns "Resource busy" when
        // concurrent commits collide (e.g. a multi-file upload, or two
        // users at once). The transaction is atomic — a conflicting
        // attempt rolls back fully — and the UPSERT keeps it idempotent,
        // so re-running is safe. Genuine failures (e.g. a dedup_key UNIQUE
        // violation) don't match `is_transient_conflict` and surface
        // immediately.
        let mut attempt = 0usize;
        loop {
            let rid = RecordId::new("document", doc_id);
            let content_data = ContentData {
                doc: rid.clone(),
                format: content.format.clone(),
                text: content.text.clone(),
                extractor: content.extractor.clone(),
            };
            let outcome: Result<DocId> = async {
                let mut response = self
                    .db
                    .query(sql)
                    .bind(("rid", rid))
                    .bind(("data", content_without_none(DocumentCreate::from(doc))))
                    .bind(("dedup_key", dedup_key.map(|s| s.to_string())))
                    .bind(("content", content_data))
                    .bind(("d", doc_id.to_string()))
                    .await?
                    .check()?;
                // Slot 1 = CREATE … RETURN id. surrealdb 3's `BEGIN`
                // consumes slot 0, shifting every statement one past its
                // position in the SQL text (same shift as `commit_turn`).
                let created: Option<IdRow> = response.take(1)?;
                created.map(|r| r.id).ok_or(Error::EmptyResult)
            }
            .await;

            match outcome {
                Err(e) if is_transient_conflict(&e) && attempt + 1 < TX_MAX_COMMIT_ATTEMPTS => {
                    attempt += 1;
                    // Tiny jittered backoff; contention clears in ms.
                    let base = 5u64 << attempt; // 10, 20, 40 …
                    let jitter = (doc_id.len() as u64 * 7 + attempt as u64 * 13) % 11;
                    tokio::time::sleep(std::time::Duration::from_millis(base + jitter)).await;
                    tracing::warn!(doc_id, attempt, "commit_upload transient conflict; retrying");
                    continue;
                }
                other => return other,
            }
        }
    }

    async fn delete_upload_session(&self, doc_id: &str) -> Result<()> {
        self.db
            .query("DELETE upload_session WHERE doc_id = $d")
            .bind(("d", doc_id.to_string()))
            .await?
            .check()?;
        Ok(())
    }

    async fn record_ingestion_rejection(&self, rec: &IngestionRejection) -> Result<()> {
        // PERMISSIONS on ingestion_rejection: `FOR create … WHERE FALSE`
        // — user sessions can't write. The handler routes this through
        // SystemDb (SystemStorage) instead. Surfacing it on AuthedDb
        // returns a clear error so a caller that wires it wrong fails
        // loud.
        let _ = rec;
        Err(Error::NotImplemented(
            "ingestion_rejection writes must go through SystemDb (PERMISSIONS deny user-session writes)".into(),
        ))
    }

    async fn get_ingestion_rejection(&self, doc_id: &str) -> Result<Option<IngestionRejection>> {
        let mut response = self
            .db
            .query(
                "SELECT * FROM ingestion_rejection \
                 WHERE doc_id = $d \
                 ORDER BY rejected_at DESC LIMIT 1",
            )
            .bind(("d", doc_id.to_string()))
            .await?;
        let row: Option<IngestionRejectionWire> = response.take(0)?;
        Ok(row.map(IngestionRejection::from))
    }
}

/// Max attempts for a write transaction that may hit a transient
/// datastore conflict. SurrealDB is optimistic-concurrency; the client
/// is expected to retry. 4 attempts (≈10–80 ms total backoff) clears
/// realistic concurrent-upload contention without masking real failures.
const TX_MAX_COMMIT_ATTEMPTS: usize = 4;

/// True only for SurrealDB's *transient* write-write conflict errors
/// (concurrent commits colliding) — safe to retry. Deliberately does NOT
/// match the generic "failed transaction" wrapper, so a real failure
/// inside the transaction (e.g. a UNIQUE-index violation) is not retried.
/// There's no typed variant, so match the message.
fn is_transient_conflict(e: &Error) -> bool {
    let s = e.to_string().to_ascii_lowercase();
    s.contains("resource busy")
        || s.contains("read or write conflict")
        || s.contains("transaction conflict")
}

/// Clamp `top_k` into a small literal-safe range. SurrealDB's KNN /
/// HNSW operators expect unsigned integer literals (no parameter
/// binding); brute-force `LIMIT` likewise wants a literal in some
/// engine builds. The cap is defence-in-depth against a sloppy caller.
fn sanitize_top_k(k: usize) -> usize {
    k.clamp(1, 1000)
}

fn build_filter_clause(f: &Filters) -> String {
    let inner = build_filter_clause_no_and(f);
    if inner.is_empty() {
        String::new()
    } else {
        format!("AND {inner}")
    }
}

fn build_filter_clause_no_and(f: &Filters) -> String {
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
    parts.join(" AND ")
}
