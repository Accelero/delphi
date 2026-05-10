//! SurrealDB implementation of the [`Storage`] trait.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::Root;
use surrealdb::RecordId;
use surrealdb::Surreal;

use crate::error::{Error, Result};
use crate::storage::{
    Chunk, ChunkId, ChunkSearchResult, Content, Counts, DocId, Document, FeedCursor, FeedItem,
    Filters, Storage,
};

const SCHEMA_SURQL: &str = include_str!("../../schema.surql");

pub struct SurrealStorage {
    db: Surreal<Any>,
}

impl SurrealStorage {
    /// Connect to SurrealDB. The URL drives the engine choice at runtime:
    ///   - `ws://host:port`  → remote WebSocket (production / dev compose)
    ///   - `http://host:port` → remote HTTP
    ///   - `memory` / `mem://` → embedded in-memory (used by tests)
    ///   - `rocksdb:///path`  → embedded persistent (single-process use)
    ///
    /// `signin` is only required for engines that authenticate (the remote
    /// ones); for the in-memory engine we skip it.
    pub async fn connect(
        url: &str,
        user: &str,
        password: &str,
        namespace: &str,
        database: &str,
    ) -> Result<Self> {
        let db = surrealdb::engine::any::connect(url).await?;
        if engine_requires_auth(url) {
            db.signin(Root {
                username: user,
                password,
            })
            .await?;
        }
        db.use_ns(namespace).use_db(database).await?;
        Ok(Self { db })
    }

    /// Borrow the underlying Surreal client. The auth bootstrap module
    /// needs it so it can run user / tenant / membership upserts against
    /// the same multiplexed connection rather than opening a second one.
    pub fn db(&self) -> &Surreal<Any> {
        &self.db
    }
}

impl SurrealStorage {
    /// Spin up an embedded in-memory SurrealDB. **Test-only convenience** —
    /// every call returns a fresh, empty database, so callers should run
    /// `init_schema()` afterwards. Production callers must go through
    /// [`Self::connect`] with a real URL.
    pub async fn in_memory(namespace: &str, database: &str) -> Result<Self> {
        Self::connect("memory", "", "", namespace, database).await
    }
}

/// `memory` / `rocksdb:` are local engines that don't gate on credentials.
/// Anything else (ws/wss/http/https/tcp/…) does.
fn engine_requires_auth(url: &str) -> bool {
    !(url == "memory"
        || url.starts_with("mem://")
        || url.starts_with("rocksdb:")
        || url.starts_with("surrealkv:")
        || url.starts_with("file:"))
}

/// Construct a [`SurrealStorage`] from environment variables.
///
/// Returns the concrete handle (not the [`Storage`] trait object) because
/// `api::serve` also needs the underlying `Surreal<Any>` for tenant/user
/// bootstrapping. Callers that don't need a raw handle should go through
/// [`crate::config::storage_from_env`] instead.
pub(crate) async fn surreal_from_env() -> Result<Arc<SurrealStorage>> {
    let url = env_or("SURREAL_URL", "ws://surrealdb:8000/rpc");
    let user = env_or("SURREAL_USER", "root");
    let password = env_or("SURREAL_PASS", "root");
    let namespace = env_or("SURREAL_NS", "delphi");
    let database = env_or("SURREAL_DB", "main");
    let storage = SurrealStorage::connect(&url, &user, &password, &namespace, &database).await?;
    Ok(Arc::new(storage))
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

#[cfg(test)]
mod feed_query_tests {
    use super::*;
    use crate::storage::{Document, FeedCursor};
    use chrono::{Duration, Utc};
    use surrealdb::RecordId;

    async fn seed_user(s: &SurrealStorage) -> RecordId {
        let mut r = s
            .db
            .query(
                "CREATE app_user CONTENT { iss: 'test', sub: 'u1', email: 'u1@example.com' } RETURN id",
            )
            .await
            .unwrap();
        let row: Option<IdRow> = r.take(0).unwrap();
        row.unwrap().id
    }

    fn doc(canonical_id: &str) -> Document {
        Document {
            id: None,
            canonical_id: canonical_id.into(),
            source_type: "test".into(),
            source_uri: format!("https://test/{canonical_id}"),
            storage_uri: None,
            title: Some(format!("Title {canonical_id}")),
            authors: vec!["A".into()],
            published_at: None,
            ingested_at: None,
            language: None,
            summary: None,
            content_hash: format!("hash-{canonical_id}"),
            version: 1,
            metadata: serde_json::json!({}),
        }
    }

    async fn fresh() -> SurrealStorage {
        let s = SurrealStorage::in_memory("feed_query_test", "main").await.unwrap();
        s.init_schema().await.unwrap();
        s
    }

    #[tokio::test]
    async fn list_feed_returns_empty_when_no_documents() {
        let s = fresh().await;
        let user = seed_user(&s).await;
        let items = s.list_feed(&user, None, 50).await.unwrap();
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn list_feed_orders_newest_first_and_marks_read() {
        let s = fresh().await;
        let user = seed_user(&s).await;

        // Insert three docs and override ingested_at to control ordering.
        let mut ids = Vec::new();
        for (i, c) in ["a", "b", "c"].iter().enumerate() {
            let id = s.upsert_document(&doc(c)).await.unwrap();
            // Backdate so we have predictable ordering: a oldest, c newest.
            let ts = (Utc::now() - Duration::seconds(100 - i as i64 * 10)).to_rfc3339();
            s.db
                .query("UPDATE $rid SET ingested_at = <datetime>$ts")
                .bind(("rid", id.clone()))
                .bind(("ts", ts))
                .await
                .unwrap()
                .check()
                .unwrap();
            ids.push(id);
        }

        // Mark middle doc as read
        s.mark_read(&user, &ids[1]).await.unwrap();

        let items = s.list_feed(&user, None, 50).await.unwrap();
        assert_eq!(items.len(), 3);
        // Newest first → c, b, a
        assert_eq!(items[0].document.canonical_id, "c");
        assert_eq!(items[1].document.canonical_id, "b");
        assert_eq!(items[2].document.canonical_id, "a");
        assert!(!items[0].read, "c should be unread");
        assert!(items[1].read, "b should be read");
        assert!(!items[2].read, "a should be unread");
    }

    #[tokio::test]
    async fn list_feed_paginates_with_cursor() {
        let s = fresh().await;
        let user = seed_user(&s).await;

        for i in 0..5 {
            let id = s.upsert_document(&doc(&format!("d{i}"))).await.unwrap();
            let ts = (Utc::now() - Duration::seconds(100 - i * 10)).to_rfc3339();
            s.db
                .query("UPDATE $rid SET ingested_at = <datetime>$ts")
                .bind(("rid", id))
                .bind(("ts", ts))
                .await
                .unwrap()
                .check()
                .unwrap();
        }

        let page1 = s.list_feed(&user, None, 2).await.unwrap();
        assert_eq!(page1.len(), 2);
        let last = page1.last().unwrap();
        let cursor = FeedCursor {
            ingested_at: last.document.ingested_at.clone().unwrap(),
            id: last.document.id.clone().unwrap(),
        };
        let page2 = s.list_feed(&user, Some(cursor), 2).await.unwrap();
        assert_eq!(page2.len(), 2);

        // No overlap: assert the ids in page2 are not in page1.
        let p1: Vec<_> = page1.iter().filter_map(|i| i.document.id.clone()).collect();
        for item in &page2 {
            let id = item.document.id.clone().unwrap();
            assert!(!p1.contains(&id), "page2 should not overlap page1");
        }
    }

    #[tokio::test]
    async fn mark_read_is_idempotent_and_unread_removes() {
        let s = fresh().await;
        let user = seed_user(&s).await;
        let id = s.upsert_document(&doc("x")).await.unwrap();

        s.mark_read(&user, &id).await.unwrap();
        s.mark_read(&user, &id).await.unwrap(); // should not error
        let items = s.list_feed(&user, None, 50).await.unwrap();
        assert!(items[0].read);

        s.mark_unread(&user, &id).await.unwrap();
        s.mark_unread(&user, &id).await.unwrap(); // should not error
        let items = s.list_feed(&user, None, 50).await.unwrap();
        assert!(!items[0].read);
    }
}

#[cfg(test)]
mod endpoint_tests {
    use super::engine_requires_auth;

    #[test]
    fn auth_required_for_remote_engines() {
        assert!(engine_requires_auth("ws://surrealdb:8000/rpc"));
        assert!(engine_requires_auth("wss://x:8000"));
        assert!(engine_requires_auth("http://x:8000"));
        assert!(engine_requires_auth("https://x:8000"));
    }

    #[test]
    fn auth_skipped_for_local_engines() {
        assert!(!engine_requires_auth("memory"));
        assert!(!engine_requires_auth("mem://"));
        assert!(!engine_requires_auth("rocksdb:/data/db"));
    }
}

#[derive(Debug, Deserialize)]
struct IdRow {
    id: RecordId,
}

#[derive(Debug, Deserialize)]
struct CountRow {
    n: u64,
}

#[derive(Debug, Deserialize)]
struct CursorRow {
    cursor: serde_json::Value,
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
    async fn init_schema(&self) -> Result<()> {
        self.db.query(SCHEMA_SURQL).await?.check()?;
        Ok(())
    }

    // ---- documents ---------------------------------------------------------

    async fn upsert_document(&self, doc: &Document) -> Result<DocId> {
        let mut response = self
            .db
            .query("SELECT id FROM document WHERE canonical_id = $cid LIMIT 1")
            .bind(("cid", doc.canonical_id.clone()))
            .await?;
        let existing: Option<IdRow> = response.take(0)?;

        if let Some(IdRow { id }) = existing {
            self.db
                .query("UPDATE $rid MERGE $data")
                .bind(("rid", id.clone()))
                .bind(("data", doc.clone()))
                .await?
                .check()?;
            Ok(id)
        } else {
            let mut response = self
                .db
                .query("CREATE document CONTENT $data RETURN id")
                .bind(("data", doc.clone()))
                .await?;
            let row: Option<IdRow> = response.take(0)?;
            row.map(|r| r.id).ok_or(Error::EmptyResult)
        }
    }

    async fn get_document(&self, id: &DocId) -> Result<Option<Document>> {
        let result: Option<Document> = self.db.select(id).await?;
        Ok(result)
    }

    async fn get_document_by_canonical(
        &self,
        canonical_id: &str,
    ) -> Result<Option<Document>> {
        let mut response = self
            .db
            .query("SELECT * FROM document WHERE canonical_id = $cid LIMIT 1")
            .bind(("cid", canonical_id.to_string()))
            .await?;
        Ok(response.take(0)?)
    }

    async fn delete_document(&self, id: &DocId) -> Result<()> {
        // SurrealDB has no ON DELETE CASCADE; cascade manually.
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

    async fn upsert_chunks(
        &self,
        doc_id: &DocId,
        chunks: &[Chunk],
    ) -> Result<Vec<ChunkId>> {
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

    // ---- source state ------------------------------------------------------

    async fn get_source_cursor(&self, adapter: &str) -> Result<Option<serde_json::Value>> {
        let mut response = self
            .db
            .query("SELECT cursor FROM source_state WHERE adapter = $name LIMIT 1")
            .bind(("name", adapter.to_string()))
            .await?;
        let row: Option<CursorRow> = response.take(0)?;
        Ok(row.map(|r| r.cursor))
    }

    async fn put_source_cursor(
        &self,
        adapter: &str,
        cursor: &serde_json::Value,
    ) -> Result<()> {
        let mut response = self
            .db
            .query("SELECT id FROM source_state WHERE adapter = $name LIMIT 1")
            .bind(("name", adapter.to_string()))
            .await?;
        let existing: Option<IdRow> = response.take(0)?;

        if let Some(IdRow { id }) = existing {
            self.db
                .query("UPDATE $rid MERGE { cursor: $cursor, updated_at: time::now() }")
                .bind(("rid", id))
                .bind(("cursor", cursor.clone()))
                .await?
                .check()?;
        } else {
            self.db
                .query("CREATE source_state CONTENT { adapter: $name, cursor: $cursor }")
                .bind(("name", adapter.to_string()))
                .bind(("cursor", cursor.clone()))
                .await?
                .check()?;
        }
        Ok(())
    }

    // ---- discovery feed ----------------------------------------------------

    async fn list_feed(
        &self,
        user_id: &RecordId,
        cursor: Option<FeedCursor>,
        limit: usize,
    ) -> Result<Vec<FeedItem>> {
        // Two-query merge instead of an in-DB join. The alternative — a
        // nested-SELECT subquery for the `read` flag — round-trips a row
        // through serde via FeedItem<#[flatten] Document>, which the
        // SurrealDB SDK refuses to deserialize when Document carries a
        // `serde_json::Value` (flatten + untagged enum collide).
        let where_cursor = if cursor.is_some() {
            "WHERE ingested_at < $cursor_ts \
               OR (ingested_at = $cursor_ts AND id < $cursor_id)"
        } else {
            ""
        };
        let sql = format!(
            "SELECT * FROM document \
             {where_cursor} \
             ORDER BY ingested_at DESC, id DESC \
             LIMIT $limit"
        );
        let mut q = self.db.query(sql).bind(("limit", limit as i64));
        if let Some(c) = cursor {
            q = q
                .bind(("cursor_ts", c.ingested_at))
                .bind(("cursor_id", c.id));
        }
        let mut response = q.await?;
        let docs: Vec<Document> = response.take(0)?;

        if docs.is_empty() {
            return Ok(Vec::new());
        }
        let doc_ids: Vec<RecordId> =
            docs.iter().filter_map(|d| d.id.clone()).collect();
        let mut response = self
            .db
            .query(
                "SELECT VALUE document FROM feed_read \
                 WHERE user = $user AND document IN $doc_ids",
            )
            .bind(("user", user_id.clone()))
            .bind(("doc_ids", doc_ids))
            .await?;
        let read_ids: Vec<RecordId> = response.take(0)?;
        let read_set: std::collections::HashSet<RecordId> = read_ids.into_iter().collect();

        Ok(docs
            .into_iter()
            .map(|d| {
                let read = d.id.as_ref().is_some_and(|id| read_set.contains(id));
                FeedItem { document: d, read }
            })
            .collect())
    }

    async fn mark_read(&self, user_id: &RecordId, doc_id: &DocId) -> Result<()> {
        // Idempotent: lookup-then-create. The unique index on (user,
        // document) is the safety net if a race slips through.
        let mut response = self
            .db
            .query(
                "SELECT id FROM feed_read \
                 WHERE user = $user AND document = $doc LIMIT 1",
            )
            .bind(("user", user_id.clone()))
            .bind(("doc", doc_id.clone()))
            .await?;
        let existing: Option<IdRow> = response.take(0)?;
        if existing.is_some() {
            return Ok(());
        }
        self.db
            .query("CREATE feed_read CONTENT { user: $user, document: $doc }")
            .bind(("user", user_id.clone()))
            .bind(("doc", doc_id.clone()))
            .await?
            .check()?;
        Ok(())
    }

    async fn mark_unread(&self, user_id: &RecordId, doc_id: &DocId) -> Result<()> {
        self.db
            .query("DELETE feed_read WHERE user = $user AND document = $doc")
            .bind(("user", user_id.clone()))
            .bind(("doc", doc_id.clone()))
            .await?
            .check()?;
        Ok(())
    }

    // ---- ops ---------------------------------------------------------------

    async fn counts(&self) -> Result<Counts> {
        let documents = count_table(&self.db, "document").await?;
        let document_content = count_table(&self.db, "document_content").await?;
        let chunks = count_table(&self.db, "chunk").await?;
        let document_versions = count_table(&self.db, "document_version").await?;
        Ok(Counts {
            documents,
            document_content,
            chunks,
            document_versions,
        })
    }

    async fn wipe(&self) -> Result<()> {
        for table in ["chunk", "document_content", "document_version", "document"] {
            self.db
                .query(format!("DELETE {table}"))
                .await?
                .check()?;
        }
        Ok(())
    }
}

async fn count_table(db: &Surreal<Any>, table: &str) -> Result<u64> {
    let mut response = db
        .query(format!("SELECT count() AS n FROM {table} GROUP ALL"))
        .await?;
    let row: Option<CountRow> = response.take(0)?;
    Ok(row.map(|r| r.n).unwrap_or(0))
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
