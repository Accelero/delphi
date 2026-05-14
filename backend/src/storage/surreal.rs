//! SurrealDB implementation of the [`Storage`] trait.
//!
//! Wire-shape concern: SurrealDB rejects raw RFC3339 strings on `TYPE
//! datetime` columns, so the public `Document` model uses
//! `chrono::DateTime<Utc>` and a private `DocumentWire` struct converts
//! to/from `surrealdb::Datetime` at the (de)serialize boundary. Closes
//! audit finding M4 — SurrealDB types no longer leak across the storage
//! interface.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::engine::any::Any;
use surrealdb::{Datetime, RecordId, Surreal};

use crate::error::{Error, Result};
use crate::storage::{
    Chunk, ChunkId, ChunkSearchResult, Content, DocId, Document, FeedCursor, Filters, Storage,
};

/// Storage trait implementation against a SurrealDB connection.
///
/// Constructed by [`super::RequestDbPool::from_system`] (Phase 1 shares
/// the system connection; Phase 2 will hand each request its own
/// authenticated connection).
pub struct SurrealStorage {
    db: Surreal<Any>,
}

impl SurrealStorage {
    /// Wrap an existing connection. The connection must already be
    /// signed in and have `use_ns` / `use_db` configured.
    pub fn from_handle(db: Surreal<Any>) -> Self {
        Self { db }
    }
}

// ─── wire structs ─────────────────────────────────────────────────────────
//
// These exist because SurrealDB's serializer expects its own native
// `Datetime` for `TYPE datetime` columns, while the public `Document`
// model uses `chrono::DateTime<Utc>`. Conversion happens here, so the
// rest of the crate sees only chrono.

#[derive(Debug, Serialize, Deserialize)]
struct DocumentWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<RecordId>,
    tenant_id: RecordId,
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
            tenant_id: d.tenant_id.clone(),
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
struct IdRow {
    id: RecordId,
}

#[derive(Debug, Deserialize)]
struct CursorRow {
    cursor: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ContentData {
    tenant_id: RecordId,
    doc: RecordId,
    format: String,
    text: String,
    extractor: String,
}

#[derive(Debug, Serialize)]
struct ChunkData {
    tenant_id: RecordId,
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

    async fn upsert_document(&self, tenant: &RecordId, doc: &Document) -> Result<DocId> {
        let mut response = self
            .db
            .query(
                "SELECT id FROM document \
                 WHERE tenant_id = $t AND canonical_id = $cid LIMIT 1",
            )
            .bind(("t", tenant.clone()))
            .bind(("cid", doc.canonical_id.clone()))
            .await?;
        let existing: Option<IdRow> = response.take(0)?;

        // Always stamp the tenant_id from the trusted argument, even if
        // the caller's `Document.tenant_id` differs. Defence-in-depth:
        // a buggy caller can't smuggle a foreign tenant via the document
        // payload.
        let mut wire = DocumentWire::from(doc);
        wire.tenant_id = tenant.clone();

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

    async fn get_document(&self, tenant: &RecordId, id: &DocId) -> Result<Option<Document>> {
        let mut response = self
            .db
            .query("SELECT * FROM $rid WHERE tenant_id = $t LIMIT 1")
            .bind(("rid", id.clone()))
            .bind(("t", tenant.clone()))
            .await?;
        let row: Option<DocumentWire> = response.take(0)?;
        Ok(row.map(Document::from))
    }

    async fn get_document_by_canonical(
        &self,
        tenant: &RecordId,
        canonical_id: &str,
    ) -> Result<Option<Document>> {
        let mut response = self
            .db
            .query(
                "SELECT * FROM document \
                 WHERE tenant_id = $t AND canonical_id = $cid LIMIT 1",
            )
            .bind(("t", tenant.clone()))
            .bind(("cid", canonical_id.to_string()))
            .await?;
        let row: Option<DocumentWire> = response.take(0)?;
        Ok(row.map(Document::from))
    }

    async fn delete_document(&self, tenant: &RecordId, id: &DocId) -> Result<()> {
        // SurrealDB has no ON DELETE CASCADE; cascade manually. Every
        // child query also filters by tenant — defence-in-depth in case
        // `id` is somehow cross-tenant.
        self.db
            .query("DELETE document_content WHERE doc = $rid AND tenant_id = $t")
            .bind(("rid", id.clone()))
            .bind(("t", tenant.clone()))
            .await?
            .check()?;
        self.db
            .query("DELETE chunk WHERE doc = $rid AND tenant_id = $t")
            .bind(("rid", id.clone()))
            .bind(("t", tenant.clone()))
            .await?
            .check()?;
        self.db
            .query("DELETE document_version WHERE doc = $rid AND tenant_id = $t")
            .bind(("rid", id.clone()))
            .bind(("t", tenant.clone()))
            .await?
            .check()?;
        self.db
            .query("DELETE $rid WHERE tenant_id = $t")
            .bind(("rid", id.clone()))
            .bind(("t", tenant.clone()))
            .await?
            .check()?;
        Ok(())
    }

    // ---- content -----------------------------------------------------------

    async fn upsert_content(
        &self,
        tenant: &RecordId,
        doc_id: &DocId,
        content: &Content,
    ) -> Result<()> {
        let mut response = self
            .db
            .query(
                "SELECT id FROM document_content \
                 WHERE doc = $rid AND tenant_id = $t LIMIT 1",
            )
            .bind(("rid", doc_id.clone()))
            .bind(("t", tenant.clone()))
            .await?;
        let existing: Option<IdRow> = response.take(0)?;

        let data = ContentData {
            tenant_id: tenant.clone(),
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

    async fn get_content(&self, tenant: &RecordId, doc_id: &DocId) -> Result<Option<Content>> {
        let mut response = self
            .db
            .query(
                "SELECT format, text, extractor FROM document_content \
                 WHERE doc = $rid AND tenant_id = $t LIMIT 1",
            )
            .bind(("rid", doc_id.clone()))
            .bind(("t", tenant.clone()))
            .await?;
        Ok(response.take(0)?)
    }

    // ---- chunks ------------------------------------------------------------

    async fn upsert_chunks(
        &self,
        tenant: &RecordId,
        doc_id: &DocId,
        chunks: &[Chunk],
    ) -> Result<Vec<ChunkId>> {
        let mut ids = Vec::with_capacity(chunks.len());
        for c in chunks {
            let data = ChunkData {
                tenant_id: tenant.clone(),
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
                       AND tenant_id = $t \
                       AND ordinal = $ord \
                       AND embedding_model = $model \
                       AND chunk_strategy = $strategy \
                     LIMIT 1",
                )
                .bind(("rid", doc_id.clone()))
                .bind(("t", tenant.clone()))
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

    async fn list_chunks(&self, tenant: &RecordId, doc_id: &DocId) -> Result<Vec<Chunk>> {
        let mut response = self
            .db
            .query(
                "SELECT * FROM chunk \
                 WHERE doc = $rid AND tenant_id = $t \
                 ORDER BY ordinal ASC",
            )
            .bind(("rid", doc_id.clone()))
            .bind(("t", tenant.clone()))
            .await?;
        Ok(response.take(0)?)
    }

    async fn delete_chunks(&self, tenant: &RecordId, doc_id: &DocId) -> Result<()> {
        self.db
            .query("DELETE chunk WHERE doc = $rid AND tenant_id = $t")
            .bind(("rid", doc_id.clone()))
            .bind(("t", tenant.clone()))
            .await?
            .check()?;
        Ok(())
    }

    // ---- search ------------------------------------------------------------

    async fn search_vector(
        &self,
        tenant: &RecordId,
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
             WHERE tenant_id = $t AND embedding <|$k|> $q {where_clause} \
             ORDER BY score ASC \
             LIMIT $k"
        );

        let mut q = self
            .db
            .query(sql)
            .bind(("t", tenant.clone()))
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
        tenant: &RecordId,
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
             WHERE tenant_id = $t AND text @0@ $q {where_clause} \
             ORDER BY score DESC \
             LIMIT $k"
        );

        let mut q = self
            .db
            .query(sql)
            .bind(("t", tenant.clone()))
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

    async fn get_source_cursor(
        &self,
        tenant: &RecordId,
        adapter: &str,
    ) -> Result<Option<serde_json::Value>> {
        let mut response = self
            .db
            .query(
                "SELECT cursor FROM source_state \
                 WHERE tenant_id = $t AND adapter = $name LIMIT 1",
            )
            .bind(("t", tenant.clone()))
            .bind(("name", adapter.to_string()))
            .await?;
        let row: Option<CursorRow> = response.take(0)?;
        Ok(row.map(|r| r.cursor))
    }

    async fn put_source_cursor(
        &self,
        tenant: &RecordId,
        adapter: &str,
        cursor: &serde_json::Value,
    ) -> Result<()> {
        let mut response = self
            .db
            .query(
                "SELECT id FROM source_state \
                 WHERE tenant_id = $t AND adapter = $name LIMIT 1",
            )
            .bind(("t", tenant.clone()))
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
                .query(
                    "CREATE source_state CONTENT \
                     { tenant_id: $t, adapter: $name, cursor: $cursor }",
                )
                .bind(("t", tenant.clone()))
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
        tenant: &RecordId,
        cursor: Option<FeedCursor>,
        limit: usize,
    ) -> Result<Vec<Document>> {
        let where_cursor = if cursor.is_some() {
            "AND (ingested_at < $cursor_ts \
                  OR (ingested_at = $cursor_ts AND id < $cursor_id))"
        } else {
            ""
        };
        let sql = format!(
            "SELECT * FROM document \
             WHERE tenant_id = $t {where_cursor} \
             ORDER BY ingested_at DESC, id DESC \
             LIMIT $limit"
        );
        let mut q = self
            .db
            .query(sql)
            .bind(("t", tenant.clone()))
            .bind(("limit", limit as i64));
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

#[cfg(test)]
mod feed_query_tests {
    use super::*;
    use crate::storage::SystemDb;
    use chrono::{Duration, Utc};

    async fn fresh() -> (SystemDb, SurrealStorage, RecordId) {
        let system = SystemDb::in_memory("feed_query_test", "main").await.unwrap();
        system.init_schema().await.unwrap();
        let storage = SurrealStorage::from_handle(system.raw().clone());
        let tenant = create_tenant(&system, "test").await;
        (system, storage, tenant)
    }

    async fn create_tenant(system: &SystemDb, slug: &str) -> RecordId {
        let mut r = system
            .raw()
            .query("CREATE tenant CONTENT { slug: $slug, name: 'Test' } RETURN id")
            .bind(("slug", slug.to_string()))
            .await
            .unwrap();
        let row: Option<IdRow> = r.take(0).unwrap();
        row.unwrap().id
    }

    fn doc(tenant: &RecordId, canonical_id: &str) -> Document {
        Document {
            id: None,
            tenant_id: tenant.clone(),
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

    #[tokio::test]
    async fn list_feed_returns_empty_when_no_documents() {
        let (_system, storage, tenant) = fresh().await;
        let items = storage.list_feed(&tenant, None, 50).await.unwrap();
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn list_feed_orders_newest_first() {
        let (system, storage, tenant) = fresh().await;

        for (i, c) in ["a", "b", "c"].iter().enumerate() {
            let id = storage.upsert_document(&tenant, &doc(&tenant, c)).await.unwrap();
            let ts = (Utc::now() - Duration::seconds(100 - i as i64 * 10)).to_rfc3339();
            system
                .raw()
                .query("UPDATE $rid SET ingested_at = <datetime>$ts")
                .bind(("rid", id))
                .bind(("ts", ts))
                .await
                .unwrap()
                .check()
                .unwrap();
        }

        let items = storage.list_feed(&tenant, None, 50).await.unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].canonical_id, "c");
        assert_eq!(items[1].canonical_id, "b");
        assert_eq!(items[2].canonical_id, "a");
    }

    #[tokio::test]
    async fn list_feed_paginates_with_cursor() {
        let (system, storage, tenant) = fresh().await;

        for i in 0..5 {
            let id = storage
                .upsert_document(&tenant, &doc(&tenant, &format!("d{i}")))
                .await
                .unwrap();
            let ts = (Utc::now() - Duration::seconds(100 - i * 10)).to_rfc3339();
            system
                .raw()
                .query("UPDATE $rid SET ingested_at = <datetime>$ts")
                .bind(("rid", id))
                .bind(("ts", ts))
                .await
                .unwrap()
                .check()
                .unwrap();
        }

        let page1 = storage.list_feed(&tenant, None, 2).await.unwrap();
        assert_eq!(page1.len(), 2);
        let last = page1.last().unwrap();
        let cursor = FeedCursor {
            ingested_at: last.ingested_at.unwrap(),
            id: last.id.clone().unwrap(),
        };
        let page2 = storage
            .list_feed(&tenant, Some(cursor), 2)
            .await
            .unwrap();
        assert_eq!(page2.len(), 2);

        let p1: Vec<_> = page1.iter().filter_map(|d| d.id.clone()).collect();
        for item in &page2 {
            let id = item.id.clone().unwrap();
            assert!(!p1.contains(&id), "page2 should not overlap page1");
        }
    }

    #[tokio::test]
    async fn list_feed_isolates_per_tenant() {
        let (system, storage, tenant_a) = fresh().await;
        let tenant_b = create_tenant(&system, "tenant-b").await;

        // Same canonical_id in two tenants → two distinct rows (per-tenant
        // canonical UNIQUE index).
        storage
            .upsert_document(&tenant_a, &doc(&tenant_a, "shared"))
            .await
            .unwrap();
        storage
            .upsert_document(&tenant_b, &doc(&tenant_b, "shared"))
            .await
            .unwrap();

        let a_view = storage.list_feed(&tenant_a, None, 50).await.unwrap();
        assert_eq!(a_view.len(), 1, "tenant A sees only its own doc");

        let b_view = storage.list_feed(&tenant_b, None, 50).await.unwrap();
        assert_eq!(b_view.len(), 1, "tenant B sees only its own doc");
    }
}
