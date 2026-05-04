//! SurrealDB implementation of the [`Storage`] trait.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use surrealdb::engine::remote::ws::{Client, Ws};
use surrealdb::opt::auth::Root;
use surrealdb::RecordId;
use surrealdb::Surreal;

use crate::error::{Error, Result};
use crate::storage::{
    Chunk, ChunkId, ChunkSearchResult, Content, Counts, DocId, Document, Filters, Storage,
};

const SCHEMA_SURQL: &str = include_str!("../../schema.surql");

pub struct SurrealStorage {
    db: Surreal<Client>,
}

impl SurrealStorage {
    /// Connect to a remote SurrealDB over WebSocket.
    /// `endpoint` is `host:port` (no scheme, no `/rpc` suffix).
    pub async fn connect(
        endpoint: &str,
        user: &str,
        password: &str,
        namespace: &str,
        database: &str,
    ) -> Result<Self> {
        let db = Surreal::new::<Ws>(endpoint).await?;
        db.signin(Root {
            username: user,
            password,
        })
        .await?;
        db.use_ns(namespace).use_db(database).await?;
        Ok(Self { db })
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

async fn count_table(db: &Surreal<Client>, table: &str) -> Result<u64> {
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
