use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use surrealdb::RecordId;

use crate::error::{Error, Result};
use crate::storage::{Content, DocId, Document, Storage};

// `ingested_at` is left as `None` so the SurrealDB schema's `DEFAULT
// time::now()` fires on CREATE; `UPDATE … MERGE` preserves the existing
// value, which is correct semantics ("when first seen").

/// Input to every ingestion path.
///
/// In-tree adapters produce this from `SourceAdapter::fetch`; external
/// callers POST it as JSON to `/api/ingestion/documents`. Same shape,
/// same downstream handling.
///
/// `tenant_id` is stamped by the caller — handlers from
/// `AuthContext.tenant_id`, scheduler from
/// `SOURCES_DEFAULT_TENANT_SLUG`. The pipeline writes it onto every
/// `Document` row and forwards it to `NotifyingSink` so SSE consumers
/// can filter their stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestRequest {
    pub tenant_id: RecordId,

    pub canonical_id: String,
    pub source_type: String,
    pub source_uri: String,

    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub authors: Vec<String>,
    #[serde(default)]
    pub published_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub language: Option<String>,

    /// Author/publisher-written short prose (paper abstract, book flap
    /// copy, article deck). Distinct from `raw_text` (the body).
    /// Adapters that only have a summary set this; the body stays
    /// `None`.
    #[serde(default)]
    pub summary: Option<String>,

    /// Optional full body text. When present it is persisted via
    /// `Storage::upsert_content` and drives the dedup hash. Empty for
    /// metadata-only adapters.
    #[serde(default)]
    pub raw_text: Option<String>,

    /// URL pointing at the original artefact (PDF, EPUB, …) the
    /// adapter has stashed in object storage. Pipeline copies it to
    /// `Document.storage_uri`; nothing else is done with it here.
    #[serde(default)]
    pub storage_uri: Option<String>,

    #[serde(default)]
    pub metadata: serde_json::Value,
}

/// Wire format: `{"outcome": "created", "id": "document:…", "version": 1}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum IngestOutcome {
    /// First time this `canonical_id` was seen.
    Created { id: DocId, version: i64 },
    /// Already on file with the same `content_hash` — no write performed.
    Unchanged { id: DocId, version: i64 },
    /// Already on file but `content_hash` changed; `version` was bumped.
    Versioned { id: DocId, version: i64 },
}

/// The single contract every ingest path calls.
#[async_trait]
pub trait IngestSink: Send + Sync {
    async fn ingest(&self, req: IngestRequest) -> Result<IngestOutcome>;
}

/// Reference [`IngestSink`] implementation. Owns dedup + version logic.
///
/// `Clone` because `AppState` is cloned per request; the inner storage
/// handle is itself an `Arc`, so this is cheap.
#[derive(Clone)]
pub struct Pipeline {
    storage: Arc<dyn Storage>,
}

impl Pipeline {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl IngestSink for Pipeline {
    async fn ingest(&self, req: IngestRequest) -> Result<IngestOutcome> {
        let content_hash = compute_content_hash(&req);

        let existing = self
            .storage
            .get_document_by_canonical(&req.tenant_id, &req.canonical_id)
            .await?;

        match existing {
            None => {
                let doc = build_document(&req, content_hash, 1);
                let id = self.storage.upsert_document(&req.tenant_id, &doc).await?;
                self.persist_text_if_present(&req.tenant_id, &id, &req).await?;
                Ok(IngestOutcome::Created { id, version: 1 })
            }
            Some(existing) if existing.content_hash == content_hash => {
                let id = existing.id.ok_or(Error::EmptyResult)?;
                Ok(IngestOutcome::Unchanged {
                    id,
                    version: existing.version,
                })
            }
            Some(existing) => {
                let new_version = existing.version + 1;
                let doc = build_document(&req, content_hash, new_version);
                let id = self.storage.upsert_document(&req.tenant_id, &doc).await?;
                self.persist_text_if_present(&req.tenant_id, &id, &req).await?;
                Ok(IngestOutcome::Versioned {
                    id,
                    version: new_version,
                })
            }
        }
    }
}

impl Pipeline {
    async fn persist_text_if_present(
        &self,
        tenant: &RecordId,
        id: &DocId,
        req: &IngestRequest,
    ) -> Result<()> {
        if let Some(text) = &req.raw_text {
            self.storage
                .upsert_content(
                    tenant,
                    id,
                    &Content {
                        text: text.clone(),
                        format: "text".into(),
                        extractor: "ingest".into(),
                    },
                )
                .await?;
        }
        Ok(())
    }
}

fn build_document(req: &IngestRequest, content_hash: String, version: i64) -> Document {
    Document {
        id: None,
        tenant_id: req.tenant_id.clone(),
        canonical_id: req.canonical_id.clone(),
        source_type: req.source_type.clone(),
        source_uri: req.source_uri.clone(),
        storage_uri: req.storage_uri.clone(),
        title: req.title.clone(),
        authors: req.authors.clone(),
        published_at: req.published_at,
        ingested_at: None,
        language: req.language.clone(),
        summary: req.summary.clone(),
        content_hash,
        version,
        metadata: ensure_object(req.metadata.clone()),
    }
}

/// Surreal's `metadata` column is `FLEXIBLE TYPE object` — `null`/scalar
/// values fail the type check on UPDATE MERGE. Coerce anything non-object
/// to an empty object so the pipeline tolerates sloppy callers.
fn ensure_object(v: serde_json::Value) -> serde_json::Value {
    if v.is_object() {
        v
    } else {
        serde_json::Value::Object(serde_json::Map::new())
    }
}

fn compute_content_hash(req: &IngestRequest) -> String {
    // Priority: body (raw_text) trumps summary trumps identity. That
    // way an abstract-only adapter still version-bumps when the
    // abstract changes, but a body change always wins once we have one.
    let mut hasher = Sha256::new();
    if let Some(text) = &req.raw_text {
        hasher.update(text.as_bytes());
    } else if let Some(summary) = &req.summary {
        hasher.update(summary.as_bytes());
    } else {
        // No body, no summary: hash a stable projection of identity so
        // re-fetches of the same metadata-only record dedup. NUL
        // separators avoid the "title=AB,uri=C" / "title=A,uri=BC"
        // collision.
        hasher.update(req.canonical_id.as_bytes());
        hasher.update(b"\0");
        if let Some(t) = &req.title {
            hasher.update(t.as_bytes());
        }
        hasher.update(b"\0");
        hasher.update(req.source_uri.as_bytes());
    }
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{RequestDbPool, SystemDb};

    async fn fresh_pipeline() -> (Pipeline, RecordId) {
        let system = SystemDb::in_memory("ingestion_test", "main")
            .await
            .expect("connect in-mem");
        system.init_schema().await.expect("init schema");
        // Seed a tenant for the test.
        let mut r = system
            .raw()
            .query("CREATE tenant CONTENT { slug: 'test', name: 'Test' } RETURN id")
            .await
            .unwrap();
        #[derive(serde::Deserialize)]
        struct IdRow {
            id: RecordId,
        }
        let row: Option<IdRow> = r.take(0).unwrap();
        let tenant = row.unwrap().id;

        let pool: Arc<dyn Storage> = Arc::new(RequestDbPool::from_system(&system));
        // `system` drops here. The Surreal<Any> clone inside the pool keeps
        // the underlying connection alive (Surreal<Any> is Arc-internally).
        let _ = system;
        (Pipeline::new(pool), tenant)
    }

    fn req(tenant: &RecordId, canonical_id: &str, raw_text: Option<&str>) -> IngestRequest {
        IngestRequest {
            tenant_id: tenant.clone(),
            canonical_id: canonical_id.into(),
            source_type: "test".into(),
            source_uri: format!("https://test.example/{canonical_id}"),
            title: Some("Test Title".into()),
            authors: vec!["A. Author".into()],
            published_at: None,
            language: Some("en".into()),
            summary: None,
            raw_text: raw_text.map(Into::into),
            storage_uri: None,
            metadata: serde_json::Value::Null,
        }
    }

    #[tokio::test]
    async fn first_ingest_creates() {
        let (p, t) = fresh_pipeline().await;
        let out = p.ingest(req(&t, "doc-1", Some("hello"))).await.unwrap();
        assert!(
            matches!(out, IngestOutcome::Created { version: 1, .. }),
            "expected Created, got {out:?}"
        );
    }

    #[tokio::test]
    async fn re_ingesting_identical_request_returns_unchanged() {
        let (p, t) = fresh_pipeline().await;
        let _ = p.ingest(req(&t, "doc-1", Some("hello"))).await.unwrap();
        let out = p.ingest(req(&t, "doc-1", Some("hello"))).await.unwrap();
        assert!(
            matches!(out, IngestOutcome::Unchanged { version: 1, .. }),
            "expected Unchanged, got {out:?}"
        );
    }

    #[tokio::test]
    async fn changed_text_bumps_version() {
        let (p, t) = fresh_pipeline().await;
        let _ = p.ingest(req(&t, "doc-1", Some("hello"))).await.unwrap();
        let out = p.ingest(req(&t, "doc-1", Some("hello, again"))).await.unwrap();
        assert!(
            matches!(out, IngestOutcome::Versioned { version: 2, .. }),
            "expected Versioned with v=2, got {out:?}"
        );
    }

    #[tokio::test]
    async fn metadata_only_dedup_works_when_no_text() {
        let (p, t) = fresh_pipeline().await;
        let _ = p.ingest(req(&t, "doc-1", None)).await.unwrap();
        let out = p.ingest(req(&t, "doc-1", None)).await.unwrap();
        assert!(
            matches!(out, IngestOutcome::Unchanged { .. }),
            "expected Unchanged, got {out:?}"
        );
    }
}
