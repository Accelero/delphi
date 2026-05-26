//! [`RagSink`]: an [`IngestSink`] middleware that runs the RAG pipeline
//! (extract → chunk → embed → upsert) after the inner sink persists the
//! document.
//!
//! Composed at request build time on top of [`super::Pipeline`] (and,
//! when desired, wrapped further by [`super::NotifyingSink`]). The
//! ordering is: inner upserts the Document → RagSink reads its
//! `storage_uri` through the shared `ObjectStore`, extracts +
//! chunks + embeds + upserts chunks, and (if a document embedder is
//! configured) embeds `title + [SEP] + abstract` and writes it back
//! onto the document row.
//!
//! Failures here are non-fatal — every step is logged and skipped if
//! the inputs aren't available (no PDF, embedder unreachable, etc.).
//! The arXiv adapter does the same with body extraction; RAG follows
//! that posture so ingest stays robust to transient sidecar issues.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::chunker::{chunk_words, ChunkConfig};
use crate::embedder::Embedder;
use crate::error::Result;
use crate::object_store::ObjectStore;
use crate::storage::{Chunk, DocId, Document, Storage};
use crate::text_extractor::{TextExtractor, Word};

use super::{IngestOutcome, IngestRequest, IngestSink};

/// Decorator that performs chunking + embedding after the inner sink's
/// Document upsert succeeds.
pub struct RagSink {
    inner: Arc<dyn IngestSink>,
    storage: Arc<dyn Storage>,
    object_store: Arc<dyn ObjectStore>,
    extractor: Arc<dyn TextExtractor>,
    chunk_embedder: Option<Arc<dyn Embedder>>,
    document_embedder: Option<Arc<dyn Embedder>>,
    chunk_cfg: ChunkConfig,
}

impl RagSink {
    pub fn new(
        inner: Arc<dyn IngestSink>,
        storage: Arc<dyn Storage>,
        object_store: Arc<dyn ObjectStore>,
        extractor: Arc<dyn TextExtractor>,
        chunk_embedder: Option<Arc<dyn Embedder>>,
        document_embedder: Option<Arc<dyn Embedder>>,
        chunk_cfg: ChunkConfig,
    ) -> Self {
        Self {
            inner,
            storage,
            object_store,
            extractor,
            chunk_embedder,
            document_embedder,
            chunk_cfg,
        }
    }

    /// Per-pillar enablement: only run the chunk pipeline on
    /// first-creation / version-bump (`Created` / `Versioned`).
    /// `Unchanged` means the content_hash matches the last run — chunks
    /// already exist and rerunning them would be wasted compute.
    fn should_process(outcome: &IngestOutcome) -> Option<DocId> {
        match outcome {
            IngestOutcome::Created { id, .. } => Some(id.clone()),
            IngestOutcome::Versioned { id, .. } => Some(id.clone()),
            IngestOutcome::Unchanged { .. } => None,
        }
    }
}

#[async_trait]
impl IngestSink for RagSink {
    async fn ingest(&self, req: IngestRequest) -> Result<IngestOutcome> {
        // Snapshot the inputs we'll need *after* the inner sink runs
        // (the request value is consumed by `inner.ingest`).
        let storage_uri = req.storage_uri.clone();
        let title = req.title.clone();
        let summary = req.summary.clone();
        let canonical_id = req.canonical_id.clone();

        let outcome = self.inner.ingest(req).await?;
        let Some(doc_id) = Self::should_process(&outcome) else {
            return Ok(outcome);
        };

        // Chunk + embed body (gated on having a stored PDF + a chunk
        // embedder). Any failure here is warn-and-continue.
        if let Some(ref uri) = storage_uri {
            if let Some(emb) = &self.chunk_embedder {
                if let Err(e) = self.run_chunk_pipeline(&doc_id, uri, emb).await {
                    tracing::warn!(error = %e, %canonical_id, "rag: chunk pipeline failed; continuing");
                }
            }
        }

        // Embed title + abstract into `paper_embedding`. We update the
        // document row in-place rather than going through `upsert_document`
        // (which would re-hash content and version-bump). The dedicated
        // helper performs a MERGE on just the two embedding fields.
        if let Some(emb) = &self.document_embedder {
            if let (Some(t), Some(a)) = (title.as_ref(), summary.as_ref()) {
                if let Err(e) = self
                    .run_paper_embedding(&doc_id, t, a, emb.as_ref())
                    .await
                {
                    tracing::warn!(error = %e, %canonical_id, "rag: paper embedding failed; continuing");
                }
            }
        }

        Ok(outcome)
    }
}

impl RagSink {
    async fn run_chunk_pipeline(
        &self,
        doc_id: &DocId,
        storage_uri: &str,
        embedder: &Arc<dyn Embedder>,
    ) -> Result<()> {
        let bytes: Bytes = self.object_store.get_by_url(storage_uri).await?;
        let words: Vec<Word> = self.extractor.extract(bytes).await?;
        if words.is_empty() {
            tracing::info!(?doc_id, "rag: extractor returned no words; skipping chunks");
            return Ok(());
        }
        let chunks = chunk_words(&words, self.chunk_cfg);
        if chunks.is_empty() {
            return Ok(());
        }
        let texts: Vec<String> = chunks.iter().map(|c| c.text.clone()).collect();
        let vectors = embedder.passages(&texts).await?;
        if vectors.len() != chunks.len() {
            tracing::warn!(
                got = vectors.len(),
                want = chunks.len(),
                "rag: TEI returned wrong number of vectors; skipping upsert"
            );
            return Ok(());
        }
        let model_name = embedder.model_name().to_string();
        let strategy = self.chunk_cfg.strategy.to_string();
        let rows: Vec<Chunk> = chunks
            .into_iter()
            .zip(vectors.into_iter())
            .map(|(c, v)| Chunk {
                id: None,
                doc: None,
                ordinal: c.ordinal,
                char_start: c.char_start,
                char_end: c.char_end,
                bboxes: if c.bboxes.is_empty() {
                    None
                } else {
                    Some(c.bboxes)
                },
                text: c.text,
                embedding: v,
                embedding_model: model_name.clone(),
                chunk_strategy: strategy.clone(),
            })
            .collect();
        self.storage.upsert_chunks(doc_id, &rows).await?;
        Ok(())
    }

    async fn run_paper_embedding(
        &self,
        doc_id: &DocId,
        title: &str,
        abstract_: &str,
        embedder: &dyn Embedder,
    ) -> Result<()> {
        let vector = embedder.document(title, abstract_).await?;
        // Read existing row → mutate two fields → upsert. This uses the
        // already-existing `upsert_document` path which keys on canonical_id;
        // the Document is otherwise unchanged so content_hash + version
        // don't move.
        let Some(mut doc) = self.storage.get_document(doc_id).await? else {
            return Ok(());
        };
        doc.paper_embedding = Some(vector);
        doc.paper_embedding_model = Some(embedder.model_name().to_string());
        // Make sure id round-trips so upsert_document hits the existing row.
        if doc.id.is_none() {
            doc.id = Some(doc_id.clone());
        }
        let _ = self.upsert_paper_embedding(&doc).await?;
        Ok(())
    }

    /// Light-touch wrapper around `upsert_document` so callers don't have
    /// to know about the dedup-by-canonical-id behaviour. The existing
    /// trait method MERGEs payload fields onto the existing row when
    /// `canonical_id` matches, which is exactly what we want here.
    async fn upsert_paper_embedding(&self, doc: &Document) -> Result<DocId> {
        self.storage.upsert_document(doc).await
    }
}
