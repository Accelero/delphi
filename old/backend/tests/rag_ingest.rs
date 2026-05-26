//! RAG ingest: drives `POST /api/ingestion/documents` with a fixture PDF
//! sitting in MemObjectStore and asserts the post-condition.
//!
//! - chunks land in the `chunk` table for the right tenant,
//! - `chunk.embedding` carries the fake-embedder's vector,
//! - `chunk.bboxes` is populated (or at least `Some([])` ≠ absent),
//! - `document.paper_embedding` is populated by the doc-embedder.
//!
//! Hermetic: real `pdftotext -bbox-layout` shells out (it's on the host
//! and in the backend container, same path), but no TEI sidecar — the
//! `FakeEmbedder` produces deterministic vectors.

#![allow(clippy::too_many_arguments)]

mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use serde_json::json;
use common::fake_embedder::FakeEmbedder;
use common::{AuthRequestBuilder, TestApp};

use delphi::embedder::Embedder;
use delphi::storage::{Chunk, Document, Storage};
use delphi::text_extractor::{PdftotextExtractor, TextExtractor};

/// Reads the fixture PDF (small born-digital "Delphi viewer e2e
/// fixture") so the extract → chunk → embed → upsert chain has
/// realistic input.
async fn fixture_pdf_bytes() -> Bytes {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("tests/fixtures/minimal.pdf");
    Bytes::from(tokio::fs::read(&path).await.expect("read fixture pdf"))
}

#[tokio::test]
async fn ingest_runs_extract_chunk_embed_upsert_pipeline() {
    let chunk_embedder: Arc<dyn Embedder> =
        Arc::new(FakeEmbedder::new("bge-small-en-v1.5", 384));
    let doc_embedder: Arc<dyn Embedder> = Arc::new(FakeEmbedder::new("specter2", 768));
    let extractor: Arc<dyn TextExtractor> = Arc::new(PdftotextExtractor::new());

    let app = TestApp::build_with_rag(
        Some(extractor),
        Some(chunk_embedder.clone()),
        Some(doc_embedder.clone()),
    )
    .await;

    // Drop the fixture PDF into the in-memory object store under a
    // known URL the ingest request points at.
    let bytes = fixture_pdf_bytes().await;
    let uri = app
        .object_store
        .put("rag/fixture.pdf", bytes)
        .await
        .expect("put fixture into MemObjectStore");

    let body = json!({
        "canonical_id": "rag-test-1",
        "source_type": "manual",
        "source_uri": "https://example.test/rag-test-1",
        "title": "Test Paper",
        "summary": "Test abstract.",
        "storage_uri": uri,
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/ingestion/documents")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let req = AuthRequestBuilder::default()
        .roles("owner")
        .apply(req);

    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.text());
    let outcome: serde_json::Value = res.json();
    assert_eq!(outcome["outcome"], "created");

    // Drive the storage layer directly (system path) to read back the
    // chunks + document. The IngestOutcome wire shape has `id` as a
    // structured RecordId; looking up by canonical_id is simpler.
    let storage = app.system.storage_for(app.default_tenant_id.clone());
    let doc: Document = storage
        .get_document_by_canonical("rag-test-1")
        .await
        .expect("get_document_by_canonical")
        .expect("doc exists");
    let doc_id = doc.id.clone().expect("doc.id");
    let chunks: Vec<Chunk> = storage.list_chunks(&doc_id).await.expect("list_chunks");
    assert!(!chunks.is_empty(), "expected at least one chunk written");
    for c in &chunks {
        assert!(!c.embedding.is_empty(), "chunk embedding empty");
        assert_eq!(c.embedding.len(), chunk_embedder.dim());
        assert_eq!(c.embedding_model, "bge-small-en-v1.5");
        // The fixture is one-page, so bboxes for each chunk should be
        // non-empty (at least one line rectangle).
        let bboxes = c.bboxes.as_ref().expect("bboxes populated");
        assert!(!bboxes.is_empty(), "bboxes for chunk #{} empty", c.ordinal);
        for b in bboxes {
            assert!(b.page >= 1);
            assert!(b.w >= 0.0);
        }
    }

    // Re-read the document to pick up the paper_embedding update that
    // the RAG sink writes after the inner sink's first upsert.
    let doc = storage
        .get_document(&doc_id)
        .await
        .expect("get_document")
        .expect("doc exists");
    assert_eq!(doc.title.as_deref(), Some("Test Paper"));
    let pe = doc.paper_embedding.as_ref().expect("paper_embedding populated");
    assert_eq!(pe.len(), doc_embedder.dim());
    assert_eq!(doc.paper_embedding_model.as_deref(), Some("specter2"));
}

#[tokio::test]
async fn ingest_without_pdf_still_writes_document_and_optional_paper_embedding() {
    // No `storage_uri` ⇒ no chunks. `paper_embedding` still lands
    // because `title + summary` are present.
    let doc_embedder: Arc<dyn Embedder> = Arc::new(FakeEmbedder::new("specter2", 768));
    let app =
        TestApp::build_with_rag(None, None, Some(doc_embedder.clone())).await;

    let body = json!({
        "canonical_id": "rag-no-pdf",
        "source_type": "manual",
        "source_uri": "https://example.test/rag-no-pdf",
        "title": "Metadata Only",
        "summary": "Just an abstract.",
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/ingestion/documents")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let req = AuthRequestBuilder::default()
        .roles("owner")
        .apply(req);
    let res = app.send(req).await;
    assert_eq!(res.status, StatusCode::OK, "{}", res.text());

    let storage = app.system.storage_for(app.default_tenant_id.clone());
    let doc = storage
        .get_document_by_canonical("rag-no-pdf")
        .await
        .unwrap()
        .unwrap();
    let doc_id = doc.id.clone().unwrap();
    let chunks = storage.list_chunks(&doc_id).await.expect("list_chunks");
    assert!(chunks.is_empty(), "no PDF → no chunks");
    let doc = storage.get_document(&doc_id).await.unwrap().unwrap();
    let pe = doc.paper_embedding.as_ref().expect("paper_embedding present");
    assert_eq!(pe.len(), 768);
}
