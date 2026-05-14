//! Per-document text extraction with positions.
//!
//! The output type is extractor-agnostic — a flat stream of [`Word`]s,
//! each carrying its page number and PDF-point bounding box. Downstream
//! modules (chunker, viewer overlay) consume this without knowing which
//! extractor produced it.
//!
//! v1 ships exactly one impl: [`PdftotextExtractor`], which shells out
//! to `pdftotext -bbox-layout` (already present on the backend container
//! for the arXiv adapter's body-text path). Swapping in GROBID / Marker /
//! Nougat / Mistral-OCR later is a new impl behind the same trait — the
//! chunker, the `chunk` row shape, and the viewer don't move.

mod pdftotext_bbox;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::Result;

pub use pdftotext_bbox::PdftotextExtractor;

/// One token from a document, with its position on the page.
///
/// Bounding box is in PDF point space (1/72 inch, origin **bottom-left**).
/// The frontend flips to CSS (top-left origin) at render time using the
/// page's height + rotation; storing the native coords keeps the math in
/// one place.
#[derive(Debug, Clone, PartialEq)]
pub struct Word {
    /// 1-indexed page number.
    pub page: i64,
    pub x: f64,
    pub y: f64,
    pub w: f64,
    pub h: f64,
    pub text: String,
}

#[async_trait]
pub trait TextExtractor: Send + Sync {
    /// Extract a flat reading-order stream of [`Word`]s from `bytes`.
    ///
    /// Empty output ⇒ the extractor could not get text (scanned PDF,
    /// encrypted, malformed). Callers should treat that as a soft
    /// failure and continue ingest with body=None — matching the arXiv
    /// adapter's posture.
    async fn extract(&self, bytes: Bytes) -> Result<Vec<Word>>;
}
