//! Metadata autofill seam.
//!
//! The completion pipeline (`pipeline.rs`) feeds the extracted document
//! text plus the user's prefill to a [`MetadataExtractor`], which returns
//! structured [`ExtractedMetadata`]. A clean trait seam ships now with a
//! [`NoopExtractor`] placeholder; the real LLM-backed `LlmExtractor`
//! (Phase 3) drops in behind the same trait without touching callers.
//!
//! Extractor output is **untrusted** — LLM (or any future extractor)
//! results are validated (`validate_descriptive_metadata`) before they
//! reach the merge, exactly like client input is. The merge policy is
//! prefill-wins (`merge_metadata`): the user's explicit values always
//! override autofill; autofill only fills the blanks.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::error::Result;

/// What the user supplied on the upload form (single-file prefill). All
/// fields optional; multi-file uploads pass an empty prefill.
#[derive(Debug, Default, Clone)]
pub struct DocumentPrefill {
    pub title: Option<String>,
    pub authors: Vec<String>,
    pub summary: Option<String>,
    pub language: Option<String>,
}

/// Structured metadata an extractor can produce. Maps onto top-level
/// `Document` fields plus the free-form `metadata` blob. `None` / empty
/// means "the extractor had nothing" — it never overwrites a set prefill
/// value.
#[derive(Debug, Default, Clone)]
pub struct ExtractedMetadata {
    pub title: Option<String>,
    pub authors: Vec<String>,
    pub summary: Option<String>,
    pub language: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    /// Shallow-merged into `document.metadata` (prefill keys win).
    pub extra: serde_json::Value,
}

/// Context handed to an extractor: the raw text from stage 5 plus the
/// user prefill (so the extractor knows what's already set).
pub struct ExtractionContext<'a> {
    pub text: &'a str,
    pub prefill: &'a DocumentPrefill,
}

#[async_trait]
pub trait MetadataExtractor: Send + Sync {
    async fn extract(&self, ctx: &ExtractionContext<'_>) -> Result<ExtractedMetadata>;
}

/// Placeholder shipped now. Returns empty — only the user prefill is used
/// until the LLM extractor lands (Phase 3).
pub struct NoopExtractor;

#[async_trait]
impl MetadataExtractor for NoopExtractor {
    async fn extract(&self, _ctx: &ExtractionContext<'_>) -> Result<ExtractedMetadata> {
        Ok(ExtractedMetadata::default())
    }
}

/// Result of merging prefill over autofill. The pipeline turns this into
/// the persisted `Document` fields.
#[derive(Debug, Default, Clone)]
pub struct MergedMetadata {
    pub title: Option<String>,
    pub authors: Vec<String>,
    pub summary: Option<String>,
    pub language: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    pub extra: serde_json::Value,
}

/// Merge policy: **prefill wins**. For each field, take the prefill value
/// if set, else the autofill value if set, else leave unset. `authors`
/// merges as "prefill if non-empty, else autofill". `extra` is a shallow
/// object merge with prefill keys winning; non-object `extra` from either
/// side is skipped (treated as "nothing to merge").
pub fn merge_metadata(prefill: &DocumentPrefill, autofill: &ExtractedMetadata) -> MergedMetadata {
    let pick = |p: &Option<String>, a: &Option<String>| p.clone().or_else(|| a.clone());

    let authors = if !prefill.authors.is_empty() {
        prefill.authors.clone()
    } else {
        autofill.authors.clone()
    };

    let extra = shallow_merge_objects(&autofill.extra, &prefill_extra());

    MergedMetadata {
        title: pick(&prefill.title, &autofill.title),
        authors,
        summary: pick(&prefill.summary, &autofill.summary),
        language: pick(&prefill.language, &autofill.language),
        // Prefill has no published_at field today; autofill may supply one.
        published_at: autofill.published_at,
        extra,
    }
}

/// The prefill carries no free-form `extra` today (the form only exposes
/// title/summary/authors/language). Kept as a seam so a future prefill
/// `extra` blob slots into the shallow merge with prefill-wins semantics.
fn prefill_extra() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

/// Shallow object merge: start from `base` (autofill), then overlay
/// `overlay` (prefill) keys, which win. If either side isn't an object,
/// fall back to the object side (or empty).
fn shallow_merge_objects(base: &serde_json::Value, overlay: &serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    let mut out = match base {
        Value::Object(m) => m.clone(),
        _ => serde_json::Map::new(),
    };
    if let Value::Object(m) = overlay {
        for (k, v) in m {
            out.insert(k.clone(), v.clone());
        }
    }
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn prefill_wins_over_autofill() {
        let prefill = DocumentPrefill {
            title: Some("User Title".into()),
            authors: vec!["Alice".into()],
            summary: None,
            language: Some("en".into()),
        };
        let autofill = ExtractedMetadata {
            title: Some("LLM Title".into()),
            authors: vec!["Bob".into(), "Carol".into()],
            summary: Some("LLM summary".into()),
            language: Some("fr".into()),
            published_at: None,
            extra: json!({ "venue": "ICML" }),
        };
        let merged = merge_metadata(&prefill, &autofill);
        // Prefill title wins.
        assert_eq!(merged.title.as_deref(), Some("User Title"));
        // Prefill authors (non-empty) win.
        assert_eq!(merged.authors, vec!["Alice".to_string()]);
        // Summary blank in prefill → autofill fills it.
        assert_eq!(merged.summary.as_deref(), Some("LLM summary"));
        // Prefill language wins.
        assert_eq!(merged.language.as_deref(), Some("en"));
        // extra from autofill survives (no prefill key collides).
        assert_eq!(merged.extra["venue"], json!("ICML"));
    }

    #[test]
    fn empty_prefill_takes_autofill() {
        let prefill = DocumentPrefill::default();
        let autofill = ExtractedMetadata {
            title: Some("LLM Title".into()),
            authors: vec!["Bob".into()],
            summary: Some("s".into()),
            language: Some("de".into()),
            published_at: None,
            extra: json!({}),
        };
        let merged = merge_metadata(&prefill, &autofill);
        assert_eq!(merged.title.as_deref(), Some("LLM Title"));
        assert_eq!(merged.authors, vec!["Bob".to_string()]);
        assert_eq!(merged.summary.as_deref(), Some("s"));
        assert_eq!(merged.language.as_deref(), Some("de"));
    }

    #[test]
    fn unset_both_stays_unset() {
        let merged = merge_metadata(&DocumentPrefill::default(), &ExtractedMetadata::default());
        assert!(merged.title.is_none());
        assert!(merged.summary.is_none());
        assert!(merged.language.is_none());
        assert!(merged.authors.is_empty());
        assert!(merged.published_at.is_none());
    }

    #[tokio::test]
    async fn noop_extractor_returns_empty() {
        let prefill = DocumentPrefill::default();
        let ctx = ExtractionContext {
            text: "some text",
            prefill: &prefill,
        };
        let got = NoopExtractor.extract(&ctx).await.unwrap();
        assert!(got.title.is_none());
        assert!(got.authors.is_empty());
    }
}
