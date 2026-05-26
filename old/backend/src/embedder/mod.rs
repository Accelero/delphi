//! Embedding-model abstraction.
//!
//! v1 ships two enabled-by-default models, each served by a TEI sidecar:
//!
//! - **BGE-small-en-v1.5** (chunk-level, 384-dim). Optional `passage:` /
//!   `query:` prefixes per the model's training recipe.
//! - **SPECTER2** (document-level, 768-dim). Fed `title + [SEP] +
//!   abstract` joined on the model-specific separator.
//!
//! The trait is asymmetric on purpose: callers don't care that BGE wants
//! prefixes and SPECTER2 wants joining — they call `passages` /
//! `document` / `query` and the impl performs the right preparation.
//!
//! Registry-driven, not pure-config: the `prepare_*` transforms can't be
//! expressed as configuration without giving up safety (a config typo
//! that quietly degrades retrieval 5–15% is the failure mode we want to
//! make impossible).

mod registry;
mod tei;

use async_trait::async_trait;

use crate::error::Result;

pub use registry::{embedder_from_env, EmbedderConfig, EmbedderLoad, RegistryError};
pub use tei::TeiEmbedder;

#[async_trait]
pub trait Embedder: Send + Sync {
    /// Stable name written to `chunk.embedding_model` /
    /// `document.paper_embedding_model`. Stored alongside the vector so
    /// future model swaps don't have to re-embed in place.
    fn model_name(&self) -> &str;

    /// Output dimensionality. Read by the schema/migration code, not the
    /// hot path; included so the wrong dim doesn't reach SurrealDB's
    /// HNSW index (which silently rejects mismatched vectors).
    fn dim(&self) -> usize;

    /// Embed a batch of corpus passages — applies the model's
    /// passage-side transform (e.g. `passage:` prefix for BGE).
    async fn passages(&self, texts: &[String]) -> Result<Vec<Vec<f32>>>;

    /// Embed a single document-level input — applies the model's
    /// document-side transform (e.g. `title + [SEP] + abstract` for
    /// SPECTER2). The default impl is just `passages` with a one-element
    /// batch; document-class models override.
    async fn document(&self, title: &str, abstract_: &str) -> Result<Vec<f32>> {
        let joined = format!("{title} {abstract_}");
        let v = self.passages(&[joined]).await?;
        v.into_iter()
            .next()
            .ok_or(crate::error::Error::EmptyResult)
    }

    /// Embed a single query string — applies the model's query-side
    /// transform (e.g. `query:` prefix for BGE).
    async fn query(&self, text: &str) -> Result<Vec<f32>>;
}
