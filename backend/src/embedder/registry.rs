//! Hard-coded embedder registry.
//!
//! Two enabled-by-default models, each behind a master switch in env.
//! Adding a third entry is a 5-line PR: define the `prepare_*` trio,
//! pick a model name + dimensionality, add a `RegistryEntry` arm here.
//!
//! ## Tolerance on boot
//!
//! [`embedder_from_env`] returns an [`EmbedderLoad`] — chunk and document
//! embedders are each `Option<Arc<dyn Embedder>>`. The composition root
//! ignores a `None` (RAG features that need that model are simply
//! skipped). The intent is that `EMBEDDER_*_ENABLED=false` or an
//! unreachable TEI sidecar doesn't crash the backend boot — RAG is a
//! pillar, not a critical-path; reaching for retrieval without an
//! embedder is what fails, not startup.

use std::sync::Arc;

use thiserror::Error;

use crate::error::Result;

use super::tei::{bge_passage, bge_query, identity, specter2_document, TeiEmbedder};
use super::Embedder;

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("unknown embedding model: {0}")]
    UnknownModel(String),
}

/// Per-embedder config slot loaded from env. Public so tests / future
/// composition roots can construct it directly.
#[derive(Debug, Clone)]
pub struct EmbedderConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub model_name: String,
}

/// Both embeddings the pipeline might use. `None` means "not configured
/// / disabled / failed to construct" — ingest and retrieval treat it as
/// "skip that part of the pipeline" rather than as fatal.
pub struct EmbedderLoad {
    pub chunk: Option<Arc<dyn Embedder>>,
    pub document: Option<Arc<dyn Embedder>>,
}

/// Construct embedders from env. Per the design doc:
///
/// | Var | Default | Effect |
/// |---|---|---|
/// | `EMBEDDER_CHUNK_ENDPOINT`     | `http://tei-chunk:80`   | TEI URL for BGE-small. |
/// | `EMBEDDER_CHUNK_ENABLED`      | `true`                  | Master switch. |
/// | `EMBEDDER_CHUNK_MODEL_NAME`   | `bge-small-en-v1.5`     | Passed to TEI; also written to `chunk.embedding_model`. |
/// | `EMBEDDER_DOCUMENT_ENDPOINT`  | `http://tei-paper:80`   | TEI URL for SPECTER2. |
/// | `EMBEDDER_DOCUMENT_ENABLED`   | `true`                  | Master switch. |
/// | `EMBEDDER_DOCUMENT_MODEL_NAME`| `specter2`              | Passed to TEI; also written to `document.paper_embedding_model`. |
pub fn embedder_from_env() -> Result<EmbedderLoad> {
    let chunk_cfg = EmbedderConfig {
        enabled: env_bool("EMBEDDER_CHUNK_ENABLED", true),
        endpoint: std::env::var("EMBEDDER_CHUNK_ENDPOINT")
            .unwrap_or_else(|_| "http://tei-chunk:80".into()),
        model_name: std::env::var("EMBEDDER_CHUNK_MODEL_NAME")
            .unwrap_or_else(|_| "bge-small-en-v1.5".into()),
    };
    let doc_cfg = EmbedderConfig {
        enabled: env_bool("EMBEDDER_DOCUMENT_ENABLED", true),
        endpoint: std::env::var("EMBEDDER_DOCUMENT_ENDPOINT")
            .unwrap_or_else(|_| "http://tei-paper:80".into()),
        model_name: std::env::var("EMBEDDER_DOCUMENT_MODEL_NAME")
            .unwrap_or_else(|_| "specter2".into()),
    };
    let chunk: Option<Arc<dyn Embedder>> = if chunk_cfg.enabled {
        match build(&chunk_cfg) {
            Ok(e) => Some(e),
            Err(e) => {
                tracing::warn!(error = %e, "chunk embedder failed to construct; chunking disabled");
                None
            }
        }
    } else {
        None
    };
    let document: Option<Arc<dyn Embedder>> = if doc_cfg.enabled {
        match build(&doc_cfg) {
            Ok(e) => Some(e),
            Err(e) => {
                tracing::warn!(error = %e, "document embedder failed to construct; paper embedding disabled");
                None
            }
        }
    } else {
        None
    };
    Ok(EmbedderLoad { chunk, document })
}

fn build(cfg: &EmbedderConfig) -> Result<Arc<dyn Embedder>> {
    let m = lookup(&cfg.model_name).ok_or_else(|| {
        crate::error::Error::InvalidConfig(format!("unknown embedder model {:?}", cfg.model_name))
    })?;
    let embedder = TeiEmbedder::new(
        cfg.endpoint.clone(),
        cfg.model_name.clone(),
        m.dim,
        m.prepare_passage,
        m.prepare_query,
        m.prepare_document,
    )?;
    Ok(Arc::new(embedder))
}

struct ModelMeta {
    dim: usize,
    prepare_passage: super::tei::PreparePassage,
    prepare_query: super::tei::PrepareQuery,
    prepare_document: Option<super::tei::PrepareDocument>,
}

fn lookup(model: &str) -> Option<ModelMeta> {
    // Case-insensitive lookup; the public env name is the wire form
    // (also what gets written to `chunk.embedding_model`), but we
    // tolerate casing differences for ergonomics.
    match model.to_ascii_lowercase().as_str() {
        // BGE-small-en-v1.5 — 384-dim. Chunk-class.
        "bge-small-en-v1.5" | "bge-small" => Some(ModelMeta {
            dim: 384,
            prepare_passage: bge_passage,
            prepare_query: bge_query,
            prepare_document: None,
        }),
        // SPECTER2 — 768-dim. Document-class.
        "specter2" | "allenai/specter2_base" => Some(ModelMeta {
            dim: 768,
            // For paper-rank fall-back batches; document path uses the
            // dedicated `prepare_document` join.
            prepare_passage: identity,
            prepare_query: identity,
            prepare_document: Some(specter2_document),
        }),
        _ => None,
    }
}

fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(s) => matches!(s.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        Err(_) => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_models_resolve_with_expected_dims() {
        assert_eq!(lookup("bge-small-en-v1.5").unwrap().dim, 384);
        assert_eq!(lookup("BGE-Small").unwrap().dim, 384);
        assert_eq!(lookup("specter2").unwrap().dim, 768);
    }

    #[test]
    fn unknown_model_returns_none_not_panic() {
        assert!(lookup("nope").is_none());
    }

    #[test]
    fn env_bool_accepts_canonical_truthy_values() {
        std::env::set_var("TEST_EMBED_BOOL_X", "true");
        assert!(env_bool("TEST_EMBED_BOOL_X", false));
        std::env::set_var("TEST_EMBED_BOOL_X", "0");
        assert!(!env_bool("TEST_EMBED_BOOL_X", true));
        std::env::remove_var("TEST_EMBED_BOOL_X");
        assert!(env_bool("TEST_EMBED_BOOL_X", true));
    }
}
