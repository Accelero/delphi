//! Deterministic `Embedder` for hermetic RAG integration tests.
//!
//! Maps a text → a fixed-dim, deterministic vector built from a SHA-256
//! of the text. Same input always yields the same vector, regardless of
//! `passages` vs `query` vs `document` route. That's enough for the
//! integration tests that just want to verify "embeddings land in the
//! row" — not that retrieval actually surfaces semantically similar
//! chunks.

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use delphi::embedder::Embedder;
use delphi::error::Result;

pub struct FakeEmbedder {
    pub model: String,
    pub dim: usize,
}

impl FakeEmbedder {
    pub fn new(model: impl Into<String>, dim: usize) -> Self {
        Self {
            model: model.into(),
            dim,
        }
    }
}

fn deterministic_vector(text: &str, dim: usize) -> Vec<f32> {
    // Tile a SHA-256 digest into `dim` floats in [-1, 1]. Cheap, stable
    // across runs, and yields distinct vectors for distinct inputs.
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    let seed = hasher.finalize();
    let mut out = Vec::with_capacity(dim);
    for i in 0..dim {
        let byte = seed[i % seed.len()];
        // Map 0..=255 to roughly -1..=1.
        let f = (byte as f32 - 127.5) / 127.5;
        out.push(f);
    }
    out
}

#[async_trait]
impl Embedder for FakeEmbedder {
    fn model_name(&self) -> &str {
        &self.model
    }
    fn dim(&self) -> usize {
        self.dim
    }
    async fn passages(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        Ok(texts.iter().map(|t| deterministic_vector(t, self.dim)).collect())
    }
    async fn query(&self, text: &str) -> Result<Vec<f32>> {
        Ok(deterministic_vector(text, self.dim))
    }
    async fn document(&self, title: &str, abstract_: &str) -> Result<Vec<f32>> {
        Ok(deterministic_vector(&format!("{title}[SEP]{abstract_}"), self.dim))
    }
}
