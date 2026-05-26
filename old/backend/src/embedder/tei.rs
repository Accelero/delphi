//! HTTP client for Hugging Face's Text Embeddings Inference (TEI) server.
//!
//! TEI exposes `POST /embed` accepting a JSON body:
//!
//! ```json
//! { "inputs": ["text 1", "text 2", ...] }
//! ```
//!
//! and returning a 2D array of floats. The base URL maps to either the
//! `tei-chunk` or `tei-paper` sidecar via env.
//!
//! Per-model **input preparation** (passage / query prefixes, title +
//! [SEP] + abstract joining) is captured by [`PreparePassage`],
//! [`PrepareQuery`], and [`PrepareDocument`] strategies passed into the
//! [`TeiEmbedder`] at construction by the registry.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;

use crate::error::{Error, Result};

use super::Embedder;

/// Default whole-request timeout for the TEI HTTP call. Embedding a
/// batch of paragraph-sized chunks should finish well within this on
/// any reasonable hardware; long-tailing past the cap usually signals
/// the sidecar is wedged.
const DEFAULT_HTTP_TIMEOUT_SECS: u64 = 60;

pub type PreparePassage = fn(&str) -> String;
pub type PrepareQuery = fn(&str) -> String;
pub type PrepareDocument = fn(&str, &str) -> String;

/// TEI-backed embedder. Construct via the registry rather than directly
/// so the per-model prepare strategies stay coupled to their model
/// name — they're easy to get wrong silently otherwise.
pub struct TeiEmbedder {
    http: Client,
    base_url: String,
    model_name: String,
    dim: usize,
    prepare_passage: PreparePassage,
    prepare_query: PrepareQuery,
    prepare_document: Option<PrepareDocument>,
}

impl TeiEmbedder {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        base_url: impl Into<String>,
        model_name: impl Into<String>,
        dim: usize,
        prepare_passage: PreparePassage,
        prepare_query: PrepareQuery,
        prepare_document: Option<PrepareDocument>,
    ) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(DEFAULT_HTTP_TIMEOUT_SECS))
            .build()?;
        Ok(Self {
            http,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            model_name: model_name.into(),
            dim,
            prepare_passage,
            prepare_query,
            prepare_document,
        })
    }

    async fn embed_batch(&self, inputs: Vec<String>) -> Result<Vec<Vec<f32>>> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }
        let url = format!("{}/embed", self.base_url);
        let resp = self
            .http
            .post(&url)
            .json(&json!({ "inputs": inputs }))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Adapter {
                name: format!("tei:{}", self.model_name),
                message: format!("HTTP {status}: {body}"),
            });
        }
        // TEI's /embed returns a 2D array. Some build variants return
        // `{ "embeddings": [...] }`; accept either.
        let vecs = parse_tei_response(&resp.text().await?, &self.model_name)?;
        Ok(vecs)
    }
}

#[async_trait]
impl Embedder for TeiEmbedder {
    fn model_name(&self) -> &str {
        &self.model_name
    }
    fn dim(&self) -> usize {
        self.dim
    }
    async fn passages(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let prepared: Vec<String> = texts.iter().map(|t| (self.prepare_passage)(t)).collect();
        self.embed_batch(prepared).await
    }
    async fn query(&self, text: &str) -> Result<Vec<f32>> {
        let prepared = (self.prepare_query)(text);
        let mut out = self.embed_batch(vec![prepared]).await?;
        out.pop().ok_or(Error::EmptyResult)
    }
    async fn document(&self, title: &str, abstract_: &str) -> Result<Vec<f32>> {
        // Document-class models (SPECTER2) carry a model-specific join;
        // chunk-class models fall through to the default `title abstract`.
        let input = match self.prepare_document {
            Some(f) => f(title, abstract_),
            None => format!("{title} {abstract_}"),
        };
        let mut out = self.embed_batch(vec![input]).await?;
        out.pop().ok_or(Error::EmptyResult)
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TeiResponse {
    Bare(Vec<Vec<f32>>),
    Wrapped { embeddings: Vec<Vec<f32>> },
}

fn parse_tei_response(body: &str, model: &str) -> Result<Vec<Vec<f32>>> {
    let parsed: TeiResponse = serde_json::from_str(body).map_err(|e| Error::Adapter {
        name: format!("tei:{model}"),
        message: format!("parse response: {e}; body[0..200]={:?}", &body.chars().take(200).collect::<String>()),
    })?;
    Ok(match parsed {
        TeiResponse::Bare(v) => v,
        TeiResponse::Wrapped { embeddings } => embeddings,
    })
}

// ─── per-model prepare strategies ──────────────────────────────────────────

/// `prepare_passage` for BGE-small / -base / -large family: optional
/// `"passage: "` prefix. The actual training corpus is mixed; the
/// `passage:` prefix is well-documented and harmless when omitted, so
/// we apply it for safety and consistency.
pub(super) fn bge_passage(t: &str) -> String {
    format!("passage: {t}")
}

pub(super) fn bge_query(t: &str) -> String {
    format!("query: {t}")
}

/// SPECTER2 join: `title + [SEP] + abstract`. The training pipeline
/// passes both fields through this canonical separator; getting it wrong
/// degrades cosine quality measurably.
pub(super) fn specter2_document(title: &str, abstract_: &str) -> String {
    format!("{title}[SEP]{abstract_}")
}

/// Used when the registry needs a passage transform for a doc-class
/// model called via the bare `passages([single_string])` route (e.g.
/// pre-joined `title[SEP]abstract`). Identity.
pub(super) fn identity(t: &str) -> String {
    t.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bge_prefixes_are_what_the_recipe_says() {
        assert_eq!(bge_passage("hello"), "passage: hello");
        assert_eq!(bge_query("what is x?"), "query: what is x?");
    }

    #[test]
    fn specter2_joins_with_canonical_separator() {
        assert_eq!(specter2_document("T", "A"), "T[SEP]A");
    }

    #[test]
    fn parses_bare_2d_response() {
        let body = "[[0.1, 0.2], [0.3, 0.4]]";
        let v = parse_tei_response(body, "test").unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0], vec![0.1f32, 0.2]);
    }

    #[test]
    fn parses_wrapped_response() {
        let body = r#"{"embeddings": [[0.1, 0.2]]}"#;
        let v = parse_tei_response(body, "test").unwrap();
        assert_eq!(v, vec![vec![0.1f32, 0.2]]);
    }

    #[test]
    fn malformed_response_returns_adapter_error() {
        let err = parse_tei_response("not json", "m").unwrap_err();
        match err {
            crate::error::Error::Adapter { name, .. } => assert_eq!(name, "tei:m"),
            other => panic!("expected Adapter error, got {other:?}"),
        }
    }

    // wiremock-backed end-to-end test: spin up a local HTTP server that
    // asserts the body matches the per-model prepare transform, then
    // returns a synthetic embedding vector.
    #[tokio::test]
    async fn passes_passage_prefix_to_tei_for_bge() {
        use wiremock::matchers::{body_partial_json, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embed"))
            .and(body_partial_json(serde_json::json!({
                "inputs": ["passage: hello"]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(
                [[0.1f32, 0.2f32, 0.3f32]]
            )))
            .expect(1)
            .mount(&server)
            .await;
        let e = TeiEmbedder::new(
            server.uri(),
            "bge-small-en-v1.5",
            3,
            bge_passage,
            bge_query,
            None,
        )
        .unwrap();
        let out = e.passages(&["hello".into()]).await.unwrap();
        assert_eq!(out, vec![vec![0.1f32, 0.2, 0.3]]);
    }

    #[tokio::test]
    async fn passes_query_prefix_to_tei_for_bge() {
        use wiremock::matchers::{body_partial_json, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embed"))
            .and(body_partial_json(serde_json::json!({
                "inputs": ["query: q?"]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(
                [[1.0f32, 2.0f32]]
            )))
            .expect(1)
            .mount(&server)
            .await;
        let e = TeiEmbedder::new(
            server.uri(),
            "bge-small-en-v1.5",
            2,
            bge_passage,
            bge_query,
            None,
        )
        .unwrap();
        let out = e.query("q?").await.unwrap();
        assert_eq!(out, vec![1.0f32, 2.0]);
    }

    #[tokio::test]
    async fn passes_title_sep_abstract_to_tei_for_specter2() {
        use wiremock::matchers::{body_partial_json, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embed"))
            .and(body_partial_json(serde_json::json!({
                "inputs": ["My Paper[SEP]An abstract."]
            })))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!([[0.5f32, 0.5f32, 0.5f32]])),
            )
            .expect(1)
            .mount(&server)
            .await;
        let e = TeiEmbedder::new(
            server.uri(),
            "specter2",
            3,
            identity,
            identity,
            Some(specter2_document),
        )
        .unwrap();
        let v = e.document("My Paper", "An abstract.").await.unwrap();
        assert_eq!(v.len(), 3);
    }
}
