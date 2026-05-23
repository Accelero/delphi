//! LLM-backed [`MetadataExtractor`] — the real autofill (roadmap §1).
//!
//! Reads the head of the extracted document text and asks an [`LlmClient`]
//! for a single JSON object of bibliographic metadata, which is parsed into
//! [`ExtractedMetadata`]. The model is the chat client by default,
//! configurable to any OpenAI-compatible endpoint via `DELPHI_EXTRACT_*`
//! (see `docs/architecture/metadata-extractor.md`).
//!
//! Everything here is **best-effort**: the bytes are already committed by
//! the time stage 6 runs, so a stream error, a timeout, or unparseable
//! output degrades to empty metadata rather than failing the upload. The
//! pipeline (stage 7) independently *validates* this untrusted output
//! before it reaches the merge.
//!
//! The `LlmClient` trait exposes only `stream_chat → text`, with no
//! JSON-mode / `response_format` hook, so we prompt for a bare JSON object
//! and parse it defensively (slice the outermost `{…}`, tolerate code
//! fences and surrounding prose).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use tokio::time::timeout;
use tracing::warn;

use super::autofill::{ExtractedMetadata, ExtractionContext, MetadataExtractor};
use crate::error::Result;
use crate::llm::{DeltaStream, LlmClient, LlmDelta, LlmMessage, Role};

const SYSTEM_PROMPT: &str = "\
You extract bibliographic metadata from the opening text of a document \
(usually a research paper or article). Respond with ONLY a single JSON \
object and nothing else — no prose, no markdown, no code fences. Use \
exactly these keys:\n\
{\"title\": string|null, \"authors\": string[], \"summary\": string|null, \
\"language\": string|null, \"published_at\": string|null, \"venue\": \
string|null, \"doi\": string|null}\n\n\
Rules:\n\
- Extract only what is explicitly present in the text. Never guess or invent.\n\
- If a field is not clearly stated, use null (or [] for authors).\n\
- authors: full names in the order shown.\n\
- summary: the abstract if present, else a one-sentence summary; null if there is no basis.\n\
- language: ISO 639-1 code (e.g. \"en\"); null if unsure.\n\
- published_at: ISO 8601 (YYYY-MM-DD), or just the year if that is all that is known; null if unknown.\n\
- Output only the JSON object.";

/// Default head of the extracted text fed to the model (~3k tokens).
const DEFAULT_MAX_INPUT_CHARS: usize = 12_000;
/// Default hard bound on the extraction call.
const DEFAULT_TIMEOUT_SECS: u64 = 30;

pub struct LlmExtractor {
    llm: Arc<dyn LlmClient>,
    max_input_chars: usize,
    timeout: Duration,
}

impl LlmExtractor {
    pub fn new(llm: Arc<dyn LlmClient>, max_input_chars: usize, timeout: Duration) -> Self {
        Self {
            llm,
            max_input_chars,
            timeout,
        }
    }

    /// Read the operational knobs (`DELPHI_EXTRACT_MAX_INPUT_CHARS`,
    /// `DELPHI_EXTRACT_TIMEOUT_SECS`) with defaults; the client is built by
    /// `llm::extractor_llm_from_env` and injected.
    pub fn from_env(llm: Arc<dyn LlmClient>) -> Self {
        let max_input_chars = env_usize("DELPHI_EXTRACT_MAX_INPUT_CHARS", DEFAULT_MAX_INPUT_CHARS);
        let timeout_secs = env_u64("DELPHI_EXTRACT_TIMEOUT_SECS", DEFAULT_TIMEOUT_SECS);
        Self::new(llm, max_input_chars, Duration::from_secs(timeout_secs))
    }
}

#[async_trait]
impl MetadataExtractor for LlmExtractor {
    async fn extract(&self, ctx: &ExtractionContext<'_>) -> Result<ExtractedMetadata> {
        // No text (e.g. extraction was skipped / stage-4 sniff still
        // bypassed) ⇒ nothing to read; skip the LLM call entirely.
        let head: String = ctx.text.chars().take(self.max_input_chars).collect();
        if head.trim().is_empty() {
            return Ok(ExtractedMetadata::default());
        }

        let messages = vec![
            LlmMessage {
                role: Role::System,
                content: SYSTEM_PROMPT.to_string(),
            },
            LlmMessage {
                role: Role::User,
                content: head,
            },
        ];

        let stream = match self.llm.stream_chat(messages).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "metadata extractor: stream_chat failed; no autofill");
                return Ok(ExtractedMetadata::default());
            }
        };

        let raw = match timeout(self.timeout, collect_text(stream)).await {
            Ok(t) => t,
            Err(_) => {
                warn!(timeout_secs = self.timeout.as_secs(), "metadata extractor: timed out; no autofill");
                return Ok(ExtractedMetadata::default());
            }
        };

        Ok(parse_metadata(&raw).unwrap_or_else(|| {
            warn!("metadata extractor: could not parse JSON from model output; no autofill");
            ExtractedMetadata::default()
        }))
    }
}

async fn collect_text(mut stream: DeltaStream) -> String {
    let mut out = String::new();
    while let Some(item) = stream.next().await {
        if let Ok(LlmDelta::Text(t)) = item {
            out.push_str(&t);
        }
    }
    out
}

/// Wire shape the model is asked to emit. All fields tolerant: missing keys
/// default, `authors` accepts an array, a single string, or null.
#[derive(Debug, Default, Deserialize)]
struct ExtractedWire {
    #[serde(default)]
    title: Option<String>,
    #[serde(default, deserialize_with = "de_authors")]
    authors: Vec<String>,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    language: Option<String>,
    #[serde(default)]
    published_at: Option<String>,
    #[serde(default)]
    venue: Option<String>,
    #[serde(default)]
    doi: Option<String>,
}

/// Parse the model output into `ExtractedMetadata`, or `None` if no JSON
/// object can be recovered. `venue`/`doi` fold into the free-form `extra`.
fn parse_metadata(raw: &str) -> Option<ExtractedMetadata> {
    let json = extract_json_object(raw)?;
    let wire: ExtractedWire = serde_json::from_str(json).ok()?;

    let mut extra = serde_json::Map::new();
    if let Some(v) = clean(wire.venue) {
        extra.insert("venue".into(), Value::String(v));
    }
    if let Some(d) = clean(wire.doi) {
        extra.insert("doi".into(), Value::String(d));
    }

    Some(ExtractedMetadata {
        title: clean(wire.title),
        authors: wire.authors,
        summary: clean(wire.summary),
        language: clean(wire.language),
        published_at: wire.published_at.as_deref().and_then(parse_date),
        extra: Value::Object(extra),
    })
}

/// Slice the outermost `{ … }` so code fences / surrounding prose don't
/// defeat parsing.
fn extract_json_object(raw: &str) -> Option<&str> {
    let start = raw.find('{')?;
    let end = raw.rfind('}')?;
    (end > start).then(|| &raw[start..=end])
}

/// Trim, and treat empty as absent.
fn clean(s: Option<String>) -> Option<String> {
    s.map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

/// Best-effort date parse: full RFC3339, `YYYY-MM-DD`, `YYYY-MM`, or `YYYY`.
fn parse_date(s: &str) -> Option<DateTime<Utc>> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    let normalized = match s.len() {
        4 if s.chars().all(|c| c.is_ascii_digit()) => format!("{s}-01-01"),
        7 => format!("{s}-01"), // YYYY-MM
        _ => s.to_string(),
    };
    NaiveDate::parse_from_str(&normalized, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|dt| Utc.from_utc_datetime(&dt))
}

fn de_authors<'de, D>(d: D) -> std::result::Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        Many(Vec<String>),
        One(String),
    }
    let norm = |s: String| s.trim().to_string();
    Ok(match Option::<OneOrMany>::deserialize(d)? {
        Some(OneOrMany::Many(v)) => v.into_iter().map(norm).filter(|s| !s.is_empty()).collect(),
        Some(OneOrMany::One(s)) => {
            let s = norm(s);
            if s.is_empty() {
                vec![]
            } else {
                vec![s]
            }
        }
        None => vec![],
    })
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::DocumentPrefill;
    use futures::stream;

    /// Emits the scripted string as one text delta.
    struct ScriptedLlm(String);

    #[async_trait]
    impl LlmClient for ScriptedLlm {
        async fn stream_chat(&self, _messages: Vec<LlmMessage>) -> Result<DeltaStream> {
            let delta = Ok(LlmDelta::Text(self.0.clone()));
            Ok(Box::pin(stream::iter(vec![delta])))
        }
    }

    async fn run(script: &str, text: &str) -> ExtractedMetadata {
        let llm: Arc<dyn LlmClient> = Arc::new(ScriptedLlm(script.to_string()));
        let extractor = LlmExtractor::new(llm, 12_000, Duration::from_secs(5));
        let prefill = DocumentPrefill::default();
        let ctx = ExtractionContext { text, prefill: &prefill };
        extractor.extract(&ctx).await.unwrap()
    }

    #[tokio::test]
    async fn clean_json_maps_onto_metadata() {
        let script = r#"{"title":"Attention Is All You Need","authors":["Ashish Vaswani","Noam Shazeer"],"summary":"A new architecture.","language":"en","published_at":"2017-06-12","venue":"NeurIPS","doi":"10.5555/abc"}"#;
        let got = run(script, "some paper text").await;
        assert_eq!(got.title.as_deref(), Some("Attention Is All You Need"));
        assert_eq!(got.authors, vec!["Ashish Vaswani".to_string(), "Noam Shazeer".to_string()]);
        assert_eq!(got.summary.as_deref(), Some("A new architecture."));
        assert_eq!(got.language.as_deref(), Some("en"));
        assert_eq!(
            got.published_at.unwrap().format("%Y-%m-%d").to_string(),
            "2017-06-12"
        );
        assert_eq!(got.extra["venue"], Value::String("NeurIPS".into()));
        assert_eq!(got.extra["doi"], Value::String("10.5555/abc".into()));
    }

    #[tokio::test]
    async fn tolerates_code_fences_and_prose() {
        let script = "Here is the metadata:\n```json\n{\"title\": \"X\", \"authors\": \"Solo Author\"}\n```\nHope that helps!";
        let got = run(script, "text").await;
        assert_eq!(got.title.as_deref(), Some("X"));
        // authors-as-string is coerced to a one-element vec.
        assert_eq!(got.authors, vec!["Solo Author".to_string()]);
        assert!(got.summary.is_none());
    }

    #[tokio::test]
    async fn year_only_date_parses_to_jan_first() {
        let got = run(r#"{"title":"T","published_at":"2017"}"#, "text").await;
        assert_eq!(
            got.published_at.unwrap().format("%Y-%m-%d").to_string(),
            "2017-01-01"
        );
    }

    #[tokio::test]
    async fn null_fields_become_none() {
        let got = run(
            r#"{"title":null,"authors":null,"summary":"","language":null,"published_at":"not a date"}"#,
            "text",
        )
        .await;
        assert!(got.title.is_none());
        assert!(got.authors.is_empty());
        assert!(got.summary.is_none()); // empty string treated as absent
        assert!(got.published_at.is_none()); // unparseable date dropped
    }

    #[tokio::test]
    async fn garbage_output_degrades_to_empty() {
        let got = run("the model refused and wrote a sonnet instead", "text").await;
        assert!(got.title.is_none());
        assert!(got.authors.is_empty());
        assert!(got.extra.as_object().map(|m| m.is_empty()).unwrap_or(true));
    }

    #[tokio::test]
    async fn empty_text_skips_extraction() {
        // Script would parse, but empty input short-circuits before any call.
        let got = run(r#"{"title":"should not appear"}"#, "   ").await;
        assert!(got.title.is_none());
    }
}
