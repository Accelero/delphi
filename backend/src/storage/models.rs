use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::RecordId;

/// SurrealDB record id, e.g. `document:abc…`.
pub type DocId = RecordId;
pub type ChunkId = RecordId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// Populated by the backend on read; ignored on write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<RecordId>,

    pub canonical_id: String,
    pub source_type: String,
    pub source_uri: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default)]
    pub authors: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingested_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,

    /// Hex-encoded SHA-256 of normalized content. Dedup key.
    pub content_hash: String,

    #[serde(default = "default_version")]
    pub version: i64,

    #[serde(default)]
    pub metadata: serde_json::Value,
}

fn default_version() -> i64 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Content {
    pub text: String,
    #[serde(default = "default_format")]
    pub format: String,
    #[serde(default = "default_extractor")]
    pub extractor: String,
}

fn default_format() -> String {
    "text".into()
}

fn default_extractor() -> String {
    "manual".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    /// Populated by the backend on read; ignored on write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<RecordId>,

    pub ordinal: i64,
    pub char_start: i64,
    pub char_end: i64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bbox: Option<serde_json::Value>,

    pub text: String,
    pub embedding: Vec<f32>,
    pub embedding_model: String,
    pub chunk_strategy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkSearchResult {
    pub chunk_id: RecordId,
    pub doc_id: RecordId,
    pub text: String,
    pub score: f64,
    pub ordinal: i64,
    pub char_start: i64,
    pub char_end: i64,
    pub page: Option<i64>,
}

/// Backend-agnostic filter struct. Backends may ignore unknown filters but
/// should at least support these.
#[derive(Debug, Default, Clone)]
pub struct Filters {
    pub embedding_model: Option<String>,
    pub chunk_strategy: Option<String>,
    pub source_type: Option<String>,
}
