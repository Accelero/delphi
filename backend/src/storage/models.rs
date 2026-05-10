use serde::{Deserialize, Serialize};
use surrealdb::{Datetime, RecordId};

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
    /// SurrealDB-native datetime (vs. `chrono::DateTime`) so the SDK
    /// serializer emits the protocol's datetime tag instead of an ISO
    /// string — Surreal rejects raw strings for `TYPE datetime` fields.
    /// Convert from `chrono::DateTime<Utc>` via `.into()` at the
    /// ingestion boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_at: Option<Datetime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingested_at: Option<Datetime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,

    /// Author/publisher-written short prose (paper abstract, book flap
    /// copy, article deck). Distinct from `document_content.text`, which
    /// holds the body. Optional: not every source provides one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,

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

/// One row in the discovery feed. A `Document` plus per-user read state.
/// Built in storage by joining `document` with `feed_read`; not stored
/// directly. Serialized as `{ ...document fields, "read": bool }` for
/// the API.
#[derive(Debug, Clone, Serialize)]
pub struct FeedItem {
    #[serde(flatten)]
    pub document: Document,
    pub read: bool,
}

/// Anchor for cursor-paginated feed reads. The API layer base64-encodes
/// this for the wire; the storage layer takes it as a typed value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedCursor {
    pub ingested_at: Datetime,
    pub id: DocId,
}
