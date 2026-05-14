use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::RecordId;

/// SurrealDB record id, e.g. `document:abc…`.
pub type DocId = RecordId;
pub type ChunkId = RecordId;

/// Serde adapter that puts an `Option<RecordId>` on the wire as the
/// canonical `"table:key"` string instead of SurrealDB's structured
/// form (`{tb, id: {String}}`). Symmetric — deserialize accepts both
/// the new string form and the legacy structured form so older
/// payloads (or direct SurrealDB query responses going through this
/// type) keep working.
///
/// Storage code does *not* go through this adapter — the
/// `DocumentWire` shape inside `storage/surreal.rs` is what touches
/// the engine. This only affects the public-facing JSON wire (the
/// `/api/discovery/feed` response and the SSE event payload), where
/// emitting the structured form leaks SurrealDB internals across the
/// boundary and breaks string-based identity comparison on the client.
pub mod opt_record_id_str {
    use serde::{Deserialize, Deserializer, Serializer};
    use surrealdb::RecordId;

    pub fn serialize<S: Serializer>(v: &Option<RecordId>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(id) => s.serialize_some(&id.to_string()),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<RecordId>, D::Error> {
        // Accept either a string ("table:key") or the legacy structured
        // form, so that data round-tripped via either shape still parses.
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Str(String),
            Structured(RecordId),
        }
        let parsed: Option<Wire> = Option::deserialize(d)?;
        Ok(parsed.map(|w| match w {
            Wire::Str(s) => match s.split_once(':') {
                Some((tb, key)) => RecordId::from((tb, key)),
                // Bare key without table prefix — implausible from any
                // current producer, but be tolerant: synthesise as a
                // `document` id (the only id-bearing public model that
                // uses this adapter).
                None => RecordId::from(("document", s.as_str())),
            },
            Wire::Structured(rid) => rid,
        }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// Populated by the backend on read; ignored on write.
    /// Wire format: `"document:<key>"` string. See [`opt_record_id_str`].
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "opt_record_id_str"
    )]
    pub id: Option<RecordId>,

    /// Multi-tenancy: every domain row carries the tenant it belongs to.
    /// Populated by the ingestion pipeline from `AuthContext.tenant_id`
    /// (HTTP path) or from `SOURCES_DEFAULT_TENANT_SLUG` (scheduler).
    /// `Storage::upsert_document` writes it; reads filter by it.
    pub tenant_id: RecordId,

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

/// Anchor for cursor-paginated feed reads. The API layer base64-encodes
/// this for the wire; the storage layer takes it as a typed value.
/// `chrono::DateTime<Utc>` at the public boundary; SurrealDB-native
/// datetime conversion is hidden inside `SurrealStorage`.
#[derive(Debug, Clone)]
pub struct FeedCursor {
    pub ingested_at: DateTime<Utc>,
    pub id: DocId,
}
