use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::RecordId;

/// SurrealDB record id, e.g. `document:abc…`.
pub type DocId = RecordId;
pub type ChunkId = RecordId;
pub type ConversationId = RecordId;
pub type MessageId = RecordId;

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

    /// Multi-tenancy: filled engine-side from `$auth.tenant_id` via the
    /// schema's `DEFAULT` clause on write, populated on read. Application
    /// code does not set this — engine-side PERMISSIONS enforce tenant
    /// scoping based on the request's JWT-authenticated session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<RecordId>,

    /// Natural-source dedup key (`doi:…`, etc.). Optional: manual uploads
    /// leave it unset and are identified by their record id alone.
    /// `None` serialises as absent so the engine stores `NONE`, not `""` —
    /// load-bearing for the unique-when-set index and the conflict
    /// pre-check (see `commit_upload`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_id: Option<String>,
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

    /// Optional document-level embedding (RAG v1: SPECTER2 over
    /// `title + [SEP] + abstract`). 768-dim when populated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paper_embedding: Option<Vec<f32>>,
    /// Model name written alongside `paper_embedding` for cross-version
    /// safety (when we eventually migrate to a different SPECTER2-class
    /// model, mixed rows are distinguishable).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paper_embedding_model: Option<String>,

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

/// Compute the per-tenant dedup key for the `dedup_key` UNIQUE index.
///
/// Returns `None` when `canonical_id` is absent (manual uploads — these
/// rows are excluded from the unique index, so any number of them
/// coexist), else `"<tenant_id>|<canonical_id>"` so dedup is scoped per
/// tenant (cross-tenant same canonical_id is allowed; same-tenant
/// duplicate is rejected). Both `document` and `upload_session` use this.
pub fn dedup_key(tenant_id: &RecordId, canonical_id: Option<&str>) -> Option<String> {
    canonical_id.map(|cid| format!("{tenant_id}|{cid}"))
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

/// One line-bounding rectangle of a chunk on a specific PDF page. PDF
/// coordinate space (origin bottom-left, points = 1/72 inch). The viewer
/// flips to CSS top-left coords using the page's height + rotation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Bbox {
    pub page: i64,
    pub x: f64,
    pub y: f64,
    pub w: f64,
    pub h: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    /// Populated by the backend on read; ignored on write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<RecordId>,

    /// Foreign key to the owning document. Populated on read (via the
    /// schema's `doc` field). On the write path the value is supplied
    /// out-of-band (the `upsert_chunks(&doc_id, …)` parameter), so the
    /// struct serializer skips it.
    #[serde(default, skip_serializing)]
    pub doc: Option<RecordId>,

    pub ordinal: i64,
    pub char_start: i64,
    pub char_end: i64,

    /// Per-line rectangles spanning the chunk's text. Multi-page chunks
    /// carry boxes from each page they cross.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bboxes: Option<Vec<Bbox>>,

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
}

/// Backend-agnostic filter struct. Backends may ignore unknown filters but
/// should at least support these.
#[derive(Debug, Default, Clone)]
pub struct Filters {
    pub embedding_model: Option<String>,
    pub chunk_strategy: Option<String>,
    pub source_type: Option<String>,
}

/// A persisted chat conversation. The owning `user` field is implied by
/// engine PERMISSIONS (`user = $auth.id`) and is not exposed on the wire —
/// the caller is always the owner of any conversation they can see.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conversation {
    /// Populated by the backend on read; ignored on write.
    /// Wire format: `"conversation:<key>"` string.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "opt_record_id_str"
    )]
    pub id: Option<RecordId>,

    /// Filled engine-side from `$auth.tenant_id` on write, populated on read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<RecordId>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<DateTime<Utc>>,
}

/// One resolved RAG citation, persisted on an assistant `message` row so
/// a reloaded conversation can render its `[N]` markers without re-running
/// retrieval. Storage-owned (no `api` dependency); its field layout is
/// the wire shape the SPA consumes, identical to `sse::CitationEntry` —
/// the worker maps from one to the other.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Citation {
    /// Bracket number rendered as `[n]` in the assistant text.
    pub n: usize,
    /// `chunk:<key>` — what the frontend feeds to `/api/chunks/:id`.
    pub chunk_id: String,
    /// `document:<key>` — used by the deep-link `/feed?doc=&chunk=`.
    pub doc_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<i64>,
}

/// A single chat message inside a [`Conversation`]. `tenant_id` and the
/// `conversation` link are engine-managed; on the wire we only expose
/// what the SPA needs to render the message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "opt_record_id_str"
    )]
    pub id: Option<RecordId>,
    pub role: String,
    pub content: String,
    /// Parent message in the linear chat history. `None` for the first
    /// message of a conversation; otherwise the id of the prior
    /// assistant message (or, for an assistant row, the user message it
    /// answers). Used by `commit_turn` for "last writer wins" semantics
    /// and by the frontend to thread the next submit.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "opt_record_id_str"
    )]
    pub parent_id: Option<RecordId>,
    /// Resolved RAG citations for an assistant message. `None` for user
    /// messages and assistant turns that cited nothing. Populated on read
    /// (history) and written by [`Storage::commit_turn`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub citations: Option<Vec<Citation>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
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

/// Parameters for `Storage::create_upload_session`. Mirrors what the
/// `POST /api/ingestion/uploads` handler has after the metadata validator
/// runs and `ObjectStore::create_multipart_upload` has minted an
/// `upload_id`.
#[derive(Debug, Clone)]
pub struct CreateUploadSessionParams {
    pub doc_id: String,
    pub s3_key: String,
    pub s3_upload_id: String,
    /// Optional dedup key; `None` for manual uploads (bound as `NONE`).
    pub canonical_id: Option<String>,
    /// Per-tenant dedup index value: `Some("<tenant_id>|<canonical_id>")`
    /// when `canonical_id` is set, else `None`. Computed by the handler
    /// via [`dedup_key`] (the engine can't derive it; see schema comment).
    pub dedup_key: Option<String>,
    pub source_type: String,
    pub source_uri: String,
    pub title: Option<String>,
    pub declared_size: u64,
    pub declared_content_type: String,
    pub declared_metadata: serde_json::Value,
}

/// One upload session row, as returned by `Storage::get_upload_session`
/// and the cleaner's list helpers. `tenant_id` and `user_id` are
/// engine-managed; we expose them so handlers can do the redundant
/// belt-and-suspenders identity check from the plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadSession {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<RecordId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<RecordId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<RecordId>,

    pub doc_id: String,
    pub s3_key: String,
    pub s3_upload_id: String,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_id: Option<String>,
    pub source_type: String,
    pub source_uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub declared_size: i64,
    pub declared_content_type: String,
    #[serde(default)]
    pub declared_metadata: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
}

/// Side-channel rejection record. Written by the validator-reject path
/// inside `POST /uploads/:id/complete`; reaped by the nightly cleaner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionRejection {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<RecordId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<RecordId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<RecordId>,

    pub doc_id: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sniffed_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejected_at: Option<DateTime<Utc>>,
}

/// `commit_upload` error path: a row with the same
/// `(tenant_id, canonical_id)` already exists. The handler returns 422
/// with `existing_doc_id` so the SPA can deep-link to the document.
#[derive(Debug, Clone)]
pub struct CanonicalIdConflict {
    pub existing_doc_id: DocId,
}
