//! Port traits and their data types.
//!
//! Everything here is expressed in terms the use cases understand — "append an
//! event", not "publish to JetStream". Adapters in `delphi-document-adapters`
//! implement these; `crate::testing` provides deterministic in-memory versions.

use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use delphi_document_domain::{DocumentEvent, DocumentState};
use serde::{Deserialize, Serialize};

use crate::command::UploadCompleted;
use crate::cursor::DocumentCursor;
use crate::errors::{
    BlobError, ContextError, EventStoreError, QueueError, ReadError, ScanError, ValidateError,
};
use crate::upload_state::{StoredUpload, UploadState};

// ---------------------------------------------------------------- event store

/// What the caller believes the subject's last sequence to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expect {
    /// The document must not exist yet: `Nats-Expected-Last-Subject-Sequence: 0`.
    CreateOnly,
    Exactly(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Appended {
    pub stream_seq: u64,
    pub version: u64,
    /// The store deduplicated this publish on its message id. When true the
    /// expected-sequence header was *not* evaluated, so a locally computed
    /// version may be wrong and must be re-read.
    pub duplicate: bool,
}

#[async_trait]
pub trait EventStore: Send + Sync + 'static {
    async fn append(
        &self,
        event: DocumentEvent,
        expect: Expect,
    ) -> Result<Appended, EventStoreError>;

    /// Authoritative `(version, stream_seq)` for a document, bypassing the
    /// projection. Preflight uses this for existence because the projection
    /// lags and a document created seconds ago would 404 spuriously.
    async fn last(
        &self,
        tenant: &str,
        document_id: &str,
    ) -> Result<Option<(u64, u64)>, EventStoreError>;

    async fn read_stream(
        &self,
        tenant: &str,
        document_id: &str,
    ) -> Result<Vec<(u64, DocumentEvent)>, EventStoreError>;
}

// ----------------------------------------------------------------- blob store

pub type BoxAsyncRead = Pin<Box<dyn tokio::io::AsyncRead + Send>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PresignedPart {
    pub part_number: u16,
    /// Always a `PUT`; the verb is not reported because it cannot vary.
    pub url: String,
    /// Kept for the batch case. A client signing one part immediately before
    /// uploading it never has to look, but one that asks for a window of parts
    /// in a single request needs to know when they go stale.
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadedPart {
    pub part_number: u16,
    pub etag: String,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedPart {
    pub part_number: u16,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobHead {
    pub byte_size: u64,
    pub content_type: Option<String>,
    pub last_modified: DateTime<Utc>,
}

#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    async fn begin_multipart(&self, key: &str, content_type: &str) -> Result<String, BlobError>;

    async fn presign_part(
        &self,
        key: &str,
        upload: &str,
        part: u16,
        ttl: Duration,
    ) -> Result<PresignedPart, BlobError>;

    /// `None` when the multipart no longer exists. Implementations must page:
    /// S3 returns at most 1000 parts per call.
    async fn list_parts(
        &self,
        key: &str,
        upload: &str,
    ) -> Result<Option<Vec<UploadedPart>>, BlobError>;

    async fn complete_multipart(
        &self,
        key: &str,
        upload: &str,
        parts: &[CompletedPart],
    ) -> Result<(), BlobError>;

    async fn abort_multipart(&self, key: &str, upload: &str) -> Result<(), BlobError>;

    async fn head(&self, key: &str) -> Result<Option<BlobHead>, BlobError>;

    /// The whole object, streamed. Only the scanner needs this.
    async fn open_read(&self, key: &str) -> Result<BoxAsyncRead, BlobError>;

    /// The first `len` bytes, or the whole object if it is shorter.
    ///
    /// Separate from [`BlobStore::open_read`] because it is a *ranged* read.
    /// The content sniff wants 512 bytes; taking them off the front of a
    /// full-object GET makes storage start streaming a possibly multi-gigabyte
    /// body that is then thrown away.
    async fn read_prefix(&self, key: &str, len: usize) -> Result<Vec<u8>, BlobError>;

    async fn delete(&self, key: &str) -> Result<(), BlobError>;
}

// --------------------------------------------------------------- verification

/// Consumes the stream once and returns BOTH the verdict and the digest — the
/// worker has no other source for `DocumentCreated.checksum`.
#[async_trait]
pub trait BlobScanner: Send + Sync + 'static {
    async fn scan(&self, blob: BoxAsyncRead) -> Result<ScanOutcome, ScanError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanOutcome {
    pub verdict: ScanVerdict,
    pub sha256_hex: String,
    pub byte_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanVerdict {
    Clean,
    Infected { signature: String },
}

#[async_trait]
pub trait ContentValidator: Send + Sync + 'static {
    async fn validate(
        &self,
        head: &BlobHead,
        prefix: &[u8],
        declared: &DeclaredContent,
    ) -> Result<ContentVerdict, ValidateError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredContent {
    pub filename: String,
    pub content_type: String,
    pub byte_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentVerdict {
    Ok,
    Rejected { reason: String },
}

// ---------------------------------------------------------------- upload state

/// The upload's single tracker. See [`crate::upload_state`].
///
/// Everything here is keyed by the caller, so the store cannot answer a
/// question about *other people's* uploads — deliberately, since that is the
/// boundary between the user-scoped upload and the tenant-scoped document.
#[async_trait]
pub trait UploadStateStore: Send + Sync + 'static {
    /// `create`, not `put`: a second preflight must never silently rewrite the
    /// parameters a client is already slicing to.
    async fn create(&self, state: &UploadState) -> Result<(), ContextError>;

    /// The record and the revision it was read at, for a later CAS. `None` once
    /// the TTL has elapsed — which is a real state, not just a miss.
    async fn get(
        &self,
        tenant: &str,
        user: &str,
        upload_id: &str,
    ) -> Result<Option<StoredUpload>, ContextError>;

    /// Compare-and-swap on `revision`. `Err(ContextError::Conflict)` if the
    /// record moved underneath the caller, `Err(ContextError::Expired)` if it
    /// is gone.
    ///
    /// CAS rather than a blind put because two writers race for this record and
    /// a terminal status must survive — see `UploadStatus::is_terminal`.
    async fn update(&self, state: &UploadState, revision: u64) -> Result<u64, ContextError>;

    /// Best-effort cleanup when preflight fails after the record was written,
    /// so a stale record cannot outlive its multipart.
    async fn delete(&self, tenant: &str, user: &str, upload_id: &str) -> Result<(), ContextError>;
}

// ------------------------------------------------------------------ read model

#[async_trait]
pub trait DocumentReadModel: Send + Sync + 'static {
    async fn get(
        &self,
        tenant: &str,
        document_id: &str,
    ) -> Result<Option<DocumentState>, ReadError>;

    /// Tenant-scoped, deliberately not owner-scoped: a document belongs to the
    /// tenant, and every member may read it. Keep this in step with `get`,
    /// which has always been tenant-scoped — the two disagreeing is how a
    /// document became readable by id but invisible in a listing.
    ///
    /// Ordered `(updated_at DESC, document_id DESC)` and resumed strictly after
    /// `after`. Both halves of that key are required: see [`DocumentCursor`].
    async fn list(
        &self,
        tenant: &str,
        limit: u32,
        after: Option<&DocumentCursor>,
    ) -> Result<Vec<DocumentState>, ReadError>;
}

// ------------------------------------------------------------------ work queue

#[async_trait]
pub trait WorkQueue: Send + Sync + 'static {
    async fn publish_upload_completed(&self, cmd: UploadCompleted) -> Result<(), QueueError>;
}

// ------------------------------------------------------------------ ambient IO

/// Ports so use cases are deterministic under test.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> DateTime<Utc>;
}

pub trait IdGen: Send + Sync + 'static {
    fn ulid(&self) -> String;
}
