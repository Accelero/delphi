//! Original-artefact storage (PDFs, EPUBs, …).
//!
//! Adapters that have access to a document's bytes (e.g. arXiv with its
//! free PDFs) hand them to an `ObjectStore` and stash the returned URL
//! on `Document.storage_uri`. The same URL is later dereferenced for
//! re-extraction or "show original" surfaces.
//!
//! Backend choice is **inferred from a URL** — single env var
//! `OBJECT_STORE_URL` carries it. Local-FS for single-user, S3 for
//! SaaS later. See [`from_url`].
//!
//! ## Why a separate module from `Storage`
//!
//! - **Different access pattern.** Blobs are write-once, read-rare;
//!   the DB is read-heavy and structured. Putting MBs of PDF in
//!   SurrealDB rows would blow the working set.
//! - **Different scaling.** Object storage scales independently of
//!   the database (S3 vs Postgres). Keeping the abstraction lets us
//!   move either one without touching the other.
//!
//! ## Multipart
//!
//! Direct-to-S3 uploads (ingestion v2) use the multipart methods:
//! `create_multipart_upload` / `presign_upload_part` /
//! `complete_multipart_upload`. `LocalFsObjectStore` ships an
//! in-process shim implementing the same interface so integration
//! tests don't need MinIO. The presigned-URL signing only makes sense
//! against a real S3 endpoint; the shim returns a local HTTP-style URL
//! reserved for tests that drive the upload via direct `put_part` calls
//! rather than an HTTP client.

mod local_fs;
mod mem;
mod multipart;
mod s3;
mod url;

use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::{Error, Result};

pub use local_fs::LocalFsObjectStore;
pub use mem::MemObjectStore;
pub use multipart::{
    storage_uri_for_key, CompleteOutcome, MultipartEntry, ObjectEntry, ObjectMeta, PartRef,
    PresignedUrl,
};
pub use url::from_url;

/// Read-write key/value blob store.
///
/// Keys are virtual paths (`originals/2106.09685v1.pdf`), no leading slash,
/// no `..` segments — implementations refuse those for path-traversal
/// safety.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Write `bytes` under `key`. Overwrites if present (last writer
    /// wins). Returns the fully-qualified URL the impl uses to refer
    /// back to this object — what callers persist on
    /// `Document.storage_uri`.
    async fn put(&self, key: &str, bytes: Bytes) -> Result<String>;

    async fn get(&self, key: &str) -> Result<Bytes>;

    /// Ranged GET. Used by the ingestion v2 object validator to sniff
    /// magic bytes without downloading the full object. Implementations
    /// without native range support may fall back to a full read; that's
    /// fine for tests but should be avoided in production.
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes> {
        let _ = range;
        let _ = key;
        Err(Error::NotImplemented(
            "ObjectStore::get_range not supported by this backend".into(),
        ))
    }

    async fn delete(&self, key: &str) -> Result<()>;

    async fn exists(&self, key: &str) -> Result<bool>;

    /// Read back an object given the URL `put` previously returned.
    /// Implementations parse their own URL form (`file://…`,
    /// `mem://…`, eventually `s3://…`) and reject URLs they don't own.
    async fn get_by_url(&self, url: &str) -> Result<Bytes>;

    /// HEAD: object metadata. Used by the validator at `/complete` to
    /// verify committed size and capture the ETag without downloading
    /// the body.
    async fn head(&self, key: &str) -> Result<ObjectMeta> {
        let _ = key;
        Err(Error::NotImplemented(
            "ObjectStore::head not supported by this backend".into(),
        ))
    }

    // ---- multipart ---------------------------------------------------------

    /// Open a multipart upload. Returns the provider-issued upload id.
    /// `content_type` is recorded as object metadata; **not** enforced
    /// by S3 (see `docs/architecture/ingestion-v2.md` "What S3 actually
    /// enforces"). Real enforcement happens at `/complete` in the
    /// validator.
    async fn create_multipart_upload(
        &self,
        key: &str,
        content_type: &str,
    ) -> Result<String> {
        let _ = (key, content_type);
        Err(Error::NotImplemented(
            "ObjectStore::create_multipart_upload not supported by this backend".into(),
        ))
    }

    /// Presign a PUT for one part of a multipart upload. Returns the
    /// URL the client uploads bytes to directly.
    async fn presign_upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: u16,
        ttl: std::time::Duration,
    ) -> Result<PresignedUrl> {
        let _ = (key, upload_id, part_number, ttl);
        Err(Error::NotImplemented(
            "ObjectStore::presign_upload_part not supported by this backend".into(),
        ))
    }

    /// Combine the uploaded parts into the final object. Returns the
    /// committed object's metadata.
    async fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[PartRef],
    ) -> Result<CompleteOutcome> {
        let _ = (key, upload_id, parts);
        Err(Error::NotImplemented(
            "ObjectStore::complete_multipart_upload not supported by this backend".into(),
        ))
    }

    /// Abort a multipart upload, releasing whatever bytes the provider
    /// is holding. Idempotent — aborting a missing upload is a no-op.
    async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> Result<()> {
        let _ = (key, upload_id);
        Err(Error::NotImplemented(
            "ObjectStore::abort_multipart_upload not supported by this backend".into(),
        ))
    }

    /// List committed objects under `prefix`. Used by the nightly cleaner.
    async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectEntry>> {
        let _ = prefix;
        Err(Error::NotImplemented(
            "ObjectStore::list_objects not supported by this backend".into(),
        ))
    }

    /// List in-progress multipart uploads. Used by the nightly cleaner
    /// to abort orphans.
    async fn list_multipart_uploads(&self) -> Result<Vec<MultipartEntry>> {
        Err(Error::NotImplemented(
            "ObjectStore::list_multipart_uploads not supported by this backend".into(),
        ))
    }

    // ---- test-only multipart shim ----------------------------------------

    /// Direct part upload, used by the `LocalFsObjectStore` multipart
    /// shim in tests. Production callers go through the presigned URL
    /// returned by `presign_upload_part` instead. Default implementation
    /// returns `NotImplemented`; the shim overrides.
    #[doc(hidden)]
    async fn upload_part_direct(
        &self,
        key: &str,
        upload_id: &str,
        part_number: u16,
        bytes: Bytes,
    ) -> Result<String> {
        let _ = (key, upload_id, part_number, bytes);
        Err(Error::NotImplemented(
            "ObjectStore::upload_part_direct not supported by this backend".into(),
        ))
    }
}
