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

mod local_fs;
mod mem;
mod s3;
mod url;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::Result;

pub use local_fs::LocalFsObjectStore;
pub use mem::MemObjectStore;
pub use url::from_url;

/// Read-write key/value blob store.
///
/// Keys are virtual paths (`arxiv/2106.09685v1.pdf`), no leading slash,
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

    async fn delete(&self, key: &str) -> Result<()>;

    async fn exists(&self, key: &str) -> Result<bool>;

    /// Read back an object given the URL `put` previously returned.
    /// Implementations parse their own URL form (`file://…`,
    /// `mem://…`, eventually `s3://…`) and reject URLs they don't own.
    async fn get_by_url(&self, url: &str) -> Result<Bytes>;
}
