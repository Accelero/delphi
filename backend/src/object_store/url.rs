//! `DELPHI_INGEST_OBJECT_STORE_URL` → `Arc<dyn ObjectStore>` / `Arc<dyn AccessMinter>`
//! dispatchers.

use std::sync::Arc;

use crate::error::{Error, Result};

use super::s3::{S3ObjectStore, S3PresignAccess};
use super::{AccessMinter, ObjectStore};

/// Construct an `ObjectStore` from a URL.
///
/// Recognised schemes:
/// - `s3://bucket/prefix` — S3-compatible (MinIO / Hetzner / R2 / B2 /
///   AWS). The only production backend. The bucket from the URL is
///   ignored in favour of `DELPHI_INGEST_S3_BUCKET`; the endpoints, region,
///   credentials, and path-style flag come from the `INGEST_S3_*` env
///   vars via [`S3ObjectStore::from_env`].
///
/// Any other scheme (including the old `file://` local-FS form) is
/// rejected: there is no LocalFs fallback. Tests use `MemObjectStore`
/// directly.
pub fn from_url(url: &str) -> Result<Arc<dyn ObjectStore>> {
    if url.starts_with("s3://") {
        let store = S3ObjectStore::from_env()?;
        return Ok(Arc::new(store));
    }
    Err(Error::InvalidConfig(format!(
        "DELPHI_INGEST_OBJECT_STORE_URL must be an s3:// URL (got {url}); LocalFs is removed"
    )))
}

/// Construct the client-facing [`AccessMinter`] from the same
/// `DELPHI_INGEST_OBJECT_STORE_URL`. Today every `s3://` deployment uses
/// [`S3PresignAccess`] (presigned URLs over the public endpoint); the
/// deferred `CdnAccess` / `StsAccess` / `ProxyAccess` minters swap in
/// here behind a deployment-config knob without touching callers — see
/// `docs/architecture/object-access.md`.
pub fn access_minter_from_url(url: &str) -> Result<Arc<dyn AccessMinter>> {
    if url.starts_with("s3://") {
        let access = S3PresignAccess::from_env()?;
        return Ok(Arc::new(access));
    }
    Err(Error::InvalidConfig(format!(
        "DELPHI_INGEST_OBJECT_STORE_URL must be an s3:// URL (got {url}); LocalFs is removed"
    )))
}
