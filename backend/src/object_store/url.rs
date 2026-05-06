//! `OBJECT_STORE_URL` → `Arc<dyn ObjectStore>` dispatcher.

use std::path::PathBuf;
use std::sync::Arc;

use crate::error::Result;

use super::s3;
use super::{LocalFsObjectStore, ObjectStore};

/// Construct an `ObjectStore` from a URL.
///
/// Recognised schemes:
/// - `file:///abs/path` — filesystem-backed (the default).
/// - `s3://bucket/prefix` — placeholder; returns
///   `Error::NotImplemented` in slice 2.
/// - no scheme (`/abs/path` or `./relative`) — filesystem-backed.
pub fn from_url(url: &str) -> Result<Arc<dyn ObjectStore>> {
    if let Some(rest) = url.strip_prefix("file://") {
        let path = PathBuf::from(rest);
        return Ok(Arc::new(LocalFsObjectStore::new(path)?));
    }
    if url.starts_with("s3://") {
        return Err(s3::not_yet_supported(url));
    }
    // No scheme → treat as filesystem path. Allows `OBJECT_STORE_URL=
    // /var/lib/delphi/originals` or `./data/originals` shorthand.
    let path = PathBuf::from(url);
    Ok(Arc::new(LocalFsObjectStore::new(path)?))
}
