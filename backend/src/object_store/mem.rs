//! In-memory `ObjectStore` for tests. Not used by production code.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::RwLock;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::{Error, Result};

use super::multipart::ObjectMeta;
use super::ObjectStore;

#[derive(Default)]
pub struct MemObjectStore {
    inner: RwLock<HashMap<String, Bytes>>,
}

impl MemObjectStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ObjectStore for MemObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<String> {
        self.inner
            .write()
            .expect("MemObjectStore poisoned")
            .insert(key.to_string(), bytes);
        Ok(format!("mem://{key}"))
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        self.inner
            .read()
            .expect("MemObjectStore poisoned")
            .get(key)
            .cloned()
            .ok_or_else(|| Error::Adapter {
                name: "mem-object-store".into(),
                message: format!("key not found: {key}"),
            })
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner
            .write()
            .expect("MemObjectStore poisoned")
            .remove(key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self
            .inner
            .read()
            .expect("MemObjectStore poisoned")
            .contains_key(key))
    }

    async fn get_by_url(&self, url: &str) -> Result<Bytes> {
        let key = url.strip_prefix("mem://").ok_or_else(|| {
            Error::InvalidConfig(format!("not a mem:// URL: {url}"))
        })?;
        self.get(key).await
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes> {
        let full = self.get(key).await?;
        let start = range.start as usize;
        let end = (range.end as usize).min(full.len());
        if start >= full.len() {
            return Ok(Bytes::new());
        }
        Ok(full.slice(start..end))
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta> {
        let bytes = self.get(key).await?;
        Ok(ObjectMeta {
            size: bytes.len() as u64,
            etag: format!("\"mem-{}\"", bytes.len()),
            content_type: None,
            last_modified: None,
        })
    }
}
