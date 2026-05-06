//! In-memory `ObjectStore` for tests. Not used by production code.

use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::{Error, Result};

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
}
