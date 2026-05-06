//! Filesystem-backed `ObjectStore`. Default for single-user deploys.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs;

use crate::error::{Error, Result};

use super::ObjectStore;

/// Files live under `root/{key}`. `put` writes atomically: stage to a
/// `.tmp` sibling, then `rename` (atomic within the same filesystem).
pub struct LocalFsObjectStore {
    root: PathBuf,
}

impl LocalFsObjectStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        // Canonicalise so the URLs we hand back are absolute and stable
        // — relative paths in `Document.storage_uri` would be a
        // foot-gun on restart from a different CWD.
        let root = std::fs::canonicalize(&root)?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn resolve(&self, key: &str) -> Result<PathBuf> {
        ensure_safe_key(key)?;
        Ok(self.root.join(key))
    }

    fn url_for(&self, abs_path: &Path) -> String {
        // POSIX-style file URL. Good enough for Linux deployments;
        // Windows would need slash normalisation but we don't ship there.
        format!("file://{}", abs_path.display())
    }
}

#[async_trait]
impl ObjectStore for LocalFsObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<String> {
        let target = self.resolve(key)?;
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).await?;
        }
        let tmp = target.with_extension(format!(
            "{}.tmp",
            target
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("part")
        ));
        fs::write(&tmp, &bytes).await?;
        fs::rename(&tmp, &target).await?;
        Ok(self.url_for(&target))
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        let target = self.resolve(key)?;
        let v = fs::read(&target).await?;
        Ok(Bytes::from(v))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let target = self.resolve(key)?;
        match fs::remove_file(&target).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let target = self.resolve(key)?;
        Ok(fs::try_exists(&target).await?)
    }
}

fn ensure_safe_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(Error::InvalidConfig("object key is empty".into()));
    }
    if key.starts_with('/') {
        return Err(Error::InvalidConfig(
            "object key must be relative (no leading slash)".into(),
        ));
    }
    for seg in key.split('/') {
        if seg == ".." {
            return Err(Error::InvalidConfig(
                "object key contains `..` segment".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_safety_rejects_traversal() {
        assert!(ensure_safe_key("a/b/c.pdf").is_ok());
        assert!(ensure_safe_key("/abs/path").is_err());
        assert!(ensure_safe_key("a/../etc/passwd").is_err());
        assert!(ensure_safe_key("").is_err());
    }
}
