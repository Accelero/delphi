//! Filesystem-backed `ObjectStore`. Default for single-user deploys.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs;

use crate::error::{Error, Result};

use super::ObjectStore;

/// Per-process counter that disambiguates tmp filenames when two `put`
/// calls land on the same key concurrently. Combined with `process::id()`
/// it's also unique across processes sharing the same root.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Files live under `root/{key}`. `put` writes atomically: stage to a
/// uniquely-named `.tmp` sibling, then `rename` (atomic within the same
/// filesystem).
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
        let base_ext = target
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("part");
        let seq = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = target.with_extension(format!(
            "{base_ext}.{pid}.{seq}.tmp",
            pid = std::process::id(),
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
    use std::sync::Arc;

    #[test]
    fn key_safety_rejects_traversal() {
        assert!(ensure_safe_key("a/b/c.pdf").is_ok());
        assert!(ensure_safe_key("/abs/path").is_err());
        assert!(ensure_safe_key("a/../etc/passwd").is_err());
        assert!(ensure_safe_key("").is_err());
    }

    #[tokio::test]
    async fn concurrent_put_same_key_all_succeed() {
        // Before the unique-suffix fix, the second writer would clobber
        // the first writer's tmp file mid-flight; one of the two renames
        // then races against a missing source. This test asserts every
        // concurrent put returns Ok, and that the final file contents
        // match one of the writers (rename is last-writer-wins, which
        // is the intended semantic).
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFsObjectStore::new(dir.path()).unwrap());

        let n = 16;
        let mut handles = Vec::with_capacity(n);
        for i in 0..n {
            let store = Arc::clone(&store);
            let payload = format!("writer-{i}");
            handles.push(tokio::spawn(async move {
                store.put("shared.bin", Bytes::from(payload)).await
            }));
        }
        for h in handles {
            h.await.unwrap().expect("concurrent put should succeed");
        }

        let got = store.get("shared.bin").await.unwrap();
        let s = std::str::from_utf8(&got).unwrap();
        assert!(
            s.starts_with("writer-"),
            "final contents should be one writer's payload, got {s:?}"
        );

        // No tmp files should linger — every put renamed its own tmp.
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        let tmp_left: Vec<_> = entries.iter().filter(|n| n.ends_with(".tmp")).collect();
        assert!(tmp_left.is_empty(), "tmp files left behind: {tmp_left:?}");
    }
}
