//! Filesystem-backed `ObjectStore`. Default for single-user deploys.
//!
//! Includes an in-process multipart-upload shim: parts are staged under
//! `root/.multipart/<upload_id>/<part_number>` until `complete`, which
//! concatenates them in part-number order into the final key with a
//! single atomic rename. The shim is integration-test plumbing only —
//! production direct-to-storage uploads target an S3-compatible
//! provider via [`super::s3::S3ObjectStore`].

use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tokio::fs;

use crate::error::{Error, Result};

use super::multipart::{
    storage_uri_for_key, CompleteOutcome, MultipartEntry, ObjectEntry, ObjectMeta, PartRef,
    PresignedUrl,
};
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
    /// In-process registry of open multipart uploads. Maps `upload_id`
    /// to the final `key` plus initiation timestamp, so `complete` and
    /// `abort` can locate the staging dir and assemble the final object
    /// even when the caller doesn't re-supply the key (matches S3's
    /// API, which only needs `upload_id`).
    multipart_index: Mutex<HashMap<String, MultipartState>>,
}

struct MultipartState {
    key: String,
    initiated: DateTime<Utc>,
}

impl LocalFsObjectStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        // Canonicalise so the URLs we hand back are absolute and stable
        // — relative paths in `Document.storage_uri` would be a
        // foot-gun on restart from a different CWD.
        let root = std::fs::canonicalize(&root)?;
        Ok(Self {
            root,
            multipart_index: Mutex::new(HashMap::new()),
        })
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

    fn multipart_dir(&self, upload_id: &str) -> PathBuf {
        self.root.join(".multipart").join(upload_id)
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

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes> {
        // No streaming reader: load the whole file then slice. The
        // multipart shim isn't a performance path — production validators
        // run against S3 with a real ranged GET.
        let full = self.get(key).await?;
        let start = range.start as usize;
        let end = (range.end as usize).min(full.len());
        if start >= full.len() {
            return Ok(Bytes::new());
        }
        Ok(full.slice(start..end))
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

    async fn get_by_url(&self, url: &str) -> Result<Bytes> {
        let rest = url
            .strip_prefix("file://")
            .ok_or_else(|| Error::InvalidConfig(format!("not a file:// URL: {url}")))?;
        let abs = PathBuf::from(rest);
        // Constrain reads to under `root` to defeat any storage_uri that
        // wandered outside it (corrupt row, traversal-by-rewrite).
        let canonical = std::fs::canonicalize(&abs)
            .map_err(|_| Error::InvalidConfig(format!("object not found: {url}")))?;
        if !canonical.starts_with(&self.root) {
            return Err(Error::InvalidConfig(format!(
                "object URL escapes store root: {url}"
            )));
        }
        let v = fs::read(&canonical).await?;
        Ok(Bytes::from(v))
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta> {
        let target = self.resolve(key)?;
        let md = fs::metadata(&target).await?;
        let modified = md.modified().ok().map(DateTime::<Utc>::from);
        Ok(ObjectMeta {
            size: md.len(),
            // Synthetic ETag: size + mtime. Stable per content
            // generation, opaque to callers — they only compare for
            // equality.
            etag: format!(
                "\"{}-{}\"",
                md.len(),
                modified.map(|m| m.timestamp()).unwrap_or(0)
            ),
            content_type: None,
            last_modified: modified,
        })
    }

    async fn create_multipart_upload(&self, key: &str, _content_type: &str) -> Result<String> {
        ensure_safe_key(key)?;
        let upload_id = format!(
            "mpu-{}-{}",
            std::process::id(),
            TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let staging = self.multipart_dir(&upload_id);
        fs::create_dir_all(&staging).await?;
        self.multipart_index
            .lock()
            .expect("multipart_index poisoned")
            .insert(
                upload_id.clone(),
                MultipartState {
                    key: key.to_string(),
                    initiated: Utc::now(),
                },
            );
        Ok(upload_id)
    }

    async fn presign_upload_part(
        &self,
        _key: &str,
        upload_id: &str,
        part_number: u16,
        _ttl: Duration,
    ) -> Result<PresignedUrl> {
        // The local-FS shim has no HTTP-signing concept. Return a
        // `local-multipart://` pseudo-URL so handler tests can
        // round-trip the value without needing a real S3 endpoint.
        if !self
            .multipart_index
            .lock()
            .expect("multipart_index poisoned")
            .contains_key(upload_id)
        {
            return Err(Error::InvalidConfig(format!(
                "unknown multipart upload_id: {upload_id}"
            )));
        }
        Ok(PresignedUrl(format!(
            "local-multipart://{upload_id}/{part_number}"
        )))
    }

    async fn upload_part_direct(
        &self,
        _key: &str,
        upload_id: &str,
        part_number: u16,
        bytes: Bytes,
    ) -> Result<String> {
        if !self
            .multipart_index
            .lock()
            .expect("multipart_index poisoned")
            .contains_key(upload_id)
        {
            return Err(Error::InvalidConfig(format!(
                "unknown multipart upload_id: {upload_id}"
            )));
        }
        let staging = self.multipart_dir(upload_id);
        let part_path = staging.join(format!("{part_number:05}.part"));
        fs::write(&part_path, &bytes).await?;
        // Synthetic ETag: byte length + simple hash. Real S3 returns
        // the MD5 of the part; tests only compare for equality.
        let etag = format!("\"part-{}-{}\"", part_number, bytes.len());
        Ok(etag)
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[PartRef],
    ) -> Result<CompleteOutcome> {
        let state = self
            .multipart_index
            .lock()
            .expect("multipart_index poisoned")
            .remove(upload_id)
            .ok_or_else(|| {
                Error::InvalidConfig(format!("unknown multipart upload_id: {upload_id}"))
            })?;
        if state.key != key {
            return Err(Error::InvalidConfig(format!(
                "multipart key mismatch: upload was opened on {} but complete asked for {}",
                state.key, key
            )));
        }

        let staging = self.multipart_dir(upload_id);
        let mut sorted = parts.to_vec();
        sorted.sort_by_key(|p| p.part_number);

        let target = self.resolve(key)?;
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).await?;
        }
        let seq = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = target.with_extension(format!("mpu.{pid}.{seq}.tmp", pid = std::process::id(),));
        let mut combined: Vec<u8> = Vec::new();
        for p in &sorted {
            let part_path = staging.join(format!("{:05}.part", p.part_number));
            let bytes = fs::read(&part_path).await.map_err(|e| {
                Error::InvalidConfig(format!(
                    "missing part {} for upload {}: {}",
                    p.part_number, upload_id, e
                ))
            })?;
            combined.extend_from_slice(&bytes);
        }
        fs::write(&tmp, &combined).await?;
        fs::rename(&tmp, &target).await?;
        // Best-effort: drop the staging directory.
        let _ = fs::remove_dir_all(&staging).await;

        let etag = format!("\"local-{}-{}\"", combined.len(), sorted.len());
        Ok(CompleteOutcome {
            etag,
            storage_uri: self.url_for(&target),
        })
    }

    async fn abort_multipart_upload(&self, _key: &str, upload_id: &str) -> Result<()> {
        self.multipart_index
            .lock()
            .expect("multipart_index poisoned")
            .remove(upload_id);
        let staging = self.multipart_dir(upload_id);
        match fs::remove_dir_all(&staging).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectEntry>> {
        ensure_safe_key(prefix).ok();
        let base = self.root.join(prefix);
        let mut out = Vec::new();
        if !fs::try_exists(&base).await? {
            return Ok(out);
        }
        let mut stack = vec![base.clone()];
        while let Some(dir) = stack.pop() {
            let mut rd = match fs::read_dir(&dir).await {
                Ok(rd) => rd,
                Err(_) => continue,
            };
            while let Some(ent) = rd.next_entry().await? {
                let p = ent.path();
                // Skip the multipart staging area entirely.
                if p.file_name().map(|n| n == ".multipart").unwrap_or(false) {
                    continue;
                }
                let md = match ent.metadata().await {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                if md.is_dir() {
                    stack.push(p);
                } else {
                    let rel = p
                        .strip_prefix(&self.root)
                        .map(|r| r.to_string_lossy().to_string())
                        .unwrap_or_default();
                    out.push(ObjectEntry {
                        key: rel,
                        size: md.len(),
                        last_modified: md.modified().ok().map(DateTime::<Utc>::from),
                    });
                }
            }
        }
        Ok(out)
    }

    async fn list_multipart_uploads(&self) -> Result<Vec<MultipartEntry>> {
        let guard = self
            .multipart_index
            .lock()
            .expect("multipart_index poisoned");
        Ok(guard
            .iter()
            .map(|(id, st)| MultipartEntry {
                key: st.key.clone(),
                upload_id: id.clone(),
                initiated: Some(st.initiated),
            })
            .collect())
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

// Suppress unused-import warning on local helper that is only called
// inside non-test paths once the bigger module compiles.
#[allow(dead_code)]
fn _link_for_doc(_root: &Path, key: &str) -> String {
    storage_uri_for_key("local", key)
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

    #[tokio::test]
    async fn multipart_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsObjectStore::new(dir.path()).unwrap();

        let upload_id = store
            .create_multipart_upload("tenants/test/abc", "application/pdf")
            .await
            .unwrap();

        let etag1 = store
            .upload_part_direct(
                "tenants/test/abc",
                &upload_id,
                1,
                Bytes::from_static(b"hello "),
            )
            .await
            .unwrap();
        let etag2 = store
            .upload_part_direct(
                "tenants/test/abc",
                &upload_id,
                2,
                Bytes::from_static(b"world"),
            )
            .await
            .unwrap();

        let outcome = store
            .complete_multipart_upload(
                "tenants/test/abc",
                &upload_id,
                &[
                    PartRef {
                        part_number: 1,
                        etag: etag1,
                    },
                    PartRef {
                        part_number: 2,
                        etag: etag2,
                    },
                ],
            )
            .await
            .unwrap();
        assert!(outcome.storage_uri.starts_with("file://"));

        let body = store.get("tenants/test/abc").await.unwrap();
        assert_eq!(&body[..], b"hello world");
    }

    #[tokio::test]
    async fn multipart_abort_drops_state() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsObjectStore::new(dir.path()).unwrap();
        let upload_id = store
            .create_multipart_upload("tenants/test/abc", "application/pdf")
            .await
            .unwrap();
        store
            .upload_part_direct("tenants/test/abc", &upload_id, 1, Bytes::from_static(b"x"))
            .await
            .unwrap();
        store
            .abort_multipart_upload("tenants/test/abc", &upload_id)
            .await
            .unwrap();
        // Subsequent complete should fail (upload_id forgotten).
        let res = store
            .complete_multipart_upload("tenants/test/abc", &upload_id, &[])
            .await;
        assert!(res.is_err());
    }
}
