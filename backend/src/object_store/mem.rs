//! In-memory `ObjectStore` for tests. Not used by production code.
//!
//! Includes an in-process multipart-upload shim ported from the
//! (now-deleted) `LocalFsObjectStore`: parts are staged in the in-memory
//! map under a synthetic key until `complete`, which concatenates them in
//! part-number order into the final key. The shim is the only in-process
//! multipart implementation the integration suite uses
//! (`ingestion_uploads.rs`, `upload_session_cross_tenant.rs`), so it
//! keeps those tests running without Docker — matching `testing.md`'s
//! no-testcontainers ethos. The real `s3://` production backend lives in
//! [`super::s3::S3ObjectStore`]; the only MinIO-testcontainer test is
//! `backend/tests/object_store_s3.rs` (gated by `MINIO_TEST_ENDPOINT`).

use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use axum::http::Method;
use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::error::{Error, Result};

use super::access::{AccessGrant, AccessMinter, AccessOp};
use super::multipart::{
    storage_uri_for_key, CompleteOutcome, MultipartEntry, ObjectEntry, ObjectMeta, PartRef,
    PresignedUrl,
};
use super::ObjectStore;

/// Per-process counter that disambiguates synthetic multipart upload ids.
static MPU_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
pub struct MemObjectStore {
    inner: RwLock<HashMap<String, Bytes>>,
    /// Open multipart uploads: `upload_id` → state (final key + staged
    /// parts + initiation timestamp). `complete` assembles the final
    /// object; `abort` drops the state.
    multipart_index: Mutex<HashMap<String, MultipartState>>,
}

struct MultipartState {
    key: String,
    initiated: DateTime<Utc>,
    /// part_number → bytes
    parts: HashMap<u16, Bytes>,
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
        let key = url
            .strip_prefix("mem://")
            .ok_or_else(|| Error::InvalidConfig(format!("not a mem:// URL: {url}")))?;
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

    // ---- multipart shim ----------------------------------------------------

    async fn create_multipart_upload(&self, key: &str, _content_type: &str) -> Result<String> {
        let upload_id = format!(
            "mpu-{}-{}",
            std::process::id(),
            MPU_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        self.multipart_index
            .lock()
            .expect("multipart_index poisoned")
            .insert(
                upload_id.clone(),
                MultipartState {
                    key: key.to_string(),
                    initiated: Utc::now(),
                    parts: HashMap::new(),
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
        // The in-memory shim has no HTTP-signing concept. Return a
        // `mem-multipart://` pseudo-URL so handler tests can round-trip
        // the value without needing a real S3 endpoint.
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
            "mem-multipart://{upload_id}/{part_number}"
        )))
    }

    async fn upload_part_direct(
        &self,
        _key: &str,
        upload_id: &str,
        part_number: u16,
        bytes: Bytes,
    ) -> Result<String> {
        let etag = format!("\"part-{}-{}\"", part_number, bytes.len());
        let mut guard = self
            .multipart_index
            .lock()
            .expect("multipart_index poisoned");
        let state = guard.get_mut(upload_id).ok_or_else(|| {
            Error::InvalidConfig(format!("unknown multipart upload_id: {upload_id}"))
        })?;
        state.parts.insert(part_number, bytes);
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

        let mut sorted = parts.to_vec();
        sorted.sort_by_key(|p| p.part_number);

        let mut combined: Vec<u8> = Vec::new();
        for p in &sorted {
            let bytes = state.parts.get(&p.part_number).ok_or_else(|| {
                Error::InvalidConfig(format!(
                    "missing part {} for upload {}",
                    p.part_number, upload_id
                ))
            })?;
            combined.extend_from_slice(bytes);
        }
        let len = combined.len();
        self.inner
            .write()
            .expect("MemObjectStore poisoned")
            .insert(key.to_string(), Bytes::from(combined));

        let etag = format!("\"mem-{}-{}\"", len, sorted.len());
        Ok(CompleteOutcome {
            etag,
            // Same canonical form the production S3 backend renders, so
            // tests can assert on it via a shared helper.
            storage_uri: format!("mem-multipart://{key}"),
        })
    }

    async fn abort_multipart_upload(&self, _key: &str, upload_id: &str) -> Result<()> {
        self.multipart_index
            .lock()
            .expect("multipart_index poisoned")
            .remove(upload_id);
        Ok(())
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectEntry>> {
        let guard = self.inner.read().expect("MemObjectStore poisoned");
        Ok(guard
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| ObjectEntry {
                key: k.clone(),
                size: v.len() as u64,
                last_modified: None,
            })
            .collect())
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

// Keep the canonical-URI helper referenced so a future production caller
// rendering `mem://` doesn't drift from the S3 form.
#[allow(dead_code)]
fn _link_for_doc(key: &str) -> String {
    storage_uri_for_key("mem", key)
}

/// In-process [`AccessMinter`] for tests. Mints a deterministic
/// `mem-access://<key>?op=…` pseudo-URL embedding the exact key + op the
/// caller asked for, so integration tests can drive the `/view-url`
/// handler (and assert the right key is minted / cross-tenant access is
/// refused before minting) without a real S3 endpoint. The real,
/// HTTP-signing minter is [`super::S3PresignAccess`].
#[derive(Default)]
pub struct MemAccess;

impl MemAccess {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AccessMinter for MemAccess {
    async fn mint(&self, key: &str, op: AccessOp, ttl: Duration) -> Result<AccessGrant> {
        let (suffix, method) = match op {
            AccessOp::Download => ("op=download".to_string(), Method::GET),
            AccessOp::UploadPart {
                upload_id,
                part_number,
            } => (
                format!("op=upload-part&uploadId={upload_id}&partNumber={part_number}"),
                Method::PUT,
            ),
        };
        let expires_at = Utc::now()
            + chrono::Duration::from_std(ttl).unwrap_or_else(|_| chrono::Duration::seconds(0));
        Ok(AccessGrant {
            url: format!("mem-access://{key}?{suffix}"),
            method,
            headers: Vec::new(),
            expires_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn multipart_round_trip() {
        let store = MemObjectStore::new();
        let upload_id = store
            .create_multipart_upload("tenants/test/abc", "application/pdf")
            .await
            .unwrap();

        let etag1 = store
            .upload_part_direct("tenants/test/abc", &upload_id, 1, Bytes::from_static(b"hello "))
            .await
            .unwrap();
        let etag2 = store
            .upload_part_direct("tenants/test/abc", &upload_id, 2, Bytes::from_static(b"world"))
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
        assert!(outcome.storage_uri.starts_with("mem-multipart://"));

        let body = store.get("tenants/test/abc").await.unwrap();
        assert_eq!(&body[..], b"hello world");
    }

    #[tokio::test]
    async fn multipart_abort_drops_state() {
        let store = MemObjectStore::new();
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

    #[tokio::test]
    async fn presign_returns_mem_multipart_url() {
        let store = MemObjectStore::new();
        let upload_id = store
            .create_multipart_upload("k/abc", "application/pdf")
            .await
            .unwrap();
        let url = store
            .presign_upload_part("k/abc", &upload_id, 1, Duration::from_secs(60))
            .await
            .unwrap();
        assert!(url.as_str().starts_with("mem-multipart://"));
    }
}
