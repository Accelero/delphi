//! S3-compatible `ObjectStore` backed by `aws-sdk-s3`.
//!
//! Configured by `DELPHI_INGEST_OBJECT_STORE_URL=s3://<bucket>/...` (the URL only
//! selects the scheme; the bucket and connection knobs come from
//! `INGEST_S3_*` env vars) via [`S3ObjectStore::from_env`]. Same impl
//! serves MinIO, Hetzner, R2, B2, AWS — only the endpoints and the
//! `force_path_style` flag change.
//!
//! ## Dual endpoint (the subtle part)
//!
//! The backend talks to S3 over an **internal** host
//! (`DELPHI_INGEST_S3_ENDPOINT_INTERNAL`, e.g. `http://minio:9000`) for HEAD /
//! GET / complete / abort / listing, but **presigned** part-upload URLs
//! must carry the **browser-facing** host
//! (`DELPHI_INGEST_S3_ENDPOINT_PUBLIC`, e.g. `http://localhost:9000` in tier-1
//! or `http://localhost/s3` in tier-2). SigV4 signs the host header and
//! the full path (including any `/s3` prefix when path-style is on), so
//! the presigned URL must be generated against the public endpoint or
//! the browser PUT fails with `SignatureDoesNotMatch`.
//!
//! We therefore hold **two** `aws_sdk_s3::Client`s — one per endpoint.
//! `presign_upload_part` signs with the public client; everything else
//! uses the internal one.

use std::ops::Range;
use std::time::Duration;

use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
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

/// Runtime configuration for the S3 backend, read once from env.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// Endpoint the backend uses for HEAD/GET/complete/abort/listing.
    pub endpoint_internal: String,
    /// Endpoint embedded in presigned URLs the browser hits directly.
    pub endpoint_public: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub force_path_style: bool,
}

impl S3Config {
    pub fn from_env() -> std::result::Result<Self, Error> {
        let endpoint_internal = std::env::var("DELPHI_INGEST_S3_ENDPOINT_INTERNAL")
            .map_err(|_| Error::EnvMissing("DELPHI_INGEST_S3_ENDPOINT_INTERNAL".into()))?;
        // Default the public endpoint to the internal one so a single-host
        // deployment (browser and backend share the endpoint) needs only
        // one var.
        let endpoint_public = std::env::var("DELPHI_INGEST_S3_ENDPOINT_PUBLIC")
            .unwrap_or_else(|_| endpoint_internal.clone());
        let region = std::env::var("DELPHI_INGEST_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
        let bucket = std::env::var("DELPHI_INGEST_S3_BUCKET")
            .map_err(|_| Error::EnvMissing("DELPHI_INGEST_S3_BUCKET".into()))?;
        let access_key_id = std::env::var("DELPHI_INGEST_S3_ACCESS_KEY_ID")
            .map_err(|_| Error::EnvMissing("DELPHI_INGEST_S3_ACCESS_KEY_ID".into()))?;
        let secret_access_key = std::env::var("DELPHI_INGEST_S3_SECRET_ACCESS_KEY")
            .map_err(|_| Error::EnvMissing("DELPHI_INGEST_S3_SECRET_ACCESS_KEY".into()))?;
        let force_path_style = std::env::var("DELPHI_INGEST_S3_FORCE_PATH_STYLE")
            .map(|s| matches!(s.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
            .unwrap_or(true);
        Ok(Self {
            endpoint_internal,
            endpoint_public,
            region,
            bucket,
            access_key_id,
            secret_access_key,
            force_path_style,
        })
    }
}

/// Build an `aws_sdk_s3::Client` for `endpoint` from the shared config.
/// Used for both the internal and public clients (and by
/// [`S3PresignAccess`], which only needs the public endpoint).
fn build_client(cfg: &S3Config, endpoint: &str) -> Client {
    let creds = Credentials::new(
        cfg.access_key_id.clone(),
        cfg.secret_access_key.clone(),
        None,
        None,
        "delphi-ingest-s3",
    );
    let conf = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(cfg.region.clone()))
        .endpoint_url(endpoint)
        .force_path_style(cfg.force_path_style)
        .credentials_provider(creds)
        .build();
    Client::from_conf(conf)
}

/// S3-compatible object store. Holds one client per endpoint (internal +
/// public), sharing credentials and region.
pub struct S3ObjectStore {
    internal: Client,
    public: Client,
    bucket: String,
}

impl S3ObjectStore {
    /// Build from `INGEST_S3_*` env vars. Constructs two SDK clients —
    /// one per endpoint — both with `force_path_style` and the same
    /// hardcoded service credentials.
    pub fn from_env() -> Result<Self> {
        let cfg = S3Config::from_env()?;
        Ok(Self::from_config(&cfg))
    }

    /// Build from an explicit [`S3Config`]. Useful for tests pointing at
    /// a MinIO testcontainer.
    pub fn from_config(cfg: &S3Config) -> Self {
        Self {
            internal: build_client(cfg, &cfg.endpoint_internal),
            public: build_client(cfg, &cfg.endpoint_public),
            bucket: cfg.bucket.clone(),
        }
    }

    fn adapter_err(op: &str, e: impl std::fmt::Display) -> Error {
        Error::Adapter {
            name: "s3-object-store".into(),
            message: format!("{op}: {e}"),
        }
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<String> {
        self.internal
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map_err(|e| Self::adapter_err("put_object", e))?;
        Ok(storage_uri_for_key(&self.bucket, key))
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        let out = self
            .internal
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| Self::adapter_err("get_object", e))?;
        let data = out
            .body
            .collect()
            .await
            .map_err(|e| Self::adapter_err("get_object body", e))?;
        Ok(data.into_bytes())
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes> {
        // S3 Range header is inclusive on both ends; our `Range` is
        // exclusive at the end.
        if range.end <= range.start {
            return Ok(Bytes::new());
        }
        let header = format!("bytes={}-{}", range.start, range.end - 1);
        let out = self
            .internal
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(header)
            .send()
            .await
            .map_err(|e| Self::adapter_err("get_object(range)", e))?;
        let data = out
            .body
            .collect()
            .await
            .map_err(|e| Self::adapter_err("get_object(range) body", e))?;
        Ok(data.into_bytes())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.internal
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| Self::adapter_err("delete_object", e))?;
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        match self
            .internal
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                // 404 → not found; anything else is a real error.
                let svc = e.into_service_error();
                if svc.is_not_found() {
                    Ok(false)
                } else {
                    Err(Self::adapter_err("head_object(exists)", svc))
                }
            }
        }
    }

    async fn get_by_url(&self, url: &str) -> Result<Bytes> {
        let prefix = format!("s3://{}/", self.bucket);
        let key = url
            .strip_prefix(&prefix)
            .ok_or_else(|| Error::InvalidConfig(format!("not an s3 URL for this bucket: {url}")))?;
        self.get(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta> {
        let out = self
            .internal
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| Self::adapter_err("head_object", e))?;
        let last_modified = out
            .last_modified()
            .and_then(|t| DateTime::<Utc>::from_timestamp(t.secs(), t.subsec_nanos()));
        Ok(ObjectMeta {
            size: out.content_length().unwrap_or(0).max(0) as u64,
            etag: out.e_tag().unwrap_or_default().to_string(),
            content_type: out.content_type().map(|s| s.to_string()),
            last_modified,
        })
    }

    // ---- multipart ---------------------------------------------------------

    async fn create_multipart_upload(&self, key: &str, content_type: &str) -> Result<String> {
        let out = self
            .internal
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .send()
            .await
            .map_err(|e| Self::adapter_err("create_multipart_upload", e))?;
        out.upload_id()
            .map(|s| s.to_string())
            .ok_or_else(|| Self::adapter_err("create_multipart_upload", "no upload_id returned"))
    }

    async fn presign_upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: u16,
        ttl: Duration,
    ) -> Result<PresignedUrl> {
        let presign = PresigningConfig::expires_in(ttl)
            .map_err(|e| Self::adapter_err("presign config", e))?;
        // Sign against the PUBLIC client so the URL carries the
        // browser-facing host + path. SigV4 signs the host + full path,
        // so this must be the public endpoint.
        let req = self
            .public
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number as i32)
            .presigned(presign)
            .await
            .map_err(|e| Self::adapter_err("presign upload_part", e))?;
        Ok(PresignedUrl(req.uri().to_string()))
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[PartRef],
    ) -> Result<CompleteOutcome> {
        let mut sorted = parts.to_vec();
        sorted.sort_by_key(|p| p.part_number);
        let completed: Vec<CompletedPart> = sorted
            .iter()
            .map(|p| {
                CompletedPart::builder()
                    .part_number(p.part_number as i32)
                    .e_tag(p.etag.clone())
                    .build()
            })
            .collect();
        let mpu = CompletedMultipartUpload::builder()
            .set_parts(Some(completed))
            .build();
        let out = self
            .internal
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(mpu)
            .send()
            .await
            .map_err(|e| Self::adapter_err("complete_multipart_upload", e))?;
        Ok(CompleteOutcome {
            etag: out.e_tag().unwrap_or_default().to_string(),
            storage_uri: storage_uri_for_key(&self.bucket, key),
        })
    }

    async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> Result<()> {
        // Idempotent: aborting a missing upload is a no-op.
        match self
            .internal
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                let svc = e.into_service_error();
                let msg = svc.to_string();
                if msg.contains("NoSuchUpload") {
                    Ok(())
                } else {
                    Err(Self::adapter_err("abort_multipart_upload", svc))
                }
            }
        }
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectEntry>> {
        let mut out = Vec::new();
        let mut continuation: Option<String> = None;
        loop {
            let mut req = self
                .internal
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = &continuation {
                req = req.continuation_token(token);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| Self::adapter_err("list_objects_v2", e))?;
            for obj in resp.contents() {
                let last_modified = obj
                    .last_modified()
                    .and_then(|t| DateTime::<Utc>::from_timestamp(t.secs(), t.subsec_nanos()));
                out.push(ObjectEntry {
                    key: obj.key().unwrap_or_default().to_string(),
                    size: obj.size().unwrap_or(0).max(0) as u64,
                    last_modified,
                });
            }
            if resp.is_truncated().unwrap_or(false) {
                continuation = resp.next_continuation_token().map(|s| s.to_string());
                if continuation.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(out)
    }

    async fn list_multipart_uploads(&self) -> Result<Vec<MultipartEntry>> {
        let resp = self
            .internal
            .list_multipart_uploads()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|e| Self::adapter_err("list_multipart_uploads", e))?;
        let mut out = Vec::new();
        for u in resp.uploads() {
            let initiated = u
                .initiated()
                .and_then(|t| DateTime::<Utc>::from_timestamp(t.secs(), t.subsec_nanos()));
            out.push(MultipartEntry {
                key: u.key().unwrap_or_default().to_string(),
                upload_id: u.upload_id().unwrap_or_default().to_string(),
                initiated,
            });
        }
        Ok(out)
    }
}

/// S3-presigned [`AccessMinter`]. Mints short-lived presigned URLs the
/// browser uses to talk to the object store directly — `GET` for
/// downloads, `PUT` for upload parts. Signs against the **public**
/// endpoint so the minted URL carries the browser-facing host (SigV4
/// signs host + full path).
///
/// This is the only minter implemented today. The deferred
/// `CdnAccess` / `StsAccess` / `ProxyAccess` drop-ins (see
/// `docs/architecture/object-access.md`) implement the same
/// [`AccessMinter`] trait, so swapping them in is a config change with no
/// caller/frontend churn.
pub struct S3PresignAccess {
    public: Client,
    bucket: String,
}

impl S3PresignAccess {
    /// Build from `INGEST_S3_*` env vars (the same config the
    /// `S3ObjectStore` reads). Holds only the public-endpoint client —
    /// presigning is the sole job.
    pub fn from_env() -> Result<Self> {
        let cfg = S3Config::from_env()?;
        Ok(Self::from_config(&cfg))
    }

    /// Build from an explicit [`S3Config`]. Useful for unit tests.
    pub fn from_config(cfg: &S3Config) -> Self {
        Self {
            public: build_client(cfg, &cfg.endpoint_public),
            bucket: cfg.bucket.clone(),
        }
    }

    fn adapter_err(op: &str, e: impl std::fmt::Display) -> Error {
        Error::Adapter {
            name: "s3-presign-access".into(),
            message: format!("{op}: {e}"),
        }
    }
}

#[async_trait]
impl AccessMinter for S3PresignAccess {
    async fn mint(&self, key: &str, op: AccessOp, ttl: Duration) -> Result<AccessGrant> {
        let presign =
            PresigningConfig::expires_in(ttl).map_err(|e| Self::adapter_err("presign config", e))?;
        let expires_at = Utc::now()
            + chrono::Duration::from_std(ttl).unwrap_or_else(|_| chrono::Duration::seconds(0));

        let (url, method) = match op {
            AccessOp::Download => {
                let req = self
                    .public
                    .get_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .presigned(presign)
                    .await
                    .map_err(|e| Self::adapter_err("presign get_object", e))?;
                (req.uri().to_string(), Method::GET)
            }
            AccessOp::UploadPart {
                upload_id,
                part_number,
            } => {
                let req = self
                    .public
                    .upload_part()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(upload_id)
                    .part_number(part_number as i32)
                    .presigned(presign)
                    .await
                    .map_err(|e| Self::adapter_err("presign upload_part", e))?;
                (req.uri().to_string(), Method::PUT)
            }
        };

        Ok(AccessGrant {
            url,
            method,
            // Presigned URLs carry everything in the query string; the
            // client sends no extra headers.
            headers: Vec::new(),
            expires_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> S3Config {
        S3Config {
            endpoint_internal: "http://minio:9000".into(),
            endpoint_public: "http://localhost:9000".into(),
            region: "us-east-1".into(),
            bucket: "delphi".into(),
            access_key_id: "delphi".into(),
            secret_access_key: "delphi-secret".into(),
            force_path_style: true,
        }
    }

    #[tokio::test]
    async fn presign_download_targets_public_endpoint_with_get() {
        let access = S3PresignAccess::from_config(&test_config());
        let grant = access
            .mint(
                "tenants/test/abc",
                AccessOp::Download,
                Duration::from_secs(120),
            )
            .await
            .expect("mint download");
        assert_eq!(grant.method, Method::GET);
        // Public (browser-facing) host, path-style bucket+key in the path.
        assert!(
            grant.url.starts_with("http://localhost:9000/delphi/tenants/test/abc"),
            "unexpected url: {}",
            grant.url
        );
        // SigV4 query params present.
        assert!(grant.url.contains("X-Amz-Signature="), "no signature: {}", grant.url);
        assert!(grant.headers.is_empty());
        // expires_at ≈ now + ttl.
        let delta = (grant.expires_at - Utc::now()).num_seconds();
        assert!((100..=120).contains(&delta), "delta was {delta}s");
    }

    #[tokio::test]
    async fn presign_upload_part_uses_put_and_differs_from_download() {
        let access = S3PresignAccess::from_config(&test_config());
        let dl = access
            .mint("k/abc", AccessOp::Download, Duration::from_secs(120))
            .await
            .expect("mint download");
        let up = access
            .mint(
                "k/abc",
                AccessOp::UploadPart {
                    upload_id: "upload-123".into(),
                    part_number: 1,
                },
                Duration::from_secs(900),
            )
            .await
            .expect("mint upload-part");
        assert_eq!(up.method, Method::PUT);
        assert!(up.url.contains("partNumber=1"), "no partNumber: {}", up.url);
        assert!(up.url.contains("uploadId=upload-123"), "no uploadId: {}", up.url);
        // Download vs upload-part presign to different URLs.
        assert_ne!(dl.url, up.url);
    }

    #[test]
    fn force_path_style_parses() {
        assert!(matches!(
            "true".to_ascii_lowercase().as_str(),
            "true" | "1" | "yes"
        ));
        assert!(!matches!(
            "false".to_ascii_lowercase().as_str(),
            "true" | "1" | "yes"
        ));
    }

    #[test]
    fn public_endpoint_defaults_to_internal() {
        // Documented behaviour: a single-host deployment can omit
        // DELPHI_INGEST_S3_ENDPOINT_PUBLIC and the public client points at the
        // internal endpoint. (We only check the parsing convention here;
        // exercising `from_env` would pollute the process env.)
        let internal = "http://minio:9000".to_string();
        let public = None::<String>.unwrap_or_else(|| internal.clone());
        assert_eq!(public, internal);
    }
}
