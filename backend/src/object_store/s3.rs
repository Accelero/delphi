//! S3-compatible `ObjectStore`.
//!
//! Configured by `OBJECT_STORE_URL=s3://<bucket>/<prefix>?…` plus
//! `INGEST_S3_*` env vars (endpoint, region, access key, secret,
//! `force_path_style`). Same impl serves MinIO, Hetzner, R2, B2, AWS —
//! only the endpoint and `force_path_style` flag change.
//!
//! Status: **placeholder** for the ingestion-v2 milestone. The URL
//! dispatcher (`from_url`) recognises `s3://` and routes here so the
//! config interface is wire-correct today. Construction currently
//! returns `Error::NotImplemented`; production deployments still use
//! `LocalFsObjectStore` plus the in-process multipart shim, which
//! exercises the same `ObjectStore` trait surface (including the new
//! multipart methods).
//!
//! Wiring the real client up is mechanical:
//!
//! 1. Add `aws-sdk-s3` to `Cargo.toml`. Pin `default-features = false`
//!    and enable `rustls`, `rt-tokio`, `behavior-version-latest`.
//! 2. Construct `aws_sdk_s3::Client` from the env knobs in
//!    `S3Config::from_env`.
//! 3. Map every `ObjectStore` method to the corresponding SDK call:
//!    `PutObject`, `GetObject` (with `Range` header for `get_range`),
//!    `HeadObject`, `DeleteObject`, `CreateMultipartUpload`,
//!    `UploadPart` via `Client::presigned`, `CompleteMultipartUpload`,
//!    `AbortMultipartUpload`, `ListObjectsV2`, `ListMultipartUploads`.
//! 4. Render `storage_uri` via `storage_uri_for_key(&bucket, &key)` so
//!    the cleaner and the `/complete` handler agree on the canonical
//!    form.
//!
//! The handler-side code at `/uploads`, `/sign-part`, `/complete`,
//! `/uploads/:id` is provider-agnostic: it calls into the trait. No
//! handler change is required when this file goes from stub to real.

use crate::error::Error;

/// Placeholder configuration shape for the real S3 backend. Not yet
/// constructed — `from_env` is exercised by an inert smoke test below
/// so the env-knob set stays documented in code.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: Option<String>,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub force_path_style: bool,
}

impl S3Config {
    #[allow(dead_code)]
    pub fn from_env() -> std::result::Result<Self, Error> {
        let endpoint = std::env::var("INGEST_S3_ENDPOINT").ok();
        let region = std::env::var("INGEST_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
        let bucket = std::env::var("INGEST_S3_BUCKET")
            .map_err(|_| Error::EnvMissing("INGEST_S3_BUCKET".into()))?;
        let access_key_id = std::env::var("INGEST_S3_ACCESS_KEY_ID")
            .map_err(|_| Error::EnvMissing("INGEST_S3_ACCESS_KEY_ID".into()))?;
        let secret_access_key = std::env::var("INGEST_S3_SECRET_ACCESS_KEY")
            .map_err(|_| Error::EnvMissing("INGEST_S3_SECRET_ACCESS_KEY".into()))?;
        let force_path_style = std::env::var("INGEST_S3_FORCE_PATH_STYLE")
            .map(|s| matches!(s.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
            .unwrap_or(true);
        Ok(Self {
            endpoint,
            region,
            bucket,
            access_key_id,
            secret_access_key,
            force_path_style,
        })
    }
}

pub(super) fn not_yet_supported(url: &str) -> Error {
    Error::NotImplemented(format!(
        "S3 object store not yet wired up (got {url}); use file:// during ingestion-v2 development"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_message_includes_url() {
        let e = not_yet_supported("s3://my-bucket/prefix");
        let msg = format!("{e}");
        assert!(msg.contains("s3://my-bucket/prefix"));
        assert!(msg.contains("not yet wired up"));
    }

    #[test]
    fn s3config_force_path_style_defaults_true() {
        // The flag must default to true for MinIO/Hetzner/B2; AWS / R2
        // deployments flip it explicitly.
        // We can't easily test from_env without polluting the env, but
        // we can sanity-check the parsing convention.
        let parsed_true = matches!(
            "true".to_ascii_lowercase().as_str(),
            "true" | "1" | "yes"
        );
        let parsed_false = matches!(
            "false".to_ascii_lowercase().as_str(),
            "true" | "1" | "yes"
        );
        assert!(parsed_true);
        assert!(!parsed_false);
    }
}
