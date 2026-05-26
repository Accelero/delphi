//! Shared types for the multipart-upload API.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Object metadata returned by `HEAD`.
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub size: u64,
    pub etag: String,
    pub content_type: Option<String>,
    pub last_modified: Option<DateTime<Utc>>,
}

/// One uploaded part's identity: part number (1-indexed, S3 convention)
/// plus the ETag the provider returned after `UploadPart`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartRef {
    pub part_number: u16,
    pub etag: String,
}

/// Outcome of `complete_multipart_upload`.
#[derive(Debug, Clone)]
pub struct CompleteOutcome {
    pub etag: String,
    /// Provider-returned storage URI in the canonical form
    /// (`s3://<bucket>/<key>` or `file://<abs-path>`).
    pub storage_uri: String,
}

/// Listing entry from `list_objects`.
#[derive(Debug, Clone)]
pub struct ObjectEntry {
    pub key: String,
    pub size: u64,
    pub last_modified: Option<DateTime<Utc>>,
}

/// Listing entry from `list_multipart_uploads` — used by the cleaner
/// to abort orphaned in-flight uploads.
#[derive(Debug, Clone)]
pub struct MultipartEntry {
    pub key: String,
    pub upload_id: String,
    pub initiated: Option<DateTime<Utc>>,
}

/// Presigned URL returned by `presign_upload_part`. Wrapped in a
/// newtype so callers can't accidentally pass a different `String`
/// where a presigned URL is expected.
#[derive(Debug, Clone, Serialize)]
pub struct PresignedUrl(pub String);

impl PresignedUrl {
    pub fn into_inner(self) -> String {
        self.0
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Canonical `storage_uri` for a given bucket+key. Used by the
/// `/complete` handler and the cleaner so both render the same string.
///
/// Format: `s3://<bucket>/<key>` — no querystring, no leading slash on
/// the key.
pub fn storage_uri_for_key(bucket: &str, key: &str) -> String {
    let key = key.strip_prefix('/').unwrap_or(key);
    format!("s3://{bucket}/{key}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_form_round_trips() {
        assert_eq!(
            storage_uri_for_key("delphi", "tenants/test/abc"),
            "s3://delphi/tenants/test/abc"
        );
        // Leading slash on the key is stripped — never present in the
        // canonical form.
        assert_eq!(
            storage_uri_for_key("delphi", "/tenants/test/abc"),
            "s3://delphi/tenants/test/abc"
        );
    }
}
