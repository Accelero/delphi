//! S3 `ObjectStore` — placeholder for slice 2.
//!
//! The URL dispatcher (`from_url`) recognises `s3://` and routes here so
//! the config interface (`OBJECT_STORE_URL=s3://…`) is wire-correct
//! today. Construction returns `Error::NotImplemented`; the day we want
//! S3 in production, this file gets a real impl with no caller changes.

use crate::error::Error;

pub(super) fn not_yet_supported(url: &str) -> Error {
    Error::NotImplemented(format!(
        "S3 object store not yet implemented (got {url}); use file:// for now"
    ))
}

// When implementing for real:
//
// pub struct S3ObjectStore {
//     bucket: String,
//     prefix: String,
//     client: aws_sdk_s3::Client,
// }
//
// impl S3ObjectStore {
//     pub async fn from_url(url: &str) -> Result<Self> { ... }
// }
//
// #[async_trait]
// impl ObjectStore for S3ObjectStore { ... }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_message_includes_url() {
        let e = not_yet_supported("s3://my-bucket/prefix");
        let msg = format!("{e}");
        assert!(msg.contains("s3://my-bucket/prefix"));
        assert!(msg.contains("not yet implemented"));
    }
}
