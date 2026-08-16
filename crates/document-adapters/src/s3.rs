//! S3 / MinIO adapter.
//!
//! Two clients: one signed against the *internal* endpoint for server-side
//! calls, one against the *public* endpoint for presigning, because a presigned
//! URL is only valid for the host it was signed against.

use std::time::Duration;

use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart as S3CompletedPart};
use aws_sdk_s3::Client;
use chrono::{DateTime, TimeZone, Utc};
use delphi_document_app::{
    BlobError, BlobErrorKind, BlobHead, BlobStore, BoxAsyncRead,
    CompletedPart, PresignedPart, UploadedPart,
};

use crate::config::S3Config;

#[derive(Clone)]
pub struct S3BlobStore {
    internal: Client,
    public: Client,
    bucket: String,
}

impl S3BlobStore {
    pub fn new(config: &S3Config) -> Self {
        Self {
            internal: build_client(config, &config.endpoint_internal),
            public: build_client(config, &config.endpoint_public),
            bucket: config.bucket.clone(),
        }
    }
}

#[async_trait]
impl BlobStore for S3BlobStore {
    async fn begin_multipart(&self, key: &str, content_type: &str) -> Result<String, BlobError> {
        let output = self
            .internal
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .send()
            .await
            .map_err(|error| classify(error, "create multipart upload"))?;
        output
            .upload_id()
            .map(str::to_owned)
            .ok_or_else(|| BlobError::permanent("create_multipart_upload returned no upload id"))
    }

    async fn presign_part(
        &self,
        key: &str,
        upload: &str,
        part: u16,
        ttl: Duration,
    ) -> Result<PresignedPart, BlobError> {
        let presign = PresigningConfig::expires_in(ttl)
            .map_err(|error| BlobError::permanent(format!("presign config: {error}")))?;
        let request = self
            .public
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload)
            .part_number(i32::from(part))
            .presigned(presign)
            .await
            .map_err(|error| classify(error, "presign upload part"))?;

        Ok(PresignedPart {
            part_number: part,
            // A bearer capability for exactly this method, key, and part
            // number. Never log it.
            url: request.uri().to_string(),
            expires_at: Utc::now()
                + chrono::Duration::from_std(ttl).unwrap_or_else(|_| chrono::Duration::zero()),
        })
    }

    async fn list_parts(
        &self,
        key: &str,
        upload: &str,
    ) -> Result<Option<Vec<UploadedPart>>, BlobError> {
        let mut parts = Vec::new();
        let mut marker: Option<i32> = None;

        // S3 returns at most 1000 parts per call, so this must page or a large
        // upload silently looks like it only has its first 1000 parts.
        loop {
            let mut request = self
                .internal
                .list_parts()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(upload);
            if let Some(marker) = marker {
                request = request.part_number_marker(marker.to_string());
            }
            let output = match request.send().await {
                Ok(output) => output,
                Err(error) => {
                    let classified = classify(error, "list parts");
                    if classified.kind == BlobErrorKind::NoSuchUpload {
                        return Ok(None);
                    }
                    return Err(classified);
                }
            };

            for part in output.parts() {
                parts.push(UploadedPart {
                    part_number: part.part_number().unwrap_or_default().max(0) as u16,
                    etag: part.e_tag().unwrap_or_default().to_owned(),
                    size: part.size().unwrap_or_default().max(0) as u64,
                });
            }

            if output.is_truncated().unwrap_or(false) {
                marker = output
                    .next_part_number_marker()
                    .and_then(|value| value.parse().ok());
                if marker.is_none() {
                    break;
                }
            } else {
                break;
            }
        }

        Ok(Some(parts))
    }

    async fn complete_multipart(
        &self,
        key: &str,
        upload: &str,
        parts: &[CompletedPart],
    ) -> Result<(), BlobError> {
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(
                parts
                    .iter()
                    .map(|part| {
                        S3CompletedPart::builder()
                            .part_number(i32::from(part.part_number))
                            .e_tag(part.etag.clone())
                            .build()
                    })
                    .collect(),
            ))
            .build();

        self.internal
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|error| classify(error, "complete multipart upload"))?;
        Ok(())
    }

    async fn abort_multipart(&self, key: &str, upload: &str) -> Result<(), BlobError> {
        match self
            .internal
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => {
                let classified = classify(error, "abort multipart upload");
                // Already gone is the outcome we wanted.
                if classified.kind == BlobErrorKind::NoSuchUpload {
                    return Ok(());
                }
                Err(classified)
            }
        }
    }

    async fn head(&self, key: &str) -> Result<Option<BlobHead>, BlobError> {
        match self
            .internal
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => Ok(Some(BlobHead {
                byte_size: output.content_length().unwrap_or_default().max(0) as u64,
                content_type: output.content_type().map(str::to_owned),
                last_modified: output
                    .last_modified()
                    .and_then(to_chrono)
                    .unwrap_or_else(Utc::now),
            })),
            Err(error) => {
                let classified = classify(error, "head object");
                if classified.kind == BlobErrorKind::NotFound {
                    return Ok(None);
                }
                Err(classified)
            }
        }
    }

    async fn open_read(&self, key: &str) -> Result<BoxAsyncRead, BlobError> {
        let output = self
            .internal
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| classify(error, "get object"))?;
        // Streamed, not buffered: the scanner must be able to read a 5 TiB
        // object without the worker holding it in memory.
        Ok(Box::pin(output.body.into_async_read()))
    }

    async fn read_prefix(&self, key: &str, len: usize) -> Result<Vec<u8>, BlobError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        // A real `Range` header, not `open_read().take(n)`. Taking the first
        // bytes off a full-object GET makes storage begin streaming the whole
        // body and then abandons the connection mid-transfer; for the
        // multi-gigabyte objects this pipeline is built for that is the
        // difference between one small request and a cancelled large one.
        // A range longer than the object is not an error: S3 clamps it.
        let output = self
            .internal
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(format!("bytes=0-{}", len - 1))
            .send()
            .await
            .map_err(|error| classify(error, "get object prefix"))?;
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|error| BlobError::transient(format!("read object prefix: {error}")))?;
        Ok(bytes.to_vec())
    }

    async fn delete(&self, key: &str) -> Result<(), BlobError> {
        self.internal
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| classify(error, "delete object"))?;
        Ok(())
    }
}

fn to_chrono(time: &aws_sdk_s3::primitives::DateTime) -> Option<DateTime<Utc>> {
    Utc.timestamp_opt(time.secs(), time.subsec_nanos()).single()
}

/// Map an SDK error onto the three things the worker can do about it: give up,
/// retry, or treat the object as already handled.
///
/// This mapping is why `BlobErrorKind` exists — without it the retry decision
/// would live in the use case and drag the AWS SDK inward.
fn classify<E>(error: SdkError<E>, context: &str) -> BlobError
where
    E: std::error::Error + aws_sdk_s3::error::ProvideErrorMetadata + 'static,
{
    let message = format!("{context}: {error}");

    // Transport-level failures never reached S3's logic, so they are always
    // worth retrying.
    let transient = matches!(
        error,
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_)
    );
    if transient {
        return BlobError::new(BlobErrorKind::Transient, message);
    }

    if let SdkError::ServiceError(service) = &error {
        let status = service.raw().status().as_u16();
        let code = service.err().code().unwrap_or_default().to_owned();
        let kind = match code.as_str() {
            "NoSuchUpload" => BlobErrorKind::NoSuchUpload,
            "NoSuchKey" | "NotFound" => BlobErrorKind::NotFound,
            "InvalidPart" | "InvalidPartOrder" | "EntityTooSmall" => BlobErrorKind::InvalidParts,
            "SlowDown" | "RequestTimeout" | "InternalError" | "ServiceUnavailable" => {
                BlobErrorKind::Transient
            }
            _ if status == 404 => BlobErrorKind::NotFound,
            // Throttling and server faults are the retryable HTTP families.
            _ if status == 429 || (500..600).contains(&status) => BlobErrorKind::Transient,
            _ => BlobErrorKind::Permanent,
        };
        return BlobError::new(kind, format!("{message} (code {code}, status {status})"));
    }

    // Construction, serialisation, and response-parsing failures are all
    // deterministic.
    BlobError::new(BlobErrorKind::Permanent, message)
}

fn build_client(config: &S3Config, endpoint: &str) -> Client {
    let credentials = Credentials::new(
        config.access_key_id.clone(),
        config.secret_access_key.clone(),
        None,
        None,
        "delphi-document-s3",
    );
    let built = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(config.region.clone()))
        .endpoint_url(endpoint)
        .force_path_style(config.force_path_style)
        .credentials_provider(credentials)
        .build();
    Client::from_conf(built)
}
