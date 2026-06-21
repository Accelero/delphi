use aws_credential_types::Credentials;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use delphi_contracts::CompletedUploadPart;

#[derive(Debug, Clone)]
pub struct S3MultipartStore {
    internal: Client,
    bucket: String,
}

#[derive(Debug, Clone)]
struct S3Config {
    endpoint_internal: String,
    region: String,
    bucket: String,
    access_key_id: String,
    secret_access_key: String,
    force_path_style: bool,
}

impl S3MultipartStore {
    pub fn from_env() -> anyhow::Result<Self> {
        let cfg = S3Config::from_env()?;
        Ok(Self {
            internal: build_client(&cfg),
            bucket: cfg.bucket,
        })
    }

    pub async fn complete_multipart_upload(
        &self,
        key: &str,
        multipart_upload_id: &str,
        parts: &[CompletedUploadPart],
    ) -> anyhow::Result<()> {
        let mut sorted = parts.to_vec();
        sorted.sort_by_key(|part| part.part_number);
        let completed = sorted
            .into_iter()
            .map(|part| {
                CompletedPart::builder()
                    .part_number(i32::from(part.part_number))
                    .e_tag(part.etag)
                    .build()
            })
            .collect::<Vec<_>>();
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed))
            .build();
        self.internal
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(multipart_upload_id)
            .multipart_upload(upload)
            .send()
            .await?;
        Ok(())
    }

    pub async fn abort_multipart_upload(
        &self,
        key: &str,
        multipart_upload_id: &str,
    ) -> anyhow::Result<()> {
        match self
            .internal
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(multipart_upload_id)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => {
                if let Some(service_error) = error.as_service_error() {
                    if service_error.is_no_such_upload() {
                        return Ok(());
                    }
                }
                Err(error.into())
            }
        }
    }

    pub async fn object_exists(&self, key: &str) -> anyhow::Result<bool> {
        match self
            .internal
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(error) => {
                if let Some(service_error) = error.as_service_error() {
                    if service_error.is_not_found() {
                        return Ok(false);
                    }
                }
                Err(error.into())
            }
        }
    }
}

impl S3Config {
    fn from_env() -> anyhow::Result<Self> {
        let endpoint_internal = std::env::var("DELPHI_INGEST_S3_ENDPOINT_INTERNAL")?;
        let region =
            std::env::var("DELPHI_INGEST_S3_REGION").unwrap_or_else(|_| "us-east-1".to_owned());
        let bucket = std::env::var("DELPHI_INGEST_S3_BUCKET")?;
        let access_key_id = std::env::var("DELPHI_INGEST_S3_ACCESS_KEY_ID")?;
        let secret_access_key = std::env::var("DELPHI_INGEST_S3_SECRET_ACCESS_KEY")?;
        let force_path_style = std::env::var("DELPHI_INGEST_S3_FORCE_PATH_STYLE")
            .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
            .unwrap_or(true);
        Ok(Self {
            endpoint_internal,
            region,
            bucket,
            access_key_id,
            secret_access_key,
            force_path_style,
        })
    }
}

fn build_client(cfg: &S3Config) -> Client {
    let credentials = Credentials::new(
        cfg.access_key_id.clone(),
        cfg.secret_access_key.clone(),
        None,
        None,
        "delphi-document-worker-s3",
    );
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(cfg.region.clone()))
        .endpoint_url(&cfg.endpoint_internal)
        .force_path_style(cfg.force_path_style)
        .credentials_provider(credentials)
        .build();
    Client::from_conf(config)
}
