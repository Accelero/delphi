use aws_credential_types::Credentials;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::Client;
use chrono::{DateTime, Utc};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct UploadGrant {
    pub url: String,
    pub method: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct S3MultipartStore {
    internal: Client,
    public: Client,
    bucket: String,
}

#[derive(Debug, Clone)]
struct S3Config {
    endpoint_internal: String,
    endpoint_public: String,
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
            internal: build_client(&cfg, &cfg.endpoint_internal),
            public: build_client(&cfg, &cfg.endpoint_public),
            bucket: cfg.bucket,
        })
    }

    pub async fn create_multipart_upload(
        &self,
        key: &str,
        content_type: &str,
    ) -> anyhow::Result<String> {
        let output = self
            .internal
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .send()
            .await?;
        output
            .upload_id()
            .map(str::to_owned)
            .ok_or_else(|| anyhow::anyhow!("S3 create_multipart_upload returned no upload id"))
    }

    pub async fn presign_upload_part(
        &self,
        key: &str,
        multipart_upload_id: &str,
        part_number: u16,
        ttl: Duration,
    ) -> anyhow::Result<UploadGrant> {
        let presign = PresigningConfig::expires_in(ttl)?;
        let request = self
            .public
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(multipart_upload_id)
            .part_number(i32::from(part_number))
            .presigned(presign)
            .await?;
        let expires_at = Utc::now()
            + chrono::Duration::from_std(ttl).unwrap_or_else(|_| chrono::Duration::zero());
        Ok(UploadGrant {
            url: request.uri().to_string(),
            method: "PUT".to_owned(),
            expires_at,
        })
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
            Ok(_) => {}
            Err(error) => {
                if let Some(service_error) = error.as_service_error() {
                    if service_error.is_no_such_upload() {
                        return Ok(());
                    }
                }
                return Err(error.into());
            }
        }
        Ok(())
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
        let endpoint_public = std::env::var("DELPHI_INGEST_S3_ENDPOINT_PUBLIC")
            .unwrap_or_else(|_| endpoint_internal.clone());
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
            endpoint_public,
            region,
            bucket,
            access_key_id,
            secret_access_key,
            force_path_style,
        })
    }
}

fn build_client(cfg: &S3Config, endpoint: &str) -> Client {
    let credentials = Credentials::new(
        cfg.access_key_id.clone(),
        cfg.secret_access_key.clone(),
        None,
        None,
        "delphi-ingest-s3",
    );
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(cfg.region.clone()))
        .endpoint_url(endpoint)
        .force_path_style(cfg.force_path_style)
        .credentials_provider(credentials)
        .build();
    Client::from_conf(config)
}
