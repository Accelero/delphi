//! Loopback HTTP client the scheduler uses to call its own
//! `/api/ingestion/documents` endpoint.
//!
//! Same trust boundary as user requests: the call carries a bearer JWT
//! produced by a [`ServiceIdentity`] and is validated by the same
//! [`crate::auth::JwtClaimsExtractor`] every browser request goes
//! through. The scheduler no longer sits above RBAC — it authenticates
//! into the same pipeline as everything else.

use std::sync::Arc;
use std::time::Duration;

use crate::auth::ServiceIdentity;
use crate::error::{Error, Result};
use crate::ingestion::{IngestOutcome, IngestRequestBody};

pub struct IngestApiClient {
    http: reqwest::Client,
    base_url: String,
    identity: Arc<dyn ServiceIdentity>,
}

const ADAPTER_NAME: &str = "ingest-api";

impl IngestApiClient {
    pub fn new(base_url: String, identity: Arc<dyn ServiceIdentity>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .expect("reqwest client builds");
        Self {
            http,
            base_url,
            identity,
        }
    }

    pub async fn ingest(&self, body: IngestRequestBody) -> Result<IngestOutcome> {
        let token = self
            .identity
            .fresh_token()
            .await
            .map_err(|e| Error::Adapter {
                name: ADAPTER_NAME.into(),
                message: format!("service identity refresh failed: {e}"),
            })?;
        let url = format!("{}/api/ingestion/documents", self.base_url);
        let resp = self
            .http
            .post(&url)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::Adapter {
                name: ADAPTER_NAME.into(),
                message: format!("ingest API {status}: {text}"),
            });
        }
        let outcome = resp
            .json::<IngestOutcome>()
            .await
            .map_err(Error::Http)?;
        Ok(outcome)
    }
}
