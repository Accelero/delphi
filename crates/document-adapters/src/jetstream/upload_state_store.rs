//! The upload's whole state, in one NATS KV bucket.
//!
//! The bucket's `max_age` is the upload's lifetime: there is no sweeper, no
//! retention job, and no row anywhere else. When the TTL elapses the upload
//! stops existing, and the worker's expiry path reclaims whatever it left in
//! object storage.

use async_nats::jetstream::kv::{CreateErrorKind, Store, UpdateErrorKind};
use async_trait::async_trait;
use delphi_document_app::{ContextError, StoredUpload, UploadState, UploadStateStore};

#[derive(Clone)]
pub struct KvUploadStateStore {
    bucket: Store,
}

impl KvUploadStateStore {
    pub fn new(bucket: Store) -> Self {
        Self { bucket }
    }
}

#[async_trait]
impl UploadStateStore for KvUploadStateStore {
    async fn create(&self, state: &UploadState) -> Result<(), ContextError> {
        let payload = serde_json::to_vec(state)
            .map_err(|error| ContextError::Payload(format!("encode upload state: {error}")))?;

        // `create`, not `put`: a second preflight must never silently rewrite
        // the parameters a client is already slicing to.
        match self.bucket.create(&state.own_key(), payload.into()).await {
            Ok(_) => Ok(()),
            Err(error) if matches!(error.kind(), CreateErrorKind::AlreadyExists) => {
                Err(ContextError::AlreadyExists)
            }
            Err(error) => Err(ContextError::Unavailable(format!(
                "create upload state: {error}"
            ))),
        }
    }

    async fn get(
        &self,
        tenant: &str,
        user: &str,
        upload_id: &str,
    ) -> Result<Option<StoredUpload>, ContextError> {
        // `entry`, not `get`: the revision is what makes the later write a
        // compare-and-swap rather than a last-writer-wins clobber.
        //
        // The key contains the caller, so another user derives a different key
        // and simply finds nothing — a structural 404 with no existence
        // disclosure.
        let key = UploadState::key(tenant, user, upload_id);
        let entry = self
            .bucket
            .entry(&key)
            .await
            .map_err(|error| ContextError::Unavailable(format!("get upload state: {error}")))?;

        let Some(entry) = entry else {
            return Ok(None);
        };
        // A deleted or purged key comes back as a tombstone with an empty
        // value; treating that as a record would decode into garbage.
        if entry.operation != async_nats::jetstream::kv::Operation::Put {
            return Ok(None);
        }

        let state = serde_json::from_slice(&entry.value)
            .map_err(|error| ContextError::Payload(format!("decode upload state: {error}")))?;
        Ok(Some(StoredUpload {
            state,
            revision: entry.revision,
        }))
    }

    async fn update(&self, state: &UploadState, revision: u64) -> Result<u64, ContextError> {
        let payload = serde_json::to_vec(state)
            .map_err(|error| ContextError::Payload(format!("encode upload state: {error}")))?;

        match self
            .bucket
            .update(&state.own_key(), payload.into(), revision)
            .await
        {
            Ok(revision) => Ok(revision),
            // JetStream reports both "someone else wrote" and "the key is
            // gone" as a wrong-last-sequence error. They need different
            // answers — one retries, the other cleans up — so re-read to tell
            // them apart.
            Err(error) if matches!(error.kind(), UpdateErrorKind::WrongLastRevision) => {
                match self.bucket.entry(&state.own_key()).await {
                    Ok(Some(entry))
                        if entry.operation == async_nats::jetstream::kv::Operation::Put =>
                    {
                        Err(ContextError::Conflict)
                    }
                    Ok(_) => Err(ContextError::Expired),
                    Err(error) => Err(ContextError::Unavailable(format!(
                        "re-read upload state after a lost CAS: {error}"
                    ))),
                }
            }
            Err(error) => Err(ContextError::Unavailable(format!(
                "update upload state: {error}"
            ))),
        }
    }

    async fn delete(&self, tenant: &str, user: &str, upload_id: &str) -> Result<(), ContextError> {
        let key = UploadState::key(tenant, user, upload_id);
        self.bucket
            .purge(&key)
            .await
            .map_err(|error| ContextError::Unavailable(format!("purge upload state: {error}")))
    }
}
