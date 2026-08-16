//! The API-facing use cases: preflight, renew, complete, and reads.
//!
//! Nothing here knows about HTTP. Errors come back as [`DocumentError`] and the
//! service layer maps them to status codes.

use std::sync::Arc;
use std::time::Duration;

use delphi_document_domain::{
    part_count, part_size_bytes, validate_metadata_patch, DocState, DocumentState, MetadataPatch,
};

use crate::command::{ConflictPolicy, UploadCompleted};
use crate::cursor::DocumentCursor;
use crate::errors::DocumentError;
use crate::keys::{object_key, upload_completed_command_id};
use crate::ports::{
    BlobStore, Clock, DocumentReadModel, EventStore, IdGen, PresignedPart,
    UploadStateStore, UploadedPart, WorkQueue,
};
use crate::transition;
use crate::upload_state::{UploadMode, UploadState, UploadStatus};

const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// Hard ceiling on `GET /api/documents?limit=`.
///
/// Clamping lives here rather than in the HTTP layer so that the page and its
/// `next` cursor are decided by the same number. When the handler clamped
/// separately the two disagreed, and `limit=500` returned 200 items with
/// `next: null` — a silent truncation that looks exactly like the end of the
/// listing.
pub const MAX_LIST_LIMIT: u32 = 200;

#[derive(Debug, Clone)]
pub struct UploadPolicy {
    /// Short and re-issuable. Deliberately **not** tied to the context TTL: an
    /// earlier design did, which capped every upload at five minutes.
    pub part_url_ttl: Duration,
    /// Largest object this deployment accepts, checked at preflight.
    ///
    /// S3's own limit is 5 TiB, which is not a policy — it is the absence of
    /// one. Every later stage has to read the whole object (the scan streams it
    /// to compute the checksum), so the real bound is what a worker can chew
    /// through inside the redelivery window, not what storage can hold.
    pub max_upload_bytes: u64,
    /// The part size this deployment slices at, when the part cap is not
    /// binding. Server-owned: the client MUST use exactly what preflight
    /// returns, which is this or larger.
    pub part_size_bytes: u64,
}

pub struct DocumentService {
    events: Arc<dyn EventStore>,
    blobs: Arc<dyn BlobStore>,
    uploads: Arc<dyn UploadStateStore>,
    documents: Arc<dyn DocumentReadModel>,
    queue: Arc<dyn WorkQueue>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdGen>,
    policy: UploadPolicy,
}

#[derive(Debug, Clone)]
pub struct PreflightRequest {
    /// Absent = create, present = replace.
    pub document_id: Option<String>,
    pub filename: String,
    pub size: u64,
    pub content_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PreflightResponse {
    pub upload_id: String,
    pub document_id: String,
    pub key: String,
    pub part_size_bytes: u64,
    pub part_count: u16,
}

#[derive(Debug, Clone)]
pub struct RenewRequest {
    pub from_part: Option<u16>,
    pub count: Option<u16>,
}

/// Geometry is not echoed: it is fixed at preflight and cannot change, and
/// each part carries its own `expires_at`.
#[derive(Debug, Clone)]
pub struct RenewResponse {
    pub parts: Vec<PresignedPart>,
}

#[derive(Debug, Clone)]
pub struct UploadedPartsResponse {
    pub part_size_bytes: u64,
    pub part_count: u16,
    pub parts: Vec<UploadedPart>,
}

#[derive(Debug, Clone)]
pub struct CompleteRequest {
    pub if_match: Option<u64>,
    pub on_conflict: ConflictPolicy,
    pub patch: MetadataPatch,
}

/// A page of the listing together with where to resume.
///
/// `next` is computed here, next to the clamp that decided the page size, so a
/// caller cannot pair a full page with a "there is no more" answer.
#[derive(Debug, Clone)]
pub struct DocumentPage {
    pub items: Vec<DocumentState>,
    pub next: Option<DocumentCursor>,
}

impl DocumentService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        events: Arc<dyn EventStore>,
        blobs: Arc<dyn BlobStore>,
        uploads: Arc<dyn UploadStateStore>,
        documents: Arc<dyn DocumentReadModel>,
        queue: Arc<dyn WorkQueue>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdGen>,
        policy: UploadPolicy,
    ) -> Self {
        Self {
            events,
            blobs,
            uploads,
            documents,
            queue,
            clock,
            ids,
            policy,
        }
    }

    // -------------------------------------------------------------- preflight

    /// `POST /api/uploads`. Must be called **before** the client constructs its
    /// uploader: part size is server-owned and browser uploaders fix their
    /// chunk boundaries at construction time.
    pub async fn preflight(
        &self,
        tenant_id: &str,
        user_id: &str,
        request: PreflightRequest,
    ) -> Result<PreflightResponse, DocumentError> {
        let filename = request.filename.trim().to_owned();
        if filename.is_empty() {
            return Err(DocumentError::Invalid("filename cannot be empty".to_owned()));
        }
        // Before the multipart is opened, so an oversized declaration costs
        // nothing. `declared_size` is re-checked against the assembled object
        // at `/complete`, which is what makes this a real cap rather than a
        // hint: a client that lies here is rejected there.
        if request.size > self.policy.max_upload_bytes {
            return Err(DocumentError::TooLarge(format!(
                "file is {} bytes, over the {} byte upload limit",
                request.size, self.policy.max_upload_bytes
            )));
        }
        let part_size = part_size_bytes(request.size, self.policy.part_size_bytes)?;
        let parts_total = part_count(request.size, part_size)?;

        // Authorising here is the point: otherwise a user uploads 400 MB and
        // only then learns they cannot write the target.
        let (document_id, mode) = match request.document_id {
            Some(document_id) => {
                self.resolve_replace_target(tenant_id, &document_id).await?;
                (document_id, UploadMode::Replace)
            }
            None => (self.ids.ulid(), UploadMode::Create),
        };

        let upload_id = self.ids.ulid();
        let key = object_key(tenant_id, &upload_id);
        let content_type = request
            .content_type
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(DEFAULT_CONTENT_TYPE)
            .to_owned();

        let multipart_upload_id = self
            .blobs
            .begin_multipart(&key, &content_type)
            .await
            .map_err(|error| DocumentError::internal("begin multipart", error))?;

        // From here on, every failure must leave no multipart behind. A crash
        // (rather than an error) leaves an empty one, which minio's
        // incomplete-multipart reaper clears.
        let now = self.clock.now();
        let state = UploadState {
            tenant_id: tenant_id.to_owned(),
            owner_user_id: user_id.to_owned(),
            upload_id: upload_id.clone(),
            document_id: document_id.clone(),
            mode,
            storage_key: key.clone(),
            multipart_upload_id: multipart_upload_id.clone(),
            filename: filename.clone(),
            content_type,
            declared_size: request.size,
            part_size_bytes: part_size,
            part_count: parts_total,
            status: UploadStatus::Uploading,
            created_at: now,
            updated_at: now,
        };

        let outcome = self.uploads.create(&state).await;
        match outcome {
            Ok(()) => Ok(PreflightResponse {
                upload_id,
                document_id,
                key,
                part_size_bytes: part_size,
                part_count: parts_total,
            }),
            Err(error) => {
                self.unwind_preflight(&state).await;
                Err(error.into())
            }
        }
    }

    // Preflight presigns nothing, and writes exactly one record.
    //
    // Nothing is presigned because clients sign each part immediately before
    // uploading it, so a batch minted here would be URLs that mostly expire
    // unused — at an HMAC apiece, up to 10 000 of them, on a request the user
    // is waiting on before any byte moves.
    //
    // One record because there used to be two: a KV record for the parameters
    // and a Postgres row for the status. That split is what put two unordered
    // writers on a single upload.

    /// Undo a failed preflight so no stale record can outlive its multipart.
    async fn unwind_preflight(&self, state: &UploadState) {
        if let Err(error) = self
            .uploads
            .delete(&state.tenant_id, &state.owner_user_id, &state.upload_id)
            .await
        {
            tracing::warn!(%error, upload_id = %state.upload_id, "could not delete upload state after failed preflight");
        }
        if let Err(error) = self
            .blobs
            .abort_multipart(&state.storage_key, &state.multipart_upload_id)
            .await
        {
            tracing::warn!(%error, upload_id = %state.upload_id, "could not abort multipart after failed preflight; the incomplete-multipart reaper will clear it");
        }
    }

    /// Existence comes from the **event store**, not the projection: the
    /// projection lags, and a document created seconds ago would 404
    /// spuriously. The projection is consulted only for `state`, and its
    /// absence is treated as "too new to be deleted".
    async fn resolve_replace_target(
        &self,
        tenant_id: &str,
        document_id: &str,
    ) -> Result<(), DocumentError> {
        if self.events.last(tenant_id, document_id).await?.is_none() {
            return Err(DocumentError::NotFound);
        }
        match self.documents.get(tenant_id, document_id).await? {
            Some(document) if document.state == DocState::Deleted => Err(DocumentError::Deleted),
            _ => Ok(()),
        }
    }

    // ------------------------------------------------------------------ parts

    /// `GET /api/uploads/{upload_id}/parts`. What S3 already holds.
    ///
    /// This is the resume half of the upload contract, and browser uploaders
    /// depend on it: a part the client did not upload *in this pass* has an
    /// ETag the client has never seen, and `CompleteMultipartUpload` needs
    /// every ETag. Without this endpoint a client can only ever restart.
    pub async fn uploaded_parts(
        &self,
        tenant_id: &str,
        user_id: &str,
        upload_id: &str,
    ) -> Result<UploadedPartsResponse, DocumentError> {
        let context = self.load_context(tenant_id, user_id, upload_id).await?;
        let Some(uploaded) = self
            .blobs
            .list_parts(&context.storage_key, &context.multipart_upload_id)
            .await
            .map_err(|error| DocumentError::internal("list parts", error))?
        else {
            return Err(DocumentError::Gone);
        };

        // Only parts whose length is exactly what this geometry calls for may
        // be resumed. A part of any other size cannot have come from a correct
        // slicing of this file, and reporting it would let the client skip
        // re-uploading bytes that are wrong. Dropping it here means the client
        // simply uploads it again, which overwrites it.
        let parts = uploaded
            .into_iter()
            .filter(|part| Some(part.size) == expected_part_size(&context, part.part_number))
            .collect();

        Ok(UploadedPartsResponse {
            part_size_bytes: context.part_size_bytes,
            part_count: context.part_count,
            parts,
        })
    }

    // ------------------------------------------------------------------ renew

    /// `POST /api/uploads/{upload_id}/renew`. The part-signing endpoint.
    ///
    /// The normal caller is an uploader signing one part immediately before it
    /// uploads it, which is why the part TTL can stay at 300s while the upload
    /// window is 24h: a URL is used seconds after it is minted.
    pub async fn renew(
        &self,
        tenant_id: &str,
        user_id: &str,
        upload_id: &str,
        request: RenewRequest,
    ) -> Result<RenewResponse, DocumentError> {
        let context = self.load_context(tenant_id, user_id, upload_id).await?;

        // Two different questions share this endpoint, and they want opposite
        // answers about parts already in S3:
        //
        // - `from_part` given: the client is *naming* the parts it wants URLs
        //   for. Honour it even for an uploaded part — an uploader that retries
        //   a PUT whose response it never saw asks for exactly that, and
        //   refusing leaves it with no way to finish.
        // - `from_part` omitted: the client is asking "where do I resume?", so
        //   skipping what is already stored is the whole point.
        //
        // Only the second question needs storage. Signing is pure local
        // computation, and this now runs once per part, so paying a `ListParts`
        // — which pages at 1000 — on the naming path would double the round
        // trips of every upload. A multipart that has gone away is still
        // caught: `GET /parts` runs at the start of every pass.
        let (first, skip): (u16, Vec<u16>) = match request.from_part {
            Some(from) if from >= 1 && from <= context.part_count => (from, Vec::new()),
            Some(_) => {
                return Err(DocumentError::Invalid(format!(
                    "from_part must be between 1 and {}",
                    context.part_count
                )))
            }
            None => {
                let Some(uploaded) = self
                    .blobs
                    .list_parts(&context.storage_key, &context.multipart_upload_id)
                    .await
                    .map_err(|error| DocumentError::internal("list parts", error))?
                else {
                    // The multipart is gone: reaped, aborted, or completed.
                    return Err(DocumentError::Gone);
                };
                let already: Vec<u16> = uploaded.iter().map(|part| part.part_number).collect();
                let first = (1..=context.part_count)
                    .find(|part| !already.contains(part))
                    .unwrap_or(context.part_count);
                (first, already)
            }
        };
        // No batch cap: `count: None` means "every part from here on".
        //
        // There is nothing left for a cap to protect. The part count is already
        // bounded by the geometry — `part_count <= MAX_PARTS`, and in practice
        // `max_upload_bytes / part_size` is far tighter — so a second limit
        // would only be a smaller copy of a bound that already holds.
        //
        // Signing is local computation at roughly 67 microseconds and 578 bytes
        // per part (measured), and the batch is *cheaper* than the equivalent
        // stream of single-part calls: this path verifies the caller and reads
        // the upload record once, where N calls do both N times.
        let last = match request.count {
            Some(count) => first.saturating_add(count.max(1) - 1).min(context.part_count),
            None => context.part_count,
        };

        let parts = self.presign_window(&context, first, last, &skip).await?;

        Ok(RenewResponse { parts })
    }

    async fn presign_window(
        &self,
        context: &UploadState,
        first: u16,
        last: u16,
        skip: &[u16],
    ) -> Result<Vec<PresignedPart>, DocumentError> {
        let mut parts = Vec::new();
        for part_number in first..=last {
            if skip.contains(&part_number) {
                continue;
            }
            let part = self
                .blobs
                .presign_part(
                    &context.storage_key,
                    &context.multipart_upload_id,
                    part_number,
                    self.policy.part_url_ttl,
                )
                .await
                // Never log or surface the URL itself: it is a bearer
                // capability for that method, key, and part number.
                .map_err(|error| DocumentError::internal("presign part", error))?;
            parts.push(part);
        }
        Ok(parts)
    }

    // --------------------------------------------------------------- complete

    /// `POST /api/uploads/{upload_id}/complete`.
    ///
    /// Appends no event and touches no document — it only enqueues the work
    /// that will. `document_id` is not accepted here; it was fixed at
    /// preflight.
    ///
    /// **No parts list either.** The worker asks storage what it holds. A
    /// client-echoed list of ETags was never the integrity check it looked
    /// like: on a resumed upload some of those values came from our own
    /// `GET /parts`, so the client was handing back what we had just told it.
    /// S3 knows authoritatively, so this call carries only intent — the
    /// conflict policy and the metadata patch.
    pub async fn complete(
        &self,
        tenant_id: &str,
        user_id: &str,
        upload_id: &str,
        request: CompleteRequest,
    ) -> Result<(), DocumentError> {
        validate_metadata_patch(&request.patch)?;

        let context = self.load_context(tenant_id, user_id, upload_id).await?;

        if request.if_match.is_some() && context.mode == UploadMode::Create {
            return Err(DocumentError::Invalid(
                "if_match is not valid for a new document".to_owned(),
            ));
        }
        let command = UploadCompleted {
            v: UploadCompleted::contract_version(),
            command_id: upload_completed_command_id(tenant_id, upload_id),
            tenant_id: tenant_id.to_owned(),
            owner_user_id: user_id.to_owned(),
            upload_id: upload_id.to_owned(),
            document_id: context.document_id.clone(),
            mode: context.mode,
            storage_key: context.storage_key.clone(),
            multipart_upload_id: context.multipart_upload_id.clone(),
            filename: context.filename.clone(),
            content_type: context.content_type.clone(),
            declared_size: context.declared_size,
            if_match: request.if_match,
            on_conflict: request.on_conflict,
            patch: request.patch,
            ts: self.clock.now(),
        };

        // Publish before marking the record: the work item is the durable
        // record of the intent, the status is only what the client polls.
        self.queue.publish_upload_completed(command).await?;

        // Which means this write can lose a race with the worker's terminal
        // one, and must. `set_status` refuses to move a terminal record, so a
        // small file that finishes inside this window stays `accepted`.
        transition::set_status(
            &self.uploads,
            tenant_id,
            user_id,
            upload_id,
            UploadStatus::Scanning,
            self.clock.now(),
        )
        .await?;

        Ok(())
    }

    // ------------------------------------------------------------------ reads

    /// `GET /api/uploads/{upload_id}`. The KV record is the only source.
    ///
    /// `404` once the TTL elapses, and that is the whole retention story: there
    /// is no archive of finished uploads. A rejection appends no event, so
    /// after the window the only evidence an upload ever happened is the
    /// document — if it got that far.
    pub async fn upload_state(
        &self,
        tenant_id: &str,
        user_id: &str,
        upload_id: &str,
    ) -> Result<UploadState, DocumentError> {
        // Another user derives a different key and misses, so this is a `404`
        // with no existence disclosure rather than a `403`.
        transition::load(&self.uploads, tenant_id, user_id, upload_id)
            .await?
            .ok_or(DocumentError::NotFound)
    }

    /// `GET /api/documents/{document_id}`.
    ///
    /// `404` until the first event is *folded*. The projection is the read
    /// path, so there is a lag window after the event is durable. Expected, not
    /// an error.
    pub async fn get_document(
        &self,
        tenant_id: &str,
        document_id: &str,
    ) -> Result<DocumentState, DocumentError> {
        // No `uploads_in_progress`. "Who else is uploading to this document?"
        // is a cross-user question, and uploads now live in a user-scoped
        // keyspace that cannot answer one. That warning is gone deliberately:
        // it was the only thing keeping upload state in Postgres.
        self.documents
            .get(tenant_id, document_id)
            .await?
            .ok_or(DocumentError::NotFound)
    }

    /// `GET /api/documents?limit=&cursor=`.
    pub async fn list_documents(
        &self,
        tenant_id: &str,
        limit: u32,
        after: Option<&DocumentCursor>,
    ) -> Result<DocumentPage, DocumentError> {
        let limit = limit.clamp(1, MAX_LIST_LIMIT);
        let items = self.documents.list(tenant_id, limit, after).await?;
        // Only advertise a cursor on a full page; a short page is the end of
        // the listing and a cursor there costs the client one empty request.
        let next = (items.len() as u32 == limit)
            .then(|| {
                items.last().map(|document| DocumentCursor {
                    updated_at: document.updated_at,
                    document_id: document.document_id.clone(),
                })
            })
            .flatten();
        Ok(DocumentPage { items, next })
    }

    async fn load_context(
        &self,
        tenant_id: &str,
        user_id: &str,
        upload_id: &str,
    ) -> Result<UploadState, DocumentError> {
        transition::load(&self.uploads, tenant_id, user_id, upload_id)
            .await?
            .ok_or(DocumentError::NotFound)
    }
}

/// How many bytes part `part_number` must hold for this upload's geometry:
/// a full part everywhere but the last, and the remainder there. `None` for a
/// part number outside `1..=part_count`.
fn expected_part_size(context: &UploadState, part_number: u16) -> Option<u64> {
    if part_number == 0 || part_number > context.part_count {
        return None;
    }
    if part_number < context.part_count {
        return Some(context.part_size_bytes);
    }
    // `checked_*` rather than plain arithmetic: the geometry makes this exact,
    // but it comes from a decoded KV record, and a corrupt one must produce
    // "no expected size" (so the part is re-uploaded) rather than a panic.
    let consumed = u64::from(context.part_count - 1).checked_mul(context.part_size_bytes)?;
    context.declared_size.checked_sub(consumed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::UploadedPart;
    use crate::command::NATS_MAX_PAYLOAD_BYTES;
    use crate::keys::object_key;
    use crate::testing::*;

    struct Harness {
        service: DocumentService,
        events: Arc<MemoryEventStore>,
        blobs: Arc<MemoryBlobStore>,
        uploads: Arc<MemoryUploadStateStore>,
        documents: Arc<MemoryReadModel>,
        queue: Arc<MemoryWorkQueue>,
        clock: Arc<FixedClock>,
    }

    fn harness() -> Harness {
        harness_with(UploadPolicy {
            part_url_ttl: Duration::from_secs(300),
            max_upload_bytes: 8 * 1024 * 1024 * 1024,
            part_size_bytes: 20 * 1024 * 1024,
        })
    }

    fn harness_with(policy: UploadPolicy) -> Harness {
        let events = Arc::new(MemoryEventStore::default());
        let blobs = Arc::new(MemoryBlobStore::default());
        let uploads = Arc::new(MemoryUploadStateStore::default());
        let documents = Arc::new(MemoryReadModel::default());
        let queue = Arc::new(MemoryWorkQueue::default());
        let clock = Arc::new(FixedClock::default());
        let service = DocumentService::new(
            events.clone(),
            blobs.clone(),
            uploads.clone(),
            documents.clone(),
            queue.clone(),
            clock.clone(),
            Arc::new(SeqIdGen::default()),
            policy,
        );
        Harness {
            service,
            events,
            blobs,
            uploads,
            documents,
            queue,
            clock,
        }
    }

    fn preflight_request(size: u64) -> PreflightRequest {
        PreflightRequest {
            document_id: None,
            filename: "report.pdf".to_owned(),
            size,
            content_type: Some("application/pdf".to_owned()),
        }
    }

    fn complete_request() -> CompleteRequest {
        CompleteRequest {
            if_match: None,
            on_conflict: ConflictPolicy::Supersede,
            patch: MetadataPatch::default(),
        }
    }

    #[tokio::test]
    async fn preflight_mints_ids_and_records_an_attempt_without_presigning() {
        let h = harness();
        let response = h
            .service
            .preflight("acme", "user-1", preflight_request(50 * 1024 * 1024))
            .await
            .expect("preflight");

        assert_eq!(response.part_count, 3);
        assert_eq!(
            response.key,
            format!("tenants/acme/blobs/{}/original", response.upload_id)
        );
        assert_ne!(response.upload_id, response.document_id);

        let state = h
            .uploads
            .snapshot("acme", "user-1", &response.upload_id)
            .expect("upload state");
        assert_eq!(state.status, UploadStatus::Uploading);
        assert_eq!(state.mode, UploadMode::Create);
        assert_eq!(h.uploads.len(), 1);
    }

    #[tokio::test]
    async fn renew_signs_one_part_or_every_remaining_part() {
        let h = harness_with(UploadPolicy {
            part_url_ttl: Duration::from_secs(300),
            max_upload_bytes: 8 * 1024 * 1024 * 1024,
            part_size_bytes: 20 * 1024 * 1024,
        });
        let response = h
            .service
            .preflight("acme", "user-1", preflight_request(100 * 1024 * 1024))
            .await
            .expect("preflight");
        assert_eq!(response.part_count, 5);

        // What an uploader actually asks for: this part, now.
        let one = h
            .service
            .renew(
                "acme",
                "user-1",
                &response.upload_id,
                RenewRequest {
                    from_part: Some(4),
                    count: Some(1),
                },
            )
            .await
            .expect("renew");
        assert_eq!(
            one.parts.iter().map(|p| p.part_number).collect::<Vec<_>>(),
            vec![4]
        );

        // Omitting `count` means "everything from here", not a fixed window.
        // The geometry is the only bound, so this runs to `part_count`.
        let rest = h
            .service
            .renew(
                "acme",
                "user-1",
                &response.upload_id,
                RenewRequest {
                    from_part: Some(3),
                    count: None,
                },
            )
            .await
            .expect("renew");
        assert_eq!(
            rest.parts.iter().map(|p| p.part_number).collect::<Vec<_>>(),
            vec![3, 4, 5]
        );

        // And a caller may still ask for a narrower window.
        let window = h
            .service
            .renew(
                "acme",
                "user-1",
                &response.upload_id,
                RenewRequest {
                    from_part: Some(2),
                    count: Some(2),
                },
            )
            .await
            .expect("renew");
        assert_eq!(
            window
                .parts
                .iter()
                .map(|p| p.part_number)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[tokio::test]
    async fn a_whole_upload_can_be_signed_in_one_request() {
        let h = harness();
        let response = h
            .service
            .preflight("acme", "user-1", preflight_request(2 * 1024 * 1024 * 1024))
            .await
            .expect("preflight");
        assert_eq!(response.part_count, 103);

        let all = h
            .service
            .renew(
                "acme",
                "user-1",
                &response.upload_id,
                RenewRequest {
                    from_part: Some(1),
                    count: None,
                },
            )
            .await
            .expect("renew");

        assert_eq!(all.parts.len(), 103);
        assert_eq!(all.parts.first().map(|p| p.part_number), Some(1));
        assert_eq!(all.parts.last().map(|p| p.part_number), Some(103));
    }

    #[tokio::test]
    async fn signing_a_named_part_does_not_consult_storage() {
        // Signing is local computation. Making it depend on `ListParts` would
        // put an S3 round trip — paged at 1000 — in front of every single part.
        let h = harness();
        let response = h
            .service
            .preflight("acme", "user-1", preflight_request(60 * 1024 * 1024))
            .await
            .expect("preflight");
        let context = h
            .uploads
            .snapshot("acme", "user-1", &response.upload_id)
            .expect("upload state");
        h.blobs
            .drop_multipart(&context.storage_key, &context.multipart_upload_id);

        let signed = h
            .service
            .renew(
                "acme",
                "user-1",
                &response.upload_id,
                RenewRequest {
                    from_part: Some(2),
                    count: Some(1),
                },
            )
            .await
            .expect("naming a part must not require the multipart to be listed");
        assert_eq!(
            signed
                .parts
                .iter()
                .map(|p| p.part_number)
                .collect::<Vec<_>>(),
            vec![2]
        );
    }

    #[tokio::test]
    async fn renew_skips_parts_already_uploaded_and_defaults_to_the_first_gap() {
        let h = harness();
        let response = h
            .service
            .preflight("acme", "user-1", preflight_request(60 * 1024 * 1024))
            .await
            .expect("preflight");
        let context = h
            .uploads
            .snapshot("acme", "user-1", &response.upload_id)
            .expect("upload state");

        h.blobs.upload_part(
            &context.storage_key,
            &context.multipart_upload_id,
            UploadedPart {
                part_number: 1,
                etag: "\"a\"".to_owned(),
                size: 20 * 1024 * 1024,
            },
        );

        let renewed = h
            .service
            .renew(
                "acme",
                "user-1",
                &response.upload_id,
                RenewRequest {
                    from_part: None,
                    count: None,
                },
            )
            .await
            .expect("renew");

        assert_eq!(
            renewed
                .parts
                .iter()
                .map(|p| p.part_number)
                .collect::<Vec<_>>(),
            vec![2, 3],
            "part 1 is already uploaded and must not be re-presigned"
        );
    }

    #[tokio::test]
    async fn an_explicit_from_part_re_signs_a_part_that_is_already_uploaded() {
        // An uploader that retried a PUT whose response it never saw asks for
        // exactly this. Refusing would leave it unable to finish.
        let h = harness();
        let response = h
            .service
            .preflight("acme", "user-1", preflight_request(60 * 1024 * 1024))
            .await
            .expect("preflight");
        let context = h
            .uploads
            .snapshot("acme", "user-1", &response.upload_id)
            .expect("upload state");
        h.blobs.upload_part(
            &context.storage_key,
            &context.multipart_upload_id,
            UploadedPart {
                part_number: 1,
                etag: "\"a\"".to_owned(),
                size: 20 * 1024 * 1024,
            },
        );

        let renewed = h
            .service
            .renew(
                "acme",
                "user-1",
                &response.upload_id,
                RenewRequest {
                    from_part: Some(1),
                    count: Some(1),
                },
            )
            .await
            .expect("renew");

        assert_eq!(
            renewed
                .parts
                .iter()
                .map(|p| p.part_number)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[tokio::test]
    async fn uploaded_parts_reports_what_s3_holds_and_drops_wrong_sized_parts() {
        // 50 MiB at a 20 MiB part size: parts 1 and 2 are full, part 3 is the
        // 10 MiB remainder.
        let h = harness();
        let response = h
            .service
            .preflight("acme", "user-1", preflight_request(50 * 1024 * 1024))
            .await
            .expect("preflight");
        let context = h
            .uploads
            .snapshot("acme", "user-1", &response.upload_id)
            .expect("upload state");

        for (part_number, size) in [
            (1_u16, 20 * 1024 * 1024_u64),
            // Cannot have come from a correct slicing of this file.
            (2, 5 * 1024 * 1024),
            (3, 10 * 1024 * 1024),
        ] {
            h.blobs.upload_part(
                &context.storage_key,
                &context.multipart_upload_id,
                UploadedPart {
                    part_number,
                    etag: format!("\"etag-{part_number}\""),
                    size,
                },
            );
        }

        let listed = h
            .service
            .uploaded_parts("acme", "user-1", &response.upload_id)
            .await
            .expect("uploaded parts");

        assert_eq!(listed.part_count, 3);
        assert_eq!(listed.part_size_bytes, 20 * 1024 * 1024);
        assert_eq!(
            listed
                .parts
                .iter()
                .map(|p| (p.part_number, p.etag.clone()))
                .collect::<Vec<_>>(),
            vec![
                (1, "\"etag-1\"".to_owned()),
                (3, "\"etag-3\"".to_owned())
            ],
            "the short part 2 must be re-uploaded, not resumed"
        );
    }

    #[tokio::test]
    async fn uploaded_parts_is_scoped_to_its_owner_and_gone_once_reaped() {
        let h = harness();
        let response = h
            .service
            .preflight("acme", "user-1", preflight_request(1024))
            .await
            .expect("preflight");

        let theirs = h
            .service
            .uploaded_parts("acme", "user-2", &response.upload_id)
            .await;
        assert!(matches!(theirs, Err(DocumentError::NotFound)));

        let context = h
            .uploads
            .snapshot("acme", "user-1", &response.upload_id)
            .expect("upload state");
        h.blobs
            .drop_multipart(&context.storage_key, &context.multipart_upload_id);

        let reaped = h
            .service
            .uploaded_parts("acme", "user-1", &response.upload_id)
            .await;
        assert!(matches!(reaped, Err(DocumentError::Gone)));
    }

    #[tokio::test]
    async fn renew_reports_gone_when_the_multipart_has_been_reaped() {
        let h = harness();
        let response = h
            .service
            .preflight("acme", "user-1", preflight_request(1024))
            .await
            .expect("preflight");
        let context = h
            .uploads
            .snapshot("acme", "user-1", &response.upload_id)
            .expect("upload state");
        h.blobs
            .drop_multipart(&context.storage_key, &context.multipart_upload_id);

        let result = h
            .service
            .renew(
                "acme",
                "user-1",
                &response.upload_id,
                RenewRequest {
                    from_part: None,
                    count: None,
                },
            )
            .await;
        assert!(matches!(result, Err(DocumentError::Gone)));
    }

    #[tokio::test]
    async fn an_expired_context_makes_complete_a_clean_not_found() {
        let h = harness();
        let response = h
            .service
            .preflight("acme", "user-1", preflight_request(1024))
            .await
            .expect("preflight");
        h.uploads.expire_all();

        let result = h
            .service
            .complete(
                "acme",
                "user-1",
                &response.upload_id,
                complete_request(),
            )
            .await;
        assert!(matches!(result, Err(DocumentError::NotFound)));
    }

    #[tokio::test]
    async fn another_user_cannot_complete_someone_elses_upload() {
        let h = harness();
        let response = h
            .service
            .preflight("acme", "user-1", preflight_request(1024))
            .await
            .expect("preflight");

        let result = h
            .service
            .complete(
                "acme",
                "user-2",
                &response.upload_id,
                complete_request(),
            )
            .await;
        // 404, not 403: no existence disclosure.
        assert!(matches!(result, Err(DocumentError::NotFound)));
        assert!(h.queue.published().is_empty());
    }

    #[tokio::test]
    async fn complete_enqueues_a_self_contained_work_item() {
        let h = harness();
        let response = h
            .service
            .preflight("acme", "user-1", preflight_request(1024))
            .await
            .expect("preflight");

        h.service
            .complete(
                "acme",
                "user-1",
                &response.upload_id,
                CompleteRequest {
                    if_match: None,
                    on_conflict: ConflictPolicy::Supersede,
                    patch: MetadataPatch {
                        title: Some("Annual Report".to_owned()),
                        ..Default::default()
                    },
                },
            )
            .await
            .expect("complete");

        let published = h.queue.published();
        assert_eq!(published.len(), 1);
        let command = &published[0];
        assert_eq!(command.upload_id, response.upload_id);
        assert_eq!(command.document_id, response.document_id);
        assert_eq!(command.storage_key, response.key);
        assert_eq!(command.declared_size, 1024);
        assert_eq!(command.patch.title.as_deref(), Some("Annual Report"));
        assert!(!command.multipart_upload_id.is_empty());

        let state = h
            .uploads
            .snapshot("acme", "user-1", &response.upload_id)
            .expect("upload state");
        assert_eq!(state.status, UploadStatus::Scanning);
    }

    #[tokio::test]
    async fn complete_cannot_push_an_already_finished_upload_back_to_scanning() {
        // The race this pins: `/complete` publishes the work item and only then
        // marks the record `scanning`. A worker that picks the item up and
        // finishes it in that gap has already written `accepted` — and if the
        // api-service's write were allowed to land on top, the upload would
        // report `scanning` forever, because nothing else will ever touch it.
        let h = harness();
        let response = h
            .service
            .preflight("acme", "user-1", preflight_request(1024))
            .await
            .expect("preflight");

        let accepted = UploadStatus::Accepted {
            version: 1,
            superseded: false,
        };
        crate::transition::set_status(
            &(h.uploads.clone() as Arc<dyn crate::ports::UploadStateStore>),
            "acme",
            "user-1",
            &response.upload_id,
            accepted.clone(),
            h.clock.now(),
        )
        .await
        .expect("the worker gets there first");

        h.service
            .complete(
                "acme",
                "user-1",
                &response.upload_id,
                complete_request(),
            )
            .await
            .expect("complete");

        let state = h
            .uploads
            .snapshot("acme", "user-1", &response.upload_id)
            .expect("upload state");
        assert_eq!(
            state.status, accepted,
            "a terminal status must not be overwritten"
        );
    }

    #[tokio::test]
    async fn a_second_complete_is_deduped_so_the_first_part_list_wins() {
        let h = harness();
        let response = h
            .service
            .preflight("acme", "user-1", preflight_request(30 * 1024 * 1024))
            .await
            .expect("preflight");

        h.service
            .complete(
                "acme",
                "user-1",
                &response.upload_id,
                complete_request(),
            )
            .await
            .expect("first complete");
        h.service
            .complete(
                "acme",
                "user-1",
                &response.upload_id,
                complete_request(),
            )
            .await
            .expect("second complete still returns 202");

        // Dedupe still collapses the two publishes into one work item; there is
        // no longer a parts list for the "first one wins" rule to apply to.
        assert_eq!(h.queue.published().len(), 1);
    }

    #[tokio::test]
    async fn if_match_in_create_mode_is_a_bad_request() {
        let h = harness();
        let response = h
            .service
            .preflight("acme", "user-1", preflight_request(1024))
            .await
            .expect("preflight");

        let result = h
            .service
            .complete(
                "acme",
                "user-1",
                &response.upload_id,
                CompleteRequest {
                    if_match: Some(3),
                    on_conflict: ConflictPolicy::Supersede,
                    patch: MetadataPatch::default(),
                },
            )
            .await;
        assert!(matches!(result, Err(DocumentError::Invalid(_))));
    }

    #[tokio::test]
    async fn the_largest_valid_command_is_far_under_the_transport_limit() {
        // This test is what replaced a per-request size check.
        //
        // `/complete` used to serialise the command once to measure it and
        // again to publish it, guarding a ceiling no valid input could reach.
        // What actually bounds the command is `validate_metadata_patch`, which
        // runs three lines earlier — so the regression worth catching is
        // "someone raised a metadata limit", and this catches it here, at build
        // time, instead of on every upload.
        let h = harness();
        let response = h
            .service
            .preflight("acme", "user-1", preflight_request(1024))
            .await
            .expect("preflight");

        let command = UploadCompleted {
            v: UploadCompleted::contract_version(),
            command_id: upload_completed_command_id("acme", &response.upload_id),
            tenant_id: "t".repeat(64),
            owner_user_id: "u".repeat(64),
            upload_id: response.upload_id.clone(),
            document_id: response.document_id.clone(),
            mode: UploadMode::Replace,
            storage_key: object_key(&"t".repeat(64), &response.upload_id),
            multipart_upload_id: "m".repeat(128),
            filename: "f".repeat(255),
            content_type: "c".repeat(128),
            declared_size: u64::MAX,
            if_match: Some(u64::MAX),
            on_conflict: ConflictPolicy::Supersede,
            patch: MetadataPatch {
                title: Some("T".repeat(delphi_document_domain::MAX_TITLE_CHARS)),
                tags: Some(vec![
                    "g".repeat(delphi_document_domain::MAX_TAG_CHARS);
                    delphi_document_domain::MAX_TAGS
                ]),
                description: Some("D".repeat(delphi_document_domain::MAX_DESCRIPTION_CHARS)),
                metadata: Some(serde_json::json!({
                    "k": "v".repeat(delphi_document_domain::MAX_METADATA_BYTES)
                })),
            },
            ts: h.clock.now(),
        };

        let encoded = serde_json::to_vec(&command).expect("encode").len();
        // A tenth of the transport limit is the margin being asserted: not just
        // "it fits", but "raising a metadata limit cannot quietly make it stop
        // fitting". Since the parts list left the command this sits near 40 KiB.
        assert!(
            encoded * 10 < NATS_MAX_PAYLOAD_BYTES,
            "the worst valid command is {encoded} bytes; that is within 10x of \
             the {NATS_MAX_PAYLOAD_BYTES} byte transport limit, so a metadata \
             limit has grown far enough to need a real check again"
        );
    }

    #[tokio::test]
    async fn replace_preflight_uses_the_event_store_for_existence() {
        let h = harness();
        // Nothing in the projection yet — only the event exists. Preflight must
        // still find the document.
        h.events
            .append(
                sample_created_event("acme", "doc-1", "upload-1"),
                crate::ports::Expect::CreateOnly,
            )
            .await
            .expect("seed event");

        let response = h
            .service
            .preflight(
                "acme",
                "user-1",
                PreflightRequest {
                    document_id: Some("doc-1".to_owned()),
                    ..preflight_request(1024)
                },
            )
            .await
            .expect("replace preflight");

        assert_eq!(response.document_id, "doc-1");
        let state = h
            .uploads
            .snapshot("acme", "user-1", &response.upload_id)
            .expect("upload state");
        assert_eq!(state.mode, UploadMode::Replace);
    }

    #[tokio::test]
    async fn replacing_an_unknown_or_deleted_document_fails_at_preflight() {
        let h = harness();
        let missing = h
            .service
            .preflight(
                "acme",
                "user-1",
                PreflightRequest {
                    document_id: Some("ghost".to_owned()),
                    ..preflight_request(1024)
                },
            )
            .await;
        assert!(matches!(missing, Err(DocumentError::NotFound)));

        h.events
            .append(
                sample_created_event("acme", "doc-1", "upload-1"),
                crate::ports::Expect::CreateOnly,
            )
            .await
            .expect("seed event");
        let mut projected = delphi_document_domain::apply(
            None,
            &sample_created_event("acme", "doc-1", "upload-1"),
            1,
        )
        .expect("fold");
        projected.state = DocState::Deleted;
        h.documents.upsert(projected);

        let deleted = h
            .service
            .preflight(
                "acme",
                "user-1",
                PreflightRequest {
                    document_id: Some("doc-1".to_owned()),
                    ..preflight_request(1024)
                },
            )
            .await;
        assert!(matches!(deleted, Err(DocumentError::Deleted)));
    }

    #[tokio::test]
    async fn a_failed_preflight_leaves_no_multipart_or_context_behind() {
        let h = harness();
        // Make the context write fail by pre-claiming the key the id generator
        // will produce: ids are `id-0001` (document) then `id-0002` (upload).
        let poisoned = UploadState {
            tenant_id: "acme".to_owned(),
            owner_user_id: "user-1".to_owned(),
            upload_id: "id-0002".to_owned(),
            document_id: "id-0001".to_owned(),
            mode: UploadMode::Create,
            storage_key: "tenants/acme/blobs/id-0002/original".to_owned(),
            multipart_upload_id: "mp-0".to_owned(),
            filename: "old.pdf".to_owned(),
            content_type: "application/pdf".to_owned(),
            declared_size: 1,
            part_size_bytes: 1,
            part_count: 1,
            status: UploadStatus::Uploading,
            created_at: h.clock.now(),
            updated_at: h.clock.now(),
        };
        h.uploads.create(&poisoned).await.expect("seed state");

        let result = h
            .service
            .preflight("acme", "user-1", preflight_request(1024))
            .await;
        assert!(result.is_err());
        assert_eq!(
            h.blobs.aborted_keys(),
            vec!["tenants/acme/blobs/id-0002/original".to_owned()]
        );
    }

    #[tokio::test]
    async fn upload_state_is_scoped_to_its_owner() {
        let h = harness();
        let response = h
            .service
            .preflight("acme", "user-1", preflight_request(1024))
            .await
            .expect("preflight");

        let mine = h
            .service
            .upload_state("acme", "user-1", &response.upload_id)
            .await
            .expect("status");
        assert_eq!(mine.status, UploadStatus::Uploading);

        let theirs = h
            .service
            .upload_state("acme", "user-2", &response.upload_id)
            .await;
        assert!(matches!(theirs, Err(DocumentError::NotFound)));
    }

    #[tokio::test]
    async fn a_document_read_no_longer_reports_who_else_is_uploading() {
        // Deliberately removed rather than reimplemented. Uploads live in a
        // user-scoped KV keyspace, which cannot answer "who else is uploading
        // to this document?" — and answering it was the only reason upload
        // state was ever in Postgres.
        let h = harness();
        h.events
            .append(
                sample_created_event("acme", "doc-1", "upload-1"),
                crate::ports::Expect::CreateOnly,
            )
            .await
            .expect("seed event");
        h.documents.project(&h.events.events("acme", "doc-1"));

        h.service
            .preflight(
                "acme",
                "user-1",
                PreflightRequest {
                    document_id: Some("doc-1".to_owned()),
                    ..preflight_request(1024)
                },
            )
            .await
            .expect("replace preflight");

        let document = h
            .service
            .get_document("acme", "doc-1")
            .await
            .expect("document");
        assert_eq!(document.version, 1);
    }

    #[tokio::test]
    async fn listing_returns_every_document_in_the_tenant_whoever_uploaded_it() {
        // A document belongs to the tenant, not to the user who uploaded it —
        // the same rule `get_document` applies. When these two disagreed, a
        // document was readable by id but absent from its own tenant's listing.
        let h = harness();
        for (document_id, owner) in [("doc-1", "user-1"), ("doc-2", "user-2")] {
            let mut projected = delphi_document_domain::apply(
                None,
                &sample_created_event("acme", document_id, document_id),
                1,
            )
            .expect("fold");
            projected.owner_user_id = owner.to_owned();
            h.documents.upsert(projected);
        }
        // Tenancy is still a hard boundary.
        let outsider =
            delphi_document_domain::apply(None, &sample_created_event("other", "doc-3", "doc-3"), 1)
                .expect("fold");
        h.documents.upsert(outsider);

        let page = h
            .service
            .list_documents("acme", 50, None)
            .await
            .expect("list");
        let mut ids: Vec<_> = page
            .items
            .into_iter()
            .map(|document| document.document_id)
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["doc-1".to_owned(), "doc-2".to_owned()]);
        assert!(page.next.is_none(), "a short page is the end of the listing");
    }

    #[tokio::test]
    async fn an_over_large_limit_is_clamped_without_claiming_the_listing_ended() {
        // The bug this pins: the clamp and the "is the page full?" test used to
        // live in different layers, so asking for 500 returned MAX_LIST_LIMIT
        // items with `next: null` — indistinguishable from the last page.
        let h = harness();
        for index in 0..(MAX_LIST_LIMIT + 10) {
            let mut projected = delphi_document_domain::apply(
                None,
                &sample_created_event("acme", &format!("doc-{index:04}"), "upload"),
                1,
            )
            .expect("fold");
            projected.updated_at = fixed_time(i64::from(index));
            h.documents.upsert(projected);
        }

        let page = h
            .service
            .list_documents("acme", 500, None)
            .await
            .expect("list");
        assert_eq!(page.items.len(), MAX_LIST_LIMIT as usize);
        assert!(
            page.next.is_some(),
            "a clamped, full page must still hand back a cursor"
        );
    }

    #[tokio::test]
    async fn paging_does_not_drop_documents_that_share_an_updated_at() {
        // Every document has the same timestamp, so `updated_at` alone cannot
        // separate the pages: a strict `updated_at < cursor` would return an
        // empty second page and lose three of the four rows.
        let h = harness();
        for index in 0..4 {
            let mut projected = delphi_document_domain::apply(
                None,
                &sample_created_event("acme", &format!("doc-{index}"), "upload"),
                1,
            )
            .expect("fold");
            projected.updated_at = fixed_time(0);
            h.documents.upsert(projected);
        }

        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let page = h
                .service
                .list_documents("acme", 2, cursor.as_ref())
                .await
                .expect("list");
            seen.extend(page.items.iter().map(|d| d.document_id.clone()));
            match page.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
            assert!(seen.len() <= 4, "paging is not terminating");
        }

        seen.sort();
        assert_eq!(
            seen,
            vec!["doc-0", "doc-1", "doc-2", "doc-3"]
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn a_file_over_the_upload_cap_never_opens_a_multipart() {
        let h = harness_with(UploadPolicy {
            part_url_ttl: Duration::from_secs(300),
            max_upload_bytes: 1024,
            part_size_bytes: 20 * 1024 * 1024,
        });

        let result = h
            .service
            .preflight("acme", "user-1", preflight_request(1025))
            .await;

        assert!(matches!(result, Err(DocumentError::TooLarge(_))));
        assert_eq!(h.uploads.len(), 0);
        assert!(
            h.blobs.open_multipart_count() == 0,
            "the cap must be checked before storage is touched"
        );
    }

    #[tokio::test]
    async fn a_document_is_not_readable_before_its_first_event_is_folded() {
        let h = harness();
        h.events
            .append(
                sample_created_event("acme", "doc-1", "upload-1"),
                crate::ports::Expect::CreateOnly,
            )
            .await
            .expect("seed event");

        let result = h.service.get_document("acme", "doc-1").await;
        assert!(matches!(result, Err(DocumentError::NotFound)));
    }
}
