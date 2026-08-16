//! `FinishUpload` — the worker-side use case.
//!
//! One work item drives everything: assemble the multipart, validate, scan,
//! append. The caller owns the transport concerns (heartbeats, ack, nak, term)
//! and drives them from [`FinishOutcome`], so no NATS type appears here.
//!
//! **Ack after append, never before.**

use std::sync::Arc;

use delphi_document_domain::{
    apply, Actor, DocumentBlobValidated, DocumentCreated, DocumentEvent, DocumentEventPayload,
    DocumentState, FoldError, DOCUMENT_CONTRACT_VERSION,
};

use crate::append::{append_create, append_update};
use crate::command::{ConflictPolicy, UploadCompleted};
use crate::digest::checksum;
use crate::errors::{AppendError, BlobErrorKind};
use crate::keys::{blob_validated_event_id, created_event_id};
use crate::ports::{
    BlobHead, BlobScanner, BlobStore, Clock, CompletedPart, ContentValidator, ContentVerdict,
    DeclaredContent, EventStore, ScanVerdict, UploadStateStore,
};
use crate::transition::{self, Transition};
use crate::upload_state::{reject_reason, UploadMode, UploadStatus};

/// How much of the object the content validator gets to sniff.
const PREFIX_BYTES: usize = 512;

/// The component name recorded on system-authored events.
pub const WORKER_COMPONENT: &str = "document-worker";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinishOutcome {
    Accepted {
        document_id: String,
        version: u64,
        /// The uploader was looking at a version other than the one this
        /// upload superseded — someone else's change was overwritten.
        superseded: bool,
    },
    /// Terminal. The object is gone and the upload record says why. The caller
    /// must ack (or term); redelivering cannot change the answer.
    Rejected { reason: String },
    /// Transient. The caller should nak and let redelivery retry.
    Retry { error: String },
}

pub struct UploadFinisher {
    blobs: Arc<dyn BlobStore>,
    scanner: Arc<dyn BlobScanner>,
    validator: Arc<dyn ContentValidator>,
    events: Arc<dyn EventStore>,
    uploads: Arc<dyn UploadStateStore>,
    clock: Arc<dyn Clock>,
}

impl UploadFinisher {
    pub fn new(
        blobs: Arc<dyn BlobStore>,
        scanner: Arc<dyn BlobScanner>,
        validator: Arc<dyn ContentValidator>,
        events: Arc<dyn EventStore>,
        uploads: Arc<dyn UploadStateStore>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            blobs,
            scanner,
            validator,
            events,
            uploads,
            clock,
        }
    }

    /// Run one work item to a terminal answer.
    ///
    /// `final_delivery` is true on the last permitted attempt: a transient
    /// error there becomes a rejection, because leaving the object and the
    /// upload record in limbo is worse than declaring failure. `max_deliver`
    /// being finite is what makes this reachable, and it is the *only* thing
    /// that ever cleans up a doomed upload — nothing sweeps assembled objects.
    pub async fn finish(&self, command: &UploadCompleted, final_delivery: bool) -> FinishOutcome {
        match self.run(command).await {
            FinishOutcome::Retry { error } if final_delivery => {
                tracing::warn!(
                    upload_id = %command.upload_id,
                    %error,
                    "upload failed on its final delivery; rejecting"
                );
                self.reject(command, reject_reason::PIPELINE_FAILED).await
            }
            outcome => outcome,
        }
    }

    async fn run(&self, command: &UploadCompleted) -> FinishOutcome {
        // 0. The upload record is the upload. Consult it before touching
        //    anything, because two of its states mean "do not run the pipeline".
        match self.load_state(command).await {
            Ok(Some(status)) if status.is_terminal() => {
                // A redelivery of work that already finished — the ack was lost,
                // not the work. Report the recorded answer rather than
                // re-assembling and re-scanning a multi-gigabyte object.
                tracing::info!(
                    upload_id = %command.upload_id,
                    status = status.as_str(),
                    "work item redelivered after a terminal answer; replaying it"
                );
                return replay(command, status);
            }
            Ok(Some(_)) => {}
            Ok(None) => return self.discard_expired(command).await,
            Err(error) => return FinishOutcome::Retry { error },
        }

        // 1. Assemble — unless an earlier delivery already did.
        let head = match self.assemble(command).await {
            Assembled::Ready(head) => head,
            Assembled::Terminal(outcome) => return outcome,
        };

        // 2. The declared size is a promise made at preflight; hold the upload
        //    to it before spending a scan on it. S3 has no notion of a declared
        //    size — it only reports what it stored — so this comparison is ours.
        let head = match head {
            Some(head) => head,
            None => match self.blobs.head(&command.storage_key).await {
                Ok(Some(head)) => head,
                Ok(None) => return self.reject(command, reject_reason::MULTIPART_LOST).await,
                Err(error) => {
                    return FinishOutcome::Retry {
                        error: error.to_string(),
                    }
                }
            },
        };
        if head.byte_size != command.declared_size {
            return self.reject(command, reject_reason::SIZE_MISMATCH).await;
        }

        // 3. Sniff the head of the object. A ranged read: this wants 512 bytes,
        //    and the scan in step 4 is the only thing that should ever pull a
        //    whole object across the wire.
        let prefix = match self.blobs.read_prefix(&command.storage_key, PREFIX_BYTES).await {
            Ok(prefix) => prefix,
            Err(error) => {
                return FinishOutcome::Retry {
                    error: error.to_string(),
                }
            }
        };
        match self.validate_content(command, &head, &prefix).await {
            Ok(ContentVerdict::Ok) => {}
            Ok(ContentVerdict::Rejected { reason }) => {
                tracing::info!(upload_id = %command.upload_id, %reason, "content validation rejected the upload");
                return self.reject(command, reject_reason::CONTENT_REJECTED).await;
            }
            Err(error) => return FinishOutcome::Retry { error },
        }

        // 4. Scan. This is also the only source of the checksum.
        let reader = match self.blobs.open_read(&command.storage_key).await {
            Ok(reader) => reader,
            Err(error) => {
                return FinishOutcome::Retry {
                    error: error.to_string(),
                }
            }
        };
        let outcome = match self.scanner.scan(reader).await {
            Ok(outcome) => outcome,
            Err(error) => {
                return FinishOutcome::Retry {
                    error: error.to_string(),
                }
            }
        };
        if let ScanVerdict::Infected { signature } = &outcome.verdict {
            tracing::warn!(upload_id = %command.upload_id, %signature, "malware detected");
            return self.reject(command, reject_reason::MALWARE_DETECTED).await;
        }
        // The scanner counted the bytes it actually read; a mismatch here means
        // the object changed under us between HEAD and GET.
        if outcome.byte_count != command.declared_size {
            return self.reject(command, reject_reason::SIZE_MISMATCH).await;
        }

        // 5. Append, then record the outcome. Never the other way round.
        match command.mode {
            UploadMode::Create => self.append_created(command, &outcome.sha256_hex).await,
            UploadMode::Replace => self.append_replaced(command, &outcome.sha256_hex).await,
        }
    }

    /// Bring the object into existence, or explain why it cannot be.
    ///
    /// The parts list is **not** carried on the command. It used to be, echoed
    /// by the client from the ETags it saw — but that token was always partly
    /// hollow: on any resumed upload some of those ETags came from our own
    /// `GET /parts`, so the client handed back values it had never observed.
    /// S3 knows authoritatively, so ask it at the moment of use.
    ///
    /// `ListParts` returning `None` is the *only* place "already assembled" is
    /// decided. An earlier draft inferred it three separate times — once here,
    /// once from an empty list, and once from a doomed `CompleteMultipartUpload`
    /// — which cost four `HEAD`s and a pointless round trip on every redelivery
    /// of an upload that had already succeeded, which is exactly what a lost ack
    /// produces.
    async fn assemble(&self, command: &UploadCompleted) -> Assembled {
        let parts = match self
            .blobs
            .list_parts(&command.storage_key, &command.multipart_upload_id)
            .await
        {
            Ok(Some(parts)) => parts,
            // The multipart is gone: an earlier delivery completed it, or it was
            // reaped. One HEAD decides, and either way nothing is left to
            // assemble. The head is carried forward so step 2 need not re-fetch.
            Ok(None) => {
                return match self.blobs.head(&command.storage_key).await {
                    Ok(Some(head)) => {
                        tracing::info!(
                            upload_id = %command.upload_id,
                            "multipart already completed; continuing idempotently"
                        );
                        Assembled::Ready(Some(head))
                    }
                    Ok(None) => Assembled::Terminal(
                        self.reject(command, reject_reason::MULTIPART_LOST).await,
                    ),
                    Err(error) => Assembled::Terminal(FinishOutcome::Retry {
                        error: error.to_string(),
                    }),
                };
            }
            Err(error) => {
                return Assembled::Terminal(FinishOutcome::Retry {
                    error: error.to_string(),
                })
            }
        };

        // The multipart is open, so it cannot also have been completed — the
        // object key is derived from this upload id alone. An empty list
        // therefore means `/complete` arrived before anything was uploaded.
        if parts.is_empty() {
            tracing::info!(upload_id = %command.upload_id, "completed with no uploaded parts");
            return Assembled::Terminal(self.reject(command, reject_reason::INVALID_PARTS).await);
        }

        // S3 assembles in the order given and requires ascending part numbers.
        // `ListParts` already returns them sorted, across pages; sorting again
        // costs nothing and removes the dependency on that.
        let mut completed: Vec<CompletedPart> = parts
            .into_iter()
            .map(|part| CompletedPart {
                part_number: part.part_number,
                etag: part.etag,
            })
            .collect();
        completed.sort_by_key(|part| part.part_number);

        match self
            .blobs
            .complete_multipart(
                &command.storage_key,
                &command.multipart_upload_id,
                &completed,
            )
            .await
        {
            Ok(()) => Assembled::Ready(None),
            Err(error) => match error.kind {
                // Another delivery completed it between our ListParts and this
                // call. Rare, and the object's presence still settles it.
                BlobErrorKind::NoSuchUpload => match self.blobs.head(&command.storage_key).await {
                    Ok(Some(head)) => Assembled::Ready(Some(head)),
                    Ok(None) => Assembled::Terminal(
                        self.reject(command, reject_reason::MULTIPART_LOST).await,
                    ),
                    Err(error) => Assembled::Terminal(FinishOutcome::Retry {
                        error: error.to_string(),
                    }),
                },
                BlobErrorKind::Transient => Assembled::Terminal(FinishOutcome::Retry {
                    error: error.to_string(),
                }),
                BlobErrorKind::InvalidParts
                | BlobErrorKind::NotFound
                | BlobErrorKind::Permanent => Assembled::Terminal(
                    self.reject(command, reject_reason::INVALID_PARTS).await,
                ),
            },
        }
    }

    /// The record's status, or `None` if the TTL took it.
    async fn load_state(&self, command: &UploadCompleted) -> Result<Option<UploadStatus>, String> {
        transition::load(
            &self.uploads,
            &command.tenant_id,
            &command.owner_user_id,
            &command.upload_id,
        )
        .await
        .map(|state| state.map(|state| state.status))
        .map_err(|error| error.to_string())
    }

    /// The record expired before this work item ran: clean up and give up.
    ///
    /// **The event-log check is not optional.** The obvious reading of "no
    /// record, so delete the object" destroys live documents: a successful
    /// upload whose ack was lost, redelivered after the TTL elapsed, has no
    /// record either — and its bytes are the ones a document is serving. So the
    /// log is asked first, and it is authoritative. Only bytes that never
    /// became a document version are deleted.
    async fn discard_expired(&self, command: &UploadCompleted) -> FinishOutcome {
        match self.applied_version(command).await {
            Ok(Some(version)) => {
                tracing::info!(
                    upload_id = %command.upload_id,
                    version,
                    "upload state expired, but the event is in the log; keeping the blob"
                );
                return FinishOutcome::Accepted {
                    document_id: command.document_id.clone(),
                    version,
                    superseded: false,
                };
            }
            Ok(None) => {}
            // Never delete on a failed read. Retry; the final delivery will
            // still clean up, and by then the log has had every chance to answer.
            Err(error) => return FinishOutcome::Retry { error },
        }

        tracing::warn!(
            upload_id = %command.upload_id,
            key = %command.storage_key,
            "upload state expired before the work item ran; reclaiming the bytes"
        );
        self.reclaim(command).await;
        FinishOutcome::Rejected {
            reason: reject_reason::UPLOAD_EXPIRED.to_owned(),
        }
    }

    /// The version this upload produced, if its event is already in the log.
    ///
    /// Matches on `blob_ref`, over the **whole history** rather than the head:
    /// a later upload may already have superseded ours, so `current_blob` not
    /// naming us does not mean we never applied.
    async fn applied_version(&self, command: &UploadCompleted) -> Result<Option<u64>, String> {
        let history = self
            .events
            .read_stream(&command.tenant_id, &command.document_id)
            .await
            .map_err(|error| error.to_string())?;
        Ok(history
            .iter()
            .find(|(_, event)| event.payload.blob_ref() == Some(command.upload_id.as_str()))
            .map(|(_, event)| event.version))
    }

    async fn validate_content(
        &self,
        command: &UploadCompleted,
        head: &BlobHead,
        prefix: &[u8],
    ) -> Result<ContentVerdict, String> {
        self.validator
            .validate(
                head,
                prefix,
                &DeclaredContent {
                    filename: command.filename.clone(),
                    content_type: command.content_type.clone(),
                    byte_size: command.declared_size,
                },
            )
            .await
            .map_err(|error| error.to_string())
    }

    // ------------------------------------------------------------ append paths

    async fn append_created(&self, command: &UploadCompleted, sha256_hex: &str) -> FinishOutcome {
        let event = DocumentEvent {
            v: DOCUMENT_CONTRACT_VERSION,
            event_id: created_event_id(&command.tenant_id, &command.upload_id),
            tenant_id: command.tenant_id.clone(),
            document_id: command.document_id.clone(),
            // The create names the uploader; `owner_user_id` is folded from it.
            actor: Actor::User {
                user_id: command.owner_user_id.clone(),
            },
            version: 1,
            ts: self.clock.now(),
            payload: DocumentEventPayload::DocumentCreated(DocumentCreated {
                blob_ref: command.upload_id.clone(),
                filename: command.filename.clone(),
                content_type: command.content_type.clone(),
                byte_size: command.declared_size,
                checksum: checksum(sha256_hex),
                patch: command.patch.clone(),
            }),
        };

        match append_create(self.events.as_ref(), event).await {
            Ok(appended) => {
                self.accept(command, appended.version, false).await
            }
            // A previous delivery already created it. Success, not an error,
            // and never retried.
            Err(AppendError::AlreadyCreated) => {
                match self
                    .events
                    .last(&command.tenant_id, &command.document_id)
                    .await
                {
                    Ok(Some((version, _))) => {
                        tracing::info!(
                            upload_id = %command.upload_id,
                            version,
                            "document was already created by an earlier delivery"
                        );
                        self.accept(command, version, false).await
                    }
                    Ok(None) => FinishOutcome::Retry {
                        error: "create conflicted but the document has no events".to_owned(),
                    },
                    Err(error) => FinishOutcome::Retry {
                        error: error.to_string(),
                    },
                }
            }
            Err(error) => FinishOutcome::Retry {
                error: error.to_string(),
            },
        }
    }

    async fn append_replaced(&self, command: &UploadCompleted, sha256_hex: &str) -> FinishOutcome {
        let history = match self
            .events
            .read_stream(&command.tenant_id, &command.document_id)
            .await
        {
            Ok(history) => history,
            Err(error) => {
                return FinishOutcome::Retry {
                    error: error.to_string(),
                }
            }
        };

        // Redelivery guard. Compare against the WHOLE history, not the current
        // head: a concurrent upload may already have superseded ours, so
        // `current_blob != upload_id` does not mean we have not applied.
        if let Some((_, applied)) = history
            .iter()
            .find(|(_, event)| event.payload.blob_ref() == Some(command.upload_id.as_str()))
        {
            let superseded = superseded_flag(applied);
            tracing::info!(
                upload_id = %command.upload_id,
                version = applied.version,
                "this upload is already in the document's history"
            );
            return self.accept(command, applied.version, superseded).await;
        }

        let current_version = match fold_history(&history) {
            Ok(Some(state)) => state.version,
            Ok(None) => {
                // Preflight resolved the document through the event store, so
                // an empty history means it was never really there.
                return self.reject(command, reject_reason::MULTIPART_LOST).await;
            }
            Err(error) => {
                tracing::error!(
                    document_id = %command.document_id,
                    %error,
                    "document history does not fold; refusing to append to it"
                );
                return self.reject(command, reject_reason::CORRUPT_HISTORY).await;
            }
        };

        let based_on_version = command.if_match;
        if let Some(expected) = command.if_match {
            if expected != current_version && command.on_conflict == ConflictPolicy::Fail {
                return self.reject(command, reject_reason::VERSION_CONFLICT).await;
            }
        }

        let checksum = checksum(sha256_hex);
        let now = self.clock.now();
        let build = |version: u64| DocumentEvent {
            v: DOCUMENT_CONTRACT_VERSION,
            event_id: blob_validated_event_id(&command.tenant_id, &command.upload_id),
            tenant_id: command.tenant_id.clone(),
            document_id: command.document_id.clone(),
            actor: Actor::User {
                user_id: command.owner_user_id.clone(),
            },
            version,
            ts: now,
            payload: DocumentEventPayload::DocumentBlobValidated(DocumentBlobValidated {
                blob_ref: command.upload_id.clone(),
                filename: command.filename.clone(),
                content_type: command.content_type.clone(),
                byte_size: command.declared_size,
                checksum: checksum.clone(),
                patch: command.patch.clone(),
                based_on_version,
            }),
        };

        // `client_version` is None: the supersede decision was made above, so
        // letting the helper re-check `if_match` would turn a deliberate
        // supersede into a conflict.
        match append_update(
            self.events.as_ref(),
            &command.tenant_id,
            &command.document_id,
            None,
            build,
        )
        .await
        {
            Ok(appended) => {
                let superseded = based_on_version
                    .is_some_and(|based| based != appended.version.saturating_sub(1));
                self.accept(command, appended.version, superseded).await
            }
            Err(error) => FinishOutcome::Retry {
                error: error.to_string(),
            },
        }
    }

    // -------------------------------------------------------------- outcomes

    async fn accept(
        &self,
        command: &UploadCompleted,
        version: u64,
        superseded: bool,
    ) -> FinishOutcome {
        self.record(
            command,
            UploadStatus::Accepted {
                version,
                superseded,
            },
        )
        .await;
        FinishOutcome::Accepted {
            document_id: command.document_id.clone(),
            version,
            superseded,
        }
    }

    /// Reclaim the bytes, record why, and stop.
    ///
    /// This is the only place an assembled object is ever deleted. Nothing
    /// sweeps blobs on a timer, so bytes that never became a document would
    /// otherwise be kept forever — and unvalidated bytes are exactly the ones
    /// that must not survive, since a leaked object key would still read them.
    async fn reject(&self, command: &UploadCompleted, reason: &str) -> FinishOutcome {
        self.reclaim(command).await;
        self.record(
            command,
            UploadStatus::Rejected {
                reason: reason.to_owned(),
            },
        )
        .await;
        FinishOutcome::Rejected {
            reason: reason.to_owned(),
        }
    }

    /// Give the bytes back. Both callers have already established that no
    /// document version references them.
    async fn reclaim(&self, command: &UploadCompleted) {
        // Abort first: if `complete_multipart` never succeeded the multipart is
        // still open, and `delete` alone would leave it for minio's reaper.
        if let Err(error) = self
            .blobs
            .abort_multipart(&command.storage_key, &command.multipart_upload_id)
            .await
        {
            tracing::warn!(%error, upload_id = %command.upload_id, "could not abort multipart on reject");
        }
        if let Err(error) = self.blobs.delete(&command.storage_key).await {
            // Nothing will retry this: the item is about to be termed and no
            // sweeper looks at assembled objects. Loud on purpose.
            tracing::error!(%error, upload_id = %command.upload_id, key = %command.storage_key, "could not delete a rejected upload's object; it is now orphaned");
        }
    }

    /// Write the terminal answer to the record — after the event, never before.
    ///
    /// Losing this write must never cost us the event we just made durable, so
    /// every failure is a log line. The client sees a stale `scanning` until the
    /// TTL, and the document is there regardless.
    async fn record(&self, command: &UploadCompleted, status: UploadStatus) {
        let outcome = transition::finish_with(
            &self.uploads,
            (
                &command.tenant_id,
                &command.owner_user_id,
                &command.upload_id,
            ),
            status,
            self.clock.now(),
        )
        .await;

        match outcome {
            Ok(Transition::Applied(_)) => {}
            // The record died between the pipeline and here. The event is
            // already durable, so there is nothing to undo — and resurrecting
            // the record would only leak a key the TTL just collected.
            Ok(Transition::Expired) => {
                tracing::info!(upload_id = %command.upload_id, "upload state expired before its outcome could be recorded");
            }
            Ok(Transition::AlreadyTerminal(state)) => {
                tracing::info!(
                    upload_id = %command.upload_id,
                    status = state.status.as_str(),
                    "upload already had a terminal status; keeping the first answer"
                );
            }
            Err(error) => {
                tracing::warn!(%error, upload_id = %command.upload_id, "could not record the upload outcome");
            }
        }
    }
}

/// Outcome of the assemble step: either the object exists and the pipeline
/// continues — carrying a `BlobHead` when one was already fetched, so step 2
/// need not ask twice — or the work item is finished.
enum Assembled {
    Ready(Option<BlobHead>),
    Terminal(FinishOutcome),
}

/// Turn a recorded terminal status back into the outcome the caller acts on.
fn replay(command: &UploadCompleted, status: UploadStatus) -> FinishOutcome {
    match status {
        UploadStatus::Accepted {
            version,
            superseded,
        } => FinishOutcome::Accepted {
            document_id: command.document_id.clone(),
            version,
            superseded,
        },
        UploadStatus::Rejected { reason } => FinishOutcome::Rejected { reason },
        // `is_terminal` gated this; a non-terminal status here is a bug, and
        // retrying is the answer that cannot lose data.
        other => FinishOutcome::Retry {
            error: format!("replay called with the non-terminal status {}", other.as_str()),
        },
    }
}

fn fold_history(history: &[(u64, DocumentEvent)]) -> Result<Option<DocumentState>, FoldError> {
    let mut state = None;
    for (seq, event) in history {
        state = Some(apply(state, event, *seq)?);
    }
    Ok(state)
}

fn superseded_flag(event: &DocumentEvent) -> bool {
    match &event.payload {
        DocumentEventPayload::DocumentBlobValidated(validated) => validated
            .based_on_version
            .is_some_and(|based| based != event.version.saturating_sub(1)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::BlobError;
    use crate::keys::{object_key, upload_completed_command_id};
    use crate::ports::{ContentVerdict, Expect, UploadedPart};
    use crate::testing::*;
    use crate::upload_state::UploadState;
    use delphi_document_domain::MetadataPatch;

    const TENANT: &str = "acme";
    const USER: &str = "user-1";
    const UPLOAD: &str = "upload-9";
    const SIZE: u64 = 32;

    struct Harness {
        finisher: UploadFinisher,
        blobs: Arc<MemoryBlobStore>,
        events: Arc<MemoryEventStore>,
        uploads: Arc<MemoryUploadStateStore>,
    }

    fn harness() -> Harness {
        harness_with(
            Arc::new(StubScanner::default()),
            Arc::new(StubValidator::default()),
        )
    }

    fn harness_with(
        scanner: Arc<dyn BlobScanner>,
        validator: Arc<dyn ContentValidator>,
    ) -> Harness {
        let blobs = Arc::new(MemoryBlobStore::default());
        let events = Arc::new(MemoryEventStore::default());
        let uploads = Arc::new(MemoryUploadStateStore::default());
        let finisher = UploadFinisher::new(
            blobs.clone(),
            scanner,
            validator,
            events.clone(),
            uploads.clone(),
            Arc::new(FixedClock::default()),
        );
        Harness {
            finisher,
            blobs,
            events,
            uploads,
        }
    }

    /// A command whose single part has already been "uploaded" to the store.
    fn command(mode: UploadMode, document_id: &str, upload_id: &str) -> UploadCompleted {
        UploadCompleted {
            v: DOCUMENT_CONTRACT_VERSION,
            command_id: upload_completed_command_id(TENANT, upload_id),
            tenant_id: TENANT.to_owned(),
            owner_user_id: USER.to_owned(),
            upload_id: upload_id.to_owned(),
            document_id: document_id.to_owned(),
            mode,
            storage_key: object_key(TENANT, upload_id),
            multipart_upload_id: "mp-1".to_owned(),
            filename: "report.pdf".to_owned(),
            content_type: "application/pdf".to_owned(),
            declared_size: SIZE,
            if_match: None,
            on_conflict: ConflictPolicy::Supersede,
            patch: MetadataPatch {
                title: Some("Report".to_owned()),
                ..Default::default()
            },
            ts: fixed_time(0),
        }
    }

    fn stage_parts(blobs: &MemoryBlobStore, command: &UploadCompleted, size: u64) {
        blobs.upload_part(
            &command.storage_key,
            &command.multipart_upload_id,
            UploadedPart {
                part_number: 1,
                etag: "\"e1\"".to_owned(),
                size,
            },
        );
    }

    /// The KV record `/complete` would have left behind. Every test that
    /// expects the pipeline to run needs one — without it the worker correctly
    /// concludes the upload expired.
    fn stage_state(uploads: &MemoryUploadStateStore, command: &UploadCompleted) {
        uploads.seed(UploadState {
            tenant_id: command.tenant_id.clone(),
            owner_user_id: command.owner_user_id.clone(),
            upload_id: command.upload_id.clone(),
            document_id: command.document_id.clone(),
            mode: command.mode,
            storage_key: command.storage_key.clone(),
            multipart_upload_id: command.multipart_upload_id.clone(),
            filename: command.filename.clone(),
            content_type: command.content_type.clone(),
            declared_size: command.declared_size,
            part_size_bytes: command.declared_size.max(1),
            part_count: 1,
            status: UploadStatus::Scanning,
            created_at: fixed_time(0),
            updated_at: fixed_time(0),
        });
    }

    /// Both halves of what `/complete` leaves behind.
    fn stage(h: &Harness, command: &UploadCompleted, size: u64) {
        stage_parts(&h.blobs, command, size);
        stage_state(&h.uploads, command);
    }

    #[tokio::test]
    async fn a_clean_create_produces_exactly_one_event() {
        let h = harness();
        let cmd = command(UploadMode::Create, "doc-1", UPLOAD);
        stage(&h, &cmd, SIZE);

        let outcome = h.finisher.finish(&cmd, false).await;

        assert_eq!(
            outcome,
            FinishOutcome::Accepted {
                document_id: "doc-1".to_owned(),
                version: 1,
                superseded: false
            }
        );
        assert_eq!(h.events.event_count(TENANT, "doc-1"), 1);
        let state = h.uploads.snapshot(TENANT, USER, UPLOAD).expect("upload state");
        assert_eq!(
            state.status,
            UploadStatus::Accepted {
                version: 1,
                superseded: false
            }
        );
    }

    #[tokio::test]
    async fn the_checksum_on_the_event_comes_from_the_scan() {
        let h = harness();
        let cmd = command(UploadMode::Create, "doc-1", UPLOAD);
        stage(&h, &cmd, SIZE);
        h.finisher.finish(&cmd, false).await;

        let (_, event) = h.events.events(TENANT, "doc-1").remove(0);
        let DocumentEventPayload::DocumentCreated(created) = event.payload else {
            panic!("expected a create");
        };
        let expected = crate::digest::checksum(&crate::digest::sha256_hex(
            &std::iter::repeat_n(b'x', SIZE as usize).collect::<Vec<_>>(),
        ));
        assert_eq!(created.checksum, expected);
        assert_eq!(created.byte_size, SIZE);
        assert_eq!(created.patch.title.as_deref(), Some("Report"));
    }

    #[tokio::test]
    async fn a_redelivered_create_after_the_dedupe_window_still_yields_one_event() {
        let h = harness();
        let cmd = command(UploadMode::Create, "doc-1", UPLOAD);
        stage(&h, &cmd, SIZE);
        h.finisher.finish(&cmd, false).await;

        // The multipart is consumed and the dedupe window has notionally
        // elapsed, so the second run must rely on the create conflict.
        stage(&h, &cmd, SIZE);
        let outcome = h.finisher.finish(&cmd, false).await;

        assert!(matches!(outcome, FinishOutcome::Accepted { version: 1, .. }));
        assert_eq!(h.events.event_count(TENANT, "doc-1"), 1);
    }

    #[tokio::test]
    async fn a_replace_appends_a_second_version() {
        let h = harness();
        h.events
            .append(sample_created_event(TENANT, "doc-1", "upload-1"), Expect::CreateOnly)
            .await
            .expect("seed");

        let cmd = command(UploadMode::Replace, "doc-1", UPLOAD);
        stage(&h, &cmd, SIZE);
        let outcome = h.finisher.finish(&cmd, false).await;

        assert_eq!(
            outcome,
            FinishOutcome::Accepted {
                document_id: "doc-1".to_owned(),
                version: 2,
                superseded: false
            }
        );
        assert_eq!(h.events.event_count(TENANT, "doc-1"), 2);
    }

    #[tokio::test]
    async fn the_replace_guard_scans_the_whole_history_not_just_the_head() {
        let h = harness();
        h.events
            .append(sample_created_event(TENANT, "doc-1", "upload-1"), Expect::CreateOnly)
            .await
            .expect("seed");

        let cmd = command(UploadMode::Replace, "doc-1", UPLOAD);
        stage(&h, &cmd, SIZE);
        h.finisher.finish(&cmd, false).await;

        // A concurrent upload supersedes ours, so `current_blob` no longer
        // names it. A redelivery must still recognise that we already applied.
        h.events
            .append(
                sample_validated_event(TENANT, "doc-1", "upload-later", 3),
                Expect::Exactly(2),
            )
            .await
            .expect("concurrent replace");

        stage(&h, &cmd, SIZE);
        let outcome = h.finisher.finish(&cmd, false).await;

        assert!(matches!(outcome, FinishOutcome::Accepted { version: 2, .. }));
        assert_eq!(
            h.events.event_count(TENANT, "doc-1"),
            3,
            "the redelivery must not append a fourth event"
        );
    }

    #[tokio::test]
    async fn a_superseding_replace_is_flagged_for_its_author() {
        let h = harness();
        h.events
            .append(sample_created_event(TENANT, "doc-1", "upload-1"), Expect::CreateOnly)
            .await
            .expect("seed");
        h.events
            .append(
                sample_validated_event(TENANT, "doc-1", "upload-2", 2),
                Expect::Exactly(1),
            )
            .await
            .expect("someone else's change");

        // Our uploader was looking at v1 but v2 already landed.
        let mut cmd = command(UploadMode::Replace, "doc-1", UPLOAD);
        cmd.if_match = Some(1);
        stage(&h, &cmd, SIZE);

        let outcome = h.finisher.finish(&cmd, false).await;
        assert_eq!(
            outcome,
            FinishOutcome::Accepted {
                document_id: "doc-1".to_owned(),
                version: 3,
                superseded: true
            }
        );
    }

    #[tokio::test]
    async fn on_conflict_fail_rejects_instead_of_superseding() {
        let h = harness();
        h.events
            .append(sample_created_event(TENANT, "doc-1", "upload-1"), Expect::CreateOnly)
            .await
            .expect("seed");
        h.events
            .append(
                sample_validated_event(TENANT, "doc-1", "upload-2", 2),
                Expect::Exactly(1),
            )
            .await
            .expect("someone else's change");

        let mut cmd = command(UploadMode::Replace, "doc-1", UPLOAD);
        cmd.if_match = Some(1);
        cmd.on_conflict = ConflictPolicy::Fail;
        stage(&h, &cmd, SIZE);

        let outcome = h.finisher.finish(&cmd, false).await;
        assert_eq!(
            outcome,
            FinishOutcome::Rejected {
                reason: reject_reason::VERSION_CONFLICT.to_owned()
            }
        );
        assert_eq!(h.events.event_count(TENANT, "doc-1"), 2);
        assert!(h.blobs.deleted_keys().contains(&cmd.storage_key));
    }

    #[tokio::test]
    async fn a_size_mismatch_rejects_and_reclaims_the_bytes() {
        let h = harness();
        let cmd = command(UploadMode::Create, "doc-1", UPLOAD);
        stage(&h, &cmd, SIZE + 1);

        let outcome = h.finisher.finish(&cmd, false).await;

        assert_eq!(
            outcome,
            FinishOutcome::Rejected {
                reason: reject_reason::SIZE_MISMATCH.to_owned()
            }
        );
        assert_eq!(h.events.event_count(TENANT, "doc-1"), 0);
        assert!(h.blobs.object(&cmd.storage_key).is_none());
    }

    #[tokio::test]
    async fn an_infected_object_never_becomes_a_document() {
        let h = harness_with(
            Arc::new(StubScanner {
                force_infected: true,
            }),
            Arc::new(StubValidator::default()),
        );
        let cmd = command(UploadMode::Create, "doc-1", UPLOAD);
        stage(&h, &cmd, SIZE);

        let outcome = h.finisher.finish(&cmd, false).await;

        assert_eq!(
            outcome,
            FinishOutcome::Rejected {
                reason: reject_reason::MALWARE_DETECTED.to_owned()
            }
        );
        assert_eq!(h.events.event_count(TENANT, "doc-1"), 0);
        assert!(h.blobs.object(&cmd.storage_key).is_none());
        let state = h.uploads.snapshot(TENANT, USER, UPLOAD).expect("upload state");
        assert_eq!(
            state.status,
            UploadStatus::Rejected {
                reason: reject_reason::MALWARE_DETECTED.to_owned()
            }
        );
    }

    #[tokio::test]
    async fn content_rejection_is_terminal() {
        let h = harness_with(
            Arc::new(StubScanner::default()),
            Arc::new(StubValidator {
                verdict: ContentVerdict::Rejected {
                    reason: "not a pdf".to_owned(),
                },
            }),
        );
        let cmd = command(UploadMode::Create, "doc-1", UPLOAD);
        stage(&h, &cmd, SIZE);

        let outcome = h.finisher.finish(&cmd, false).await;
        assert_eq!(
            outcome,
            FinishOutcome::Rejected {
                reason: reject_reason::CONTENT_REJECTED.to_owned()
            }
        );
    }

    #[tokio::test]
    async fn invalid_parts_are_permanent_but_a_network_blip_is_retried() {
        let h = harness();
        let cmd = command(UploadMode::Create, "doc-1", UPLOAD);
        stage(&h, &cmd, SIZE);
        h.blobs
            .fail_complete_with(BlobError::new(BlobErrorKind::InvalidParts, "bad etag"));
        assert_eq!(
            h.finisher.finish(&cmd, false).await,
            FinishOutcome::Rejected {
                reason: reject_reason::INVALID_PARTS.to_owned()
            }
        );

        let h = harness();
        let cmd = command(UploadMode::Create, "doc-1", UPLOAD);
        stage(&h, &cmd, SIZE);
        h.blobs
            .fail_complete_with(BlobError::transient("connection reset"));
        assert!(matches!(
            h.finisher.finish(&cmd, false).await,
            FinishOutcome::Retry { .. }
        ));
        assert_eq!(h.events.event_count(TENANT, "doc-1"), 0);
        // A retryable failure must NOT delete the object or the attempt row's
        // hope of succeeding on the next delivery.
        assert!(h.blobs.deleted_keys().is_empty());
    }

    #[tokio::test]
    async fn a_transient_failure_on_the_final_delivery_becomes_a_rejection() {
        let h = harness();
        let cmd = command(UploadMode::Create, "doc-1", UPLOAD);
        stage(&h, &cmd, SIZE);
        h.blobs
            .fail_complete_with(BlobError::transient("connection reset"));

        let outcome = h.finisher.finish(&cmd, true).await;

        assert_eq!(
            outcome,
            FinishOutcome::Rejected {
                reason: reject_reason::PIPELINE_FAILED.to_owned()
            }
        );
        let state = h.uploads.snapshot(TENANT, USER, UPLOAD).expect("upload state");
        assert!(state.status.is_terminal());
    }

    #[tokio::test]
    async fn the_parts_list_comes_from_storage_not_the_command() {
        // The command carries no parts at all. Everything storage holds is
        // assembled, in ascending order, however it was uploaded.
        let h = harness();
        let cmd = command(UploadMode::Create, "doc-1", UPLOAD);
        stage_state(&h.uploads, &cmd);
        // Deliberately staged out of order, and in more than one part.
        for (part_number, size) in [(2_u16, SIZE), (1, SIZE)] {
            h.blobs.upload_part(
                &cmd.storage_key,
                &cmd.multipart_upload_id,
                UploadedPart {
                    part_number,
                    etag: format!("\"e{part_number}\""),
                    size,
                },
            );
        }

        let outcome = h.finisher.finish(&cmd, false).await;

        assert!(
            matches!(outcome, FinishOutcome::Rejected { .. }),
            "two parts of SIZE each cannot match a declared SIZE, so this must \
             reject on size — the point is that it assembled at all: {outcome:?}"
        );
        assert_eq!(
            h.blobs.completed_parts(&cmd.storage_key),
            Some(vec![1, 2]),
            "storage must have been asked to assemble both parts, in order"
        );
    }

    #[tokio::test]
    async fn completing_before_anything_was_uploaded_is_terminal() {
        let h = harness();
        let cmd = command(UploadMode::Create, "doc-1", UPLOAD);
        stage_state(&h.uploads, &cmd);
        // The multipart exists but holds nothing.
        h.blobs
            .begin_multipart_as(&cmd.storage_key, &cmd.multipart_upload_id);

        let outcome = h.finisher.finish(&cmd, false).await;

        assert_eq!(
            outcome,
            FinishOutcome::Rejected {
                reason: reject_reason::INVALID_PARTS.to_owned()
            }
        );
    }

    #[tokio::test]
    async fn a_lost_multipart_with_no_object_is_terminal() {
        let h = harness();
        let cmd = command(UploadMode::Create, "doc-1", UPLOAD);
        // The record is there, so the upload has not expired — but neither the
        // multipart nor the object exists, which is the case this pins.
        stage_state(&h.uploads, &cmd);

        let outcome = h.finisher.finish(&cmd, false).await;

        assert_eq!(
            outcome,
            FinishOutcome::Rejected {
                reason: reject_reason::MULTIPART_LOST.to_owned()
            }
        );
    }

    #[tokio::test]
    async fn an_expired_record_reclaims_the_bytes_and_gives_up() {
        // The TTL elapsed before the work item ran. Nothing references these
        // bytes and nothing ever will, so they go back.
        let h = harness();
        let cmd = command(UploadMode::Create, "doc-1", UPLOAD);
        stage_parts(&h.blobs, &cmd, SIZE);

        let outcome = h.finisher.finish(&cmd, false).await;

        assert_eq!(
            outcome,
            FinishOutcome::Rejected {
                reason: reject_reason::UPLOAD_EXPIRED.to_owned()
            }
        );
        assert_eq!(h.events.event_count(TENANT, "doc-1"), 0);
        assert!(h.blobs.aborted_keys().contains(&cmd.storage_key));
    }

    #[tokio::test]
    async fn an_expired_record_never_deletes_a_blob_the_log_already_names() {
        // The dangerous case. A successful upload whose ack was lost, then
        // redelivered after the TTL, has no record either — and its bytes are
        // what a live document is serving. Deleting on "no record" alone would
        // destroy that document's content.
        let h = harness();
        let cmd = command(UploadMode::Create, "doc-1", UPLOAD);
        stage(&h, &cmd, SIZE);
        let first = h.finisher.finish(&cmd, false).await;
        assert!(matches!(first, FinishOutcome::Accepted { version: 1, .. }));

        // The ack is lost and the record ages out.
        h.uploads.expire_all();
        let redelivered = h.finisher.finish(&cmd, false).await;

        assert!(
            matches!(redelivered, FinishOutcome::Accepted { version: 1, .. }),
            "the log is authoritative: this upload already became a version"
        );
        assert!(
            h.blobs.object(&cmd.storage_key).is_some(),
            "the document's bytes must survive"
        );
        assert!(h.blobs.deleted_keys().is_empty());
    }

    #[tokio::test]
    async fn a_redelivery_after_a_terminal_answer_replays_it_without_rescanning() {
        let h = harness();
        let cmd = command(UploadMode::Create, "doc-1", UPLOAD);
        stage(&h, &cmd, SIZE);
        h.finisher.finish(&cmd, false).await;

        // No parts staged for the second run: reaching the pipeline at all
        // would fail. The recorded answer is returned instead.
        let replayed = h.finisher.finish(&cmd, false).await;

        assert_eq!(
            replayed,
            FinishOutcome::Accepted {
                document_id: "doc-1".to_owned(),
                version: 1,
                superseded: false
            }
        );
        assert_eq!(h.events.event_count(TENANT, "doc-1"), 1);
    }
}
