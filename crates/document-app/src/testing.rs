//! Deterministic in-memory implementations of every port.
//!
//! Use-case tests run entirely against these: no containers, no network, no
//! wall clock. Behaviour that matters for correctness is modelled faithfully —
//! in particular the event store deduplicates on `event_id` *before* evaluating
//! the expected sequence, exactly as JetStream does, because that ordering is
//! what makes `Appended::duplicate` load-bearing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use delphi_document_domain::{
    apply, Actor, DocumentBlobValidated, DocumentCreated, DocumentEvent, DocumentEventPayload,
    DocumentState, MetadataPatch, DOCUMENT_CONTRACT_VERSION,
};

use crate::command::UploadCompleted;
use crate::cursor::DocumentCursor;
use crate::errors::{
    BlobError, BlobErrorKind, ContextError, EventStoreError, QueueError, ReadError, ScanError,
    ValidateError,
};
use crate::ports::{
    Appended, BlobHead, BlobScanner, BlobStore, BoxAsyncRead, Clock, CompletedPart,
    ContentValidator, ContentVerdict, DeclaredContent, DocumentReadModel, EventStore, Expect,
    IdGen, PresignedPart, ScanOutcome, ScanVerdict, UploadStateStore, UploadedPart, WorkQueue,
};
use crate::upload_state::{StoredUpload, UploadState, UploadStatus};

pub fn fixed_time(offset_secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_760_000_000 + offset_secs, 0)
        .single()
        .expect("timestamp is unambiguous")
}

// ------------------------------------------------------------- event fixtures

pub fn sample_created_event(tenant: &str, document_id: &str, blob_ref: &str) -> DocumentEvent {
    DocumentEvent {
        v: DOCUMENT_CONTRACT_VERSION,
        event_id: crate::keys::created_event_id(tenant, blob_ref),
        tenant_id: tenant.to_owned(),
        document_id: document_id.to_owned(),
        actor: Actor::User {
            user_id: "user-1".to_owned(),
        },
        version: 1,
        ts: fixed_time(0),
        payload: DocumentEventPayload::DocumentCreated(DocumentCreated {
            blob_ref: blob_ref.to_owned(),
            filename: "report.pdf".to_owned(),
            content_type: "application/pdf".to_owned(),
            byte_size: 1024,
            checksum: "sha256:aa".to_owned(),
            patch: MetadataPatch::default(),
        }),
    }
}

pub fn sample_validated_event(
    tenant: &str,
    document_id: &str,
    blob_ref: &str,
    version: u64,
) -> DocumentEvent {
    DocumentEvent {
        v: DOCUMENT_CONTRACT_VERSION,
        event_id: crate::keys::blob_validated_event_id(tenant, blob_ref),
        tenant_id: tenant.to_owned(),
        document_id: document_id.to_owned(),
        actor: Actor::System {
            component: "document-worker".to_owned(),
        },
        version,
        ts: fixed_time(1),
        payload: DocumentEventPayload::DocumentBlobValidated(DocumentBlobValidated {
            blob_ref: blob_ref.to_owned(),
            filename: "report.pdf".to_owned(),
            content_type: "application/pdf".to_owned(),
            byte_size: 2048,
            checksum: "sha256:bb".to_owned(),
            patch: MetadataPatch::default(),
            based_on_version: Some(version - 1),
        }),
    }
}

// ---------------------------------------------------------------- event store

#[derive(Default)]
struct EventStoreState {
    /// `(tenant, document_id)` -> ordered `(stream_seq, event)`.
    streams: HashMap<(String, String), Vec<(u64, DocumentEvent)>>,
    /// `(tenant, document_id, event_id)` -> the sequence it first landed at.
    seen: HashMap<(String, String, String), u64>,
    head: u64,
    conflicts_remaining: u64,
    always_conflict: bool,
    unavailable: bool,
}

#[derive(Default)]
pub struct MemoryEventStore {
    state: Mutex<EventStoreState>,
}

impl MemoryEventStore {
    /// The next `append` fails the expected-sequence check exactly once, as if
    /// a concurrent event had landed in the read-then-append window.
    pub fn inject_conflict_once(&self) {
        self.state.lock().expect("lock").conflicts_remaining += 1;
    }

    pub fn always_conflict(&self) {
        self.state.lock().expect("lock").always_conflict = true;
    }

    pub fn set_unavailable(&self, unavailable: bool) {
        self.state.lock().expect("lock").unavailable = unavailable;
    }

    pub fn event_count(&self, tenant: &str, document_id: &str) -> usize {
        self.state
            .lock()
            .expect("lock")
            .streams
            .get(&(tenant.to_owned(), document_id.to_owned()))
            .map(Vec::len)
            .unwrap_or(0)
    }

    pub fn events(&self, tenant: &str, document_id: &str) -> Vec<(u64, DocumentEvent)> {
        self.state
            .lock()
            .expect("lock")
            .streams
            .get(&(tenant.to_owned(), document_id.to_owned()))
            .cloned()
            .unwrap_or_default()
    }
}

#[async_trait]
impl EventStore for MemoryEventStore {
    async fn append(
        &self,
        event: DocumentEvent,
        expect: Expect,
    ) -> Result<Appended, EventStoreError> {
        let mut state = self.state.lock().expect("lock");
        if state.unavailable {
            return Err(EventStoreError::Unavailable("injected".to_owned()));
        }

        let stream_key = (event.tenant_id.clone(), event.document_id.clone());
        let dedupe_key = (
            event.tenant_id.clone(),
            event.document_id.clone(),
            event.event_id.clone(),
        );

        // Dedupe wins over the expected-sequence header, as in JetStream.
        if let Some(&sequence) = state.seen.get(&dedupe_key) {
            let version = state
                .streams
                .get(&stream_key)
                .and_then(|events| events.iter().find(|(seq, _)| *seq == sequence))
                .map(|(_, existing)| existing.version)
                .unwrap_or(event.version);
            return Ok(Appended {
                stream_seq: sequence,
                version,
                duplicate: true,
            });
        }

        if state.always_conflict {
            return Err(EventStoreError::Conflict);
        }
        if state.conflicts_remaining > 0 {
            state.conflicts_remaining -= 1;
            return Err(EventStoreError::Conflict);
        }

        let last_seq = state
            .streams
            .get(&stream_key)
            .and_then(|events| events.last())
            .map(|(seq, _)| *seq)
            .unwrap_or(0);
        let expected = match expect {
            Expect::CreateOnly => 0,
            Expect::Exactly(seq) => seq,
        };
        if last_seq != expected {
            return Err(EventStoreError::Conflict);
        }

        state.head += 1;
        let sequence = state.head;
        state.seen.insert(dedupe_key, sequence);
        let version = event.version;
        state
            .streams
            .entry(stream_key)
            .or_default()
            .push((sequence, event));

        Ok(Appended {
            stream_seq: sequence,
            version,
            duplicate: false,
        })
    }

    async fn last(
        &self,
        tenant: &str,
        document_id: &str,
    ) -> Result<Option<(u64, u64)>, EventStoreError> {
        let state = self.state.lock().expect("lock");
        if state.unavailable {
            return Err(EventStoreError::Unavailable("injected".to_owned()));
        }
        Ok(state
            .streams
            .get(&(tenant.to_owned(), document_id.to_owned()))
            .and_then(|events| events.last())
            .map(|(seq, event)| (event.version, *seq)))
    }

    async fn read_stream(
        &self,
        tenant: &str,
        document_id: &str,
    ) -> Result<Vec<(u64, DocumentEvent)>, EventStoreError> {
        let state = self.state.lock().expect("lock");
        if state.unavailable {
            return Err(EventStoreError::Unavailable("injected".to_owned()));
        }
        Ok(state
            .streams
            .get(&(tenant.to_owned(), document_id.to_owned()))
            .cloned()
            .unwrap_or_default())
    }
}

// ------------------------------------------------------------------ blob store

#[derive(Default)]
struct BlobState {
    /// `(key, multipart_upload_id)` -> uploaded parts.
    multiparts: HashMap<(String, String), Vec<UploadedPart>>,
    /// key -> (bytes, last_modified).
    objects: HashMap<String, (Vec<u8>, DateTime<Utc>)>,
    next_multipart: u64,
    complete_error: Option<BlobError>,
    /// key -> the part numbers `complete_multipart` was handed.
    assembled: HashMap<String, Vec<u16>>,
    deleted: Vec<String>,
    aborted: Vec<String>,
}

#[derive(Default)]
pub struct MemoryBlobStore {
    state: Mutex<BlobState>,
}

impl MemoryBlobStore {
    /// Pre-place an object as if the parts had been uploaded and completed.
    pub fn put_object(&self, key: &str, bytes: Vec<u8>, at: DateTime<Utc>) {
        self.state
            .lock()
            .expect("lock")
            .objects
            .insert(key.to_owned(), (bytes, at));
    }

    pub fn object(&self, key: &str) -> Option<Vec<u8>> {
        self.state
            .lock()
            .expect("lock")
            .objects
            .get(key)
            .map(|(bytes, _)| bytes.clone())
    }

    pub fn fail_complete_with(&self, error: BlobError) {
        self.state.lock().expect("lock").complete_error = Some(error);
    }

    pub fn deleted_keys(&self) -> Vec<String> {
        self.state.lock().expect("lock").deleted.clone()
    }

    pub fn aborted_keys(&self) -> Vec<String> {
        self.state.lock().expect("lock").aborted.clone()
    }

    /// Record uploaded parts so `complete_multipart` has something to assemble.
    pub fn upload_part(&self, key: &str, upload: &str, part: UploadedPart) {
        self.state
            .lock()
            .expect("lock")
            .multiparts
            .entry((key.to_owned(), upload.to_owned()))
            .or_default()
            .push(part);
    }

    pub fn drop_multipart(&self, key: &str, upload: &str) {
        self.state
            .lock()
            .expect("lock")
            .multiparts
            .remove(&(key.to_owned(), upload.to_owned()));
    }

    /// The part numbers storage was actually asked to assemble, in the order
    /// given. `None` if `complete_multipart` was never called for this key.
    pub fn completed_parts(&self, key: &str) -> Option<Vec<u16>> {
        self.state.lock().expect("lock").assembled.get(key).cloned()
    }

    /// Register a multipart with a chosen id, so a test can stage an upload
    /// that exists but holds no parts.
    pub fn begin_multipart_as(&self, key: &str, upload: &str) {
        self.state
            .lock()
            .expect("lock")
            .multiparts
            .entry((key.to_owned(), upload.to_owned()))
            .or_default();
    }

    pub fn open_multipart_count(&self) -> usize {
        self.state.lock().expect("lock").multiparts.len()
    }
}

#[async_trait]
impl BlobStore for MemoryBlobStore {
    async fn begin_multipart(&self, key: &str, _content_type: &str) -> Result<String, BlobError> {
        let mut state = self.state.lock().expect("lock");
        state.next_multipart += 1;
        let upload = format!("mp-{}", state.next_multipart);
        state.multiparts.insert((key.to_owned(), upload.clone()), Vec::new());
        Ok(upload)
    }

    async fn presign_part(
        &self,
        key: &str,
        upload: &str,
        part: u16,
        ttl: Duration,
    ) -> Result<PresignedPart, BlobError> {
        Ok(PresignedPart {
            part_number: part,
            url: format!("https://blobs.test/{key}?uploadId={upload}&partNumber={part}"),
            expires_at: fixed_time(ttl.as_secs() as i64),
        })
    }

    async fn list_parts(
        &self,
        key: &str,
        upload: &str,
    ) -> Result<Option<Vec<UploadedPart>>, BlobError> {
        Ok(self
            .state
            .lock()
            .expect("lock")
            .multiparts
            .get(&(key.to_owned(), upload.to_owned()))
            .cloned())
    }

    async fn complete_multipart(
        &self,
        key: &str,
        upload: &str,
        parts: &[CompletedPart],
    ) -> Result<(), BlobError> {
        let mut state = self.state.lock().expect("lock");
        if let Some(error) = state.complete_error.clone() {
            return Err(error);
        }
        let Some(uploaded) = state.multiparts.remove(&(key.to_owned(), upload.to_owned())) else {
            return Err(BlobError::new(
                BlobErrorKind::NoSuchUpload,
                "no such multipart upload",
            ));
        };
        state
            .assembled
            .insert(key.to_owned(), parts.iter().map(|p| p.part_number).collect());
        let mut bytes = Vec::new();
        for part in parts {
            let Some(found) = uploaded.iter().find(|u| u.part_number == part.part_number) else {
                return Err(BlobError::new(
                    BlobErrorKind::InvalidParts,
                    format!("part {} was never uploaded", part.part_number),
                ));
            };
            if found.etag != part.etag {
                return Err(BlobError::new(
                    BlobErrorKind::InvalidParts,
                    format!("etag mismatch on part {}", part.part_number),
                ));
            }
            bytes.extend(std::iter::repeat_n(b'x', found.size as usize));
        }
        state
            .objects
            .insert(key.to_owned(), (bytes, fixed_time(0)));
        Ok(())
    }

    async fn abort_multipart(&self, key: &str, upload: &str) -> Result<(), BlobError> {
        let mut state = self.state.lock().expect("lock");
        state.multiparts.remove(&(key.to_owned(), upload.to_owned()));
        state.aborted.push(key.to_owned());
        Ok(())
    }

    async fn head(&self, key: &str) -> Result<Option<BlobHead>, BlobError> {
        Ok(self
            .state
            .lock()
            .expect("lock")
            .objects
            .get(key)
            .map(|(bytes, at)| BlobHead {
                byte_size: bytes.len() as u64,
                content_type: None,
                last_modified: *at,
            }))
    }

    async fn open_read(&self, key: &str) -> Result<BoxAsyncRead, BlobError> {
        let bytes = self
            .state
            .lock()
            .expect("lock")
            .objects
            .get(key)
            .map(|(bytes, _)| bytes.clone())
            .ok_or_else(|| BlobError::new(BlobErrorKind::NotFound, "no such object"))?;
        Ok(Box::pin(std::io::Cursor::new(bytes)))
    }

    async fn read_prefix(&self, key: &str, len: usize) -> Result<Vec<u8>, BlobError> {
        let mut bytes = self
            .state
            .lock()
            .expect("lock")
            .objects
            .get(key)
            .map(|(bytes, _)| bytes.clone())
            .ok_or_else(|| BlobError::new(BlobErrorKind::NotFound, "no such object"))?;
        bytes.truncate(len);
        Ok(bytes)
    }

    async fn delete(&self, key: &str) -> Result<(), BlobError> {
        let mut state = self.state.lock().expect("lock");
        state.objects.remove(key);
        state.deleted.push(key.to_owned());
        Ok(())
    }
}

// ---------------------------------------------------------------- upload state

#[derive(Default)]
struct UploadStateInner {
    entries: HashMap<String, StoredUpload>,
    /// Forces the next `update` to lose its CAS, after quietly landing this
    /// status — how a competing writer is simulated.
    conflict_once: Option<UploadStatus>,
}

#[derive(Default)]
pub struct MemoryUploadStateStore {
    inner: Mutex<UploadStateInner>,
}

impl MemoryUploadStateStore {
    pub fn seed(&self, state: UploadState) {
        self.inner.lock().expect("lock").entries.insert(
            state.own_key(),
            StoredUpload {
                state,
                revision: 1,
            },
        );
    }

    pub fn snapshot(&self, tenant: &str, user: &str, upload_id: &str) -> Option<UploadState> {
        self.inner
            .lock()
            .expect("lock")
            .entries
            .get(&UploadState::key(tenant, user, upload_id))
            .map(|stored| stored.state.clone())
    }

    pub fn len(&self) -> usize {
        self.inner.lock().expect("lock").entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Simulate the bucket `max_age` elapsing.
    pub fn expire_all(&self) {
        self.inner.lock().expect("lock").entries.clear();
    }

    /// The next `update` loses its CAS; `winner` is what the other writer left
    /// behind, which the caller's re-read will then find.
    pub fn fail_next_update_with_conflict(&self, winner: UploadStatus) {
        self.inner.lock().expect("lock").conflict_once = Some(winner);
    }
}

#[async_trait]
impl UploadStateStore for MemoryUploadStateStore {
    async fn create(&self, state: &UploadState) -> Result<(), ContextError> {
        let mut inner = self.inner.lock().expect("lock");
        let key = state.own_key();
        if inner.entries.contains_key(&key) {
            return Err(ContextError::AlreadyExists);
        }
        inner.entries.insert(
            key,
            StoredUpload {
                state: state.clone(),
                revision: 1,
            },
        );
        Ok(())
    }

    async fn get(
        &self,
        tenant: &str,
        user: &str,
        upload_id: &str,
    ) -> Result<Option<StoredUpload>, ContextError> {
        Ok(self
            .inner
            .lock()
            .expect("lock")
            .entries
            .get(&UploadState::key(tenant, user, upload_id))
            .cloned())
    }

    async fn update(&self, state: &UploadState, revision: u64) -> Result<u64, ContextError> {
        let mut inner = self.inner.lock().expect("lock");
        let key = state.own_key();

        if let Some(winner) = inner.conflict_once.take() {
            if let Some(stored) = inner.entries.get_mut(&key) {
                stored.state.status = winner;
                stored.revision += 1;
            }
            return Err(ContextError::Conflict);
        }

        let Some(stored) = inner.entries.get_mut(&key) else {
            return Err(ContextError::Expired);
        };
        if stored.revision != revision {
            return Err(ContextError::Conflict);
        }
        stored.state = state.clone();
        stored.revision += 1;
        Ok(stored.revision)
    }

    async fn delete(&self, tenant: &str, user: &str, upload_id: &str) -> Result<(), ContextError> {
        self.inner
            .lock()
            .expect("lock")
            .entries
            .remove(&UploadState::key(tenant, user, upload_id));
        Ok(())
    }
}

// ------------------------------------------------------------------ read model

#[derive(Default)]
struct ReadModelState {
    documents: HashMap<(String, String), DocumentState>,
    checkpoint: Option<(u64, DateTime<Utc>)>,
}

#[derive(Default)]
pub struct MemoryReadModel {
    state: Mutex<ReadModelState>,
}

impl MemoryReadModel {
    pub fn upsert(&self, document: DocumentState) {
        self.state.lock().expect("lock").documents.insert(
            (document.tenant_id.clone(), document.document_id.clone()),
            document,
        );
    }

    pub fn set_checkpoint(&self, stream_seq: u64, updated_at: DateTime<Utc>) {
        self.state.lock().expect("lock").checkpoint = Some((stream_seq, updated_at));
    }

    pub fn clear_checkpoint(&self) {
        self.state.lock().expect("lock").checkpoint = None;
    }

    /// Fold a document's whole stream into the read model, the way the real
    /// projection loop would.
    pub fn project(&self, events: &[(u64, DocumentEvent)]) {
        let mut state = None;
        let mut last_seq = 0;
        for (seq, event) in events {
            state = Some(apply(state, event, *seq).expect("fixture folds"));
            last_seq = *seq;
        }
        if let Some(document) = state {
            self.upsert(document);
            self.set_checkpoint(last_seq, fixed_time(0));
        }
    }
}

#[async_trait]
impl DocumentReadModel for MemoryReadModel {
    async fn get(
        &self,
        tenant: &str,
        document_id: &str,
    ) -> Result<Option<DocumentState>, ReadError> {
        Ok(self
            .state
            .lock()
            .expect("lock")
            .documents
            .get(&(tenant.to_owned(), document_id.to_owned()))
            .cloned())
    }

    async fn list(
        &self,
        tenant: &str,
        limit: u32,
        after: Option<&DocumentCursor>,
    ) -> Result<Vec<DocumentState>, ReadError> {
        let state = self.state.lock().expect("lock");
        // The same total order the SQL uses, spelled out: descending on
        // `updated_at`, then descending on `document_id` to break ties.
        let key = |doc: &DocumentState| {
            (
                std::cmp::Reverse(doc.updated_at),
                std::cmp::Reverse(doc.document_id.clone()),
            )
        };
        let mut items: Vec<_> = state
            .documents
            .values()
            .filter(|doc| doc.tenant_id == tenant)
            .filter(|doc| {
                after.is_none_or(|cursor| {
                    (doc.updated_at, doc.document_id.as_str())
                        < (cursor.updated_at, cursor.document_id.as_str())
                })
            })
            .cloned()
            .collect();
        items.sort_by_key(key);
        items.truncate(limit as usize);
        Ok(items)
    }
}

// ------------------------------------------------------------------ work queue

#[derive(Default)]
pub struct MemoryWorkQueue {
    published: Mutex<Vec<UploadCompleted>>,
}

impl MemoryWorkQueue {
    pub fn published(&self) -> Vec<UploadCompleted> {
        self.published.lock().expect("lock").clone()
    }
}

#[async_trait]
impl WorkQueue for MemoryWorkQueue {
    async fn publish_upload_completed(&self, cmd: UploadCompleted) -> Result<(), QueueError> {
        // The real queue dedupes on `command_id`; model that so tests see the
        // "first part list wins" behaviour.
        let mut published = self.published.lock().expect("lock");
        if published
            .iter()
            .any(|existing| existing.command_id == cmd.command_id)
        {
            return Ok(());
        }
        published.push(cmd);
        Ok(())
    }
}

// ------------------------------------------------------------- scan / validate

/// Returns `Clean` plus a real digest, and detects the EICAR test string so the
/// reject path is exercisable without real malware.
#[derive(Default)]
pub struct StubScanner {
    pub force_infected: bool,
}

#[async_trait]
impl BlobScanner for StubScanner {
    async fn scan(&self, mut blob: BoxAsyncRead) -> Result<ScanOutcome, ScanError> {
        use tokio::io::AsyncReadExt;
        let mut bytes = Vec::new();
        blob.read_to_end(&mut bytes)
            .await
            .map_err(|error| ScanError::Read(error.to_string()))?;
        let verdict = if self.force_infected {
            ScanVerdict::Infected {
                signature: "Test.Forced".to_owned(),
            }
        } else {
            ScanVerdict::Clean
        };
        Ok(ScanOutcome {
            verdict,
            sha256_hex: crate::digest::sha256_hex(&bytes),
            byte_count: bytes.len() as u64,
        })
    }
}

pub struct StubValidator {
    pub verdict: ContentVerdict,
}

impl Default for StubValidator {
    fn default() -> Self {
        Self {
            verdict: ContentVerdict::Ok,
        }
    }
}

#[async_trait]
impl ContentValidator for StubValidator {
    async fn validate(
        &self,
        _head: &BlobHead,
        _prefix: &[u8],
        _declared: &DeclaredContent,
    ) -> Result<ContentVerdict, ValidateError> {
        Ok(self.verdict.clone())
    }
}

// ------------------------------------------------------------------ clock / id

pub struct FixedClock {
    offset: AtomicU64,
}

impl Default for FixedClock {
    fn default() -> Self {
        Self {
            offset: AtomicU64::new(0),
        }
    }
}

impl FixedClock {
    pub fn advance(&self, secs: u64) {
        self.offset.fetch_add(secs, Ordering::SeqCst);
    }
}

impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        fixed_time(self.offset.load(Ordering::SeqCst) as i64)
    }
}

/// Sequential ids so a test can predict them; the production generator is
/// `Ulid::new()`.
#[derive(Default)]
pub struct SeqIdGen {
    next: AtomicU64,
}

impl IdGen for SeqIdGen {
    fn ulid(&self) -> String {
        format!("id-{:04}", self.next.fetch_add(1, Ordering::SeqCst) + 1)
    }
}
