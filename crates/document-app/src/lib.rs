//! Use cases and port traits for the document lifecycle.
//!
//! Dependencies point inward: this crate knows `delphi-document-domain` and
//! nothing about NATS, Postgres, S3, or HTTP. A use case says "append an event"
//! and "presign a part"; the adapter crate decides what that means.
//!
//! ```text
//! services/*  ->  document-adapters  ->  document-app  ->  document-domain
//! ```
//!
//! Two stores, with a hard line between them:
//!
//! * **An upload** lives entirely in NATS KV, user-scoped, until its TTL. It is
//!   in flight, private, and disposable.
//! * **A document** lives in the event log, tenant-scoped, forever, and is
//!   served from a Postgres projection.
//!
//! The single event the worker appends is the crossover. Nothing about an
//! upload is written to Postgres.

pub mod append;
pub mod command;
pub mod cursor;
pub mod digest;
pub mod errors;
pub mod keys;
pub mod ports;
pub mod principal;
pub mod projection;
pub mod service;
pub mod testing;
pub mod transition;
pub mod upload_state;
pub mod worker;

pub use command::{ConflictPolicy, UploadCompleted};
pub use cursor::DocumentCursor;
pub use errors::{
    AppendError, BlobError, BlobErrorKind, ContextError, DocumentError, EventStoreError,
    QueueError, ReadError, ScanError, ValidateError,
};
pub use ports::{
    Appended, BlobHead, BlobScanner, BlobStore, BoxAsyncRead, Clock, CompletedPart,
    ContentValidator, ContentVerdict, DeclaredContent, DocumentReadModel, EventStore, Expect,
    IdGen, PresignedPart, ScanOutcome, ScanVerdict, UploadStateStore, UploadedPart, WorkQueue,
};
pub use principal::{Principal, PrincipalError};
pub use projection::{decode_event, project_event, DecodedEvent, ProjectionOutcome};
pub use service::{
    CompleteRequest, DocumentPage, DocumentService, PreflightRequest, PreflightResponse,
    RenewRequest, RenewResponse, UploadPolicy, UploadedPartsResponse, MAX_LIST_LIMIT,
};
pub use transition::Transition;
pub use upload_state::{reject_reason, StoredUpload, UploadMode, UploadState, UploadStatus};
pub use worker::{FinishOutcome, UploadFinisher, WORKER_COMPONENT};
