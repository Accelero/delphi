use delphi_document_domain::{GeometryError, ValidationError};
use thiserror::Error;

/// The error every use case returns. The HTTP layer maps these to status
/// codes; nothing below the service layer knows what a status code is.
///
/// Internal detail belongs in `Internal`, which is logged and never returned
/// to a client.
#[derive(Debug, Error)]
pub enum DocumentError {
    #[error("not found")]
    NotFound,
    #[error("forbidden")]
    Forbidden,
    #[error("gone")]
    Gone,
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("payload too large: {0}")]
    TooLarge(String),
    #[error("version conflict; current version is {current_version}")]
    Conflict { current_version: u64 },
    #[error("document is deleted")]
    Deleted,
    #[error("internal error: {0}")]
    Internal(String),
}

impl DocumentError {
    pub fn internal(context: &str, error: impl std::fmt::Display) -> Self {
        Self::Internal(format!("{context}: {error}"))
    }
}

impl From<ValidationError> for DocumentError {
    fn from(error: ValidationError) -> Self {
        Self::Invalid(error.to_string())
    }
}

impl From<GeometryError> for DocumentError {
    fn from(error: GeometryError) -> Self {
        Self::Invalid(error.to_string())
    }
}

/// Why a blob operation failed, classified by what the caller should do about
/// it. Adapters own the mapping from SDK errors to these; use cases branch on
/// the kind and never see an SDK type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobErrorKind {
    /// The multipart upload does not exist — reaped, aborted, or already
    /// completed by an earlier delivery.
    NoSuchUpload,
    /// The submitted parts are wrong: bad ETag, missing part, or a non-final
    /// part below the 5 MiB floor. Deterministic; retrying cannot help.
    InvalidParts,
    /// The object does not exist.
    NotFound,
    /// Network, timeout, throttle, or 5xx. Retrying may help.
    Transient,
    /// Anything else deterministic.
    Permanent,
}

#[derive(Debug, Clone, Error)]
#[error("{kind:?}: {message}")]
pub struct BlobError {
    pub kind: BlobErrorKind,
    pub message: String,
}

impl BlobError {
    pub fn new(kind: BlobErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn transient(message: impl Into<String>) -> Self {
        Self::new(BlobErrorKind::Transient, message)
    }

    pub fn permanent(message: impl Into<String>) -> Self {
        Self::new(BlobErrorKind::Permanent, message)
    }
}

#[derive(Debug, Clone, Error)]
pub enum EventStoreError {
    /// The expected-sequence check failed: something landed in the window, or
    /// the document already exists.
    #[error("expected sequence conflict")]
    Conflict,
    #[error("event store unavailable: {0}")]
    Unavailable(String),
    #[error("event store payload error: {0}")]
    Payload(String),
}

#[derive(Debug, Clone, Error)]
pub enum ContextError {
    /// A record already exists at this key. Create-once is deliberate.
    #[error("upload state already exists")]
    AlreadyExists,
    /// The compare-and-swap lost: the record moved between the read and the
    /// write. The caller re-reads and decides again.
    #[error("upload state changed underneath this write")]
    Conflict,
    /// The record is gone — the bucket TTL elapsed. Distinct from a plain miss
    /// because the worker has to clean up after it.
    #[error("upload state has expired")]
    Expired,
    #[error("upload state store unavailable: {0}")]
    Unavailable(String),
    #[error("upload state payload error: {0}")]
    Payload(String),
}

#[derive(Debug, Clone, Error)]
pub enum ReadError {
    #[error("read model unavailable: {0}")]
    Unavailable(String),
    #[error("read model payload error: {0}")]
    Payload(String),
}

#[derive(Debug, Clone, Error)]
pub enum QueueError {
    #[error("work queue unavailable: {0}")]
    Unavailable(String),
    #[error("work queue payload error: {0}")]
    Payload(String),
}

#[derive(Debug, Clone, Error)]
pub enum ScanError {
    #[error("scanner unavailable: {0}")]
    Unavailable(String),
    #[error("scan read failed: {0}")]
    Read(String),
}

#[derive(Debug, Clone, Error)]
pub enum ValidateError {
    #[error("content validator unavailable: {0}")]
    Unavailable(String),
}

/// Outcome of the two append helpers. Distinct from [`DocumentError`] because
/// the create path treats `AlreadyCreated` as success.
#[derive(Debug, Clone, Error)]
pub enum AppendError {
    /// A `DocumentCreated` already exists for this document. On the create
    /// path this means a previous delivery succeeded — not an error.
    #[error("document already created")]
    AlreadyCreated,
    #[error("document does not exist")]
    NoSuchDocument,
    #[error("client version {client} does not match current version {current}")]
    VersionMismatch { client: u64, current: u64 },
    #[error("gave up after repeated expected-sequence conflicts")]
    ConflictRetryExhausted,
    #[error(transparent)]
    Store(#[from] EventStoreError),
}

impl From<EventStoreError> for DocumentError {
    fn from(error: EventStoreError) -> Self {
        Self::internal("event store", error)
    }
}

impl From<ReadError> for DocumentError {
    fn from(error: ReadError) -> Self {
        Self::internal("read model", error)
    }
}

impl From<QueueError> for DocumentError {
    fn from(error: QueueError) -> Self {
        Self::internal("work queue", error)
    }
}

impl From<ContextError> for DocumentError {
    fn from(error: ContextError) -> Self {
        match error {
            // The record is gone, so the upload it described cannot be
            // continued. `410`, not `500`: the client's move is a fresh upload.
            ContextError::Expired => Self::Gone,
            other => Self::internal("upload state", other),
        }
    }
}

impl From<BlobError> for DocumentError {
    fn from(error: BlobError) -> Self {
        Self::internal("blob store", error)
    }
}

impl From<AppendError> for DocumentError {
    fn from(error: AppendError) -> Self {
        match error {
            AppendError::VersionMismatch { current, .. } => Self::Conflict {
                current_version: current,
            },
            AppendError::NoSuchDocument => Self::NotFound,
            other => Self::internal("append", other),
        }
    }
}
