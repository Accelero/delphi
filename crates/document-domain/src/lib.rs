//! Pure document domain: the event catalogue, the fold that turns events into
//! state, metadata validation, and multipart part geometry.
//!
//! This crate has no `async fn` and performs no IO. It is testable with no
//! fixtures, no containers, and no runtime. Everything that talks to NATS,
//! Postgres, or S3 lives in `delphi-document-adapters`; the port traits those
//! adapters implement live in `delphi-document-app`.

mod events;
mod fold;
mod geometry;
mod validation;

pub use events::{
    Actor, DocumentBlobValidated, DocumentCreated, DocumentEvent, DocumentEventPayload,
    DocumentIndexed, DocumentStageFailed, DocumentTextExtracted, MetadataPatch,
    DOCUMENT_CONTRACT_VERSION,
};
pub use fold::{apply, DocState, DocumentState, FoldError, IndexState};
pub use geometry::{
    largest_size_honouring, part_count, part_size_bytes, GeometryError, MAX_OBJECT_BYTES,
    MAX_PARTS, MAX_PART_BYTES, MIN_PART_BYTES,
};
pub use validation::{
    validate_metadata_patch, ValidationError, MAX_DESCRIPTION_CHARS, MAX_METADATA_BYTES,
    MAX_TAGS, MAX_TAG_CHARS, MAX_TITLE_CHARS,
};
