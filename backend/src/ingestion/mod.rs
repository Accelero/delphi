//! Document ingestion pipeline.
//!
//! Single contract: anything that wants to bring a document into Delphi
//! constructs an [`IngestRequest`] and calls [`IngestSink::ingest`]. The
//! HTTP endpoint at `POST /api/ingestion/documents` and the in-process
//! source-adapter scheduler (`crate::sources`) both call this exact
//! method — there is no parallel codepath.
//!
//! [`Pipeline`] is the canonical [`IngestSink`] impl. It owns:
//! - content-hash computation for dedup
//! - version bumping on content change
//! - calling `Storage::upsert_document` + `upsert_content`
//!
//! Future stages (semantic filter, embedding, notification) compose by
//! wrapping a [`Pipeline`] in an outer [`IngestSink`] — middleware-style,
//! not a parallel pipeline.

mod http;
mod notifier;
mod pipeline;
mod rag;
mod uploads;
mod validation;

pub use http::{ingest_documents, IngestRequestBody};
pub use notifier::{FeedItemEvent, NotifyingSink, DEFAULT_BROADCAST_CAPACITY};
pub use pipeline::{IngestOutcome, IngestRequest, IngestSink, Pipeline};
pub use rag::RagSink;
pub use uploads::{complete_upload, create_upload, get_upload_status, sign_upload_part, UploadsConfig};
pub use validation::{
    validate_ingestion_metadata, validate_uploaded_object, CreateUploadRequest, MetadataPolicy,
    MetadataReject, ObjectPolicy, ObjectReject, ValidatedAttrs,
};
