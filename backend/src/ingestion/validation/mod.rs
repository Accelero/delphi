//! Ingestion-v2 validation: two pure functions that gate the upload
//! pipeline.
//!
//! - [`validate_ingestion_metadata`] runs synchronously at the top of
//!   `POST /api/ingestion/uploads`, before any S3 operation. It is the
//!   only layer-1 check on the JSON request body.
//! - [`validate_uploaded_object`] runs at `/complete` against the
//!   committed S3 object. It HEADs the object, optionally ranged-GETs
//!   the first window for magic-byte sniffing, and (for PDFs)
//!   sandbox-parses for page counts and parse-failure rejection.
//!
//! Both modules expose their `*Policy` struct with explicit knobs so
//! callers can dial sensitivity per deployment. The handler composes
//! these — it never open-codes validation logic.

pub mod metadata;
pub mod object;

pub use metadata::{
    validate_ingestion_metadata, CreateUploadRequest, MetadataPolicy, MetadataReject,
};
pub use object::{validate_uploaded_object, ObjectPolicy, ObjectReject, ValidatedAttrs};
