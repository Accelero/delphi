//! Postgres adapters: the read model, the attempt store, and the projection
//! loop that is the only writer of the `document` table.

mod projection;
mod read_model;

pub use projection::{
    peek_document, reset, ProjectionLoop, ProjectorLease, DOCUMENT_PROJECTION_NAME,
};
pub use read_model::PgDocumentReadModel;

use delphi_document_domain::{DocState, DocumentState, IndexState};
use sqlx::postgres::PgRow;
use sqlx::Row;

/// Decode a `document` row back into the folded state.
///
/// Unknown enum tokens fall back rather than failing: the read path must not
/// break because a newer writer introduced a state this build does not know.
fn row_to_document(row: &PgRow) -> Result<DocumentState, sqlx::Error> {
    Ok(DocumentState {
        tenant_id: row.try_get("tenant_id")?,
        document_id: row.try_get("document_id")?,
        owner_user_id: row.try_get("owner_user_id")?,
        version: row.try_get::<i64, _>("version")?.max(0) as u64,
        stream_seq: row.try_get::<i64, _>("stream_seq")?.max(0) as u64,
        state: DocState::parse(row.try_get::<&str, _>("state")?).unwrap_or(DocState::Active),
        index_state: IndexState::parse(row.try_get::<&str, _>("index_state")?)
            .unwrap_or(IndexState::Pending),
        index_version: row
            .try_get::<Option<i64>, _>("index_version")?
            .map(|value| value.max(0) as u64),
        current_blob: row.try_get("current_blob")?,
        filename: row.try_get("filename")?,
        content_type: row.try_get("content_type")?,
        byte_size: row
            .try_get::<Option<i64>, _>("byte_size")?
            .map(|value| value.max(0) as u64),
        checksum: row.try_get("checksum")?,
        title: row.try_get("title")?,
        tags: serde_json::from_value(row.try_get("tags")?).unwrap_or_default(),
        description: row.try_get("description")?,
        metadata: row.try_get("metadata")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

const DOCUMENT_COLUMNS: &str = "tenant_id, document_id, owner_user_id, version, stream_seq, \
     state, index_state, index_version, current_blob, filename, content_type, byte_size, \
     checksum, title, tags, description, metadata, created_at, updated_at";
