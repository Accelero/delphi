use async_trait::async_trait;
use delphi_document_app::{DocumentCursor, DocumentReadModel, ReadError};
use delphi_document_domain::DocumentState;
use sqlx::PgPool;

use super::{row_to_document, DOCUMENT_COLUMNS};

#[derive(Clone)]
pub struct PgDocumentReadModel {
    pool: PgPool,
}

impl PgDocumentReadModel {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn read_error(context: &str, error: sqlx::Error) -> ReadError {
    ReadError::Unavailable(format!("{context}: {error}"))
}

#[async_trait]
impl DocumentReadModel for PgDocumentReadModel {
    async fn get(
        &self,
        tenant: &str,
        document_id: &str,
    ) -> Result<Option<DocumentState>, ReadError> {
        let sql = format!(
            "SELECT {DOCUMENT_COLUMNS} FROM document WHERE tenant_id = $1 AND document_id = $2"
        );
        let row = sqlx::query(&sql)
            .bind(tenant)
            .bind(document_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| read_error("get document", error))?;
        row.as_ref()
            .map(row_to_document)
            .transpose()
            .map_err(|error| ReadError::Payload(format!("decode document row: {error}")))
    }

    async fn list(
        &self,
        tenant: &str,
        limit: u32,
        after: Option<&DocumentCursor>,
    ) -> Result<Vec<DocumentState>, ReadError> {
        // Keyset pagination on exactly the key `document_tenant_page_idx` is
        // ordered by, so the page is an index range scan and never a sort.
        //
        // The row comparison `(updated_at, document_id) < ($2, $3)` is the
        // whole point: `updated_at` alone is not unique, and comparing on it
        // alone dropped every row that tied with the end of the previous page.
        //
        // Scoped to the tenant only: a document belongs to the tenant, not to
        // whoever uploaded it, which is the same rule `get` has always applied.
        let sql = format!(
            "SELECT {DOCUMENT_COLUMNS} FROM document
             WHERE tenant_id = $1
               AND ($2::timestamptz IS NULL
                    OR (updated_at, document_id) < ($2, $3))
             ORDER BY updated_at DESC, document_id DESC
             LIMIT $4"
        );
        let rows = sqlx::query(&sql)
            .bind(tenant)
            .bind(after.map(|cursor| cursor.updated_at))
            .bind(after.map(|cursor| cursor.document_id.as_str()))
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
            .map_err(|error| read_error("list documents", error))?;
        rows.iter()
            .map(row_to_document)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| ReadError::Payload(format!("decode document row: {error}")))
    }
}
