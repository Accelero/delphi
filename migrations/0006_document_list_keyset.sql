-- The document listing pages on `(updated_at, document_id)`, not `updated_at`.
--
-- `updated_at` is not unique: two uploads accepted in the same microsecond, or a
-- batch import, give several documents the same value. Paging with a strict
-- `updated_at < $cursor` then skipped every row that tied with the last row of
-- the previous page — silent, size-dependent data loss in a listing that is now
-- tenant-wide rather than per-user, so ties are much more likely.
--
-- `document_id` is unique within a tenant, so `(updated_at DESC, document_id
-- DESC)` is a total order and the row comparison `(updated_at, document_id) <
-- ($2, $3)` resumes exactly where the previous page stopped.
--
-- The index must carry both columns in that order, or the query sorts.

DROP INDEX IF EXISTS document_tenant_updated_idx;

CREATE INDEX document_tenant_page_idx
  ON document (tenant_id, updated_at DESC, document_id DESC);
