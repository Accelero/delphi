-- Listing documents is tenant-scoped, not owner-scoped.
--
-- A document belongs to the tenant and every member may read it, which is the
-- rule `GET /api/documents/{id}` always applied. The listing disagreed, so a
-- document was readable by id but invisible to everyone except its uploader.
--
-- The old index led with `owner_user_id`, which the new query no longer filters
-- on, so it can no longer serve the ORDER BY without a sort.

DROP INDEX IF EXISTS document_owner_updated_idx;

CREATE INDEX IF NOT EXISTS document_tenant_updated_idx
  ON document (tenant_id, updated_at DESC);
