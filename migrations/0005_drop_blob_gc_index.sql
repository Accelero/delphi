-- The unreferenced-blob sweeper is gone; blobs are kept.
--
-- `document_current_blob_idx` existed solely to make the sweeper's
-- "is this blob still the serving blob?" probe cheap. Nothing asks that
-- question any more.
--
-- `document.current_blob` itself stays. It is genuine projection state — which
-- blob currently serves a document — and the version-retention design will need
-- it to know what a `DocumentBlobPruned` event should name.

DROP INDEX IF EXISTS document_current_blob_idx;
