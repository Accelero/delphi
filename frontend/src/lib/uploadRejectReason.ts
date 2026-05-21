/**
 * Map backend ingestion reject reason codes (stable short strings emitted
 * by `ObjectReject::reason_code` and the completion pipeline) to friendly
 * user-facing messages for the upload tracker.
 */
const REASONS: Record<string, string> = {
  size_mismatch: "File size didn't match what was declared.",
  content_type_mismatch: "File contents don't match the declared type.",
  not_in_allowlist: "This file type isn't allowed.",
  polyglot: "File matched more than one format and was rejected.",
  pdf_parse_failed: "The PDF couldn't be parsed.",
  pdf_parse_timeout: "The PDF took too long to parse.",
  pdf_too_many_pages: "The PDF has too many pages.",
  utf8_decode_failed: "The text file isn't valid UTF-8.",
  head_failed: "The uploaded object couldn't be read back.",
  sniff_failed: "The uploaded object couldn't be inspected.",
  metadata_rejected: "Required document metadata was missing or invalid.",
  canonical_id_conflict: "This document already exists.",
};

/** Friendly message for a backend reject reason code (or HTTP-derived
 *  pseudo-codes the manager synthesises). Falls back to the raw code. */
export function uploadRejectReason(code: string | undefined): string {
  if (!code) return "Upload failed.";
  if (code in REASONS) return REASONS[code];
  if (code === "duplicate_file")
    return "This file is already being uploaded — wait for it to finish, or refresh the page before retrying.";
  if (code === "create_failed") return "Couldn't start the upload.";
  if (code === "upload_failed") return "The file failed to upload.";
  if (code === "complete_failed") return "Couldn't finalize the upload.";
  if (code === "timeout") return "The upload timed out.";
  return code;
}
