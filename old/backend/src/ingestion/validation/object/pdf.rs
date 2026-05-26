//! PDF validator + the PDFiD-style active-content scan.

use crate::object_store::ObjectMeta;

use super::format::FormatValidator;
use super::sniff;
use super::{ObjectPolicy, ObjectReject, ValidatedAttrs};

pub(super) struct PdfValidator;

impl FormatValidator for PdfValidator {
    fn validate(
        &self,
        sniff: &[u8],
        head: &ObjectMeta,
        policy: &ObjectPolicy,
    ) -> Result<ValidatedAttrs, ObjectReject> {
        // Bytes must back the PDF claim.
        if !sniff::is_pdf(sniff) {
            let sniffed = sniff::infer_binary(sniff).unwrap_or_else(|| "unknown".to_string());
            return Err(ObjectReject::ContentTypeMismatch {
                declared: "application/pdf".to_string(),
                sniffed,
            });
        }
        // Size cap — reject without ever downloading the body.
        if head.size > policy.pdf_max_input_bytes {
            return Err(ObjectReject::SizeMismatch {
                declared: head.size,
                actual: head.size,
            });
        }
        // Polyglot probe: a file that is simultaneously PDF and another
        // allowlisted format (e.g. ZIP) is rejected.
        if policy.reject_polyglots {
            let matched = sniff::polyglot_matches(sniff);
            let allowlisted_hits = matched
                .iter()
                .filter(|t| policy.allowed_content_types.contains(*t))
                .count();
            if allowlisted_hits > 1 {
                return Err(ObjectReject::Polyglot { matched });
            }
        }
        Ok(ValidatedAttrs {
            size: head.size,
            etag: head.etag.clone(),
            sniffed_content_type: "application/pdf".to_string(),
        })
    }
}

/// Active-content tokens — the structural keywords a malicious PDF uses to
/// run code on open or carry a payload. PDFiD (Didier Stevens) scans for
/// exactly these.
const ACTIVE_CONTENT_TOKENS: &[&str] = &[
    "/JavaScript",
    "/JS",
    "/OpenAction",
    "/AA",
    "/Launch",
    "/EmbeddedFile",
];

/// Lexical substring scan over the full (bounded) PDF bytes for active
/// content. Returns the first token found, or `None` if clean. **Not a
/// PDF parser** — a plain byte search, so it adds no parse surface.
pub(super) fn scan_active_content(bytes: &[u8]) -> Option<&'static str> {
    ACTIVE_CONTENT_TOKENS
        .iter()
        .copied()
        .find(|tok| contains_subslice(bytes, tok.as_bytes()))
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_pdf_has_no_active_content() {
        assert_eq!(scan_active_content(b"%PDF-1.4\nhello\n%%EOF"), None);
    }

    #[test]
    fn detects_each_active_token() {
        assert_eq!(
            scan_active_content(b"%PDF-1.4 /OpenAction 1 0 R"),
            Some("/OpenAction")
        );
        assert_eq!(
            scan_active_content(b"... /JavaScript (app.alert) ..."),
            Some("/JavaScript")
        );
        assert_eq!(
            scan_active_content(b"trailer /Launch /F (calc.exe)"),
            Some("/Launch")
        );
        assert_eq!(
            scan_active_content(b"/EmbeddedFile stream..."),
            Some("/EmbeddedFile")
        );
    }
}
