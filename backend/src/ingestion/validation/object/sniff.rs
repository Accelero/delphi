//! Shared byte-level helpers for the object validators: positive UTF-8
//! text detection, the size-tolerance check, magic-byte probes, and the
//! polyglot probe. Pure functions, no I/O.

/// True iff `bytes` are (or are a valid UTF-8 prefix of) UTF-8 text. A
/// multi-byte char split at the truncated sniff-window boundary yields an
/// "unexpected end" error (`error_len() == None`) — that prefix is still
/// valid text, so we accept it.
pub(super) fn looks_like_utf8_text(bytes: &[u8]) -> bool {
    match std::str::from_utf8(bytes) {
        Ok(_) => true,
        Err(e) => e.error_len().is_none() && e.valid_up_to() > 0,
    }
}

/// True iff `declared` and `actual` are within `tolerance` bytes. A
/// tolerance of 0 (the default) requires exact match.
pub(super) fn within_tolerance(declared: u64, actual: u64, tolerance: u64) -> bool {
    let diff = declared.max(actual) - declared.min(actual);
    diff <= tolerance
}

/// PDF magic header.
pub(super) fn is_pdf(bytes: &[u8]) -> bool {
    bytes.starts_with(b"%PDF-")
}

/// The MIME `infer` recognises *only when it's a binary* signature.
/// `infer` matches some text types too (`text/html`, `text/xml`); those are
/// not disguised binaries, so we filter them out — a `Some(_)` here means
/// "recognised non-text binary" (png/zip/pdf/…), which the text validators
/// and the prober reject.
pub(super) fn infer_binary(bytes: &[u8]) -> Option<String> {
    let mime = infer::get(bytes)?.mime_type();
    if mime.starts_with("text/") {
        None
    } else {
        Some(mime.to_string())
    }
}

/// Every content type the head bytes match, for polyglot detection.
/// `infer` returns a single best match; we additionally probe the PDF and
/// ZIP head signatures explicitly so a file that is simultaneously both is
/// caught.
pub(super) fn polyglot_matches(bytes: &[u8]) -> Vec<String> {
    let mut all = Vec::new();
    if let Some(t) = infer::get(bytes) {
        all.push(t.mime_type().to_string());
    }
    if is_pdf(bytes) && !all.iter().any(|t| t == "application/pdf") {
        all.push("application/pdf".to_string());
    }
    if bytes.starts_with(b"PK\x03\x04") && !all.iter().any(|t| t == "application/zip") {
        all.push("application/zip".to_string());
    }
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tolerance_arithmetic() {
        assert!(within_tolerance(100, 100, 0));
        assert!(!within_tolerance(100, 101, 0));
        assert!(within_tolerance(100, 101, 1));
        assert!(within_tolerance(101, 100, 1));
        assert!(!within_tolerance(100, 105, 4));
    }

    #[test]
    fn utf8_prefix_at_boundary_tolerated() {
        // "é" (0xC3 0xA9) truncated to its lead byte: a valid prefix.
        assert!(looks_like_utf8_text(&[b'h', b'i', 0xC3]));
        assert!(looks_like_utf8_text(b"plain ascii"));
        assert!(!looks_like_utf8_text(&[0xff, 0xfe, 0x00]));
    }

    #[test]
    fn pdf_magic() {
        assert!(is_pdf(b"%PDF-1.7 ..."));
        assert!(!is_pdf(b"not a pdf"));
    }
}
