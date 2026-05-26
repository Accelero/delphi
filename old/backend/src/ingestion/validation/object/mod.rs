//! Object validator: the corpus-admission gate for uploaded bytes.
//!
//! Runs at `POST /uploads/:id/complete` against the freshly-committed S3
//! object. **Dispatch-on-file-ending**: the ending *selects* a validator,
//! the bytes *confirm* it (the ending is an attacker-controlled claim, so
//! it routes but never decides). Unrecognised endings fall to the
//! [`probe`] (sniff-and-recover). See
//! `docs/architecture/object-validator.md`.
//!
//! Pipeline:
//!  1. `head` — verify actual size against `declared_size` within
//!     `size_tolerance_bytes`, capture the ETag.
//!  2. `get_range(0..sniff_window_bytes)` — fetch the magic-byte window.
//!  3. Dispatch on the file ending → a [`FormatValidator`] (PDF / text) or
//!     the prober. Each confirms the bytes match before accepting.
//!  4. Central allowlist gate: the resolved canonical type must be
//!     allowlisted (honours the per-deployment `allowed_content_types`).
//!
//! The deeper PDF active-content scan ([`scan_pdf_active_content`]) needs
//! the *full* bytes and runs as a pipeline stage in `completion.rs`, not
//! inside a [`FormatValidator`] (which only sees the sniff window).

mod format;
mod pdf;
mod sniff;
mod text;

use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;

use crate::object_store::{ObjectMeta, ObjectStore};

use format::FormatValidator;

#[derive(Debug, Clone)]
pub struct ObjectPolicy {
    pub allowed_content_types: HashSet<String>,
    pub size_tolerance_bytes: u64,
    pub sniff_window_bytes: usize,
    pub pdf_parse_timeout: Duration,
    pub pdf_max_pages: usize,
    pub pdf_max_input_bytes: u64,
    pub reject_polyglots: bool,
    /// Reject PDFs carrying active content (`/JavaScript`, `/OpenAction`,
    /// …). On by default; a single-user deployment that trusts its own
    /// PDFs can turn it off. See [`scan_pdf_active_content`].
    pub reject_pdf_active_content: bool,
}

impl Default for ObjectPolicy {
    fn default() -> Self {
        Self {
            allowed_content_types: ["application/pdf", "text/plain", "text/markdown", "text/html"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            size_tolerance_bytes: 0,
            sniff_window_bytes: 4096,
            pdf_parse_timeout: Duration::from_secs(30),
            pdf_max_pages: 2000,
            pdf_max_input_bytes: 50 * 1024 * 1024,
            reject_polyglots: true,
            reject_pdf_active_content: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectReject {
    SizeMismatch { declared: u64, actual: u64 },
    ContentTypeMismatch { declared: String, sniffed: String },
    NotInAllowlist,
    Polyglot { matched: Vec<String> },
    PdfParseFailed,
    PdfParseTimeout,
    PdfTooManyPages,
    /// PDF carries active content (embedded JS / auto-run actions / launch
    /// / embedded files). Rejected at admission — see
    /// [`scan_pdf_active_content`].
    PdfActiveContent,
    Utf8DecodeFailed,
    HeadFailed(String),
    SniffFailed(String),
}

impl ObjectReject {
    /// Stable, short string used by the rejection log + the SPA's status
    /// poll response. Don't expose the structured payload directly — it
    /// can carry sniffed bytes that look like attacker-controlled input.
    pub fn reason_code(&self) -> &'static str {
        match self {
            Self::SizeMismatch { .. } => "size_mismatch",
            Self::ContentTypeMismatch { .. } => "content_type_mismatch",
            Self::NotInAllowlist => "not_in_allowlist",
            Self::Polyglot { .. } => "polyglot",
            Self::PdfParseFailed => "pdf_parse_failed",
            Self::PdfParseTimeout => "pdf_parse_timeout",
            Self::PdfTooManyPages => "pdf_too_many_pages",
            Self::PdfActiveContent => "pdf_active_content",
            Self::Utf8DecodeFailed => "utf8_decode_failed",
            Self::HeadFailed(_) => "head_failed",
            Self::SniffFailed(_) => "sniff_failed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ValidatedAttrs {
    pub size: u64,
    pub etag: String,
    pub sniffed_content_type: String,
}

/// Validate the committed object. `filename` is the untrusted client
/// filename — only its lowercased extension is read, to *route* to a
/// validator; the bytes are authoritative. `None`/unknown ending → the
/// sniff-and-recover [`probe`].
pub async fn validate_uploaded_object(
    filename: Option<&str>,
    key: &str,
    declared_size: u64,
    object_store: &dyn ObjectStore,
    policy: &ObjectPolicy,
) -> Result<ValidatedAttrs, ObjectReject> {
    // 1. HEAD: actual size + ETag.
    let head = object_store
        .head(key)
        .await
        .map_err(|e| ObjectReject::HeadFailed(e.to_string()))?;
    if !sniff::within_tolerance(declared_size, head.size, policy.size_tolerance_bytes) {
        return Err(ObjectReject::SizeMismatch {
            declared: declared_size,
            actual: head.size,
        });
    }

    // 2. Sniff window (cheap — capped at `sniff_window_bytes`).
    let window_end = (policy.sniff_window_bytes as u64).min(head.size);
    let sniff_bytes = if window_end == 0 {
        Bytes::new()
    } else {
        object_store
            .get_range(key, 0..window_end)
            .await
            .map_err(|e| ObjectReject::SniffFailed(e.to_string()))?
    };

    // 3. Dispatch on the ending; the validator confirms against the bytes.
    let ending = format::ending_of(filename);
    let validated = match format::dispatch(ending.as_deref()) {
        Some(validator) => validator.validate(&sniff_bytes, &head, policy)?,
        None => probe(&sniff_bytes, &head, policy)?,
    };

    // 4. Central allowlist gate — honours the per-deployment allowlist
    //    (a deployment may narrow it, e.g. disallow `text/html`).
    if !policy
        .allowed_content_types
        .contains(&validated.sniffed_content_type)
    {
        return Err(ObjectReject::NotInAllowlist);
    }

    Ok(validated)
}

/// The prober: the dispatcher's else-arm for unrecognised / missing
/// endings. Sniff-and-recover — a real PDF (by magic) still works, any
/// valid UTF-8 text is admitted as `text/plain`, everything else rejects.
fn probe(
    sniff: &[u8],
    head: &ObjectMeta,
    policy: &ObjectPolicy,
) -> Result<ValidatedAttrs, ObjectReject> {
    if sniff::is_pdf(sniff) {
        return pdf::PdfValidator.validate(sniff, head, policy);
    }
    if sniff::infer_binary(sniff).is_some() {
        // A recognised binary that isn't an allowlisted format.
        return Err(ObjectReject::NotInAllowlist);
    }
    if sniff::looks_like_utf8_text(sniff) {
        return Ok(ValidatedAttrs {
            size: head.size,
            etag: head.etag.clone(),
            sniffed_content_type: "text/plain".to_string(),
        });
    }
    Err(ObjectReject::NotInAllowlist)
}

/// PDFiD-style active-content scan over the full (bounded) PDF bytes:
/// a lexical substring search for the keywords that signal embedded
/// scripting / auto-run actions / launch / embedded files. Returns the
/// first token found, or `None` if clean. It is **not** a PDF parser — no
/// new parse surface — and is bounded by the caller's `pdf_max_input_bytes`
/// download. Runs as a pipeline stage in `completion.rs`.
pub fn scan_pdf_active_content(bytes: &[u8]) -> Option<&'static str> {
    pdf::scan_active_content(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::MemObjectStore;
    use std::sync::Arc;

    fn pdf_bytes() -> Bytes {
        let mut v = b"%PDF-1.4\n%\xE2\xE3\xCF\xD3\n".to_vec();
        v.extend_from_slice(&[0u8; 64]);
        Bytes::from(v)
    }

    fn text_bytes() -> Bytes {
        Bytes::from_static(b"hello world\nthis is a plain text file\n")
    }

    async fn validate(
        store: &MemObjectStore,
        filename: Option<&str>,
        key: &str,
        size: u64,
    ) -> Result<ValidatedAttrs, ObjectReject> {
        validate_uploaded_object(filename, key, size, store, &ObjectPolicy::default()).await
    }

    #[tokio::test]
    async fn pdf_ending_real_pdf_accepted() {
        let store = MemObjectStore::new();
        let body = pdf_bytes();
        store.put("k/pdf", body.clone()).await.unwrap();
        let res = validate(&store, Some("paper.pdf"), "k/pdf", body.len() as u64)
            .await
            .expect("happy path");
        assert_eq!(res.sniffed_content_type, "application/pdf");
        assert_eq!(res.size, body.len() as u64);
    }

    #[tokio::test]
    async fn txt_ending_text_accepted() {
        let store = MemObjectStore::new();
        let body = text_bytes();
        store.put("k/txt", body.clone()).await.unwrap();
        let res = validate(&store, Some("notes.txt"), "k/txt", body.len() as u64)
            .await
            .expect("happy path");
        assert_eq!(res.sniffed_content_type, "text/plain");
    }

    #[tokio::test]
    async fn md_ending_emits_markdown_subtype() {
        let store = MemObjectStore::new();
        let body = text_bytes();
        store.put("k/md", body.clone()).await.unwrap();
        let res = validate(&store, Some("README.md"), "k/md", body.len() as u64)
            .await
            .expect("happy path");
        assert_eq!(res.sniffed_content_type, "text/markdown");
    }

    #[tokio::test]
    async fn arbitrary_text_ending_is_text_plain() {
        // Liberal: any UTF-8 text ingests, even an ending we don't special-case.
        let store = MemObjectStore::new();
        let body = Bytes::from_static(b"def main():\n    pass\n");
        store.put("k/py", body.clone()).await.unwrap();
        let res = validate(&store, Some("script.py"), "k/py", body.len() as u64)
            .await
            .expect("source code is text");
        assert_eq!(res.sniffed_content_type, "text/plain");
    }

    #[tokio::test]
    async fn pdf_ending_but_text_bytes_rejected() {
        // Reject-on-mismatch: a positive `.pdf` claim must be a real PDF.
        let store = MemObjectStore::new();
        let body = text_bytes();
        store.put("k/lie", body.clone()).await.unwrap();
        let err = validate(&store, Some("fake.pdf"), "k/lie", body.len() as u64)
            .await
            .expect_err("should reject");
        assert!(
            matches!(err, ObjectReject::ContentTypeMismatch { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn txt_ending_but_binary_rejected() {
        // Disguised binary under a text ending.
        let store = MemObjectStore::new();
        let png = Bytes::from_static(&[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 0]);
        store.put("k/png", png.clone()).await.unwrap();
        let err = validate(&store, Some("evil.txt"), "k/png", png.len() as u64)
            .await
            .expect_err("should reject");
        assert!(
            matches!(err, ObjectReject::ContentTypeMismatch { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn no_ending_pdf_recovered_by_magic() {
        let store = MemObjectStore::new();
        let body = pdf_bytes();
        store.put("k/nopdf", body.clone()).await.unwrap();
        let res = validate(&store, None, "k/nopdf", body.len() as u64)
            .await
            .expect("prober recovers a real PDF");
        assert_eq!(res.sniffed_content_type, "application/pdf");
    }

    #[tokio::test]
    async fn no_ending_text_recovered_as_plain() {
        let store = MemObjectStore::new();
        let body = text_bytes();
        store.put("k/notxt", body.clone()).await.unwrap();
        let res = validate(&store, None, "k/notxt", body.len() as u64)
            .await
            .expect("prober recovers text");
        assert_eq!(res.sniffed_content_type, "text/plain");
    }

    #[tokio::test]
    async fn no_ending_binary_rejected() {
        let store = MemObjectStore::new();
        let body = Bytes::from_static(&[0xff, 0xfe, 0x00, 0x01, 0x02, 0x9c, 0xed]);
        store.put("k/bin", body.clone()).await.unwrap();
        let err = validate(&store, None, "k/bin", body.len() as u64)
            .await
            .expect_err("unknown binary rejects");
        assert!(matches!(err, ObjectReject::NotInAllowlist), "got {err:?}");
    }

    #[tokio::test]
    async fn size_mismatch_rejected() {
        let store = MemObjectStore::new();
        let body = pdf_bytes();
        store.put("k/size", body.clone()).await.unwrap();
        let err = validate(&store, Some("a.pdf"), "k/size", body.len() as u64 + 1)
            .await
            .expect_err("should reject");
        assert!(matches!(err, ObjectReject::SizeMismatch { .. }));
    }

    #[tokio::test]
    async fn oversize_pdf_rejected() {
        let store = MemObjectStore::new();
        let policy = ObjectPolicy {
            pdf_max_input_bytes: 32,
            ..ObjectPolicy::default()
        };
        let body = pdf_bytes(); // > 32 bytes
        store.put("k/big", body.clone()).await.unwrap();
        let err =
            validate_uploaded_object(Some("a.pdf"), "k/big", body.len() as u64, &store, &policy)
                .await
                .expect_err("should reject");
        assert!(matches!(err, ObjectReject::SizeMismatch { .. }));
    }

    #[tokio::test]
    async fn disallowed_subtype_rejected_by_central_gate() {
        // A deployment that narrows the allowlist to exclude text/html: a
        // valid .html upload is rejected by the central gate even though
        // the text validator accepts the bytes.
        let store = MemObjectStore::new();
        let body = Bytes::from_static(b"<html><body>hi</body></html>");
        store.put("k/html", body.clone()).await.unwrap();
        let mut policy = ObjectPolicy::default();
        policy.allowed_content_types.remove("text/html");
        let err = validate_uploaded_object(
            Some("page.html"),
            "k/html",
            body.len() as u64,
            &store,
            &policy,
        )
        .await
        .expect_err("html disallowed");
        assert!(matches!(err, ObjectReject::NotInAllowlist), "got {err:?}");
    }

    #[tokio::test]
    async fn html_ending_emits_html_subtype() {
        let store = MemObjectStore::new();
        let body = Bytes::from_static(b"<html><body>hi</body></html>");
        store.put("k/h", body.clone()).await.unwrap();
        let res = validate(&store, Some("page.html"), "k/h", body.len() as u64)
            .await
            .expect("html accepted");
        assert_eq!(res.sniffed_content_type, "text/html");
    }

    #[tokio::test]
    async fn arc_dyn_object_store_accepted() {
        let store: Arc<dyn ObjectStore> = Arc::new(MemObjectStore::new());
        let _ = validate_uploaded_object(
            Some("x.pdf"),
            "missing",
            0,
            &*store,
            &ObjectPolicy::default(),
        )
        .await;
    }

    #[test]
    fn reason_codes_stable() {
        assert_eq!(ObjectReject::NotInAllowlist.reason_code(), "not_in_allowlist");
        assert_eq!(ObjectReject::PdfActiveContent.reason_code(), "pdf_active_content");
        assert_eq!(
            ObjectReject::SizeMismatch {
                declared: 1,
                actual: 2
            }
            .reason_code(),
            "size_mismatch"
        );
    }
}
