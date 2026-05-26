//! Shared text validator. Every text ending (`.txt`, `.md`, `.html`, and
//! anything liberal-routed to text) runs the *same* checks — valid UTF-8,
//! not a disguised binary. The only per-ending difference is the subtype
//! it emits, which selects the downstream extractor (HTML strips tags;
//! the rest pass through). There is no per-text-format security check:
//! the boundary for text is the size cap (resource), the disguised-binary
//! sniff (here), and the render sink (XSS, in the SPA).

use crate::object_store::ObjectMeta;

use super::format::FormatValidator;
use super::sniff;
use super::{ObjectPolicy, ObjectReject, ValidatedAttrs};

pub(super) struct TextValidator {
    subtype: &'static str,
}

impl TextValidator {
    pub(super) fn plain() -> Self {
        Self {
            subtype: "text/plain",
        }
    }
    pub(super) fn markdown() -> Self {
        Self {
            subtype: "text/markdown",
        }
    }
    pub(super) fn html() -> Self {
        Self {
            subtype: "text/html",
        }
    }
}

impl FormatValidator for TextValidator {
    fn validate(
        &self,
        sniff: &[u8],
        head: &ObjectMeta,
        _policy: &ObjectPolicy,
    ) -> Result<ValidatedAttrs, ObjectReject> {
        // A recognised binary signature under a text ending ⇒ disguised
        // binary (e.g. a PNG named `notes.txt`).
        if let Some(binary) = sniff::infer_binary(sniff) {
            return Err(ObjectReject::ContentTypeMismatch {
                declared: self.subtype.to_string(),
                sniffed: binary,
            });
        }
        if !sniff::looks_like_utf8_text(sniff) {
            return Err(ObjectReject::Utf8DecodeFailed);
        }
        Ok(ValidatedAttrs {
            size: head.size,
            etag: head.etag.clone(),
            sniffed_content_type: self.subtype.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head(size: u64) -> ObjectMeta {
        ObjectMeta {
            size,
            etag: "\"e\"".to_string(),
            content_type: None,
            last_modified: None,
        }
    }

    #[test]
    fn utf8_text_accepted_with_subtype() {
        let body = b"hello world\n";
        let res = TextValidator::html()
            .validate(body, &head(body.len() as u64), &ObjectPolicy::default())
            .expect("text accepted");
        assert_eq!(res.sniffed_content_type, "text/html");
    }

    #[test]
    fn disguised_binary_rejected() {
        let png = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 0];
        let err = TextValidator::plain()
            .validate(&png, &head(png.len() as u64), &ObjectPolicy::default())
            .expect_err("png is not text");
        assert!(matches!(err, ObjectReject::ContentTypeMismatch { .. }));
    }

    #[test]
    fn invalid_utf8_rejected() {
        let bytes = [0xff, 0xfe, 0x9c, 0xed];
        let err = TextValidator::plain()
            .validate(&bytes, &head(bytes.len() as u64), &ObjectPolicy::default())
            .expect_err("not utf8");
        assert!(matches!(err, ObjectReject::Utf8DecodeFailed));
    }
}
