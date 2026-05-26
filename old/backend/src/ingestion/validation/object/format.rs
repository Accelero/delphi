//! File-ending → validator dispatch.
//!
//! The ending *selects* a validator; the validator *confirms* against the
//! bytes. The ending is an attacker-controlled claim (`evil.bin` →
//! `paper.pdf`), so it can route but never decide. An `impl` that trusts
//! the ending instead of the bytes is a security bug.

use crate::object_store::ObjectMeta;

use super::pdf::PdfValidator;
use super::text::TextValidator;
use super::{ObjectPolicy, ObjectReject, ValidatedAttrs};

/// Confirm a sniff window against the format the file ending claimed.
pub(super) trait FormatValidator {
    fn validate(
        &self,
        sniff: &[u8],
        head: &ObjectMeta,
        policy: &ObjectPolicy,
    ) -> Result<ValidatedAttrs, ObjectReject>;
}

/// Lowercased file extension (no dot) of `filename`'s basename, or `None`
/// when there's no usable ending. **Untrusted input**: we read only the
/// last dot-segment of the basename for routing — never open a path. A
/// dotfile (`.bashrc`), a name with no dot, an over-long or non-alnum
/// extension all resolve to `None` → the prober.
pub(super) fn ending_of(filename: Option<&str>) -> Option<String> {
    let name = filename?;
    // Basename: strip any path the client may have smuggled in.
    let base = name.rsplit(['/', '\\']).next().unwrap_or(name);
    let (stem, ext) = base.rsplit_once('.')?;
    if stem.is_empty() {
        // Leading-dot name like ".gitignore" — no real extension.
        return None;
    }
    let ext = ext.to_ascii_lowercase();
    if ext.is_empty() || ext.len() > 16 || !ext.chars().all(|c| c.is_ascii_alphanumeric()) {
        return None;
    }
    Some(ext)
}

/// The validator a known ending dispatches to. `None` ⇒ unrecognised
/// ending → the dispatcher's prober (sniff-and-recover). Endings we don't
/// special-case (`.py`, `.json`, …) deliberately return `None` so the
/// prober admits them liberally as `text/plain` when they're valid text.
pub(super) fn dispatch(ending: Option<&str>) -> Option<Box<dyn FormatValidator>> {
    match ending {
        Some("pdf") => Some(Box::new(PdfValidator)),
        Some("html") | Some("htm") => Some(Box::new(TextValidator::html())),
        Some("md") | Some("markdown") => Some(Box::new(TextValidator::markdown())),
        Some("txt") => Some(Box::new(TextValidator::plain())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ending_extraction_matrix() {
        assert_eq!(ending_of(Some("paper.pdf")).as_deref(), Some("pdf"));
        assert_eq!(ending_of(Some("PAPER.PDF")).as_deref(), Some("pdf")); // lowercased
        assert_eq!(ending_of(Some("a.tar.gz")).as_deref(), Some("gz")); // last segment
        assert_eq!(ending_of(Some("/etc/passwd.txt")).as_deref(), Some("txt")); // basename only
        assert_eq!(ending_of(Some("..\\..\\x.md")).as_deref(), Some("md"));
        assert_eq!(ending_of(Some("noext")), None);
        assert_eq!(ending_of(Some(".gitignore")), None); // dotfile, no stem
        assert_eq!(ending_of(Some("trailing.")), None); // empty ext
        assert_eq!(ending_of(Some("weird.tar!evil")), None); // non-alnum ext
        assert_eq!(ending_of(Some("x.superlongextension")), None); // > 16
        assert_eq!(ending_of(None), None);
    }

    #[test]
    fn dispatch_routes_known_endings() {
        assert!(dispatch(Some("pdf")).is_some());
        assert!(dispatch(Some("txt")).is_some());
        assert!(dispatch(Some("md")).is_some());
        assert!(dispatch(Some("html")).is_some());
        assert!(dispatch(Some("htm")).is_some());
        assert!(dispatch(Some("py")).is_none()); // → prober
        assert!(dispatch(None).is_none());
    }
}
