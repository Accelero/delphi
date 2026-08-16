//! Verification adapters.
//!
//! These are **complete implementations of well-defined ports whose behaviour
//! is deliberately permissive for now** — not shortcuts. The seams are real:
//! swapping [`PermissiveScanner`] for ClamAV means writing one adapter in this
//! crate and changing one wiring line, with nothing else touched.

use async_trait::async_trait;
use delphi_document_app::{
    BlobHead, BlobScanner, BoxAsyncRead, ContentValidator, ContentVerdict, DeclaredContent,
    ScanError, ScanOutcome, ScanVerdict, ValidateError,
};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

/// The EICAR anti-malware test file. Not malware — a string every scanner is
/// required to flag, which is what makes the reject path testable end to end
/// without shipping a real sample.
const EICAR: &[u8] =
    br"X5O!P%@AP[4\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*";

/// How much is read per chunk while streaming the object.
const CHUNK_BYTES: usize = 64 * 1024;

/// Returns `Clean` plus a genuine digest.
///
/// The digest is not incidental: it is the **only** source of
/// `DocumentCreated.checksum`, so this adapter must hash even while its verdict
/// is a placeholder.
pub struct PermissiveScanner;

#[async_trait]
impl BlobScanner for PermissiveScanner {
    async fn scan(&self, mut blob: BoxAsyncRead) -> Result<ScanOutcome, ScanError> {
        let mut hasher = Sha256::new();
        let mut byte_count = 0_u64;
        let mut buffer = vec![0_u8; CHUNK_BYTES];
        // Keep a window across chunk boundaries so a signature split by the
        // chunk size is still found.
        let mut window: Vec<u8> = Vec::with_capacity(EICAR.len() * 2);
        let mut infected = false;

        loop {
            let read = blob
                .read(&mut buffer)
                .await
                .map_err(|error| ScanError::Read(error.to_string()))?;
            if read == 0 {
                break;
            }
            let chunk = &buffer[..read];
            hasher.update(chunk);
            byte_count += read as u64;

            if !infected {
                window.extend_from_slice(chunk);
                if contains(&window, EICAR) {
                    infected = true;
                }
                if window.len() > EICAR.len() {
                    let keep = window.len() - (EICAR.len() - 1);
                    window.drain(..keep);
                }
            }
        }

        let verdict = if infected {
            ScanVerdict::Infected {
                signature: "Eicar-Test-Signature".to_owned(),
            }
        } else {
            ScanVerdict::Clean
        };
        tracing::info!(
            byte_count,
            clean = !infected,
            "scanned blob with the permissive scanner; a real engine plugs in here"
        );

        Ok(ScanOutcome {
            verdict,
            sha256_hex: hex::encode(hasher.finalize()),
            byte_count,
        })
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Declared-size match plus a magic-byte sniff.
///
/// Format-specific deep validation replaces this later; the port shape does not
/// change when it does.
pub struct BasicContentValidator;

#[async_trait]
impl ContentValidator for BasicContentValidator {
    async fn validate(
        &self,
        head: &BlobHead,
        prefix: &[u8],
        declared: &DeclaredContent,
    ) -> Result<ContentVerdict, ValidateError> {
        if head.byte_size != declared.byte_size {
            return Ok(ContentVerdict::Rejected {
                reason: format!(
                    "declared {} bytes but the object is {}",
                    declared.byte_size, head.byte_size
                ),
            });
        }

        if let Some(sniffed) = sniff(prefix) {
            if !content_type_matches(sniffed, &declared.content_type) {
                return Ok(ContentVerdict::Rejected {
                    reason: format!(
                        "content looks like {sniffed} but was declared as {}",
                        declared.content_type
                    ),
                });
            }
        }

        tracing::info!(
            filename = %declared.filename,
            content_type = %declared.content_type,
            "validated content shape; format-specific validation plugs in here"
        );
        Ok(ContentVerdict::Ok)
    }
}

/// Only formats with an unambiguous magic number. Anything not listed sniffs to
/// `None` and is accepted: a permissive validator must not reject what it
/// simply cannot recognise.
fn sniff(prefix: &[u8]) -> Option<&'static str> {
    const SIGNATURES: &[(&[u8], &str)] = &[
        (b"%PDF-", "application/pdf"),
        (b"\x89PNG\r\n\x1a\n", "image/png"),
        (b"\xff\xd8\xff", "image/jpeg"),
        (b"GIF87a", "image/gif"),
        (b"GIF89a", "image/gif"),
        (b"PK\x03\x04", "application/zip"),
        (b"\x1f\x8b", "application/gzip"),
        (b"%!PS", "application/postscript"),
    ];
    SIGNATURES
        .iter()
        .find(|(magic, _)| prefix.starts_with(magic))
        .map(|(_, mime)| *mime)
}

fn content_type_matches(sniffed: &str, declared: &str) -> bool {
    let declared = declared
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    if declared.is_empty() || declared == "application/octet-stream" {
        // The client did not commit to a type, so there is nothing to
        // contradict.
        return true;
    }
    if declared == sniffed {
        return true;
    }
    // A ZIP container is the physical form of every OOXML and ODF document, so
    // sniffing "zip" cannot contradict those declarations.
    sniffed == "application/zip"
        && (declared.starts_with("application/vnd.openxmlformats-officedocument.")
            || declared.starts_with("application/vnd.oasis.opendocument.")
            || declared == "application/epub+zip"
            || declared == "application/java-archive")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn head(byte_size: u64) -> BlobHead {
        BlobHead {
            byte_size,
            content_type: None,
            last_modified: Utc::now(),
        }
    }

    fn declared(content_type: &str, byte_size: u64) -> DeclaredContent {
        DeclaredContent {
            filename: "file".to_owned(),
            content_type: content_type.to_owned(),
            byte_size,
        }
    }

    #[tokio::test]
    async fn the_scanner_returns_a_real_digest_and_byte_count() {
        let bytes = b"hello world".to_vec();
        let outcome = PermissiveScanner
            .scan(Box::pin(std::io::Cursor::new(bytes.clone())))
            .await
            .expect("scan");

        assert_eq!(outcome.verdict, ScanVerdict::Clean);
        assert_eq!(outcome.byte_count, bytes.len() as u64);
        assert_eq!(outcome.sha256_hex, hex::encode(Sha256::digest(&bytes)));
    }

    #[tokio::test]
    async fn the_eicar_string_is_flagged_even_when_it_straddles_a_chunk() {
        let mut bytes = vec![b'.'; CHUNK_BYTES - 10];
        bytes.extend_from_slice(EICAR);
        bytes.extend_from_slice(&[b'.'; 100]);

        let outcome = PermissiveScanner
            .scan(Box::pin(std::io::Cursor::new(bytes)))
            .await
            .expect("scan");

        assert!(matches!(outcome.verdict, ScanVerdict::Infected { .. }));
    }

    #[tokio::test]
    async fn an_empty_object_scans_to_the_empty_digest() {
        let outcome = PermissiveScanner
            .scan(Box::pin(std::io::Cursor::new(Vec::new())))
            .await
            .expect("scan");
        assert_eq!(outcome.byte_count, 0);
        assert_eq!(outcome.verdict, ScanVerdict::Clean);
    }

    #[tokio::test]
    async fn a_size_mismatch_is_rejected() {
        let verdict = BasicContentValidator
            .validate(&head(10), b"", &declared("application/pdf", 20))
            .await
            .expect("validate");
        assert!(matches!(verdict, ContentVerdict::Rejected { .. }));
    }

    #[tokio::test]
    async fn magic_bytes_that_contradict_the_declared_type_are_rejected() {
        let verdict = BasicContentValidator
            .validate(&head(5), b"%PDF-1.7", &declared("image/png", 5))
            .await
            .expect("validate");
        assert!(matches!(verdict, ContentVerdict::Rejected { .. }));
    }

    #[tokio::test]
    async fn unrecognised_and_uncommitted_content_is_accepted() {
        // No signature at all.
        let verdict = BasicContentValidator
            .validate(&head(5), b"plain", &declared("text/csv", 5))
            .await
            .expect("validate");
        assert_eq!(verdict, ContentVerdict::Ok);

        // A signature, but the client declared nothing specific.
        let verdict = BasicContentValidator
            .validate(
                &head(5),
                b"%PDF-1.7",
                &declared("application/octet-stream", 5),
            )
            .await
            .expect("validate");
        assert_eq!(verdict, ContentVerdict::Ok);
    }

    #[tokio::test]
    async fn a_docx_is_not_rejected_for_being_a_zip() {
        let verdict = BasicContentValidator
            .validate(
                &head(5),
                b"PK\x03\x04rest",
                &declared(
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    5,
                ),
            )
            .await
            .expect("validate");
        assert_eq!(verdict, ContentVerdict::Ok);
    }

    #[tokio::test]
    async fn a_charset_parameter_does_not_break_the_comparison() {
        let verdict = BasicContentValidator
            .validate(&head(5), b"%PDF-1.7", &declared("application/pdf; q=0.9", 5))
            .await
            .expect("validate");
        assert_eq!(verdict, ContentVerdict::Ok);
    }
}
