//! Stage 5: bounded, in-backend text extraction.
//!
//! **Deliberate, capped exception to "bytes never traverse the backend."**
//! The upload path keeps bytes off the backend (direct browser→S3), but
//! `/complete` reads the committed object back once, under hard bounds, so
//! the autofill extractor (and full-text search) has something to read.
//! See `docs/architecture/ingestion.md` ("The completion pipeline").
//!
//! The bounded read-back happens in `completion.rs` (one ranged GET capped
//! at `ObjectPolicy.pdf_max_input_bytes`, shared with the PDF
//! active-content scan). This module is handed those already-bounded bytes
//! and only decides *how* to turn them into text:
//! - PDFs run through the sandboxed `pdftotext` discipline
//!   (`PdftotextExtractor`: timeout + `kill_on_drop` + capped stdout), then
//!   the `Vec<Word>` stream is joined into flat text.
//! - text/markdown is a UTF-8 passthrough.
//! - text/html is stripped to flat text via `html2text` (tags are index
//!   noise; stripping is fidelity, not security — XSS is a render-sink
//!   concern handled in the SPA).
//!
//! Extraction failure is **non-fatal** to the pipeline: the caller treats
//! an `Err` as "empty text" and proceeds (the bytes are already committed;
//! the user can re-extract later).

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::storage::Content;
use crate::text_extractor::{PdftotextExtractor, TextExtractor};

/// Turn already-fetched, already-bounded object `bytes` into flat text,
/// dispatching on the validator's resolved `content_type`.
pub async fn extract_text(content_type: &str, bytes: Bytes) -> Result<Content> {
    match content_type {
        "application/pdf" => extract_pdf(bytes).await,
        "text/plain" | "text/markdown" => extract_text_like(bytes, content_type),
        "text/html" => extract_html(bytes),
        other => Err(Error::NotImplemented(format!(
            "text extraction not supported for content-type {other}"
        ))),
    }
}

async fn extract_pdf(bytes: Bytes) -> Result<Content> {
    // PdftotextExtractor owns the sandbox discipline (timeout, capped
    // stdout, kill_on_drop). Join its `Vec<Word>` into flat reading-order
    // text.
    let extractor = PdftotextExtractor::new();
    let words = extractor.extract(bytes).await?;
    let text = join_words(&words);
    Ok(Content {
        text,
        format: "text".into(),
        extractor: "pdftotext".into(),
    })
}

fn extract_text_like(bytes: Bytes, content_type: &str) -> Result<Content> {
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| Error::Adapter {
            name: "text-extract".into(),
            message: "object is not valid UTF-8".into(),
        })?
        .to_string();
    let format = if content_type == "text/markdown" {
        "markdown"
    } else {
        "text"
    };
    Ok(Content {
        text,
        format: format.into(),
        extractor: "passthrough".into(),
    })
}

/// Strip HTML to flat reading-order text via `html2text`. No network fetch,
/// no resource loading; the input is the same capped read every arm uses.
fn extract_html(bytes: Bytes) -> Result<Content> {
    // UTF-8 tail check (the validator confirmed only the sniff window).
    if std::str::from_utf8(&bytes).is_err() {
        return Err(Error::Adapter {
            name: "html-extract".into(),
            message: "html object is not valid UTF-8".into(),
        });
    }
    let text = html2text::from_read(&bytes[..], 80).map_err(|e| Error::Adapter {
        name: "html-extract".into(),
        message: format!("html render failed: {e}"),
    })?;
    Ok(Content {
        text,
        format: "text".into(),
        extractor: "html2text".into(),
    })
}

/// Join a reading-order `Word` stream into flat text. Inserts a newline
/// when the page changes, a space between words on the same page.
fn join_words(words: &[crate::text_extractor::Word]) -> String {
    let mut out = String::new();
    let mut last_page: Option<i64> = None;
    for w in words {
        match last_page {
            Some(p) if p == w.page => out.push(' '),
            Some(_) => out.push('\n'),
            None => {}
        }
        out.push_str(&w.text);
        last_page = Some(w.page);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::text_extractor::Word;

    fn word(page: i64, text: &str) -> Word {
        Word {
            page,
            x: 0.0,
            y: 0.0,
            w: 0.0,
            h: 0.0,
            text: text.into(),
        }
    }

    #[test]
    fn join_words_spaces_and_newlines() {
        let words = vec![word(1, "hello"), word(1, "world"), word(2, "next")];
        assert_eq!(join_words(&words), "hello world\nnext");
    }

    #[tokio::test]
    async fn text_passthrough_extracts() {
        let content = extract_text("text/plain", Bytes::from_static(b"plain body text"))
            .await
            .unwrap();
        assert_eq!(content.text, "plain body text");
        assert_eq!(content.format, "text");
        assert_eq!(content.extractor, "passthrough");
    }

    #[tokio::test]
    async fn markdown_format_recorded() {
        let content = extract_text("text/markdown", Bytes::from_static(b"# heading"))
            .await
            .unwrap();
        assert_eq!(content.format, "markdown");
    }

    #[tokio::test]
    async fn html_stripped_to_text() {
        let content = extract_text(
            "text/html",
            Bytes::from_static(b"<html><body><h1>Title</h1><p>Hello world</p></body></html>"),
        )
        .await
        .unwrap();
        assert_eq!(content.extractor, "html2text");
        assert!(content.text.contains("Title"), "got: {:?}", content.text);
        assert!(content.text.contains("Hello world"), "got: {:?}", content.text);
        assert!(!content.text.contains("<p>"), "tags not stripped: {:?}", content.text);
    }
}
