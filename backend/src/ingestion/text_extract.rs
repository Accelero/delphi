//! Stage 5: bounded, in-backend text extraction.
//!
//! **Deliberate, capped exception to "bytes never traverse the backend."**
//! The upload path keeps bytes off the backend (direct browser→S3), but
//! `/complete` reads the committed object back once, under hard bounds, so
//! the autofill extractor (and full-text search) has something to read.
//! See `docs/SECURITY.md` ("Ingestion read-back exception").
//!
//! Bounds:
//! - Download is capped at `ObjectPolicy.pdf_max_input_bytes` via a single
//!   ranged GET — never an unbounded body read.
//! - PDFs run through the existing sandboxed `pdftotext` discipline
//!   (`PdftotextExtractor`: timeout + `kill_on_drop` + capped stdout),
//!   then the `Vec<Word>` stream is joined into flat text.
//! - text/markdown is a bounded read of the capped bytes + UTF-8 validate.
//!
//! Extraction failure is **non-fatal** to the pipeline: the caller treats
//! an `Err` as "empty text" and proceeds (the bytes are already committed;
//! the user can re-extract later).

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::object_store::ObjectStore;
use crate::storage::Content;
use crate::text_extractor::{PdftotextExtractor, TextExtractor};

/// Extract flat text from the committed object, bounded by `max_input_bytes`.
/// Returns a [`Content`] ready for the `document_content` insert.
pub async fn extract_text(
    object_store: &dyn ObjectStore,
    key: &str,
    content_type: &str,
    max_input_bytes: u64,
) -> Result<Content> {
    // Bounded download: one ranged GET, never the full unbounded body.
    let bytes = object_store.get_range(key, 0..max_input_bytes).await?;

    match content_type {
        "application/pdf" => extract_pdf(bytes).await,
        "text/plain" | "text/markdown" => extract_text_like(bytes, content_type),
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
    use crate::object_store::MemObjectStore;
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
        let store = MemObjectStore::new();
        store
            .put("k/txt", Bytes::from_static(b"plain body text"))
            .await
            .unwrap();
        let content = extract_text(&store, "k/txt", "text/plain", 1024)
            .await
            .unwrap();
        assert_eq!(content.text, "plain body text");
        assert_eq!(content.format, "text");
        assert_eq!(content.extractor, "passthrough");
    }

    #[tokio::test]
    async fn markdown_format_recorded() {
        let store = MemObjectStore::new();
        store
            .put("k/md", Bytes::from_static(b"# heading"))
            .await
            .unwrap();
        let content = extract_text(&store, "k/md", "text/markdown", 1024)
            .await
            .unwrap();
        assert_eq!(content.format, "markdown");
    }

    #[tokio::test]
    async fn download_is_bounded_by_cap() {
        // The ranged GET caps the read: only the first N bytes reach us.
        let store = MemObjectStore::new();
        store
            .put("k/big", Bytes::from(vec![b'a'; 100]))
            .await
            .unwrap();
        let content = extract_text(&store, "k/big", "text/plain", 10)
            .await
            .unwrap();
        assert_eq!(content.text.len(), 10);
    }
}
