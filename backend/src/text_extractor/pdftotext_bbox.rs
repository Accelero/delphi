//! `pdftotext -bbox-layout` text extractor.
//!
//! Shells out to poppler's `pdftotext` (already on the backend
//! container — used by the arXiv adapter for body extraction) and
//! parses its `-bbox-layout` HTML output into a flat reading-order
//! stream of [`Word`]s with PDF-point bounding boxes.
//!
//! ## Coordinate space
//!
//! Poppler emits `xMin / yMin / xMax / yMax` in PDF user-space points,
//! origin **top-left**. The rest of the codebase (chunker, schema,
//! frontend overlay math) wants PDF-native origin **bottom-left**, so
//! we flip `y` at parse time using the parent `<page height="...">`:
//!
//! ```text
//!   y_pdf = page_height - yMax
//!   h     = yMax - yMin
//! ```
//!
//! That keeps the storage shape consistent with what `react-pdf`'s
//! `pageProxy.view` exposes and the design doc's transform expects.

use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

use crate::error::{Error, Result};

use super::{TextExtractor, Word};

/// 30s is plenty for any sane paper; a pathological PDF can take
/// indefinite time so the wall-clock cap keeps the ingest path bounded.
const DEFAULT_TIMEOUT_SECS: u64 = 30;
/// Hard cap on raw HTML bytes from pdftotext. The bbox-layout form is
/// substantially larger than plain text (≈5–10×), so 16 MB comfortably
/// fits a long monograph without inviting a PDF-bomb.
const DEFAULT_MAX_OUT_BYTES: usize = 16 * 1024 * 1024;
const ADAPTER_NAME: &str = "pdftotext_bbox";

pub struct PdftotextExtractor {
    timeout: Duration,
    max_out_bytes: usize,
}

impl Default for PdftotextExtractor {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECS),
            max_out_bytes: DEFAULT_MAX_OUT_BYTES,
        }
    }
}

impl PdftotextExtractor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_timeout(mut self, t: Duration) -> Self {
        self.timeout = t;
        self
    }

    pub fn with_max_out_bytes(mut self, n: usize) -> Self {
        self.max_out_bytes = n;
        self
    }
}

#[async_trait]
impl TextExtractor for PdftotextExtractor {
    async fn extract(&self, bytes: Bytes) -> Result<Vec<Word>> {
        let html = run_pdftotext_bbox(bytes, self.timeout, self.max_out_bytes).await?;
        Ok(parse_bbox_layout_html(&html))
    }
}

async fn run_pdftotext_bbox(
    bytes: Bytes,
    timeout: Duration,
    max_out: usize,
) -> Result<String> {
    let mut child = Command::new("pdftotext")
        .arg("-q")
        .arg("-bbox-layout")
        .arg("-enc")
        .arg("UTF-8")
        .arg("-")
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let mut stdin = child.stdin.take().ok_or_else(|| Error::Adapter {
        name: ADAPTER_NAME.into(),
        message: "stdin not captured".into(),
    })?;
    let mut stdout = child.stdout.take().ok_or_else(|| Error::Adapter {
        name: ADAPTER_NAME.into(),
        message: "stdout not captured".into(),
    })?;
    let mut stderr = child.stderr.take().ok_or_else(|| Error::Adapter {
        name: ADAPTER_NAME.into(),
        message: "stderr not captured".into(),
    })?;

    let writer = tokio::spawn(async move {
        let _ = stdin.write_all(&bytes).await;
        let _ = stdin.shutdown().await;
    });

    let read_capped = async {
        let mut out: Vec<u8> = Vec::with_capacity(64 * 1024);
        let mut buf = [0u8; 16 * 1024];
        let mut truncated = false;
        loop {
            let n = match stdout.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    return Err(Error::Adapter {
                        name: ADAPTER_NAME.into(),
                        message: format!("stdout read: {e}"),
                    });
                }
            };
            let take = max_out.saturating_sub(out.len()).min(n);
            out.extend_from_slice(&buf[..take]);
            if out.len() >= max_out {
                truncated = true;
                break;
            }
        }
        Ok::<(Vec<u8>, bool), Error>((out, truncated))
    };

    let (out, truncated) = match tokio::time::timeout(timeout, read_capped).await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            let _ = child.kill().await;
            let _ = writer.await;
            return Err(e);
        }
        Err(_) => {
            let _ = child.kill().await;
            let _ = writer.await;
            return Err(Error::Adapter {
                name: ADAPTER_NAME.into(),
                message: format!("timed out after {}s", timeout.as_secs()),
            });
        }
    };
    if truncated {
        let _ = child.kill().await;
    }
    let _ = writer.await;
    let mut err_buf = String::new();
    let _ = stderr.read_to_string(&mut err_buf).await;
    let status = child.wait().await?;
    if !status.success() && out.is_empty() && !truncated {
        return Err(Error::Adapter {
            name: ADAPTER_NAME.into(),
            message: format!("pdftotext failed: {err_buf}"),
        });
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}

// ─── HTML parser ───────────────────────────────────────────────────────────
//
// The bbox-layout output is XML-ish: a single `<doc>`, one `<page>` per
// page carrying its width/height, then nested `<flow><block><line><word>`
// elements. We parse with a simple hand-rolled scanner over UTF-8 bytes
// because:
//
//   - `quick-xml` is already a dep but parsing real-world `pdftotext`
//     output through a strict XML reader is fiddly (entity refs,
//     occasional malformed attributes from oddly-encoded source fonts).
//   - We only need `<page height="X">` (for the y-flip) and `<word
//     xMin yMin xMax yMax>TEXT</word>` — three concepts, easy regex-
//     free state machine.

#[derive(Default)]
struct PageCtx {
    page: i64,
    height: f64,
}

pub(crate) fn parse_bbox_layout_html(html: &str) -> Vec<Word> {
    let mut out: Vec<Word> = Vec::new();
    let mut page_ctx = PageCtx::default();
    let bytes = html.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Find the next `<`
        let Some(lt) = memchr(b'<', &bytes[i..]) else {
            break;
        };
        let tag_start = i + lt + 1;
        // Find the closing `>` for this tag.
        let Some(gt) = memchr(b'>', &bytes[tag_start..]) else {
            break;
        };
        let tag_end = tag_start + gt;
        let tag_body = &html[tag_start..tag_end];
        i = tag_end + 1;

        // Skip closing tags.
        if tag_body.starts_with('/') {
            continue;
        }
        let lower = tag_body
            .split(|c: char| c.is_whitespace() || c == '/')
            .next()
            .unwrap_or("")
            .to_ascii_lowercase();
        match lower.as_str() {
            "page" => {
                page_ctx.page += 1;
                page_ctx.height = parse_attr_f64(tag_body, "height").unwrap_or(0.0);
            }
            "word" => {
                let x_min = parse_attr_f64(tag_body, "xmin").unwrap_or(0.0);
                let y_min = parse_attr_f64(tag_body, "ymin").unwrap_or(0.0);
                let x_max = parse_attr_f64(tag_body, "xmax").unwrap_or(0.0);
                let y_max = parse_attr_f64(tag_body, "ymax").unwrap_or(0.0);
                // Inner text: from `>` to the next `<`.
                let Some(close_lt) = memchr(b'<', &bytes[i..]) else {
                    break;
                };
                let text_raw = &html[i..i + close_lt];
                i += close_lt; // leave `<` for the outer loop to consume
                let text = decode_entities(text_raw.trim()).to_string();
                if text.is_empty() {
                    continue;
                }
                // Flip to PDF-native bottom-left origin.
                let w = (x_max - x_min).max(0.0);
                let h = (y_max - y_min).max(0.0);
                let y = (page_ctx.height - y_max).max(0.0);
                out.push(Word {
                    page: page_ctx.page.max(1),
                    x: x_min,
                    y,
                    w,
                    h,
                    text,
                });
            }
            _ => {}
        }
    }
    out
}

fn parse_attr_f64(tag_body: &str, attr_lower: &str) -> Option<f64> {
    // Simple byte scanner: tag_body is ASCII for attribute names and
    // numeric values (pdftotext doesn't emit non-ASCII attribute names),
    // so byte-indexed slicing is safe even though the inner text might
    // contain UTF-8 — we don't look at inner text here.
    let bytes = tag_body.as_bytes();
    let mut i = 0;
    let n = bytes.len();
    // Skip leading element name (until whitespace).
    while i < n && !bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    while i < n {
        // Skip whitespace.
        while i < n && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= n {
            return None;
        }
        // Read attribute name up to `=` or whitespace.
        let name_start = i;
        while i < n && bytes[i] != b'=' && !bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i == name_start {
            return None;
        }
        let name = &tag_body[name_start..i];
        // Skip whitespace and `=`.
        while i < n && (bytes[i] == b'=' || bytes[i].is_ascii_whitespace()) {
            i += 1;
        }
        if i >= n {
            return None;
        }
        let value = if bytes[i] == b'"' || bytes[i] == b'\'' {
            let quote = bytes[i];
            i += 1;
            let v_start = i;
            while i < n && bytes[i] != quote {
                i += 1;
            }
            let v = &tag_body[v_start..i];
            if i < n {
                i += 1;
            }
            v
        } else {
            let v_start = i;
            while i < n && !bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            &tag_body[v_start..i]
        };
        if name.eq_ignore_ascii_case(attr_lower) {
            return value.parse::<f64>().ok();
        }
    }
    None
}

/// Decode the small set of XML entities pdftotext can emit.
fn decode_entities(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    let bytes = s.as_bytes();
    while i < bytes.len() {
        if bytes[i] == b'&' {
            if let Some(semi) = memchr(b';', &bytes[i + 1..]) {
                let entity = &s[i + 1..i + 1 + semi];
                let replacement = match entity {
                    "amp" => Some("&"),
                    "lt" => Some("<"),
                    "gt" => Some(">"),
                    "quot" => Some("\""),
                    "apos" => Some("'"),
                    _ => None,
                };
                if let Some(r) = replacement {
                    out.push_str(r);
                    i += 1 + semi + 1;
                    continue;
                }
                if let Some(stripped) = entity.strip_prefix('#') {
                    let code = if let Some(hex) = stripped.strip_prefix('x') {
                        u32::from_str_radix(hex, 16).ok()
                    } else {
                        stripped.parse::<u32>().ok()
                    };
                    if let Some(c) = code.and_then(char::from_u32) {
                        out.push(c);
                        i += 1 + semi + 1;
                        continue;
                    }
                }
            }
        }
        // Copy one char (UTF-8 safe).
        let ch_len = s[i..]
            .chars()
            .next()
            .map(|c| c.len_utf8())
            .unwrap_or(1);
        out.push_str(&s[i..i + ch_len]);
        i += ch_len;
    }
    out
}

fn memchr(needle: u8, hay: &[u8]) -> Option<usize> {
    hay.iter().position(|&b| b == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"<doc>
<page width="612.000000" height="792.000000">
  <flow>
    <block xMin="72.0" yMin="72.0" xMax="540.0" yMax="120.0">
      <line xMin="72.0" yMin="72.0" xMax="540.0" yMax="84.0">
        <word xMin="72.0" yMin="72.0" xMax="120.0" yMax="84.0">Hello</word>
        <word xMin="130.0" yMin="72.0" xMax="200.0" yMax="84.0">world</word>
      </line>
      <line xMin="72.0" yMin="100.0" xMax="540.0" yMax="112.0">
        <word xMin="72.0" yMin="100.0" xMax="160.0" yMax="112.0">Second</word>
      </line>
    </block>
  </flow>
</page>
<page width="612.000000" height="792.000000">
  <flow>
    <block>
      <line>
        <word xMin="100.0" yMin="50.0" xMax="180.0" yMax="62.0">Page2</word>
      </line>
    </block>
  </flow>
</page>
</doc>"#;

    #[test]
    fn parses_sample_html_into_word_stream() {
        let words = parse_bbox_layout_html(SAMPLE);
        assert_eq!(words.len(), 4);
        assert_eq!(words[0].text, "Hello");
        assert_eq!(words[0].page, 1);
        // Top-down y_min=72, page height 792 → bottom-up y = 792 - 84 = 708.
        assert!((words[0].y - 708.0).abs() < 0.01);
        assert!((words[0].h - 12.0).abs() < 0.01);
        assert_eq!(words[1].text, "world");
        assert_eq!(words[2].text, "Second");
        assert_eq!(words[3].text, "Page2");
        assert_eq!(words[3].page, 2);
    }

    #[test]
    fn handles_xml_entities_in_word_text() {
        let html = r#"<doc><page width="612" height="792"><flow><block><line>
            <word xMin="0" yMin="0" xMax="10" yMax="10">A&amp;B</word>
            <word xMin="0" yMin="0" xMax="10" yMax="10">&#65;</word>
        </line></block></flow></page></doc>"#;
        let w = parse_bbox_layout_html(html);
        assert_eq!(w.len(), 2);
        assert_eq!(w[0].text, "A&B");
        assert_eq!(w[1].text, "A");
    }

    #[test]
    fn page_numbers_increment_monotonically() {
        let words = parse_bbox_layout_html(SAMPLE);
        let mut last = 0;
        for word in words {
            assert!(word.page >= last, "non-monotone page");
            last = word.page;
        }
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn runs_against_real_minimal_pdf() {
        // The repo fixture is a 1-page born-digital PDF "Delphi viewer e2e
        // fixture". Plenty to verify the shell-out + parser actually
        // produce a non-empty word stream on a real file.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("tests/fixtures/minimal.pdf");
        if !path.exists() {
            // Some CI shapes (or future repo moves) might not have the
            // fixture; skip rather than fail.
            eprintln!("skipping: fixture missing at {}", path.display());
            return;
        }
        let bytes = tokio::fs::read(&path).await.expect("read fixture");
        let ext = PdftotextExtractor::new();
        let words = ext.extract(bytes::Bytes::from(bytes)).await.expect("extract");
        // The fixture contains text "Delphi viewer e2e fixture". Even
        // when poppler can't parse layout it should still emit at least
        // one of those tokens.
        assert!(!words.is_empty(), "expected at least one word");
        let joined = words
            .iter()
            .map(|w| w.text.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            joined.contains("Delphi") || joined.contains("fixture"),
            "expected fixture text in word stream; got: {joined:?}"
        );
        // All page numbers >= 1, coords plausible.
        for w in &words {
            assert!(w.page >= 1);
            assert!(w.x >= 0.0 && w.x < 10_000.0);
            assert!(w.y >= 0.0 && w.y < 10_000.0);
        }
    }
}
