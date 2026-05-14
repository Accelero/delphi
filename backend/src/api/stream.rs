//! Vercel AI SDK Data Stream Protocol formatting.
//!
//! Records are newline-delimited, each prefixed with a single character
//! identifying the kind. We only emit a small subset for v1:
//!
//!   `0:<json string>\n`  → text delta
//!   `2:<json array>\n`   → data block (citations payload lives here)
//!   `3:<json string>\n`  → error message
//!   `d:<json object>\n`  → finish marker (with finish reason)
//!
//! Reference: <https://sdk.vercel.ai/docs/ai-sdk-ui/stream-protocol>

use serde::Serialize;
use serde_json::json;

/// One text-delta record. Body is a JSON-encoded string.
pub fn text(s: &str) -> String {
    format!("0:{}\n", serde_json::to_string(s).unwrap_or_else(|_| "\"\"".into()))
}

/// One error record. The body is a JSON-encoded string.
pub fn error(msg: &str) -> String {
    format!("3:{}\n", serde_json::to_string(msg).unwrap_or_else(|_| "\"\"".into()))
}

/// Final finish marker. `reason` is e.g. "stop", "length", "error".
pub fn finish(reason: &str) -> String {
    let body = json!({ "finishReason": reason });
    format!("d:{}\n", body)
}

/// Per-marker payload streamed inside the `citations` data block.
#[derive(Debug, Clone, Serialize)]
pub struct CitationEntry {
    /// Bracket number rendered as `[n]` in the assistant text.
    pub n: usize,
    /// `chunk:<key>` — what the frontend feeds to `/api/chunks/:id`.
    pub chunk_id: String,
    /// `document:<key>` — used by the deep-link `/feed?doc=&chunk=`.
    pub doc_id: String,
    /// Best-effort display title for tooltip / context.
    pub doc_title: Option<String>,
    /// First page the chunk appears on (1-indexed). Convenience hint;
    /// the viewer still consults `bboxes` for the authoritative position.
    pub page: Option<i64>,
}

/// One data block (`2:<json array>\n`) carrying the citation table for
/// the assistant's reply. Sent **before** the first `0:` text delta so
/// the client has the table ready by the time it sees `[N]` markers.
pub fn citations(entries: &[CitationEntry]) -> String {
    let body =
        serde_json::to_string(&json!([{ "type": "citations", "chunks": entries }]))
            .unwrap_or_else(|_| "[]".into());
    format!("2:{body}\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_delta_snapshot() {
        // Byte-level snapshot — guards against accidental protocol drift.
        assert_eq!(text("hello"), "0:\"hello\"\n");
        assert_eq!(text("with\nnewline"), "0:\"with\\nnewline\"\n");
    }

    #[test]
    fn error_snapshot() {
        assert_eq!(error("boom"), "3:\"boom\"\n");
    }

    #[test]
    fn finish_snapshot() {
        assert_eq!(finish("stop"), "d:{\"finishReason\":\"stop\"}\n");
        assert_eq!(finish("error"), "d:{\"finishReason\":\"error\"}\n");
    }

    #[test]
    fn citations_block_shape() {
        let entries = vec![
            CitationEntry {
                n: 1,
                chunk_id: "chunk:abc".into(),
                doc_id: "document:xyz".into(),
                doc_title: Some("Title".into()),
                page: Some(3),
            },
            CitationEntry {
                n: 2,
                chunk_id: "chunk:def".into(),
                doc_id: "document:xyz".into(),
                doc_title: None,
                page: None,
            },
        ];
        let line = citations(&entries);
        // Prefix + trailing newline.
        assert!(line.starts_with("2:"));
        assert!(line.ends_with('\n'));
        // Parse the JSON-encoded array body to verify shape rather than
        // relying on exact whitespace.
        let body = &line[2..line.len() - 1];
        let parsed: serde_json::Value = serde_json::from_str(body).expect("valid json");
        let arr = parsed.as_array().expect("array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["type"], "citations");
        let chunks = arr[0]["chunks"].as_array().expect("chunks array");
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0]["chunk_id"], "chunk:abc");
        assert_eq!(chunks[1]["page"], serde_json::Value::Null);
    }
}
