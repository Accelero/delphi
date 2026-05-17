//! Vercel AI SDK Data Stream Protocol formatting.
//!
//! Records are newline-delimited, each prefixed with a single character
//! identifying the kind. We only emit a small subset for v1:
//!
//!   `0:<json string>\n`  → text delta
//!   `2:<json array>\n`   → data block (citations payload lives here)
//!   `3:<json string>\n`  → error message
//!   `8:<json object>\n`  → task announcement (delphi extension)
//!   `d:<json object>\n`  → finish marker (with finish reason + assistant id)
//!
//! Reference: <https://sdk.vercel.ai/docs/ai-sdk-ui/stream-protocol>
//!
//! The `8:` task frame is a delphi-side extension: it carries the
//! `task_id` the client needs in order to call `/stop/{task_id}`. The
//! AI SDK parser ignores unknown tags, so emitting it is safe against
//! any consumer that follows the official spec.

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

/// Task announcement — emitted **first** on the stream so the client
/// knows the `task_id` before any text arrives. Used by the stop button
/// to address the worker.
pub fn task(task_id: &str) -> String {
    let body = json!({ "taskId": task_id });
    format!("8:{}\n", body)
}

/// Final finish marker. `reason` is e.g. "stop", "length", "error".
/// `assistant_message_id` is the record id of the persisted assistant
/// reply (used by the client as `parent_id` for the next submit). Pass
/// an empty string when no message was persisted (cancelled turn).
pub fn finish(reason: &str, assistant_message_id: &str) -> String {
    let body = json!({
        "finishReason": reason,
        "assistantMessageId": assistant_message_id,
    });
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
    fn task_snapshot() {
        assert_eq!(
            task("01JABCDXYZ"),
            "8:{\"taskId\":\"01JABCDXYZ\"}\n"
        );
    }

    #[test]
    fn finish_snapshot() {
        assert_eq!(
            finish("stop", "message:abc"),
            "d:{\"finishReason\":\"stop\",\"assistantMessageId\":\"message:abc\"}\n"
        );
        assert_eq!(
            finish("error", ""),
            "d:{\"finishReason\":\"error\",\"assistantMessageId\":\"\"}\n"
        );
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
