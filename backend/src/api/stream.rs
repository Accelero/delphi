//! Vercel AI SDK Data Stream Protocol formatting.
//!
//! Records are newline-delimited, each prefixed with a single character
//! identifying the kind. We only emit a small subset for v1:
//!
//!   `0:<json string>\n`  → text delta
//!   `3:<json string>\n`  → error message
//!   `d:<json object>\n`  → finish marker (with finish reason)
//!
//! Reference: <https://sdk.vercel.ai/docs/ai-sdk-ui/stream-protocol>

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
