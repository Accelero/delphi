//! SSE wire-format writers for the per-conversation chat stream.
//!
//! Real Server-Sent Events: each frame is `event: <name>\ndata: <json>\n\n`.
//! The browser's native `EventSource` understands this format and dispatches
//! named events to per-name handlers, which is exactly what
//! `useChatStream` registers on the frontend.
//!
//! Frame ordering for one turn:
//!
//!   user_message → [citations] → text* → (finish | error+finish | clear)
//!
//! Each helper returns the formatted [`Bytes`] verbatim — callers buffer them
//! in `SessionState::frames` for replay-on-subscribe **and** fan them out
//! live to every connected subscriber, so the wire format must be exactly
//! once-per-call.
//!
//! Snapshot tests below pin the byte layout. Don't soften them to
//! "roughly-shaped" assertions — the snapshot tests are the
//! testing-strategy guardrail against silent protocol drift (see
//! `docs/architecture/testing.md` § Vibe-coded guardrails).

use bytes::Bytes;
use serde::Serialize;
use serde_json::json;

/// Format an SSE frame: `event: <name>\ndata: <json>\n\n`.
fn frame(name: &str, data: &str) -> Bytes {
    let mut s = String::with_capacity(name.len() + data.len() + 16);
    s.push_str("event: ");
    s.push_str(name);
    s.push_str("\ndata: ");
    s.push_str(data);
    s.push_str("\n\n");
    Bytes::from(s)
}

/// First frame of every turn. Carries the persisted user message id (which
/// the client uses as `parent_id` for any subsequent submit while the turn
/// is in flight) and the user's text — the client appends it to the
/// `messages` list and unconditionally clears any in-flight assistant
/// overlay (the "reset rule" — see the v3 plan).
pub fn user_message(id: &str, content: &str) -> Bytes {
    let body = json!({ "id": id, "content": content }).to_string();
    frame("user_message", &body)
}

/// One text-delta record. The body is a JSON-encoded string so callers can
/// `JSON.parse(event.data)` without ambiguity around control characters.
pub fn text(s: &str) -> Bytes {
    let body = serde_json::to_string(s).unwrap_or_else(|_| "\"\"".into());
    frame("text", &body)
}

/// Citations table for the current turn. Sent **before** the first text
/// delta so the client has the `[N]` mapping ready by the time markers
/// appear in the streamed prose.
pub fn citations(entries: &[CitationEntry]) -> Bytes {
    let body = serde_json::to_string(entries).unwrap_or_else(|_| "[]".into());
    frame("citations", &body)
}

/// Mid-turn error. Followed by either a `finish` (graceful close) or a
/// `clear` (worker aborted), so the client always knows when to leave the
/// error state.
pub fn error(msg: &str) -> Bytes {
    let body = serde_json::to_string(msg).unwrap_or_else(|_| "\"\"".into());
    frame("error", &body)
}

/// Final frame on a committed turn. `reason` is `"stop"` for natural EOF,
/// `"error"` for mid-stream failures the worker still chose to commit.
/// `assistant_message_id` is the record id of the persisted assistant
/// reply; the client uses it as `parent_id` on the next submit.
pub fn finish(reason: &str, assistant_message_id: &str) -> Bytes {
    let body = json!({
        "finishReason": reason,
        "assistantMessageId": assistant_message_id,
    })
    .to_string();
    frame("finish", &body)
}

/// Abort frame. Tells every subscriber to drop the in-flight overlay
/// **and** the optimistic `user_message` entry it inserted at turn start —
/// the user message is never persisted on a cancelled turn, so the
/// rollback is total. Data payload is the JSON literal `null`.
pub fn clear() -> Bytes {
    frame("clear", "null")
}

/// Title update (v4). Pushed to a conversation's live subscribers after a
/// first-turn auto-title is generated and persisted, so open tabs refresh
/// the sidebar without a refetch. Delivered **out of turn** (via
/// `TurnBus::emit`, after the turn's `finish`), so it is not part of any
/// turn's persisted messages. Idempotent on the client — a tab that
/// already loaded the title from the DB just re-applies the same value.
/// Payload is the JSON-encoded title string.
pub fn title(t: &str) -> Bytes {
    let body = serde_json::to_string(t).unwrap_or_else(|_| "\"\"".into());
    frame("title", &body)
}

/// Resync control frame (v4). Tells a client whose cursor fell out of the
/// in-memory window (a turn committed while it was disconnected) to
/// re-read committed history from SurrealDB and keep streaming. The
/// universal correctness net: because the durable copy exists before any
/// ephemeral byte is trimmed, no GC policy can lose data. Carries no
/// `id:` line (it is a control signal, not a replayable data frame, and
/// must not become the client's `Last-Event-Id`). Payload is `null`.
pub fn resync() -> Bytes {
    frame("resync", "null")
}

/// Per-marker payload streamed inside the `citations` array.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn as_str(b: &Bytes) -> &str {
        std::str::from_utf8(b).expect("utf-8")
    }

    #[test]
    fn user_message_snapshot() {
        let b = user_message("message:01J", "hi");
        assert_eq!(
            as_str(&b),
            "event: user_message\ndata: {\"id\":\"message:01J\",\"content\":\"hi\"}\n\n"
        );
    }

    #[test]
    fn text_snapshot() {
        assert_eq!(as_str(&text("hello")), "event: text\ndata: \"hello\"\n\n");
        assert_eq!(
            as_str(&text("with\nnewline")),
            "event: text\ndata: \"with\\nnewline\"\n\n"
        );
    }

    #[test]
    fn error_snapshot() {
        assert_eq!(as_str(&error("boom")), "event: error\ndata: \"boom\"\n\n");
    }

    #[test]
    fn finish_snapshot() {
        assert_eq!(
            as_str(&finish("stop", "message:abc")),
            "event: finish\ndata: {\"finishReason\":\"stop\",\"assistantMessageId\":\"message:abc\"}\n\n"
        );
        assert_eq!(
            as_str(&finish("error", "")),
            "event: finish\ndata: {\"finishReason\":\"error\",\"assistantMessageId\":\"\"}\n\n"
        );
    }

    #[test]
    fn clear_snapshot() {
        assert_eq!(as_str(&clear()), "event: clear\ndata: null\n\n");
    }

    #[test]
    fn resync_snapshot() {
        assert_eq!(as_str(&resync()), "event: resync\ndata: null\n\n");
    }

    #[test]
    fn title_snapshot() {
        assert_eq!(
            as_str(&title("Capital of France")),
            "event: title\ndata: \"Capital of France\"\n\n"
        );
        // JSON-escaped so control chars / quotes round-trip via JSON.parse.
        assert_eq!(
            as_str(&title("a \"quoted\" title")),
            "event: title\ndata: \"a \\\"quoted\\\" title\"\n\n"
        );
    }

    #[test]
    fn citations_snapshot() {
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
        let b = citations(&entries);
        assert_eq!(
            as_str(&b),
            "event: citations\ndata: [\
             {\"n\":1,\"chunk_id\":\"chunk:abc\",\"doc_id\":\"document:xyz\",\"doc_title\":\"Title\",\"page\":3},\
             {\"n\":2,\"chunk_id\":\"chunk:def\",\"doc_id\":\"document:xyz\",\"doc_title\":null,\"page\":null}\
             ]\n\n"
        );
    }
}
