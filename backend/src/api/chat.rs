//! Streaming chat completion for a persisted conversation.
//!
//! Route: `POST /api/chat/conversations/{id}/messages`.
//!
//! Lifecycle:
//!  1. Verify the conversation exists for this caller (PERMISSIONS gate).
//!  2. Load full message history via `list_messages`.
//!  3. Persist the user's new message.
//!  4. Run RAG retrieval against that message (best-effort — see below).
//!  5. Stream the LLM reply as Vercel AI SDK Data Stream Protocol records,
//!     while accumulating into a buffer.
//!  6. On stream end (or error), persist the assistant reply with whatever
//!     bytes we have.
//!  7. If the conversation had no title at request entry, synthesise one
//!     synchronously after step 6 by re-invoking the LLM with a short
//!     prompt. This delays the trailing `finish` marker by ~1–2s, which is
//!     acceptable for v1; an async path would need a cloneable `AuthedDb`,
//!     which the pool's release semantics intentionally forbid.
//!
//! ## RAG retrieval (step 4)
//!
//! When a chunk embedder is configured, before calling the LLM the
//! handler:
//!
//! 1. Embeds the latest user message with the chunk embedder's `query()`
//!    transform.
//! 2. Runs KNN over `chunk.embedding` (tenant-scoped — engine PERMISSIONS).
//! 3. Expands each hit by `±RAG_RETRIEVAL_NEIGHBOR_RADIUS` (same doc,
//!    adjacent ordinal).
//! 4. Prepends a `Role::System` message enumerating each chunk with a
//!    `[N]` marker so the LLM can cite.
//! 5. Streams a `citations` data block as the first record, then the
//!    LLM's text deltas; the frontend resolves `[N]` against the table.
//!
//! Retrieval is best-effort: if there's no embedder, no chunks match,
//! or KNN/expansion errors, the handler falls through to the pre-RAG
//! flow (history + user message only). It never fails the chat.

use std::collections::HashSet;
use std::convert::Infallible;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use futures::stream::{self, StreamExt};
use serde::Deserialize;
use surrealdb::RecordId;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::api::stream::{self as proto, CitationEntry};
use crate::auth::AuthContext;
use crate::llm::{LlmClient, LlmDelta, LlmMessage, Role};
use crate::state::AppState;
use crate::storage::{
    AuthedDb, ChatMessage, Chunk, ChunkSearchResult, ConversationId, Filters, Storage,
};

const DEFAULT_TOP_K: usize = 5;
const DEFAULT_NEIGHBOR_RADIUS: i64 = 1;

/// Body sent by `@ai-sdk/react`'s `useChat`. Extra fields (id, parts, etc.)
/// are ignored.
#[derive(Debug, Deserialize)]
pub struct ChatRequest {
    #[serde(default)]
    pub messages: Vec<ChatRequestMessage>,
}

#[derive(Debug, Deserialize)]
pub struct ChatRequestMessage {
    pub role: String,
    #[serde(default)]
    pub content: String,
    /// `useChat` v3+ sends a `parts` array; flatten any text parts into
    /// `content` if `content` is empty.
    #[serde(default)]
    pub parts: Vec<MessagePart>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum MessagePart {
    Text {
        #[serde(default)]
        text: String,
    },
    #[serde(other)]
    Other,
}

impl ChatRequestMessage {
    fn collapse_text(&self) -> String {
        if !self.content.is_empty() {
            return self.content.clone();
        }
        self.parts
            .iter()
            .filter_map(|p| match p {
                MessagePart::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("")
    }
}

fn parse_conversation_id(key: &str) -> Result<ConversationId, Response> {
    let k = key.trim();
    if k.is_empty() || k.contains(':') || k.len() != key.len() {
        return Err((StatusCode::BAD_REQUEST, "invalid conversation key").into_response());
    }
    Ok(RecordId::from(("conversation", k)))
}

fn role_to_llm(role: &str) -> Option<Role> {
    match role {
        "system" => Some(Role::System),
        "user" => Some(Role::User),
        "assistant" => Some(Role::Assistant),
        _ => None,
    }
}

fn history_to_llm(messages: &[ChatMessage]) -> Vec<LlmMessage> {
    messages
        .iter()
        .filter_map(|m| {
            role_to_llm(&m.role).map(|role| LlmMessage {
                role,
                content: m.content.clone(),
            })
        })
        .collect()
}

pub async fn post_message(
    State(state): State<AppState>,
    Extension(db): Extension<Arc<AuthedDb>>,
    auth: AuthContext,
    Path(key): Path<String>,
    Json(req): Json<ChatRequest>,
) -> Response {
    let conv_id = match parse_conversation_id(&key) {
        Ok(id) => id,
        Err(r) => return r,
    };

    // 1. Verify the conversation exists (and is visible) for this caller.
    let conversation = match db.get_conversation(&conv_id).await {
        Ok(Some(c)) => c,
        Ok(None) => return (StatusCode::NOT_FOUND, "conversation not found").into_response(),
        Err(e) => {
            error!(error = %e, "get_conversation failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response();
        }
    };

    // 2. Pick out only the last user message from the request body — the
    //    SPA sends the full history, but we already have the canonical
    //    history in storage.
    let last_user_text = req
        .messages
        .iter()
        .rev()
        .find(|m| m.role == "user")
        .map(|m| m.collapse_text())
        .unwrap_or_default();
    if last_user_text.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "no user message").into_response();
    }

    // 3. Load history.
    let history = match db.list_messages(&conv_id).await {
        Ok(m) => m,
        Err(e) => {
            error!(error = %e, "list_messages failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "lookup failed").into_response();
        }
    };

    // 4. Persist the new user message before kicking off the LLM, so a
    //    mid-stream crash still leaves the user's message in the log.
    if let Err(e) = db.append_message(&conv_id, "user", &last_user_text).await {
        error!(error = %e, "append user message failed");
        return (StatusCode::INTERNAL_SERVER_ERROR, "append failed").into_response();
    }

    info!(
        user_id = %auth.user_id,
        conversation = %conv_id,
        history_len = history.len(),
        "chat message received"
    );

    // 5. Build the prompt: existing history + the just-persisted user msg.
    let mut prompt: Vec<LlmMessage> = history_to_llm(&history);
    prompt.push(LlmMessage {
        role: Role::User,
        content: last_user_text.clone(),
    });

    // 6. RAG retrieval (best-effort). When a chunk embedder is wired in,
    //    embed the user's message, KNN, expand to neighbors, prepend a
    //    `[N]`-labelled system message so the LLM can cite. Any failure
    //    falls through to the pre-RAG flow without erroring the chat.
    let citations = if let Some(embedder) = state.chunk_embedder.clone() {
        match retrieve_for_query(db.as_ref(), embedder.as_ref(), &last_user_text).await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "rag retrieval failed; continuing without citations");
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let citation_block = if citations.is_empty() {
        None
    } else {
        let system = build_system_prompt(&citations);
        // Prepend so the LLM sees citation context before the history.
        prompt.insert(
            0,
            LlmMessage {
                role: Role::System,
                content: system,
            },
        );
        Some(citation_entries(&citations))
    };

    let llm = state.llm.clone();
    let upstream = match llm.stream_chat(prompt.clone()).await {
        Ok(s) => s,
        Err(e) => {
            error!("stream_chat init failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "llm error").into_response();
        }
    };

    let errored = Arc::new(AtomicBool::new(false));
    let errored_for_finish = errored.clone();
    let buffer = Arc::new(Mutex::new(String::new()));
    let buffer_for_finish = buffer.clone();
    let db_for_finish = db.clone();
    let llm_for_finish = llm.clone();
    let needs_title = conversation.title.is_none();
    let user_text_for_finish = last_user_text;

    // The `citations` data block streams first (if any), then text deltas.
    let citations_lead: Option<String> = citation_block.as_ref().map(|c| proto::citations(c));
    let lead_stream = stream::iter(
        citations_lead
            .into_iter()
            .map(|s| Ok::<_, Infallible>(s.into_bytes())),
    );
    let body_stream = upstream
        .map(move |item| {
            let line = match item {
                Ok(LlmDelta::Text(t)) => {
                    // Best-effort buffer accumulation. `try_lock` is fine —
                    // this map closure is the only writer and runs serially.
                    if let Ok(mut buf) = buffer.try_lock() {
                        buf.push_str(&t);
                    }
                    proto::text(&t)
                }
                Err(e) => {
                    error!("llm stream error: {e}");
                    errored.store(true, Ordering::Relaxed);
                    proto::error("llm stream error")
                }
            };
            Ok::<_, Infallible>(line.into_bytes())
        })
        .chain(stream::once(async move {
            // Persist the assistant reply (whatever we got — partial counts
            // as partial). Even an empty buffer gets persisted so the
            // history shape stays predictable.
            let final_text = {
                let g = buffer_for_finish.lock().await;
                g.clone()
            };
            if let Err(e) = db_for_finish
                .append_message(&conv_id, "assistant", &final_text)
                .await
            {
                warn!(error = %e, "persisting assistant message failed");
            }

            // Synchronous, best-effort title generation. We hold the
            // AuthedDb until this resolves rather than spawning, because
            // AuthedDb is intentionally !Clone (pool release is
            // single-owner). The 1–2s delay before the finish marker is
            // acceptable for v1.
            if needs_title && !final_text.is_empty() {
                if let Some(title) =
                    generate_title(llm_for_finish.as_ref(), &user_text_for_finish, &final_text)
                        .await
                {
                    if let Err(e) = db_for_finish.rename_conversation(&conv_id, &title).await {
                        warn!(error = %e, "auto-title rename failed");
                    }
                }
            }

            let reason = if errored_for_finish.load(Ordering::Relaxed) {
                "error"
            } else {
                "stop"
            };
            Ok::<_, Infallible>(proto::finish(reason).into_bytes())
        }));

    let body = lead_stream.chain(body_stream);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .header("x-vercel-ai-data-stream", "v1")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(body))
        .unwrap()
}

/// Ask the LLM for a short title. Best-effort: returns `None` on any
/// failure rather than escalating — auto-titling is a nice-to-have, not
/// a critical path.
async fn generate_title(
    llm: &dyn LlmClient,
    user_msg: &str,
    assistant_msg: &str,
) -> Option<String> {
    let prompt = vec![
        LlmMessage {
            role: Role::System,
            content: "You produce concise chat titles. Respond with ONLY the title \
                 (no quotes, no preamble), 60 characters or less, summarising \
                 the user's question."
                .into(),
        },
        LlmMessage {
            role: Role::User,
            content: format!("User: {user_msg}\n\nAssistant: {assistant_msg}\n\nTitle:"),
        },
    ];
    let mut stream = match llm.stream_chat(prompt).await {
        Ok(s) => s,
        Err(e) => {
            warn!("title llm call failed: {e}");
            return None;
        }
    };
    let mut buf = String::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(LlmDelta::Text(t)) => buf.push_str(&t),
            Err(e) => {
                warn!("title stream error: {e}");
                return None;
            }
        }
    }
    let cleaned = clean_title(&buf);
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

fn clean_title(raw: &str) -> String {
    let mut s = raw.trim().to_string();
    // Strip wrapping quotes / smart quotes.
    let quotes = ['"', '\'', '“', '”', '‘', '’'];
    if s.chars().count() >= 2 {
        if let (Some(first), Some(last)) = (s.chars().next(), s.chars().last()) {
            if quotes.contains(&first) && quotes.contains(&last) {
                // Drop one char from each end (byte-safe via chars).
                let inner: String = s.chars().skip(1).collect();
                let drop_last = inner.chars().count().saturating_sub(1);
                s = inner.chars().take(drop_last).collect();
                s = s.trim().to_string();
            }
        }
    }
    // Truncate to 60 chars without splitting a grapheme.
    if s.chars().count() > 60 {
        s = s.chars().take(60).collect();
    }
    s
}

/// One row in the assembled retrieval context. Survives KNN expansion +
/// neighbor expansion + de-dup; what the system prompt enumerates.
#[derive(Debug, Clone)]
struct Retrieved {
    chunk_id: surrealdb::RecordId,
    doc_id: surrealdb::RecordId,
    #[allow(dead_code)] // kept for future neighbor-aware reranking
    ordinal: i64,
    text: String,
    /// Title looked up from the document row when available — feeds
    /// the `[N] "Title" (page X)` lead-in.
    doc_title: Option<String>,
    /// First page mentioned in the chunk's bboxes (best-effort).
    page: Option<i64>,
}

async fn retrieve_for_query(
    db: &AuthedDb,
    embedder: &dyn crate::embedder::Embedder,
    query: &str,
) -> crate::error::Result<Vec<Retrieved>> {
    let top_k = env_usize("RAG_RETRIEVAL_TOP_K", DEFAULT_TOP_K).max(1);
    let radius = env_i64("RAG_RETRIEVAL_NEIGHBOR_RADIUS", DEFAULT_NEIGHBOR_RADIUS).max(0);

    let qv = embedder.query(query).await?;
    let filters = Filters {
        embedding_model: Some(embedder.model_name().to_string()),
        ..Default::default()
    };
    let hits: Vec<ChunkSearchResult> = db.search_vector(&qv, top_k, &filters).await?;
    if hits.is_empty() {
        return Ok(Vec::new());
    }

    // Expand: for each unique (doc, ordinal) hit, load the chunk window
    // [ord-r, ord+r]. Then de-dup so multiple hits in the same window
    // don't double-quote.
    let mut seen: HashSet<surrealdb::RecordId> = HashSet::new();
    let mut out: Vec<Retrieved> = Vec::new();
    for hit in &hits {
        let lo = (hit.ordinal - radius).max(0);
        let hi = hit.ordinal + radius;
        let window = db
            .list_chunks_in_range(&hit.doc_id, lo, hi)
            .await
            .unwrap_or_default();
        let doc_title = db
            .get_document(&hit.doc_id)
            .await
            .ok()
            .flatten()
            .and_then(|d| d.title);
        for c in window {
            if let Some(cid) = c.id.clone() {
                if !seen.insert(cid.clone()) {
                    continue;
                }
                out.push(Retrieved {
                    chunk_id: cid,
                    doc_id: hit.doc_id.clone(),
                    ordinal: c.ordinal,
                    text: c.text,
                    doc_title: doc_title.clone(),
                    page: c.bboxes.as_ref().and_then(|b| b.first().map(|b| b.page)),
                });
            }
        }
    }
    Ok(out)
}

fn build_system_prompt(rows: &[Retrieved]) -> String {
    let mut s = String::from(
        "You have access to the following excerpts from the user's corpus.\n\
         When you make a claim drawn from one of them, append the corresponding \
         [N] marker. Cite only chunks you actually used.\n\n",
    );
    for (i, r) in rows.iter().enumerate() {
        let n = i + 1;
        let title = r.doc_title.as_deref().unwrap_or("(untitled)");
        match r.page {
            Some(p) => s.push_str(&format!("[{n}] \"{title}\" (page {p})\n")),
            None => s.push_str(&format!("[{n}] \"{title}\"\n")),
        }
        s.push_str(&r.text);
        s.push_str("\n\n");
    }
    s
}

fn citation_entries(rows: &[Retrieved]) -> Vec<CitationEntry> {
    rows.iter()
        .enumerate()
        .map(|(i, r)| CitationEntry {
            n: i + 1,
            chunk_id: r.chunk_id.to_string(),
            doc_id: r.doc_id.to_string(),
            doc_title: r.doc_title.clone(),
            page: r.page,
        })
        .collect()
}

// helper: we don't use `Chunk` directly in the API surface, but the
// integration test does. Reference it so the type stays in scope; the
// compiler will drop this if unused.
#[allow(dead_code)]
fn _chunk_is_used(_c: Chunk) {}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_i64(key: &str, default: i64) -> i64 {
    env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::clean_title;

    #[test]
    fn strips_surrounding_quotes() {
        assert_eq!(clean_title("\"Some title\""), "Some title");
        assert_eq!(clean_title("'foo'"), "foo");
        assert_eq!(clean_title("“smart”"), "smart");
    }

    #[test]
    fn truncates_to_sixty_chars() {
        let long = "a".repeat(100);
        assert_eq!(clean_title(&long).chars().count(), 60);
    }

    #[test]
    fn trims_whitespace() {
        assert_eq!(clean_title("   hi   "), "hi");
    }
}
