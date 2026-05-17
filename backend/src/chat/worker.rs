//! Per-turn background worker.
//!
//! Spawned by `POST /api/chat/conversations/{id}/messages` after the
//! user message is persisted. The worker:
//!
//! 1. Acquires `turn_lock` (serialises against any in-flight or queued
//!    turn for the same session).
//! 2. Checks out its own [`AuthedDb`] from the pool using the caller's
//!    snapshotted bearer (`AuthedDb` is `!Clone` and released-on-Drop,
//!    so it can't be smuggled past the request that minted it — see
//!    the storage module's pool comments).
//! 3. Loads history + optionally runs RAG retrieval, builds the prompt.
//! 4. Streams the LLM reply, appending framed `proto::*` bytes to the
//!    session buffer after every delta.
//! 5. Stop button: a per-turn [`CancellationToken`] races each delta;
//!    on cancel we emit `proto::finish("stop")` and persist whatever
//!    we have, identical to the clean-finish path.
//! 6. Under `finalize_lock`: write the assistant message, run the
//!    best-effort title generator if the conversation was unnamed,
//!    then clear the buffer (advancing `base_offset`).
//! 7. Drops its `Arc<SessionState>` and the `Semaphore` permit. Any
//!    queued submission's worker wakes up and runs next.
//!
//! Errors at any stage emit `proto::error(...)` into the buffer and
//! fall through to the same persist+commit path — the partial reply
//! is still a reply.

use std::collections::HashSet;
use std::env;
use std::sync::Arc;

use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::api::stream::{self as proto, CitationEntry};
use crate::auth::AuthContext;
use crate::embedder::Embedder;
use crate::llm::{LlmClient, LlmDelta, LlmMessage, Role};
use crate::state::AppState;
use crate::storage::{
    AuthedDb, ChatMessage, ChunkSearchResult, ConversationId, Filters, RequestDbPool, Storage,
};

use super::state::SessionState;

const DEFAULT_TOP_K: usize = 5;
const DEFAULT_NEIGHBOR_RADIUS: i64 = 1;

/// Reason the turn ended. Reported as the `finishReason` in the
/// trailing `d:` frame and used for tracing.
#[derive(Debug, Clone, Copy)]
enum StopReason {
    /// LLM stream ran to completion.
    Eof,
    /// User clicked the stop button (or any tab POSTed `/stop`).
    User,
    /// Upstream returned an error mid-stream.
    Error,
}

impl StopReason {
    fn wire(self) -> &'static str {
        match self {
            StopReason::Eof => "stop",
            StopReason::User => "stop",
            StopReason::Error => "error",
        }
    }
}

/// Everything the worker needs to drive one turn. Constructed by the
/// POST handler after it persists the user message and resolves the
/// caller's identity. Plain-data, owned values only — the worker
/// outlives the request that spawned it.
pub struct TurnRequest {
    pub conversation_id: ConversationId,
    pub user_text: String,
    pub conversation_had_title: bool,
    /// JWT we'll feed to `pool.acquire(bearer)` to get a fresh
    /// `AuthedDb`. Same value the original request used.
    pub bearer: String,
    /// Caller identity, snapshotted from the request. Kept for tracing
    /// — the actual DB-side identity comes from the bearer.
    pub auth: AuthContext,
    /// Shared LLM / embedder handles cloned from `AppState`.
    pub llm: Arc<dyn LlmClient>,
    pub chunk_embedder: Option<Arc<dyn Embedder>>,
    pub pool: RequestDbPool,
}

/// Spawn the worker. Caller passes in the session state (kept alive
/// by the returned future + any attached readers); the future itself
/// is detached via `tokio::spawn` so the POST handler can return 202
/// immediately.
pub fn spawn(state: Arc<SessionState>, req: TurnRequest) {
    tokio::spawn(run(state, req));
}

async fn run(state: Arc<SessionState>, req: TurnRequest) {
    // Acquire the turn semaphore. The permit drops with `_permit` at
    // the end of this function — that's how queued submissions get
    // the green light. We don't `forget()` the permit on any branch.
    let _permit = match state.turn_lock.acquire().await {
        Ok(p) => p,
        Err(_) => {
            // Semaphore closed — only happens at process shutdown.
            warn!(conv = %req.conversation_id, "turn_lock closed; aborting worker");
            return;
        }
    };

    // Install the cancellation token for the stop endpoint to find.
    let cancel = CancellationToken::new();
    {
        let mut g = state.current_turn_cancel.lock().await;
        *g = Some(cancel.clone());
    }

    let outcome = drive_turn(&state, &req, &cancel).await;

    // Drop the per-turn cancel handle on exit so a stale stop request
    // can't fire against the next turn's worker.
    {
        let mut g = state.current_turn_cancel.lock().await;
        *g = None;
    }

    // _permit drops here, releasing the semaphore for the next queued submission.
    drop(_permit);

    // Tracing-only — failures inside drive_turn already emitted their own bytes.
    if let Err(e) = outcome {
        error!(conv = %req.conversation_id, error = %e, "turn ended with internal error");
    }
}

/// Returns Ok(()) when the turn ran end-to-end (clean or partial); an
/// Err only signals an internal/coding failure that prevented us from
/// even attempting the LLM call. Stream-side errors are folded into
/// the buffer as `proto::error` + `proto::finish("error")`.
async fn drive_turn(
    state: &Arc<SessionState>,
    req: &TurnRequest,
    cancel: &CancellationToken,
) -> Result<(), String> {
    // Pool checkout — happens fresh on every turn, on the worker side.
    // `AuthedDb` is `!Clone`/pool-released; we keep it for the duration
    // of this turn and drop it at the end so the connection goes back
    // to the pool.
    let db = match req.pool.acquire(&req.bearer).await {
        Ok(d) => d,
        Err(e) => {
            error!(conv = %req.conversation_id, error = %e, "worker pool acquire failed");
            state
                .append(proto::error("auth setup failed").as_bytes())
                .await;
            state
                .append(proto::finish("error").as_bytes())
                .await;
            return Err(format!("pool acquire: {e}"));
        }
    };

    // Load history. The user message was already persisted by the POST
    // handler, so it shows up here.
    let history = match db.list_messages(&req.conversation_id).await {
        Ok(m) => m,
        Err(e) => {
            error!(conv = %req.conversation_id, error = %e, "list_messages failed");
            state
                .append(proto::error("history lookup failed").as_bytes())
                .await;
            state.append(proto::finish("error").as_bytes()).await;
            return Err(format!("list_messages: {e}"));
        }
    };

    let mut prompt: Vec<LlmMessage> = history_to_llm(&history);

    // RAG retrieval (best-effort). Same shape as the pre-redesign
    // handler: embed the user's message, KNN, expand neighbours, prepend
    // a `[N]`-tagged system message so the LLM can cite. Any failure
    // falls through to the pre-RAG flow without erroring the chat.
    let citations = if let Some(embedder) = req.chunk_embedder.clone() {
        match retrieve_for_query(&db, embedder.as_ref(), &req.user_text).await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "rag retrieval failed; continuing without citations");
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    if !citations.is_empty() {
        let system = build_system_prompt(&citations);
        prompt.insert(
            0,
            LlmMessage {
                role: Role::System,
                content: system,
            },
        );
        // Emit the `citations` data block first so the client has the
        // table ready before any `[N]` markers in the text deltas.
        let entries = citation_entries(&citations);
        state.append(proto::citations(&entries).as_bytes()).await;
    }

    info!(
        user_id = %req.auth.user_id,
        conv = %req.conversation_id,
        history_len = history.len(),
        "worker driving turn"
    );

    // Open the LLM stream.
    let mut upstream = match req.llm.stream_chat(prompt).await {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "stream_chat init failed");
            state.append(proto::error("llm error").as_bytes()).await;
            state.append(proto::finish("error").as_bytes()).await;
            return Err(format!("stream_chat: {e}"));
        }
    };

    // Per-delta loop. Cancellation races each `.next()`; on cancel we
    // drop the upstream (which propagates a cancel to the provider via
    // `rig`'s stream Drop) and break to the finalize path.
    let mut assistant_buf = String::new();
    let stop_reason = loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!(conv = %req.conversation_id, "turn cancelled by user");
                break StopReason::User;
            }
            item = upstream.next() => match item {
                Some(Ok(LlmDelta::Text(t))) => {
                    assistant_buf.push_str(&t);
                    state.append(proto::text(&t).as_bytes()).await;
                }
                Some(Err(e)) => {
                    error!(error = %e, "llm stream error");
                    state.append(proto::error("llm stream error").as_bytes()).await;
                    break StopReason::Error;
                }
                None => break StopReason::Eof,
            }
        }
    };

    // Drop the upstream stream explicitly so its Drop runs before we
    // hold finalize_lock; for clean Eof this is a no-op, for User /
    // Error it cancels the provider request.
    drop(upstream);

    // Emit the trailing `d:` frame. Readers' "is streaming?" flag
    // flips back to false on this frame.
    state.append(proto::finish(stop_reason.wire()).as_bytes()).await;

    // Finalize: persist + best-effort title + clear buffer. All under
    // finalize_lock so the new-tab handshake can't observe a "neither
    // in DB nor in buffer" gap.
    let _g = state.lock_finalize().await;

    if let Err(e) = db
        .append_message(&req.conversation_id, "assistant", &assistant_buf)
        .await
    {
        warn!(error = %e, "persisting assistant message failed");
    }

    if req.conversation_had_title == false && !assistant_buf.is_empty() {
        if let Some(title) =
            generate_title(req.llm.as_ref(), &req.user_text, &assistant_buf).await
        {
            if let Err(e) = db.rename_conversation(&req.conversation_id, &title).await {
                warn!(error = %e, "auto-title rename failed");
            }
        }
    }

    state.clear_after_commit().await;
    drop(_g);

    Ok(())
}

// ---------------------------------------------------------------------------
// helpers — these are intentionally close clones of the equivalents that
// used to live in `api/chat.rs`. After step 6 the originals are deleted
// and the worker becomes the single owner.
// ---------------------------------------------------------------------------

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

fn role_to_llm(role: &str) -> Option<Role> {
    match role {
        "system" => Some(Role::System),
        "user" => Some(Role::User),
        "assistant" => Some(Role::Assistant),
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct Retrieved {
    chunk_id: surrealdb::RecordId,
    doc_id: surrealdb::RecordId,
    #[allow(dead_code)]
    ordinal: i64,
    text: String,
    doc_title: Option<String>,
    page: Option<i64>,
}

async fn retrieve_for_query(
    db: &AuthedDb,
    embedder: &dyn Embedder,
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
    let quotes = ['"', '\'', '“', '”', '‘', '’'];
    if s.chars().count() >= 2 {
        if let (Some(first), Some(last)) = (s.chars().next(), s.chars().last()) {
            if quotes.contains(&first) && quotes.contains(&last) {
                let inner: String = s.chars().skip(1).collect();
                let drop_last = inner.chars().count().saturating_sub(1);
                s = inner.chars().take(drop_last).collect();
                s = s.trim().to_string();
            }
        }
    }
    if s.chars().count() > 60 {
        s = s.chars().take(60).collect();
    }
    s
}

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

/// Build a `TurnRequest` from the conventional API-handler inputs. Lets
/// `api::chat::post_message` stay small (it knows nothing about the
/// worker internals beyond this constructor + `spawn`).
pub fn turn_request(
    conversation_id: ConversationId,
    user_text: String,
    conversation_had_title: bool,
    bearer: String,
    auth: AuthContext,
    app: &AppState,
) -> TurnRequest {
    TurnRequest {
        conversation_id,
        user_text,
        conversation_had_title,
        bearer,
        auth,
        llm: app.llm.clone(),
        chunk_embedder: app.chunk_embedder.clone(),
        pool: app.request_db_pool.clone(),
    }
}
