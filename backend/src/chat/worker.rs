//! Per-turn background worker (v3).
//!
//! Spawned by `POST /api/chat/conversations/{key}/messages` after the
//! POST handler has already called `SessionState::start_turn` (which
//! buffers the `user_message` SSE frame and registers the cancel
//! token). The worker:
//!
//! 1. Acquires its own [`AuthedDb`] from the pool using the caller's
//!    snapshotted bearer (`AuthedDb` is `!Clone` and released-on-Drop,
//!    so we don't smuggle the request's handle — we mint a fresh one
//!    here).
//! 2. Loads history + optionally runs RAG retrieval, builds the prompt.
//! 3. Emits `citations` (if any) via `SessionState::emit`, then
//!    streams the LLM reply into the session as `text` frames.
//! 4. A per-turn [`CancellationToken`] races each delta. On cancel we
//!    bail — `SessionState::abort` (called by the `/stop` handler) has
//!    already emitted the `clear` frame and cleared `current`.
//! 5. On natural EOF / mid-stream error we flip phase to `Committing`
//!    (via [`SessionState::enter_committing`]); if the flip fails the
//!    turn was aborted while we were still in the LLM loop and we bail
//!    without writing to the DB.
//! 6. We commit the user+assistant pair atomically via
//!    [`Storage::commit_turn`], then call [`SessionState::finish`] —
//!    that emits the trailing `finish` frame, marks phase `Committed`,
//!    and clears `current`.
//! 7. Title generation is detached: `tokio::spawn` after `finish`, so
//!    the SSE `finish` frame reaches the UI immediately. The title
//!    task acquires its own `AuthedDb`.
//!
//! ### Panic guard
//!
//! The worker body runs inside a `WorkerGuard` whose `Drop` calls
//! `session.abort()` on unwind. Without it, a panic mid-turn would
//! leave `current` permanently `Some` and every subsequent POST for
//! that conversation would return 409 forever.

use std::collections::HashSet;
use std::env;
use std::sync::Arc;

use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::api::sse::{self, CitationEntry};
use crate::auth::AuthContext;
use crate::embedder::Embedder;
use crate::llm::{LlmClient, LlmDelta, LlmMessage, Role};
use crate::state::AppState;
use crate::storage::{
    AuthedDb, ChatMessage, ChunkSearchResult, ConversationId, Filters, MessageId, RequestDbPool,
    Storage,
};

use super::registry::TaskId;
use super::session::SessionState;

const DEFAULT_TOP_K: usize = 5;
const DEFAULT_NEIGHBOR_RADIUS: i64 = 1;

/// Reason the turn ended. Reported as the `finishReason` in the
/// trailing `finish` SSE frame and used for tracing.
#[derive(Debug, Clone, Copy)]
enum StopReason {
    Eof,
    /// `/stop` came in and we noticed during the LLM loop.
    Cancelled,
    Error,
}

impl StopReason {
    fn wire(self) -> &'static str {
        match self {
            StopReason::Eof => "stop",
            StopReason::Cancelled => "stop",
            StopReason::Error => "error",
        }
    }
}

/// Everything the worker needs to drive one turn. Owned values only —
/// the worker outlives the request that spawned it.
pub struct TurnRequest {
    pub conversation_id: ConversationId,
    /// Client-generated ULID (no `message:` prefix). Becomes the record
    /// id of the persisted user message.
    pub user_message_id: String,
    pub user_text: String,
    /// Last known assistant message id (or `None` for the first turn).
    pub parent_id: Option<MessageId>,
    /// JWT we'll feed to `pool.acquire(bearer)` to get a fresh
    /// `AuthedDb`. Same value the original request used.
    pub bearer: String,
    /// Caller identity, snapshotted from the request. Kept for tracing.
    pub auth: AuthContext,
    pub llm: Arc<dyn LlmClient>,
    pub chunk_embedder: Option<Arc<dyn Embedder>>,
    pub pool: RequestDbPool,
}

/// Spawn the worker. The POST handler has already buffered the
/// `user_message` frame via [`SessionState::start_turn`]; this just
/// detaches the LLM loop. The session pointer is shared (Arc); the
/// cancel token comes from `start_turn` so `/stop` can flip it.
pub fn spawn_worker(
    session: Arc<SessionState>,
    task_id: TaskId,
    cancel: CancellationToken,
    req: TurnRequest,
) {
    tokio::spawn(run(session, task_id, cancel, req));
}

async fn run(
    session: Arc<SessionState>,
    task_id: TaskId,
    cancel: CancellationToken,
    req: TurnRequest,
) {
    let mut guard = WorkerGuard {
        session: session.clone(),
        armed: true,
    };

    let outcome = drive_turn(&session, task_id, &cancel, &req).await;

    if let Err(e) = outcome {
        error!(conv = %req.conversation_id, task = %task_id, error = %e, "turn ended with internal error");
    }

    // Normal exit path: the guard's abort-on-drop has already been
    // disarmed below (in the no-panic branch). On panic, `armed`
    // remains true and `Drop` calls `session.abort()`.
    guard.armed = false;
    drop(guard);
}

/// Drive one full turn against the session. Returns `Err` only for
/// internal failures the caller should log; user-visible errors are
/// reported via SSE `error` frames inside this function.
async fn drive_turn(
    session: &SessionState,
    task_id: TaskId,
    cancel: &CancellationToken,
    req: &TurnRequest,
) -> Result<(), String> {
    // Pool checkout — fresh `AuthedDb` for this worker, released back
    // when this function returns.
    let db = match req.pool.acquire(&req.bearer).await {
        Ok(d) => d,
        Err(e) => {
            error!(conv = %req.conversation_id, error = %e, "worker pool acquire failed");
            session.emit(sse::error("auth setup failed"));
            session.abort();
            return Err(format!("pool acquire: {e}"));
        }
    };

    let history = match db.list_messages(&req.conversation_id).await {
        Ok(m) => m,
        Err(e) => {
            error!(conv = %req.conversation_id, error = %e, "list_messages failed");
            session.emit(sse::error("history lookup failed"));
            session.abort();
            return Err(format!("list_messages: {e}"));
        }
    };

    // Snapshot whether the conversation was unnamed BEFORE the commit.
    let conversation_had_title = match db.get_conversation(&req.conversation_id).await {
        Ok(Some(c)) => c.title.is_some(),
        Ok(None) => false,
        Err(e) => {
            error!(conv = %req.conversation_id, error = %e, "get_conversation failed");
            false
        }
    };

    let mut prompt: Vec<LlmMessage> = history_to_llm(&history);
    prompt.push(LlmMessage {
        role: Role::User,
        content: req.user_text.clone(),
    });

    // RAG retrieval (best-effort).
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
        let entries = citation_entries(&citations);
        session.emit(sse::citations(&entries));
    }

    info!(
        user_id = %req.auth.user_id,
        conv = %req.conversation_id,
        task = %task_id,
        history_len = history.len(),
        "worker driving turn"
    );

    let mut upstream = match req.llm.stream_chat(prompt).await {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "stream_chat init failed");
            session.emit(sse::error("llm error"));
            session.abort();
            return Err(format!("stream_chat: {e}"));
        }
    };

    // Per-delta loop. Cancellation races each `.next()`; on cancel we
    // BAIL — `SessionState::abort` has already emitted `clear` and
    // cleared `current`.
    let mut assistant_buf = String::new();
    let stop_reason = loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!(conv = %req.conversation_id, task = %task_id, "turn cancelled by /stop");
                break StopReason::Cancelled;
            }
            item = upstream.next() => match item {
                Some(Ok(LlmDelta::Text(t))) => {
                    assistant_buf.push_str(&t);
                    session.emit(sse::text(&t));
                }
                Some(Err(e)) => {
                    error!(error = %e, "llm stream error");
                    session.emit(sse::error("llm stream error"));
                    break StopReason::Error;
                }
                None => break StopReason::Eof,
            }
        }
    };

    drop(upstream); // run rig's stream Drop before commit

    if matches!(stop_reason, StopReason::Cancelled) {
        // No DB write, no finish — abort already cleared everything.
        return Ok(());
    }

    // Commit/abort race: try to flip phase to Committing. If the abort
    // raced us between the last LLM delta and here, the flip returns
    // false and we bail without touching the DB.
    if !session.enter_committing() {
        info!(conv = %req.conversation_id, task = %task_id, "abort raced before commit; skipping commit");
        return Ok(());
    }

    let assistant_id = match db
        .commit_turn(
            &req.conversation_id,
            &req.user_message_id,
            &req.user_text,
            req.parent_id.as_ref(),
            &assistant_buf,
        )
        .await
    {
        Ok(id) => id,
        Err(e) => {
            error!(error = %e, "commit_turn failed");
            session.emit(sse::error("commit failed"));
            session.finish(sse::finish("error", ""));
            return Err(format!("commit_turn: {e}"));
        }
    };

    let assistant_id_str = assistant_id.to_string();

    // Emit finish FIRST so the UI unblocks; detach title generation.
    session.finish(sse::finish(stop_reason.wire(), &assistant_id_str));

    if !conversation_had_title && !assistant_buf.is_empty() {
        let pool = req.pool.clone();
        let bearer = req.bearer.clone();
        let conv = req.conversation_id.clone();
        let llm = req.llm.clone();
        let user_msg = req.user_text.clone();
        let assistant_msg = assistant_buf.clone();
        tokio::spawn(async move {
            let title = match generate_title(llm.as_ref(), &user_msg, &assistant_msg).await {
                Some(t) => t,
                None => return,
            };
            // Title task acquires its own AuthedDb; same JWT, same
            // session contract. Best-effort — log on failure, no retry.
            let db = match pool.acquire(&bearer).await {
                Ok(d) => d,
                Err(e) => {
                    warn!(error = %e, "title task pool acquire failed");
                    return;
                }
            };
            if let Err(e) = db.rename_conversation(&conv, &title).await {
                warn!(error = %e, "auto-title rename failed");
            }
        });
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Worker panic guard
// ---------------------------------------------------------------------------

struct WorkerGuard {
    session: Arc<SessionState>,
    armed: bool,
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        if self.armed {
            // Unwind path: clear the turn so the conversation isn't
            // wedged at 409 forever. The `clear` frame tells live
            // subscribers to roll back the overlay.
            self.session.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// helpers — copied near-verbatim from the previous worker
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

/// Build a `TurnRequest` from conventional API-handler inputs. Keeps
/// `api::chat::post_message` from having to know the worker's full
/// field layout.
pub fn turn_request(
    conversation_id: ConversationId,
    user_message_id: String,
    user_text: String,
    parent_id: Option<MessageId>,
    bearer: String,
    auth: AuthContext,
    app: &AppState,
) -> TurnRequest {
    TurnRequest {
        conversation_id,
        user_message_id,
        user_text,
        parent_id,
        bearer,
        auth,
        llm: app.llm.clone(),
        chunk_embedder: app.chunk_embedder.clone(),
        pool: app.request_db_pool.clone(),
    }
}
