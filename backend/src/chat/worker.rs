//! Per-turn background worker (v4 — single writer).
//!
//! Spawned by `POST /api/chat/conversations/{key}/messages` after the
//! handler has claimed the turn via [`crate::chat::TurnBus::try_start`]
//! (which buffered the `user_message` frame and returned a
//! [`TurnHandle`]). The worker is the **only** writer of this turn's
//! stream, which makes the commit↔abort race structurally impossible —
//! there is no phase machine (§8). It:
//!
//! 1. Acquires its own [`AuthedDb`] from the pool using the caller's
//!    snapshotted bearer (`AuthedDb` is `!Clone` and released-on-Drop,
//!    so we don't smuggle the request's handle — we mint a fresh one
//!    here).
//! 2. Loads history + optionally runs RAG retrieval, builds the prompt.
//! 3. `append`s the `citations` frame (if any), then streams the LLM
//!    reply as `text` frames via the handle.
//! 4. A biased `select!` races [`TurnHandle::cancelled`] against each
//!    delta:
//!    - **cancel branch** ⟺ `terminate(clear)`, no DB write — a
//!      cancelled turn persists nothing (R7).
//!    - **EOF / error branch** ⟺ `commit_turn` then `terminate(finish)`.
//!    These are mutually exclusive, so "clear emitted **and** rows
//!    persisted" cannot happen.
//! 5. Title generation is detached: `tokio::spawn` after `terminate`, so
//!    the `finish` frame reaches the UI immediately. The title task
//!    acquires its own `AuthedDb`.
//!
//! ### Panic guard
//!
//! [`TurnHandle::Drop`] emits `clear` and releases the single-flight slot
//! if `terminate` never ran (worker panic / early return), so a panic
//! mid-turn can't wedge the conversation at 409 forever. This replaces
//! v3's separate `WorkerGuard`.

use std::collections::HashSet;
use std::env;
use std::sync::Arc;

use futures::StreamExt;
use tracing::{error, info, warn};

use crate::api::sse::{self, CitationEntry};
use crate::auth::AuthContext;
use crate::embedder::Embedder;
use crate::llm::{LlmClient, LlmDelta, LlmMessage, Role};
use crate::state::AppState;
use crate::storage::{
    AuthedDb, ChatMessage, ChunkSearchResult, Citation, ConversationId, Filters, MessageId,
    RequestDbPool, Storage,
};

use super::bus::{TaskId, TurnBus, TurnHandle};

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
    /// The turn transport, so the detached auto-title task can push a
    /// `title` frame to live subscribers after `finish` (off the turn's
    /// critical path).
    pub turn_bus: Arc<dyn TurnBus>,
}

/// Spawn the worker. The POST handler has already claimed the turn via
/// `TurnBus::try_start` (buffering the `user_message` frame) and handed
/// us the [`TurnHandle`]; this just detaches the LLM loop.
pub fn spawn_worker(handle: TurnHandle, task_id: TaskId, req: TurnRequest) {
    tokio::spawn(run(handle, task_id, req));
}

async fn run(mut handle: TurnHandle, task_id: TaskId, req: TurnRequest) {
    // Every branch in `drive_turn` terminates the handle explicitly
    // (`finish` or `clear`); if it returns early on an internal error it
    // has already terminated. `TurnHandle::Drop` is the panic-only
    // backstop.
    if let Err(e) = drive_turn(&mut handle, task_id, &req).await {
        error!(conv = %req.conversation_id, task = %task_id, error = %e, "turn ended with internal error");
    }
}

/// Drive one full turn through the handle. Returns `Err` only for
/// internal failures the caller should log; user-visible errors are
/// reported via SSE `error` frames inside this function. Every path
/// terminates the handle before returning.
async fn drive_turn(
    handle: &mut TurnHandle,
    task_id: TaskId,
    req: &TurnRequest,
) -> Result<(), String> {
    // Pool checkout — fresh `AuthedDb` for this worker, released back
    // when this function returns.
    let db = match req.pool.acquire(&req.bearer).await {
        Ok(d) => d,
        Err(e) => {
            error!(conv = %req.conversation_id, error = %e, "worker pool acquire failed");
            handle.append(sse::error("auth setup failed")).await;
            handle.terminate(sse::clear()).await;
            return Err(format!("pool acquire: {e}"));
        }
    };

    let history = match db.list_messages(&req.conversation_id).await {
        Ok(m) => m,
        Err(e) => {
            error!(conv = %req.conversation_id, error = %e, "list_messages failed");
            handle.append(sse::error("history lookup failed")).await;
            handle.terminate(sse::clear()).await;
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
        handle.append(sse::citations(&entries)).await;
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
            handle.append(sse::error("llm error")).await;
            handle.terminate(sse::clear()).await;
            return Err(format!("stream_chat: {e}"));
        }
    };

    // Per-delta loop. A biased `select!` races the cancel token against
    // each `.next()`. The worker is the sole writer, so the branch we
    // break on *is* the decision: cancel ⇒ clear (no DB); EOF/error ⇒
    // commit + finish. No phase machine (§8).
    let mut assistant_buf = String::new();
    let stop_reason = loop {
        tokio::select! {
            biased;
            _ = handle.cancelled() => {
                info!(conv = %req.conversation_id, task = %task_id, "turn cancelled by /stop");
                break StopReason::Cancelled;
            }
            item = upstream.next() => match item {
                Some(Ok(LlmDelta::Text(t))) => {
                    assistant_buf.push_str(&t);
                    handle.append(sse::text(&t)).await;
                }
                Some(Err(e)) => {
                    error!(error = %e, "llm stream error");
                    handle.append(sse::error("llm stream error")).await;
                    break StopReason::Error;
                }
                None => break StopReason::Eof,
            }
        }
    };

    drop(upstream); // run rig's stream Drop before commit

    if matches!(stop_reason, StopReason::Cancelled) {
        // Cancel branch: emit `clear`, write nothing (R7). Mutually
        // exclusive with the commit branch below — a `/stop` that arrives
        // after we already broke on EOF is a no-op (the token is never
        // re-checked), so that turn commits and emits `finish` instead.
        handle.terminate(sse::clear()).await;
        return Ok(());
    }

    let assistant_id = match db
        .commit_turn(
            &req.conversation_id,
            &req.user_message_id,
            &req.user_text,
            req.parent_id.as_ref(),
            &assistant_buf,
            &storage_citations(&citations),
        )
        .await
    {
        Ok(id) => id,
        Err(e) => {
            error!(error = %e, "commit_turn failed");
            handle.append(sse::error("commit failed")).await;
            handle.terminate(sse::finish("error", "")).await;
            return Err(format!("commit_turn: {e}"));
        }
    };

    let assistant_id_str = assistant_id.to_string();

    // Emit `finish` immediately so the UI unblocks — the turn is complete
    // and durable. The first-turn auto-title runs off this critical path.
    handle
        .terminate(sse::finish(stop_reason.wire(), &assistant_id_str))
        .await;

    // First-turn auto-title, detached so it never blocks `finish`:
    // generate the title, persist it (`rename`), then push a `title` frame
    // to the conversation's live subscribers via the bus so open tabs
    // refresh the sidebar without a refetch. The rename is the durable
    // source of truth (reload shows it regardless); the push is a
    // best-effort, idempotent live update. Only a conversation's first
    // turn pays this.
    if !conversation_had_title && !assistant_buf.is_empty() {
        let pool = req.pool.clone();
        let bearer = req.bearer.clone();
        let conv = req.conversation_id.clone();
        let llm = req.llm.clone();
        let bus = req.turn_bus.clone();
        let user_msg = req.user_text.clone();
        let assistant_msg = assistant_buf.clone();
        tokio::spawn(async move {
            let title = match generate_title(llm.as_ref(), &user_msg, &assistant_msg).await {
                Some(t) => t,
                None => return,
            };
            // Title task acquires its own AuthedDb; same JWT, same session
            // contract. Best-effort — log on failure, no retry.
            let db = match pool.acquire(&bearer).await {
                Ok(d) => d,
                Err(e) => {
                    warn!(error = %e, "title task pool acquire failed");
                    return;
                }
            };
            if let Err(e) = db.rename_conversation(&conv, &title).await {
                warn!(error = %e, "auto-title rename failed");
                return;
            }
            // Push the new title to any connected tabs (no-op if none).
            bus.emit(&conv, sse::title(&title)).await;
        });
    }

    Ok(())
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

/// Durable form of the same citation table — written onto the assistant
/// `message` row by `commit_turn`. Identical field layout to
/// [`CitationEntry`] (the live SSE shape), but storage-owned so the
/// storage layer carries no `api` dependency.
fn storage_citations(rows: &[Retrieved]) -> Vec<Citation> {
    rows.iter()
        .enumerate()
        .map(|(i, r)| Citation {
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
        turn_bus: app.turn_bus.clone(),
    }
}
