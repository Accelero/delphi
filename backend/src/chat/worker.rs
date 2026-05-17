//! Per-turn background worker.
//!
//! Spawned by `POST /api/chat/conversations/{id}/messages`. The worker:
//!
//! 1. Acquires its own [`AuthedDb`] from the pool using the caller's
//!    snapshotted bearer (`AuthedDb` is `!Clone` and released-on-Drop,
//!    so we don't smuggle the request's handle — we mint a fresh one
//!    here. See the storage module's pool comments).
//! 2. Emits the `8:` task frame (so the client can address /stop), and
//!    a `2:` citations block if RAG retrieval yielded any.
//! 3. Loads history + optionally runs RAG retrieval, builds the prompt.
//! 4. Streams the LLM reply into the caller's mpsc as framed `proto::*`
//!    bytes.
//! 5. Stop button: a per-turn [`CancellationToken`] races each delta.
//!    On cancel we **discard** — the client already aborted, nothing to
//!    persist.
//! 6. On natural EOF / mid-stream error we commit the user+assistant
//!    pair atomically via [`Storage::commit_turn`] (last-writer-wins
//!    against any racing turn against the same parent), then emit the
//!    trailing `d:` frame carrying the assistant message id.
//! 7. Title generation runs once after commit if the conversation was
//!    unnamed. Best-effort; failure doesn't affect the `d:` emission.
//!
//! ### Backpressure on the response body
//!
//! `try_send` errors when the client has dropped the response body
//! (chat switch / tab close) are **ignored**. The worker keeps pulling
//! from the LLM and commits at the end — that's the "chat switch
//! survives" property called out in the design doc.

use std::collections::HashSet;
use std::env;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::api::stream::{self as proto, CitationEntry};
use crate::auth::AuthContext;
use crate::embedder::Embedder;
use crate::llm::{LlmClient, LlmDelta, LlmMessage, Role};
use crate::state::AppState;
use crate::storage::{
    AuthedDb, ChatMessage, ChunkSearchResult, ConversationId, Filters, MessageId, RequestDbPool,
    Storage,
};

use super::registry::{TaskId, TaskRegistry};

const DEFAULT_TOP_K: usize = 5;
const DEFAULT_NEIGHBOR_RADIUS: i64 = 1;
/// Capacity for the per-turn mpsc. Sized generously for chat-rate
/// streaming so the LLM loop never blocks on a slow socket — when the
/// client drops, `try_send` returns `Disconnected` and we ignore it.
const STREAM_CHANNEL_CAPACITY: usize = 64;

/// Reason the turn ended. Reported as the `finishReason` in the
/// trailing `d:` frame and used for tracing.
#[derive(Debug, Clone, Copy)]
enum StopReason {
    /// LLM stream ran to completion.
    Eof,
    /// User clicked the stop button → `/stop/{task_id}` cancelled us.
    Cancelled,
    /// Upstream returned an error mid-stream.
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
    /// `commit_turn` uses this to "last-writer-wins" against any racing
    /// turn submitted against the same parent.
    pub parent_id: Option<MessageId>,
    /// JWT we'll feed to `pool.acquire(bearer)` to get a fresh
    /// `AuthedDb`. Same value the original request used.
    pub bearer: String,
    /// Caller identity, snapshotted from the request. Kept for tracing;
    /// the DB-side identity comes from the bearer.
    pub auth: AuthContext,
    pub llm: Arc<dyn LlmClient>,
    pub chunk_embedder: Option<Arc<dyn Embedder>>,
    pub pool: RequestDbPool,
}

/// Spawn the worker. Allocates the [`TaskId`], registers a fresh cancel
/// token, sets up the mpsc, and detaches the worker future. Returns the
/// task id (so the POST handler can record-keep / log) and the receiver
/// (which the handler wraps in a `Body::from_stream`).
pub fn spawn_worker(
    tasks: Arc<TaskRegistry>,
    req: TurnRequest,
) -> (TaskId, mpsc::Receiver<Bytes>) {
    let task_id = TaskId::new();
    let cancel = CancellationToken::new();
    tasks.insert(task_id, cancel.clone());

    let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
    tokio::spawn(run(tasks, task_id, cancel, tx, req));
    (task_id, rx)
}

async fn run(
    tasks: Arc<TaskRegistry>,
    task_id: TaskId,
    cancel: CancellationToken,
    tx: mpsc::Sender<Bytes>,
    req: TurnRequest,
) {
    // Always remove ourselves from the registry on exit, regardless of
    // how the turn ends.
    let _guard = scopeguard(move || {
        let _ = tasks.remove(&task_id);
    });

    // First wire frame: tell the client our task id.
    let _ = tx.try_send(Bytes::from(proto::task(&task_id.to_string())));

    let outcome = drive_turn(task_id, &cancel, &tx, &req).await;

    if let Err(e) = outcome {
        error!(conv = %req.conversation_id, task = %task_id, error = %e, "turn ended with internal error");
    }
    // _guard drops here → tasks.remove fires.
}

/// Drive one full turn. `try_send`-failures on `tx` are intentionally
/// ignored — the client may have dropped the body; we keep going and
/// commit at the end.
async fn drive_turn(
    task_id: TaskId,
    cancel: &CancellationToken,
    tx: &mpsc::Sender<Bytes>,
    req: &TurnRequest,
) -> Result<(), String> {
    // Pool checkout — fresh `AuthedDb` for this worker, released back
    // when this function returns.
    let db = match req.pool.acquire(&req.bearer).await {
        Ok(d) => d,
        Err(e) => {
            error!(conv = %req.conversation_id, error = %e, "worker pool acquire failed");
            let _ = tx.try_send(Bytes::from(proto::error("auth setup failed")));
            let _ = tx.try_send(Bytes::from(proto::finish("error", "")));
            return Err(format!("pool acquire: {e}"));
        }
    };

    // Load committed history (the user message we're about to commit is
    // NOT persisted yet — `commit_turn` writes it at the end).
    let history = match db.list_messages(&req.conversation_id).await {
        Ok(m) => m,
        Err(e) => {
            error!(conv = %req.conversation_id, error = %e, "list_messages failed");
            let _ = tx.try_send(Bytes::from(proto::error("history lookup failed")));
            let _ = tx.try_send(Bytes::from(proto::finish("error", "")));
            return Err(format!("list_messages: {e}"));
        }
    };

    // Snapshot whether the conversation was unnamed BEFORE we run the
    // commit.
    let conversation_had_title = match db.get_conversation(&req.conversation_id).await {
        Ok(Some(c)) => c.title.is_some(),
        Ok(None) => false,
        Err(e) => {
            error!(conv = %req.conversation_id, error = %e, "get_conversation failed");
            false
        }
    };

    let mut prompt: Vec<LlmMessage> = history_to_llm(&history);
    // The user message isn't in `history` yet (we commit at the end);
    // append it so the LLM sees the current turn.
    prompt.push(LlmMessage {
        role: Role::User,
        content: req.user_text.clone(),
    });

    // RAG retrieval (best-effort). Same shape as before: embed the
    // user's message, KNN, expand neighbours, prepend a `[N]`-tagged
    // system message so the LLM can cite.
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
        let _ = tx.try_send(Bytes::from(proto::citations(&entries)));
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
            let _ = tx.try_send(Bytes::from(proto::error("llm error")));
            let _ = tx.try_send(Bytes::from(proto::finish("error", "")));
            return Err(format!("stream_chat: {e}"));
        }
    };

    // Per-delta loop. Cancellation races each `.next()`; on cancel we
    // DISCARD — nothing persisted, no `d:` frame (client already aborted).
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
                    let _ = tx.try_send(Bytes::from(proto::text(&t)));
                }
                Some(Err(e)) => {
                    error!(error = %e, "llm stream error");
                    let _ = tx.try_send(Bytes::from(proto::error("llm stream error")));
                    break StopReason::Error;
                }
                None => break StopReason::Eof,
            }
        }
    };

    drop(upstream); // run rig's stream Drop before commit

    if matches!(stop_reason, StopReason::Cancelled) {
        // DISCARD: no DB write, no `d:` frame.
        return Ok(());
    }

    // Commit the user+assistant pair atomically.
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
            let _ = tx.try_send(Bytes::from(proto::error("commit failed")));
            let _ = tx.try_send(Bytes::from(proto::finish("error", "")));
            return Err(format!("commit_turn: {e}"));
        }
    };

    let assistant_id_str = assistant_id.to_string();

    // Best-effort title generation after a successful commit. Failure
    // doesn't affect the `d:` emission.
    if !conversation_had_title && !assistant_buf.is_empty() {
        if let Some(title) =
            generate_title(req.llm.as_ref(), &req.user_text, &assistant_buf).await
        {
            if let Err(e) = db.rename_conversation(&req.conversation_id, &title).await {
                warn!(error = %e, "auto-title rename failed");
            }
        }
    }

    let _ = tx.try_send(Bytes::from(proto::finish(
        stop_reason.wire(),
        &assistant_id_str,
    )));

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

// ---------------------------------------------------------------------------
// scopeguard — three-line replacement so we don't pull in a crate
// ---------------------------------------------------------------------------

struct ScopeGuard<F: FnOnce()> {
    f: Option<F>,
}

impl<F: FnOnce()> Drop for ScopeGuard<F> {
    fn drop(&mut self) {
        if let Some(f) = self.f.take() {
            f();
        }
    }
}

fn scopeguard<F: FnOnce()>(f: F) -> ScopeGuard<F> {
    ScopeGuard { f: Some(f) }
}
