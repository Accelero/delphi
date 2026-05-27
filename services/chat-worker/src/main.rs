use axum::{routing::get, Router};
use chrono::Utc;
use delphi_config::{init_tracing, ServiceConfig};
use delphi_contracts::{
    ChatEvent, ChatEventEnvelope, ClearReason, FinishReason, InterruptReason, MessageRole,
    TurnRequested, CONTRACT_VERSION,
};
use delphi_llm::{llm_from_env, title_llm_from_env, LlmClient, LlmDelta, LlmMessage, Role};
use delphi_nats::{
    ChatBus, ChatLock, ChatLockState, ChatTerminalUpdate, NatsChatBus, NatsChatBusOptions,
    StopSignal,
};
use delphi_storage::{ChatRepository, SurrealChatRepository};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tokio::time::MissedTickBehavior;

const DEFAULT_CHAT_COMMAND_ACK_PROGRESS_SECONDS: u64 = 30;
const DEFAULT_CHAT_STOP_POLL_SECONDS: u64 = 1;

#[derive(Clone)]
struct WorkerState {
    repo: SurrealChatRepository,
    bus: NatsChatBus,
    llm: Arc<dyn LlmClient>,
    title_llm: Arc<dyn LlmClient>,
    worker_id: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let config = ServiceConfig::from_env(3003)?;
    let worker_id = std::env::var("CHAT_WORKER_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("chat-worker-{}", uuid::Uuid::new_v4()));
    tracing::info!(addr = %config.bind_addr, worker_id = %worker_id, "starting chat-worker");

    let llm = llm_from_env()?;
    let title_llm = title_llm_from_env(&llm)?;
    let state = WorkerState {
        repo: SurrealChatRepository::connect(
            &config.surreal_url,
            &config.surreal_namespace,
            &config.surreal_database,
            &config.surreal_user,
            &config.surreal_password,
        )
        .await?,
        bus: NatsChatBus::connect(
            &config.nats_url,
            NatsChatBusOptions {
                subscribe_commands: true,
                stop_worker_id: Some(worker_id.clone()),
                ..NatsChatBusOptions::default()
            },
        )
        .await?,
        llm,
        title_llm,
        worker_id,
    };
    let health_listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    tokio::spawn(async move {
        let app = Router::new().route("/healthz", get(healthz));
        if let Err(error) = axum::serve(health_listener, app).await {
            tracing::error!(?error, "chat-worker health server failed");
        }
    });
    let mut commands = state.bus.subscribe_commands();

    loop {
        let command = commands.recv().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(error) = run_turn(state, command).await {
                tracing::error!(?error, "chat turn failed");
            }
        });
    }
}

async fn healthz() -> &'static str {
    "ok"
}

async fn run_turn(state: WorkerState, command: TurnRequested) -> anyhow::Result<()> {
    let existing = state
        .bus
        .load_lock(&command.tenant_id, &command.conversation_id)
        .await?;
    let Some(existing) = existing else {
        publish_failed_turn(
            &state,
            &command,
            &command.user_id,
            "chat turn payload missing before worker claim".to_owned(),
        )
        .await?;
        state.bus.ack_turn_requested(&command.turn_id).await?;
        return Ok(());
    };

    if existing.turn_id != command.turn_id {
        return Err(anyhow::anyhow!("conversation is owned by a different turn"));
    }

    if existing.is_terminal() {
        publish_missing_terminal_event(&state, &command, &existing).await?;
        state.bus.ack_turn_requested(&command.turn_id).await?;
        state
            .bus
            .release_lock(
                &command.tenant_id,
                &command.conversation_id,
                &command.turn_id,
            )
            .await;
        return Ok(());
    }

    if existing.state == ChatLockState::Running {
        if !existing.is_expired() {
            return Err(anyhow::anyhow!(
                "chat turn is already running with a fresh lease"
            ));
        }
        let failed = state
            .bus
            .mark_lock_terminal(
                &command.tenant_id,
                &command.conversation_id,
                &command.turn_id,
                None,
                ChatTerminalUpdate {
                    state: ChatLockState::Failed,
                    assistant_message_id: None,
                    content: None,
                    error: Some("chat worker lease expired before completion".to_owned()),
                },
            )
            .await?;
        state
            .repo
            .record_turn_failed(
                &command.tenant_id,
                &failed.user_id,
                &command.conversation_id,
                &command.turn_id,
                "chat worker lease expired before completion",
            )
            .await?;
        publish_missing_terminal_event(&state, &command, &failed).await?;
        state.bus.ack_turn_requested(&command.turn_id).await?;
        state
            .bus
            .release_lock(
                &command.tenant_id,
                &command.conversation_id,
                &command.turn_id,
            )
            .await;
        return Ok(());
    }

    let lock = state
        .bus
        .claim_lock(
            &command.tenant_id,
            &command.conversation_id,
            &command.turn_id,
            &state.worker_id,
        )
        .await?;

    let (shutdown_maintenance, maintenance_errors, maintenance_task) =
        start_turn_maintenance(state.clone(), command.clone());

    let result = drive_turn(state.clone(), command.clone(), lock, maintenance_errors).await;
    let _ = shutdown_maintenance.send(true);
    let _ = maintenance_task.await;

    if result.is_ok() {
        state.bus.ack_turn_requested(&command.turn_id).await?;
        state
            .bus
            .release_lock(
                &command.tenant_id,
                &command.conversation_id,
                &command.turn_id,
            )
            .await;
    }
    result
}

async fn publish_missing_terminal_event(
    state: &WorkerState,
    command: &TurnRequested,
    lock: &ChatLock,
) -> anyhow::Result<()> {
    if lock.terminal_event_published {
        return Ok(());
    }
    match lock.state {
        ChatLockState::Committed => {
            let assistant_message_id = lock
                .assistant_message_id
                .clone()
                .ok_or_else(|| anyhow::anyhow!("committed turn missing assistant message id"))?;
            publish_event(
                state,
                command,
                ChatEvent::Finish {
                    assistant_message_id,
                    finish_reason: FinishReason::Stop,
                },
            )
            .await?;
        }
        ChatLockState::Interrupted => {
            let assistant_message_id = lock
                .assistant_message_id
                .clone()
                .ok_or_else(|| anyhow::anyhow!("interrupted turn missing assistant message id"))?;
            publish_event(
                state,
                command,
                ChatEvent::Interrupted {
                    assistant_message_id,
                    content: lock.terminal_content.clone().unwrap_or_default(),
                    finish_reason: InterruptReason::UserInterrupted,
                },
            )
            .await?;
        }
        ChatLockState::Failed => {
            publish_event(
                state,
                command,
                ChatEvent::Error {
                    message: "The assistant failed before the turn could be saved.".to_owned(),
                },
            )
            .await?;
            publish_event(
                state,
                command,
                ChatEvent::Clear {
                    reason: ClearReason::FailedBeforeCommit,
                },
            )
            .await?;
        }
        ChatLockState::Requested | ChatLockState::Running => return Ok(()),
    }
    state
        .bus
        .mark_terminal_event_published(
            &command.tenant_id,
            &command.conversation_id,
            &command.turn_id,
        )
        .await?;
    Ok(())
}

async fn drive_turn(
    state: WorkerState,
    command: TurnRequested,
    lock: ChatLock,
    mut maintenance_errors: watch::Receiver<Option<String>>,
) -> anyhow::Result<()> {
    let mut stops = state.bus.subscribe_stops();
    let conversation = state
        .repo
        .get_conversation(&command.tenant_id, &lock.user_id, &command.conversation_id)
        .await?;
    let should_generate_title =
        conversation.title == "New chat" && conversation.messages.is_empty();

    publish_event(
        &state,
        &command,
        ChatEvent::TurnStarted {
            turn_id: command.turn_id.clone(),
        },
    )
    .await?;
    publish_event(
        &state,
        &command,
        ChatEvent::UserMessage {
            id: lock.user_message_id.clone(),
            content: lock.text.clone(),
        },
    )
    .await?;

    if lock.stop_requested || stop_requested(&state, &mut stops, &command).await? {
        commit_interrupted_turn(&state, &command, &lock, String::new()).await?;
        return Ok(());
    }

    let mut assistant_text = String::new();
    let mut stream = match state
        .llm
        .stream_chat(llm_messages(conversation.messages, &lock.text))
        .await
    {
        Ok(stream) => stream,
        Err(error) => {
            fail_owned_turn(
                &state,
                &command,
                &lock,
                format!("LLM request failed: {error}"),
            )
            .await?;
            return Ok(());
        }
    };

    let mut stop_poll = tokio::time::interval(duration_from_env(
        "CHAT_STOP_POLL_SECONDS",
        DEFAULT_CHAT_STOP_POLL_SECONDS,
    ));
    stop_poll.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            changed = maintenance_errors.changed() => {
                if changed.is_ok() {
                    let message = { maintenance_errors.borrow().clone() };
                    if let Some(message) = message {
                        fail_owned_turn(&state, &command, &lock, message.clone()).await?;
                        return Ok(());
                    }
                }
            }
            _ = stop_poll.tick() => {
                if lock_stop_requested(&state, &command).await? {
                    commit_interrupted_turn(&state, &command, &lock, assistant_text).await?;
                    return Ok(());
                }
            }
            signal = stops.recv() => {
                match signal {
                    Ok(signal) if is_stop_for_turn(&signal, &command) => {
                        commit_interrupted_turn(&state, &command, &lock, assistant_text).await?;
                        return Ok(());
                    }
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => {}
                }
            }
            delta = stream.next() => {
                let Some(delta) = delta else {
                    break;
                };

                match delta {
                    Ok(LlmDelta::Text(chunk)) => {
                        assistant_text.push_str(&chunk);
                        publish_event(&state, &command, ChatEvent::TextDelta { delta: chunk }).await?;
                    }
                    Err(error) => {
                        fail_owned_turn(&state, &command, &lock, format!("LLM stream failed: {error}"))
                            .await?;
                        return Ok(());
                    }
                }
            }
        }
    }

    let assistant_message_id = ulid::Ulid::new().to_string();
    if let Err(error) = state
        .repo
        .commit_turn(
            &command.tenant_id,
            &lock.user_id,
            &command.conversation_id,
            &command.turn_id,
            &lock.user_message_id,
            &lock.text,
            lock.parent_message_id.as_deref(),
            &assistant_message_id,
            &assistant_text,
            Vec::new(),
        )
        .await
    {
        let _ = fail_owned_turn(
            &state,
            &command,
            &lock,
            format!("failed to commit completed chat turn: {error}"),
        )
        .await;
        return Ok(());
    }
    let terminal_lock = state
        .bus
        .mark_lock_terminal(
            &command.tenant_id,
            &command.conversation_id,
            &command.turn_id,
            Some(&state.worker_id),
            ChatTerminalUpdate {
                state: ChatLockState::Committed,
                assistant_message_id: Some(assistant_message_id.clone()),
                content: None,
                error: None,
            },
        )
        .await?;
    publish_event(
        &state,
        &command,
        ChatEvent::Finish {
            assistant_message_id,
            finish_reason: FinishReason::Stop,
        },
    )
    .await?;
    state
        .bus
        .mark_terminal_event_published(
            &terminal_lock.tenant_id,
            &terminal_lock.conversation_id,
            &terminal_lock.turn_id,
        )
        .await?;
    if should_generate_title && !assistant_text.trim().is_empty() {
        spawn_title_generation(state, command, lock, assistant_text);
    }
    Ok(())
}

fn start_turn_maintenance(
    state: WorkerState,
    command: TurnRequested,
) -> (
    watch::Sender<bool>,
    watch::Receiver<Option<String>>,
    tokio::task::JoinHandle<()>,
) {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let (error_tx, error_rx) = watch::channel(None);
    let task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(duration_from_env(
            "CHAT_COMMAND_ACK_PROGRESS_SECONDS",
            DEFAULT_CHAT_COMMAND_ACK_PROGRESS_SECONDS,
        ));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                _ = interval.tick() => {
                    if let Err(error) = state.bus.progress_turn_requested(&command.turn_id).await {
                        tracing::warn!(?error, turn_id = %command.turn_id, "failed to send chat command progress ack");
                    }
                    if let Err(error) = state.bus.renew_lock(
                        &command.tenant_id,
                        &command.conversation_id,
                        &command.turn_id,
                        &state.worker_id,
                    ).await {
                        let message = format!("chat turn lock renewal failed: {error}");
                        let _ = error_tx.send(Some(message));
                        break;
                    }
                }
            }
        }
    });
    (shutdown_tx, error_rx, task)
}

fn duration_from_env(name: &str, default_seconds: u64) -> Duration {
    let seconds = std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .unwrap_or(default_seconds);
    Duration::from_secs(seconds)
}

async fn commit_interrupted_turn(
    state: &WorkerState,
    command: &TurnRequested,
    lock: &ChatLock,
    assistant_text: String,
) -> anyhow::Result<()> {
    let assistant_message_id = ulid::Ulid::new().to_string();
    if let Err(error) = state
        .repo
        .commit_interrupted_turn(
            &command.tenant_id,
            &lock.user_id,
            &command.conversation_id,
            &command.turn_id,
            &lock.user_message_id,
            &lock.text,
            lock.parent_message_id.as_deref(),
            &assistant_message_id,
            &assistant_text,
            Vec::new(),
        )
        .await
    {
        let _ = fail_owned_turn(
            state,
            command,
            lock,
            format!("failed to commit interrupted chat turn: {error}"),
        )
        .await;
        return Ok(());
    }
    let terminal_lock = state
        .bus
        .mark_lock_terminal(
            &command.tenant_id,
            &command.conversation_id,
            &command.turn_id,
            Some(&state.worker_id),
            ChatTerminalUpdate {
                state: ChatLockState::Interrupted,
                assistant_message_id: Some(assistant_message_id.clone()),
                content: Some(assistant_text.clone()),
                error: None,
            },
        )
        .await?;
    publish_event(
        state,
        command,
        ChatEvent::Interrupted {
            assistant_message_id,
            content: assistant_text,
            finish_reason: InterruptReason::UserInterrupted,
        },
    )
    .await?;
    state
        .bus
        .mark_terminal_event_published(
            &terminal_lock.tenant_id,
            &terminal_lock.conversation_id,
            &terminal_lock.turn_id,
        )
        .await?;
    Ok(())
}

fn llm_messages(history: Vec<delphi_contracts::MessageDto>, user_text: &str) -> Vec<LlmMessage> {
    let mut messages = Vec::with_capacity(history.len() + 1);
    let last_history_message_id = history.last().map(|message| message.id.clone());
    for message in history {
        let interrupted_tail =
            Some(message.id.as_str()) == last_history_message_id.as_deref() && message.interrupted;
        messages.push(LlmMessage {
            role: match message.role {
                MessageRole::User => Role::User,
                MessageRole::Assistant => Role::Assistant,
                MessageRole::System => Role::System,
            },
            content: message.content,
        });
        if interrupted_tail {
            messages.push(LlmMessage {
                role: Role::System,
                content: "The previous assistant response was interrupted by the user and may be incomplete. Treat it as partial context, not as a finished answer."
                    .to_owned(),
            });
        }
    }
    messages.push(LlmMessage {
        role: Role::User,
        content: user_text.to_owned(),
    });
    messages
}

fn spawn_title_generation(
    state: WorkerState,
    command: TurnRequested,
    lock: ChatLock,
    assistant_text: String,
) {
    tokio::spawn(async move {
        let Some(title) =
            generate_title(state.title_llm.as_ref(), &lock.text, &assistant_text).await
        else {
            return;
        };
        match state
            .repo
            .rename_conversation_if_default(
                &command.tenant_id,
                &lock.user_id,
                &command.conversation_id,
                title.clone(),
            )
            .await
        {
            Ok(Some(_)) => {
                if let Err(error) =
                    publish_event(&state, &command, ChatEvent::TitleUpdated { title }).await
                {
                    tracing::warn!(?error, "failed to publish generated chat title");
                }
            }
            Ok(None) => {}
            Err(error) => tracing::warn!(?error, "failed to persist generated chat title"),
        }
    });
}

async fn generate_title(
    llm: &dyn LlmClient,
    user_msg: &str,
    assistant_msg: &str,
) -> Option<String> {
    let prompt = vec![
        LlmMessage {
            role: Role::System,
            content: "You produce concise chat titles. Respond with ONLY the title (no quotes, no preamble), 60 characters or less, summarising the user's question."
                .to_owned(),
        },
        LlmMessage {
            role: Role::User,
            content: format!("User: {user_msg}\n\nAssistant: {assistant_msg}\n\nTitle:"),
        },
    ];
    let mut stream = match llm.stream_chat(prompt).await {
        Ok(stream) => stream,
        Err(error) => {
            tracing::warn!(?error, "title llm call failed");
            return None;
        }
    };
    let mut output = String::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(LlmDelta::Text(text)) => output.push_str(&text),
            Err(error) => {
                tracing::warn!(?error, "title llm stream failed");
                return None;
            }
        }
    }
    let title = clean_title(&output);
    (!title.is_empty()).then_some(title)
}

fn clean_title(raw: &str) -> String {
    let mut title = raw.trim().to_owned();
    let quotes = ['"', '\'', '“', '”', '‘', '’'];
    if title.chars().count() >= 2 {
        if let (Some(first), Some(last)) = (title.chars().next(), title.chars().last()) {
            if quotes.contains(&first) && quotes.contains(&last) {
                title = title
                    .chars()
                    .skip(1)
                    .take(title.chars().count().saturating_sub(2))
                    .collect::<String>()
                    .trim()
                    .to_owned();
            }
        }
    }
    if title.chars().count() > 60 {
        title = title.chars().take(60).collect();
    }
    title
}

async fn fail_owned_turn(
    state: &WorkerState,
    command: &TurnRequested,
    lock: &ChatLock,
    message: String,
) -> anyhow::Result<()> {
    let failed = state
        .bus
        .mark_lock_terminal(
            &command.tenant_id,
            &command.conversation_id,
            &command.turn_id,
            Some(&state.worker_id),
            ChatTerminalUpdate {
                state: ChatLockState::Failed,
                assistant_message_id: None,
                content: None,
                error: Some(message.clone()),
            },
        )
        .await?;
    publish_failed_turn(state, command, &lock.user_id, message).await?;
    state
        .bus
        .mark_terminal_event_published(&failed.tenant_id, &failed.conversation_id, &failed.turn_id)
        .await?;
    Ok(())
}

async fn publish_failed_turn(
    state: &WorkerState,
    command: &TurnRequested,
    user_id: &str,
    message: String,
) -> anyhow::Result<()> {
    let _ = state
        .repo
        .record_turn_failed(
            &command.tenant_id,
            user_id,
            &command.conversation_id,
            &command.turn_id,
            &message,
        )
        .await;
    publish_event(
        state,
        command,
        ChatEvent::Error {
            message: "The assistant failed before the turn could be saved.".to_owned(),
        },
    )
    .await?;
    publish_event(
        state,
        command,
        ChatEvent::Clear {
            reason: ClearReason::FailedBeforeCommit,
        },
    )
    .await?;
    tracing::error!(%message, "chat turn failed before commit");
    Ok(())
}

async fn publish_event(
    state: &WorkerState,
    command: &TurnRequested,
    event: ChatEvent,
) -> anyhow::Result<String> {
    let envelope = ChatEventEnvelope {
        v: CONTRACT_VERSION,
        tenant_id: command.tenant_id.clone(),
        user_id: command.user_id.clone(),
        conversation_id: command.conversation_id.clone(),
        turn_id: command.turn_id.clone(),
        ts: Utc::now(),
        event,
    };
    Ok(state.bus.publish_event(envelope).await?)
}

fn received_stop(stops: &mut broadcast::Receiver<StopSignal>, command: &TurnRequested) -> bool {
    loop {
        match stops.try_recv() {
            Ok(signal) => {
                if signal.tenant_id == command.tenant_id
                    && signal.conversation_id == command.conversation_id
                    && signal.turn_id == command.turn_id
                {
                    return true;
                }
            }
            Err(broadcast::error::TryRecvError::Empty) => return false,
            Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(broadcast::error::TryRecvError::Closed) => return false,
        }
    }
}

fn is_stop_for_turn(signal: &StopSignal, command: &TurnRequested) -> bool {
    signal.tenant_id == command.tenant_id
        && signal.conversation_id == command.conversation_id
        && signal.turn_id == command.turn_id
}

async fn stop_requested(
    state: &WorkerState,
    stops: &mut broadcast::Receiver<StopSignal>,
    command: &TurnRequested,
) -> anyhow::Result<bool> {
    if received_stop(stops, command) {
        return Ok(true);
    }
    lock_stop_requested(state, command).await
}

async fn lock_stop_requested(state: &WorkerState, command: &TurnRequested) -> anyhow::Result<bool> {
    Ok(state
        .bus
        .stop_requested(
            &command.tenant_id,
            &command.conversation_id,
            &command.turn_id,
        )
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use delphi_contracts::MessageDto;

    fn message(id: &str, role: MessageRole, content: &str, interrupted: bool) -> MessageDto {
        MessageDto {
            id: id.to_owned(),
            role,
            content: content.to_owned(),
            parent_message_id: None,
            citations: Vec::new(),
            turn_id: Some("01HX0000000000000000000004".to_owned()),
            interrupted,
            finish_reason: interrupted.then(|| "user_interrupted".to_owned()),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn llm_messages_marks_interrupted_tail_as_partial_context() {
        let history = vec![
            message(
                "01HX0000000000000000000004",
                MessageRole::User,
                "summarize this",
                false,
            ),
            message(
                "01HX0000000000000000000005",
                MessageRole::Assistant,
                "The summary starts with",
                true,
            ),
        ];

        let messages = llm_messages(history, "continue from there");

        assert!(messages.iter().any(|message| {
            message.role == Role::System
                && message.content.contains("interrupted by the user")
                && message.content.contains("partial context")
        }));
        assert_eq!(messages.last().unwrap().role, Role::User);
        assert_eq!(messages.last().unwrap().content, "continue from there");
    }

    #[test]
    fn llm_messages_does_not_mark_older_interrupted_messages() {
        let history = vec![
            message(
                "01HX0000000000000000000004",
                MessageRole::Assistant,
                "old partial",
                true,
            ),
            message(
                "01HX0000000000000000000005",
                MessageRole::User,
                "later message",
                false,
            ),
        ];

        let messages = llm_messages(history, "continue from there");

        assert!(!messages
            .iter()
            .any(|message| message.content.contains("interrupted by the user")));
    }
}
