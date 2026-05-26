use chrono::Utc;
use delphi_config::{init_tracing, ServiceConfig};
use delphi_contracts::{
    ChatEvent, ChatEventEnvelope, ClearReason, FinishReason, InterruptReason, MessageRole,
    TurnRequested, CONTRACT_VERSION,
};
use delphi_llm::{llm_from_env, LlmClient, LlmDelta, LlmMessage, Role};
use delphi_nats::{ChatBus, NatsChatBus, NatsChatBusOptions, StopSignal};
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
        llm: llm_from_env()?,
        worker_id,
    };
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

async fn run_turn(state: WorkerState, command: TurnRequested) -> anyhow::Result<()> {
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

    let result = drive_turn(
        state.clone(),
        command.clone(),
        lock.stop_requested,
        maintenance_errors,
    )
    .await;
    let _ = shutdown_maintenance.send(true);
    let _ = maintenance_task.await;

    state
        .bus
        .release_lock(
            &command.tenant_id,
            &command.conversation_id,
            &command.turn_id,
        )
        .await;
    state.bus.ack_turn_requested(&command.turn_id).await?;
    result
}

async fn drive_turn(
    state: WorkerState,
    command: TurnRequested,
    initial_stop_requested: bool,
    mut maintenance_errors: watch::Receiver<Option<String>>,
) -> anyhow::Result<()> {
    let mut stops = state.bus.subscribe_stops();
    let conversation = state
        .repo
        .get_conversation(
            &command.tenant_id,
            &command.user_id,
            &command.conversation_id,
        )
        .await?;

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
            id: command.user_message_id.clone(),
            content: command.text.clone(),
        },
    )
    .await?;

    if initial_stop_requested || stop_requested(&state, &mut stops, &command).await? {
        commit_interrupted_turn(&state, &command, String::new()).await?;
        return Ok(());
    }

    let mut assistant_text = String::new();
    let mut stream = match state
        .llm
        .stream_chat(llm_messages(conversation.messages, &command))
        .await
    {
        Ok(stream) => stream,
        Err(error) => {
            publish_failed_turn(&state, &command, format!("LLM request failed: {error}")).await?;
            return Err(error);
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
                        publish_failed_turn(&state, &command, message.clone()).await?;
                        return Err(anyhow::anyhow!(message));
                    }
                }
            }
            _ = stop_poll.tick() => {
                if lock_stop_requested(&state, &command).await? {
                    commit_interrupted_turn(&state, &command, assistant_text).await?;
                    return Ok(());
                }
            }
            signal = stops.recv() => {
                match signal {
                    Ok(signal) if is_stop_for_turn(&signal, &command) => {
                        commit_interrupted_turn(&state, &command, assistant_text).await?;
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
                        publish_failed_turn(&state, &command, format!("LLM stream failed: {error}"))
                            .await?;
                        return Err(error);
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
            &command.user_id,
            &command.conversation_id,
            &command.turn_id,
            &command.user_message_id,
            &command.text,
            command.parent_message_id.as_deref(),
            &assistant_message_id,
            &assistant_text,
            Vec::new(),
        )
        .await
    {
        let _ = publish_failed_turn(
            &state,
            &command,
            format!("failed to commit completed chat turn: {error}"),
        )
        .await;
        return Err(error.into());
    }
    publish_event(
        &state,
        &command,
        ChatEvent::Finish {
            assistant_message_id,
            finish_reason: FinishReason::Stop,
        },
    )
    .await?;
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
    assistant_text: String,
) -> anyhow::Result<()> {
    let assistant_message_id = ulid::Ulid::new().to_string();
    if let Err(error) = state
        .repo
        .commit_interrupted_turn(
            &command.tenant_id,
            &command.user_id,
            &command.conversation_id,
            &command.turn_id,
            &command.user_message_id,
            &command.text,
            command.parent_message_id.as_deref(),
            &assistant_message_id,
            &assistant_text,
            Vec::new(),
        )
        .await
    {
        let _ = publish_failed_turn(
            state,
            command,
            format!("failed to commit interrupted chat turn: {error}"),
        )
        .await;
        return Err(error.into());
    }
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
    Ok(())
}

fn llm_messages(
    history: Vec<delphi_contracts::MessageDto>,
    command: &TurnRequested,
) -> Vec<LlmMessage> {
    let mut messages = Vec::with_capacity(history.len() + 1);
    messages.push(LlmMessage {
        role: Role::System,
        content: "You are delphi, a research assistant.".to_owned(),
    });
    messages.extend(history.into_iter().map(|message| LlmMessage {
        role: match message.role {
            MessageRole::User => Role::User,
            MessageRole::Assistant => Role::Assistant,
            MessageRole::System => Role::System,
        },
        content: message.content,
    }));
    messages.push(LlmMessage {
        role: Role::User,
        content: command.text.clone(),
    });
    messages
}

async fn publish_failed_turn(
    state: &WorkerState,
    command: &TurnRequested,
    message: String,
) -> anyhow::Result<()> {
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
