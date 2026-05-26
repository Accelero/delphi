use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use delphi_auth::{AuthContext, AuthError, AuthVerifier};
use delphi_config::{init_tracing, ServiceConfig};
use delphi_contracts::{ClientWsMessage, ConversationDetail, ServerWsMessage};
use delphi_nats::{ChatBus, NatsChatBus, NatsChatBusOptions, ReplayIndex, ReplayTurn};
use delphi_storage::{ChatRepository, SurrealChatRepository};
use std::collections::{HashMap, HashSet};
use tower_http::trace::TraceLayer;

#[derive(Clone)]
struct AppState {
    auth: AuthVerifier,
    bus: NatsChatBus,
    repo: SurrealChatRepository,
}

impl axum::extract::FromRef<AppState> for AuthVerifier {
    fn from_ref(state: &AppState) -> Self {
        state.auth.clone()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let config = ServiceConfig::from_env(3002)?;
    let state = AppState {
        auth: AuthVerifier::from_env()?,
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
                subscribe_events: true,
                ..NatsChatBusOptions::default()
            },
        )
        .await?,
    };

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/ws/chat", get(chat_ws))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    tracing::info!(addr = %config.bind_addr, "starting realtime-service");
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn healthz() -> &'static str {
    "ok"
}

async fn chat_ws(
    auth: AuthContext,
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Result<Response, WsAuthError> {
    Ok(ws.on_upgrade(move |socket| run_socket(socket, auth, state)))
}

async fn run_socket(mut socket: WebSocket, auth: AuthContext, state: AppState) {
    tracing::info!(user_id = %auth.user_id, tenant_id = %auth.tenant_id, "websocket connected");
    let mut subscriptions = HashSet::<String>::new();
    let mut last_sent_sequences = HashMap::<String, u64>::new();
    let mut events = state.bus.subscribe_events();

    loop {
        tokio::select! {
            inbound = socket.recv() => {
                match inbound {
                    Some(Ok(Message::Text(text))) => {
                        if !handle_client_message(
                            &mut socket,
                            &mut subscriptions,
                            &mut last_sent_sequences,
                            &text,
                            &auth,
                            &state,
                        ).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(bytes))) => {
                        if socket.send(Message::Pong(bytes)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        tracing::warn!(?error, "websocket receive error");
                        break;
                    }
                }
            }
            event = events.recv() => {
                let Ok(event) = event else {
                    continue;
                };
                if event.envelope.tenant_id != auth.tenant_id {
                    continue;
                }
                if !subscriptions.contains(&event.envelope.conversation_id) {
                    continue;
                }
                if event.envelope.user_id != auth.user_id {
                    continue;
                }
                let sequence = parse_event_id(&event.event_id).unwrap_or(0);
                let last_sent = last_sent_sequences
                    .get(&event.envelope.conversation_id)
                    .copied()
                    .unwrap_or(0);
                if sequence <= last_sent {
                    continue;
                }

                let msg = ServerWsMessage::Event {
                    conversation_id: event.envelope.conversation_id.clone(),
                    event_id: event.event_id,
                    event: event.envelope.event,
                };
                if send_json(&mut socket, &msg).await.is_err() {
                    break;
                }
                last_sent_sequences.insert(msg_conversation_id(&msg), sequence);
            }
        }
    }

    tracing::info!(user_id = %auth.user_id, "websocket disconnected");
}

async fn handle_client_message(
    socket: &mut WebSocket,
    subscriptions: &mut HashSet<String>,
    last_sent_sequences: &mut HashMap<String, u64>,
    text: &str,
    auth: &AuthContext,
    state: &AppState,
) -> bool {
    let decoded = serde_json::from_str::<ClientWsMessage>(text);
    match decoded {
        Ok(ClientWsMessage::SubscribeConversation {
            conversation_id,
            last_event_id,
        }) => {
            let conversation = match state
                .repo
                .get_conversation(&auth.tenant_id, &auth.user_id, &conversation_id)
                .await
            {
                Ok(conversation) => conversation,
                Err(_) => {
                    return send_json(
                        socket,
                        &ServerWsMessage::Error {
                            code: "not_found".to_owned(),
                            message: "conversation not found".to_owned(),
                        },
                    )
                    .await
                    .is_ok();
                }
            };

            let replay_plan = match build_replay_plan(
                state,
                auth,
                &conversation_id,
                last_event_id.as_deref(),
                &conversation,
            )
            .await
            {
                Ok(plan) => plan,
                Err(ReplayDecisionError::ResyncRequired) => {
                    return send_json(socket, &ServerWsMessage::ResyncRequired { conversation_id })
                        .await
                        .is_ok();
                }
                Err(ReplayDecisionError::Internal(error)) => {
                    tracing::warn!(%error, conversation_id, "failed to prepare websocket replay");
                    return send_json(
                        socket,
                        &ServerWsMessage::Error {
                            code: "replay_failed".to_owned(),
                            message: "failed to prepare websocket replay".to_owned(),
                        },
                    )
                    .await
                    .is_ok();
                }
            };

            subscriptions.insert(conversation_id.clone());
            if send_json(
                socket,
                &ServerWsMessage::Subscribed {
                    conversation_id: conversation_id.clone(),
                },
            )
            .await
            .is_err()
            {
                return false;
            }
            let mut last_sent = replay_plan.last_seen_sequence;
            for event in replay_plan.events {
                if event.envelope.user_id != auth.user_id {
                    continue;
                }
                let sequence = parse_event_id(&event.event_id).unwrap_or(0);
                if sequence <= last_sent {
                    continue;
                }
                let msg = ServerWsMessage::Event {
                    conversation_id: conversation_id.clone(),
                    event_id: event.event_id,
                    event: event.envelope.event,
                };
                if send_json(socket, &msg).await.is_err() {
                    return false;
                }
                last_sent = sequence;
            }
            last_sent_sequences.insert(conversation_id, last_sent);
            true
        }
        Ok(ClientWsMessage::UnsubscribeConversation { conversation_id }) => {
            subscriptions.remove(&conversation_id);
            true
        }
        Ok(ClientWsMessage::Ping { nonce }) => send_json(socket, &ServerWsMessage::Pong { nonce })
            .await
            .is_ok(),
        Err(_) => send_json(
            socket,
            &ServerWsMessage::Error {
                code: "invalid_message".to_owned(),
                message: "invalid websocket message".to_owned(),
            },
        )
        .await
        .is_ok(),
    }
}

#[derive(Debug)]
struct ReplayPlan {
    events: Vec<delphi_nats::SequencedChatEvent>,
    last_seen_sequence: u64,
}

#[derive(Debug)]
enum ReplayDecisionError {
    ResyncRequired,
    Internal(String),
}

async fn build_replay_plan(
    state: &AppState,
    auth: &AuthContext,
    conversation_id: &str,
    last_event_id: Option<&str>,
    conversation: &ConversationDetail,
) -> Result<ReplayPlan, ReplayDecisionError> {
    let index = state
        .bus
        .load_replay_index(&auth.tenant_id, conversation_id)
        .await
        .map_err(|error| ReplayDecisionError::Internal(error.to_string()))?;
    let latest_sequence = state
        .bus
        .latest_event_sequence()
        .await
        .map_err(|error| ReplayDecisionError::Internal(error.to_string()))?;

    let Some(index) = index else {
        return if last_event_id.is_some() {
            Err(ReplayDecisionError::ResyncRequired)
        } else {
            Ok(ReplayPlan {
                events: Vec::new(),
                last_seen_sequence: 0,
            })
        };
    };

    let Some((start_seq, end_seq, last_seen_sequence)) =
        replay_range(&index, last_event_id, conversation, latest_sequence)?
    else {
        return Ok(ReplayPlan {
            events: Vec::new(),
            last_seen_sequence: last_event_id.and_then(parse_event_id).unwrap_or(0),
        });
    };

    let events = state
        .bus
        .replay_events(&auth.tenant_id, conversation_id, start_seq, end_seq)
        .await
        .map_err(|error| ReplayDecisionError::Internal(error.to_string()))?;

    if last_event_id.is_none() && events.is_empty() && start_seq <= end_seq {
        return Err(ReplayDecisionError::ResyncRequired);
    }

    Ok(ReplayPlan {
        events,
        last_seen_sequence,
    })
}

fn replay_range(
    index: &ReplayIndex,
    last_event_id: Option<&str>,
    conversation: &ConversationDetail,
    latest_sequence: u64,
) -> Result<Option<(u64, u64, u64)>, ReplayDecisionError> {
    let Some(last_event_id) = last_event_id else {
        let Some(current) = index.current_turn.as_ref() else {
            return Ok(None);
        };
        if current.end_seq.is_some() || conversation_has_turn(conversation, &current.turn_id) {
            return Ok(None);
        }
        let end_seq = current.end_seq.unwrap_or(latest_sequence);
        return Ok(Some((current.start_seq, end_seq, 0)));
    };

    let last_sequence = parse_event_id(last_event_id).ok_or(ReplayDecisionError::ResyncRequired)?;
    if let Some(current) = index.current_turn.as_ref() {
        if turn_contains_sequence(current, last_sequence, latest_sequence) {
            let end_seq = current.end_seq.unwrap_or(latest_sequence);
            if last_sequence >= end_seq {
                return Ok(None);
            }
            return Ok(Some((last_sequence + 1, end_seq, last_sequence)));
        }
    }

    if let Some(previous) = index.previous_turn.as_ref() {
        if turn_contains_sequence(previous, last_sequence, latest_sequence) {
            let end_seq = index
                .current_turn
                .as_ref()
                .map(|turn| turn.end_seq.unwrap_or(latest_sequence))
                .or(previous.end_seq)
                .unwrap_or(latest_sequence);
            if last_sequence >= end_seq {
                return Ok(None);
            }
            return Ok(Some((last_sequence + 1, end_seq, last_sequence)));
        }
    }

    Err(ReplayDecisionError::ResyncRequired)
}

fn turn_contains_sequence(turn: &ReplayTurn, sequence: u64, latest_sequence: u64) -> bool {
    let end_seq = turn.end_seq.unwrap_or(latest_sequence);
    sequence >= turn.start_seq && sequence <= end_seq
}

fn conversation_has_turn(conversation: &ConversationDetail, turn_id: &str) -> bool {
    conversation
        .messages
        .iter()
        .any(|message| message.turn_id.as_deref() == Some(turn_id))
}

fn parse_event_id(value: &str) -> Option<u64> {
    value.parse::<u64>().ok()
}

fn msg_conversation_id(msg: &ServerWsMessage) -> String {
    match msg {
        ServerWsMessage::Event {
            conversation_id, ..
        } => conversation_id.clone(),
        _ => String::new(),
    }
}

async fn send_json(socket: &mut WebSocket, msg: &ServerWsMessage) -> Result<(), axum::Error> {
    let text = serde_json::to_string(msg).expect("server websocket message serializes");
    socket.send(Message::Text(text.into())).await
}

struct WsAuthError;

impl From<AuthError> for WsAuthError {
    fn from(_: AuthError) -> Self {
        Self
    }
}

impl IntoResponse for WsAuthError {
    fn into_response(self) -> Response {
        StatusCode::UNAUTHORIZED.into_response()
    }
}
