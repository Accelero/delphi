use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use delphi_auth::{AuthContext, AuthError, AuthVerifier};
use delphi_config::{init_tracing, ServiceConfig};
use delphi_contracts::{ClientWsMessage, ConversationDetail, ServerWsMessage};
use delphi_nats::{
    ChatBus, NatsChatBus, NatsChatBusOptions, ReplayIndex, ReplayTurn, SequencedChatEvent,
};
use delphi_storage::{ChatRepository, SurrealChatRepository};
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::task::JoinHandle;
use tower_http::trace::TraceLayer;

const DEFAULT_WS_OUTBOUND_QUEUE_SIZE: usize = 256;
const DEFAULT_WS_EVENT_QUEUE_SIZE: usize = 1024;
const DEFAULT_CONVERSATION_EVENT_BUFFER_SIZE: usize = 1024;

#[derive(Clone)]
struct AppState {
    auth: AuthVerifier,
    bus: NatsChatBus,
    events: ConversationEventRegistry,
    repo: SurrealChatRepository,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct ConversationKey {
    tenant_id: String,
    conversation_id: String,
}

#[derive(Clone, Default)]
struct ConversationEventRegistry {
    hubs: Arc<Mutex<HashMap<ConversationKey, Weak<ConversationEventHub>>>>,
}

struct ConversationEventHub {
    sender: broadcast::Sender<SequencedChatEvent>,
    task: JoinHandle<()>,
}

impl ConversationEventHub {
    fn subscribe(self: &Arc<Self>) -> ConversationEventSubscription {
        ConversationEventSubscription {
            hub: self.clone(),
            receiver: self.sender.subscribe(),
        }
    }
}

impl Drop for ConversationEventHub {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct ConversationEventSubscription {
    hub: Arc<ConversationEventHub>,
    receiver: broadcast::Receiver<SequencedChatEvent>,
}

impl ConversationEventRegistry {
    async fn subscribe(
        &self,
        bus: &NatsChatBus,
        tenant_id: &str,
        conversation_id: &str,
    ) -> Result<ConversationEventSubscription, delphi_nats::ChatBusError> {
        let key = ConversationKey {
            tenant_id: tenant_id.to_owned(),
            conversation_id: conversation_id.to_owned(),
        };

        if let Some(hub) = self.existing_hub(&key).await {
            return Ok(hub.subscribe());
        }

        let mut source = bus
            .subscribe_conversation_events(tenant_id, conversation_id)
            .await?;
        let (sender, _) = broadcast::channel(conversation_event_buffer_size());
        let fanout_sender = sender.clone();
        let task = tokio::spawn(async move {
            while let Some(event) = source.recv().await {
                let _ = fanout_sender.send(event);
            }
        });
        let hub = Arc::new(ConversationEventHub { sender, task });

        let mut hubs = self.hubs.lock().await;
        if let Some(existing) = hubs.get(&key).and_then(Weak::upgrade) {
            return Ok(existing.subscribe());
        }
        hubs.insert(key, Arc::downgrade(&hub));
        Ok(hub.subscribe())
    }

    async fn existing_hub(&self, key: &ConversationKey) -> Option<Arc<ConversationEventHub>> {
        self.hubs.lock().await.get(key).and_then(Weak::upgrade)
    }
}

struct ActiveSubscription {
    task: JoinHandle<()>,
}

enum SocketEvent {
    Event(SequencedChatEvent),
    Lagged {
        conversation_id: String,
        skipped: u64,
    },
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
        bus: NatsChatBus::connect(&config.nats_url, NatsChatBusOptions::default()).await?,
        events: ConversationEventRegistry::default(),
        repo: SurrealChatRepository::connect(
            &config.surreal_url,
            &config.surreal_namespace,
            &config.surreal_database,
            &config.surreal_user,
            &config.surreal_password,
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

async fn run_socket(socket: WebSocket, auth: AuthContext, state: AppState) {
    tracing::info!(user_id = %auth.user_id, tenant_id = %auth.tenant_id, "websocket connected");
    let mut subscriptions = HashMap::<String, ActiveSubscription>::new();
    let mut last_sent_sequences = HashMap::<String, u64>::new();
    let (mut socket_sender, mut socket_receiver) = socket.split();
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Message>(ws_outbound_queue_size());
    let (socket_event_tx, mut socket_event_rx) =
        mpsc::channel::<SocketEvent>(ws_event_queue_size());
    let writer = tokio::spawn(async move {
        while let Some(message) = outbound_rx.recv().await {
            if socket_sender.send(message).await.is_err() {
                break;
            }
        }
    });

    loop {
        tokio::select! {
            inbound = socket_receiver.next() => {
                match inbound {
                    Some(Ok(Message::Text(text))) => {
                        if !handle_client_message(
                            &outbound_tx,
                            &mut subscriptions,
                            &mut last_sent_sequences,
                            &socket_event_tx,
                            &text,
                            &auth,
                            &state,
                        ).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(bytes))) => {
                        if send_ws_message(&outbound_tx, Message::Pong(bytes)).is_err() {
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
            event = socket_event_rx.recv() => {
                let event = match event {
                    Some(SocketEvent::Event(event)) => event,
                    Some(SocketEvent::Lagged { conversation_id, skipped }) => {
                        tracing::warn!(
                            skipped,
                            user_id = %auth.user_id,
                            tenant_id = %auth.tenant_id,
                            conversation_id,
                            "websocket conversation event receiver lagged; requesting resync"
                        );
                        if send_server_message(
                            &outbound_tx,
                            &ServerWsMessage::ResyncRequired {
                                conversation_id: conversation_id.clone(),
                            },
                        )
                        .is_err()
                        {
                            break;
                        }
                        last_sent_sequences.remove(&conversation_id);
                        continue;
                    }
                    None => break,
                };
                if event.envelope.tenant_id != auth.tenant_id {
                    continue;
                }
                if !subscriptions.contains_key(&event.envelope.conversation_id) {
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
                if send_server_message(&outbound_tx, &msg).is_err() {
                    tracing::warn!(
                        user_id = %auth.user_id,
                        conversation_id = %event.envelope.conversation_id,
                        "websocket outbound queue full or closed; disconnecting slow client"
                    );
                    break;
                }
                last_sent_sequences.insert(msg_conversation_id(&msg), sequence);
            }
        }
    }

    for (_, subscription) in subscriptions {
        subscription.task.abort();
    }
    drop(outbound_tx);
    writer.abort();
    let _ = writer.await;
    tracing::info!(user_id = %auth.user_id, "websocket disconnected");
}

async fn handle_client_message(
    outbound: &mpsc::Sender<Message>,
    subscriptions: &mut HashMap<String, ActiveSubscription>,
    last_sent_sequences: &mut HashMap<String, u64>,
    socket_events: &mpsc::Sender<SocketEvent>,
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
                    return send_server_message(
                        outbound,
                        &ServerWsMessage::Error {
                            code: "not_found".to_owned(),
                            message: "conversation not found".to_owned(),
                        },
                    )
                    .is_ok();
                }
            };

            if !subscriptions.contains_key(&conversation_id) {
                let subscription = match subscribe_socket_to_conversation(
                    state,
                    auth,
                    &conversation_id,
                    socket_events,
                )
                .await
                {
                    Ok(subscription) => subscription,
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            conversation_id,
                            "failed to subscribe websocket to conversation events"
                        );
                        return send_server_message(
                            outbound,
                            &ServerWsMessage::Error {
                                code: "subscribe_failed".to_owned(),
                                message: "failed to subscribe to conversation events".to_owned(),
                            },
                        )
                        .is_ok();
                    }
                };
                subscriptions.insert(conversation_id.clone(), subscription);
            }

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
                    return send_server_message(
                        outbound,
                        &ServerWsMessage::ResyncRequired { conversation_id },
                    )
                    .is_ok();
                }
                Err(ReplayDecisionError::Internal(error)) => {
                    tracing::warn!(%error, conversation_id, "failed to prepare websocket replay");
                    return send_server_message(
                        outbound,
                        &ServerWsMessage::Error {
                            code: "replay_failed".to_owned(),
                            message: "failed to prepare websocket replay".to_owned(),
                        },
                    )
                    .is_ok();
                }
            };

            if send_server_message(
                outbound,
                &ServerWsMessage::Subscribed {
                    conversation_id: conversation_id.clone(),
                },
            )
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
                if send_server_message(outbound, &msg).is_err() {
                    return false;
                }
                last_sent = sequence;
            }
            last_sent_sequences.insert(conversation_id, last_sent);
            true
        }
        Ok(ClientWsMessage::UnsubscribeConversation { conversation_id }) => {
            if let Some(subscription) = subscriptions.remove(&conversation_id) {
                subscription.task.abort();
            }
            last_sent_sequences.remove(&conversation_id);
            true
        }
        Ok(ClientWsMessage::Ping { nonce }) => {
            send_server_message(outbound, &ServerWsMessage::Pong { nonce }).is_ok()
        }
        Err(_) => send_server_message(
            outbound,
            &ServerWsMessage::Error {
                code: "invalid_message".to_owned(),
                message: "invalid websocket message".to_owned(),
            },
        )
        .is_ok(),
    }
}

async fn subscribe_socket_to_conversation(
    state: &AppState,
    auth: &AuthContext,
    conversation_id: &str,
    socket_events: &mpsc::Sender<SocketEvent>,
) -> Result<ActiveSubscription, delphi_nats::ChatBusError> {
    let subscription = state
        .events
        .subscribe(&state.bus, &auth.tenant_id, conversation_id)
        .await?;
    let ConversationEventSubscription { hub, mut receiver } = subscription;
    let sender = socket_events.clone();
    let conversation_id = conversation_id.to_owned();
    let task = tokio::spawn(async move {
        let _hub = hub;
        loop {
            match receiver.recv().await {
                Ok(event) => {
                    if sender.send(SocketEvent::Event(event)).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    if sender
                        .send(SocketEvent::Lagged {
                            conversation_id: conversation_id.clone(),
                            skipped,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
    Ok(ActiveSubscription { task })
}

#[derive(Debug)]
struct ReplayPlan {
    events: Vec<delphi_nats::SequencedChatEvent>,
    last_seen_sequence: u64,
}

#[derive(Debug)]
struct ReplayRange {
    start_seq: u64,
    end_seq: u64,
    last_seen_sequence: u64,
    required_start_seq: Option<u64>,
    required_end_seq: Option<u64>,
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

    let Some(range) = replay_range(&index, last_event_id, conversation, latest_sequence)? else {
        return Ok(ReplayPlan {
            events: Vec::new(),
            last_seen_sequence: last_event_id.and_then(parse_event_id).unwrap_or(0),
        });
    };

    let events = state
        .bus
        .replay_events(
            &auth.tenant_id,
            conversation_id,
            range.start_seq,
            range.end_seq,
        )
        .await
        .map_err(|error| ReplayDecisionError::Internal(error.to_string()))?;

    if replay_is_incomplete(&events, &range) {
        return Err(ReplayDecisionError::ResyncRequired);
    }

    Ok(ReplayPlan {
        events,
        last_seen_sequence: range.last_seen_sequence,
    })
}

fn replay_range(
    index: &ReplayIndex,
    last_event_id: Option<&str>,
    conversation: &ConversationDetail,
    latest_sequence: u64,
) -> Result<Option<ReplayRange>, ReplayDecisionError> {
    let Some(last_event_id) = last_event_id else {
        let Some(current) = index.current_turn.as_ref() else {
            return Ok(None);
        };
        if current.end_seq.is_some() || conversation_has_turn(conversation, &current.turn_id) {
            return Ok(None);
        }
        let end_seq = current.end_seq.unwrap_or(latest_sequence);
        return Ok(Some(ReplayRange {
            start_seq: current.start_seq,
            end_seq,
            last_seen_sequence: 0,
            required_start_seq: Some(current.start_seq),
            required_end_seq: current.end_seq,
        }));
    };

    let last_sequence = parse_event_id(last_event_id).ok_or(ReplayDecisionError::ResyncRequired)?;
    if let Some(current) = index.current_turn.as_ref() {
        if turn_contains_sequence(current, last_sequence, latest_sequence) {
            let end_seq = current.end_seq.unwrap_or(latest_sequence);
            if last_sequence >= end_seq {
                return Ok(None);
            }
            return Ok(Some(ReplayRange {
                start_seq: last_sequence + 1,
                end_seq,
                last_seen_sequence: last_sequence,
                required_start_seq: None,
                required_end_seq: current.end_seq,
            }));
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
            return Ok(Some(ReplayRange {
                start_seq: last_sequence + 1,
                end_seq,
                last_seen_sequence: last_sequence,
                required_start_seq: None,
                required_end_seq: match index.current_turn.as_ref() {
                    Some(current) => current.end_seq,
                    None => previous.end_seq,
                },
            }));
        }
    }

    Err(ReplayDecisionError::ResyncRequired)
}

fn replay_is_incomplete(events: &[delphi_nats::SequencedChatEvent], range: &ReplayRange) -> bool {
    if range.required_start_seq.is_some() && events.is_empty() {
        return true;
    }

    if let Some(required_start_seq) = range.required_start_seq {
        let first_sequence = events
            .first()
            .and_then(|event| parse_event_id(&event.event_id));
        if first_sequence != Some(required_start_seq) {
            return true;
        }
    }

    if let Some(required_end_seq) = range.required_end_seq {
        let last_sequence = events
            .last()
            .and_then(|event| parse_event_id(&event.event_id));
        if last_sequence != Some(required_end_seq) {
            return true;
        }
    }

    false
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

fn send_server_message(
    outbound: &mpsc::Sender<Message>,
    msg: &ServerWsMessage,
) -> Result<(), mpsc::error::TrySendError<Message>> {
    let text = serde_json::to_string(msg).expect("server websocket message serializes");
    send_ws_message(outbound, Message::Text(text.into()))
}

fn send_ws_message(
    outbound: &mpsc::Sender<Message>,
    message: Message,
) -> Result<(), mpsc::error::TrySendError<Message>> {
    outbound.try_send(message)
}

fn ws_outbound_queue_size() -> usize {
    std::env::var("REALTIME_WS_OUTBOUND_QUEUE_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_WS_OUTBOUND_QUEUE_SIZE)
}

fn ws_event_queue_size() -> usize {
    std::env::var("REALTIME_WS_EVENT_QUEUE_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_WS_EVENT_QUEUE_SIZE)
}

fn conversation_event_buffer_size() -> usize {
    std::env::var("REALTIME_CONVERSATION_EVENT_BUFFER_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CONVERSATION_EVENT_BUFFER_SIZE)
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
