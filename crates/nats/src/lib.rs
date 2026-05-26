use async_nats::jetstream::consumer::{self, AckPolicy, DeliverPolicy, PullConsumer, PushConsumer};
use async_nats::jetstream::kv::UpdateErrorKind;
use async_nats::jetstream::kv::{self, Operation, Store};
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::stream::{Config as StreamConfig, RetentionPolicy, StorageType};
use async_nats::jetstream::AckKind;
use async_nats::jetstream::{self, Context};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use delphi_contracts::{ChatEvent, ChatEventEnvelope, TurnRequested};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{broadcast, Mutex};

const CHAT_COMMANDS_STREAM: &str = "CHAT_COMMANDS";
const CHAT_EVENTS_STREAM: &str = "CHAT_EVENTS";
const CHAT_LOCKS_BUCKET: &str = "CHAT_LOCKS";
const CHAT_REPLAY_BUCKET: &str = "CHAT_REPLAY";
const CHAT_WORKER_CONSUMER: &str = "chat-worker-turns";
const DEFAULT_CHAT_LOCK_TTL_SECONDS: u64 = 15 * 60;
const DEFAULT_CHAT_EVENTS_MAX_AGE_SECONDS: u64 = 30 * 60;
const DEFAULT_CHAT_REPLAY_TTL_SECONDS: u64 = 25 * 60;
const DEFAULT_CHAT_COMMAND_ACK_WAIT_SECONDS: u64 = 120;

#[derive(Debug, Error)]
pub enum ChatBusError {
    #[error("chat bus unavailable")]
    Unavailable,
    #[error("chat bus payload error: {0}")]
    Payload(String),
    #[error("chat conversation already has an in-flight turn")]
    InFlight,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatLock {
    pub tenant_id: String,
    pub conversation_id: String,
    pub turn_id: String,
    pub state: ChatLockState,
    pub worker_id: Option<String>,
    pub stop_requested: bool,
    pub stop_requested_by: Option<String>,
    pub stop_requested_at: Option<DateTime<Utc>>,
    pub lease_expires_at: DateTime<Utc>,
}

impl ChatLock {
    pub fn requested(tenant_id: String, conversation_id: String, turn_id: String) -> Self {
        Self {
            tenant_id,
            conversation_id,
            turn_id,
            state: ChatLockState::Requested,
            worker_id: None,
            stop_requested: false,
            stop_requested_by: None,
            stop_requested_at: None,
            lease_expires_at: lock_expires_at(),
        }
    }

    fn is_expired(&self) -> bool {
        self.lease_expires_at <= Utc::now()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChatLockState {
    Requested,
    Running,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StopSignal {
    pub tenant_id: String,
    pub conversation_id: String,
    pub turn_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ReplayIndex {
    pub previous_turn: Option<ReplayTurn>,
    pub current_turn: Option<ReplayTurn>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayTurn {
    pub turn_id: String,
    pub start_seq: u64,
    pub end_seq: Option<u64>,
}

#[async_trait]
pub trait ChatBus: Clone + Send + Sync + 'static {
    async fn acquire_lock(&self, lock: ChatLock) -> Result<(), ChatBusError>;
    async fn claim_lock(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        turn_id: &str,
        worker_id: &str,
    ) -> Result<ChatLock, ChatBusError>;
    async fn request_stop(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        requested_by: &str,
    ) -> Result<Option<ChatLock>, ChatBusError>;
    async fn stop_requested(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        turn_id: &str,
    ) -> Result<bool, ChatBusError>;
    async fn renew_lock(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        turn_id: &str,
        worker_id: &str,
    ) -> Result<ChatLock, ChatBusError>;
    async fn release_lock(&self, tenant_id: &str, conversation_id: &str, turn_id: &str);
    async fn publish_turn_requested(&self, command: TurnRequested) -> Result<(), ChatBusError>;
    async fn progress_turn_requested(&self, turn_id: &str) -> Result<(), ChatBusError>;
    async fn ack_turn_requested(&self, turn_id: &str) -> Result<(), ChatBusError>;
    async fn publish_event(&self, event: ChatEventEnvelope) -> Result<String, ChatBusError>;
    async fn load_replay_index(
        &self,
        tenant_id: &str,
        conversation_id: &str,
    ) -> Result<Option<ReplayIndex>, ChatBusError>;
    async fn replay_events(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        start_seq: u64,
        end_seq: u64,
    ) -> Result<Vec<SequencedChatEvent>, ChatBusError>;
    async fn latest_event_sequence(&self) -> Result<u64, ChatBusError>;
    async fn publish_stop(
        &self,
        worker_id: &str,
        tenant_id: &str,
        conversation_id: &str,
        turn_id: &str,
    ) -> Result<(), ChatBusError>;
    fn subscribe_events(&self) -> broadcast::Receiver<SequencedChatEvent>;
    fn subscribe_commands(&self) -> broadcast::Receiver<TurnRequested>;
    fn subscribe_stops(&self) -> broadcast::Receiver<StopSignal>;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SequencedChatEvent {
    pub event_id: String,
    pub envelope: ChatEventEnvelope,
}

#[derive(Debug, Clone)]
pub struct ChatSubjects;

impl ChatSubjects {
    pub fn command_turn_requested() -> &'static str {
        "chat.commands.turn_requested"
    }

    pub fn events(tenant_id: &str, conversation_id: &str) -> String {
        format!("chat.events.{tenant_id}.{conversation_id}")
    }

    pub fn control(worker_id: &str) -> String {
        format!("chat.control.worker.{worker_id}.stop")
    }

    pub fn lock_key(tenant_id: &str, conversation_id: &str) -> String {
        format!("{tenant_id}/{conversation_id}")
    }
}

#[derive(Debug, Clone, Default)]
pub struct NatsChatBusOptions {
    pub subscribe_commands: bool,
    pub subscribe_events: bool,
    pub stop_worker_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NatsChatBus {
    client: async_nats::Client,
    js: Context,
    locks: Store,
    replay: Store,
    inner: Arc<Inner>,
}

impl NatsChatBus {
    pub async fn connect(url: &str, options: NatsChatBusOptions) -> Result<Self, ChatBusError> {
        let client = async_nats::connect(url)
            .await
            .map_err(|error| ChatBusError::Payload(format!("connect to NATS: {error}")))?;
        let js = jetstream::new(client.clone());
        configure_jetstream(&js).await?;
        let locks = js
            .create_or_update_key_value(kv::Config {
                bucket: CHAT_LOCKS_BUCKET.to_owned(),
                description: "delphi chat conversation leases".to_owned(),
                history: 1,
                max_age: chat_lock_ttl(),
                storage: StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|error| ChatBusError::Payload(format!("create chat lock bucket: {error}")))?;
        let replay = js
            .create_or_update_key_value(kv::Config {
                bucket: CHAT_REPLAY_BUCKET.to_owned(),
                description: "delphi chat realtime replay indexes".to_owned(),
                history: 1,
                max_age: chat_replay_ttl(),
                storage: StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|error| {
                ChatBusError::Payload(format!("create chat replay bucket: {error}"))
            })?;
        let bus = Self {
            client,
            js,
            locks,
            replay,
            inner: Arc::new(Inner::default()),
        };
        bus.start_subscribers(options).await?;
        Ok(bus)
    }

    async fn start_subscribers(&self, options: NatsChatBusOptions) -> Result<(), ChatBusError> {
        if options.subscribe_commands {
            let stream = self
                .js
                .get_or_create_stream(command_stream_config())
                .await
                .map_err(|error| ChatBusError::Payload(format!("open command stream: {error}")))?;
            let consumer: PullConsumer = stream
                .get_or_create_consumer(
                    CHAT_WORKER_CONSUMER,
                    consumer::pull::Config {
                        durable_name: Some(CHAT_WORKER_CONSUMER.to_owned()),
                        filter_subject: ChatSubjects::command_turn_requested().to_owned(),
                        ack_policy: AckPolicy::Explicit,
                        ack_wait: chat_command_ack_wait(),
                        max_ack_pending: 64,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|error| {
                    ChatBusError::Payload(format!("create command consumer: {error}"))
                })?;
            let sender = self.inner.commands.clone();
            let pending_acks = self.inner.pending_acks.clone();
            tokio::spawn(async move {
                let mut messages = match consumer
                    .stream()
                    .max_messages_per_batch(16)
                    .messages()
                    .await
                {
                    Ok(messages) => messages,
                    Err(error) => {
                        tracing::error!(%error, "chat command consumer failed to start");
                        return;
                    }
                };
                while let Some(message) = messages.next().await {
                    let Ok(message) = message else {
                        tracing::warn!(?message, "chat command consumer receive failed");
                        continue;
                    };
                    match serde_json::from_slice::<TurnRequested>(&message.payload) {
                        Ok(command) => {
                            pending_acks
                                .lock()
                                .await
                                .insert(command.turn_id.clone(), message);
                            let _ = sender.send(command);
                        }
                        Err(error) => {
                            tracing::warn!(%error, "invalid chat command payload");
                            let _ = message.ack().await;
                        }
                    }
                }
            });
        }

        if options.subscribe_events {
            let inbox = self.client.new_inbox();
            let stream = self
                .js
                .get_or_create_stream(event_stream_config())
                .await
                .map_err(|error| ChatBusError::Payload(format!("open event stream: {error}")))?;
            let consumer: PushConsumer = stream
                .create_consumer(consumer::push::Config {
                    deliver_subject: inbox,
                    deliver_policy: DeliverPolicy::New,
                    ack_policy: AckPolicy::None,
                    inactive_threshold: Duration::from_secs(60),
                    filter_subject: "chat.events.>".to_owned(),
                    ..Default::default()
                })
                .await
                .map_err(|error| {
                    ChatBusError::Payload(format!("create event consumer: {error}"))
                })?;
            let mut messages = consumer
                .messages()
                .await
                .map_err(|error| ChatBusError::Payload(format!("subscribe events: {error}")))?;
            let sender = self.inner.events.clone();
            tokio::spawn(async move {
                while let Some(message) = messages.next().await {
                    let Ok(message) = message else {
                        tracing::warn!(?message, "chat event consumer receive failed");
                        continue;
                    };
                    let sequence = match message.info() {
                        Ok(info) => info.stream_sequence,
                        Err(error) => {
                            tracing::warn!(%error, "chat event missing jetstream metadata");
                            continue;
                        }
                    };
                    match serde_json::from_slice::<SequencedChatEvent>(&message.payload) {
                        Ok(mut event) => {
                            event.event_id = sequence.to_string();
                            let _ = sender.send(event);
                        }
                        Err(error) => tracing::warn!(%error, "invalid chat event payload"),
                    };
                }
            });
        }

        if let Some(worker_id) = options.stop_worker_id {
            let mut subscriber = self
                .client
                .subscribe(ChatSubjects::control(&worker_id))
                .await
                .map_err(|error| ChatBusError::Payload(format!("subscribe stops: {error}")))?;
            let sender = self.inner.stops.clone();
            tokio::spawn(async move {
                while let Some(message) = subscriber.next().await {
                    match serde_json::from_slice::<StopSignal>(&message.payload) {
                        Ok(signal) => {
                            let _ = sender.send(signal);
                        }
                        Err(error) => tracing::warn!(%error, "invalid chat stop payload"),
                    }
                }
            });
        }

        Ok(())
    }
}

#[async_trait]
impl ChatBus for NatsChatBus {
    async fn acquire_lock(&self, lock: ChatLock) -> Result<(), ChatBusError> {
        acquire_kv_lock(&self.locks, lock).await
    }

    async fn claim_lock(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        turn_id: &str,
        worker_id: &str,
    ) -> Result<ChatLock, ChatBusError> {
        claim_kv_lock(&self.locks, tenant_id, conversation_id, turn_id, worker_id).await
    }

    async fn request_stop(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        requested_by: &str,
    ) -> Result<Option<ChatLock>, ChatBusError> {
        request_kv_stop(&self.locks, tenant_id, conversation_id, requested_by).await
    }

    async fn stop_requested(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        turn_id: &str,
    ) -> Result<bool, ChatBusError> {
        stop_requested_in_kv(&self.locks, tenant_id, conversation_id, turn_id).await
    }

    async fn renew_lock(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        turn_id: &str,
        worker_id: &str,
    ) -> Result<ChatLock, ChatBusError> {
        renew_kv_lock(&self.locks, tenant_id, conversation_id, turn_id, worker_id).await
    }

    async fn release_lock(&self, tenant_id: &str, conversation_id: &str, turn_id: &str) {
        let key = ChatSubjects::lock_key(tenant_id, conversation_id);
        let entry = match self.locks.entry(key.clone()).await {
            Ok(Some(entry)) => entry,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(%error, %key, "failed to read chat lock for release");
                return;
            }
        };
        if entry.operation != Operation::Put {
            return;
        }
        let stored = match serde_json::from_slice::<ChatLock>(&entry.value) {
            Ok(stored) => stored,
            Err(error) => {
                tracing::warn!(%error, %key, "invalid chat lock payload during release");
                return;
            }
        };
        if stored.turn_id != turn_id {
            return;
        }
        if let Err(error) = self
            .locks
            .purge_expect_revision(key.clone(), Some(entry.revision))
            .await
        {
            tracing::warn!(%error, %key, "failed to release chat lock");
        }
    }

    async fn publish_turn_requested(&self, command: TurnRequested) -> Result<(), ChatBusError> {
        let payload = serde_json::to_vec(&command)
            .map_err(|error| ChatBusError::Payload(error.to_string()))?;
        let ack = self
            .js
            .send_publish(
                ChatSubjects::command_turn_requested().to_owned(),
                PublishMessage::build()
                    .payload(payload.into())
                    .message_id(&command.turn_id)
                    .expected_stream(CHAT_COMMANDS_STREAM),
            )
            .await
            .map_err(|error| ChatBusError::Payload(format!("publish command: {error}")))?;
        ack.await
            .map_err(|error| ChatBusError::Payload(format!("ack command publish: {error}")))?;
        Ok(())
    }

    async fn ack_turn_requested(&self, turn_id: &str) -> Result<(), ChatBusError> {
        let message = self.inner.pending_acks.lock().await.remove(turn_id);
        if let Some(message) = message {
            message
                .ack()
                .await
                .map_err(|error| ChatBusError::Payload(format!("ack command: {error}")))?;
        }
        Ok(())
    }

    async fn progress_turn_requested(&self, turn_id: &str) -> Result<(), ChatBusError> {
        let pending_acks = self.inner.pending_acks.lock().await;
        if let Some(message) = pending_acks.get(turn_id) {
            message
                .ack_with(AckKind::Progress)
                .await
                .map_err(|error| ChatBusError::Payload(format!("progress ack command: {error}")))?;
        }
        Ok(())
    }

    async fn publish_event(&self, event: ChatEventEnvelope) -> Result<String, ChatBusError> {
        let subject = ChatSubjects::events(&event.tenant_id, &event.conversation_id);
        let event_kind = event.event.clone();
        let sequenced = SequencedChatEvent {
            event_id: String::new(),
            envelope: event,
        };
        let payload = serde_json::to_vec(&sequenced)
            .map_err(|error| ChatBusError::Payload(error.to_string()))?;
        let ack = self
            .js
            .send_publish(
                subject,
                PublishMessage::build()
                    .payload(payload.into())
                    .expected_stream(CHAT_EVENTS_STREAM),
            )
            .await
            .map_err(|error| ChatBusError::Payload(format!("publish event: {error}")))?;
        let ack = ack
            .await
            .map_err(|error| ChatBusError::Payload(format!("ack event publish: {error}")))?;
        update_replay_for_event(&self.replay, &sequenced.envelope, ack.sequence, event_kind)
            .await?;
        Ok(ack.sequence.to_string())
    }

    async fn load_replay_index(
        &self,
        tenant_id: &str,
        conversation_id: &str,
    ) -> Result<Option<ReplayIndex>, ChatBusError> {
        load_replay_index_from_kv(&self.replay, tenant_id, conversation_id).await
    }

    async fn replay_events(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        start_seq: u64,
        end_seq: u64,
    ) -> Result<Vec<SequencedChatEvent>, ChatBusError> {
        if start_seq == 0 || end_seq < start_seq {
            return Ok(Vec::new());
        }
        let stream = self
            .js
            .get_or_create_stream(event_stream_config())
            .await
            .map_err(|error| ChatBusError::Payload(format!("open event stream: {error}")))?;
        let subject = ChatSubjects::events(tenant_id, conversation_id);
        let mut events = Vec::new();
        for sequence in start_seq..=end_seq {
            let message = stream.get_raw_message(sequence).await;
            let Ok(message) = message else {
                continue;
            };
            if message.subject.as_str() != subject {
                continue;
            }
            let mut event = serde_json::from_slice::<SequencedChatEvent>(&message.payload)
                .map_err(|error| {
                    ChatBusError::Payload(format!("decode event {sequence}: {error}"))
                })?;
            if event.envelope.tenant_id != tenant_id
                || event.envelope.conversation_id != conversation_id
            {
                continue;
            }
            event.event_id = sequence.to_string();
            events.push(event);
        }
        Ok(events)
    }

    async fn latest_event_sequence(&self) -> Result<u64, ChatBusError> {
        let mut stream = self
            .js
            .get_or_create_stream(event_stream_config())
            .await
            .map_err(|error| ChatBusError::Payload(format!("open event stream: {error}")))?;
        let info = stream
            .info()
            .await
            .map_err(|error| ChatBusError::Payload(format!("read event stream info: {error}")))?;
        Ok(info.state.last_sequence)
    }

    async fn publish_stop(
        &self,
        worker_id: &str,
        tenant_id: &str,
        conversation_id: &str,
        turn_id: &str,
    ) -> Result<(), ChatBusError> {
        let subject = ChatSubjects::control(worker_id);
        let payload = serde_json::to_vec(&StopSignal {
            tenant_id: tenant_id.to_owned(),
            conversation_id: conversation_id.to_owned(),
            turn_id: turn_id.to_owned(),
        })
        .map_err(|error| ChatBusError::Payload(error.to_string()))?;
        self.client
            .publish(subject, payload.into())
            .await
            .map_err(|error| ChatBusError::Payload(format!("publish stop: {error}")))?;
        Ok(())
    }

    fn subscribe_events(&self) -> broadcast::Receiver<SequencedChatEvent> {
        self.inner.events.subscribe()
    }

    fn subscribe_commands(&self) -> broadcast::Receiver<TurnRequested> {
        self.inner.commands.subscribe()
    }

    fn subscribe_stops(&self) -> broadcast::Receiver<StopSignal> {
        self.inner.stops.subscribe()
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryChatBus {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    commands: broadcast::Sender<TurnRequested>,
    events: broadcast::Sender<SequencedChatEvent>,
    stops: broadcast::Sender<StopSignal>,
    locks: Mutex<HashMap<String, ChatLock>>,
    replay: Mutex<HashMap<String, ReplayIndex>>,
    event_log: Mutex<Vec<SequencedChatEvent>>,
    next_event_sequence: Mutex<u64>,
    pending_acks: Arc<Mutex<HashMap<String, async_nats::jetstream::Message>>>,
}

impl Default for Inner {
    fn default() -> Self {
        let (commands, _) = broadcast::channel(256);
        let (events, _) = broadcast::channel(1024);
        let (stops, _) = broadcast::channel(256);
        Self {
            commands,
            events,
            stops,
            locks: Mutex::new(HashMap::new()),
            replay: Mutex::new(HashMap::new()),
            event_log: Mutex::new(Vec::new()),
            next_event_sequence: Mutex::new(0),
            pending_acks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryChatBus {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
        }
    }
}

#[async_trait]
impl ChatBus for InMemoryChatBus {
    async fn acquire_lock(&self, lock: ChatLock) -> Result<(), ChatBusError> {
        let key = ChatSubjects::lock_key(&lock.tenant_id, &lock.conversation_id);
        let mut locks = self.inner.locks.lock().await;
        if locks
            .get(&key)
            .is_some_and(|existing| !existing.is_expired())
        {
            return Err(ChatBusError::InFlight);
        }
        locks.insert(key, lock);
        Ok(())
    }

    async fn claim_lock(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        turn_id: &str,
        worker_id: &str,
    ) -> Result<ChatLock, ChatBusError> {
        let key = ChatSubjects::lock_key(tenant_id, conversation_id);
        let mut locks = self.inner.locks.lock().await;
        match locks.get_mut(&key) {
            Some(existing) if existing.turn_id == turn_id => {
                if existing.state == ChatLockState::Running
                    && existing.worker_id.as_deref() != Some(worker_id)
                    && !existing.is_expired()
                {
                    return Err(ChatBusError::InFlight);
                }
                existing.state = ChatLockState::Running;
                existing.worker_id = Some(worker_id.to_owned());
                existing.lease_expires_at = lock_expires_at();
                Ok(existing.clone())
            }
            Some(existing) if !existing.is_expired() => Err(ChatBusError::InFlight),
            Some(_) | None => {
                let mut lock = ChatLock::requested(
                    tenant_id.to_owned(),
                    conversation_id.to_owned(),
                    turn_id.to_owned(),
                );
                lock.state = ChatLockState::Running;
                lock.worker_id = Some(worker_id.to_owned());
                locks.insert(key, lock.clone());
                Ok(lock)
            }
        }
    }

    async fn request_stop(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        requested_by: &str,
    ) -> Result<Option<ChatLock>, ChatBusError> {
        let key = ChatSubjects::lock_key(tenant_id, conversation_id);
        let mut locks = self.inner.locks.lock().await;
        match locks.get_mut(&key) {
            Some(lock) if !lock.is_expired() => {
                lock.stop_requested = true;
                lock.stop_requested_by = Some(requested_by.to_owned());
                lock.stop_requested_at = Some(Utc::now());
                Ok(Some(lock.clone()))
            }
            Some(_) => {
                locks.remove(&key);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn stop_requested(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        turn_id: &str,
    ) -> Result<bool, ChatBusError> {
        let key = ChatSubjects::lock_key(tenant_id, conversation_id);
        let locks = self.inner.locks.lock().await;
        Ok(locks.get(&key).is_some_and(|lock| {
            !lock.is_expired() && lock.turn_id == turn_id && lock.stop_requested
        }))
    }

    async fn renew_lock(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        turn_id: &str,
        worker_id: &str,
    ) -> Result<ChatLock, ChatBusError> {
        let key = ChatSubjects::lock_key(tenant_id, conversation_id);
        let mut locks = self.inner.locks.lock().await;
        let Some(lock) = locks.get_mut(&key) else {
            return Err(ChatBusError::InFlight);
        };
        if lock.turn_id != turn_id
            || lock.worker_id.as_deref() != Some(worker_id)
            || lock.is_expired()
        {
            return Err(ChatBusError::InFlight);
        }
        lock.lease_expires_at = lock_expires_at();
        Ok(lock.clone())
    }

    async fn release_lock(&self, tenant_id: &str, conversation_id: &str, turn_id: &str) {
        let key = ChatSubjects::lock_key(tenant_id, conversation_id);
        let mut locks = self.inner.locks.lock().await;
        if locks.get(&key).is_some_and(|lock| lock.turn_id == turn_id) {
            locks.remove(&key);
        }
    }

    async fn publish_turn_requested(&self, command: TurnRequested) -> Result<(), ChatBusError> {
        let _ = self.inner.commands.send(command);
        Ok(())
    }

    async fn ack_turn_requested(&self, _turn_id: &str) -> Result<(), ChatBusError> {
        Ok(())
    }

    async fn progress_turn_requested(&self, _turn_id: &str) -> Result<(), ChatBusError> {
        Ok(())
    }

    async fn publish_event(&self, event: ChatEventEnvelope) -> Result<String, ChatBusError> {
        let mut next_sequence = self.inner.next_event_sequence.lock().await;
        *next_sequence += 1;
        let sequence = *next_sequence;
        drop(next_sequence);
        let event_kind = event.event.clone();
        let sequenced = SequencedChatEvent {
            event_id: sequence.to_string(),
            envelope: event,
        };
        update_in_memory_replay(
            &self.inner.replay,
            &sequenced.envelope,
            sequence,
            event_kind,
        )
        .await;
        self.inner.event_log.lock().await.push(sequenced.clone());
        let _ = self.inner.events.send(sequenced);
        Ok(sequence.to_string())
    }

    async fn load_replay_index(
        &self,
        tenant_id: &str,
        conversation_id: &str,
    ) -> Result<Option<ReplayIndex>, ChatBusError> {
        let key = ChatSubjects::lock_key(tenant_id, conversation_id);
        Ok(self.inner.replay.lock().await.get(&key).cloned())
    }

    async fn replay_events(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        start_seq: u64,
        end_seq: u64,
    ) -> Result<Vec<SequencedChatEvent>, ChatBusError> {
        let events = self.inner.event_log.lock().await;
        Ok(events
            .iter()
            .filter(|event| {
                event
                    .event_id
                    .parse::<u64>()
                    .is_ok_and(|sequence| sequence >= start_seq && sequence <= end_seq)
                    && event.envelope.tenant_id == tenant_id
                    && event.envelope.conversation_id == conversation_id
            })
            .cloned()
            .collect())
    }

    async fn latest_event_sequence(&self) -> Result<u64, ChatBusError> {
        Ok(*self.inner.next_event_sequence.lock().await)
    }

    async fn publish_stop(
        &self,
        _worker_id: &str,
        tenant_id: &str,
        conversation_id: &str,
        turn_id: &str,
    ) -> Result<(), ChatBusError> {
        let _ = self.inner.stops.send(StopSignal {
            tenant_id: tenant_id.to_owned(),
            conversation_id: conversation_id.to_owned(),
            turn_id: turn_id.to_owned(),
        });
        Ok(())
    }

    fn subscribe_events(&self) -> broadcast::Receiver<SequencedChatEvent> {
        self.inner.events.subscribe()
    }

    fn subscribe_commands(&self) -> broadcast::Receiver<TurnRequested> {
        self.inner.commands.subscribe()
    }

    fn subscribe_stops(&self) -> broadcast::Receiver<StopSignal> {
        self.inner.stops.subscribe()
    }
}

async fn acquire_kv_lock(locks: &Store, lock: ChatLock) -> Result<(), ChatBusError> {
    let key = ChatSubjects::lock_key(&lock.tenant_id, &lock.conversation_id);
    if let Some(entry) = locks
        .entry(key.clone())
        .await
        .map_err(|error| ChatBusError::Payload(format!("read chat lock: {error}")))?
    {
        if entry.operation != Operation::Put {
            return create_kv_lock(locks, key, lock).await;
        }
        let stored = serde_json::from_slice::<ChatLock>(&entry.value)
            .map_err(|error| ChatBusError::Payload(format!("decode chat lock: {error}")))?;
        if !stored.is_expired() {
            return Err(ChatBusError::InFlight);
        }
        locks
            .purge_expect_revision(key.clone(), Some(entry.revision))
            .await
            .map_err(|error| ChatBusError::Payload(format!("purge expired chat lock: {error}")))?;
    }

    create_kv_lock(locks, key, lock).await
}

async fn create_kv_lock(locks: &Store, key: String, lock: ChatLock) -> Result<(), ChatBusError> {
    let payload =
        serde_json::to_vec(&lock).map_err(|error| ChatBusError::Payload(error.to_string()))?;
    match locks.create(key, payload.into()).await {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == kv::CreateErrorKind::AlreadyExists => {
            Err(ChatBusError::InFlight)
        }
        Err(error) => Err(ChatBusError::Payload(format!("create chat lock: {error}"))),
    }
}

async fn claim_kv_lock(
    locks: &Store,
    tenant_id: &str,
    conversation_id: &str,
    turn_id: &str,
    worker_id: &str,
) -> Result<ChatLock, ChatBusError> {
    let key = ChatSubjects::lock_key(tenant_id, conversation_id);
    for _ in 0..8 {
        let entry = locks
            .entry(key.clone())
            .await
            .map_err(|error| ChatBusError::Payload(format!("read chat lock: {error}")))?;
        let Some(entry) = entry else {
            let mut lock = ChatLock::requested(
                tenant_id.to_owned(),
                conversation_id.to_owned(),
                turn_id.to_owned(),
            );
            lock.state = ChatLockState::Running;
            lock.worker_id = Some(worker_id.to_owned());
            let payload = serde_json::to_vec(&lock)
                .map_err(|error| ChatBusError::Payload(error.to_string()))?;
            return match locks.create(key.clone(), payload.into()).await {
                Ok(_) => Ok(lock),
                Err(error) if error.kind() == kv::CreateErrorKind::AlreadyExists => continue,
                Err(error) => Err(ChatBusError::Payload(format!("create chat lock: {error}"))),
            };
        };
        if entry.operation != Operation::Put {
            let mut lock = ChatLock::requested(
                tenant_id.to_owned(),
                conversation_id.to_owned(),
                turn_id.to_owned(),
            );
            lock.state = ChatLockState::Running;
            lock.worker_id = Some(worker_id.to_owned());
            let payload = serde_json::to_vec(&lock)
                .map_err(|error| ChatBusError::Payload(error.to_string()))?;
            return match locks.create(key.clone(), payload.into()).await {
                Ok(_) => Ok(lock),
                Err(error) if error.kind() == kv::CreateErrorKind::AlreadyExists => continue,
                Err(error) => Err(ChatBusError::Payload(format!("create chat lock: {error}"))),
            };
        }
        let mut lock = serde_json::from_slice::<ChatLock>(&entry.value)
            .map_err(|error| ChatBusError::Payload(format!("decode chat lock: {error}")))?;
        if lock.turn_id != turn_id && !lock.is_expired() {
            return Err(ChatBusError::InFlight);
        }
        if lock.turn_id == turn_id
            && lock.state == ChatLockState::Running
            && lock.worker_id.as_deref() != Some(worker_id)
            && !lock.is_expired()
        {
            return Err(ChatBusError::InFlight);
        }
        if lock.turn_id != turn_id || lock.is_expired() {
            lock = ChatLock::requested(
                tenant_id.to_owned(),
                conversation_id.to_owned(),
                turn_id.to_owned(),
            );
        }
        lock.state = ChatLockState::Running;
        lock.worker_id = Some(worker_id.to_owned());
        lock.lease_expires_at = lock_expires_at();
        let payload =
            serde_json::to_vec(&lock).map_err(|error| ChatBusError::Payload(error.to_string()))?;
        match locks
            .update(key.clone(), payload.into(), entry.revision)
            .await
        {
            Ok(_) => return Ok(lock),
            Err(error) if error.kind() == UpdateErrorKind::WrongLastRevision => continue,
            Err(error) => return Err(ChatBusError::Payload(format!("claim chat lock: {error}"))),
        }
    }
    Err(ChatBusError::Payload(
        "claim chat lock CAS retry limit exceeded".to_owned(),
    ))
}

async fn request_kv_stop(
    locks: &Store,
    tenant_id: &str,
    conversation_id: &str,
    requested_by: &str,
) -> Result<Option<ChatLock>, ChatBusError> {
    let key = ChatSubjects::lock_key(tenant_id, conversation_id);
    for _ in 0..8 {
        let entry = locks
            .entry(key.clone())
            .await
            .map_err(|error| ChatBusError::Payload(format!("read chat lock: {error}")))?;
        let Some(entry) = entry else {
            return Ok(None);
        };
        if entry.operation != Operation::Put {
            return Ok(None);
        }
        let mut lock = serde_json::from_slice::<ChatLock>(&entry.value)
            .map_err(|error| ChatBusError::Payload(format!("decode chat lock: {error}")))?;
        if lock.is_expired() {
            let _ = locks
                .purge_expect_revision(key.clone(), Some(entry.revision))
                .await;
            return Ok(None);
        }
        lock.stop_requested = true;
        lock.stop_requested_by = Some(requested_by.to_owned());
        lock.stop_requested_at = Some(Utc::now());
        let payload =
            serde_json::to_vec(&lock).map_err(|error| ChatBusError::Payload(error.to_string()))?;
        match locks
            .update(key.clone(), payload.into(), entry.revision)
            .await
        {
            Ok(_) => return Ok(Some(lock)),
            Err(error) if error.kind() == UpdateErrorKind::WrongLastRevision => continue,
            Err(error) => return Err(ChatBusError::Payload(format!("request stop: {error}"))),
        }
    }
    Err(ChatBusError::Payload(
        "request stop CAS retry limit exceeded".to_owned(),
    ))
}

async fn stop_requested_in_kv(
    locks: &Store,
    tenant_id: &str,
    conversation_id: &str,
    turn_id: &str,
) -> Result<bool, ChatBusError> {
    let key = ChatSubjects::lock_key(tenant_id, conversation_id);
    let entry = locks
        .entry(key)
        .await
        .map_err(|error| ChatBusError::Payload(format!("read chat lock: {error}")))?;
    let Some(entry) = entry else {
        return Ok(false);
    };
    if entry.operation != Operation::Put {
        return Ok(false);
    }
    let lock = serde_json::from_slice::<ChatLock>(&entry.value)
        .map_err(|error| ChatBusError::Payload(format!("decode chat lock: {error}")))?;
    Ok(!lock.is_expired() && lock.turn_id == turn_id && lock.stop_requested)
}

async fn renew_kv_lock(
    locks: &Store,
    tenant_id: &str,
    conversation_id: &str,
    turn_id: &str,
    worker_id: &str,
) -> Result<ChatLock, ChatBusError> {
    let key = ChatSubjects::lock_key(tenant_id, conversation_id);
    for _ in 0..8 {
        let entry = locks
            .entry(key.clone())
            .await
            .map_err(|error| ChatBusError::Payload(format!("read chat lock: {error}")))?;
        let Some(entry) = entry else {
            return Err(ChatBusError::InFlight);
        };
        if entry.operation != Operation::Put {
            return Err(ChatBusError::InFlight);
        }
        let mut lock = serde_json::from_slice::<ChatLock>(&entry.value)
            .map_err(|error| ChatBusError::Payload(format!("decode chat lock: {error}")))?;
        if lock.turn_id != turn_id
            || lock.worker_id.as_deref() != Some(worker_id)
            || lock.is_expired()
        {
            return Err(ChatBusError::InFlight);
        }
        lock.lease_expires_at = lock_expires_at();
        let payload =
            serde_json::to_vec(&lock).map_err(|error| ChatBusError::Payload(error.to_string()))?;
        match locks
            .update(key.clone(), payload.into(), entry.revision)
            .await
        {
            Ok(_) => return Ok(lock),
            Err(error) if error.kind() == UpdateErrorKind::WrongLastRevision => continue,
            Err(error) => return Err(ChatBusError::Payload(format!("renew chat lock: {error}"))),
        }
    }
    Err(ChatBusError::Payload(
        "renew chat lock CAS retry limit exceeded".to_owned(),
    ))
}

async fn load_replay_index_from_kv(
    replay: &Store,
    tenant_id: &str,
    conversation_id: &str,
) -> Result<Option<ReplayIndex>, ChatBusError> {
    let key = ChatSubjects::lock_key(tenant_id, conversation_id);
    let entry = replay
        .entry(key)
        .await
        .map_err(|error| ChatBusError::Payload(format!("read chat replay index: {error}")))?;
    let Some(entry) = entry else {
        return Ok(None);
    };
    if entry.operation != Operation::Put {
        return Ok(None);
    }
    let index = serde_json::from_slice::<ReplayIndex>(&entry.value)
        .map_err(|error| ChatBusError::Payload(format!("decode chat replay index: {error}")))?;
    Ok(Some(index))
}

async fn update_replay_for_event(
    replay: &Store,
    envelope: &ChatEventEnvelope,
    sequence: u64,
    event: ChatEvent,
) -> Result<(), ChatBusError> {
    if !is_replay_index_event(&event) {
        return Ok(());
    }

    let key = ChatSubjects::lock_key(&envelope.tenant_id, &envelope.conversation_id);
    for _ in 0..8 {
        let entry = replay
            .entry(key.clone())
            .await
            .map_err(|error| ChatBusError::Payload(format!("read chat replay index: {error}")))?;
        let revision = entry
            .as_ref()
            .filter(|entry| entry.operation == Operation::Put)
            .map(|entry| entry.revision);
        let mut index = match entry
            .as_ref()
            .filter(|entry| entry.operation == Operation::Put)
        {
            Some(entry) => {
                serde_json::from_slice::<ReplayIndex>(&entry.value).map_err(|error| {
                    ChatBusError::Payload(format!("decode chat replay index: {error}"))
                })?
            }
            None => ReplayIndex::default(),
        };

        apply_replay_event(&mut index, envelope, sequence, &event);
        let payload =
            serde_json::to_vec(&index).map_err(|error| ChatBusError::Payload(error.to_string()))?;

        if let Some(revision) = revision {
            match replay.update(key.clone(), payload.into(), revision).await {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == UpdateErrorKind::WrongLastRevision => continue,
                Err(error) => {
                    return Err(ChatBusError::Payload(format!(
                        "update chat replay index: {error}"
                    )));
                }
            }
        } else {
            match replay.create(key.clone(), payload.into()).await {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == kv::CreateErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(ChatBusError::Payload(format!(
                        "create chat replay index: {error}"
                    )));
                }
            }
        }
    }
    Err(ChatBusError::Payload(
        "update chat replay index CAS retry limit exceeded".to_owned(),
    ))
}

async fn update_in_memory_replay(
    replay: &Mutex<HashMap<String, ReplayIndex>>,
    envelope: &ChatEventEnvelope,
    sequence: u64,
    event: ChatEvent,
) {
    if !is_replay_index_event(&event) {
        return;
    }
    let key = ChatSubjects::lock_key(&envelope.tenant_id, &envelope.conversation_id);
    let mut replay = replay.lock().await;
    let index = replay.entry(key).or_default();
    apply_replay_event(index, envelope, sequence, &event);
}

fn apply_replay_event(
    index: &mut ReplayIndex,
    envelope: &ChatEventEnvelope,
    sequence: u64,
    event: &ChatEvent,
) {
    match event {
        ChatEvent::TurnStarted { .. } => {
            if index
                .current_turn
                .as_ref()
                .is_some_and(|turn| turn.turn_id == envelope.turn_id)
            {
                return;
            }
            index.previous_turn = index.current_turn.take();
            index.current_turn = Some(ReplayTurn {
                turn_id: envelope.turn_id.clone(),
                start_seq: sequence,
                end_seq: None,
            });
        }
        ChatEvent::Finish { .. } | ChatEvent::Interrupted { .. } | ChatEvent::Clear { .. } => {
            if let Some(turn) = index
                .current_turn
                .as_mut()
                .filter(|turn| turn.turn_id == envelope.turn_id)
            {
                turn.end_seq = Some(sequence);
            } else if let Some(turn) = index
                .previous_turn
                .as_mut()
                .filter(|turn| turn.turn_id == envelope.turn_id)
            {
                turn.end_seq = Some(sequence);
            }
        }
        _ => {}
    }
}

fn is_replay_index_event(event: &ChatEvent) -> bool {
    matches!(
        event,
        ChatEvent::TurnStarted { .. }
            | ChatEvent::Finish { .. }
            | ChatEvent::Interrupted { .. }
            | ChatEvent::Clear { .. }
    )
}

fn lock_expires_at() -> DateTime<Utc> {
    Utc::now() + ChronoDuration::from_std(chat_lock_ttl()).expect("lock ttl fits chrono")
}

fn chat_lock_ttl() -> Duration {
    duration_from_env("CHAT_LOCK_TTL_SECONDS", DEFAULT_CHAT_LOCK_TTL_SECONDS)
}

fn chat_events_max_age() -> Duration {
    duration_from_env(
        "CHAT_EVENTS_MAX_AGE_SECONDS",
        DEFAULT_CHAT_EVENTS_MAX_AGE_SECONDS,
    )
}

fn chat_replay_ttl() -> Duration {
    duration_from_env("CHAT_REPLAY_TTL_SECONDS", DEFAULT_CHAT_REPLAY_TTL_SECONDS)
}

fn chat_command_ack_wait() -> Duration {
    duration_from_env(
        "CHAT_COMMAND_ACK_WAIT_SECONDS",
        DEFAULT_CHAT_COMMAND_ACK_WAIT_SECONDS,
    )
}

fn duration_from_env(name: &str, default_seconds: u64) -> Duration {
    let seconds = std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .unwrap_or(default_seconds);
    Duration::from_secs(seconds)
}

async fn configure_jetstream(js: &Context) -> Result<(), ChatBusError> {
    js.create_or_update_stream(command_stream_config())
        .await
        .map_err(|error| ChatBusError::Payload(format!("create command stream: {error}")))?;
    js.create_or_update_stream(event_stream_config())
        .await
        .map_err(|error| ChatBusError::Payload(format!("create event stream: {error}")))?;
    Ok(())
}

fn event_stream_config() -> StreamConfig {
    StreamConfig {
        name: CHAT_EVENTS_STREAM.to_owned(),
        subjects: vec!["chat.events.*.*".to_owned()],
        retention: RetentionPolicy::Limits,
        max_age: chat_events_max_age(),
        storage: StorageType::File,
        ..Default::default()
    }
}

fn command_stream_config() -> StreamConfig {
    StreamConfig {
        name: CHAT_COMMANDS_STREAM.to_owned(),
        subjects: vec![ChatSubjects::command_turn_requested().to_owned()],
        retention: RetentionPolicy::WorkQueue,
        duplicate_window: Duration::from_secs(10 * 60),
        storage: StorageType::File,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_stop_before_worker_claim_is_preserved_on_claim() {
        let bus = InMemoryChatBus::default();
        bus.acquire_lock(ChatLock::requested(
            "tenant-a".to_owned(),
            "conv-a".to_owned(),
            "turn-a".to_owned(),
        ))
        .await
        .unwrap();

        let stopped = bus
            .request_stop("tenant-a", "conv-a", "user-a")
            .await
            .unwrap()
            .unwrap();
        assert!(stopped.stop_requested);
        assert_eq!(stopped.worker_id, None);

        let claimed = bus
            .claim_lock("tenant-a", "conv-a", "turn-a", "worker-a")
            .await
            .unwrap();
        assert_eq!(claimed.worker_id.as_deref(), Some("worker-a"));
        assert!(claimed.stop_requested);
        assert!(bus
            .stop_requested("tenant-a", "conv-a", "turn-a")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn in_memory_stop_after_worker_claim_routes_to_worker() {
        let bus = InMemoryChatBus::default();
        bus.acquire_lock(ChatLock::requested(
            "tenant-a".to_owned(),
            "conv-a".to_owned(),
            "turn-a".to_owned(),
        ))
        .await
        .unwrap();
        bus.claim_lock("tenant-a", "conv-a", "turn-a", "worker-a")
            .await
            .unwrap();

        let stopped = bus
            .request_stop("tenant-a", "conv-a", "user-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stopped.worker_id.as_deref(), Some("worker-a"));
        assert!(stopped.stop_requested);
    }

    #[tokio::test]
    async fn in_memory_renew_lock_requires_current_worker_owner() {
        let bus = InMemoryChatBus::default();
        bus.acquire_lock(ChatLock::requested(
            "tenant-a".to_owned(),
            "conv-a".to_owned(),
            "turn-a".to_owned(),
        ))
        .await
        .unwrap();
        bus.claim_lock("tenant-a", "conv-a", "turn-a", "worker-a")
            .await
            .unwrap();

        let renewed = bus
            .renew_lock("tenant-a", "conv-a", "turn-a", "worker-a")
            .await
            .unwrap();
        assert_eq!(renewed.worker_id.as_deref(), Some("worker-a"));

        let wrong_worker = bus
            .renew_lock("tenant-a", "conv-a", "turn-a", "worker-b")
            .await;
        assert!(matches!(wrong_worker, Err(ChatBusError::InFlight)));
    }
}
