use async_nats::error::Error as NatsError;
use async_nats::jetstream::consumer::{pull, DeliverPolicy};
use async_nats::jetstream::context::PublishErrorKind;
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::stream::{DirectGetErrorKind, Stream};
use async_nats::jetstream::Context;
use async_trait::async_trait;
use delphi_document_app::{Appended, EventStore, EventStoreError, Expect};
use delphi_document_domain::DocumentEvent;
use futures::StreamExt;

use super::{event_subject, DOCUMENT_EVENTS_STREAM};

/// What the projection loop subscribes to. Because the work stream lives under
/// a different root, this can never pick up a work command.
pub const DOCUMENT_EVENTS_FILTER: &str = "documents.>";

/// How many events one `read_stream` call will pull before giving up. A
/// document with more history than this has outgrown the "fold the whole
/// stream" strategy and needs snapshots.
const MAX_STREAM_READ: usize = 10_000;

#[derive(Clone)]
pub struct JetStreamEventStore {
    js: Context,
    stream: Stream,
}

impl JetStreamEventStore {
    pub async fn new(js: Context) -> Result<Self, crate::error::AdapterError> {
        let stream = js.get_stream(DOCUMENT_EVENTS_STREAM).await.map_err(|error| {
            crate::error::AdapterError::Topology(format!("open {DOCUMENT_EVENTS_STREAM}: {error}"))
        })?;
        Ok(Self { js, stream })
    }

    pub fn stream(&self) -> &Stream {
        &self.stream
    }
}

#[async_trait]
impl EventStore for JetStreamEventStore {
    async fn append(
        &self,
        event: DocumentEvent,
        expect: Expect,
    ) -> Result<Appended, EventStoreError> {
        let subject = event_subject(&event.tenant_id, &event.document_id);
        let version = event.version;
        let payload = serde_json::to_vec(&event)
            .map_err(|error| EventStoreError::Payload(format!("encode event: {error}")))?;

        let expected = match expect {
            Expect::CreateOnly => 0,
            Expect::Exactly(sequence) => sequence,
        };
        let message = PublishMessage::build()
            // Deterministic, so a redelivery of the same work produces the same
            // id and JetStream deduplicates it.
            .message_id(&event.event_id)
            // Per-*subject*, not global: `Nats-Expected-Last-Sequence` would
            // serialise every document in the system behind one another.
            .expected_last_subject_sequence(expected)
            .payload(payload.into());

        let ack = self
            .js
            .send_publish(subject, message)
            .await
            .map_err(map_publish_error)?
            .await
            .map_err(map_publish_error)?;

        Ok(Appended {
            stream_seq: ack.sequence,
            version,
            duplicate: ack.duplicate,
        })
    }

    async fn last(
        &self,
        tenant: &str,
        document_id: &str,
    ) -> Result<Option<(u64, u64)>, EventStoreError> {
        let subject = event_subject(tenant, document_id);
        match self.stream.direct_get_last_for_subject(subject).await {
            Ok(message) => {
                let event: DocumentEvent = serde_json::from_slice(&message.payload)
                    .map_err(|error| EventStoreError::Payload(format!("decode event: {error}")))?;
                Ok(Some((event.version, message.sequence)))
            }
            Err(error) if matches!(error.kind(), DirectGetErrorKind::NotFound) => Ok(None),
            Err(error) => Err(EventStoreError::Unavailable(format!(
                "direct get last: {error}"
            ))),
        }
    }

    async fn read_stream(
        &self,
        tenant: &str,
        document_id: &str,
    ) -> Result<Vec<(u64, DocumentEvent)>, EventStoreError> {
        let subject = event_subject(tenant, document_id);

        // Ephemeral and ordered: it exists for this read only and is cleaned up
        // by its inactivity threshold.
        let consumer = self
            .stream
            .create_consumer(pull::OrderedConfig {
                filter_subject: subject.clone(),
                deliver_policy: DeliverPolicy::All,
                ..Default::default()
            })
            .await
            .map_err(|error| {
                EventStoreError::Unavailable(format!("create read consumer: {error}"))
            })?;

        let pending = consumer
            .cached_info()
            .num_pending
            .min(MAX_STREAM_READ as u64) as usize;
        if pending == 0 {
            return Ok(Vec::new());
        }

        let mut messages = consumer
            .messages()
            .await
            .map_err(|error| EventStoreError::Unavailable(format!("read stream: {error}")))?;

        let mut events = Vec::with_capacity(pending);
        while events.len() < pending {
            let Some(message) = messages.next().await else {
                break;
            };
            let message = message
                .map_err(|error| EventStoreError::Unavailable(format!("read stream: {error}")))?;
            let sequence = message
                .info()
                .map_err(|error| {
                    EventStoreError::Payload(format!("missing jetstream metadata: {error}"))
                })?
                .stream_sequence;
            let event: DocumentEvent = serde_json::from_slice(&message.payload)
                .map_err(|error| EventStoreError::Payload(format!("decode event: {error}")))?;
            events.push((sequence, event));
        }

        if events.len() == MAX_STREAM_READ {
            tracing::warn!(
                document_id,
                "document history hit the read cap; folding it is no longer cheap"
            );
        }

        Ok(events)
    }
}

fn map_publish_error(error: NatsError<PublishErrorKind>) -> EventStoreError {
    match error.kind() {
        // The expected-sequence check failed: either the document already
        // exists (create) or something landed in our window (update).
        PublishErrorKind::WrongLastSequence | PublishErrorKind::WrongLastMessageId => {
            EventStoreError::Conflict
        }
        _ => EventStoreError::Unavailable(error.to_string()),
    }
}
