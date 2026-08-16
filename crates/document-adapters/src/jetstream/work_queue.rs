use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::{pull, AckPolicy, PullConsumer};
use async_nats::jetstream::message::Message as JsMessage;
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::AckKind;
use async_nats::jetstream::Context;
use async_trait::async_trait;
use delphi_document_app::{QueueError, UploadCompleted, WorkQueue};
use futures::StreamExt;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use super::DOCUMENT_WORK_STREAM;
use crate::error::AdapterError;

pub const UPLOAD_COMPLETED_SUBJECT: &str = "document_work.v1.upload_completed";
pub const DOCUMENT_WORK_CONSUMER: &str = "document-worker-upload-completed";

#[derive(Clone)]
pub struct JetStreamWorkQueue {
    js: Context,
}

impl JetStreamWorkQueue {
    pub fn new(js: Context) -> Self {
        Self { js }
    }
}

#[async_trait]
impl WorkQueue for JetStreamWorkQueue {
    async fn publish_upload_completed(&self, cmd: UploadCompleted) -> Result<(), QueueError> {
        let payload = serde_json::to_vec(&cmd)
            .map_err(|error| QueueError::Payload(format!("encode work item: {error}")))?;
        let message = PublishMessage::build()
            // Derived from the upload id alone: a second `/complete` with a
            // different parts list is deduped and ignored. First list wins.
            .message_id(&cmd.command_id)
            .payload(payload.into());

        // Await the ack: `/complete` must not return 202 for work that was
        // never durably enqueued.
        self.js
            .send_publish(UPLOAD_COMPLETED_SUBJECT, message)
            .await
            .map_err(|error| QueueError::Unavailable(format!("publish work item: {error}")))?
            .await
            .map_err(|error| QueueError::Unavailable(format!("await work item ack: {error}")))?;

        Ok(())
    }
}

/// One in-flight work item, with its delivery deadline kept alive.
pub struct WorkItem {
    message: Arc<JsMessage>,
    heartbeat: Option<JoinHandle<()>>,
    pub command: UploadCompleted,
    pub num_delivered: u64,
    /// The last delivery this consumer will make. A transient failure here has
    /// to become a terminal answer, because there is no next attempt.
    pub is_final_delivery: bool,
}

impl WorkItem {
    /// Only after the event is durable. **Ack after append, never before.**
    pub async fn ack(mut self) {
        self.stop_heartbeat();
        if let Err(error) = self.message.ack().await {
            tracing::warn!(%error, command_id = %self.command.command_id, "ack failed; the item will be redelivered");
        }
    }

    /// Transient failure: let redelivery retry.
    pub async fn nak(mut self, delay: Duration) {
        self.stop_heartbeat();
        if let Err(error) = self
            .message
            .ack_with(AckKind::Nak(Some(delay)))
            .await
        {
            tracing::warn!(%error, command_id = %self.command.command_id, "nak failed");
        }
    }

    /// Poison: never redeliver. The work stream's `max_age` is what eventually
    /// clears a termed message.
    pub async fn term(mut self) {
        self.stop_heartbeat();
        if let Err(error) = self.message.ack_with(AckKind::Term).await {
            tracing::warn!(%error, command_id = %self.command.command_id, "term failed");
        }
    }

    fn stop_heartbeat(&mut self) {
        if let Some(handle) = self.heartbeat.take() {
            handle.abort();
        }
    }
}

impl Drop for WorkItem {
    fn drop(&mut self) {
        // If the item is dropped without a decision, stop pretending it is
        // still being worked on so redelivery happens promptly.
        self.stop_heartbeat();
    }
}

pub struct WorkQueueConsumer {
    consumer: PullConsumer,
    ack_wait: Duration,
    max_deliver: u32,
    concurrency: usize,
}

impl WorkQueueConsumer {
    pub async fn connect(
        js: &Context,
        ack_wait: Duration,
        max_deliver: u32,
        max_ack_pending: usize,
        concurrency: usize,
    ) -> Result<Self, AdapterError> {
        let stream = js.get_stream(DOCUMENT_WORK_STREAM).await.map_err(|error| {
            AdapterError::Topology(format!("open {DOCUMENT_WORK_STREAM}: {error}"))
        })?;
        // `create_consumer`, not `get_or_create_consumer`: the latter hands back
        // whatever is already there, so a changed `ack_wait` or `max_deliver`
        // would apply only on a cluster that had never run before.
        let consumer: PullConsumer = stream
            .create_consumer(pull::Config {
                durable_name: Some(DOCUMENT_WORK_CONSUMER.to_owned()),
                filter_subject: UPLOAD_COMPLETED_SUBJECT.to_owned(),
                ack_policy: AckPolicy::Explicit,
                ack_wait,
                // MUST be finite. The last delivery is the only thing that ever
                // converts a stuck upload into a rejection, and rejection is
                // the only thing that deletes its bytes.
                max_deliver: i64::from(max_deliver),
                max_ack_pending: max_ack_pending as i64,
                ..Default::default()
            })
            .await
            .map_err(|error| {
                AdapterError::Topology(format!("create {DOCUMENT_WORK_CONSUMER}: {error}"))
            })?;

        Ok(Self {
            consumer,
            ack_wait,
            max_deliver,
            concurrency: concurrency.max(1),
        })
    }

    /// Handle work items until the connection ends. Undecodable payloads are
    /// termed in place — redelivering them cannot make them parse.
    ///
    /// Items run **concurrently**, up to `concurrency`. Finishing an upload is
    /// dominated by waiting on storage — assemble, head, sniff, then stream the
    /// whole object past a scanner — so handling them one at a time made a
    /// single large file block every small one behind it on this instance, no
    /// matter how much `max_ack_pending` JetStream was willing to give us.
    /// The semaphore is acquired *before* the next message is pulled, so
    /// backpressure reaches the server instead of piling up unacked work here.
    pub async fn run<F, Fut>(&self, handle: F) -> Result<(), AdapterError>
    where
        F: Fn(WorkItem) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut messages = self
            .consumer
            .stream()
            .max_messages_per_batch(16)
            .messages()
            .await
            .map_err(|error| AdapterError::Topology(format!("open work stream: {error}")))?;

        let limiter = Arc::new(Semaphore::new(self.concurrency));
        let handle = Arc::new(handle);

        while let Some(message) = messages.next().await {
            let message = match message {
                Ok(message) => message,
                Err(error) => {
                    tracing::warn!(%error, "work queue receive failed");
                    continue;
                }
            };

            let num_delivered = match message.info() {
                Ok(info) => info.delivered.max(0) as u64,
                Err(error) => {
                    tracing::warn!(%error, "work item missing jetstream metadata; terminating it");
                    let _ = message.ack_with(AckKind::Term).await;
                    continue;
                }
            };

            let command: UploadCompleted = match serde_json::from_slice(&message.payload) {
                Ok(command) => command,
                Err(error) => {
                    tracing::error!(%error, "undecodable work item; terminating it");
                    let _ = message.ack_with(AckKind::Term).await;
                    continue;
                }
            };

            let message = Arc::new(message);
            let item = WorkItem {
                heartbeat: Some(spawn_heartbeat(message.clone(), self.ack_wait)),
                message,
                num_delivered,
                is_final_delivery: num_delivered >= u64::from(self.max_deliver),
                command,
            };

            // Held for the whole handler, released when the spawned task ends.
            // `Semaphore::close` is never called, so this cannot fail.
            let Ok(permit) = limiter.clone().acquire_owned().await else {
                return Ok(());
            };
            let handle = handle.clone();
            tokio::spawn(async move {
                handle(item).await;
                drop(permit);
            });
        }

        Ok(())
    }
}

/// `AckKind::Progress` extends the deadline by one `ack_wait` **from when it is
/// sent**, so a single call before a multi-minute scan buys `ack_wait`, not the
/// duration of the scan. Getting this wrong silently produces concurrent
/// duplicate deliveries of the same work.
fn spawn_heartbeat(message: Arc<JsMessage>, ack_wait: Duration) -> JoinHandle<()> {
    let interval = (ack_wait / 2).max(Duration::from_secs(1));
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            if let Err(error) = message.ack_with(AckKind::Progress).await {
                tracing::warn!(%error, "work item heartbeat failed; the item may be redelivered");
                return;
            }
        }
    })
}
