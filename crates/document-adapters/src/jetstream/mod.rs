//! NATS JetStream topology and adapters.

mod upload_state_store;
mod event_store;
mod work_queue;

pub use upload_state_store::KvUploadStateStore;
pub use event_store::{JetStreamEventStore, DOCUMENT_EVENTS_FILTER};
pub use work_queue::{
    JetStreamWorkQueue, WorkItem, WorkQueueConsumer, DOCUMENT_WORK_CONSUMER,
    UPLOAD_COMPLETED_SUBJECT,
};

use std::time::Duration;

use async_nats::jetstream::stream::{Config as StreamConfig, DiscardPolicy, RetentionPolicy};
use async_nats::jetstream::{kv, stream::StorageType, Context};

use crate::error::AdapterError;

pub const DOCUMENT_EVENTS_STREAM: &str = "DOCUMENT_EVENTS";
pub const DOCUMENT_WORK_STREAM: &str = "DOCUMENT_WORK";
pub const UPLOAD_STATE_BUCKET: &str = "UPLOAD_STATE";

/// Must exceed the maximum redelivery span:
///
/// ```text
/// duplicate_window > (max_deliver × ack_wait) + max_pipeline_duration
/// ```
///
/// With `max_deliver: 5` and `ack_wait: 120s` the floor is 10 minutes *plus*
/// however long a scan can legitimately run under progress heartbeats. Two
/// hours gives real margin; a 10 minute window would leave replace-mode appends
/// unprotected outside it.
pub const DUPLICATE_WINDOW: Duration = Duration::from_secs(2 * 3600);

/// How many subject partitions the event stream is split across.
///
/// **Permanent contract.** Changing it — or the hash — forces a full rebuild,
/// because every document's subject would move.
pub const EVENT_PARTITIONS: u32 = 16;

/// `documents.<tenant_id>.<partition>.<document_id>`
///
/// The hash is pinned to CRC32 on purpose. `DefaultHasher` is explicitly not
/// stable across builds, so using it would silently repartition the stream on a
/// compiler upgrade.
pub fn event_subject(tenant_id: &str, document_id: &str) -> String {
    let partition = crc32fast::hash(document_id.as_bytes()) % EVENT_PARTITIONS;
    format!("documents.{tenant_id}.{partition:02}.{document_id}")
}

/// Bind to the upload-state bucket that `api-service` declared.
///
/// Deliberately cannot create one. A service that could would need the upload
/// TTL to say what it was creating, and every service holding that number is
/// exactly how the bucket ended up with whichever value restarted last.
pub async fn bind_upload_state(js: &Context) -> Result<kv::Store, AdapterError> {
    js.get_key_value(UPLOAD_STATE_BUCKET).await.map_err(|error| {
        AdapterError::Topology(format!(
            "open {UPLOAD_STATE_BUCKET}: {error}. api-service declares the \
             document topology and must start first; everything else binds to \
             it and never declares it."
        ))
    })
}

/// Create both streams and the KV bucket, and reconcile an existing one.
///
/// **Called by `api-service` only** — it is the topology's single author.
///
/// `create_or_update_*`, never `get_or_create_*`. The latter returns the
/// existing object untouched, so a changed `duplicate_window`, `max_age`, or
/// `max_deliver` here would apply on a fresh deployment and be silently ignored
/// on every environment that already ran — the worst kind of drift, because the
/// code says one thing and the running system does another. The subject list
/// and retention policy cannot be updated in place by JetStream; changing
/// either still needs a rebuild.
pub async fn ensure_topology(js: &Context, upload_ttl: Duration) -> Result<kv::Store, AdapterError> {
    js.create_or_update_stream(StreamConfig {
        name: DOCUMENT_EVENTS_STREAM.to_owned(),
        description: Some("append-only document event log; the source of truth".to_owned()),
        subjects: vec!["documents.>".to_owned()],
        retention: RetentionPolicy::Limits,
        // Infinite. Acking never deletes; the log is replayable forever.
        max_age: Duration::ZERO,
        // If a size limit is ever added, refuse new writes rather than silently
        // dropping history.
        discard: DiscardPolicy::New,
        storage: StorageType::File,
        // Required for `direct_get_last_for_subject`.
        allow_direct: true,
        duplicate_window: DUPLICATE_WINDOW,
        ..Default::default()
    })
    .await
    .map_err(|error| AdapterError::Topology(format!("create {DOCUMENT_EVENTS_STREAM}: {error}")))?;

    // The work subject root must NOT be `documents.`: a four-token
    // `documents.work.v1.upload_completed` would be matched by both
    // `documents.>` and `documents.*.*.*`, so JetStream refuses to create the
    // second stream (overlapping subjects, err 10065) — and the projection
    // loop's `documents.>` filter would try to fold work commands as events.
    js.create_or_update_stream(StreamConfig {
        name: DOCUMENT_WORK_STREAM.to_owned(),
        description: Some("document pipeline work items; deleted on ack".to_owned()),
        subjects: vec!["document_work.>".to_owned()],
        retention: RetentionPolicy::WorkQueue,
        // Infinite, and that is safe because **`max_deliver` bounds behaviour
        // while `max_age` only bounds storage**. After its last delivery an
        // item is never handed out again, so it cannot come back later and
        // reclaim something; keeping it costs bytes, not correctness.
        //
        // A finite value here used to be justified by "a Termed message would
        // otherwise live forever". That is simply untrue on this server: TERM
        // removes a message from a WorkQueue stream, same as ACK.
        //
        // What an infinite age does keep is the one item that exhausted
        // `max_deliver` without ever being acked — which only happens if the
        // process died mid-handler on every delivery. That is a poison item
        // worth finding, not garbage worth collecting, and it is invisible to
        // consumers either way (`num_pending` does not count it).
        max_age: Duration::ZERO,
        storage: StorageType::File,
        duplicate_window: DUPLICATE_WINDOW,
        ..Default::default()
    })
    .await
    .map_err(|error| AdapterError::Topology(format!("create {DOCUMENT_WORK_STREAM}: {error}")))?;

    // The upload's entire lifetime, and its only cleanup mechanism. Nothing
    // sweeps this bucket; `max_age` is the retention policy.
    //
    // The same value drives the object-storage reaper, and neither has to go
    // first: whichever expires first, the other side cleans up after it. That
    // is why this is one number rather than an ordered pair.
    //
    // `max_age` rather than a per-entry `Nats-TTL`, which the server supports:
    // `update()` sends no TTL header, so a compare-and-swap would not inherit
    // one and the record would stop expiring the moment its status changed.
    // A per-entry TTL would also have to be known by the worker, which CASes
    // the terminal status — putting this number in two services instead of one.
    //
    // `history: 1` is deliberate. The record is compare-and-swapped on its
    // revision, which needs no history — and keeping old revisions of an
    // upload's status would only make the bucket grow for no reader.
    js.create_or_update_key_value(kv::Config {
        bucket: UPLOAD_STATE_BUCKET.to_owned(),
        description: "the whole upload, from preflight to its terminal answer".to_owned(),
        history: 1,
        max_age: upload_ttl,
        storage: StorageType::File,
        ..Default::default()
    })
    .await
    .map_err(|error| AdapterError::Topology(format!("create {UPLOAD_STATE_BUCKET}: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subjects_have_four_tokens_and_a_padded_partition() {
        let subject = event_subject("acme", "01JZ8QK9");
        let tokens: Vec<_> = subject.split('.').collect();
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0], "documents");
        assert_eq!(tokens[1], "acme");
        assert_eq!(tokens[2].len(), 2, "partition must be zero-padded");
        assert_eq!(tokens[3], "01JZ8QK9");
    }

    #[test]
    fn the_partition_is_stable_and_derived_from_the_document_id_alone() {
        // Pinned values: a change here means every existing subject moved.
        assert_eq!(event_subject("acme", "doc-1"), "documents.acme.09.doc-1");
        assert_eq!(event_subject("other", "doc-1"), "documents.other.09.doc-1");
        assert_eq!(event_subject("acme", "doc-2"), "documents.acme.03.doc-2");
    }

    #[test]
    fn every_partition_stays_inside_the_configured_count() {
        for index in 0..1000 {
            let subject = event_subject("acme", &format!("doc-{index}"));
            let partition: u32 = subject.split('.').nth(2).expect("partition").parse().unwrap();
            assert!(partition < EVENT_PARTITIONS);
        }
    }

    #[test]
    fn the_work_subject_does_not_collide_with_the_event_subject_space() {
        assert!(!UPLOAD_COMPLETED_SUBJECT.starts_with("documents."));
        let event_tokens = event_subject("acme", "doc-1").split('.').count();
        let work_tokens = UPLOAD_COMPLETED_SUBJECT.split('.').count();
        // Even if the roots ever converged, differing token counts alone would
        // not save us from `documents.>`, so assert the root explicitly above.
        assert_eq!(event_tokens, 4);
        assert_eq!(work_tokens, 3);
    }
}
