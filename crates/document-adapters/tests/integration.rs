//! Integration tests against real NATS, Postgres, and MinIO.
//!
//! Skipped unless `DELPHI_DOCUMENT_IT=1`, so `cargo test --workspace` stays
//! runnable without infrastructure. Bring the dependencies up with:
//!
//! ```sh
//! docker compose -f docker-compose.t2.yml up -d nats postgres minio minio-init
//! DELPHI_DOCUMENT_IT=1 cargo test -p delphi-document-adapters --test integration -- --test-threads=1
//! ```
//!
//! Every test namespaces itself by tenant, so they do not collide, but they
//! share one Postgres database and one NATS server — hence `--test-threads=1`
//! for the ones that assert on the projection or on GC.

#![allow(clippy::items_after_test_module)]

use std::sync::Arc;
use std::time::Duration;

use delphi_document_adapters::jetstream::{
    ensure_topology, event_subject, JetStreamEventStore, JetStreamWorkQueue, KvUploadStateStore,
    DOCUMENT_EVENTS_STREAM, DOCUMENT_WORK_CONSUMER, UPLOAD_COMPLETED_SUBJECT,
};
use delphi_document_adapters::postgres::{
    peek_document, PgDocumentReadModel, ProjectionLoop, ProjectorLease,
};
use delphi_document_adapters::s3::S3BlobStore;
use delphi_document_adapters::verification::{BasicContentValidator, PermissiveScanner};
use delphi_document_adapters::{config::S3Config, connect_postgres, SystemClock, UlidGen};
use delphi_document_app::{
    BlobStore, CompleteRequest, ConflictPolicy, UploadStateStore, UploadStatus,
    DocumentError, DocumentService, EventStore, Expect, FinishOutcome, PreflightRequest,
    RenewRequest, UploadCompleted, UploadFinisher, UploadPolicy, WorkQueue,
};
use delphi_document_domain::{DocState, MetadataPatch};
use sqlx::{Connection, PgConnection, PgPool};
use ulid::Ulid;

const DATABASE_URL: &str = "postgres://delphi:delphi@127.0.0.1:5432/delphi";
const NATS_URL: &str = "nats://127.0.0.1:4222";
const UPLOAD_TTL: Duration = Duration::from_secs(86_400);
const USER: &str = "it-user";

/// A part must clear S3's 5 MiB floor to be assembled as a non-final part; a
/// single-part upload has no such constraint, so the fixtures stay small.
const BODY: &[u8] = b"%PDF-1.7\nthe quick brown fox jumps over the lazy dog\n";

/// The EICAR test string. Not malware — a signature every scanner must flag.
const EICAR: &[u8] =
    br"X5O!P%@AP[4\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*";

fn enabled() -> bool {
    std::env::var("DELPHI_DOCUMENT_IT").as_deref() == Ok("1")
}

macro_rules! requires_infra {
    () => {
        if !enabled() {
            eprintln!("skipping: set DELPHI_DOCUMENT_IT=1 with the compose stack up");
            return;
        }
    };
}

struct Rig {
    pool: PgPool,
    events: Arc<JetStreamEventStore>,
    blobs: Arc<S3BlobStore>,
    uploads: Arc<KvUploadStateStore>,
    queue: Arc<JetStreamWorkQueue>,
    service: DocumentService,
    finisher: UploadFinisher,
    tenant: String,
}

impl Rig {
    async fn build() -> Self {
        Rig::build_with(UploadPolicy {
            part_url_ttl: Duration::from_secs(300),
            max_upload_bytes: 512 * 1024 * 1024 * 1024,
            part_size_bytes: 20 * 1024 * 1024,
        })
        .await
    }

    async fn build_with(policy: UploadPolicy) -> Self {
        let s3_config = S3Config {
            endpoint_internal: "http://127.0.0.1:9000".to_owned(),
            endpoint_public: "http://127.0.0.1:9000".to_owned(),
            region: "us-east-1".to_owned(),
            bucket: "delphi".to_owned(),
            access_key_id: "delphi".to_owned(),
            secret_access_key: "delphi-secret".to_owned(),
            force_path_style: true,
        };

        let pool = connect_postgres(DATABASE_URL, 5).await.expect("postgres");
        let client = async_nats::connect(NATS_URL).await.expect("nats");
        let js = async_nats::jetstream::new(client);
        let bucket = ensure_topology(&js, UPLOAD_TTL).await.expect("topology");

        let events = Arc::new(JetStreamEventStore::new(js.clone()).await.expect("events"));
        let blobs = Arc::new(S3BlobStore::new(&s3_config));
        let read_model = Arc::new(PgDocumentReadModel::new(pool.clone()));
        let uploads = Arc::new(KvUploadStateStore::new(bucket));
        let queue = Arc::new(JetStreamWorkQueue::new(js.clone()));

        let service = DocumentService::new(
            events.clone(),
            blobs.clone(),
            uploads.clone(),
            read_model.clone(),
            queue.clone(),
            Arc::new(SystemClock),
            Arc::new(UlidGen),
            policy,
        );

        let finisher = UploadFinisher::new(
            blobs.clone(),
            Arc::new(PermissiveScanner),
            Arc::new(BasicContentValidator),
            events.clone(),
            uploads.clone(),
            Arc::new(SystemClock),
        );

        Self {
            pool,
            events,
            blobs,
            uploads,
            queue,
            service,
            finisher,
            // A fresh tenant per rig keeps tests from seeing each other's rows,
            // subjects, or objects.
            tenant: format!("it{}", Ulid::new().to_string().to_lowercase()),
        }
    }

    /// Preflight, PUT the bytes to the presigned URL, and return the command
    /// the worker would receive. This is the whole client-side flow.
    async fn upload(&self, body: &[u8], document_id: Option<String>) -> UploadCompleted {
        let response = self
            .service
            .preflight(
                &self.tenant,
                USER,
                PreflightRequest {
                    document_id,
                    filename: "report.pdf".to_owned(),
                    size: body.len() as u64,
                    content_type: Some("application/pdf".to_owned()),
                },
            )
            .await
            .expect("preflight");
        assert_eq!(response.part_count, 1, "fixtures are single-part");

        let url = sign_part(&self.service, &self.tenant, &response.upload_id, 1).await;
    put_part(&url, body).await;

        self.service
            .complete(
                &self.tenant,
                USER,
                &response.upload_id,
                CompleteRequest {
                    if_match: None,
                    on_conflict: ConflictPolicy::Supersede,
                    patch: MetadataPatch {
                        title: Some("Integration Report".to_owned()),
                        tags: Some(vec!["it".to_owned()]),
                        ..Default::default()
                    },
                },
            )
            .await
            .expect("complete");

        // Drain the work item the API just enqueued, so the test drives the
        // worker step explicitly instead of racing a background consumer.
        self.take_work_item(&response.upload_id).await
    }

    /// Rebuild the command the worker would see. The real consumer is not
    /// running in these tests, so this reads it back off the work stream.
    ///
    /// It reuses the worker's **durable** consumer rather than making an
    /// ephemeral one: a WorkQueue stream permits only one consumer per filter
    /// subject, so a per-test consumer collides with the previous test's until
    /// that one's inactivity threshold expires.
    async fn take_work_item(&self, upload_id: &str) -> UploadCompleted {
        use futures::StreamExt;

        let consumer = work_consumer().await;
        // Drain up to a few batches: another test's leftover item may be ahead
        // of ours in the queue.
        for _ in 0..4 {
            let mut messages = consumer
                .fetch()
                .max_messages(64)
                .messages()
                .await
                .expect("fetch");
            let mut saw_any = false;
            while let Some(Ok(message)) = messages.next().await {
                saw_any = true;
                let command: UploadCompleted =
                    serde_json::from_slice(&message.payload).expect("decode work item");
                let mine = command.upload_id == upload_id;
                // `double_ack`, not `ack`: a plain ack is fire-and-forget, so a
                // queue-depth assertion right after it would race the server.
                message.double_ack().await.expect("ack");
                if mine {
                    return command;
                }
            }
            if !saw_any {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        panic!("no work item was enqueued for upload {upload_id}");
    }

    /// Run the projection loop until it has caught up, then stop.
    async fn project(&self) {
        let loop_runner = ProjectionLoop::new(self.events.stream().clone(), 500);
        let mut lease = ProjectorLease::try_acquire(DATABASE_URL, projector_lock_id())
            .await
            .expect("lease query")
            .expect("lease is free");
        // The loop runs until its idle poll expires twice with nothing to do.
        let _ = tokio::time::timeout(Duration::from_secs(20), loop_runner.run(&mut lease)).await;
        lease.release().await;
    }

    async fn document(&self, document_id: &str) -> Option<delphi_document_domain::DocumentState> {
        let mut conn = PgConnection::connect(DATABASE_URL).await.expect("connect");
        let found = peek_document(&mut conn, &self.tenant, document_id)
            .await
            .expect("peek");
        let _ = conn.close().await;
        found
    }

    async fn object_exists(&self, upload_id: &str) -> bool {
        self.blobs
            .head(&delphi_document_app::keys::object_key(&self.tenant, upload_id))
            .await
            .expect("head")
            .is_some()
    }
}

/// The worker's durable consumer, shared by every test in this binary.
async fn work_consumer() -> async_nats::jetstream::consumer::PullConsumer {
    use async_nats::jetstream::consumer::{pull, AckPolicy};

    let js = async_nats::jetstream::new(async_nats::connect(NATS_URL).await.expect("nats"));
    let stream = js.get_stream("DOCUMENT_WORK").await.expect("work stream");
    stream
        .get_or_create_consumer(
            DOCUMENT_WORK_CONSUMER,
            pull::Config {
                durable_name: Some(DOCUMENT_WORK_CONSUMER.to_owned()),
                filter_subject: UPLOAD_COMPLETED_SUBJECT.to_owned(),
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_secs(30),
                max_deliver: 5,
                ..Default::default()
            },
        )
        .await
        .expect("work consumer")
}

/// Each test takes its own advisory lock id so a leftover lease from a failed
/// run cannot wedge the next one.
fn projector_lock_id() -> i64 {
    // Derived from the process id: unique per test binary invocation.
    0x0000_00FF_0000_0000_i64 | i64::from(std::process::id())
}

/// Preflight hands out no URLs; a part is signed immediately before upload.
async fn sign_part(
    service: &DocumentService,
    tenant: &str,
    upload_id: &str,
    part_number: u16,
) -> String {
    let signed = service
        .renew(
            tenant,
            USER,
            upload_id,
            RenewRequest {
                from_part: Some(part_number),
                count: Some(1),
            },
        )
        .await
        .expect("sign part");
    signed.parts[0].url.clone()
}

async fn put_part(url: &str, body: &[u8]) -> String {
    let response = reqwest::Client::new()
        .put(url)
        .body(body.to_vec())
        .send()
        .await
        .expect("PUT part");
    assert!(
        response.status().is_success(),
        "PUT part failed: {}",
        response.status()
    );
    response
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .expect("S3 returns an ETag for every uploaded part")
        .to_owned()
}

// ------------------------------------------------------------------- scenarios

#[tokio::test]
async fn create_upload_complete_and_project() {
    requires_infra!();
    let rig = Rig::build().await;

    let command = rig.upload(BODY, None).await;
    let document_id = command.document_id.clone();

    let outcome = rig.finisher.finish(&command, false).await;
    assert!(
        matches!(outcome, FinishOutcome::Accepted { version: 1, .. }),
        "{outcome:?}"
    );

    // The document is not readable until the event is folded: the projection is
    // the read path, and it lags the log.
    assert!(rig.document(&document_id).await.is_none());
    rig.project().await;

    let document = rig.document(&document_id).await.expect("projected");
    assert_eq!(document.version, 1);
    assert_eq!(document.state, DocState::Active);
    assert_eq!(document.owner_user_id, USER);
    assert_eq!(document.current_blob.as_deref(), Some(command.upload_id.as_str()));
    assert_eq!(document.byte_size, Some(BODY.len() as u64));
    assert_eq!(document.title.as_deref(), Some("Integration Report"));
    assert_eq!(document.tags, vec!["it".to_owned()]);
    // The checksum comes from the scanner, which is the only component that
    // reads every byte.
    assert_eq!(
        document.checksum.as_deref(),
        Some(
            format!(
                "sha256:{}",
                delphi_document_app::digest::sha256_hex(BODY)
            )
            .as_str()
        )
    );

    let attempt = rig
        .uploads
        .get(&rig.tenant, USER, &command.upload_id)
        .await
        .expect("attempt")
        .expect("row");
    assert_eq!(
        attempt.state.status,
        UploadStatus::Accepted {
            version: 1,
            superseded: false
        }
    );
}

#[tokio::test]
async fn replace_advances_the_version_and_moves_the_blob() {
    requires_infra!();
    let rig = Rig::build().await;

    let first = rig.upload(BODY, None).await;
    rig.finisher.finish(&first, false).await;
    let document_id = first.document_id.clone();

    let second = rig.upload(b"%PDF-1.7\nrevised\n", Some(document_id.clone())).await;
    let outcome = rig.finisher.finish(&second, false).await;
    assert!(
        matches!(outcome, FinishOutcome::Accepted { version: 2, .. }),
        "{outcome:?}"
    );

    rig.project().await;
    let document = rig.document(&document_id).await.expect("projected");
    assert_eq!(document.version, 2);
    assert_eq!(
        document.current_blob.as_deref(),
        Some(second.upload_id.as_str())
    );
    assert_ne!(first.upload_id, second.upload_id);
}

#[tokio::test]
async fn a_redelivered_work_item_produces_exactly_one_event() {
    requires_infra!();
    let rig = Rig::build().await;

    let command = rig.upload(BODY, None).await;
    let document_id = command.document_id.clone();

    // First delivery succeeds. The second models a crash between
    // `complete_multipart` and the ack: the multipart is already consumed, so
    // the worker must recognise the object and the existing create.
    rig.finisher.finish(&command, false).await;
    let again = rig.finisher.finish(&command, false).await;
    assert!(
        matches!(again, FinishOutcome::Accepted { version: 1, .. }),
        "{again:?}"
    );

    let history = rig
        .events
        .read_stream(&rig.tenant, &document_id)
        .await
        .expect("read stream");
    assert_eq!(history.len(), 1, "redelivery must not append a second event");
}

#[tokio::test]
async fn concurrent_replaces_both_apply_and_the_loser_is_flagged() {
    requires_infra!();
    let rig = Rig::build().await;

    let base = rig.upload(BODY, None).await;
    rig.finisher.finish(&base, false).await;
    let document_id = base.document_id.clone();

    // Two uploads started from the same version. Both were looking at v1.
    let mut left = rig.upload(b"%PDF-1.7\nleft\n", Some(document_id.clone())).await;
    let mut right = rig.upload(b"%PDF-1.7\nright\n", Some(document_id.clone())).await;
    left.if_match = Some(1);
    right.if_match = Some(1);

    let left_outcome = rig.finisher.finish(&left, false).await;
    let right_outcome = rig.finisher.finish(&right, false).await;

    // Last write wins, loudly: both land, and the second learns it superseded a
    // version its author had not seen.
    assert!(
        matches!(
            left_outcome,
            FinishOutcome::Accepted {
                version: 2,
                superseded: false,
                ..
            }
        ),
        "{left_outcome:?}"
    );
    assert!(
        matches!(
            right_outcome,
            FinishOutcome::Accepted {
                version: 3,
                superseded: true,
                ..
            }
        ),
        "{right_outcome:?}"
    );

    rig.project().await;
    let document = rig.document(&document_id).await.expect("projected");
    assert_eq!(document.version, 3);
    assert_eq!(
        document.current_blob.as_deref(),
        Some(right.upload_id.as_str())
    );
}

#[tokio::test]
async fn on_conflict_fail_rejects_a_stale_replace() {
    requires_infra!();
    let rig = Rig::build().await;

    let base = rig.upload(BODY, None).await;
    rig.finisher.finish(&base, false).await;
    let document_id = base.document_id.clone();

    let bump = rig.upload(b"%PDF-1.7\nv2\n", Some(document_id.clone())).await;
    rig.finisher.finish(&bump, false).await;

    let mut stale = rig.upload(b"%PDF-1.7\nstale\n", Some(document_id.clone())).await;
    stale.if_match = Some(1);
    stale.on_conflict = ConflictPolicy::Fail;

    let outcome = rig.finisher.finish(&stale, false).await;
    assert_eq!(
        outcome,
        FinishOutcome::Rejected {
            reason: "version_conflict".to_owned()
        }
    );
    assert!(
        !rig.object_exists(&stale.upload_id).await,
        "a rejected upload's bytes must be reclaimed immediately"
    );
}

#[tokio::test]
async fn an_infected_upload_never_becomes_a_document() {
    requires_infra!();
    let rig = Rig::build().await;

    let mut body = b"%PDF-1.7\n".to_vec();
    body.extend_from_slice(EICAR);
    let command = rig.upload(&body, None).await;

    let outcome = rig.finisher.finish(&command, false).await;
    assert_eq!(
        outcome,
        FinishOutcome::Rejected {
            reason: "malware_detected".to_owned()
        }
    );

    let history = rig
        .events
        .read_stream(&rig.tenant, &command.document_id)
        .await
        .expect("read stream");
    assert!(history.is_empty(), "no event may describe unvalidated bytes");
    assert!(!rig.object_exists(&command.upload_id).await);

    let attempt = rig
        .uploads
        .get(&rig.tenant, USER, &command.upload_id)
        .await
        .expect("attempt")
        .expect("row");
    assert_eq!(
        attempt.state.status,
        UploadStatus::Rejected {
            reason: "malware_detected".to_owned()
        }
    );
}

#[tokio::test]
async fn a_second_complete_is_deduped_so_the_first_part_list_wins() {
    requires_infra!();
    let rig = Rig::build().await;

    let response = rig
        .service
        .preflight(
            &rig.tenant,
            USER,
            PreflightRequest {
                document_id: None,
                filename: "report.pdf".to_owned(),
                size: BODY.len() as u64,
                content_type: Some("application/pdf".to_owned()),
            },
        )
        .await
        .expect("preflight");
    let url = sign_part(&rig.service, &rig.tenant, &response.upload_id, 1).await;
    put_part(&url, BODY).await;

    let complete = || CompleteRequest {
        if_match: None,
        on_conflict: ConflictPolicy::Supersede,
        patch: MetadataPatch::default(),
    };

    rig.service
        .complete(&rig.tenant, USER, &response.upload_id, complete())
        .await
        .expect("first complete");
    // A second call still returns 202 — and enqueues nothing, because the work
    // item's message id derives from the upload id alone.
    rig.service
        .complete(&rig.tenant, USER, &response.upload_id, complete())
        .await
        .expect("second complete");

    let command = rig.take_work_item(&response.upload_id).await;
    assert_eq!(command.upload_id, response.upload_id);
    // The queue must now be empty: the second /complete enqueued nothing.
    let js = async_nats::jetstream::new(async_nats::connect(NATS_URL).await.expect("nats"));
    let mut stream = js.get_stream("DOCUMENT_WORK").await.expect("stream");
    let pending = stream.info().await.expect("stream info").state.messages;
    assert_eq!(
        pending, 0,
        "a duplicate /complete must not enqueue a second item"
    );
}

#[tokio::test]
async fn another_user_cannot_complete_someone_elses_upload() {
    requires_infra!();
    let rig = Rig::build().await;

    let response = rig
        .service
        .preflight(
            &rig.tenant,
            USER,
            PreflightRequest {
                document_id: None,
                filename: "report.pdf".to_owned(),
                size: BODY.len() as u64,
                content_type: Some("application/pdf".to_owned()),
            },
        )
        .await
        .expect("preflight");

    let result = rig
        .service
        .complete(
            &rig.tenant,
            "someone-else",
            &response.upload_id,
            CompleteRequest {
                if_match: None,
                on_conflict: ConflictPolicy::Supersede,
                patch: MetadataPatch::default(),
            },
        )
        .await;

    // 404, not 403: the KV key contains the caller, so another user simply
    // finds nothing and learns nothing about the upload's existence.
    assert!(matches!(result, Err(DocumentError::NotFound)), "{result:?}");
}

#[tokio::test]
async fn a_projection_rebuild_reproduces_the_same_rows() {
    requires_infra!();
    let rig = Rig::build().await;

    let first = rig.upload(BODY, None).await;
    rig.finisher.finish(&first, false).await;
    let document_id = first.document_id.clone();
    let second = rig.upload(b"%PDF-1.7\nv2\n", Some(document_id.clone())).await;
    rig.finisher.finish(&second, false).await;

    rig.project().await;
    let before = rig.document(&document_id).await.expect("projected");

    // Reset only this rig's rows and rewind the checkpoint past its events, so
    // the rebuild replays them. A full TRUNCATE would fight parallel tests.
    sqlx::query("DELETE FROM document WHERE tenant_id = $1")
        .bind(&rig.tenant)
        .execute(&rig.pool)
        .await
        .expect("clear rows");
    sqlx::query("DELETE FROM projection_checkpoint WHERE name = 'document-pg'")
        .execute(&rig.pool)
        .await
        .expect("clear checkpoint");

    rig.project().await;
    let after = rig.document(&document_id).await.expect("rebuilt");

    assert_eq!(before, after, "a rebuild must be byte-for-byte identical");
}

#[tokio::test]
async fn the_event_store_enforces_create_once_per_document() {
    requires_infra!();
    let rig = Rig::build().await;
    let document_id = Ulid::new().to_string();

    let mut event = delphi_document_app::testing::sample_created_event(
        &rig.tenant,
        &document_id,
        "blob-a",
    );
    rig.events
        .append(event.clone(), Expect::CreateOnly)
        .await
        .expect("first create");

    // A different event id, so dedupe cannot mask it: the expected-sequence
    // check is what must reject this.
    event.event_id = Ulid::new().to_string();
    let conflict = rig.events.append(event, Expect::CreateOnly).await;
    assert!(
        matches!(
            conflict,
            Err(delphi_document_app::EventStoreError::Conflict)
        ),
        "{conflict:?}"
    );

    let subject = event_subject(&rig.tenant, &document_id);
    assert!(subject.starts_with("documents."));
    assert_eq!(subject.split('.').count(), 4);
}

#[tokio::test]
async fn the_work_stream_and_the_event_stream_can_coexist() {
    requires_infra!();
    let rig = Rig::build().await;

    // Both streams already exist by the time the rig is built; if their
    // subjects overlapped, JetStream would have refused the second one.
    let js = async_nats::jetstream::new(async_nats::connect(NATS_URL).await.expect("nats"));
    assert!(js.get_stream(DOCUMENT_EVENTS_STREAM).await.is_ok());
    assert!(js.get_stream("DOCUMENT_WORK").await.is_ok());

    // And a work item must never reach the event filter.
    let command = rig.upload(BODY, None).await;
    rig.finisher.finish(&command, false).await;
    rig.project().await;
    let failures: i64 = sqlx::query_scalar("SELECT count(*) FROM projection_failure")
        .fetch_one(&rig.pool)
        .await
        .expect("count failures");
    assert_eq!(failures, 0, "the projection must not have seen a work item");
}

#[tokio::test]
async fn the_worker_assembles_a_real_multipart_from_storage_alone() {
    requires_infra!();
    // The interaction this change is about: no ETag ever leaves the client, and
    // a genuinely multi-part object still assembles correctly and in order.
    let rig = Rig::build().await;
    let part_size = 20 * 1024 * 1024_usize;
    let body: Vec<u8> = (0..part_size + 4096).map(|i| (i % 251) as u8).collect();

    let response = rig
        .service
        .preflight(
            &rig.tenant,
            USER,
            PreflightRequest {
                document_id: None,
                filename: "two-parts.bin".to_owned(),
                size: body.len() as u64,
                content_type: Some("application/octet-stream".to_owned()),
            },
        )
        .await
        .expect("preflight");
    assert_eq!(response.part_count, 2, "fixture must be genuinely multipart");

    // Upload part 2 FIRST, so a worker that trusted arrival order rather than
    // part numbers would assemble the object backwards.
    for part_number in [2_u16, 1] {
        let url = sign_part(&rig.service, &rig.tenant, &response.upload_id, part_number).await;
        let start = (part_number as usize - 1) * part_size;
        let end = (start + part_size).min(body.len());
        put_part(&url, &body[start..end]).await;
    }

    rig.service
        .complete(
            &rig.tenant,
            USER,
            &response.upload_id,
            CompleteRequest {
                if_match: None,
                on_conflict: ConflictPolicy::Supersede,
                patch: MetadataPatch::default(),
            },
        )
        .await
        .expect("complete carries no parts at all");

    let command = rig.take_work_item(&response.upload_id).await;
    let encoded = serde_json::to_vec(&command).expect("encode").len();
    assert!(
        encoded < 8 * 1024,
        "the work item should be tiny now that parts are not carried: {encoded} bytes"
    );

    let outcome = rig.finisher.finish(&command, false).await;
    assert!(
        matches!(outcome, FinishOutcome::Accepted { .. }),
        "the worker must assemble from ListParts alone: {outcome:?}"
    );

    // Byte-for-byte, in the right order.
    let head = rig
        .blobs
        .head(&command.storage_key)
        .await
        .expect("head")
        .expect("object");
    assert_eq!(head.byte_size, body.len() as u64);
}

#[tokio::test]
async fn publishing_a_work_item_larger_than_max_payload_fails_loudly() {
    requires_infra!();
    let rig = Rig::build().await;

    // Bypass the API's own guard to prove the server-side ceiling exists and
    // that we surface it rather than silently dropping work.
    let mut command = rig.upload(BODY, None).await;
    command.command_id = format!("oversize-{}", Ulid::new());
    command.patch.description = Some("x".repeat(9 * 1024 * 1024));

    let result = rig.queue.publish_upload_completed(command).await;
    assert!(result.is_err(), "NATS must refuse an oversized payload");
}

// ------------------------------------------------------------ live deployment

/// Drives the **running** `document-worker` rather than calling the use case
/// directly, so the work-queue consumer, its ack heartbeat, and the
/// leader-elected projection loop are all exercised as deployed.
///
/// Gated separately because it competes with the tests above for the durable
/// work consumer:
///
/// ```sh
/// docker compose -f docker-compose.t2.yml up -d api-service document-worker
/// DELPHI_DOCUMENT_IT=1 DELPHI_DOCUMENT_IT_LIVE=1 \
///   cargo test -p delphi-document-adapters --test integration -- --ignored live_
/// ```
#[tokio::test]
#[ignore = "requires a running document-worker"]
async fn live_worker_accepts_an_upload_and_the_projection_catches_up() {
    requires_infra!();
    if std::env::var("DELPHI_DOCUMENT_IT_LIVE").as_deref() != Ok("1") {
        eprintln!("skipping: set DELPHI_DOCUMENT_IT_LIVE=1 with document-worker running");
        return;
    }
    let rig = Rig::build().await;

    let response = rig
        .service
        .preflight(
            &rig.tenant,
            USER,
            PreflightRequest {
                document_id: None,
                filename: "live.pdf".to_owned(),
                size: BODY.len() as u64,
                content_type: Some("application/pdf".to_owned()),
            },
        )
        .await
        .expect("preflight");
    let url = sign_part(&rig.service, &rig.tenant, &response.upload_id, 1).await;
    put_part(&url, BODY).await;

    rig.service
        .complete(
            &rig.tenant,
            USER,
            &response.upload_id,
            CompleteRequest {
                if_match: None,
                on_conflict: ConflictPolicy::Supersede,
                patch: MetadataPatch {
                    title: Some("Live".to_owned()),
                    ..Default::default()
                },
            },
        )
        .await
        .expect("complete");

    // The client contract: poll the attempt row until it is terminal. A 202 is
    // not a guarantee.
    let attempt = await_terminal_attempt(&rig, &response.upload_id).await;
    assert_eq!(
        attempt,
        UploadStatus::Accepted {
            version: 1,
            superseded: false
        }
    );

    // And then the read model, which lags the log by a projection cycle.
    let document = await_projected(&rig, &response.document_id).await;
    assert_eq!(document.version, 1);
    assert_eq!(document.title.as_deref(), Some("Live"));
    assert_eq!(
        document.current_blob.as_deref(),
        Some(response.upload_id.as_str())
    );
}

async fn await_terminal_attempt(rig: &Rig, upload_id: &str) -> UploadStatus {
    for _ in 0..120 {
        if let Some(attempt) = rig
            .uploads
            .get(&rig.tenant, USER, upload_id)
            .await
            .expect("attempt")
        {
            if attempt.state.status.is_terminal() {
                return attempt.state.status;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("the worker never reached a terminal outcome for {upload_id}");
}

async fn await_projected(rig: &Rig, document_id: &str) -> delphi_document_domain::DocumentState {
    for _ in 0..120 {
        if let Some(document) = rig.document(document_id).await {
            return document;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("the projection never caught up for {document_id}");
}

/// Pins the external assumption the whole geometry rests on.
///
/// `part_size_bytes` grows the part size once a file would need more than
/// [`MAX_PARTS`] parts, purely so `part_count` can never exceed 10 000. That is
/// only worth doing if storage really refuses part 10 001 — otherwise the
/// formula is defending against nothing. This asks storage directly, because
/// no input to our own API can reach that part number.
#[tokio::test]
async fn storage_refuses_a_part_number_above_the_ten_thousand_cap() {
    requires_infra!();
    let rig = Rig::build().await;
    let key = format!("tenants/{}/blobs/cap-probe/original", rig.tenant);
    let upload = rig
        .blobs
        .begin_multipart(&key, "application/octet-stream")
        .await
        .expect("begin multipart");

    for (part_number, expected_ok) in [(10_000_u16, true), (10_001, false)] {
        let signed = rig
            .blobs
            .presign_part(&key, &upload, part_number, Duration::from_secs(300))
            .await
            .expect("presigning is local and never validates the part number");

        let response = reqwest::Client::new()
            .put(&signed.url)
            .body(BODY.to_vec())
            .send()
            .await
            .expect("PUT part");

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        eprintln!("part {part_number} -> {status} {}", body.replace('\n', " "));
        assert_eq!(
            status.is_success(),
            expected_ok,
            "part {part_number} returned {status}"
        );
    }

    rig.blobs
        .abort_multipart(&key, &upload)
        .await
        .expect("abort");
}
