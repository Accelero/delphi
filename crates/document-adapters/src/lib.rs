//! Port implementations for the document lifecycle: JetStream, Postgres, S3.
//!
//! Depends inward on `delphi-document-app`. Services depend on this crate for
//! wiring and on `delphi-document-app` for the use cases they call.

pub mod config;
pub mod error;
pub mod jetstream;
pub mod migrate;
pub mod postgres;
pub mod s3;
pub mod verification;

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::Context;
use chrono::{DateTime, Utc};
use delphi_document_app::{Clock, IdGen};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

pub use config::{ApiConfig, ConfigError, S3Config, TopologyConfig, WorkerConfig};
pub use error::AdapterError;

/// Wall clock.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// ULID generator.
///
/// `Ulid::new()`, never a monotonic generator: a monotonic generator increments
/// the random component within a millisecond, so one known id makes its
/// neighbours derivable — and the upload id is what keeps an object's key
/// unguessable.
pub struct UlidGen;

impl IdGen for UlidGen {
    fn ulid(&self) -> String {
        ulid::Ulid::new().to_string()
    }
}

/// Connect to Postgres and bring the schema up to date.
pub async fn connect_postgres(
    database_url: &str,
    max_connections: u32,
) -> Result<PgPool, AdapterError> {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(database_url)
        .await?;
    let applied = migrate::run(&pool).await?;
    if applied > 0 {
        tracing::info!(applied, "database schema is up to date");
    }
    Ok(pool)
}

/// Connect to NATS as the topology's **author**: declare it, then bind.
///
/// Exactly one component may do this, and it is `api-service` — the writer that
/// creates upload records. Owning the bucket's lifetime belongs with the thing
/// that puts entries in it.
///
/// The single-author rule is the whole point. When every service asserted the
/// topology at startup, each carried its own copy of the upload TTL and the
/// bucket took whichever value restarted last; two replicas with drifted config
/// flipped it back and forth on every restart (`1d → 1h → 1d`, measured).
/// Several `api-service` replicas are fine, because they share one config and
/// the write is idempotent.
pub async fn connect_jetstream_as_author(
    nats_url: &str,
    upload_ttl: Duration,
) -> Result<(Context, async_nats::jetstream::kv::Store), AdapterError> {
    let client = async_nats::connect(nats_url)
        .await
        .map_err(|error| AdapterError::Connect(error.to_string()))?;
    let js = async_nats::jetstream::new(client);
    let bucket = jetstream::ensure_topology(&js, upload_ttl).await?;
    Ok((js, bucket))
}

/// Connect to NATS and **bind** to the topology, never declare it.
///
/// For everything that is not the author. Binding means a service cannot hold
/// an opinion about the topology, which is precisely why it no longer needs the
/// numbers that describe one — `document-worker` reads neither the upload TTL
/// nor the part URL TTL now.
///
/// A missing bucket is a hard startup failure: it means `api-service` has not
/// run yet, and quietly creating one with default settings is how the drift
/// came back.
pub async fn connect_jetstream(
    nats_url: &str,
) -> Result<(Context, async_nats::jetstream::kv::Store), AdapterError> {
    let client = async_nats::connect(nats_url)
        .await
        .map_err(|error| AdapterError::Connect(error.to_string()))?;
    let js = async_nats::jetstream::new(client);
    let bucket = jetstream::bind_upload_state(&js).await?;
    Ok((js, bucket))
}

/// Everything the API and the worker both need.
#[derive(Clone)]
pub struct DocumentInfra {
    pub pool: PgPool,
    pub js: Context,
    pub events: Arc<jetstream::JetStreamEventStore>,
    pub work_queue: Arc<jetstream::JetStreamWorkQueue>,
    pub uploads: Arc<jetstream::KvUploadStateStore>,
    pub blobs: Arc<s3::S3BlobStore>,
    pub read_model: Arc<postgres::PgDocumentReadModel>,
}

impl DocumentInfra {
    /// For `api-service`, the topology's author: declare, then bind.
    ///
    /// `upload_ttl` is taken here and nowhere else, which is the point — see
    /// [`connect_jetstream_as_author`].
    pub async fn connect_as_author(
        database_url: &str,
        pg_max_connections: u32,
        nats_url: &str,
        upload_ttl: Duration,
    ) -> Result<Self, AdapterError> {
        let (js, bucket) = connect_jetstream_as_author(nats_url, upload_ttl).await?;
        Self::assemble(database_url, pg_max_connections, js, bucket).await
    }

    /// For everything else: bind to what the author declared.
    pub async fn connect(
        database_url: &str,
        pg_max_connections: u32,
        nats_url: &str,
    ) -> Result<Self, AdapterError> {
        let (js, bucket) = connect_jetstream(nats_url).await?;
        Self::assemble(database_url, pg_max_connections, js, bucket).await
    }

    async fn assemble(
        database_url: &str,
        pg_max_connections: u32,
        js: Context,
        bucket: async_nats::jetstream::kv::Store,
    ) -> Result<Self, AdapterError> {
        let s3_config = S3Config::from_env()?;
        let pool = connect_postgres(database_url, pg_max_connections).await?;

        Ok(Self {
            events: Arc::new(jetstream::JetStreamEventStore::new(js.clone()).await?),
            work_queue: Arc::new(jetstream::JetStreamWorkQueue::new(js.clone())),
            uploads: Arc::new(jetstream::KvUploadStateStore::new(bucket)),
            blobs: Arc::new(s3::S3BlobStore::new(&s3_config)),
            read_model: Arc::new(postgres::PgDocumentReadModel::new(pool.clone())),
            pool,
            js,
        })
    }
}
