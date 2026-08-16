//! The projection loop: the only writer of the `document` table.
//!
//! Two properties make it exactly-once and rebuildable:
//!
//! * rows and the checkpoint advance in **one transaction**, so a crash either
//!   loses both or neither;
//! * the consumer is **ordered and ephemeral**, positioned from the checkpoint
//!   on every start. A durable consumer's `deliver_policy` is fixed at
//!   creation, so after a rebuild (checkpoint reset to 0) it would resume from
//!   its own ack floor and replay nothing.

use std::collections::HashMap;
use std::time::Duration;

use async_nats::jetstream::consumer::{pull, DeliverPolicy};
use async_nats::jetstream::stream::Stream;
use delphi_document_app::{project_event, ProjectionOutcome};
use delphi_document_domain::DocumentState;
use futures::StreamExt;
use sqlx::{Connection, PgConnection, Postgres, Transaction};

use crate::error::AdapterError;
use crate::jetstream::DOCUMENT_EVENTS_FILTER;

use super::{row_to_document, DOCUMENT_COLUMNS};

pub const DOCUMENT_PROJECTION_NAME: &str = "document-pg";

/// How long to wait for the first message of a batch before looping back to
/// re-verify the lease. Keeps a quiet system from holding a stale lock belief.
const IDLE_POLL: Duration = Duration::from_secs(5);
/// How long to keep draining once a batch has started.
const BATCH_DRAIN: Duration = Duration::from_millis(25);

/// A held leader lease on a dedicated connection.
///
/// Session-scoped (`pg_try_advisory_lock`, not `_xact_`) and pinned to one
/// connection outside the pool: Postgres releases a session lock when its
/// connection goes away, so the lock and the work must share a session or the
/// lock guarantees nothing.
pub struct ProjectorLease {
    conn: PgConnection,
    lock_id: i64,
}

impl ProjectorLease {
    /// `Ok(None)` when another instance holds the lease.
    pub async fn try_acquire(
        database_url: &str,
        lock_id: i64,
    ) -> Result<Option<Self>, AdapterError> {
        let mut conn = PgConnection::connect(database_url).await?;
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(lock_id)
            .fetch_one(&mut conn)
            .await?;
        if !acquired {
            let _ = conn.close().await;
            return Ok(None);
        }
        Ok(Some(Self { conn, lock_id }))
    }

    /// Re-verify before every commit.
    ///
    /// If the connection drops — pool reconnect, PgBouncer, a network blip —
    /// Postgres releases the lock while this loop keeps projecting and a
    /// standby acquires it. That is exactly the two-projector scenario the
    /// design forbids, and it is invisible without this check.
    pub async fn still_held(&mut self) -> Result<bool, AdapterError> {
        let held: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_locks
                 WHERE locktype = 'advisory'
                   AND ((classid::bigint << 32) | objid::bigint) = $1
                   AND objsubid = 1
                   AND pid = pg_backend_pid()
                   AND granted
             )",
        )
        .bind(self.lock_id)
        .fetch_one(&mut self.conn)
        .await?;
        Ok(held)
    }

    pub async fn release(mut self) {
        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(self.lock_id)
            .execute(&mut self.conn)
            .await;
        let _ = self.conn.close().await;
    }
}

/// Parameterised on name and table so a rebuild can run alongside the live
/// projection and be swapped in.
pub struct ProjectionLoop {
    stream: Stream,
    name: String,
    table: String,
    batch_size: usize,
}

impl ProjectionLoop {
    pub fn new(stream: Stream, batch_size: usize) -> Self {
        Self {
            stream,
            name: DOCUMENT_PROJECTION_NAME.to_owned(),
            table: "document".to_owned(),
            batch_size: batch_size.max(1),
        }
    }

    pub fn into_target(mut self, name: impl Into<String>, table: impl Into<String>) -> Self {
        self.name = name.into();
        self.table = table.into();
        self
    }

    /// Run until the lease is lost or the event stream ends.
    pub async fn run(&self, lease: &mut ProjectorLease) -> Result<(), AdapterError> {
        let checkpoint = self.read_checkpoint(lease).await?;
        tracing::info!(
            projection = %self.name,
            checkpoint,
            "projection loop starting from its checkpoint"
        );

        let consumer = self
            .stream
            .create_consumer(pull::OrderedConfig {
                filter_subject: DOCUMENT_EVENTS_FILTER.to_owned(),
                deliver_policy: DeliverPolicy::ByStartSequence {
                    start_sequence: checkpoint + 1,
                },
                ..Default::default()
            })
            .await
            .map_err(|error| {
                AdapterError::Topology(format!("create projection consumer: {error}"))
            })?;

        let mut messages = consumer
            .messages()
            .await
            .map_err(|error| AdapterError::Topology(format!("open projection stream: {error}")))?;

        loop {
            // Re-verify before doing anything, so a lost lease is noticed even
            // on an idle stream.
            if !lease.still_held().await? {
                tracing::error!(projection = %self.name, "projector lease lost; stopping");
                return Ok(());
            }

            let mut batch: Vec<(u64, String, Vec<u8>)> = Vec::new();
            match tokio::time::timeout(IDLE_POLL, messages.next()).await {
                Ok(Some(Ok(message))) => push_message(&mut batch, message),
                Ok(Some(Err(error))) => {
                    tracing::warn!(%error, "projection consumer receive failed");
                    continue;
                }
                Ok(None) => return Ok(()),
                Err(_elapsed) => continue,
            }
            while batch.len() < self.batch_size {
                match tokio::time::timeout(BATCH_DRAIN, messages.next()).await {
                    Ok(Some(Ok(message))) => push_message(&mut batch, message),
                    Ok(Some(Err(error))) => {
                        tracing::warn!(%error, "projection consumer receive failed");
                        break;
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            if batch.is_empty() {
                continue;
            }

            // The lease check and the commit must be on the same connection, or
            // the check proves nothing about the transaction that follows.
            if !lease.still_held().await? {
                tracing::error!(projection = %self.name, "projector lease lost before commit; stopping");
                return Ok(());
            }
            self.apply_batch(lease, &batch).await?;
        }
    }

    async fn apply_batch(
        &self,
        lease: &mut ProjectorLease,
        batch: &[(u64, String, Vec<u8>)],
    ) -> Result<(), AdapterError> {
        let mut tx = lease.conn.begin().await?;
        // Within a batch a document may receive several events; fold them in
        // order against one in-memory state rather than re-reading the row.
        let mut folded: HashMap<(String, String), DocumentState> = HashMap::new();
        let mut last_seq = 0;

        for (sequence, subject, payload) in batch {
            last_seq = *sequence;
            let key = document_key_from_subject(subject);
            let prior = match &key {
                Some(key) => match folded.get(key) {
                    Some(state) => Some(state.clone()),
                    None => self.load_document(&mut tx, &key.0, &key.1).await?,
                },
                None => None,
            };

            match project_event(prior, payload, *sequence) {
                ProjectionOutcome::Upsert(state) => {
                    let key = (state.tenant_id.clone(), state.document_id.clone());
                    folded.insert(key, *state);
                }
                ProjectionOutcome::Failure {
                    payload,
                    error,
                    domain_violation,
                } => {
                    // Neither an unknown event type nor a fold error may stall
                    // the checkpoint. Because the projection is keyed per
                    // document, the hole affects one document rather than
                    // freezing the read model.
                    if domain_violation {
                        tracing::error!(
                            projection = %self.name,
                            sequence,
                            subject = %subject,
                            %error,
                            "event violates the document domain; skipping it"
                        );
                    } else {
                        tracing::warn!(
                            projection = %self.name,
                            sequence,
                            subject = %subject,
                            %error,
                            "unrecognised event; skipping it"
                        );
                    }
                    self.record_failure(&mut tx, *sequence, subject, &payload, &error)
                        .await?;
                }
            }
        }

        for state in folded.into_values() {
            self.upsert_document(&mut tx, &state).await?;
        }

        sqlx::query(
            "INSERT INTO projection_checkpoint (name, stream_seq, updated_at)
             VALUES ($1, $2, now())
             ON CONFLICT (name) DO UPDATE SET stream_seq = EXCLUDED.stream_seq, updated_at = now()",
        )
        .bind(&self.name)
        .bind(last_seq as i64)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn read_checkpoint(&self, lease: &mut ProjectorLease) -> Result<u64, AdapterError> {
        let sequence: Option<i64> =
            sqlx::query_scalar("SELECT stream_seq FROM projection_checkpoint WHERE name = $1")
                .bind(&self.name)
                .fetch_optional(&mut lease.conn)
                .await?;
        Ok(sequence.unwrap_or(0).max(0) as u64)
    }

    async fn load_document(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        tenant: &str,
        document_id: &str,
    ) -> Result<Option<DocumentState>, AdapterError> {
        let sql = format!(
            "SELECT {DOCUMENT_COLUMNS} FROM {} WHERE tenant_id = $1 AND document_id = $2",
            self.table
        );
        let row = sqlx::query(&sql)
            .bind(tenant)
            .bind(document_id)
            .fetch_optional(&mut **tx)
            .await?;
        row.as_ref().map(row_to_document).transpose().map_err(Into::into)
    }

    async fn upsert_document(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        state: &DocumentState,
    ) -> Result<(), AdapterError> {
        // The monotonic guard is on `stream_seq`, not `version`: several event
        // types deliberately repeat the version, so guarding on it would drop
        // every index and extraction result.
        let sql = format!(
            "INSERT INTO {table}
                 ({DOCUMENT_COLUMNS})
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
             ON CONFLICT (tenant_id, document_id) DO UPDATE SET
                 owner_user_id = EXCLUDED.owner_user_id,
                 version       = EXCLUDED.version,
                 stream_seq    = EXCLUDED.stream_seq,
                 state         = EXCLUDED.state,
                 index_state   = EXCLUDED.index_state,
                 index_version = EXCLUDED.index_version,
                 current_blob  = EXCLUDED.current_blob,
                 filename      = EXCLUDED.filename,
                 content_type  = EXCLUDED.content_type,
                 byte_size     = EXCLUDED.byte_size,
                 checksum      = EXCLUDED.checksum,
                 title         = EXCLUDED.title,
                 tags          = EXCLUDED.tags,
                 description   = EXCLUDED.description,
                 metadata      = EXCLUDED.metadata,
                 updated_at    = EXCLUDED.updated_at
             WHERE {table}.stream_seq < EXCLUDED.stream_seq",
            table = self.table
        );
        sqlx::query(&sql)
            .bind(&state.tenant_id)
            .bind(&state.document_id)
            .bind(&state.owner_user_id)
            .bind(state.version as i64)
            .bind(state.stream_seq as i64)
            .bind(state.state.as_str())
            .bind(state.index_state.as_str())
            .bind(state.index_version.map(|value| value as i64))
            .bind(&state.current_blob)
            .bind(&state.filename)
            .bind(&state.content_type)
            .bind(state.byte_size.map(|value| value as i64))
            .bind(&state.checksum)
            .bind(&state.title)
            .bind(serde_json::to_value(&state.tags).unwrap_or_else(|_| serde_json::json!([])))
            .bind(&state.description)
            .bind(&state.metadata)
            .bind(state.created_at)
            .bind(state.updated_at)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    async fn record_failure(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sequence: u64,
        subject: &str,
        payload: &serde_json::Value,
        error: &str,
    ) -> Result<(), AdapterError> {
        sqlx::query(
            "INSERT INTO projection_failure (name, stream_seq, subject, payload, error)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (name, stream_seq) DO NOTHING",
        )
        .bind(&self.name)
        .bind(sequence as i64)
        .bind(subject)
        .bind(payload)
        .bind(error)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

fn push_message(batch: &mut Vec<(u64, String, Vec<u8>)>, message: async_nats::jetstream::Message) {
    match message.info() {
        Ok(info) => batch.push((
            info.stream_sequence,
            message.subject.to_string(),
            message.payload.to_vec(),
        )),
        Err(error) => {
            // Without a sequence we cannot checkpoint past it, but we also
            // cannot fold it. Dropping it is the only option that keeps the
            // loop moving; the ordered consumer will not re-deliver it.
            tracing::error!(%error, "projection message missing jetstream metadata; dropped");
        }
    }
}

/// `documents.<tenant>.<partition>.<document_id>`
fn document_key_from_subject(subject: &str) -> Option<(String, String)> {
    let mut tokens = subject.split('.');
    match (
        tokens.next(),
        tokens.next(),
        tokens.next(),
        tokens.next(),
        tokens.next(),
    ) {
        (Some("documents"), Some(tenant), Some(_partition), Some(document_id), None) => {
            Some((tenant.to_owned(), document_id.to_owned()))
        }
        _ => None,
    }
}

/// Reset a projection so the next run rebuilds it from sequence 1.
pub async fn reset(conn: &mut PgConnection, name: &str, table: &str) -> Result<(), AdapterError> {
    let mut tx = conn.begin().await?;
    sqlx::query(&format!("TRUNCATE TABLE {table}"))
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM projection_checkpoint WHERE name = $1")
        .bind(name)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM projection_failure WHERE name = $1")
        .bind(name)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Read one row for tests and tooling without going through the read model.
pub async fn peek_document(
    conn: &mut PgConnection,
    tenant: &str,
    document_id: &str,
) -> Result<Option<DocumentState>, AdapterError> {
    let sql =
        format!("SELECT {DOCUMENT_COLUMNS} FROM document WHERE tenant_id = $1 AND document_id = $2");
    let row = sqlx::query(&sql)
        .bind(tenant)
        .bind(document_id)
        .fetch_optional(conn)
        .await?;
    row.as_ref().map(row_to_document).transpose().map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_document_key_comes_out_of_the_subject() {
        assert_eq!(
            document_key_from_subject("documents.acme.09.doc-1"),
            Some(("acme".to_owned(), "doc-1".to_owned()))
        );
    }

    #[test]
    fn a_subject_outside_the_scheme_yields_no_key() {
        for subject in [
            "documents.acme.09",
            "documents.acme.09.doc-1.extra",
            "document_work.v1.upload_completed",
            "",
        ] {
            assert_eq!(document_key_from_subject(subject), None, "{subject}");
        }
    }
}
