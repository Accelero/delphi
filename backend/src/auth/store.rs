//! `tower-sessions` session store backed by SurrealDB.
//!
//! Reuses the same `Surreal<Client>` connection that `SurrealStorage` holds
//! so we don't open a second WebSocket. The session record schema is defined
//! in `schema.surql` (`session` table); each row is keyed by the Id assigned
//! by tower-sessions, holds the serialized session payload in `data`, and
//! mirrors the expiry into an indexed `expiry_date` column for the periodic
//! cleanup task.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::engine::remote::ws::Client;
use surrealdb::{RecordId, Surreal};
use tower_sessions::session::{Id, Record};
use tower_sessions::session_store::{self, ExpiredDeletion, SessionStore};

#[derive(Debug, Clone)]
pub struct SurrealSessionStore {
    db: Surreal<Client>,
}

impl SurrealSessionStore {
    pub fn new(db: Surreal<Client>) -> Self {
        Self { db }
    }
}

/// Wire shape stored in the `session` table.
#[derive(Debug, Serialize, Deserialize)]
struct SessionRow {
    /// Full serialized `Record` (id + data + expiry) — round-tripped via
    /// `serde_json::Value` so the SDK can store it as a Surreal `object`.
    data: serde_json::Value,
    expiry_date: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct LoadedRow {
    data: serde_json::Value,
}

impl SurrealSessionStore {
    fn record_id(id: &Id) -> RecordId {
        RecordId::from_table_key("session", id.to_string())
    }

    fn row_from(record: &Record) -> session_store::Result<SessionRow> {
        let data = serde_json::to_value(record).map_err(|e| {
            session_store::Error::Encode(format!("session record → json: {e}"))
        })?;
        Ok(SessionRow {
            data,
            expiry_date: offset_to_chrono(record.expiry_date),
        })
    }
}

#[async_trait]
impl SessionStore for SurrealSessionStore {
    async fn create(&self, record: &mut Record) -> session_store::Result<()> {
        // The default impl falls back to save() — but logs a warning about
        // ID collisions. We follow the trait's recommendation: regenerate the
        // ID until insertion succeeds. With a 128-bit random space, retries
        // are vanishingly rare.
        loop {
            let id = Self::record_id(&record.id);
            let row = Self::row_from(record)?;
            let res: surrealdb::Result<Option<surrealdb::sql::Value>> =
                self.db.create(id).content(row).await;
            match res {
                Ok(_) => return Ok(()),
                Err(surrealdb::Error::Db(surrealdb::error::Db::RecordExists { .. })) => {
                    // Collision — regenerate id and retry.
                    record.id = Id::default();
                    continue;
                }
                Err(e) => {
                    return Err(session_store::Error::Backend(format!(
                        "surreal create session: {e}"
                    )))
                }
            }
        }
    }

    async fn save(&self, record: &Record) -> session_store::Result<()> {
        let id = Self::record_id(&record.id);
        let row = Self::row_from(record)?;
        let _: Option<surrealdb::sql::Value> = self
            .db
            .upsert(id)
            .content(row)
            .await
            .map_err(|e| session_store::Error::Backend(format!("surreal save session: {e}")))?;
        Ok(())
    }

    async fn load(&self, session_id: &Id) -> session_store::Result<Option<Record>> {
        let id = Self::record_id(session_id);
        let row: Option<LoadedRow> = self
            .db
            .select(id)
            .await
            .map_err(|e| session_store::Error::Backend(format!("surreal load session: {e}")))?;
        let Some(row) = row else { return Ok(None) };
        let record: Record = serde_json::from_value(row.data).map_err(|e| {
            session_store::Error::Decode(format!("json → session record: {e}"))
        })?;
        // tower-sessions filters expired records itself, but we double-check
        // so a stale row that the cleanup task hasn't pruned doesn't leak.
        if record.expiry_date < time::OffsetDateTime::now_utc() {
            return Ok(None);
        }
        Ok(Some(record))
    }

    async fn delete(&self, session_id: &Id) -> session_store::Result<()> {
        let id = Self::record_id(session_id);
        let _: Option<surrealdb::sql::Value> = self
            .db
            .delete(id)
            .await
            .map_err(|e| session_store::Error::Backend(format!("surreal delete session: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl ExpiredDeletion for SurrealSessionStore {
    async fn delete_expired(&self) -> session_store::Result<()> {
        self.db
            .query("DELETE session WHERE expiry_date < time::now()")
            .await
            .and_then(|mut r| r.take::<Vec<surrealdb::sql::Value>>(0).map(|_| ()))
            .map_err(|e| session_store::Error::Backend(format!("surreal cleanup: {e}")))?;
        Ok(())
    }
}

fn offset_to_chrono(t: time::OffsetDateTime) -> DateTime<Utc> {
    let nanos = t.unix_timestamp_nanos();
    let secs = nanos.div_euclid(1_000_000_000) as i64;
    let nsec = nanos.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nsec).unwrap_or_else(|| Utc::now())
}
