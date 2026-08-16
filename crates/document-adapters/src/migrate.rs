//! Ordered migration runner.
//!
//! Both `api-service` and `document-worker` invoke this at startup. It is
//! idempotent and advisory-locked, so concurrent startups are safe.
//!
//! This replaced a single `include_str!` of `0001_pg_cutover.sql` executed on
//! every connect, which could not enumerate a directory and so could never
//! apply a second migration. **Chat's schema lives in the same `migrations/`
//! directory**, so this runner covers it too.

use include_dir::{include_dir, Dir};
use sqlx::{Connection, Executor, PgPool};
use thiserror::Error;

/// Embedded at compile time; `include_str!` cannot enumerate a directory.
static MIGRATIONS: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/../../migrations");

/// Guards the whole run so two services starting together do not both try to
/// create the same table. Session-scoped and explicitly released.
const MIGRATION_LOCK_ID: i64 = 0x6465_6C70_6869_0001;

#[derive(Debug, Error)]
pub enum MigrateError {
    #[error("could not read embedded migration {0}")]
    Unreadable(String),
    #[error("migration {file} failed: {source}")]
    Failed {
        file: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Apply every unapplied migration, in filename order, each in its own
/// transaction.
///
/// No checksum validation: dev databases are reset freely, and a checksum
/// mismatch would only turn a reset into a support ticket.
pub async fn run(pool: &PgPool) -> Result<u32, MigrateError> {
    let mut conn = pool.acquire().await?;

    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(MIGRATION_LOCK_ID)
        .execute(&mut *conn)
        .await?;

    let result = apply_all(&mut conn).await;

    // Release even on failure: holding the lock on a pooled connection would
    // block every other starting service until this one's connection closed.
    if let Err(error) = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(MIGRATION_LOCK_ID)
        .execute(&mut *conn)
        .await
    {
        tracing::warn!(%error, "could not release the migration advisory lock");
    }

    result
}

async fn apply_all(conn: &mut sqlx::PgConnection) -> Result<u32, MigrateError> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migration (
             version    text PRIMARY KEY,
             applied_at timestamptz NOT NULL DEFAULT now()
         )",
    )
    .await?;

    let applied: Vec<String> = sqlx::query_scalar("SELECT version FROM schema_migration")
        .fetch_all(&mut *conn)
        .await?;

    let mut pending: Vec<_> = MIGRATIONS
        .files()
        .filter(|file| {
            file.path()
                .extension()
                .is_some_and(|extension| extension == "sql")
        })
        .collect();
    pending.sort_by_key(|file| file.path().to_path_buf());

    let mut count = 0;
    for file in pending {
        let version = file
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| MigrateError::Unreadable(file.path().display().to_string()))?
            .to_owned();
        if applied.contains(&version) {
            continue;
        }
        let sql = file
            .contents_utf8()
            .ok_or_else(|| MigrateError::Unreadable(version.clone()))?;

        let mut tx = conn.begin().await?;
        tx.execute(sql).await.map_err(|source| MigrateError::Failed {
            file: version.clone(),
            source,
        })?;
        sqlx::query("INSERT INTO schema_migration (version) VALUES ($1)")
            .bind(&version)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        tracing::info!(%version, "applied migration");
        count += 1;
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_migrations_directory_is_embedded_and_ordered() {
        let mut names: Vec<_> = MIGRATIONS
            .files()
            .filter_map(|file| file.path().file_name()?.to_str())
            .filter(|name| name.ends_with(".sql"))
            .collect();
        names.sort();
        assert!(
            names.first().is_some_and(|first| first.starts_with("0001_")),
            "expected the numbered migrations to be embedded, found {names:?}"
        );
        assert!(
            names.iter().any(|name| name.starts_with("0003_")),
            "the document projection migration must be embedded, found {names:?}"
        );
        // Numeric prefixes are what make filename order the apply order.
        for name in &names {
            assert!(
                name.len() > 5 && name[..4].chars().all(|c| c.is_ascii_digit()),
                "{name} does not start with a four-digit ordinal"
            );
        }
    }
}
