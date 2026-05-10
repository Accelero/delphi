//! [`SystemDb`] — privileged SurrealDB connection used by boot, admin, and
//! scheduler paths.
//!
//! This is the **only** part of the codebase that holds an "above-RBAC"
//! credential. It's deliberately not in [`crate::state::AppState`] —
//! request handlers physically cannot reach it. The trust surface is
//! limited to:
//!
//! - schema apply on startup
//! - `ensure_user` pre-flight (the per-request JWT path needs the
//!   `app_user` row to already exist before SurrealDB's AUTHENTICATE
//!   clause can resolve it)
//! - tenant bootstrap (`resolve_default_tenant`)
//! - admin CLI (`delphi admin status / wipe`)
//! - in-process scheduler ingest
//!
//! In Phase 1 the per-request `RequestDbPool` borrows the same
//! connection; isolation is application-layer. In Phase 2 the pool gets
//! its own connections authenticated per-request via IdP JWT, and
//! `SystemDb` stays as the small contained escape hatch above.

use serde::Deserialize;
use surrealdb::engine::any::Any;
use surrealdb::opt::auth::Root;
use surrealdb::{RecordId, Surreal};

use crate::error::{Error, Result};

const SCHEMA_SURQL: &str = include_str!("../../schema.surql");

/// Row counts per table — admin-tier diagnostic.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct Counts {
    pub documents: u64,
    pub document_content: u64,
    pub chunks: u64,
    pub document_versions: u64,
}

/// Runtime config for the `app_session` JWT access method. Passed to
/// [`SystemDb::define_jwt_access`] at startup when `AuthMode::Jwt` is
/// active.
#[derive(Debug, Clone)]
pub struct JwtAccessConfig {
    pub kind: JwtAccessKind,
    /// If `Some(iss)`, the AUTHENTICATE clause throws on JWTs whose
    /// `iss` doesn't match. Defence-in-depth even though the BFF
    /// already validated.
    pub expected_issuer: Option<String>,
    /// If `Some(aud)`, the AUTHENTICATE clause throws on JWTs whose
    /// `aud` doesn't contain this value. Handles both string and array
    /// `aud` claims.
    pub expected_audience: Option<String>,
    /// SurrealDB session lifetime per `db.authenticate`. Defaults to
    /// 1800 (30min).
    pub session_duration_secs: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum JwtAccessKind {
    /// Symmetric — backend mints internal JWTs using this secret. Used in
    /// tests and the tier-1 dev path.
    Hs512 { secret: String },
    /// Asymmetric — SurrealDB fetches the IdP's public keys from JWKS.
    /// Used in tier-2 dev and production with a real OIDC IdP.
    Jwks { url: String },
}

fn escape_surrealql_string(s: &str) -> String {
    // SurrealDB single-quoted string literal: escape backslash and
    // single quotes. The key / URL / issuer / audience strings we
    // accept are operator-controlled, but be defensive anyway.
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Privileged SurrealDB handle. Wraps `Surreal<Any>` and exposes only the
/// system-level operations.
#[derive(Clone)]
pub struct SystemDb {
    db: Surreal<Any>,
}

impl SystemDb {
    /// Construct from environment.
    ///
    /// Required env vars: `SURREAL_URL`, `SURREAL_SERVICE_USER`,
    /// `SURREAL_SERVICE_PASS`, `SURREAL_NS`, `SURREAL_DB`.
    pub async fn from_env() -> Result<Self> {
        let url = env_or("SURREAL_URL", "ws://surrealdb:8000/rpc");
        let user = env_required("SURREAL_SERVICE_USER")?;
        let password = env_required("SURREAL_SERVICE_PASS")?;
        let namespace = env_or("SURREAL_NS", "delphi");
        let database = env_or("SURREAL_DB", "main");
        Self::connect(&url, &user, &password, &namespace, &database).await
    }

    pub async fn connect(
        url: &str,
        user: &str,
        password: &str,
        namespace: &str,
        database: &str,
    ) -> Result<Self> {
        let db = surrealdb::engine::any::connect(url).await?;
        if engine_requires_auth(url) {
            db.signin(Root {
                username: user,
                password,
            })
            .await?;
        }
        db.use_ns(namespace).use_db(database).await?;
        Ok(Self { db })
    }

    /// Spin up an embedded in-memory SurrealDB. **Test-only convenience.**
    pub async fn in_memory(namespace: &str, database: &str) -> Result<Self> {
        Self::connect("memory", "", "", namespace, database).await
    }

    /// Borrow the underlying handle. Used by `auth/bootstrap.rs` (which
    /// runs raw SurrealQL upserts on `app_user` / `tenant` / `membership`),
    /// [`crate::storage::RequestDbPool::from_system`], and integration
    /// tests that drive the engine directly to verify PERMISSIONS
    /// enforcement.
    ///
    /// Application handlers must not call this — they get the typed
    /// `Storage`-trait surface via [`crate::storage::RequestDbPool`].
    pub fn raw(&self) -> &Surreal<Any> {
        &self.db
    }

    /// Apply the canonical schema. Idempotent — every statement uses
    /// `IF NOT EXISTS` / `IF EXISTS`.
    pub async fn init_schema(&self) -> Result<()> {
        self.db.query(SCHEMA_SURQL).await?.check()?;
        Ok(())
    }

    /// Configure the `app_session` JWT access method at runtime. Required
    /// for the engine-enforced tenant-isolation path — without this,
    /// `db.authenticate(jwt)` has no access definition to validate
    /// against.
    ///
    /// `$auth` is resolved from the JWT's `ID` claim directly (no
    /// AUTHENTICATE override). The backend's pre-flight `ensure_user`
    /// guarantees the row exists before minting.
    ///
    /// Re-applies cleanly via `OVERWRITE` so a key/JWKS rotation doesn't
    /// require a schema migration.
    pub async fn define_jwt_access(&self, cfg: &JwtAccessConfig) -> Result<()> {
        let validator = match &cfg.kind {
            JwtAccessKind::Hs512 { secret } => format!(
                "ALGORITHM HS512 KEY '{}'",
                escape_surrealql_string(secret)
            ),
            JwtAccessKind::Jwks { url } => {
                format!("URL '{}'", escape_surrealql_string(url))
            }
        };

        let session_secs = cfg.session_duration_secs.unwrap_or(1800);

        // Unused for now — kept on the config surface for when we wire
        // an AUTHENTICATE clause to enforce these claims (upstream-JWT
        // mode). Silenced via `let _ =`.
        let _ = &cfg.expected_issuer;
        let _ = &cfg.expected_audience;

        let stmt = format!(
            "DEFINE ACCESS OVERWRITE app_session ON DATABASE TYPE RECORD \
             WITH JWT {validator} \
             DURATION FOR SESSION {session_secs}s;"
        );
        self.db.query(stmt).await?.check()?;
        Ok(())
    }

    /// Row counts. `tenant = Some(...)` scopes per-tenant; `None` is
    /// cross-tenant (admin only).
    pub async fn counts(&self, tenant: Option<&RecordId>) -> Result<Counts> {
        let documents = count_table(&self.db, "document", tenant).await?;
        let document_content = count_table(&self.db, "document_content", tenant).await?;
        let chunks = count_table(&self.db, "chunk", tenant).await?;
        let document_versions = count_table(&self.db, "document_version", tenant).await?;
        Ok(Counts {
            documents,
            document_content,
            chunks,
            document_versions,
        })
    }

    /// Delete data; keep schema. `tenant = Some(...)` scopes per-tenant;
    /// `None` is cross-tenant (admin only).
    pub async fn wipe(&self, tenant: Option<&RecordId>) -> Result<()> {
        for table in ["chunk", "document_content", "document_version", "document", "feed_read"] {
            match tenant {
                Some(t) => {
                    self.db
                        .query(format!("DELETE {table} WHERE tenant_id = $t"))
                        .bind(("t", t.clone()))
                        .await?
                        .check()?;
                }
                None => {
                    self.db.query(format!("DELETE {table}")).await?.check()?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct CountRow {
    n: u64,
}

async fn count_table(
    db: &Surreal<Any>,
    table: &str,
    tenant: Option<&RecordId>,
) -> Result<u64> {
    let row: Option<CountRow> = match tenant {
        Some(t) => db
            .query(format!(
                "SELECT count() AS n FROM {table} WHERE tenant_id = $t GROUP ALL"
            ))
            .bind(("t", t.clone()))
            .await?
            .take(0)?,
        None => db
            .query(format!("SELECT count() AS n FROM {table} GROUP ALL"))
            .await?
            .take(0)?,
    };
    Ok(row.map(|r| r.n).unwrap_or(0))
}

/// `memory` / `rocksdb:` are local engines that don't gate on credentials.
/// Anything else (ws/wss/http/https/tcp/…) does.
pub(crate) fn engine_requires_auth(url: &str) -> bool {
    !(url == "memory"
        || url.starts_with("mem://")
        || url.starts_with("rocksdb:")
        || url.starts_with("surrealkv:")
        || url.starts_with("file:"))
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

fn env_required(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| Error::EnvMissing(key.into()))
}

#[cfg(test)]
mod endpoint_tests {
    use super::engine_requires_auth;

    #[test]
    fn auth_required_for_remote_engines() {
        assert!(engine_requires_auth("ws://surrealdb:8000/rpc"));
        assert!(engine_requires_auth("wss://x:8000"));
        assert!(engine_requires_auth("http://x:8000"));
        assert!(engine_requires_auth("https://x:8000"));
    }

    #[test]
    fn auth_skipped_for_local_engines() {
        assert!(!engine_requires_auth("memory"));
        assert!(!engine_requires_auth("mem://"));
        assert!(!engine_requires_auth("rocksdb:/data/db"));
    }
}
