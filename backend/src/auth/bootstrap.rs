//! Tenant / user / membership upserts.
//!
//! Two entry points:
//!
//! - [`ensure_user`] — runs per-request once an authenticated [`Claims`] has
//!   been established. Idempotent: SELECT on `(iss, sub)`, fall through to
//!   CREATE if missing; resolve tenant by slug (auto-create only the
//!   configured default — never auto-create arbitrary tenants from claims).
//! - [`seed_dev_world`] — dev-only. Runs once at startup so the dev tenant
//!   exists with role=owner before the first request arrives. The
//!   per-request [`ensure_user`] then becomes a no-op SELECT.
//!
//! [`resolve_default_tenant`] is the bridge: both modes need the default
//! tenant's `RecordId` resolved at startup so the request hot path doesn't
//! re-resolve it on every call.
//!
//! All operations here run under the privileged [`SystemDb`] handle —
//! they're upserting into `tenant` / `app_user` / `membership`, which
//! are the auth-foundation tables. The per-request engine-enforced
//! path can't touch them because the user record may not exist yet.

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use surrealdb::RecordId;

/// Returns true if `err`'s source chain contains a SurrealDB
/// `IndexExists` failure — i.e. another concurrent writer beat us to the
/// CREATE on a UNIQUE-indexed table (`app_user(iss,sub)`,
/// `membership(user,tenant_id)`). The upsert SELECT-then-CREATE pattern
/// has a TOCTOU window; this is how we detect that we lost the race.
fn is_index_exists(err: &anyhow::Error) -> bool {
    let mut e: Option<&dyn std::error::Error> = Some(err.as_ref());
    while let Some(cur) = e {
        if let Some(surrealdb::Error::Db(surrealdb::error::Db::IndexExists { .. })) =
            cur.downcast_ref::<surrealdb::Error>()
        {
            return true;
        }
        e = cur.source();
    }
    false
}

use super::claims::Claims;
use super::context::AuthContext;
#[cfg(feature = "dev-auth")]
use super::config::DevConfig;
use crate::storage::SystemDb;

/// Reset the SystemDb session to the privileged baseline.
///
/// Needed **only** when the SystemDb handle shares its underlying engine
/// with the [`crate::storage::RequestDbPool`]'s connections — i.e.
/// embedded test mode. A prior `db.authenticate(jwt)` on a pool clone
/// has put the shared session into RECORD mode, so the system-path
/// upserts on `tenant` / `app_user` / `membership` would otherwise get
/// denied by PERMISSIONS.
///
/// In production (remote engine) the SystemDb owns its own connection
/// that nothing else touches; the session stays at Root from
/// construction onwards, and `invalidate` + `signin` would just create
/// a cross-request race where two concurrent calls clobber each
/// other's authentication mid-query. So we skip the whole sequence
/// when `system.shared_engine()` is `false`.
async fn ensure_root_session(system: &SystemDb) {
    if !system.shared_engine() {
        return;
    }
    // Embedded engine, shared session: drop any RECORD session installed
    // by a pool-acquire so the privileged baseline (full access on
    // in-memory/rocksdb) is restored. Signin is intentionally skipped
    // because embedded engines have no Root user defined.
    let _ = system.raw().invalidate().await;
}

#[derive(Debug, Deserialize)]
struct IdRow {
    id: RecordId,
}

#[derive(Debug, Deserialize)]
struct UserRow {
    id: RecordId,
    #[serde(rename = "iss")]
    _iss: String,
    #[serde(rename = "sub")]
    _sub: String,
    email: String,
    display_name: Option<String>,
}

/// `"tenant-a"` → `"Tenant A"`. Best-effort display name for an
/// auto-provisioned tenant. Operators can edit in the admin surface
/// once that ships.
fn slug_to_display_name(slug: &str) -> String {
    slug.split(&['-', '_'][..])
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut chars = p.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().chain(chars).collect(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

async fn upsert_tenant(system: &SystemDb, slug: &str, name: &str) -> Result<RecordId> {
    ensure_root_session(system).await;
    let db = system.raw();
    let mut r = db
        .query("SELECT id FROM tenant WHERE slug = $slug LIMIT 1")
        .bind(("slug", slug.to_string()))
        .await
        .context("select tenant by slug")?;
    let existing: Option<IdRow> = r.take(0).context("decode tenant select")?;
    if let Some(t) = existing {
        return Ok(t.id);
    }
    let create = db
        .query("CREATE tenant CONTENT { slug: $slug, name: $name } RETURN id")
        .bind(("slug", slug.to_string()))
        .bind(("name", name.to_string()))
        .await
        .context("create tenant")
        .and_then(|mut r| r.take::<Option<IdRow>>(0).context("decode tenant create"));
    match create {
        Ok(Some(row)) => Ok(row.id),
        Ok(None) => Err(anyhow!("tenant CREATE returned no row")),
        Err(e) if is_index_exists(&e) => {
            // Concurrent request created the same slug first; re-select.
            let mut r = db
                .query("SELECT id FROM tenant WHERE slug = $slug LIMIT 1")
                .bind(("slug", slug.to_string()))
                .await
                .context("reselect tenant after race")?;
            r.take::<Option<IdRow>>(0)
                .context("decode tenant reselect")?
                .map(|x| x.id)
                .ok_or_else(|| anyhow!("tenant vanished after IndexExists race"))
        }
        Err(e) => Err(e),
    }
}

async fn upsert_user(
    system: &SystemDb,
    iss: &str,
    sub: &str,
    email: &str,
    display_name: Option<&str>,
    tenant: &RecordId,
) -> Result<UserRow> {
    ensure_root_session(system).await;
    let db = system.raw();
    let mut r = db
        .query(
            "SELECT id, iss, sub, email, display_name FROM app_user \
             WHERE iss = $iss AND sub = $sub LIMIT 1",
        )
        .bind(("iss", iss.to_string()))
        .bind(("sub", sub.to_string()))
        .await
        .context("select app_user")?;
    let existing: Option<UserRow> = r.take(0).context("decode app_user select")?;
    if let Some(u) = existing {
        // Best-effort refresh of cached attrs + tenant denorm. Not fatal
        // on failure. `tenant_id` is what `$auth.tenant_id` resolves to
        // in PERMISSIONS clauses on every domain table — keeping it
        // current is the load-bearing part of this update.
        let _ = db
            .query(
                "UPDATE $rid SET email = $email, display_name = $name, \
                                  tenant_id = $tid, last_seen_at = time::now()",
            )
            .bind(("rid", u.id.clone()))
            .bind(("email", email.to_string()))
            .bind(("name", display_name.map(|s| s.to_string())))
            .bind(("tid", tenant.clone()))
            .await;
        return Ok(u);
    }
    let create = db
        .query(
            "CREATE app_user CONTENT { \
                iss: $iss, sub: $sub, email: $email, display_name: $name, \
                tenant_id: $tid \
             } RETURN id, iss, sub, email, display_name",
        )
        .bind(("iss", iss.to_string()))
        .bind(("sub", sub.to_string()))
        .bind(("email", email.to_string()))
        .bind(("name", display_name.map(|s| s.to_string())))
        .bind(("tid", tenant.clone()))
        .await
        .context("create app_user")
        .and_then(|mut r| {
            r.take::<Option<UserRow>>(0)
                .context("decode app_user create")
        });
    match create {
        Ok(Some(row)) => Ok(row),
        Ok(None) => Err(anyhow!("app_user CREATE returned no row")),
        Err(e) if is_index_exists(&e) => {
            // Lost the create race against a concurrent request for the
            // same (iss, sub). Re-select to pick up the row the winner
            // inserted.
            let mut r = db
                .query(
                    "SELECT id, iss, sub, email, display_name FROM app_user \
                     WHERE iss = $iss AND sub = $sub LIMIT 1",
                )
                .bind(("iss", iss.to_string()))
                .bind(("sub", sub.to_string()))
                .await
                .context("reselect app_user after race")?;
            r.take::<Option<UserRow>>(0)
                .context("decode app_user reselect")?
                .ok_or_else(|| anyhow!("app_user vanished after IndexExists race"))
        }
        Err(e) => Err(e),
    }
}

async fn upsert_membership(
    system: &SystemDb,
    user: &RecordId,
    tenant: &RecordId,
    role: &str,
) -> Result<()> {
    ensure_root_session(system).await;
    let db = system.raw();
    let mut r = db
        .query("SELECT id FROM membership WHERE user = $u AND tenant_id = $t LIMIT 1")
        .bind(("u", user.clone()))
        .bind(("t", tenant.clone()))
        .await
        .context("select membership")?;
    let existing: Option<IdRow> = r.take(0).context("decode membership select")?;
    if existing.is_some() {
        return Ok(());
    }
    let result = db
        .query("CREATE membership CONTENT { user: $u, tenant_id: $t, role: $role }")
        .bind(("u", user.clone()))
        .bind(("t", tenant.clone()))
        .bind(("role", role.to_string()))
        .await
        .context("create membership")
        .and_then(|r| r.check().context("membership create check"));
    match result {
        Ok(_) => Ok(()),
        // Lost the create race; the membership already exists. Idempotent
        // upsert semantics: treat as success.
        Err(e) if is_index_exists(&e) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Resolve the default tenant by slug, creating it if missing. Called once
/// at startup so the per-request hot path can skip this query.
pub async fn resolve_default_tenant(system: &SystemDb, slug: &str) -> Result<RecordId> {
    upsert_tenant(system, slug, "Default").await
}

/// Per-request user upsert. Looks up `(iss, sub)`; creates the user +
/// membership against the resolved tenant if missing.
///
/// Tenant resolution: claim slug exists → use it; unknown slug →
/// **upsert** (auto-create on first sight). The BFF is trusted to
/// only emit tenant_ids the IdP admin has configured (e.g. as a
/// Keycloak user attribute), so the set of slugs that ever reach
/// here is bounded by IdP configuration. Auto-provisioning matches
/// the SaaS shape: the IdP admin grants a user a tenant attribute
/// and the tenant materialises in Delphi on first login.
///
/// `name` defaults to a title-cased slug; operators can rename in
/// the admin surface later.
pub async fn ensure_user(
    system: &SystemDb,
    claims: &Claims,
    default_tenant_slug: &str,
    default_tenant_id: &RecordId,
) -> Result<AuthContext> {
    let tenant_id = match claims.tenant_slug.as_deref() {
        Some(slug) if slug == default_tenant_slug => default_tenant_id.clone(),
        Some(slug) => upsert_tenant(system, slug, &slug_to_display_name(slug)).await?,
        None => default_tenant_id.clone(),
    };

    let user = upsert_user(
        system,
        &claims.iss,
        &claims.sub,
        &claims.email,
        claims.display_name.as_deref(),
        &tenant_id,
    )
    .await?;
    upsert_membership(system, &user.id, &tenant_id, "member").await?;

    Ok(AuthContext {
        user_id: user.id,
        tenant_id,
        email: user.email,
        display_name: user.display_name,
        iss: claims.iss.clone(),
        sub: claims.sub.clone(),
        roles: claims.roles.clone(),
        is_dev: false,
    })
}

/// Idempotent dev seed: ensures the dev tenant exists with role=owner so the
/// per-request [`ensure_user`] (which assigns role=member to fresh users) is
/// a no-op SELECT for the dev user. Returns the resolved tenant id.
#[cfg(feature = "dev-auth")]
pub async fn seed_dev_world(system: &SystemDb, cfg: &DevConfig) -> Result<RecordId> {
    let tenant_id = upsert_tenant(system, &cfg.tenant_slug, "Dev Tenant").await?;
    let user = upsert_user(
        system,
        "dev://local",
        "dev-user",
        &cfg.user_email,
        Some(&cfg.user_name),
        &tenant_id,
    )
    .await?;
    upsert_membership(system, &user.id, &tenant_id, "owner").await?;
    Ok(tenant_id)
}
