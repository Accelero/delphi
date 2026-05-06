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

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use surrealdb::engine::any::Any;
use surrealdb::{RecordId, Surreal};

use super::claims::Claims;
use super::context::AuthContext;
#[cfg(feature = "dev-auth")]
use super::config::DevConfig;

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

#[derive(Debug, Deserialize)]
struct TenantRow {
    id: RecordId,
}

async fn upsert_tenant(db: &Surreal<Any>, slug: &str, name: &str) -> Result<RecordId> {
    let mut r = db
        .query("SELECT id FROM tenant WHERE slug = $slug LIMIT 1")
        .bind(("slug", slug.to_string()))
        .await
        .context("select tenant by slug")?;
    let existing: Option<IdRow> = r.take(0).context("decode tenant select")?;
    if let Some(t) = existing {
        return Ok(t.id);
    }
    let mut r = db
        .query("CREATE tenant CONTENT { slug: $slug, name: $name } RETURN id")
        .bind(("slug", slug.to_string()))
        .bind(("name", name.to_string()))
        .await
        .context("create tenant")?;
    let row: Option<IdRow> = r.take(0).context("decode tenant create")?;
    row.map(|x| x.id)
        .ok_or_else(|| anyhow!("tenant CREATE returned no row"))
}

async fn upsert_user(
    db: &Surreal<Any>,
    iss: &str,
    sub: &str,
    email: &str,
    display_name: Option<&str>,
) -> Result<UserRow> {
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
        // Best-effort refresh of cached attrs. Not fatal on failure.
        let _ = db
            .query(
                "UPDATE $rid SET email = $email, display_name = $name, last_seen_at = time::now()",
            )
            .bind(("rid", u.id.clone()))
            .bind(("email", email.to_string()))
            .bind(("name", display_name.map(|s| s.to_string())))
            .await;
        return Ok(u);
    }
    let mut r = db
        .query(
            "CREATE app_user CONTENT { \
                iss: $iss, sub: $sub, email: $email, display_name: $name \
             } RETURN id, iss, sub, email, display_name",
        )
        .bind(("iss", iss.to_string()))
        .bind(("sub", sub.to_string()))
        .bind(("email", email.to_string()))
        .bind(("name", display_name.map(|s| s.to_string())))
        .await
        .context("create app_user")?;
    let row: Option<UserRow> = r.take(0).context("decode app_user create")?;
    row.ok_or_else(|| anyhow!("app_user CREATE returned no row"))
}

async fn upsert_membership(
    db: &Surreal<Any>,
    user: &RecordId,
    tenant: &RecordId,
    role: &str,
) -> Result<()> {
    let mut r = db
        .query("SELECT id FROM membership WHERE user = $u AND tenant = $t LIMIT 1")
        .bind(("u", user.clone()))
        .bind(("t", tenant.clone()))
        .await
        .context("select membership")?;
    let existing: Option<IdRow> = r.take(0).context("decode membership select")?;
    if existing.is_some() {
        return Ok(());
    }
    db.query("CREATE membership CONTENT { user: $u, tenant: $t, role: $role }")
        .bind(("u", user.clone()))
        .bind(("t", tenant.clone()))
        .bind(("role", role.to_string()))
        .await
        .context("create membership")?
        .check()
        .context("membership create check")?;
    Ok(())
}

/// Resolve the default tenant by slug, creating it if missing. Called once
/// at startup so the per-request hot path can skip this query.
pub async fn resolve_default_tenant(db: &Surreal<Any>, slug: &str) -> Result<RecordId> {
    upsert_tenant(db, slug, "Default").await
}

/// Per-request user upsert. Looks up `(iss, sub)`; creates the user +
/// membership against the resolved tenant if missing.
///
/// Tenant resolution: claim slug exists → use it; unknown non-default slug
/// → fall back to default with a warning (we do not auto-create arbitrary
/// tenants — self-serve org creation is an onboarding-flow concern, not
/// something to derive from a claim).
pub async fn ensure_user(
    db: &Surreal<Any>,
    claims: &Claims,
    default_tenant_slug: &str,
    default_tenant_id: &RecordId,
) -> Result<AuthContext> {
    let tenant_id = match claims.tenant_slug.as_deref() {
        Some(slug) if slug == default_tenant_slug => default_tenant_id.clone(),
        Some(slug) => {
            let mut r = db
                .query("SELECT id FROM tenant WHERE slug = $slug LIMIT 1")
                .bind(("slug", slug.to_string()))
                .await
                .context("select tenant by claim slug")?;
            let row: Option<TenantRow> = r.take(0).context("decode tenant-by-claim select")?;
            match row {
                Some(t) => t.id,
                None => {
                    tracing::warn!(
                        tenant = slug,
                        "tenant claim references unknown tenant; falling back to default"
                    );
                    default_tenant_id.clone()
                }
            }
        }
        None => default_tenant_id.clone(),
    };

    let user = upsert_user(
        db,
        &claims.iss,
        &claims.sub,
        &claims.email,
        claims.display_name.as_deref(),
    )
    .await?;
    upsert_membership(db, &user.id, &tenant_id, "member").await?;

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
pub async fn seed_dev_world(db: &Surreal<Any>, cfg: &DevConfig) -> Result<RecordId> {
    let tenant_id = upsert_tenant(db, &cfg.tenant_slug, "Dev Tenant").await?;
    let user = upsert_user(db, "dev://local", "dev-user", &cfg.user_email, Some(&cfg.user_name))
        .await?;
    upsert_membership(db, &user.id, &tenant_id, "owner").await?;
    Ok(tenant_id)
}
