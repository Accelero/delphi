//! Tenant / user / membership upserts.
//!
//! Two callers:
//!
//! - [`seed_dev_world`] — runs once at startup in dev mode to ensure the
//!   configured dev tenant + user + ownership exist. Idempotent.
//! - [`ensure_oidc_user`] — runs lazily in OIDC mode on each authenticated
//!   request: SELECT on `(iss, sub)`, fall through to CREATE if missing,
//!   resolve tenant by slug (auto-create only the configured default
//!   tenant — we never auto-create arbitrary tenants from claims).

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use surrealdb::engine::remote::ws::Client;
use surrealdb::{RecordId, Surreal};

use crate::auth::context::AuthContext;
#[cfg(feature = "dev-auth")]
use crate::auth::config::DevConfig;

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

/// Ensure a tenant with the given slug exists. Returns its RecordId.
async fn upsert_tenant(
    db: &Surreal<Client>,
    slug: &str,
    name: &str,
) -> Result<RecordId> {
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
    row.map(|x| x.id).ok_or_else(|| anyhow!("tenant CREATE returned no row"))
}

/// SELECT-then-CREATE for `app_user`. Updates email/display_name on the
/// existing row (best-effort) so cached fields stay current.
async fn upsert_user(
    db: &Surreal<Client>,
    iss: &str,
    sub: &str,
    email: &str,
    display_name: Option<&str>,
) -> Result<UserRow> {
    let mut r = db
        .query("SELECT id, iss, sub, email, display_name FROM app_user \
                WHERE iss = $iss AND sub = $sub LIMIT 1")
        .bind(("iss", iss.to_string()))
        .bind(("sub", sub.to_string()))
        .await
        .context("select app_user")?;
    let existing: Option<UserRow> = r.take(0).context("decode app_user select")?;
    if let Some(u) = existing {
        // Best-effort refresh of cached attrs. Not fatal on failure.
        let _ = db
            .query("UPDATE $rid SET email = $email, display_name = $name, last_seen_at = time::now()")
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
    db: &Surreal<Client>,
    user: &RecordId,
    tenant: &RecordId,
    role: &str,
) -> Result<()> {
    let mut r = db
        .query(
            "SELECT id FROM membership WHERE user = $u AND tenant = $t LIMIT 1",
        )
        .bind(("u", user.clone()))
        .bind(("t", tenant.clone()))
        .await
        .context("select membership")?;
    let existing: Option<IdRow> = r.take(0).context("decode membership select")?;
    if existing.is_some() {
        return Ok(());
    }
    db.query(
        "CREATE membership CONTENT { user: $u, tenant: $t, role: $role }",
    )
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
/// at startup so the OIDC lazy-upsert path can fall back to it without
/// running this query on the hot path.
pub async fn resolve_default_tenant(db: &Surreal<Client>, slug: &str) -> Result<RecordId> {
    upsert_tenant(db, slug, "Default").await
}

/// Idempotent dev seed. Returns a fully-built [`AuthContext`] that the
/// dev-injection middleware can clone into every request's extensions.
#[cfg(feature = "dev-auth")]
pub async fn seed_dev_world(db: &Surreal<Client>, cfg: &DevConfig) -> Result<AuthContext> {
    let tenant_id = upsert_tenant(db, &cfg.tenant_slug, "Dev Tenant").await?;
    let user = upsert_user(
        db,
        "dev://local",
        "dev-user",
        &cfg.user_email,
        Some(&cfg.user_name),
    )
    .await?;
    upsert_membership(db, &user.id, &tenant_id, "owner").await?;
    Ok(AuthContext {
        user_id: user.id,
        tenant_id,
        email: user.email,
        display_name: user.display_name,
        iss: "dev://local".into(),
        sub: "dev-user".into(),
        is_dev: true,
    })
}

/// OIDC lazy upsert. Looks up `(iss, sub)`; creates the user + membership
/// against the resolved tenant if missing.
///
/// `tenant_slug` is whatever came out of the configured tenant claim;
/// `default_tenant_slug` is the fallback. We auto-create only the default
/// tenant — non-default slugs that don't exist fall back to default with
/// a warning. Self-serve org creation belongs to the (future) onboarding
/// flow, not here.
pub async fn ensure_oidc_user(
    db: &Surreal<Client>,
    iss: &str,
    sub: &str,
    email: &str,
    display_name: Option<&str>,
    tenant_slug: Option<&str>,
    default_tenant_slug: &str,
    default_tenant_id: &RecordId,
) -> Result<AuthContext> {
    let tenant_id = match tenant_slug {
        Some(slug) if slug == default_tenant_slug => default_tenant_id.clone(),
        Some(slug) => {
            // Look up existing — do NOT auto-create.
            let mut r = db
                .query("SELECT id FROM tenant WHERE slug = $slug LIMIT 1")
                .bind(("slug", slug.to_string()))
                .await
                .context("select tenant by claim slug")?;
            let row: Option<TenantRow> = r.take(0).ok().flatten();
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

    let user = upsert_user(db, iss, sub, email, display_name).await?;
    upsert_membership(db, &user.id, &tenant_id, "member").await?;

    Ok(AuthContext {
        user_id: user.id,
        tenant_id,
        email: user.email,
        display_name: user.display_name,
        iss: iss.to_string(),
        sub: sub.to_string(),
        is_dev: false,
    })
}
