//! HTTP server: routes, static-SPA fallback, axum boot.

mod chat;
mod health;
mod stream;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::error_handling::HandleErrorLayer;
use axum::http::Uri;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{middleware, Extension, Router};
use axum_oidc::error::MiddlewareError;
use axum_oidc::{OidcAuthLayer, OidcLoginLayer};
use time::Duration;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
use tower_http::services::{ServeDir, ServeFile};
use tower_sessions::cookie::SameSite;
use tower_sessions::service::SignedCookie;
use tower_sessions::{Expiry, SessionManagerLayer};
use tower_sessions_core::ExpiredDeletion;
use tracing::info;

use crate::auth::config::{AuthConfig, AuthMode, OidcConfig};
use crate::auth::context::AuthContext;
use crate::auth::oidc::{ExtraClaims, OidcDeps};
use crate::auth::store::SurrealSessionStore;
use crate::auth::{bootstrap, guard};
use crate::config::surreal_from_env;
use crate::llm::llm_from_env;
use crate::state::{AppState, AuthAppState};

/// How often we sweep expired sessions out of the Surreal `session` table.
const SESSION_CLEANUP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5 * 60);
/// Cookie session lifetime. Inactivity-based — bumped on every request.
const SESSION_INACTIVITY: Duration = Duration::days(14);

pub async fn serve(bind: String, static_dir: Option<PathBuf>) -> Result<()> {
    let auth_cfg = AuthConfig::from_env().context("loading auth config")?;
    guard::enforce_production_guard(&auth_cfg.mode).context("auth guard")?;
    guard::print_banner(&auth_cfg.mode);

    let surreal = surreal_from_env()
        .await
        .context("constructing storage backend")?;
    let llm = llm_from_env().context("constructing llm client")?;

    // Background expired-session cleanup. Spawns and runs forever; the
    // handle is detached because the process exits with the server.
    let session_store = SurrealSessionStore::new(surreal.db().clone());
    {
        let store = session_store.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(SESSION_CLEANUP_INTERVAL);
            interval.tick().await; // first tick fires immediately; skip
            loop {
                interval.tick().await;
                if let Err(e) = store.delete_expired().await {
                    tracing::warn!(error = %e, "session cleanup failed");
                }
            }
        });
    }

    let session_layer = SessionManagerLayer::new(session_store)
        .with_name("delphi.sid")
        .with_http_only(true)
        .with_same_site(SameSite::Lax)
        .with_secure(auth_cfg.secure_cookies)
        .with_signed(auth_cfg.session_key.clone())
        .with_expiry(Expiry::OnInactivity(SESSION_INACTIVITY));

    // Mode-specific bootstrap: in dev, seed the dev tenant/user once and
    // stash the resulting AuthContext for the injection middleware. In OIDC,
    // resolve / create the default tenant and prepare the OidcDeps for the
    // lazy-upsert middleware.
    let (auth_state, dev_ctx, oidc_deps) = match &auth_cfg.mode {
        #[cfg(feature = "dev-auth")]
        AuthMode::Dev(c) => {
            let ctx = bootstrap::seed_dev_world(surreal.db(), c)
                .await
                .context("seeding dev tenant/user")?;
            let auth_state = AuthAppState {
                mode_label: "dev",
                default_tenant_id: Some(ctx.tenant_id.clone()),
                post_login_redirect: "/".into(),
            };
            (auth_state, Some(Arc::new(ctx)), None)
        }
        AuthMode::Oidc(c) => {
            let default_tenant_id =
                bootstrap::resolve_default_tenant(surreal.db(), &c.default_tenant_slug)
                    .await
                    .context("resolving default tenant")?;
            let auth_state = AuthAppState {
                mode_label: "oidc",
                default_tenant_id: Some(default_tenant_id.clone()),
                post_login_redirect: c.post_login_redirect.clone(),
            };
            let deps = OidcDeps {
                db: surreal.db().clone(),
                config: Arc::new(c.clone()),
                default_tenant_id: Arc::new(default_tenant_id),
            };
            (auth_state, None, Some(deps))
        }
    };

    let state = AppState {
        storage: surreal,
        llm,
        auth: Arc::new(auth_state),
    };

    let app = build_router(
        state,
        static_dir,
        auth_cfg,
        session_layer,
        dev_ctx,
        oidc_deps,
    )
    .await?;

    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("binding to {bind}"))?;
    info!("listening on {bind}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("axum::serve")?;
    Ok(())
}

async fn build_router(
    state: AppState,
    static_dir: Option<PathBuf>,
    auth_cfg: AuthConfig,
    session_layer: SessionManagerLayer<SurrealSessionStore, SignedCookie>,
    #[cfg_attr(not(feature = "dev-auth"), allow(unused_variables))] dev_ctx: Option<Arc<AuthContext>>,
    oidc_deps: Option<OidcDeps>,
) -> Result<Router> {
    // Routes that require an authenticated identity.
    let api_protected = Router::new()
        .route("/api/chat", post(chat::chat))
        .route("/api/auth/me", get(crate::auth::routes::me))
        .route("/api/auth/logout", post(crate::auth::routes::logout));

    // Routes that don't require an authenticated identity.
    let api_public = Router::new().route("/healthz", get(health::healthz));

    // Dedicated to triggering the OIDC redirect chain. On the OIDC path it
    // gets wrapped by OidcLoginLayer (forces auth → IdP redirect → callback);
    // on the dev path it just 302s to "/".
    let api_login = Router::new().route("/api/auth/login", get(crate::auth::routes::login));

    let (api_protected, api_login_branch, top_layer): (
        Router<AppState>,
        Router<AppState>,
        Option<OidcAuthLayer<ExtraClaims>>,
    ) = match &auth_cfg.mode {
        AuthMode::Oidc(c) => {
            let deps = oidc_deps.expect("oidc_deps built in OIDC mode");
            let auth_layer = build_oidc_auth_layer(c).await?;

            // Tower-style stack with HandleErrorLayer in front so
            // OidcLoginLayer's MiddlewareError gets mapped to a Response
            // (axum routers require Infallible).
            let login_stack = || {
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(handle_oidc_error))
                    .layer(OidcLoginLayer::<ExtraClaims>::new())
            };

            let api_protected = api_protected
                .layer(middleware::from_fn(crate::auth::oidc::ensure_user_ctx))
                .layer(login_stack())
                .layer(Extension(deps));
            let api_login_branch = api_login.layer(login_stack());
            (api_protected, api_login_branch, Some(auth_layer))
        }
        #[cfg(feature = "dev-auth")]
        AuthMode::Dev(_) => {
            let ctx = dev_ctx.expect("dev_ctx built in dev mode");
            let api_protected = api_protected
                .layer(middleware::from_fn(crate::auth::dev::dev_inject_middleware))
                .layer(Extension(ctx));
            (api_protected, api_login, None)
        }
    };

    let mut router = api_public
        .merge(api_protected)
        .merge(api_login_branch)
        .with_state(state);

    // OidcAuthLayer wraps everything so that `OidcClaims` can populate even
    // on /api/auth/me (a protected route uses the extension; on others the
    // extension is simply absent).
    if let Some(auth_layer) = top_layer {
        router = router.layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(handle_oidc_error))
                .layer(auth_layer),
        );
    }

    router = router
        .layer(session_layer)
        .layer(TraceLayer::new_for_http());

    if let Some(dir) = static_dir {
        let index = dir.join("index.html");
        if dir.is_dir() && index.is_file() {
            info!("serving SPA from {}", dir.display());
            router = router.fallback_service(
                ServeDir::new(&dir).fallback(ServeFile::new(index)),
            );
        } else {
            tracing::warn!(
                "STATIC_DIR={} is missing or has no index.html; SPA fallback disabled",
                dir.display()
            );
        }
    }

    Ok(router)
}

async fn handle_oidc_error(e: MiddlewareError) -> axum::response::Response {
    e.into_response()
}

async fn build_oidc_auth_layer(c: &OidcConfig) -> Result<OidcAuthLayer<ExtraClaims>> {
    let base_url: Uri = c
        .application_base_url
        .parse()
        .with_context(|| format!("parsing OIDC_APPLICATION_BASE_URL={}", c.application_base_url))?;
    let layer = OidcAuthLayer::<ExtraClaims>::discover_client(
        base_url,
        c.issuer.clone(),
        c.client_id.clone(),
        c.client_secret.clone(),
        c.scopes.clone(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("OIDC discovery failed: {e}"))?;
    Ok(layer)
}

async fn shutdown_signal() {
    use tokio::signal;
    let ctrl_c = async {
        signal::ctrl_c().await.expect("install ctrl_c handler");
    };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    info!("shutdown signal received");
}

// Stub kept for downstream callers — not used directly anymore but useful
// if/when we want a router constructor without auth (e.g., tests).
#[allow(dead_code)]
fn unused_layer_anchor<L>(_: L) {}
