//! HTTP server: routes, static-SPA fallback, axum boot.

mod chat;
mod discovery;
mod health;
mod stream;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::routing::{get, post};
use axum::{middleware, Extension, Router};
use tower_http::trace::TraceLayer;
use tower_http::services::{ServeDir, ServeFile};
use tracing::info;

use crate::auth::{
    self, AuthConfig, AuthMode, ClaimsExtractor, IdentityDeps, JwtClaimsExtractor,
};
use crate::config::system_db_from_env;
use crate::filter::{IngestFilter, NoopFilter};
use crate::ingestion::{self, NotifyingSink, Pipeline, DEFAULT_BROADCAST_CAPACITY};
use crate::llm::llm_from_env;
use crate::object_store::{self, ObjectStore};
use crate::sources;
use crate::state::AppState;
use crate::storage::{RequestDbPool, Storage};

pub async fn serve(bind: String, static_dir: Option<PathBuf>) -> Result<()> {
    let auth_cfg = AuthConfig::from_env().context("loading auth config")?;
    auth::enforce_production_guard(&auth_cfg.mode).context("auth guard")?;
    auth::print_banner(&auth_cfg.mode);

    let system = system_db_from_env()
        .await
        .context("constructing system DB handle")?;

    // Apply schema on every startup. `schema.surql` is `IF NOT EXISTS`-only
    // (with a few `REMOVE … IF EXISTS` for fields/indexes superseded by
    // tenancy), so this is a no-op when the schema is current.
    system
        .init_schema()
        .await
        .context("applying schema on startup")?;

    let llm = llm_from_env().context("constructing llm client")?;

    // Resolve the default tenant once at startup so the per-request hot
    // path doesn't re-resolve it. Dev mode also seeds its tenant + user
    // here so the first request finds them already in place.
    let default_tenant_slug = auth_cfg.default_tenant_slug().to_string();
    let default_tenant_id =
        auth::resolve_default_tenant(&system, &default_tenant_slug)
            .await
            .context("resolving default tenant")?;

    #[cfg(feature = "dev-auth")]
    if let AuthMode::Dev(dev) = &auth_cfg.mode {
        auth::seed_dev_world(&system, dev)
            .await
            .context("seeding dev tenant/user")?;
    }

    info!(
        mode = auth_cfg.mode.label(),
        tenant = %default_tenant_slug,
        "auth ready"
    );

    let identity_deps = IdentityDeps {
        system: system.clone(),
        default_tenant_slug,
        default_tenant_id: default_tenant_id.clone(),
    };

    let request_pool = Arc::new(RequestDbPool::from_system(&system));

    let (events_tx, _) = tokio::sync::broadcast::channel(DEFAULT_BROADCAST_CAPACITY);

    // Wrap the canonical Pipeline in NotifyingSink so every successful
    // first-time ingest fans out to Discovery-feed SSE subscribers.
    // Both ingest paths (HTTP + scheduler) share this exact sink, so
    // there is no codepath that creates a document silently.
    let pipeline_storage: Arc<dyn Storage> = request_pool.clone();
    let pipeline: Arc<dyn ingestion::IngestSink> = Arc::new(Pipeline::new(pipeline_storage));
    let sink: Arc<dyn ingestion::IngestSink> =
        Arc::new(NotifyingSink::new(pipeline, events_tx.clone()));

    let object_store: Arc<dyn ObjectStore> = object_store::from_url(
        &std::env::var("OBJECT_STORE_URL")
            .unwrap_or_else(|_| "file:///var/lib/delphi/originals".into()),
    )
    .context("constructing object store")?;

    // Slice 2 ships NoopFilter; the real semantic filter is a future
    // drop-in implementing the same `IngestFilter` trait.
    let filter: Arc<dyn IngestFilter> = Arc::new(NoopFilter::new());

    let state = AppState {
        db: request_pool.clone(),
        llm,
        sink: sink.clone(),
        object_store: object_store.clone(),
        events: events_tx,
    };

    // Source-adapter scheduler runs alongside the HTTP server. It shares
    // the same `IngestSink` the HTTP handler uses — internal and external
    // ingestion paths converge on one method. Scheduler ingest is pinned
    // to the tenant identified by `SOURCES_DEFAULT_TENANT_SLUG` (defaults
    // to the auth default tenant).
    let sources_enabled = std::env::var("SOURCES_ENABLED").as_deref() == Ok("true");
    let scheduler = if sources_enabled {
        let scheduler_tenant_slug = std::env::var("SOURCES_DEFAULT_TENANT_SLUG")
            .unwrap_or_else(|_| auth_cfg.default_tenant_slug().to_string());
        let scheduler_tenant_id = auth::resolve_default_tenant(&system, &scheduler_tenant_slug)
            .await
            .context("resolving scheduler tenant")?;

        let registry = sources::default_registry(object_store.clone());
        if registry.is_empty() {
            info!("SOURCES_ENABLED=true but no adapters configured; scheduler idle");
            None
        } else {
            info!(
                tenant = %scheduler_tenant_slug,
                "starting source-adapter scheduler"
            );
            let scheduler_storage: Arc<dyn Storage> = request_pool.clone();
            Some(sources::run_scheduler(
                sink,
                filter,
                scheduler_storage,
                scheduler_tenant_id,
                registry,
            ))
        }
    } else {
        info!("SOURCES_ENABLED is not 'true'; source-adapter scheduler disabled");
        None
    };

    // Today there's only one production extractor. When we want
    // defence-in-depth (backend re-validates the JWT signature against
    // the IdP's JWKS), the choice happens here based on `auth_cfg`.
    let extractor: Arc<dyn ClaimsExtractor> = Arc::new(JwtClaimsExtractor::new());

    let app = build_router(
        state,
        static_dir,
        &auth_cfg.mode,
        identity_deps,
        extractor,
    );

    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("binding to {bind}"))?;
    info!("listening on {bind}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("axum::serve")?;

    if let Some(handle) = scheduler {
        info!("stopping source-adapter scheduler");
        handle.shutdown().await;
    }
    Ok(())
}

/// Build the full axum router. Public so integration tests in
/// `backend/tests/` can construct an app with their own storage / LLM /
/// extractor injections and drive it via `tower::ServiceExt::oneshot`
/// without binding a port.
pub fn build_router(
    state: AppState,
    static_dir: Option<PathBuf>,
    mode: &AuthMode,
    identity_deps: IdentityDeps,
    extractor: Arc<dyn ClaimsExtractor>,
) -> Router {
    // Routes that require an authenticated identity.
    let api_protected = Router::new()
        .route("/api/chat", post(chat::chat))
        .route("/api/auth/me", get(auth::me))
        .route(
            "/api/ingestion/documents",
            post(ingestion::ingest_documents),
        )
        .route("/api/discovery/feed", get(discovery::feed))
        .route("/api/discovery/feed/events", get(discovery::events))
        .route(
            "/api/discovery/items/{key}/read",
            post(discovery::mark_read).delete(discovery::mark_unread),
        );

    // Routes that don't require an authenticated identity.
    let api_public = Router::new().route("/healthz", get(health::healthz));

    // Identity middleware runs on every protected route. In dev mode we
    // additionally prepend `dev_inject_middleware`, which writes a fixed
    // set of `X-Auth-*` headers — making the dev path a strict subset of
    // the production path (same extractor, same upsert, same AuthContext).
    let api_protected = api_protected
        .layer(middleware::from_fn(auth::identity_middleware))
        .layer(Extension(extractor))
        .layer(Extension(identity_deps));

    let api_protected = match mode {
        AuthMode::Header(_) => api_protected,
        #[cfg(feature = "dev-auth")]
        AuthMode::Dev(c) => api_protected
            .layer(middleware::from_fn(auth::dev_inject_middleware))
            .layer(Extension(c.clone())),
    };

    let mut router = api_public
        .merge(api_protected)
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    if let Some(dir) = static_dir {
        let index = dir.join("index.html");
        if dir.is_dir() && index.is_file() {
            info!("serving SPA from {}", dir.display());
            router = router.fallback_service(ServeDir::new(&dir).fallback(ServeFile::new(index)));
        } else {
            tracing::warn!(
                "STATIC_DIR={} is missing or has no index.html; SPA fallback disabled",
                dir.display()
            );
        }
    }

    router
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
