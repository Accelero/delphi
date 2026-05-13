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
    self, service_identity_from_env, validator_from_jwt_access, AuthConfig, AuthMode,
    ClaimsExtractor, IdentityDeps, JwtClaimsExtractor,
};
use crate::config::{jwt_access_from_env, system_db_from_env};
use crate::filter::{IngestFilter, NoopFilter};
use crate::ingestion::{self, NotifyingSink, Pipeline, DEFAULT_BROADCAST_CAPACITY};
use crate::llm::llm_from_env;
use crate::object_store::{self, ObjectStore};
use crate::sources::{self, IngestApiClient};
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

    // Define the `app_session` RECORD access method that SurrealDB will
    // use to validate the IdP JWT on every per-request `db.authenticate`.
    // Configured by env: JWKS URL for production (real IdP) or HS512
    // shared secret for tier-1 dev and tests.
    let jwt_access = jwt_access_from_env().context("loading JWT access config")?;
    system
        .define_jwt_access(&jwt_access)
        .await
        .context("defining JWT access on startup")?;

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

    let request_pool = RequestDbPool::from_env_default()
        .await
        .context("constructing request DB pool")?;

    let identity_deps = IdentityDeps {
        system: system.clone(),
        pool: request_pool.clone(),
        default_tenant_slug,
        default_tenant_id: default_tenant_id.clone(),
    };

    let (events_tx, _) = tokio::sync::broadcast::channel(DEFAULT_BROADCAST_CAPACITY);

    // Scheduler / in-process ingest writes through the privileged
    // `SystemStorage` — it operates above-RBAC by design (cross-tenant
    // by configuration, no user JWT in scope). HTTP ingestion that runs
    // under a user's identity must construct its own per-request
    // pipeline off the `AuthedDb` from the middleware.
    let scheduler_storage: Arc<dyn Storage> = system.storage();
    let scheduler_pipeline: Arc<dyn ingestion::IngestSink> =
        Arc::new(Pipeline::new(scheduler_storage.clone()));
    // NotifyingSink reads back the canonical Document on `Created` so
    // its broadcast carries the same `FeedItem` shape /api/discovery/feed
    // returns. Hand it the same SystemStorage handle the inner pipeline
    // uses — read path crosses no permission boundary (no user JWT in
    // scope here).
    let sink: Arc<dyn ingestion::IngestSink> = Arc::new(NotifyingSink::new(
        scheduler_pipeline,
        scheduler_storage.clone(),
        events_tx.clone(),
    ));

    let object_store: Arc<dyn ObjectStore> = object_store::from_url(
        &std::env::var("OBJECT_STORE_URL")
            .unwrap_or_else(|_| "file:///var/lib/delphi/originals".into()),
    )
    .context("constructing object store")?;

    // Slice 2 ships NoopFilter; the real semantic filter is a future
    // drop-in implementing the same `IngestFilter` trait.
    let filter: Arc<dyn IngestFilter> = Arc::new(NoopFilter::new());

    let state = AppState {
        llm,
        sink: sink.clone(),
        object_store: object_store.clone(),
        events: events_tx,
    };

    // Source-adapter scheduler runs alongside the HTTP server. It used
    // to share an in-process `IngestSink` with the HTTP handler;
    // post-refactor it POSTs to `/api/ingestion/documents` over
    // loopback under a service-identity JWT — same trust boundary,
    // same JWT pipeline, one ingestion API. The handler still uses
    // `state.sink`, so the storage code path is unchanged. Only the
    // identity model moved.
    //
    // Cursor persistence is still scheduler-local (system path) and
    // must agree with the service-identity tenant; both derive from
    // `SOURCES_DEFAULT_TENANT_SLUG`.
    let sources_enabled = std::env::var("SOURCES_ENABLED").as_deref() == Ok("true");
    let scheduler = if sources_enabled {
        let registry = sources::default_registry(object_store.clone());
        if registry.is_empty() {
            info!("SOURCES_ENABLED=true but no adapters configured; scheduler idle");
            None
        } else {
            let scheduler_tenant_slug = std::env::var("SOURCES_DEFAULT_TENANT_SLUG")
                .unwrap_or_else(|_| auth_cfg.default_tenant_slug().to_string());
            let scheduler_tenant_id =
                auth::resolve_default_tenant(&system, &scheduler_tenant_slug)
                    .await
                    .context("resolving scheduler tenant")?;

            let identity = service_identity_from_env("arxiv")
                .context("loading arxiv service identity")?;
            let ingest_url = std::env::var("INGEST_API_URL")
                .unwrap_or_else(|_| default_loopback_url(&bind));
            info!(
                tenant = %scheduler_tenant_slug,
                ingest_url = %ingest_url,
                "starting source-adapter scheduler"
            );
            let ingest = Arc::new(IngestApiClient::new(ingest_url, identity));
            Some(sources::run_scheduler(
                ingest,
                filter,
                scheduler_storage.clone(),
                scheduler_tenant_id,
                registry,
            ))
        }
    } else {
        info!("SOURCES_ENABLED is not 'true'; source-adapter scheduler disabled");
        None
    };

    // Defence-in-depth: re-validate every inbound JWT in-process
    // against the same key material SurrealDB validates against.
    // `validator_from_jwt_access` consumes the same `JwtAccessConfig`
    // we already passed to `define_jwt_access` above — one knob, one
    // policy, two enforcement points.
    let validator = validator_from_jwt_access(&jwt_access);
    let extractor: Arc<dyn ClaimsExtractor> = Arc::new(JwtClaimsExtractor::new(validator));

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

/// Construct the loopback URL the scheduler POSTs to when
/// `INGEST_API_URL` is not set. Reads the port from `BIND_ADDR`
/// (the same value the HTTP server listens on) and pins the host to
/// `127.0.0.1` so the call never leaves the loopback interface.
fn default_loopback_url(bind: &str) -> String {
    let port = bind
        .rsplit_once(':')
        .and_then(|(_, p)| p.parse::<u16>().ok())
        .unwrap_or(8081);
    format!("http://127.0.0.1:{port}")
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
