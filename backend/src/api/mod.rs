//! HTTP server: routes, static-SPA fallback, axum boot.

mod chat;
mod health;
mod stream;

use std::path::PathBuf;

use anyhow::{Context, Result};
use axum::Router;
use axum::routing::{get, post};
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::config::storage_from_env;
use crate::llm::llm_from_env;
use crate::state::AppState;

/// Build the axum router. Static SPA is mounted as a fallback so deep
/// links (e.g., /papers/abc123) return index.html for client-side routing.
pub fn build_router(state: AppState, static_dir: Option<PathBuf>) -> Router {
    let mut router = Router::new()
        .route("/healthz", get(health::healthz))
        .route("/api/chat", post(chat::chat))
        // future endpoints:
        // .route("/api/feed", get(feed::list))
        // .route("/api/search/vector", post(search::vector))
        // .route("/api/documents/:id", get(documents::get))
        .with_state(state)
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

    router
}

pub async fn serve(bind: String, static_dir: Option<PathBuf>) -> Result<()> {
    let storage = storage_from_env()
        .await
        .context("constructing storage backend")?;
    let llm = llm_from_env().context("constructing llm client")?;
    let state = AppState { storage, llm };

    let app = build_router(state, static_dir);

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
