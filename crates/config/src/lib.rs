use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    pub bind_addr: SocketAddr,
    pub public_base_url: String,
    pub nats_url: String,
    pub surreal_url: String,
    pub surreal_namespace: String,
    pub surreal_database: String,
    pub surreal_user: String,
    pub surreal_password: String,
}

impl ServiceConfig {
    pub fn from_env(default_port: u16) -> Result<Self> {
        let bind_addr = std::env::var("BIND_ADDR")
            .unwrap_or_else(|_| format!("0.0.0.0:{default_port}"))
            .parse()
            .context("BIND_ADDR must be a socket address")?;

        Ok(Self {
            bind_addr,
            public_base_url: env_or("PUBLIC_BASE_URL", "http://localhost:8080"),
            nats_url: env_or("NATS_URL", "nats://127.0.0.1:4222"),
            surreal_url: env_or("SURREAL_URL", "ws://127.0.0.1:8000"),
            surreal_namespace: env_or("SURREAL_NAMESPACE", "delphi"),
            surreal_database: env_or("SURREAL_DATABASE", "delphi"),
            surreal_user: env_or("SURREAL_USER", "root"),
            surreal_password: env_or("SURREAL_PASSWORD", "root"),
        })
    }
}

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tower_http=info".into()),
        )
        .try_init();
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}
