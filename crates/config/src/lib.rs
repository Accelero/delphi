use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    pub bind_addr: SocketAddr,
    pub public_base_url: String,
    pub nats_url: String,
    pub database_url: String,
    pub pg_max_connections: u32,
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
            database_url: env_or("DATABASE_URL", "postgres://delphi:delphi@127.0.0.1:5432/delphi"),
            pg_max_connections: env_u32("PG_MAX_CONNECTIONS", 10),
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

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}
