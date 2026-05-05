//! Construct a [`Storage`] from environment variables.
//!
//! Centralizing this here means the rest of the codebase never imports a
//! concrete backend; everything goes through [`storage_from_env`].

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::storage::{Storage, SurrealStorage};

pub async fn storage_from_env() -> Result<Arc<dyn Storage>> {
    Ok(surreal_from_env().await?)
}

/// Construct a [`SurrealStorage`] directly. The auth subsystem needs the
/// concrete type so it can borrow the underlying `Surreal<Client>` for the
/// session store and bootstrap upserts (single shared connection).
pub async fn surreal_from_env() -> Result<Arc<SurrealStorage>> {
    let backend = std::env::var("STORAGE_BACKEND").unwrap_or_else(|_| "surreal".into());
    match backend.as_str() {
        "surreal" => {
            let url = env_or("SURREAL_URL", "ws://surrealdb:8000/rpc");
            let user = env_or("SURREAL_USER", "root");
            let password = env_or("SURREAL_PASS", "root");
            let namespace = env_or("SURREAL_NS", "delphi");
            let database = env_or("SURREAL_DB", "main");
            let endpoint = parse_endpoint(&url)?;
            let storage = SurrealStorage::connect(
                &endpoint, &user, &password, &namespace, &database,
            )
            .await?;
            Ok(Arc::new(storage))
        }
        other => Err(Error::UnknownBackend(other.into())),
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

/// SurrealDB's `Ws` engine wants `host:port`. Strip scheme and trailing path.
fn parse_endpoint(url: &str) -> Result<String> {
    let stripped = url
        .trim_start_matches("ws://")
        .trim_start_matches("wss://")
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let host_port = stripped.split('/').next().unwrap_or(stripped);
    if host_port.is_empty() {
        return Err(Error::InvalidConfig(format!(
            "could not parse SURREAL_URL={url}"
        )));
    }
    Ok(host_port.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_endpoint_strips_schemes() {
        assert_eq!(parse_endpoint("ws://surrealdb:8000/rpc").unwrap(), "surrealdb:8000");
        assert_eq!(parse_endpoint("http://localhost:8000").unwrap(), "localhost:8000");
        assert_eq!(parse_endpoint("localhost:8000").unwrap(), "localhost:8000");
        assert_eq!(parse_endpoint("wss://db.example.com:443/rpc").unwrap(), "db.example.com:443");
    }
}
