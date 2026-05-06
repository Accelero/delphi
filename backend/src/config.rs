//! Construct a [`Storage`] from environment variables.
//!
//! Centralizing this here means the rest of the codebase never imports a
//! concrete backend; everything goes through [`storage_from_env`].

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::storage::{surreal_from_env, Storage};

pub async fn storage_from_env() -> Result<Arc<dyn Storage>> {
    let backend = std::env::var("STORAGE_BACKEND").unwrap_or_else(|_| "surreal".into());
    match backend.as_str() {
        "surreal" => Ok(surreal_from_env().await?),
        other => Err(Error::UnknownBackend(other.into())),
    }
}
