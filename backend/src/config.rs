//! Construct backend services from environment variables.
//!
//! Today the only thing here is the storage factory — kept centralized so
//! the rest of the codebase never imports a concrete backend.

use std::sync::Arc;

use crate::error::Result;
use crate::storage::SystemDb;

/// Construct the privileged [`SystemDb`] from environment. Used by the
/// bin (`api::serve`) and the admin CLI.
pub async fn system_db_from_env() -> Result<Arc<SystemDb>> {
    Ok(Arc::new(SystemDb::from_env().await?))
}
