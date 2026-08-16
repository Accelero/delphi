use thiserror::Error;

/// Startup and wiring failures. Runtime failures are reported through the port
/// error types in `delphi-document-app`, which the use cases understand.
#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("NATS connection failed: {0}")]
    Connect(String),
    #[error("NATS topology setup failed: {0}")]
    Topology(String),
    #[error("Postgres error: {0}")]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Migrate(#[from] crate::migrate::MigrateError),
    #[error(transparent)]
    Config(#[from] crate::config::ConfigError),
}
