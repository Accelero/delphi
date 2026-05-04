use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("surrealdb: {0}")]
    Surreal(#[from] surrealdb::Error),

    #[error("environment variable {0} is required but not set")]
    EnvMissing(String),

    #[error("backend returned no record where one was expected")]
    EmptyResult,

    #[error("unknown storage backend: {0}")]
    UnknownBackend(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

pub type Result<T> = std::result::Result<T, Error>;
