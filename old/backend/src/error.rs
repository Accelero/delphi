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

    #[error("http: {0}")]
    Http(#[from] reqwest::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("source adapter `{name}`: {message}")]
    Adapter { name: String, message: String },

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("not implemented: {0}")]
    NotImplemented(String),

    /// `commit_upload` raced or duplicated a `canonical_id` already
    /// present in `document`. The handler turns this into a 422 with
    /// the existing doc id so the SPA can deep-link.
    #[error("canonical_id conflict; existing doc: {existing_doc_id}")]
    CanonicalIdConflict { existing_doc_id: String },
}

pub type Result<T> = std::result::Result<T, Error>;
