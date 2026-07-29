use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("another daemon is already using {0}")]
    AlreadyRunning(PathBuf),

    #[error("daemon is not reachable at {endpoint}: {reason}")]
    DaemonUnavailable { endpoint: String, reason: String },

    #[error("storage error: {0}")]
    Storage(String),

    #[error("critical background task failed: {0}")]
    TaskFailed(String),

    #[error("RPC error {code}: {message}")]
    Rpc { code: i64, message: String },

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, AppError>;
