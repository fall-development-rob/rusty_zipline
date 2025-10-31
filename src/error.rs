//! Error types for Zipline-Rust

use thiserror::Error;

/// Main error type for Zipline-Rust
#[derive(Error, Debug)]
pub enum ZiplineError {
    #[error("Asset not found: {0}")]
    AssetNotFound(String),

    #[error("Invalid order: {0}")]
    InvalidOrder(String),

    #[error("Insufficient funds: required {required}, available {available}")]
    InsufficientFunds { required: f64, available: f64 },

    #[error("Calendar error: {0}")]
    CalendarError(String),

    #[error("Data error: {0}")]
    DataError(String),

    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

/// Result type alias for Zipline-Rust operations
pub type Result<T> = std::result::Result<T, ZiplineError>;
