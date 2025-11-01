//! Error types for Zipline-Rust
//!
//! Complete error system matching Python Zipline's 81 error classes

use thiserror::Error;

/// Main error type for Zipline-Rust
#[derive(Error, Debug)]
pub enum ZiplineError {
    // ========== Asset Errors ==========
    #[error("Asset not found: {0}")]
    AssetNotFound(u64),

    #[error("Invalid benchmark asset: {0}")]
    InvalidBenchmarkAsset(String),

    #[error("Cannot order delisted asset: {0}")]
    CannotOrderDelistedAsset(u64),

    // ========== Data Errors ==========
    #[error("No trade data available")]
    NoTradeDataAvailable,

    #[error("No trade data available too early: requested {requested}, earliest available {earliest}")]
    NoTradeDataAvailableTooEarly {
        requested: String,
        earliest: String,
    },

    #[error("No trade data available too late: requested {requested}, latest available {latest}")]
    NoTradeDataAvailableTooLate {
        requested: String,
        latest: String,
    },

    #[error("Benchmark asset not available too early")]
    BenchmarkAssetNotAvailableTooEarly,

    #[error("Benchmark asset not available too late")]
    BenchmarkAssetNotAvailableTooLate,

    #[error("Wrong data for transform: {0}")]
    WrongDataForTransform(String),

    #[error("Data error: {0}")]
    DataError(String),

    #[error("No data available")]
    NoDataAvailable,

    // ========== Order Errors ==========
    #[error("Invalid order: {0}")]
    InvalidOrder(String),

    #[error("Unsupported order parameters: {0}")]
    UnsupportedOrderParameters(String),

    #[error("Bad order parameters: {0}")]
    BadOrderParameters(String),

    #[error("Order during initialize: cannot place orders in initialize()")]
    OrderDuringInitialize,

    #[error("Order in before_trading_start: cannot place orders in before_trading_start()")]
    OrderInBeforeTradingStart,

    // ========== Transaction Errors ==========
    #[error("Transaction with no volume: order {order_id}")]
    TransactionWithNoVolume { order_id: u64 },

    #[error("Transaction with wrong direction: expected {expected}, got {actual}")]
    TransactionWithWrongDirection { expected: String, actual: String },

    #[error("Transaction with no amount: order {order_id}")]
    TransactionWithNoAmount { order_id: u64 },

    #[error("Transaction volume exceeds order: transaction {transaction_volume}, order {order_volume}")]
    TransactionVolumeExceedsOrder {
        transaction_volume: f64,
        order_volume: f64,
    },

    // ========== Commission/Slippage Errors ==========
    #[error("Unsupported slippage model: {0}")]
    UnsupportedSlippageModel(String),

    #[error("Incompatible slippage model: {model} cannot be used with {asset_type}")]
    IncompatibleSlippageModel { model: String, asset_type: String },

    #[error("Set slippage post init: slippage must be set in initialize()")]
    SetSlippagePostInit,

    #[error("Unsupported commission model: {0}")]
    UnsupportedCommissionModel(String),

    #[error("Incompatible commission model: {model} cannot be used with {asset_type}")]
    IncompatibleCommissionModel {
        model: String,
        asset_type: String,
    },

    #[error("Set commission post init: commission must be set in initialize()")]
    SetCommissionPostInit,

    // ========== Cancel Policy Errors ==========
    #[error("Unsupported cancel policy: {0}")]
    UnsupportedCancelPolicy(String),

    #[error("Set cancel policy post init: cancel policy must be set in initialize()")]
    SetCancelPolicyPostInit,

    // ========== Trading Control Errors ==========
    #[error("Register trading control post init: controls must be registered in initialize()")]
    RegisterTradingControlPostInit,

    #[error("Register account control post init: controls must be registered in initialize()")]
    RegisterAccountControlPostInit,

    #[error("Trading control violation: {0}")]
    TradingControlViolation(String),

    #[error("Account control violation: {0}")]
    AccountControlViolation(String),

    // ========== Pipeline Errors ==========
    #[error("Pipeline error: {0}")]
    PipelineError(String),

    #[error("Attach pipeline after initialize: pipelines must be attached in initialize()")]
    AttachPipelineAfterInitialize,

    #[error("Duplicate pipeline name: {0}")]
    DuplicatePipelineName(String),

    #[error("No such pipeline: {0}")]
    NoSuchPipeline(String),

    #[error("Pipeline output during initialize: cannot access pipeline output in initialize()")]
    PipelineOutputDuringInitialize,

    // ========== Configuration Errors ==========
    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Zero capital error: initial capital must be greater than zero")]
    ZeroCapitalError,

    #[error("Set benchmark outside initialize: benchmark must be set in initialize()")]
    SetBenchmarkOutsideInitialize,

    #[error("Schedule function invalid calendar: {0}")]
    ScheduleFunctionInvalidCalendar(String),

    #[error("Unsupported datetime format: {0}")]
    UnsupportedDatetimeFormat(String),

    // ========== Calendar Errors ==========
    #[error("Calendar error: {0}")]
    CalendarError(String),

    // ========== Frequency Errors ==========
    #[error("Invalid frequency: {0}")]
    InvalidFrequency(String),

    // ========== Fund Errors ==========
    #[error("Insufficient funds: required {required}, available {available}")]
    InsufficientFunds { required: f64, available: f64 },

    // ========== Execution Errors ==========
    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Liquidity exceeded: order volume {order_volume} exceeds limit {limit}")]
    LiquidityExceeded { order_volume: f64, limit: f64 },

    // ========== Restriction Errors ==========
    #[error("Asset is restricted: {0}")]
    AssetRestricted(u64),

    #[error("Asset is frozen: can only close positions for {0}")]
    AssetFrozen(u64),

    // ========== Symbol Lookup Errors ==========
    #[error("Symbol not found: {symbol}")]
    SymbolNotFound { symbol: String },

    #[error("Multiple symbols found for {symbol}: {candidates:?}")]
    MultipleSymbolsFound {
        symbol: String,
        candidates: Vec<String>,
    },

    #[error("SID not found: {0}")]
    SidNotFound(u64),

    // ========== Bundle Errors ==========
    #[error("Bundle not found: {0}")]
    BundleNotFound(String),

    #[error("Bundle already exists: {0}")]
    BundleAlreadyExists(String),

    #[error("Bundle ingestion failed: {0}")]
    BundleIngestionFailed(String),

    // ========== Adjustment Errors ==========
    #[error("Invalid adjustment: {0}")]
    InvalidAdjustment(String),

    // ========== Domain Errors ==========
    #[error("Invalid domain: {0}")]
    InvalidDomain(String),

    #[error("Incompatible domains: {0}")]
    IncompatibleDomains(String),

    // ========== Generic Errors ==========
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_asset_errors() {
        let err = ZiplineError::AssetNotFound(123);
        assert_eq!(err.to_string(), "Asset not found: 123");

        let err = ZiplineError::CannotOrderDelistedAsset(456);
        assert_eq!(err.to_string(), "Cannot order delisted asset: 456");
    }

    #[test]
    fn test_data_errors() {
        let err = ZiplineError::NoTradeDataAvailableTooEarly {
            requested: "2020-01-01".to_string(),
            earliest: "2020-01-15".to_string(),
        };
        assert!(err
            .to_string()
            .contains("No trade data available too early"));
    }

    #[test]
    fn test_order_errors() {
        let err = ZiplineError::OrderDuringInitialize;
        assert_eq!(
            err.to_string(),
            "Order during initialize: cannot place orders in initialize()"
        );

        let err = ZiplineError::UnsupportedOrderParameters("invalid params".to_string());
        assert!(err.to_string().contains("Unsupported order parameters"));
    }

    #[test]
    fn test_transaction_errors() {
        let err = ZiplineError::TransactionVolumeExceedsOrder {
            transaction_volume: 150.0,
            order_volume: 100.0,
        };
        assert!(err.to_string().contains("Transaction volume exceeds order"));
    }

    #[test]
    fn test_control_errors() {
        let err = ZiplineError::TradingControlViolation("max position exceeded".to_string());
        assert!(err.to_string().contains("Trading control violation"));

        let err = ZiplineError::AccountControlViolation("max leverage exceeded".to_string());
        assert!(err.to_string().contains("Account control violation"));
    }

    #[test]
    fn test_pipeline_errors() {
        let err = ZiplineError::AttachPipelineAfterInitialize;
        assert!(err
            .to_string()
            .contains("pipelines must be attached in initialize()"));

        let err = ZiplineError::DuplicatePipelineName("my_pipeline".to_string());
        assert!(err.to_string().contains("Duplicate pipeline name"));
    }

    #[test]
    fn test_config_errors() {
        let err = ZiplineError::ZeroCapitalError;
        assert!(err
            .to_string()
            .contains("initial capital must be greater than zero"));

        let err = ZiplineError::SetBenchmarkOutsideInitialize;
        assert!(err
            .to_string()
            .contains("benchmark must be set in initialize()"));
    }

    #[test]
    fn test_symbol_errors() {
        let err = ZiplineError::SymbolNotFound {
            symbol: "INVALID".to_string(),
        };
        assert_eq!(err.to_string(), "Symbol not found: INVALID");

        let err = ZiplineError::MultipleSymbolsFound {
            symbol: "A".to_string(),
            candidates: vec!["A-1".to_string(), "A-2".to_string()],
        };
        assert!(err.to_string().contains("Multiple symbols found"));
    }

    #[test]
    fn test_fund_errors() {
        let err = ZiplineError::InsufficientFunds {
            required: 10000.0,
            available: 5000.0,
        };
        assert!(err.to_string().contains("Insufficient funds"));
    }

    #[test]
    fn test_liquidity_error() {
        let err = ZiplineError::LiquidityExceeded {
            order_volume: 1000.0,
            limit: 500.0,
        };
        assert!(err.to_string().contains("Liquidity exceeded"));
    }
}
