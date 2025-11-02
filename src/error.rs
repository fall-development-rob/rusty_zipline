//! Error types for Zipline-Rust
//!
//! Complete error system matching Python Zipline's 81 error classes

use chrono::{DateTime, Utc};
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

    // P0 Data Availability Errors
    #[error("History window starts before first available data for asset {asset}: requested {requested_start}, first available: {first_available}")]
    HistoryWindowBeforeFirstData {
        asset: u64,
        requested_start: DateTime<Utc>,
        first_available: DateTime<Utc>,
    },

    #[error("Asset {asset} does not exist at {requested_dt}. Asset trading dates: {start_date:?} to {end_date:?}")]
    AssetNonExistent {
        asset: u64,
        requested_dt: DateTime<Utc>,
        start_date: Option<DateTime<Utc>>,
        end_date: Option<DateTime<Utc>>,
    },

    #[error("Pricing data not loaded for assets: {assets:?}. Call load_pricing() before accessing data.")]
    PricingDataNotLoaded {
        assets: Vec<u64>,
    },

    #[error("Cannot request data beyond current simulation time. Current: {current_dt}, requested: {requested_dt}")]
    NoFurtherData {
        current_dt: DateTime<Utc>,
        requested_dt: DateTime<Utc>,
    },

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

    // P0 Order Management Errors
    #[error("Order ID {order_id} not found in order tracker. Order may have been filled or cancelled.")]
    OrderIdNotFound {
        order_id: uuid::Uuid,
    },

    #[error("Cannot place order after session end. Session ended at {session_end}, order attempted at {attempted_at}")]
    OrderAfterSessionEnd {
        session_end: DateTime<Utc>,
        attempted_at: DateTime<Utc>,
    },

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

    // P0 Trading Control Errors
    #[error("Max position size exceeded for asset {asset} ({symbol}): attempted order {attempted_order} shares, max shares: {max_shares:?}, max notional: {max_notional:?}")]
    MaxPositionSizeExceeded {
        asset: u64,
        symbol: String,
        attempted_order: f64,
        max_shares: Option<f64>,
        max_notional: Option<f64>,
    },

    #[error("Max order count exceeded: {current_count} orders placed, maximum allowed: {max_count}")]
    MaxOrderCountExceeded {
        current_count: usize,
        max_count: usize,
        date: DateTime<Utc>,
    },

    #[error("Max order size exceeded for asset {asset}: order size {order_size}, max allowed: {max_size}")]
    MaxOrderSizeExceeded {
        asset: u64,
        order_size: f64,
        max_size: f64,
    },

    #[error("Max leverage exceeded: current leverage {current_leverage:.2}x, maximum allowed: {max_leverage:.2}x")]
    MaxLeverageExceeded {
        current_leverage: f64,
        max_leverage: f64,
    },

    // ========== Pipeline Errors ==========
    #[error("Pipeline error: {0}")]
    PipelineError(String),

    #[error("Attach pipeline after initialize: pipelines must be attached in initialize()")]
    AttachPipelineAfterInitialize,

    #[error("Duplicate pipeline name: {0}")]
    DuplicatePipelineName(String),

    #[error("No such pipeline: {0}")]
    NoSuchPipeline(String),

    #[error("Pipeline not found: {0}")]
    PipelineNotFound(String),

    #[error("Pipeline output during initialize: cannot access pipeline output in initialize()")]
    PipelineOutputDuringInitialize,

    // P0 Pipeline Errors
    #[error("Pipeline produced unsupported output type for column '{column}': expected {expected}, got {actual}")]
    UnsupportedPipelineOutput {
        column: String,
        expected: String,
        actual: String,
    },

    #[error("Term '{term_name}' not found in pipeline execution graph. Available terms: {available_terms:?}")]
    TermNotInGraph {
        term_name: String,
        available_terms: Vec<String>,
    },

    // ========== Configuration Errors ==========
    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Zero capital error: initial capital must be greater than zero")]
    ZeroCapitalError,

    #[error("Set benchmark outside initialize: benchmark must be set in initialize()")]
    SetBenchmarkOutsideInitialize,

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

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

    // P0 Configuration Errors
    #[error("Unsupported data frequency: {frequency}. Supported frequencies: {supported:?}")]
    UnsupportedFrequency {
        frequency: String,
        supported: Vec<String>,
    },

    #[error("Invalid trading calendar name: '{calendar}'. Available calendars: {available:?}")]
    InvalidCalendarName {
        calendar: String,
        available: Vec<String>,
    },

    // ========== Fund Errors ==========
    #[error("Insufficient funds: required {required}, available {available}")]
    InsufficientFunds { required: f64, available: f64 },

    // P0 Financial Errors
    #[error("Portfolio value became negative: {portfolio_value:.2}. This indicates a critical error in transaction processing or leverage calculation.")]
    NegativePortfolioValue {
        portfolio_value: f64,
        timestamp: DateTime<Utc>,
    },

    // ========== Execution Errors ==========
    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Liquidity exceeded: order volume {order_volume} exceeds limit {limit}")]
    LiquidityExceeded { order_volume: f64, limit: f64 },

    // ========== Restriction Errors ==========
    #[error("Asset is restricted: {0}")]
    AssetRestricted(u64),

    #[error("Restricted asset: {0}")]
    RestrictedAsset(String),

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

    // ========== Data Format Errors ==========
    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Data not found: {0}")]
    DataNotFound(String),

    #[error("Missing data: {0}")]
    MissingData(String),

    #[error("Index out of bounds: index {0}, length {1}")]
    IndexOutOfBounds(usize, usize),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("Trading before start: {0}")]
    TradingBeforeStart(String),

    #[error("Unsupported feature: {0}")]
    UnsupportedFeature(String),

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
