//! Finance module - portfolio, metrics, controls, blotter

pub mod blotter;
pub mod controls;
pub mod metrics;

pub use blotter::{Blotter, Fill, Transaction, TransactionLog};
pub use controls::{
    AccountControl, ControlManager, LongOnly, MaxLeverage, MaxOrderCount, MaxOrderSize,
    MaxPositionSize, MinLeverage, RestrictedList, TradingControl,
};
pub use metrics::{MetricsTracker, PerformanceMetrics, Trade};

// Re-export from parent for convenience
pub use crate::finance::{Portfolio, Position};
