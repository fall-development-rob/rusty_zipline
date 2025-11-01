//! Finance module - portfolio, metrics, controls, blotter, commission, slippage

pub mod blotter;
pub mod commission;
pub mod controls;
pub mod metrics;
pub mod slippage;

pub use blotter::{Blotter, Fill, Transaction, TransactionLog};
pub use commission::{
    CommissionModel, PerDollar, PerShare, PerTrade, TieredCommission, ZeroCommission,
};
pub use controls::{
    AccountControl, ControlManager, LongOnly, MaxLeverage, MaxOrderCount, MaxOrderSize,
    MaxPositionSize, MinLeverage, RestrictedList, TradingControl,
};
pub use metrics::{MetricsTracker, PerformanceMetrics, Trade};
pub use slippage::{
    FixedBasisPointsSlippage, LinearImpact, NoSlippage, SlippageModel, SquareRootImpact,
    VolumeShareSlippage,
};

// Re-export from parent for convenience
pub use crate::finance::{Portfolio, Position};
