//! Finance module - portfolio, metrics, controls, blotter, commission, slippage

pub mod account; // NEW: P0 - Account object for account-level metrics
pub mod asset_restrictions; // NEW: Asset trading restrictions
pub mod blotter;
pub mod cancel_policy; // NEW: Order cancellation policies
pub mod commission;
pub mod constants; // NEW: Trading constants and defaults
pub mod controls;
pub mod ledger; // NEW: P1 - Transaction tracking and P&L system
pub mod metrics;
pub mod portfolio;
pub mod slippage;
pub mod trading; // NEW: Trading controls and validations
pub mod transaction; // NEW: Transaction type

pub use account::Account;
pub use asset_restrictions::{
    CompositeRestrictions, HistoricalRestrictions, NoRestrictions, RestrictionReason,
    Restrictions, SecurityListRestrictions, StaticRestrictions,
};
pub use blotter::{Blotter, Fill, TransactionLog};
pub use cancel_policy::{CancelPolicy, EODCancel, EODCancelNext, NeverCancel};
pub use commission::{
    CommissionModel, PerDollar, PerShare, PerTrade, TieredCommission, ZeroCommission,
};
pub use constants::{
    DEFAULT_CAPITAL, DEFAULT_COMMISSION_PER_SHARE, DEFAULT_MAX_LEVERAGE,
    DEFAULT_MAX_ORDER_SIZE, DEFAULT_MAX_POSITION_SIZE, DEFAULT_SLIPPAGE_BPS,
    MIN_PRICE_INCREMENT, TRADING_DAYS_PER_YEAR, TRADING_HOURS_PER_DAY, ZERO_TOLERANCE,
};
pub use controls::{
    AccountControl, ControlManager, LongOnly, MaxLeverage as ControlMaxLeverage, MaxOrderCount,
    MaxOrderSize as ControlMaxOrderSize, MaxPositionSize as ControlMaxPositionSize, MinLeverage,
    PositionConcentration, RestrictedList, SectorExposure, TradingControl as ControlTradingControl,
    VolatilityLimit,
};
pub use ledger::{CostBasisMethod, Ledger, LedgerPosition, Lot, PnLSummary};
pub use metrics::{MetricsTracker, PerformanceMetrics, Trade};
pub use slippage::{
    FixedBasisPointsSlippage, LinearImpact, NoSlippage, SlippageModel, SquareRootImpact,
    VolumeShareSlippage,
};
pub use portfolio::{Portfolio, Position};
pub use trading::{MaxLeverage, MaxOrderSize, MaxPositionSize, TradingControl};
pub use transaction::Transaction;
