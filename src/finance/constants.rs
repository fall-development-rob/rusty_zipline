//! Trading constants and defaults
//!
//! Contains default values and constants used throughout the trading system

/// Default commission per share
pub const DEFAULT_COMMISSION_PER_SHARE: f64 = 0.001;

/// Default slippage (basis points)
pub const DEFAULT_SLIPPAGE_BPS: f64 = 5.0;

/// Default starting capital
pub const DEFAULT_CAPITAL: f64 = 10_000_000.0;

/// Trading calendar constants
pub const TRADING_DAYS_PER_YEAR: f64 = 252.0;
pub const TRADING_HOURS_PER_DAY: f64 = 6.5;

/// Minimum price increment (penny)
pub const MIN_PRICE_INCREMENT: f64 = 0.01;

/// Default leverage limit
pub const DEFAULT_MAX_LEVERAGE: f64 = 1.0;

/// Default position limits
pub const DEFAULT_MAX_POSITION_SIZE: f64 = 1_000_000.0;
pub const DEFAULT_MAX_ORDER_SIZE: f64 = 100_000.0;

/// Zero tolerance for floating point comparisons
pub const ZERO_TOLERANCE: f64 = 1e-10;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert_eq!(TRADING_DAYS_PER_YEAR, 252.0);
        assert_eq!(DEFAULT_CAPITAL, 10_000_000.0);
        assert!(DEFAULT_COMMISSION_PER_SHARE > 0.0);
        assert!(DEFAULT_SLIPPAGE_BPS > 0.0);
        assert!(MIN_PRICE_INCREMENT > 0.0);
    }
}
