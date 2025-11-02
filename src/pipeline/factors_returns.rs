//! Returns and risk-based factors
//!
//! This module contains factors for returns and risk metrics:
//! - Returns: Percentage returns
//! - DailyReturns: Single-period returns
//! - LogReturns: Logarithmic returns
//! - CumulativeReturns: Compound returns
//! - PercentChange: Percentage change
//! - MaxDrawdown: Maximum peak-to-trough decline (CRITICAL for risk management)

use crate::error::Result;
use crate::pipeline::engine::{Factor, FactorOutput, PipelineContext};
use chrono::{DateTime, Utc};
use hashbrown::HashMap;
use std::collections::VecDeque;

/// Returns - Simple percentage returns
#[derive(Debug, Clone)]
pub struct Returns {
    window: usize,
    values: VecDeque<f64>,
}

impl Returns {
    /// Create new Returns factor with lookback window
    pub fn new(window: usize) -> Self {
        if window == 0 {
            panic!("Window must be greater than 0");
        }
        Self {
            window,
            values: VecDeque::with_capacity(window + 1),
        }
    }

    /// Update with new price and compute return
    pub fn update(&mut self, price: f64) -> Option<f64> {
        self.values.push_back(price);

        if self.values.len() > self.window + 1 {
            self.values.pop_front();
        }

        if self.values.len() == self.window + 1 {
            let start_price = self.values.front().unwrap();
            let end_price = self.values.back().unwrap();
            Some((end_price - start_price) / start_price)
        } else {
            None
        }
    }

    /// Compute returns for a slice of prices
    pub fn compute(window: usize, prices: &[f64]) -> Vec<Option<f64>> {
        let mut returns = Self::new(window);
        prices.iter().map(|&p| returns.update(p)).collect()
    }
}

/// DailyReturns - Single period returns (1-day)
#[derive(Debug, Clone)]
pub struct DailyReturns {
    prev_price: Option<f64>,
}

impl DailyReturns {
    /// Create new DailyReturns factor
    pub fn new() -> Self {
        Self { prev_price: None }
    }

    /// Update with new price
    pub fn update(&mut self, price: f64) -> Option<f64> {
        if let Some(prev) = self.prev_price {
            let ret = (price - prev) / prev;
            self.prev_price = Some(price);
            Some(ret)
        } else {
            self.prev_price = Some(price);
            None
        }
    }

    /// Compute daily returns for a slice of prices
    pub fn compute(prices: &[f64]) -> Vec<Option<f64>> {
        let mut returns = Self::new();
        prices.iter().map(|&p| returns.update(p)).collect()
    }
}

impl Default for DailyReturns {
    fn default() -> Self {
        Self::new()
    }
}

/// PercentChange - Percentage change over window
#[derive(Debug, Clone)]
pub struct PercentChange {
    window: usize,
    values: VecDeque<f64>,
}

impl PercentChange {
    /// Create new PercentChange factor
    pub fn new(window: usize) -> Self {
        if window == 0 {
            panic!("Window must be greater than 0");
        }
        Self {
            window,
            values: VecDeque::with_capacity(window + 1),
        }
    }

    /// Update with new value
    pub fn update(&mut self, value: f64) -> Option<f64> {
        self.values.push_back(value);

        if self.values.len() > self.window + 1 {
            self.values.pop_front();
        }

        if self.values.len() == self.window + 1 {
            let old_value = self.values.front().unwrap();
            let new_value = self.values.back().unwrap();

            if old_value.abs() < f64::EPSILON {
                return None; // Avoid division by zero
            }

            Some(((new_value - old_value) / old_value) * 100.0)
        } else {
            None
        }
    }
}

/// LogReturns - Logarithmic returns
#[derive(Debug, Clone)]
pub struct LogReturns {
    window: usize,
    values: VecDeque<f64>,
}

impl LogReturns {
    /// Create new LogReturns factor
    pub fn new(window: usize) -> Self {
        if window == 0 {
            panic!("Window must be greater than 0");
        }
        Self {
            window,
            values: VecDeque::with_capacity(window + 1),
        }
    }

    /// Update with new price
    pub fn update(&mut self, price: f64) -> Option<f64> {
        self.values.push_back(price);

        if self.values.len() > self.window + 1 {
            self.values.pop_front();
        }

        if self.values.len() == self.window + 1 {
            let start_price = self.values.front().unwrap();
            let end_price = self.values.back().unwrap();

            if *start_price <= 0.0 || *end_price <= 0.0 {
                return None; // Log of non-positive numbers is undefined
            }

            Some((end_price / start_price).ln())
        } else {
            None
        }
    }

    /// Compute log returns for a slice of prices
    pub fn compute(window: usize, prices: &[f64]) -> Vec<Option<f64>> {
        let mut log_returns = Self::new(window);
        prices.iter().map(|&p| log_returns.update(p)).collect()
    }
}

/// CumulativeReturns - Cumulative product of returns
#[derive(Debug, Clone)]
pub struct CumulativeReturns {
    cumulative: f64,
}

impl CumulativeReturns {
    /// Create new CumulativeReturns factor
    pub fn new() -> Self {
        Self { cumulative: 1.0 }
    }

    /// Update with new return (not price)
    pub fn update(&mut self, ret: f64) -> f64 {
        self.cumulative *= 1.0 + ret;
        self.cumulative - 1.0 // Return as cumulative return
    }

    /// Reset cumulative returns
    pub fn reset(&mut self) {
        self.cumulative = 1.0;
    }
}

impl Default for CumulativeReturns {
    fn default() -> Self {
        Self::new()
    }
}

/// Maximum Drawdown - CRITICAL for risk management
///
/// Calculates maximum drawdown over a rolling window.
/// Maximum drawdown is the largest peak-to-trough decline in price.
///
/// Essential for risk management and position sizing.
/// Typical usage: filter out stocks with > 30% drawdown (too risky)
///
/// # Example
/// ```rust,no_run
/// use rusty_zipline::pipeline::MaxDrawdown;
///
/// // Filter out stocks with > 30% drawdown (too risky)
/// let mdd = MaxDrawdown::new(252);  // 1 year
/// // Use with pipeline filtering to exclude high-risk assets
/// ```
#[derive(Debug, Clone)]
pub struct MaxDrawdown {
    /// Number of trading days to look back
    window: usize,
    /// Rolling buffer of prices
    prices: VecDeque<f64>,
}

impl MaxDrawdown {
    /// Create new MaxDrawdown factor
    ///
    /// # Arguments
    /// * `window` - Number of days to look back (typically 252 for 1 year)
    ///
    /// # Panics
    /// Panics if window is 0
    pub fn new(window: usize) -> Self {
        assert!(window > 0, "Window must be positive");
        Self {
            window,
            prices: VecDeque::with_capacity(window),
        }
    }

    /// Calculate max drawdown for a single price series
    ///
    /// # Arguments
    /// * `prices` - Slice of historical prices
    ///
    /// # Returns
    /// Maximum drawdown as a decimal (0.30 = 30% drawdown)
    pub fn calculate_max_dd(prices: &[f64]) -> f64 {
        if prices.is_empty() {
            return f64::NAN;
        }

        let mut max_price = prices[0];
        let mut max_dd = 0.0;

        for &price in prices {
            if price > max_price {
                max_price = price;
            }

            let drawdown = (max_price - price) / max_price;
            if drawdown > max_dd {
                max_dd = drawdown;
            }
        }

        max_dd
    }

    /// Update with new price and compute current max drawdown
    ///
    /// # Returns
    /// Some(max_drawdown) if window is full, None otherwise
    pub fn update(&mut self, price: f64) -> Option<f64> {
        self.prices.push_back(price);

        if self.prices.len() > self.window {
            self.prices.pop_front();
        }

        if self.prices.len() == self.window {
            let prices_vec: Vec<f64> = self.prices.iter().copied().collect();
            Some(Self::calculate_max_dd(&prices_vec))
        } else {
            None
        }
    }

    /// Get the window length
    pub fn window_length(&self) -> usize {
        self.window
    }
}

impl Factor for MaxDrawdown {
    fn compute(&self, _timestamp: DateTime<Utc>, context: &PipelineContext) -> Result<FactorOutput> {
        let mut output = HashMap::new();

        for asset in context.assets() {
            // Get historical close prices
            let closes = context
                .data_provider()
                .get_prices(asset.id, self.window)?;

            // Calculate max drawdown
            let max_dd = if closes.len() >= self.window {
                Self::calculate_max_dd(&closes[closes.len() - self.window..])
            } else if !closes.is_empty() {
                // Use available data if we don't have full window
                Self::calculate_max_dd(&closes)
            } else {
                f64::NAN
            };

            output.insert(asset.id, max_dd);
        }

        Ok(output)
    }

    fn name(&self) -> &str {
        "MaxDrawdown"
    }

    fn clone_box(&self) -> Box<dyn Factor> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn test_returns() {
        let prices = vec![100.0, 105.0, 110.0, 108.0, 112.0];
        let returns = Returns::compute(1, &prices);

        assert_eq!(returns[0], None);
        assert_eq!(returns[1], Some(0.05)); // 5% gain
        assert_eq!(returns[2], Some(0.10 / 1.05)); // From 105 to 110
    }

    #[test]
    fn test_daily_returns() {
        let prices = vec![100.0, 105.0, 110.0, 108.0];
        let returns = DailyReturns::compute(&prices);

        assert_eq!(returns[0], None);
        assert_relative_eq!(returns[1].unwrap(), 0.05, epsilon = 1e-10);
        assert_relative_eq!(returns[2].unwrap(), 0.047619, epsilon = 1e-5);
        assert_relative_eq!(returns[3].unwrap(), -0.018182, epsilon = 1e-5);
    }

    #[test]
    fn test_percent_change() {
        let mut pc = PercentChange::new(2);

        assert_eq!(pc.update(100.0), None);
        assert_eq!(pc.update(105.0), None);
        assert_eq!(pc.update(110.0), Some(10.0)); // 10% change from 100 to 110
    }

    #[test]
    fn test_log_returns() {
        let prices = vec![100.0, 105.0, 110.0];
        let returns = LogReturns::compute(1, &prices);

        assert_eq!(returns[0], None);
        assert!(returns[1].is_some());
        assert_relative_eq!(returns[1].unwrap(), (105.0 / 100.0).ln(), epsilon = 1e-10);
    }

    #[test]
    fn test_cumulative_returns() {
        let mut cum = CumulativeReturns::new();

        let r1 = cum.update(0.10); // 10% gain
        assert_relative_eq!(r1, 0.10, epsilon = 1e-10);

        let r2 = cum.update(0.05); // 5% gain on top
        assert_relative_eq!(r2, 0.155, epsilon = 1e-10); // 1.10 * 1.05 - 1
    }

    #[test]
    fn test_cumulative_returns_reset() {
        let mut cum = CumulativeReturns::new();

        cum.update(0.10);
        cum.update(0.05);
        cum.reset();

        let r = cum.update(0.05);
        assert_relative_eq!(r, 0.05, epsilon = 1e-10);
    }

    #[test]
    fn test_max_drawdown() {
        let mut mdd = MaxDrawdown::new(5);

        // Price series with clear drawdown:
        // Prices: [100, 110, 90, 80, 85]
        // Peak: 110, trough: 80
        // Drawdown: (110 - 80) / 110 = 27.27%
        let prices = vec![100.0, 110.0, 90.0, 80.0, 85.0];

        let mut result = None;
        for price in prices {
            result = mdd.update(price);
        }

        assert!(result.is_some());
        let dd = result.unwrap();
        assert_relative_eq!(dd, 0.2727, epsilon = 0.01); // ~27.27% drawdown
    }

    #[test]
    fn test_max_drawdown_no_decline() {
        let mut mdd = MaxDrawdown::new(5);

        // Monotonically increasing - no drawdown
        let prices = vec![100.0, 101.0, 102.0, 103.0, 104.0];

        let mut result = None;
        for price in prices {
            result = mdd.update(price);
        }

        assert!(result.is_some());
        let dd = result.unwrap();
        assert!(dd < 0.01); // Minimal drawdown
    }

    #[test]
    fn test_max_drawdown_calculate() {
        // Test static calculation method
        let prices = vec![100.0, 120.0, 110.0, 90.0, 95.0];
        let dd = MaxDrawdown::calculate_max_dd(&prices);

        // Peak: 120, trough: 90
        // Drawdown: (120 - 90) / 120 = 25%
        assert_relative_eq!(dd, 0.25, epsilon = 0.01);
    }

    #[test]
    fn test_max_drawdown_empty() {
        let prices: Vec<f64> = vec![];
        let dd = MaxDrawdown::calculate_max_dd(&prices);
        assert!(dd.is_nan());
    }

    #[test]
    fn test_max_drawdown_single_price() {
        let prices = vec![100.0];
        let dd = MaxDrawdown::calculate_max_dd(&prices);
        assert_relative_eq!(dd, 0.0, epsilon = 1e-10); // No drawdown with single price
    }

    #[test]
    fn test_max_drawdown_recovery() {
        // Test that recovery doesn't reduce max drawdown
        let prices = vec![100.0, 120.0, 80.0, 110.0, 115.0];
        let dd = MaxDrawdown::calculate_max_dd(&prices);

        // Peak: 120, trough: 80, then recovers
        // Max drawdown should still be (120 - 80) / 120 = 33.33%
        assert_relative_eq!(dd, 0.3333, epsilon = 0.01);
    }

    #[test]
    fn test_max_drawdown_pipeline_integration() {
        // This test verifies the factor interface
        let mdd = MaxDrawdown::new(252);
        assert_eq!(mdd.window_length(), 252);
        assert_eq!(mdd.name(), "MaxDrawdown");
    }
}
