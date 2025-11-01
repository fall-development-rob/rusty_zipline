//! Returns factors - price-based return calculations
//!
//! This module provides various return calculation factors for pipeline analysis

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
}
