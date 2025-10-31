//! Performance analytics and metrics

use crate::types::Timestamp;
use serde::{Deserialize, Serialize};

/// Performance metrics tracker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceTracker {
    /// Portfolio value over time
    pub values: Vec<(Timestamp, f64)>,
    /// Returns over time
    pub returns: Vec<(Timestamp, f64)>,
}

impl PerformanceTracker {
    /// Create a new performance tracker
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            returns: Vec::new(),
        }
    }

    /// Record a performance data point
    pub fn record(&mut self, timestamp: Timestamp, value: f64, returns: f64) {
        self.values.push((timestamp, value));
        self.returns.push((timestamp, returns));
    }

    /// Calculate total return
    pub fn total_return(&self) -> f64 {
        self.returns.last().map(|(_, r)| *r).unwrap_or(0.0)
    }

    /// Calculate annualized return
    pub fn annualized_return(&self) -> f64 {
        if self.values.len() < 2 {
            return 0.0;
        }

        let first = self.values.first().unwrap();
        let last = self.values.last().unwrap();

        let days = (last.0 - first.0).num_days() as f64;
        if days == 0.0 {
            return 0.0;
        }

        let total_return = self.total_return();
        let years = days / 365.25;

        ((1.0 + total_return).powf(1.0 / years) - 1.0)
    }

    /// Calculate Sharpe ratio (simplified, assuming risk-free rate = 0)
    pub fn sharpe_ratio(&self) -> f64 {
        if self.returns.len() < 2 {
            return 0.0;
        }

        let returns: Vec<f64> = self.returns.iter().map(|(_, r)| *r).collect();
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;

        let variance = returns
            .iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;

        let std_dev = variance.sqrt();

        if std_dev == 0.0 {
            0.0
        } else {
            mean / std_dev * (252.0_f64).sqrt() // Annualized
        }
    }

    /// Calculate maximum drawdown
    pub fn max_drawdown(&self) -> f64 {
        if self.values.len() < 2 {
            return 0.0;
        }

        let mut max_value = self.values[0].1;
        let mut max_dd = 0.0;

        for (_, value) in &self.values {
            if *value > max_value {
                max_value = *value;
            }

            let drawdown = (max_value - value) / max_value;
            if drawdown > max_dd {
                max_dd = drawdown;
            }
        }

        max_dd
    }

    /// Calculate volatility (standard deviation of returns)
    pub fn volatility(&self) -> f64 {
        if self.returns.len() < 2 {
            return 0.0;
        }

        let returns: Vec<f64> = self.returns.iter().map(|(_, r)| *r).collect();
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;

        let variance = returns
            .iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;

        variance.sqrt() * (252.0_f64).sqrt() // Annualized
    }

    /// Calculate Sortino ratio (downside deviation)
    pub fn sortino_ratio(&self) -> f64 {
        if self.returns.len() < 2 {
            return 0.0;
        }

        let returns: Vec<f64> = self.returns.iter().map(|(_, r)| *r).collect();
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;

        let downside_variance = returns
            .iter()
            .filter(|r| **r < 0.0)
            .map(|r| r.powi(2))
            .sum::<f64>() / returns.len() as f64;

        let downside_std = downside_variance.sqrt();

        if downside_std == 0.0 {
            0.0
        } else {
            mean / downside_std * (252.0_f64).sqrt() // Annualized
        }
    }

    /// Get summary statistics
    pub fn summary(&self) -> PerformanceSummary {
        PerformanceSummary {
            total_return: self.total_return(),
            annualized_return: self.annualized_return(),
            sharpe_ratio: self.sharpe_ratio(),
            sortino_ratio: self.sortino_ratio(),
            max_drawdown: self.max_drawdown(),
            volatility: self.volatility(),
            num_periods: self.values.len(),
        }
    }
}

impl Default for PerformanceTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Performance summary statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSummary {
    pub total_return: f64,
    pub annualized_return: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub max_drawdown: f64,
    pub volatility: f64,
    pub num_periods: usize,
}

impl std::fmt::Display for PerformanceSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Performance Summary:")?;
        writeln!(f, "  Total Return:       {:.2}%", self.total_return * 100.0)?;
        writeln!(
            f,
            "  Annualized Return:  {:.2}%",
            self.annualized_return * 100.0
        )?;
        writeln!(f, "  Sharpe Ratio:       {:.2}", self.sharpe_ratio)?;
        writeln!(f, "  Sortino Ratio:      {:.2}", self.sortino_ratio)?;
        writeln!(f, "  Max Drawdown:       {:.2}%", self.max_drawdown * 100.0)?;
        writeln!(f, "  Volatility:         {:.2}%", self.volatility * 100.0)?;
        writeln!(f, "  Periods:            {}", self.num_periods)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_performance_tracker() {
        let mut tracker = PerformanceTracker::new();

        let start = Utc::now();
        tracker.record(start, 100000.0, 0.0);
        tracker.record(start + chrono::Duration::days(1), 105000.0, 0.05);
        tracker.record(start + chrono::Duration::days(2), 110000.0, 0.10);

        assert_eq!(tracker.total_return(), 0.10);
        assert!(tracker.max_drawdown() >= 0.0);
    }

    #[test]
    fn test_max_drawdown() {
        let mut tracker = PerformanceTracker::new();

        let start = Utc::now();
        tracker.record(start, 100000.0, 0.0);
        tracker.record(start + chrono::Duration::days(1), 110000.0, 0.10);
        tracker.record(start + chrono::Duration::days(2), 90000.0, -0.10);
        tracker.record(start + chrono::Duration::days(3), 95000.0, -0.05);

        let max_dd = tracker.max_drawdown();
        assert!(max_dd > 0.0);
        assert!(max_dd <= 1.0);
    }

    #[test]
    fn test_summary() {
        let mut tracker = PerformanceTracker::new();

        let start = Utc::now();
        tracker.record(start, 100000.0, 0.0);
        tracker.record(start + chrono::Duration::days(365), 120000.0, 0.20);

        let summary = tracker.summary();
        assert_eq!(summary.total_return, 0.20);
        assert!(summary.annualized_return > 0.0);
    }
}
