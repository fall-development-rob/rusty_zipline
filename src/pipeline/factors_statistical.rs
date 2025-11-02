//! Statistical factors for pipeline analysis
//!
//! This module provides statistical and risk-adjusted performance indicators

use statrs::statistics::{Data, Distribution};
use std::collections::VecDeque;

/// Correlation - Rolling correlation between two series
#[derive(Debug, Clone)]
pub struct Correlation {
    window: usize,
    x_values: VecDeque<f64>,
    y_values: VecDeque<f64>,
}

impl Correlation {
    /// Create new Correlation calculator with given window
    pub fn new(window: usize) -> Self {
        if window < 2 {
            panic!("Window must be at least 2");
        }
        Self {
            window,
            x_values: VecDeque::with_capacity(window),
            y_values: VecDeque::with_capacity(window),
        }
    }

    /// Update with new X and Y values
    pub fn update(&mut self, x: f64, y: f64) -> Option<f64> {
        self.x_values.push_back(x);
        self.y_values.push_back(y);

        if self.x_values.len() > self.window {
            self.x_values.pop_front();
            self.y_values.pop_front();
        }

        if self.x_values.len() == self.window {
            let x_vec: Vec<f64> = self.x_values.iter().copied().collect();
            let y_vec: Vec<f64> = self.y_values.iter().copied().collect();

            let x_data = Data::new(x_vec.clone());
            let y_data = Data::new(y_vec.clone());

            let x_mean = x_data.mean().unwrap_or(0.0);
            let y_mean = y_data.mean().unwrap_or(0.0);

            let covariance: f64 = x_vec
                .iter()
                .zip(y_vec.iter())
                .map(|(&xi, &yi)| (xi - x_mean) * (yi - y_mean))
                .sum::<f64>()
                / (self.window - 1) as f64;

            let x_std = x_data.std_dev().unwrap_or(0.0);
            let y_std = y_data.std_dev().unwrap_or(0.0);

            if x_std != 0.0 && y_std != 0.0 {
                Some(covariance / (x_std * y_std))
            } else {
                Some(0.0)
            }
        } else {
            None
        }
    }
}

/// Beta - Market beta calculation
#[derive(Debug, Clone)]
pub struct Beta {
    window: usize,
    asset_returns: VecDeque<f64>,
    market_returns: VecDeque<f64>,
}

impl Beta {
    /// Create new Beta calculator
    pub fn new(window: usize) -> Self {
        if window < 2 {
            panic!("Window must be at least 2");
        }
        Self {
            window,
            asset_returns: VecDeque::with_capacity(window),
            market_returns: VecDeque::with_capacity(window),
        }
    }

    /// Update with new asset and market returns
    pub fn update(&mut self, asset_return: f64, market_return: f64) -> Option<f64> {
        self.asset_returns.push_back(asset_return);
        self.market_returns.push_back(market_return);

        if self.asset_returns.len() > self.window {
            self.asset_returns.pop_front();
            self.market_returns.pop_front();
        }

        if self.asset_returns.len() == self.window {
            let asset_vec: Vec<f64> = self.asset_returns.iter().copied().collect();
            let market_vec: Vec<f64> = self.market_returns.iter().copied().collect();

            let market_data = Data::new(market_vec.clone());
            let asset_data = Data::new(asset_vec.clone());

            let market_mean = market_data.mean().unwrap_or(0.0);
            let asset_mean = asset_data.mean().unwrap_or(0.0);

            let covariance: f64 = asset_vec
                .iter()
                .zip(market_vec.iter())
                .map(|(&ai, &mi)| (ai - asset_mean) * (mi - market_mean))
                .sum::<f64>()
                / (self.window - 1) as f64;

            let market_variance: f64 = market_vec
                .iter()
                .map(|&mi| (mi - market_mean).powi(2))
                .sum::<f64>()
                / (self.window - 1) as f64;

            if market_variance != 0.0 {
                Some(covariance / market_variance)
            } else {
                Some(0.0)
            }
        } else {
            None
        }
    }
}

/// Alpha - Jensen's Alpha
#[derive(Debug, Clone)]
pub struct Alpha {
    window: usize,
    asset_returns: VecDeque<f64>,
    market_returns: VecDeque<f64>,
    risk_free_rate: f64,
}

impl Alpha {
    /// Create new Alpha calculator
    /// risk_free_rate: annualized risk-free rate
    pub fn new(window: usize, risk_free_rate: f64) -> Self {
        if window < 2 {
            panic!("Window must be at least 2");
        }
        Self {
            window,
            asset_returns: VecDeque::with_capacity(window),
            market_returns: VecDeque::with_capacity(window),
            risk_free_rate,
        }
    }

    /// Update with new asset and market returns
    pub fn update(&mut self, asset_return: f64, market_return: f64) -> Option<f64> {
        self.asset_returns.push_back(asset_return);
        self.market_returns.push_back(market_return);

        if self.asset_returns.len() > self.window {
            self.asset_returns.pop_front();
            self.market_returns.pop_front();
        }

        if self.asset_returns.len() == self.window {
            // Calculate beta first
            let asset_vec: Vec<f64> = self.asset_returns.iter().copied().collect();
            let market_vec: Vec<f64> = self.market_returns.iter().copied().collect();

            let market_data = Data::new(market_vec.clone());
            let asset_data = Data::new(asset_vec.clone());

            let market_mean = market_data.mean().unwrap_or(0.0);
            let asset_mean = asset_data.mean().unwrap_or(0.0);

            let covariance: f64 = asset_vec
                .iter()
                .zip(market_vec.iter())
                .map(|(&ai, &mi)| (ai - asset_mean) * (mi - market_mean))
                .sum::<f64>()
                / (self.window - 1) as f64;

            let market_variance: f64 = market_vec
                .iter()
                .map(|&mi| (mi - market_mean).powi(2))
                .sum::<f64>()
                / (self.window - 1) as f64;

            let beta = if market_variance != 0.0 {
                covariance / market_variance
            } else {
                0.0
            };

            // Alpha = Actual Return - Expected Return (CAPM)
            // Expected Return = Rf + Beta * (Market Return - Rf)
            let expected_return = self.risk_free_rate + beta * (market_mean - self.risk_free_rate);
            Some(asset_mean - expected_return)
        } else {
            None
        }
    }
}

/// Sharpe Ratio - Risk-adjusted return
#[derive(Debug, Clone)]
pub struct SharpeRatio {
    window: usize,
    returns: VecDeque<f64>,
    risk_free_rate: f64,
    annualization_factor: f64,
}

impl SharpeRatio {
    /// Create new Sharpe Ratio calculator
    /// periods_per_year: 252 for daily, 52 for weekly, etc.
    pub fn new(window: usize, risk_free_rate: f64, periods_per_year: f64) -> Self {
        if window < 2 {
            panic!("Window must be at least 2");
        }
        Self {
            window,
            returns: VecDeque::with_capacity(window),
            risk_free_rate,
            annualization_factor: periods_per_year.sqrt(),
        }
    }

    /// Update with new return
    pub fn update(&mut self, ret: f64) -> Option<f64> {
        self.returns.push_back(ret);

        if self.returns.len() > self.window {
            self.returns.pop_front();
        }

        if self.returns.len() == self.window {
            let returns_vec: Vec<f64> = self.returns.iter().copied().collect();
            let data = Data::new(returns_vec);

            let mean_return = data.mean().unwrap_or(0.0);
            let std_dev = data.std_dev().unwrap_or(0.0);

            if std_dev != 0.0 {
                let excess_return = mean_return - (self.risk_free_rate / self.annualization_factor.powi(2));
                Some((excess_return / std_dev) * self.annualization_factor)
            } else {
                Some(0.0)
            }
        } else {
            None
        }
    }
}

/// Sortino Ratio - Downside risk-adjusted return
#[derive(Debug, Clone)]
pub struct SortinoRatio {
    window: usize,
    returns: VecDeque<f64>,
    target_return: f64,
    annualization_factor: f64,
}

impl SortinoRatio {
    /// Create new Sortino Ratio calculator
    pub fn new(window: usize, target_return: f64, periods_per_year: f64) -> Self {
        if window < 2 {
            panic!("Window must be at least 2");
        }
        Self {
            window,
            returns: VecDeque::with_capacity(window),
            target_return,
            annualization_factor: periods_per_year.sqrt(),
        }
    }

    /// Update with new return
    pub fn update(&mut self, ret: f64) -> Option<f64> {
        self.returns.push_back(ret);

        if self.returns.len() > self.window {
            self.returns.pop_front();
        }

        if self.returns.len() == self.window {
            let returns_vec: Vec<f64> = self.returns.iter().copied().collect();
            let data = Data::new(returns_vec.clone());

            let mean_return = data.mean().unwrap_or(0.0);

            // Calculate downside deviation (only negative deviations from target)
            let downside_deviations: Vec<f64> = returns_vec
                .iter()
                .map(|&r| {
                    let deviation = r - self.target_return;
                    if deviation < 0.0 {
                        deviation.powi(2)
                    } else {
                        0.0
                    }
                })
                .collect();

            let downside_variance = downside_deviations.iter().sum::<f64>() / self.window as f64;
            let downside_deviation = downside_variance.sqrt();

            if downside_deviation != 0.0 {
                let excess_return = mean_return - self.target_return;
                Some((excess_return / downside_deviation) * self.annualization_factor)
            } else {
                Some(0.0)
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn test_correlation() {
        let mut corr = Correlation::new(5);

        // Perfect positive correlation
        for i in 0..5 {
            corr.update(i as f64, i as f64);
        }

        let result = corr.update(5.0, 5.0);
        assert!(result.is_some());
        assert_relative_eq!(result.unwrap(), 1.0, epsilon = 0.01);
    }

    #[test]
    fn test_beta() {
        let mut beta = Beta::new(10);

        for i in 0..15 {
            let market_return = 0.01;
            let asset_return = 0.015; // Higher than market

            let result = beta.update(asset_return, market_return);
            if i >= 9 {
                assert!(result.is_some());
            }
        }
    }

    #[test]
    fn test_alpha() {
        let mut alpha = Alpha::new(10, 0.02); // 2% risk-free rate

        for i in 0..15 {
            let market_return = 0.01;
            let asset_return = 0.015;

            let result = alpha.update(asset_return, market_return);
            if i >= 9 {
                assert!(result.is_some());
            }
        }
    }

    #[test]
    fn test_sharpe_ratio() {
        let mut sharpe = SharpeRatio::new(20, 0.02, 252.0);

        for i in 0..25 {
            let ret = 0.001 + (i as f64 * 0.0001);
            let result = sharpe.update(ret);

            if i >= 19 {
                assert!(result.is_some());
            }
        }
    }

    #[test]
    fn test_sortino_ratio() {
        let mut sortino = SortinoRatio::new(20, 0.0, 252.0);

        let returns = vec![0.02, -0.01, 0.03, -0.02, 0.01, 0.04, -0.01, 0.02];

        for (i, &ret) in returns.iter().enumerate().cycle().take(25) {
            let result = sortino.update(ret);
            if i >= 19 {
                assert!(result.is_some());
            }
        }
    }
}
