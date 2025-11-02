//! Comprehensive performance metrics and analytics

use chrono::{DateTime, Utc, Duration};
use serde::{Deserialize, Serialize};

/// Complete set of performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    /// Total return (final value / starting value - 1)
    pub total_return: f64,
    /// Annualized return
    pub annual_return: f64,
    /// Sharpe ratio (risk-adjusted return)
    pub sharpe_ratio: f64,
    /// Sortino ratio (downside risk-adjusted return)
    pub sortino_ratio: f64,
    /// Maximum drawdown (peak to trough decline)
    pub max_drawdown: f64,
    /// Duration of maximum drawdown
    pub max_drawdown_duration_days: i64,
    /// Calmar ratio (annual return / max drawdown)
    pub calmar_ratio: f64,
    /// Omega ratio (probability weighted ratio of gains vs losses)
    pub omega_ratio: f64,
    /// Annualized volatility (standard deviation of returns)
    pub volatility: f64,
    /// Downside deviation (volatility of negative returns)
    pub downside_risk: f64,
    /// Alpha (excess return vs benchmark)
    pub alpha: Option<f64>,
    /// Beta (correlation with benchmark)
    pub beta: Option<f64>,
    /// Win rate (percentage of profitable trades)
    pub win_rate: f64,
    /// Average winning trade
    pub avg_win: f64,
    /// Average losing trade
    pub avg_loss: f64,
    /// Profit factor (gross profit / gross loss)
    pub profit_factor: f64,
    /// Total number of trades
    pub trades_count: usize,
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            total_return: 0.0,
            annual_return: 0.0,
            sharpe_ratio: 0.0,
            sortino_ratio: 0.0,
            max_drawdown: 0.0,
            max_drawdown_duration_days: 0,
            calmar_ratio: 0.0,
            omega_ratio: 0.0,
            volatility: 0.0,
            downside_risk: 0.0,
            alpha: None,
            beta: None,
            win_rate: 0.0,
            avg_win: 0.0,
            avg_loss: 0.0,
            profit_factor: 0.0,
            trades_count: 0,
        }
    }
}

/// Individual trade record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub asset_id: u64,
    pub entry_price: f64,
    pub exit_price: f64,
    pub quantity: f64,
    pub pnl: f64,
    pub pnl_pct: f64,
    pub entry_date: DateTime<Utc>,
    pub exit_date: DateTime<Utc>,
    pub hold_duration: Duration,
}

impl Trade {
    pub fn new(
        asset_id: u64,
        entry_price: f64,
        exit_price: f64,
        quantity: f64,
        entry_date: DateTime<Utc>,
        exit_date: DateTime<Utc>,
    ) -> Self {
        let pnl = (exit_price - entry_price) * quantity;
        let pnl_pct = (exit_price - entry_price) / entry_price;
        let hold_duration = exit_date - entry_date;

        Self {
            asset_id,
            entry_price,
            exit_price,
            quantity,
            pnl,
            pnl_pct,
            entry_date,
            exit_date,
            hold_duration,
        }
    }

    pub fn is_win(&self) -> bool {
        self.pnl > 0.0
    }

    pub fn is_loss(&self) -> bool {
        self.pnl < 0.0
    }
}

/// Tracks performance metrics throughout backtest
pub struct MetricsTracker {
    /// Daily returns
    returns: Vec<f64>,
    /// Portfolio values over time
    portfolio_values: Vec<(DateTime<Utc>, f64)>,
    /// Benchmark returns (if provided)
    benchmark_returns: Option<Vec<f64>>,
    /// Starting capital
    starting_cash: f64,
    /// Completed trades
    trades: Vec<Trade>,
    /// Risk-free rate for Sharpe calculation
    risk_free_rate: f64,
}

impl MetricsTracker {
    /// Create new metrics tracker
    pub fn new(starting_cash: f64) -> Self {
        Self {
            returns: Vec::new(),
            portfolio_values: Vec::new(),
            benchmark_returns: None,
            starting_cash,
            trades: Vec::new(),
            risk_free_rate: 0.02, // Default 2% annual
        }
    }

    /// Set risk-free rate for Sharpe calculation
    pub fn set_risk_free_rate(&mut self, rate: f64) {
        self.risk_free_rate = rate;
    }

    /// Record portfolio value at timestamp
    pub fn record_value(&mut self, timestamp: DateTime<Utc>, value: f64) {
        self.portfolio_values.push((timestamp, value));

        // Calculate daily return
        if let Some((_, prev_value)) = self.portfolio_values.get(self.portfolio_values.len().saturating_sub(2)) {
            if *prev_value > 0.0 {
                let daily_return = (value - prev_value) / prev_value;
                self.returns.push(daily_return);
            }
        }
    }

    /// Record a completed trade
    pub fn record_trade(&mut self, trade: Trade) {
        self.trades.push(trade);
    }

    /// Set benchmark returns for alpha/beta calculation
    pub fn set_benchmark(&mut self, returns: Vec<f64>) {
        self.benchmark_returns = Some(returns);
    }

    /// Calculate all performance metrics
    pub fn calculate_metrics(&self) -> PerformanceMetrics {
        PerformanceMetrics {
            total_return: self.calculate_total_return(),
            annual_return: self.calculate_annual_return(),
            sharpe_ratio: self.calculate_sharpe_ratio(),
            sortino_ratio: self.calculate_sortino_ratio(),
            max_drawdown: self.calculate_max_drawdown().0,
            max_drawdown_duration_days: self.calculate_max_drawdown().1,
            calmar_ratio: self.calculate_calmar_ratio(),
            omega_ratio: self.calculate_omega_ratio(),
            volatility: self.calculate_volatility(),
            downside_risk: self.calculate_downside_risk(),
            alpha: self.calculate_alpha_beta().map(|(a, _)| a),
            beta: self.calculate_alpha_beta().map(|(_, b)| b),
            win_rate: self.calculate_win_rate(),
            avg_win: self.calculate_avg_win(),
            avg_loss: self.calculate_avg_loss(),
            profit_factor: self.calculate_profit_factor(),
            trades_count: self.trades.len(),
        }
    }

    fn calculate_total_return(&self) -> f64 {
        if let Some((_, final_value)) = self.portfolio_values.last() {
            (final_value - self.starting_cash) / self.starting_cash
        } else {
            0.0
        }
    }

    fn calculate_annual_return(&self) -> f64 {
        if self.portfolio_values.len() < 2 {
            return 0.0;
        }

        let (start_date, start_value) = self.portfolio_values[0];
        let (end_date, end_value) = self.portfolio_values[self.portfolio_values.len() - 1];

        let years = (end_date - start_date).num_days() as f64 / 365.25;
        if years > 0.0 {
            ((end_value / start_value).powf(1.0 / years)) - 1.0
        } else {
            0.0
        }
    }

    fn calculate_sharpe_ratio(&self) -> f64 {
        if self.returns.is_empty() {
            return 0.0;
        }

        let mean_return = self.returns.iter().sum::<f64>() / self.returns.len() as f64;
        let volatility = self.calculate_volatility();

        if volatility == 0.0 {
            return 0.0;
        }

        let daily_rf = self.risk_free_rate / 252.0;
        let excess_return = mean_return - daily_rf;

        (excess_return / volatility) * (252.0_f64).sqrt()
    }

    fn calculate_sortino_ratio(&self) -> f64 {
        if self.returns.is_empty() {
            return 0.0;
        }

        let mean_return = self.returns.iter().sum::<f64>() / self.returns.len() as f64;
        let downside_risk = self.calculate_downside_risk();

        if downside_risk == 0.0 {
            return 0.0;
        }

        let daily_rf = self.risk_free_rate / 252.0;
        let excess_return = mean_return - daily_rf;

        (excess_return / downside_risk) * (252.0_f64).sqrt()
    }

    fn calculate_volatility(&self) -> f64 {
        if self.returns.len() < 2 {
            return 0.0;
        }

        let mean = self.returns.iter().sum::<f64>() / self.returns.len() as f64;
        let variance = self.returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / (self.returns.len() - 1) as f64;

        variance.sqrt() * (252.0_f64).sqrt() // Annualized
    }

    fn calculate_downside_risk(&self) -> f64 {
        if self.returns.is_empty() {
            return 0.0;
        }

        let negative_returns: Vec<f64> = self.returns.iter()
            .filter(|&&r| r < 0.0)
            .copied()
            .collect();

        if negative_returns.is_empty() {
            return 0.0;
        }

        let mean = negative_returns.iter().sum::<f64>() / negative_returns.len() as f64;
        let variance = negative_returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / negative_returns.len() as f64;

        variance.sqrt() * (252.0_f64).sqrt()
    }

    fn calculate_max_drawdown(&self) -> (f64, i64) {
        if self.portfolio_values.len() < 2 {
            return (0.0, 0);
        }

        let mut max_dd = 0.0;
        let mut max_dd_duration = 0;
        let mut peak = self.portfolio_values[0].1;
        let mut peak_date = self.portfolio_values[0].0;

        for (date, value) in &self.portfolio_values {
            if *value > peak {
                peak = *value;
                peak_date = *date;
            }

            let dd = (peak - value) / peak;
            if dd > max_dd {
                max_dd = dd;
                max_dd_duration = (*date - peak_date).num_days();
            }
        }

        (max_dd, max_dd_duration)
    }

    fn calculate_calmar_ratio(&self) -> f64 {
        let annual_return = self.calculate_annual_return();
        let max_dd = self.calculate_max_drawdown().0;

        if max_dd == 0.0 {
            return 0.0;
        }

        annual_return / max_dd
    }

    fn calculate_omega_ratio(&self) -> f64 {
        if self.returns.is_empty() {
            return 0.0;
        }

        let threshold = 0.0;
        let gains: f64 = self.returns.iter()
            .filter(|&&r| r > threshold)
            .map(|r| r - threshold)
            .sum();

        let losses: f64 = self.returns.iter()
            .filter(|&&r| r < threshold)
            .map(|r| threshold - r)
            .sum();

        if losses == 0.0 {
            return f64::INFINITY;
        }

        gains / losses
    }

    fn calculate_alpha_beta(&self) -> Option<(f64, f64)> {
        let benchmark = self.benchmark_returns.as_ref()?;

        if benchmark.len() != self.returns.len() || self.returns.len() < 2 {
            return None;
        }

        // Calculate means
        let mean_returns = self.returns.iter().sum::<f64>() / self.returns.len() as f64;
        let mean_benchmark = benchmark.iter().sum::<f64>() / benchmark.len() as f64;

        // Calculate beta (covariance / variance of benchmark)
        let covariance: f64 = self.returns.iter()
            .zip(benchmark.iter())
            .map(|(r, b)| (r - mean_returns) * (b - mean_benchmark))
            .sum::<f64>() / (self.returns.len() - 1) as f64;

        let benchmark_variance: f64 = benchmark.iter()
            .map(|b| (b - mean_benchmark).powi(2))
            .sum::<f64>() / (benchmark.len() - 1) as f64;

        if benchmark_variance == 0.0 {
            return None;
        }

        let beta = covariance / benchmark_variance;

        // Calculate alpha (excess return - beta * benchmark return)
        let alpha = mean_returns - beta * mean_benchmark;

        Some((alpha * 252.0, beta)) // Annualize alpha
    }

    fn calculate_win_rate(&self) -> f64 {
        if self.trades.is_empty() {
            return 0.0;
        }

        let wins = self.trades.iter().filter(|t| t.is_win()).count();
        wins as f64 / self.trades.len() as f64
    }

    fn calculate_avg_win(&self) -> f64 {
        let wins: Vec<f64> = self.trades.iter()
            .filter(|t| t.is_win())
            .map(|t| t.pnl)
            .collect();

        if wins.is_empty() {
            return 0.0;
        }

        wins.iter().sum::<f64>() / wins.len() as f64
    }

    fn calculate_avg_loss(&self) -> f64 {
        let losses: Vec<f64> = self.trades.iter()
            .filter(|t| t.is_loss())
            .map(|t| t.pnl)
            .collect();

        if losses.is_empty() {
            return 0.0;
        }

        losses.iter().sum::<f64>() / losses.len() as f64
    }

    fn calculate_profit_factor(&self) -> f64 {
        let gross_profit: f64 = self.trades.iter()
            .filter(|t| t.is_win())
            .map(|t| t.pnl)
            .sum();

        let gross_loss: f64 = self.trades.iter()
            .filter(|t| t.is_loss())
            .map(|t| t.pnl.abs())
            .sum();

        if gross_loss == 0.0 {
            return f64::INFINITY;
        }

        gross_profit / gross_loss
    }

    /// Get returns vector
    pub fn returns(&self) -> &[f64] {
        &self.returns
    }

    /// Get portfolio values
    pub fn portfolio_values(&self) -> &[(DateTime<Utc>, f64)] {
        &self.portfolio_values
    }

    /// Get trades
    pub fn trades(&self) -> &[Trade] {
        &self.trades
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_trade_creation() {
        let entry_date = Utc::now();
        let exit_date = entry_date + Duration::days(5);

        let trade = Trade::new(1, 100.0, 110.0, 10.0, entry_date, exit_date);

        assert_eq!(trade.pnl, 100.0); // (110-100) * 10
        assert_eq!(trade.pnl_pct, 0.10); // 10% gain
        assert!(trade.is_win());
        assert!(!trade.is_loss());
    }

    #[test]
    fn test_metrics_tracker() {
        let mut tracker = MetricsTracker::new(100000.0);

        // Record some values
        let start = Utc::now();
        tracker.record_value(start, 100000.0);
        tracker.record_value(start + Duration::days(1), 101000.0);
        tracker.record_value(start + Duration::days(2), 102000.0);
        tracker.record_value(start + Duration::days(3), 100500.0);

        let metrics = tracker.calculate_metrics();

        assert!(metrics.total_return > 0.0);
        assert!(metrics.volatility > 0.0);
        assert_eq!(metrics.trades_count, 0);
    }

    #[test]
    fn test_win_rate() {
        let mut tracker = MetricsTracker::new(100000.0);

        let now = Utc::now();

        // Add 3 winning trades
        tracker.record_trade(Trade::new(1, 100.0, 110.0, 10.0, now, now + Duration::days(1)));
        tracker.record_trade(Trade::new(2, 50.0, 55.0, 20.0, now, now + Duration::days(1)));
        tracker.record_trade(Trade::new(3, 200.0, 210.0, 5.0, now, now + Duration::days(1)));

        // Add 1 losing trade
        tracker.record_trade(Trade::new(4, 100.0, 90.0, 10.0, now, now + Duration::days(1)));

        let metrics = tracker.calculate_metrics();

        assert_eq!(metrics.win_rate, 0.75); // 3/4 = 75%
        assert_eq!(metrics.trades_count, 4);
        assert!(metrics.avg_win > 0.0);
        assert!(metrics.avg_loss < 0.0);
    }

    #[test]
    fn test_sharpe_ratio() {
        let mut tracker = MetricsTracker::new(100000.0);

        let start = Utc::now();
        tracker.record_value(start, 100000.0);
        for i in 1..=100 {
            let value = 100000.0 + (i as f64 * 100.0); // Steady growth
            tracker.record_value(start + Duration::days(i), value);
        }

        let metrics = tracker.calculate_metrics();

        assert!(metrics.sharpe_ratio > 0.0);
        assert!(metrics.volatility > 0.0);
    }
}
