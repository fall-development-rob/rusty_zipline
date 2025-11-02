//! Benchmark data loading for performance comparisons
//!
//! Provides benchmark return data for calculating alpha, beta, and other
//! relative performance metrics against a reference index (e.g., S&P 500).

use crate::asset::Asset;
use crate::data::bar_reader::{BarReader, SessionLabel};
use crate::error::{Result, ZiplineError};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;

/// Return data point (timestamp, return)
#[derive(Debug, Clone, Copy)]
pub struct BenchmarkReturn {
    pub timestamp: DateTime<Utc>,
    pub returns: f64,
}

impl BenchmarkReturn {
    pub fn new(timestamp: DateTime<Utc>, returns: f64) -> Self {
        Self { timestamp, returns }
    }
}

/// Trait for reading benchmark return data
pub trait BenchmarkReader: Send + Sync {
    /// Get benchmark returns for a date range
    fn get_benchmark_returns(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<BenchmarkReturn>>;

    /// Get single day benchmark return
    fn get_benchmark_return(&self, date: DateTime<Utc>) -> Result<f64> {
        let returns = self.get_benchmark_returns(date, date)?;
        returns
            .first()
            .map(|r| r.returns)
            .ok_or_else(|| ZiplineError::DataNotFound(format!("No benchmark data for {:?}", date)))
    }

    /// Get cumulative benchmark return over period
    fn get_cumulative_return(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<f64> {
        let returns = self.get_benchmark_returns(start, end)?;
        Ok(calculate_cumulative_return(&returns))
    }

    /// Get annualized benchmark return
    fn get_annualized_return(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<f64> {
        let returns = self.get_benchmark_returns(start, end)?;
        Ok(calculate_annualized_return(&returns, start, end))
    }
}

/// S&P 500 (SPY) benchmark - default benchmark
#[derive(Debug, Clone)]
pub struct SPYBenchmark {
    /// Cached return data: date -> return
    returns_cache: HashMap<DateTime<Utc>, f64>,
    /// Bar reader for loading price data
    bar_reader: Arc<dyn BarReader>,
    /// SPY asset
    spy_asset: Asset,
}

impl SPYBenchmark {
    /// Create new SPY benchmark with bar reader
    pub fn new(bar_reader: Arc<dyn BarReader>) -> Result<Self> {
        // Create SPY asset
        let spy_asset = Asset::equity(
            u64::MAX - 1, // Special ID for SPY
            "SPY".to_string(),
            "ARCA".to_string(),
        )
        .with_name("SPDR S&P 500 ETF Trust".to_string());

        Ok(Self {
            returns_cache: HashMap::new(),
            bar_reader,
            spy_asset,
        })
    }

    /// Create with pre-loaded returns data
    pub fn with_returns(returns: Vec<(DateTime<Utc>, f64)>) -> Self {
        let spy_asset = Asset::equity(
            u64::MAX - 1,
            "SPY".to_string(),
            "ARCA".to_string(),
        );

        Self {
            returns_cache: returns.into_iter().collect(),
            bar_reader: Arc::new(crate::data::bar_reader::DailyBarReader::new()),
            spy_asset,
        }
    }

    /// Calculate and cache returns from price data
    fn calculate_returns(&mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<()> {
        let bars = self.bar_reader.get_bars(&self.spy_asset, start, end)?;

        if bars.is_empty() {
            return Err(ZiplineError::DataNotFound(
                "No SPY data available for period".to_string(),
            ));
        }

        // Calculate daily returns
        for window in bars.windows(2) {
            let prev_bar = &window[0];
            let curr_bar = &window[1];

            let daily_return = (curr_bar.close - prev_bar.close) / prev_bar.close;
            self.returns_cache.insert(curr_bar.dt, daily_return);
        }

        // First bar has zero return
        if let Some(first_bar) = bars.first() {
            self.returns_cache.insert(first_bar.dt, 0.0);
        }

        Ok(())
    }
}

impl BenchmarkReader for SPYBenchmark {
    fn get_benchmark_returns(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<BenchmarkReturn>> {
        let mut results = Vec::new();

        // Try to use cache first
        let mut current = start;
        while current <= end {
            if let Some(&returns) = self.returns_cache.get(&current) {
                results.push(BenchmarkReturn::new(current, returns));
            }
            current = current + chrono::Duration::days(1);
        }

        // If cache is empty, need to calculate
        if results.is_empty() {
            return Err(ZiplineError::DataNotFound(
                "SPY benchmark data not available - call calculate_returns first".to_string(),
            ));
        }

        Ok(results)
    }
}

/// Custom asset benchmark - use any asset as benchmark
#[derive(Debug, Clone)]
pub struct AssetBenchmark {
    /// The asset to use as benchmark
    asset: Asset,
    /// Bar reader for loading price data
    bar_reader: Arc<dyn BarReader>,
    /// Cached returns
    returns_cache: HashMap<DateTime<Utc>, f64>,
}

impl AssetBenchmark {
    /// Create new asset benchmark
    pub fn new(asset: Asset, bar_reader: Arc<dyn BarReader>) -> Self {
        Self {
            asset,
            bar_reader,
            returns_cache: HashMap::new(),
        }
    }

    /// Create with pre-calculated returns
    pub fn with_returns(asset: Asset, returns: Vec<(DateTime<Utc>, f64)>) -> Self {
        Self {
            asset: asset.clone(),
            bar_reader: Arc::new(crate::data::bar_reader::DailyBarReader::new()),
            returns_cache: returns.into_iter().collect(),
        }
    }

    /// Calculate returns from price data
    fn calculate_returns(&mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<()> {
        let bars = self.bar_reader.get_bars(&self.asset, start, end)?;

        if bars.is_empty() {
            return Err(ZiplineError::InvalidBenchmarkAsset(format!(
                "No data available for benchmark asset {}",
                self.asset.symbol
            )));
        }

        // Calculate daily returns
        for window in bars.windows(2) {
            let prev_bar = &window[0];
            let curr_bar = &window[1];

            let daily_return = (curr_bar.close - prev_bar.close) / prev_bar.close;
            self.returns_cache.insert(curr_bar.dt, daily_return);
        }

        // First bar has zero return
        if let Some(first_bar) = bars.first() {
            self.returns_cache.insert(first_bar.dt, 0.0);
        }

        Ok(())
    }
}

impl BenchmarkReader for AssetBenchmark {
    fn get_benchmark_returns(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<BenchmarkReturn>> {
        let mut results = Vec::new();

        let mut current = start;
        while current <= end {
            if let Some(&returns) = self.returns_cache.get(&current) {
                results.push(BenchmarkReturn::new(current, returns));
            }
            current = current + chrono::Duration::days(1);
        }

        if results.is_empty() {
            return Err(ZiplineError::InvalidBenchmarkAsset(format!(
                "No benchmark data for {} in specified period",
                self.asset.symbol
            )));
        }

        Ok(results)
    }
}

/// Constant benchmark - fixed return rate (for testing)
#[derive(Debug, Clone)]
pub struct ConstantBenchmark {
    /// Daily return rate
    daily_return: f64,
}

impl ConstantBenchmark {
    /// Create constant benchmark with daily return rate
    pub fn new(daily_return: f64) -> Self {
        Self { daily_return }
    }

    /// Create from annualized return rate
    pub fn from_annualized(annual_return: f64) -> Self {
        let daily_return = (1.0 + annual_return).powf(1.0 / 252.0) - 1.0;
        Self { daily_return }
    }
}

impl BenchmarkReader for ConstantBenchmark {
    fn get_benchmark_returns(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<BenchmarkReturn>> {
        let mut results = Vec::new();
        let mut current = start;

        while current <= end {
            results.push(BenchmarkReturn::new(current, self.daily_return));
            current = current + chrono::Duration::days(1);
        }

        Ok(results)
    }
}

/// Zero benchmark - no benchmark comparison
#[derive(Debug, Clone, Default)]
pub struct ZeroBenchmark;

impl ZeroBenchmark {
    pub fn new() -> Self {
        Self
    }
}

impl BenchmarkReader for ZeroBenchmark {
    fn get_benchmark_returns(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<BenchmarkReturn>> {
        let mut results = Vec::new();
        let mut current = start;

        while current <= end {
            results.push(BenchmarkReturn::new(current, 0.0));
            current = current + chrono::Duration::days(1);
        }

        Ok(results)
    }
}

/// Calculate cumulative return from daily returns
pub fn calculate_cumulative_return(returns: &[BenchmarkReturn]) -> f64 {
    returns
        .iter()
        .fold(1.0, |acc, r| acc * (1.0 + r.returns))
        - 1.0
}

/// Calculate annualized return from daily returns
pub fn calculate_annualized_return(
    returns: &[BenchmarkReturn],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> f64 {
    if returns.is_empty() {
        return 0.0;
    }

    let cumulative = calculate_cumulative_return(returns);
    let days = (end - start).num_days() as f64;

    if days == 0.0 {
        return 0.0;
    }

    let years = days / 365.25;
    (1.0 + cumulative).powf(1.0 / years) - 1.0
}

/// Calculate alpha given portfolio and benchmark returns
pub fn calculate_alpha(portfolio_return: f64, benchmark_return: f64, beta: f64) -> f64 {
    portfolio_return - beta * benchmark_return
}

/// Calculate beta using covariance and variance
pub fn calculate_beta(
    portfolio_returns: &[f64],
    benchmark_returns: &[f64],
) -> Result<f64> {
    if portfolio_returns.len() != benchmark_returns.len() {
        return Err(ZiplineError::DataError(
            "Portfolio and benchmark returns must have same length".to_string(),
        ));
    }

    if portfolio_returns.is_empty() {
        return Ok(0.0);
    }

    // Calculate means
    let portfolio_mean = portfolio_returns.iter().sum::<f64>() / portfolio_returns.len() as f64;
    let benchmark_mean = benchmark_returns.iter().sum::<f64>() / benchmark_returns.len() as f64;

    // Calculate covariance
    let covariance = portfolio_returns
        .iter()
        .zip(benchmark_returns.iter())
        .map(|(p, b)| (p - portfolio_mean) * (b - benchmark_mean))
        .sum::<f64>()
        / portfolio_returns.len() as f64;

    // Calculate benchmark variance
    let benchmark_variance = benchmark_returns
        .iter()
        .map(|b| (b - benchmark_mean).powi(2))
        .sum::<f64>()
        / benchmark_returns.len() as f64;

    if benchmark_variance == 0.0 {
        return Ok(0.0);
    }

    Ok(covariance / benchmark_variance)
}

/// Calculate information ratio (alpha / tracking error)
pub fn calculate_information_ratio(
    portfolio_returns: &[f64],
    benchmark_returns: &[f64],
) -> Result<f64> {
    if portfolio_returns.len() != benchmark_returns.len() {
        return Err(ZiplineError::DataError(
            "Portfolio and benchmark returns must have same length".to_string(),
        ));
    }

    if portfolio_returns.is_empty() {
        return Ok(0.0);
    }

    // Calculate excess returns
    let excess_returns: Vec<f64> = portfolio_returns
        .iter()
        .zip(benchmark_returns.iter())
        .map(|(p, b)| p - b)
        .collect();

    // Mean excess return (alpha)
    let mean_excess = excess_returns.iter().sum::<f64>() / excess_returns.len() as f64;

    // Tracking error (std dev of excess returns)
    let tracking_error = {
        let variance = excess_returns
            .iter()
            .map(|e| (e - mean_excess).powi(2))
            .sum::<f64>()
            / excess_returns.len() as f64;
        variance.sqrt()
    };

    if tracking_error == 0.0 {
        return Ok(0.0);
    }

    // Annualize
    Ok(mean_excess / tracking_error * (252.0_f64).sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::bar_reader::{Bar, DailyBarReader};

    fn create_test_bars() -> Vec<Bar> {
        let start = Utc::now();
        vec![
            Bar::new(100.0, 105.0, 99.0, 100.0, 1000.0, start),
            Bar::new(100.0, 106.0, 99.0, 102.0, 1100.0, start + chrono::Duration::days(1)),
            Bar::new(102.0, 107.0, 101.0, 105.0, 1200.0, start + chrono::Duration::days(2)),
            Bar::new(105.0, 108.0, 104.0, 106.0, 1300.0, start + chrono::Duration::days(3)),
        ]
    }

    #[test]
    fn test_zero_benchmark() {
        let benchmark = ZeroBenchmark::new();
        let start = Utc::now();
        let end = start + chrono::Duration::days(5);

        let returns = benchmark.get_benchmark_returns(start, end).unwrap();
        assert_eq!(returns.len(), 6); // 6 days including start and end
        assert!(returns.iter().all(|r| r.returns == 0.0));
    }

    #[test]
    fn test_constant_benchmark() {
        let benchmark = ConstantBenchmark::new(0.01); // 1% daily
        let start = Utc::now();
        let end = start + chrono::Duration::days(2);

        let returns = benchmark.get_benchmark_returns(start, end).unwrap();
        assert_eq!(returns.len(), 3);
        assert!(returns.iter().all(|r| r.returns == 0.01));
    }

    #[test]
    fn test_constant_benchmark_annualized() {
        let benchmark = ConstantBenchmark::from_annualized(0.10); // 10% annual
        let start = Utc::now();
        let end = start + chrono::Duration::days(1);

        let returns = benchmark.get_benchmark_returns(start, end).unwrap();
        assert_eq!(returns.len(), 2);
        // Daily return should be approximately 0.04% for 10% annual
        assert!(returns[0].returns > 0.0 && returns[0].returns < 0.001);
    }

    #[test]
    fn test_asset_benchmark() {
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        let start = Utc::now();

        let returns = vec![
            (start, 0.0),
            (start + chrono::Duration::days(1), 0.02),
            (start + chrono::Duration::days(2), 0.015),
        ];

        let benchmark = AssetBenchmark::with_returns(asset, returns);
        let end = start + chrono::Duration::days(2);

        let result = benchmark.get_benchmark_returns(start, end).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].returns, 0.0);
        assert_eq!(result[1].returns, 0.02);
        assert_eq!(result[2].returns, 0.015);
    }

    #[test]
    fn test_cumulative_return() {
        let start = Utc::now();
        let returns = vec![
            BenchmarkReturn::new(start, 0.01),
            BenchmarkReturn::new(start + chrono::Duration::days(1), 0.02),
            BenchmarkReturn::new(start + chrono::Duration::days(2), -0.01),
        ];

        let cumulative = calculate_cumulative_return(&returns);
        // (1.01 * 1.02 * 0.99) - 1 ≈ 0.0198
        assert!((cumulative - 0.0198).abs() < 0.0001);
    }

    #[test]
    fn test_calculate_beta() {
        let portfolio_returns = vec![0.01, 0.02, -0.01, 0.015];
        let benchmark_returns = vec![0.005, 0.015, -0.005, 0.01];

        let beta = calculate_beta(&portfolio_returns, &benchmark_returns).unwrap();
        // Beta should be positive and around 1.0-2.0 for these returns
        assert!(beta > 0.0);
        assert!(beta < 3.0);
    }

    #[test]
    fn test_calculate_alpha() {
        let portfolio_return = 0.15;
        let benchmark_return = 0.10;
        let beta = 1.2;

        let alpha = calculate_alpha(portfolio_return, benchmark_return, beta);
        // Alpha = 0.15 - 1.2 * 0.10 = 0.03
        assert!((alpha - 0.03).abs() < 0.0001);
    }

    #[test]
    fn test_calculate_information_ratio() {
        let portfolio_returns = vec![0.01, 0.02, 0.015, 0.012];
        let benchmark_returns = vec![0.008, 0.015, 0.01, 0.01];

        let ir = calculate_information_ratio(&portfolio_returns, &benchmark_returns).unwrap();
        // Should be positive since portfolio outperforms
        assert!(ir > 0.0);
    }

    #[test]
    fn test_spy_benchmark_with_returns() {
        let start = Utc::now();
        let returns = vec![
            (start, 0.0),
            (start + chrono::Duration::days(1), 0.008),
            (start + chrono::Duration::days(2), 0.012),
        ];

        let benchmark = SPYBenchmark::with_returns(returns);
        let end = start + chrono::Duration::days(2);

        let result = benchmark.get_benchmark_returns(start, end).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_annualized_return() {
        let start = Utc::now();
        let end = start + chrono::Duration::days(365);

        let returns = vec![BenchmarkReturn::new(start, 0.20)]; // 20% total return

        let annualized = calculate_annualized_return(&returns, start, end);
        // Should be approximately 20% annualized
        assert!((annualized - 0.20).abs() < 0.01);
    }
}
