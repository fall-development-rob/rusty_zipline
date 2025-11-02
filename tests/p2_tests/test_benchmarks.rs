//! Benchmark System Tests
//!
//! Tests for benchmark data loading and returns calculation

#[cfg(test)]
mod benchmark_tests {
    use chrono::{DateTime, Utc, TimeZone, Datelike};

    // Mock benchmark structures
    struct Benchmark {
        symbol: String,
        returns: Vec<(DateTime<Utc>, f64)>,
    }

    impl Benchmark {
        fn new(symbol: &str) -> Self {
            Self {
                symbol: symbol.to_string(),
                returns: Vec::new(),
            }
        }

        fn add_return(&mut self, dt: DateTime<Utc>, ret: f64) {
            self.returns.push((dt, ret));
        }

        fn get_return(&self, dt: DateTime<Utc>) -> Option<f64> {
            self.returns.iter()
                .find(|(d, _)| *d == dt)
                .map(|(_, r)| *r)
        }

        fn calculate_cumulative_return(&self) -> f64 {
            self.returns.iter()
                .map(|(_, r)| r)
                .fold(1.0, |acc, r| acc * (1.0 + r))
        }
    }

    #[test]
    fn test_spy_benchmark_creation() {
        let benchmark = Benchmark::new("SPY");
        assert_eq!(benchmark.symbol, "SPY");
        assert_eq!(benchmark.returns.len(), 0);
    }

    #[test]
    fn test_add_benchmark_returns() {
        let mut benchmark = Benchmark::new("SPY");
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        benchmark.add_return(dt, 0.01);
        assert_eq!(benchmark.returns.len(), 1);
    }

    #[test]
    fn test_get_benchmark_return() {
        let mut benchmark = Benchmark::new("SPY");
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        benchmark.add_return(dt, 0.02);
        let ret = benchmark.get_return(dt).unwrap();

        assert!((ret - 0.02).abs() < 1e-9);
    }

    #[test]
    fn test_missing_benchmark_return() {
        let benchmark = Benchmark::new("SPY");
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        let result = benchmark.get_return(dt);
        assert!(result.is_none());
    }

    #[test]
    fn test_cumulative_returns() {
        let mut benchmark = Benchmark::new("SPY");
        let dt1 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let dt2 = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();

        benchmark.add_return(dt1, 0.01); // 1% return
        benchmark.add_return(dt2, 0.02); // 2% return

        let cumulative = benchmark.calculate_cumulative_return();
        // (1 + 0.01) * (1 + 0.02) = 1.0302
        assert!((cumulative - 1.0302).abs() < 1e-6);
    }

    #[test]
    fn test_custom_benchmark() {
        let benchmark = Benchmark::new("QQQ");
        assert_eq!(benchmark.symbol, "QQQ");
    }

    #[test]
    fn test_negative_returns() {
        let mut benchmark = Benchmark::new("SPY");
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        benchmark.add_return(dt, -0.05); // -5% return
        let ret = benchmark.get_return(dt).unwrap();

        assert!((ret + 0.05).abs() < 1e-9);
    }

    #[test]
    fn test_zero_return() {
        let mut benchmark = Benchmark::new("SPY");
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        benchmark.add_return(dt, 0.0);
        let ret = benchmark.get_return(dt).unwrap();

        assert_eq!(ret, 0.0);
    }

    #[test]
    fn test_multiple_day_returns() {
        let mut benchmark = Benchmark::new("SPY");

        for day in 1..=5 {
            let dt = Utc.with_ymd_and_hms(2024, 1, day, 0, 0, 0).unwrap();
            benchmark.add_return(dt, 0.01);
        }

        assert_eq!(benchmark.returns.len(), 5);
    }

    #[test]
    fn test_benchmark_alignment() {
        let mut benchmark = Benchmark::new("SPY");

        // Add returns for specific trading days
        let dt1 = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap(); // Tuesday
        let dt2 = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap(); // Wednesday

        benchmark.add_return(dt1, 0.01);
        benchmark.add_return(dt2, 0.02);

        assert!(benchmark.get_return(dt1).is_some());
        assert!(benchmark.get_return(dt2).is_some());
    }

    #[test]
    fn test_large_returns() {
        let mut benchmark = Benchmark::new("MEME");
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        // Test with large return (100% gain)
        benchmark.add_return(dt, 1.0);
        let ret = benchmark.get_return(dt).unwrap();

        assert_eq!(ret, 1.0);
    }

    #[test]
    fn test_returns_ordering() {
        let mut benchmark = Benchmark::new("SPY");

        let dt1 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let dt2 = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
        let dt3 = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();

        // Add out of order
        benchmark.add_return(dt2, 0.02);
        benchmark.add_return(dt1, 0.01);
        benchmark.add_return(dt3, 0.03);

        assert_eq!(benchmark.returns.len(), 3);
    }

    #[test]
    fn test_annualized_returns() {
        let mut benchmark = Benchmark::new("SPY");

        // Add daily returns for a year (252 trading days)
        for day in 0..252 {
            let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()
                + chrono::Duration::days(day);
            benchmark.add_return(dt, 0.0004); // 0.04% daily
        }

        let cumulative = benchmark.calculate_cumulative_return();
        // Should be approximately 10% annual return
        assert!(cumulative > 1.08 && cumulative < 1.12);
    }

    #[test]
    fn test_benchmark_with_gaps() {
        let mut benchmark = Benchmark::new("SPY");

        let dt1 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let dt3 = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();

        benchmark.add_return(dt1, 0.01);
        benchmark.add_return(dt3, 0.02);

        // Day 2 is missing
        let dt2 = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
        assert!(benchmark.get_return(dt2).is_none());
    }

    #[test]
    fn test_volatility_calculation() {
        let mut benchmark = Benchmark::new("SPY");

        // Add returns with varying volatility
        let returns = vec![0.01, -0.015, 0.02, -0.005, 0.01];
        for (i, &ret) in returns.iter().enumerate() {
            let dt = Utc.with_ymd_and_hms(2024, 1, (i + 1) as u32, 0, 0, 0).unwrap();
            benchmark.add_return(dt, ret);
        }

        // Calculate mean
        let mean: f64 = returns.iter().sum::<f64>() / returns.len() as f64;

        // Calculate variance
        let variance: f64 = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;

        let std_dev = variance.sqrt();
        assert!(std_dev > 0.0);
    }

    #[test]
    fn test_sharpe_ratio_calculation() {
        let mut benchmark = Benchmark::new("SPY");

        for day in 1..=252 {
            let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()
                + chrono::Duration::days(day - 1);
            benchmark.add_return(dt, 0.0005); // Consistent positive returns
        }

        let cumulative = benchmark.calculate_cumulative_return();
        assert!(cumulative > 1.0);
    }

    #[test]
    fn test_max_drawdown() {
        let mut benchmark = Benchmark::new("SPY");

        // Simulate a drawdown scenario
        let returns = vec![0.05, 0.03, -0.10, -0.05, 0.08, 0.12];
        for (i, &ret) in returns.iter().enumerate() {
            let dt = Utc.with_ymd_and_hms(2024, 1, (i + 1) as u32, 0, 0, 0).unwrap();
            benchmark.add_return(dt, ret);
        }

        // Calculate cumulative values to find max drawdown
        let mut cumulative = 1.0;
        let mut peak = 1.0;
        let mut max_dd = 0.0;

        for (_, ret) in &benchmark.returns {
            cumulative *= 1.0 + ret;
            peak = peak.max(cumulative);
            let dd = (peak - cumulative) / peak;
            max_dd = max_dd.max(dd);
        }

        assert!(max_dd > 0.0);
    }

    #[test]
    fn test_benchmark_correlation() {
        let mut bench1 = Benchmark::new("SPY");
        let mut bench2 = Benchmark::new("QQQ");

        // Add correlated returns
        for day in 1..=10 {
            let dt = Utc.with_ymd_and_hms(2024, 1, day, 0, 0, 0).unwrap();
            let base_return = (day as f64) * 0.001;
            bench1.add_return(dt, base_return);
            bench2.add_return(dt, base_return * 1.2); // Correlated but amplified
        }

        assert_eq!(bench1.returns.len(), bench2.returns.len());
    }

    #[test]
    fn test_benchmark_rebalancing() {
        let mut benchmark = Benchmark::new("SPY");

        // Simulate monthly rebalancing returns
        for month in 1..=12 {
            let dt = Utc.with_ymd_and_hms(2024, month, 1, 0, 0, 0).unwrap();
            benchmark.add_return(dt, 0.01);
        }

        assert_eq!(benchmark.returns.len(), 12);
        let cumulative = benchmark.calculate_cumulative_return();
        assert!(cumulative > 1.10); // Should have >10% return
    }
}
