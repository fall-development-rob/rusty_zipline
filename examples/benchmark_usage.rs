//! Example demonstrating benchmark system usage
//!
//! Shows how to:
//! - Create different benchmark types
//! - Calculate returns and metrics
//! - Compare portfolio performance against benchmarks

use chrono::{DateTime, TimeZone, Utc};
use rusty_zipline::asset::Asset;
use rusty_zipline::data::bar_reader::{Bar, BarReader, DailyBarReader};
use rusty_zipline::data::benchmarks::{
    calculate_alpha, calculate_beta, calculate_information_ratio, AssetBenchmark,
    BenchmarkReader, ConstantBenchmark, SPYBenchmark, ZeroBenchmark,
};
use rusty_zipline::error::Result;
use std::sync::Arc;

fn main() -> Result<()> {
    println!("=== Benchmark System Examples ===\n");

    // Example 1: Zero Benchmark
    zero_benchmark_example()?;

    // Example 2: Constant Benchmark
    constant_benchmark_example()?;

    // Example 3: Asset Benchmark
    asset_benchmark_example()?;

    // Example 4: Performance Metrics
    performance_metrics_example()?;

    Ok(())
}

fn zero_benchmark_example() -> Result<()> {
    println!("1. Zero Benchmark Example");
    println!("   Use when no benchmark comparison is desired\n");

    let benchmark = ZeroBenchmark::new();
    let start = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2020, 1, 5, 0, 0, 0).unwrap();

    let returns = benchmark.get_benchmark_returns(start, end)?;
    println!("   Returns for 5 days:");
    for ret in &returns {
        println!("     {:?}: {:.4}", ret.timestamp, ret.returns);
    }
    println!("   All returns are zero (no benchmark)\n");

    Ok(())
}

fn constant_benchmark_example() -> Result<()> {
    println!("2. Constant Benchmark Example");
    println!("   Use for fixed risk-free rate or testing\n");

    // Create benchmark with 5% annualized return
    let benchmark = ConstantBenchmark::from_annualized(0.05);
    let start = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2020, 1, 3, 0, 0, 0).unwrap();

    let returns = benchmark.get_benchmark_returns(start, end)?;
    println!("   5% Annual Return = ~0.02% Daily:");
    for ret in &returns {
        println!("     {:?}: {:.6}", ret.timestamp, ret.returns);
    }

    let cumulative = benchmark.get_cumulative_return(start, end)?;
    println!("   Cumulative return: {:.4}%\n", cumulative * 100.0);

    Ok(())
}

fn asset_benchmark_example() -> Result<()> {
    println!("3. Asset Benchmark Example");
    println!("   Use any asset as benchmark (e.g., QQQ for tech-heavy strategy)\n");

    // Create sample asset and returns
    let qqq_asset = Asset::equity(1, "QQQ".to_string(), "NASDAQ".to_string())
        .with_name("Invesco QQQ Trust".to_string());

    let start = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();

    // Sample returns data
    let returns_data = vec![
        (start, 0.0),
        (start + chrono::Duration::days(1), 0.015), // +1.5%
        (start + chrono::Duration::days(2), 0.008), // +0.8%
        (start + chrono::Duration::days(3), -0.005), // -0.5%
        (start + chrono::Duration::days(4), 0.012), // +1.2%
    ];

    let benchmark = AssetBenchmark::with_returns(qqq_asset.clone(), returns_data);
    let end = start + chrono::Duration::days(4);

    let returns = benchmark.get_benchmark_returns(start, end)?;
    println!("   QQQ Returns:");
    for ret in &returns {
        println!("     {:?}: {:.3}%", ret.timestamp, ret.returns * 100.0);
    }

    let cumulative = benchmark.get_cumulative_return(start, end)?;
    let annualized = benchmark.get_annualized_return(start, end)?;

    println!("\n   Cumulative return: {:.2}%", cumulative * 100.0);
    println!("   Annualized return: {:.2}%\n", annualized * 100.0);

    Ok(())
}

fn performance_metrics_example() -> Result<()> {
    println!("4. Performance Metrics Example");
    println!("   Calculate alpha, beta, and information ratio\n");

    // Sample portfolio and benchmark returns
    let portfolio_returns = vec![
        0.015, // Day 1: +1.5%
        0.022, // Day 2: +2.2%
        -0.008, // Day 3: -0.8%
        0.018, // Day 4: +1.8%
        0.012, // Day 5: +1.2%
    ];

    let benchmark_returns = vec![
        0.010, // Day 1: +1.0%
        0.015, // Day 2: +1.5%
        -0.005, // Day 3: -0.5%
        0.012, // Day 4: +1.2%
        0.008, // Day 5: +0.8%
    ];

    // Calculate beta (portfolio sensitivity to benchmark)
    let beta = calculate_beta(&portfolio_returns, &benchmark_returns)?;
    println!("   Beta: {:.2}", beta);
    println!("     (Portfolio is {:.0}% as volatile as benchmark)", beta * 100.0);

    // Calculate alpha (excess return after adjusting for risk)
    let portfolio_total = portfolio_returns.iter().sum::<f64>() / portfolio_returns.len() as f64;
    let benchmark_total = benchmark_returns.iter().sum::<f64>() / benchmark_returns.len() as f64;
    let alpha = calculate_alpha(portfolio_total, benchmark_total, beta);

    println!("\n   Alpha: {:.2}%", alpha * 100.0);
    if alpha > 0.0 {
        println!("     (Portfolio outperformed risk-adjusted benchmark)");
    } else {
        println!("     (Portfolio underperformed risk-adjusted benchmark)");
    }

    // Calculate information ratio (risk-adjusted excess return)
    let ir = calculate_information_ratio(&portfolio_returns, &benchmark_returns)?;
    println!("\n   Information Ratio: {:.2}", ir);
    match ir {
        x if x > 0.5 => println!("     (Excellent - Strong risk-adjusted outperformance)"),
        x if x > 0.0 => println!("     (Good - Positive risk-adjusted outperformance)"),
        x if x > -0.5 => println!("     (Fair - Slight underperformance)"),
        _ => println!("     (Poor - Significant underperformance)"),
    }

    println!("\n   Summary Statistics:");
    println!(
        "     Portfolio Avg Return: {:.2}%",
        portfolio_total * 100.0
    );
    println!(
        "     Benchmark Avg Return: {:.2}%",
        benchmark_total * 100.0
    );
    println!(
        "     Excess Return: {:.2}%",
        (portfolio_total - benchmark_total) * 100.0
    );

    Ok(())
}

// Example: SPY Benchmark (requires actual data)
#[allow(dead_code)]
fn spy_benchmark_example() -> Result<()> {
    println!("5. SPY Benchmark Example (S&P 500)");
    println!("   Default benchmark for most strategies\n");

    // Create bar reader with SPY data
    let mut bar_reader = DailyBarReader::new();

    // Create SPY asset
    let spy_asset = Asset::equity(u64::MAX - 1, "SPY".to_string(), "ARCA".to_string());

    // Sample SPY data (in practice, load from data bundle)
    let start = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    let bars = vec![
        Bar::new(300.0, 305.0, 299.0, 304.0, 100000000.0, start),
        Bar::new(
            304.0,
            308.0,
            303.0,
            307.0,
            110000000.0,
            start + chrono::Duration::days(1),
        ),
        Bar::new(
            307.0,
            310.0,
            306.0,
            308.0,
            105000000.0,
            start + chrono::Duration::days(2),
        ),
    ];

    bar_reader.load_from_memory(spy_asset.id, bars)?;

    let bar_reader_arc: Arc<dyn BarReader> = Arc::new(bar_reader);
    let mut spy_benchmark = SPYBenchmark::new(bar_reader_arc)?;

    // Calculate returns
    let end = start + chrono::Duration::days(2);
    spy_benchmark.calculate_returns(start, end)?;

    let returns = spy_benchmark.get_benchmark_returns(start, end)?;
    println!("   SPY Returns:");
    for ret in &returns {
        println!("     {:?}: {:.3}%", ret.timestamp, ret.returns * 100.0);
    }

    Ok(())
}

// Example: Comparing Multiple Strategies
#[allow(dead_code)]
fn multi_strategy_comparison() -> Result<()> {
    println!("6. Multi-Strategy Comparison");
    println!("   Compare multiple strategies against benchmark\n");

    let start = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    let end = start + chrono::Duration::days(100);

    // Create benchmark (10% annual return)
    let benchmark = ConstantBenchmark::from_annualized(0.10);

    // Strategy returns
    let strategies = vec![
        ("Conservative", vec![0.008, 0.006, 0.007, 0.009, 0.008]),
        ("Moderate", vec![0.012, 0.015, -0.005, 0.014, 0.011]),
        ("Aggressive", vec![0.025, -0.015, 0.030, -0.010, 0.020]),
    ];

    let benchmark_returns = vec![0.010, 0.010, 0.010, 0.010, 0.010]; // ~10% annual

    println!("   Strategy Comparison:");
    println!("   {:<15} {:>8} {:>8} {:>8}", "Strategy", "Beta", "Alpha", "Info Ratio");
    println!("   {}", "-".repeat(45));

    for (name, returns) in strategies {
        let beta = calculate_beta(&returns, &benchmark_returns)?;
        let avg_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let avg_benchmark = benchmark_returns.iter().sum::<f64>() / benchmark_returns.len() as f64;
        let alpha = calculate_alpha(avg_return, avg_benchmark, beta);
        let ir = calculate_information_ratio(&returns, &benchmark_returns)?;

        println!(
            "   {:<15} {:>8.2} {:>7.2}% {:>8.2}",
            name,
            beta,
            alpha * 100.0,
            ir
        );
    }

    println!();
    Ok(())
}
