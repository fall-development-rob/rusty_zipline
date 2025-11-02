//! Integration tests for Algorithm API P0 features
//!
//! Tests the integration of:
//! 1. Account object
//! 2. Recording API
//! 3. Multi-asset configuration

use rusty_zipline::algorithm::{Algorithm, AssetClassConfig, Context, TradingAlgorithm};
use rusty_zipline::asset::{Asset, AssetType};
use rusty_zipline::assets::AssetFinder;
use rusty_zipline::data::BarData;
use rusty_zipline::error::Result;
use rusty_zipline::finance::{
    Account, NoSlippage, PerShare, Portfolio, Position, VolumeShareSlippage, ZeroCommission,
};
use rusty_zipline::performance::PerformanceTracker;
use std::sync::Arc;

#[test]
fn test_account_updates_with_portfolio() {
    // Create context with initial capital
    let mut context = Context::new(100000.0);

    // Add a position to portfolio
    let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
    let position = Position::new(asset, 100.0, 15000.0, 150.0);
    context.portfolio.positions.insert(1, position);
    context.portfolio.cash = 85000.0;
    context.portfolio.positions_value = 15000.0;
    context.portfolio.portfolio_value = 100000.0;

    // Update account from portfolio
    context.update_account();

    // Verify account metrics match portfolio
    assert_eq!(context.account.net_liquidation, 100000.0);
    assert_eq!(context.account.settled_cash, 85000.0);
    assert_eq!(context.account.total_positions_value, 15000.0);
    assert!(context.account.leverage > 0.0);
}

#[test]
fn test_recording_with_account_metrics() {
    let mut context = Context::new(100000.0);

    // Add position
    let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
    let position = Position::new(asset, 200.0, 20000.0, 100.0);
    context.portfolio.positions.insert(1, position);
    context.portfolio.cash = 80000.0;
    context.portfolio.positions_value = 20000.0;
    context.portfolio.portfolio_value = 100000.0;

    // Update account
    context.update_account();

    // Record account metrics
    context.record("leverage", context.account.leverage);
    context.record("buying_power", context.account.buying_power);
    context.record("cushion", context.account.cushion);
    context.record("num_positions", context.portfolio.num_positions() as f64);

    // Verify all metrics were recorded
    assert!(context.get_recorded("leverage").is_some());
    assert!(context.get_recorded("buying_power").is_some());
    assert!(context.get_recorded("cushion").is_some());
    assert_eq!(context.get_latest_recorded("num_positions"), Some(1.0));

    // Verify we can track all variable names
    let names = context.recorded_variable_names();
    assert_eq!(names.len(), 4);
}

#[test]
fn test_multi_asset_with_different_models() {
    let asset_finder = Arc::new(AssetFinder::new());
    let mut algo = TradingAlgorithm::new(asset_finder);

    // Configure different models for different asset classes
    algo.set_equities_models(
        Arc::new(VolumeShareSlippage::new(0.1, 0.05)),
        Arc::new(PerShare::new(0.001, 1.0)),
    )
    .unwrap();

    algo.set_futures_models(Arc::new(NoSlippage), Arc::new(ZeroCommission))
        .unwrap();

    // Verify equity asset gets equity models
    let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let equity = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
    assert!(algo.get_slippage_for_asset(&equity).is_some());
    assert!(algo.get_commission_for_asset(&equity).is_some());

    // Verify future asset gets future models
    let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let future = Asset::new(2, "ES".to_string(), "CME".to_string(), AssetType::Future, start_date);
    assert!(algo.get_slippage_for_asset(&future).is_some());
    assert!(algo.get_commission_for_asset(&future).is_some());

    // Verify crypto falls back to default (None in this case)
    let crypto = Asset::new(
        3,
        "BTC".to_string(),
        "COINBASE".to_string(),
        AssetType::Crypto,
    );
    // Should be None since we didn't set a default
    assert!(algo.get_slippage_for_asset(&crypto).is_none());
}

#[test]
fn test_performance_tracker_integration() {
    let mut tracker = PerformanceTracker::new();
    let mut context = Context::new(100000.0);

    // Simulate trading activity
    context.record("signal_strength", 0.75);
    context.record("confidence", 0.85);
    context.record("position_count", 0.0);

    // Add position
    let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
    let position = Position::new(asset, 100.0, 10000.0, 100.0);
    context.portfolio.positions.insert(1, position);
    context.update_account();

    // Record more metrics
    context.record("position_count", 1.0);
    context.record("leverage", context.account.leverage);

    // Transfer recorded vars to performance tracker
    tracker.update_recorded_vars(&context.recorded_vars);

    // Verify tracker has all recorded variables
    assert_eq!(tracker.num_recorded_vars(), 4);
    assert!(tracker.get_recorded("signal_strength").is_some());
    assert!(tracker.get_recorded("leverage").is_some());
}

#[test]
fn test_full_workflow_all_features() {
    // Setup
    let asset_finder = Arc::new(AssetFinder::new());
    let mut algo = TradingAlgorithm::new(asset_finder.clone());
    let mut context = Context::new(100000.0);
    let mut tracker = PerformanceTracker::new();

    // Configure multi-asset models
    algo.set_equities_models(
        Arc::new(VolumeShareSlippage::new(0.05, 0.05)),
        Arc::new(PerShare::new(0.005, 1.0)),
    )
    .unwrap();

    // Simulate first bar
    let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let equity = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
    let position = Position::new(equity.clone(), 50.0, 5000.0, 100.0);
    context.portfolio.positions.insert(1, position);
    context.portfolio.cash = 95000.0;
    context.portfolio.positions_value = 5000.0;
    context.portfolio.portfolio_value = 100000.0;

    // Update account
    context.update_account();

    // Record metrics
    context.record("portfolio_value", context.portfolio.portfolio_value);
    context.record("leverage", context.account.leverage);
    context.record("buying_power", context.account.buying_power);

    // Verify account is updated
    assert_eq!(context.account.net_liquidation, 100000.0);
    assert!(context.account.leverage > 0.0);

    // Verify recording works
    assert_eq!(context.recorded_variable_names().len(), 3);

    // Verify multi-asset config works
    assert!(algo.get_slippage_for_asset(&equity).is_some());
    assert!(algo.get_commission_for_asset(&equity).is_some());

    // Transfer to performance tracker
    tracker.update_recorded_vars(&context.recorded_vars);
    assert_eq!(tracker.num_recorded_vars(), 3);
}

#[test]
fn test_account_margin_requirements() {
    let mut context = Context::new(100000.0);

    // Add a large position (should trigger margin requirements)
    let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
    let position = Position::new(asset, 1000.0, 100000.0, 100.0);
    context.portfolio.positions.insert(1, position);
    context.portfolio.cash = 0.0;
    context.portfolio.positions_value = 100000.0;
    context.portfolio.portfolio_value = 100000.0;

    context.update_account();

    // Verify margin requirements are calculated
    assert!(context.account.initial_margin_requirement > 0.0);
    assert!(context.account.maintenance_margin_requirement > 0.0);
    assert!(context.account.initial_margin_requirement > context.account.maintenance_margin_requirement);

    // Record margin metrics
    context.record("initial_margin", context.account.initial_margin_requirement);
    context.record("maintenance_margin", context.account.maintenance_margin_requirement);
    context.record("excess_liquidity", context.account.excess_liquidity);

    assert!(context.get_recorded("initial_margin").is_some());
}

#[test]
fn test_recording_time_series() {
    let mut context = Context::new(100000.0);

    // Simulate multiple time periods
    for i in 0..10 {
        let value = 100.0 + (i as f64 * 5.0);
        context.record("price", value);
        context.record("signal", (i as f64) / 10.0);
    }

    // Verify time series data
    let prices = context.get_recorded("price").unwrap();
    assert_eq!(prices.len(), 10);
    assert_eq!(prices[0].1, 100.0);
    assert_eq!(prices[9].1, 145.0);

    let signals = context.get_recorded("signal").unwrap();
    assert_eq!(signals.len(), 10);
    assert_eq!(signals[0].1, 0.0);
    assert_eq!(signals[9].1, 0.9);
}

#[test]
fn test_multi_asset_class_config() {
    let asset_finder = Arc::new(AssetFinder::new());
    let mut algo = TradingAlgorithm::new(asset_finder);

    // Configure all major asset classes
    algo.set_equities_models(
        Arc::new(VolumeShareSlippage::new(0.1, 0.05)),
        Arc::new(PerShare::new(0.001, 1.0)),
    )
    .unwrap();

    algo.set_futures_models(Arc::new(NoSlippage), Arc::new(PerShare::new(2.50, 0.0)))
        .unwrap();

    algo.set_slippage_by_class(AssetType::Crypto, Arc::new(VolumeShareSlippage::new(0.2, 0.1)))
        .unwrap();

    // Verify all asset types are configured
    let types = algo.configured_asset_types();
    assert!(types.contains(&AssetType::Equity));
    assert!(types.contains(&AssetType::Future));
    assert!(types.contains(&AssetType::Crypto));
}

#[test]
fn test_account_buying_power_checks() {
    let mut context = Context::new(100000.0);
    context.update_account();

    // Should have full buying power initially
    assert!(context.account.has_buying_power(100000.0));
    assert!(!context.account.has_buying_power(150000.0));

    // Add position to reduce available buying power
    let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
    let position = Position::new(asset, 500.0, 50000.0, 100.0);
    context.portfolio.positions.insert(1, position);
    context.portfolio.cash = 50000.0;
    context.portfolio.positions_value = 50000.0;
    context.portfolio.portfolio_value = 100000.0;

    context.update_account();

    // Buying power should still be positive but different
    assert!(context.account.buying_power > 0.0);

    // Record buying power over time
    context.record("buying_power", context.account.buying_power);
    assert!(context.get_latest_recorded("buying_power").is_some());
}

#[test]
fn test_recorded_vars_cleared_correctly() {
    let mut context = Context::new(100000.0);

    // Record multiple variables
    context.record("temp_signal", 1.0);
    context.record("permanent_metric", 2.0);

    assert_eq!(context.recorded_variable_names().len(), 2);

    // Clear one variable
    context.clear_recorded("temp_signal");

    assert_eq!(context.recorded_variable_names().len(), 1);
    assert!(context.get_recorded("temp_signal").is_none());
    assert!(context.get_recorded("permanent_metric").is_some());
}

#[test]
fn test_account_leverage_calculation() {
    let mut context = Context::new(100000.0);

    // Add long position worth 50% of portfolio
    let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset1 = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
    let position1 = Position::new(asset1, 500.0, 50000.0, 100.0);
    context.portfolio.positions.insert(1, position1);

    context.portfolio.cash = 50000.0;
    context.portfolio.positions_value = 50000.0;
    context.portfolio.portfolio_value = 100000.0;

    context.update_account();

    // Leverage should be 0.5 (50% exposure)
    assert!((context.account.leverage - 0.5).abs() < 0.01);

    // Record leverage
    context.record("leverage", context.account.leverage);
    assert_eq!(
        context.get_latest_recorded("leverage"),
        Some(context.account.leverage)
    );
}
