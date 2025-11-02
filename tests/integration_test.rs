//! Integration tests for zipline-rust

// Include P2 test modules
mod p2_tests;

use chrono::{Duration, Utc};
use std::sync::Arc;
use rusty_zipline::{
    algorithm::{Algorithm, BuyAndHold, Context},
    asset::Asset,
    calendar::NYSECalendar,
    data::{BarData, InMemoryDataSource},
    engine::{EngineConfig, SimulationEngine},
    execution::SimulatedBroker,
    types::Bar,
};

#[test]
fn test_buy_and_hold_strategy() {
    // Setup
    let asset = Asset::equity(1, "TEST".to_string(), "NASDAQ".to_string());
    let mut data_source = InMemoryDataSource::new();
    data_source.add_asset(asset.clone());

    let start = Utc::now();
    let end = start + Duration::days(10);

    // Add test data
    for i in 0..10 {
        let timestamp = start + Duration::days(i);
        let bar = Bar::new(timestamp, 100.0, 105.0, 99.0, 103.0, 10000.0);
        data_source.add_bar(asset.id, bar);
    }

    data_source.set_date_range(start, end);

    // Run backtest
    let config = EngineConfig {
        starting_cash: 10_000.0,
        max_history_len: 100,
    };

    let calendar = Arc::new(NYSECalendar::new());
    let broker = SimulatedBroker::default_broker();
    let mut engine = SimulationEngine::new(config, broker, calendar);

    let mut algorithm = BuyAndHold::new(asset);
    let result = engine.run(&mut algorithm, &data_source, start, end);

    assert!(result.is_ok());
}

#[test]
fn test_order_execution() {
    let asset = Asset::equity(1, "TEST".to_string(), "NASDAQ".to_string());
    let mut context = Context::new(10_000.0);

    // Place order
    let order_result = context.order(asset, 10.0);
    assert!(order_result.is_ok());
    assert_eq!(context.pending_orders_count(), 1);

    // Check order details
    let order = &context.pending_orders[0];
    assert_eq!(order.quantity, 10.0);
    assert_eq!(order.filled, 0.0);
}

#[test]
fn test_portfolio_value_tracking() {
    let mut data_source = InMemoryDataSource::new();
    let asset = Asset::equity(1, "TEST".to_string(), "NASDAQ".to_string());
    data_source.add_asset(asset.clone());

    let start = Utc::now();
    let end = start + Duration::days(5);

    // Add data with price increase
    for i in 0..5 {
        let timestamp = start + Duration::days(i);
        let price = 100.0 + (i as f64 * 5.0); // +5 per day
        let bar = Bar::new(timestamp, price, price + 2.0, price - 2.0, price, 10000.0);
        data_source.add_bar(asset.id, bar);
    }

    data_source.set_date_range(start, end);

    let config = EngineConfig {
        starting_cash: 10_000.0,
        max_history_len: 100,
    };

    let calendar = Arc::new(NYSECalendar::new());
    let broker = SimulatedBroker::default_broker();
    let mut engine = SimulationEngine::new(config, broker, calendar);

    let mut algorithm = BuyAndHold::new(asset);
    let performance = engine.run(&mut algorithm, &data_source, start, end).unwrap();

    // Should have some returns due to price increase
    assert!(performance.total_return() >= 0.0);
}

#[test]
fn test_performance_metrics() {
    use zipline_rust::performance::PerformanceTracker;

    let mut tracker = PerformanceTracker::new();
    let start = Utc::now();

    // Simulate portfolio growth
    tracker.record(start, 100_000.0, 0.0);
    tracker.record(start + Duration::days(1), 105_000.0, 0.05);
    tracker.record(start + Duration::days(2), 110_000.0, 0.10);
    tracker.record(start + Duration::days(3), 108_000.0, 0.08);
    tracker.record(start + Duration::days(4), 115_000.0, 0.15);

    let summary = tracker.summary();

    assert_eq!(summary.total_return, 0.15);
    assert!(summary.max_drawdown >= 0.0);
    assert!(summary.volatility >= 0.0);
    assert_eq!(summary.num_periods, 5);
}

#[test]
fn test_calendar_functionality() {
    use chrono::NaiveDate;
    use zipline_rust::calendar::{NYSECalendar, TradingCalendar};

    let calendar = NYSECalendar::new();

    // Test weekday
    let monday = NaiveDate::from_ymd_opt(2024, 1, 8).unwrap();
    assert!(calendar.is_trading_day(monday));

    // Test weekend
    let saturday = NaiveDate::from_ymd_opt(2024, 1, 6).unwrap();
    assert!(!calendar.is_trading_day(saturday));

    // Test next trading day
    let friday = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
    let next = calendar.next_trading_day(friday).unwrap();
    assert_eq!(next, monday);
}

#[test]
fn test_data_history() {
    use zipline_rust::data::BarData;

    let mut bar_data = BarData::new(100);
    let asset = Asset::equity(1, "TEST".to_string(), "NASDAQ".to_string());

    // Add multiple bars
    for i in 0..20 {
        let bar = Bar::new(
            Utc::now(),
            100.0 + i as f64,
            105.0,
            99.0,
            103.0,
            10000.0,
        );
        bar_data.update(asset.id, bar);
    }

    // Check history length
    assert_eq!(bar_data.history_len(&asset), 20);

    // Get history
    let history = bar_data.history(&asset, 10).unwrap();
    assert_eq!(history.len(), 10);

    // Check prices
    let prices = bar_data.history_prices(&asset, 5).unwrap();
    assert_eq!(prices.len(), 5);
}
