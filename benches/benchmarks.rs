use criterion::{black_box, criterion_group, criterion_main, Criterion};
use chrono::{Duration, Utc};
use std::sync::Arc;
use zipline_rust::{
    algorithm::BuyAndHold,
    asset::Asset,
    calendar::NYSECalendar,
    data::InMemoryDataSource,
    engine::{EngineConfig, SimulationEngine},
    execution::SimulatedBroker,
    types::Bar,
};

fn benchmark_backtest(c: &mut Criterion) {
    c.bench_function("backtest_100_days", |b| {
        b.iter(|| {
            let asset = Asset::equity(1, "TEST".to_string(), "NASDAQ".to_string());
            let mut data_source = InMemoryDataSource::new();
            data_source.add_asset(asset.clone());

            let start = Utc::now();
            let end = start + Duration::days(100);

            for i in 0..100 {
                let timestamp = start + Duration::days(i);
                let bar = Bar::new(timestamp, 100.0, 105.0, 99.0, 103.0, 10000.0);
                data_source.add_bar(asset.id, bar);
            }

            data_source.set_date_range(start, end);

            let config = EngineConfig {
                starting_cash: 100_000.0,
                max_history_len: 1000,
            };

            let calendar = Arc::new(NYSECalendar::new());
            let broker = SimulatedBroker::default_broker();
            let mut engine = SimulationEngine::new(config, broker, calendar);

            let mut algorithm = BuyAndHold::new(asset);
            let _ = engine.run(&mut algorithm, &data_source, start, end);
        });
    });
}

fn benchmark_order_execution(c: &mut Criterion) {
    use zipline_rust::order::{Order, OrderSide};

    c.bench_function("order_execution_1000", |b| {
        b.iter(|| {
            let asset = Asset::equity(1, "TEST".to_string(), "NASDAQ".to_string());

            for _ in 0..1000 {
                let _order = Order::market(
                    black_box(asset.clone()),
                    OrderSide::Buy,
                    black_box(100.0),
                    Utc::now(),
                );
            }
        });
    });
}

fn benchmark_portfolio_update(c: &mut Criterion) {
    use zipline_rust::finance::Portfolio;

    c.bench_function("portfolio_update_1000", |b| {
        b.iter(|| {
            let mut portfolio = Portfolio::new(100_000.0);

            for _ in 0..1000 {
                portfolio.update_value(Utc::now());
            }
        });
    });
}

criterion_group!(
    benches,
    benchmark_backtest,
    benchmark_order_execution,
    benchmark_portfolio_update
);
criterion_main!(benches);
