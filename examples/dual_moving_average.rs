//! Dual moving average crossover strategy example

use chrono::{Duration, Utc};
use std::sync::Arc;
use zipline_rust::prelude::*;
use zipline_rust::{
    algorithm::{Algorithm, Context},
    calendar::NYSECalendar,
    data::{BarData, InMemoryDataSource},
    engine::{EngineConfig, SimulationEngine},
    execution::SimulatedBroker,
    types::Bar,
};

fn main() {
    env_logger::init();

    println!("=== Zipline-Rust: Dual Moving Average Example ===\n");

    // Create asset
    let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());

    // Create data source
    let mut data_source = InMemoryDataSource::new();
    data_source.add_asset(asset.clone());

    let start = Utc::now();
    let end = start + Duration::days(252);

    // Generate sample price data with trend
    for i in 0..252 {
        let timestamp = start + Duration::days(i);

        // Create a sine wave pattern with uptrend
        let trend = 150.0 + (i as f64 * 0.3);
        let cycle = (i as f64 / 20.0).sin() * 5.0;
        let price = trend + cycle;

        let bar = Bar::new(
            timestamp,
            price,
            price + 2.0,
            price - 2.0,
            price + 0.5,
            1_000_000.0,
        );

        data_source.add_bar(asset.id, bar);
    }

    data_source.set_date_range(start, end);

    // Create algorithm
    let mut algorithm = DualMovingAverage::new(asset, 10, 30);

    // Create engine
    let config = EngineConfig {
        starting_cash: 100_000.0,
        max_history_len: 1000,
    };

    let calendar = Arc::new(NYSECalendar::new());
    let broker = SimulatedBroker::default_broker();
    let mut engine = SimulationEngine::new(config, broker, calendar);

    // Run backtest
    println!("Running backtest...\n");

    match engine.run(&mut algorithm, &data_source, start, end) {
        Ok(performance) => {
            println!("{}", performance.summary());
        }
        Err(e) => {
            eprintln!("Error: {}", e);
        }
    }
}

struct DualMovingAverage {
    asset: Asset,
    short_window: usize,
    long_window: usize,
    invested: bool,
}

impl DualMovingAverage {
    fn new(asset: Asset, short_window: usize, long_window: usize) -> Self {
        Self {
            asset,
            short_window,
            long_window,
            invested: false,
        }
    }

    fn calculate_sma(prices: &[f64]) -> f64 {
        if prices.is_empty() {
            return 0.0;
        }
        prices.iter().sum::<f64>() / prices.len() as f64
    }
}

impl Algorithm for DualMovingAverage {
    fn initialize(&mut self, context: &mut Context) {
        println!("Dual Moving Average Strategy");
        println!("Short window: {}", self.short_window);
        println!("Long window: {}", self.long_window);
        println!("Starting cash: ${:.2}\n", context.portfolio.cash);
    }

    fn handle_data(&mut self, context: &mut Context, data: &BarData) -> Result<()> {
        // Need enough history
        if data.history_len(&self.asset) < self.long_window {
            return Ok(());
        }

        // Get historical prices
        let prices = data.history_prices(&self.asset, self.long_window)?;

        // Calculate moving averages
        let short_prices = &prices[prices.len() - self.short_window..];
        let short_ma = Self::calculate_sma(short_prices);
        let long_ma = Self::calculate_sma(&prices);

        let current_price = data.current_price(&self.asset)?;

        // Trading logic
        if short_ma > long_ma && !self.invested {
            // Buy signal
            let quantity = (context.portfolio.cash / current_price * 0.95).floor();
            if quantity > 0.0 {
                println!(
                    "BUY: {} shares at ${:.2} (Short MA: {:.2}, Long MA: {:.2})",
                    quantity, current_price, short_ma, long_ma
                );
                context.order(self.asset.clone(), quantity)?;
                self.invested = true;
            }
        } else if short_ma < long_ma && self.invested {
            // Sell signal
            if let Some(position) = context.portfolio.get_position(self.asset.id) {
                let quantity = position.quantity;
                println!(
                    "SELL: {} shares at ${:.2} (Short MA: {:.2}, Long MA: {:.2})",
                    quantity, current_price, short_ma, long_ma
                );
                context.order(self.asset.clone(), -quantity)?;
                self.invested = false;
            }
        }

        Ok(())
    }

    fn analyze(&mut self, context: &Context) -> Result<()> {
        println!("\n=== Final Results ===");
        println!("Portfolio Value: ${:.2}", context.portfolio.portfolio_value);
        println!("Total Return: {:.2}%", context.portfolio.returns * 100.0);
        Ok(())
    }
}
