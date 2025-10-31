//! Buy and hold strategy example

use chrono::{Duration, Utc};
use std::sync::Arc;
use zipline_rust::prelude::*;
use zipline_rust::{
    calendar::NYSECalendar,
    data::InMemoryDataSource,
    engine::{EngineConfig, SimulationEngine},
    execution::SimulatedBroker,
    types::Bar,
};

fn main() {
    env_logger::init();

    println!("=== Zipline-Rust: Buy and Hold Example ===\n");

    // Create asset
    let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string())
        .with_name("Apple Inc.".to_string());

    // Create data source with sample data
    let mut data_source = InMemoryDataSource::new();
    data_source.add_asset(asset.clone());

    let start = Utc::now();
    let end = start + Duration::days(252); // One trading year

    // Generate sample price data (simple uptrend)
    for i in 0..252 {
        let timestamp = start + Duration::days(i);
        let price = 150.0 + (i as f64 * 0.5); // Gradual price increase

        let bar = Bar::new(
            timestamp,
            price,           // open
            price + 2.0,     // high
            price - 2.0,     // low
            price + 0.5,     // close
            1_000_000.0,     // volume
        );

        data_source.add_bar(asset.id, bar);
    }

    data_source.set_date_range(start, end);

    // Create algorithm
    let mut algorithm = BuyAndHold::new(asset);

    // Create engine
    let config = EngineConfig {
        starting_cash: 100_000.0,
        max_history_len: 1000,
    };

    let calendar = Arc::new(NYSECalendar::new());
    let broker = SimulatedBroker::default_broker();
    let mut engine = SimulationEngine::new(config, broker, calendar);

    // Run backtest
    println!("Running backtest from {} to {}\n", start, end);

    match engine.run(&mut algorithm, &data_source, start, end) {
        Ok(performance) => {
            let summary = performance.summary();
            println!("{}", summary);
        }
        Err(e) => {
            eprintln!("Error running backtest: {}", e);
        }
    }
}

struct BuyAndHold {
    asset: Asset,
    initialized: bool,
}

impl BuyAndHold {
    fn new(asset: Asset) -> Self {
        Self {
            asset,
            initialized: false,
        }
    }
}

impl Algorithm for BuyAndHold {
    fn initialize(&mut self, context: &mut Context) {
        println!("Initializing Buy and Hold strategy");
        println!("Asset: {}", self.asset.symbol);
        println!("Starting cash: ${:.2}\n", context.portfolio.cash);
    }

    fn handle_data(&mut self, context: &mut Context, data: &BarData) -> Result<()> {
        if !self.initialized && data.has_data(&self.asset) {
            let price = data.current_price(&self.asset)?;
            let quantity = (context.portfolio.cash / price * 0.95).floor(); // Use 95% of cash

            if quantity > 0.0 {
                println!(
                    "Buying {:.0} shares of {} at ${:.2}",
                    quantity, self.asset.symbol, price
                );
                context.order(self.asset.clone(), quantity)?;
                self.initialized = true;
            }
        }

        Ok(())
    }

    fn analyze(&mut self, context: &Context) -> Result<()> {
        println!("\n=== Final Results ===");
        println!("Portfolio Value: ${:.2}", context.portfolio.portfolio_value);
        println!("Cash: ${:.2}", context.portfolio.cash);
        println!("Positions Value: ${:.2}", context.portfolio.positions_value);
        println!("Total Return: {:.2}%", context.portfolio.returns * 100.0);
        println!("Number of Positions: {}", context.portfolio.num_positions());

        Ok(())
    }
}
