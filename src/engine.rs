//! Backtesting engine with event loop

use crate::algorithm::{Algorithm, Context};
use crate::calendar::TradingCalendar;
use crate::data::{BarData, DataSource};
use crate::error::Result;
use crate::execution::{ExecutionResult, SimulatedBroker};
use crate::performance::PerformanceTracker;
use crate::types::Timestamp;
use std::sync::Arc;

/// Configuration for simulation engine
pub struct EngineConfig {
    /// Starting capital
    pub starting_cash: f64,
    /// Maximum historical bars to keep
    pub max_history_len: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            starting_cash: 100_000.0,
            max_history_len: 1000,
        }
    }
}

/// Backtesting simulation engine
pub struct SimulationEngine {
    /// Engine configuration
    config: EngineConfig,
    /// Simulated broker
    broker: SimulatedBroker,
    /// Trading calendar
    calendar: Arc<dyn TradingCalendar>,
    /// Performance tracker
    performance: PerformanceTracker,
}

impl SimulationEngine {
    /// Create a new simulation engine
    pub fn new(
        config: EngineConfig,
        broker: SimulatedBroker,
        calendar: Arc<dyn TradingCalendar>,
    ) -> Self {
        Self {
            config,
            broker,
            calendar,
            performance: PerformanceTracker::new(),
        }
    }

    /// Create engine with default configuration
    pub fn default_engine(calendar: Arc<dyn TradingCalendar>) -> Self {
        Self::new(
            EngineConfig::default(),
            SimulatedBroker::default_broker(),
            calendar,
        )
    }

    /// Run backtest
    pub fn run<A: Algorithm>(
        &mut self,
        algorithm: &mut A,
        data_source: &dyn DataSource,
        start: Timestamp,
        end: Timestamp,
    ) -> Result<PerformanceTracker> {
        // Initialize context
        let mut context = Context::new(self.config.starting_cash);
        let mut bar_data = BarData::new(self.config.max_history_len);

        // Initialize algorithm
        algorithm.initialize(&mut context);

        // Get all timestamps in range
        let mut timestamps: Vec<Timestamp> = vec![];
        let (data_start, data_end) = data_source.get_date_range();

        // Use data range if specified range is outside available data
        let sim_start = if start < data_start { data_start } else { start };
        let sim_end = if end > data_end { data_end } else { end };

        // Collect all available timestamps from data source
        let mut current_time = sim_start;
        while current_time <= sim_end {
            timestamps.push(current_time);
            current_time = current_time + chrono::Duration::minutes(1); // Adjust based on data frequency
        }

        log::info!("Starting backtest from {} to {}", sim_start, sim_end);
        log::info!("Processing {} timestamps", timestamps.len());

        // Main event loop
        for timestamp in timestamps {
            context.timestamp = timestamp;

            // Get bars for this timestamp
            let bars = data_source.get_bars(timestamp)?;

            if bars.is_empty() {
                continue;
            }

            // Update bar data
            for (asset_id, bar) in bars {
                bar_data.update(asset_id, bar);
            }

            // Call before_trading_start at market open
            // (simplified: call on first bar of each day)
            algorithm.before_trading_start(&mut context, &bar_data)?;

            // Call handle_data
            algorithm.handle_data(&mut context, &bar_data)?;

            // Process pending orders
            self.process_orders(&mut context, &bar_data)?;

            // Update portfolio value
            context.portfolio.update_value(timestamp);

            // Track performance
            self.performance.record(
                timestamp,
                context.portfolio.portfolio_value,
                context.portfolio.returns,
            );
        }

        // Analyze results
        algorithm.analyze(&context)?;

        log::info!("Backtest complete");
        log::info!(
            "Final portfolio value: {:.2}",
            context.portfolio.portfolio_value
        );
        log::info!(
            "Total return: {:.2}%",
            context.portfolio.returns * 100.0
        );

        Ok(self.performance.clone())
    }

    /// Process pending orders
    fn process_orders(&mut self, context: &mut Context, bar_data: &BarData) -> Result<()> {
        let orders = std::mem::take(&mut context.pending_orders);

        for mut order in orders {
            // Get current price
            let current_price = match bar_data.current_price(&order.asset) {
                Ok(price) => price,
                Err(e) => {
                    log::warn!("No price data for {}: {}", order.asset.symbol, e);
                    context.pending_orders.push(order);
                    continue;
                }
            };

            // Execute order
            match self
                .broker
                .execute_order(&mut order, current_price, context.timestamp)?
            {
                ExecutionResult::Filled {
                    price,
                    quantity,
                    commission,
                } => {
                    log::debug!(
                        "Filled order: {} {} @ {:.2} (commission: {:.2})",
                        quantity,
                        order.asset.symbol,
                        price,
                        commission
                    );

                    // Update portfolio
                    context.portfolio.execute_order(&order, price, commission);
                }
                ExecutionResult::NotFilled => {
                    // Keep order for next iteration
                    context.pending_orders.push(order);
                }
            }
        }

        Ok(())
    }

    /// Get performance tracker
    pub fn performance(&self) -> &PerformanceTracker {
        &self.performance
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm::BuyAndHold;
    use crate::asset::Asset;
    use crate::calendar::NYSECalendar;
    use crate::data::InMemoryDataSource;
    use crate::types::Bar;
    use chrono::Utc;

    #[test]
    fn test_engine_creation() {
        let calendar = Arc::new(NYSECalendar::new());
        let engine = SimulationEngine::default_engine(calendar);
        assert_eq!(engine.config.starting_cash, 100_000.0);
    }

    #[test]
    fn test_simple_backtest() {
        // Create data source with sample data
        let mut data_source = InMemoryDataSource::new();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        data_source.add_asset(asset.clone());

        let start = Utc::now();
        let end = start + chrono::Duration::days(5);

        // Add sample bars
        for i in 0..5 {
            let timestamp = start + chrono::Duration::days(i);
            let bar = Bar::new(
                timestamp,
                100.0 + i as f64,
                105.0 + i as f64,
                99.0 + i as f64,
                103.0 + i as f64,
                10000.0,
            );
            data_source.add_bar(1, bar);
        }

        data_source.set_date_range(start, end);

        // Create engine and algorithm
        let calendar = Arc::new(NYSECalendar::new());
        let mut engine = SimulationEngine::default_engine(calendar);
        let mut algorithm = BuyAndHold::new(asset);

        // Run backtest
        let _performance = engine.run(&mut algorithm, &data_source, start, end).unwrap();
    }
}
