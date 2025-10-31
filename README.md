# Zipline-Rust 🚀

A high-performance Rust implementation of [Zipline](https://github.com/stefan-jansen/zipline-reloaded), the Pythonic event-driven backtesting library for algorithmic trading.

## Features

- **Event-Driven Architecture**: Process historical market data sequentially with realistic order execution
- **Comprehensive Portfolio Tracking**: Track positions, cash, P&L, and portfolio metrics in real-time
- **Flexible Order Types**: Support for market, limit, stop, and stop-limit orders
- **Slippage & Commission Models**: Realistic simulation with configurable slippage and commission
- **Trading Calendar**: NYSE/NASDAQ calendar with holiday support
- **Performance Analytics**: Sharpe ratio, Sortino ratio, max drawdown, volatility, and more
- **Pipeline System**: Factor computation and data processing framework
- **Type-Safe**: Leveraging Rust's type system for correctness and performance

## Quick Start

### Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
zipline-rust = "0.1.0"
```

### Basic Example

```rust
use zipline_rust::prelude::*;

struct MyStrategy {
    asset: Asset,
}

impl Algorithm for MyStrategy {
    fn initialize(&mut self, context: &mut Context) {
        println!("Starting strategy with ${}", context.portfolio.cash);
    }

    fn handle_data(&mut self, context: &mut Context, data: &BarData) -> Result<()> {
        let price = data.current_price(&self.asset)?;

        // Your trading logic here
        if should_buy(price) {
            context.order(self.asset.clone(), 100.0)?;
        }

        Ok(())
    }
}
```

### Running a Backtest

```rust
use std::sync::Arc;
use zipline_rust::{
    engine::{EngineConfig, SimulationEngine},
    calendar::NYSECalendar,
    execution::SimulatedBroker,
};

fn main() {
    // Create engine
    let calendar = Arc::new(NYSECalendar::new());
    let config = EngineConfig {
        starting_cash: 100_000.0,
        max_history_len: 1000,
    };

    let mut engine = SimulationEngine::new(
        config,
        SimulatedBroker::default_broker(),
        calendar,
    );

    // Run backtest
    let mut strategy = MyStrategy { /* ... */ };
    let performance = engine.run(
        &mut strategy,
        &data_source,
        start_date,
        end_date,
    ).unwrap();

    // Analyze results
    println!("{}", performance.summary());
}
```

## Examples

Run the included examples:

```bash
# Buy and hold strategy
cargo run --example buy_and_hold

# Dual moving average crossover
cargo run --example dual_moving_average
```

## Architecture

### Core Components

- **Algorithm**: Trait defining your trading strategy
- **Context**: Trading context with portfolio state and order management
- **BarData**: Market data provider for current and historical prices
- **SimulationEngine**: Event loop processor for backtesting
- **Portfolio**: Position and cash tracking
- **Orders**: Order creation, execution, and lifecycle management

### Module Structure

```
src/
├── algorithm.rs      # Algorithm trait and context
├── asset.rs          # Asset definitions
├── calendar.rs       # Trading calendar
├── data.rs           # Market data handling
├── engine.rs         # Backtesting engine
├── error.rs          # Error types
├── execution.rs      # Order execution and slippage
├── finance.rs        # Portfolio and positions
├── order.rs          # Order types
├── performance.rs    # Performance metrics
├── pipeline.rs       # Data pipeline system
└── types.rs          # Core types
```

## Performance Metrics

The library calculates comprehensive performance statistics:

- **Total Return**: Cumulative portfolio return
- **Annualized Return**: Return normalized to yearly basis
- **Sharpe Ratio**: Risk-adjusted returns
- **Sortino Ratio**: Downside risk-adjusted returns
- **Maximum Drawdown**: Largest peak-to-trough decline
- **Volatility**: Standard deviation of returns

## Configuration

### Slippage Models

```rust
use zipline_rust::execution::*;

// Fixed slippage
let slippage = FixedSlippage::new(0.05);

// Volume share slippage
let slippage = VolumeShareSlippage::new(0.001); // 0.1%
```

### Commission Models

```rust
// Per-share commission
let commission = PerShareCommission::new(0.01);

// Per-trade flat fee
let commission = PerTradeCommission::new(1.0);
```

## Testing

Run the test suite:

```bash
# Unit tests
cargo test

# Integration tests
cargo test --test integration_test

# Benchmarks
cargo bench
```

## Roadmap

- [ ] Real-time data source integration
- [ ] More asset types (futures, options, crypto)
- [ ] Advanced order types (trailing stop, bracket orders)
- [ ] Multi-asset portfolio optimization
- [ ] Parallel backtesting
- [ ] Strategy optimization framework
- [ ] Live trading support

## Comparison with Python Zipline

| Feature | Zipline-Rust | Python Zipline |
|---------|--------------|----------------|
| Performance | ⚡ 10-100x faster | Standard |
| Memory Safety | ✅ Guaranteed | Manual |
| Type Safety | ✅ Compile-time | Runtime |
| Concurrency | ✅ Native | GIL limited |
| Ecosystem | Growing | Mature |

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Acknowledgments

This project is inspired by and based on the excellent work of:
- [Zipline](https://github.com/quantopian/zipline) - Original Quantopian project
- [zipline-reloaded](https://github.com/stefan-jansen/zipline-reloaded) - Maintained fork by Stefan Jansen

## Resources

- [Documentation](https://docs.rs/zipline-rust)
- [Examples](examples/)
- [API Reference](https://docs.rs/zipline-rust)

---

**Note**: This is an independent Rust implementation and is not affiliated with Quantopian or the official Zipline project.
