# Getting Started with Zipline-Rust

## Installation

### Prerequisites

- Rust 1.70 or higher
- Cargo (comes with Rust)

### Install Rust

If you don't have Rust installed:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Add Zipline-Rust to Your Project

Add to your `Cargo.toml`:

```toml
[dependencies]
zipline-rust = "0.1.0"
```

Or use cargo add:

```bash
cargo add zipline-rust
```

## Your First Strategy

### 1. Create a New Project

```bash
cargo new my_trading_strategy
cd my_trading_strategy
```

### 2. Add Zipline-Rust Dependency

Edit `Cargo.toml`:

```toml
[dependencies]
zipline-rust = "0.1.0"
chrono = "0.4"
```

### 3. Write Your Strategy

Edit `src/main.rs`:

```rust
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

// Define your strategy
struct SimpleStrategy {
    asset: Asset,
    bought: bool,
}

impl SimpleStrategy {
    fn new(asset: Asset) -> Self {
        Self {
            asset,
            bought: false,
        }
    }
}

impl Algorithm for SimpleStrategy {
    fn initialize(&mut self, context: &mut Context) {
        println!("Starting with ${:.2}", context.portfolio.cash);
    }

    fn handle_data(&mut self, context: &mut Context, data: &BarData) -> Result<()> {
        // Buy on first bar
        if !self.bought && data.has_data(&self.asset) {
            let price = data.current_price(&self.asset)?;
            let shares = (context.portfolio.cash / price * 0.95).floor();

            if shares > 0.0 {
                println!("Buying {} shares at ${:.2}", shares, price);
                context.order(self.asset.clone(), shares)?;
                self.bought = true;
            }
        }

        Ok(())
    }

    fn analyze(&mut self, context: &Context) -> Result<()> {
        println!("\n=== Results ===");
        println!("Final Value: ${:.2}", context.portfolio.portfolio_value);
        println!("Return: {:.2}%", context.portfolio.returns * 100.0);
        Ok(())
    }
}

fn main() {
    // Create asset
    let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());

    // Create data source
    let mut data_source = InMemoryDataSource::new();
    data_source.add_asset(asset.clone());

    let start = Utc::now();
    let end = start + Duration::days(30);

    // Add sample data
    for i in 0..30 {
        let timestamp = start + Duration::days(i);
        let price = 150.0 + (i as f64 * 0.5);

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

    // Create and run backtest
    let config = EngineConfig {
        starting_cash: 10_000.0,
        max_history_len: 100,
    };

    let calendar = Arc::new(NYSECalendar::new());
    let broker = SimulatedBroker::default_broker();
    let mut engine = SimulationEngine::new(config, broker, calendar);

    let mut strategy = SimpleStrategy::new(asset);

    match engine.run(&mut strategy, &data_source, start, end) {
        Ok(performance) => {
            println!("\n{}", performance.summary());
        }
        Err(e) => {
            eprintln!("Error: {}", e);
        }
    }
}
```

### 4. Run Your Strategy

```bash
cargo run
```

## Key Concepts

### Algorithm Trait

Your strategy must implement the `Algorithm` trait:

```rust
pub trait Algorithm: Send {
    fn initialize(&mut self, context: &mut Context);
    fn handle_data(&mut self, context: &mut Context, data: &BarData) -> Result<()>;
    fn before_trading_start(&mut self, context: &mut Context, data: &BarData) -> Result<()>;
    fn analyze(&mut self, context: &Context) -> Result<()>;
}
```

**Methods**:
- `initialize`: Called once at start
- `handle_data`: Called for each bar
- `before_trading_start`: Called at market open (optional)
- `analyze`: Called at end for analysis (optional)

### Context

The `Context` provides your algorithm with:

```rust
// Portfolio state
context.portfolio.cash
context.portfolio.positions
context.portfolio.portfolio_value

// Place orders
context.order(asset, quantity)?
context.order_target(asset, target_quantity)?

// Store variables
context.set("key".to_string(), value);
context.get::<Type>("key")
```

### BarData

Access market data:

```rust
// Current price
let price = data.current_price(&asset)?;

// Current bar
let bar = data.current(&asset)?;

// Historical prices
let prices = data.history_prices(&asset, 20)?;

// Historical bars
let bars = data.history(&asset, 20)?;
```

## Common Patterns

### Moving Average Crossover

```rust
fn handle_data(&mut self, context: &mut Context, data: &BarData) -> Result<()> {
    if data.history_len(&self.asset) < 50 {
        return Ok(());
    }

    let prices = data.history_prices(&self.asset, 50)?;

    let short_ma: f64 = prices[prices.len() - 10..].iter().sum::<f64>() / 10.0;
    let long_ma: f64 = prices.iter().sum::<f64>() / 50.0;

    let current_position = context.portfolio
        .get_position(self.asset.id)
        .map(|p| p.quantity)
        .unwrap_or(0.0);

    if short_ma > long_ma && current_position == 0.0 {
        // Buy signal
        let price = data.current_price(&self.asset)?;
        let shares = (context.portfolio.cash / price * 0.95).floor();
        context.order(self.asset.clone(), shares)?;
    } else if short_ma < long_ma && current_position > 0.0 {
        // Sell signal
        context.order_target(self.asset.clone(), 0.0)?;
    }

    Ok(())
}
```

### Portfolio Rebalancing

```rust
fn handle_data(&mut self, context: &mut Context, data: &BarData) -> Result<()> {
    // Rebalance to target allocation
    let target_weight = 0.5; // 50% allocation
    let target_value = context.portfolio.portfolio_value * target_weight;

    let price = data.current_price(&self.asset)?;
    let target_shares = (target_value / price).floor();

    context.order_target(self.asset.clone(), target_shares)?;

    Ok(())
}
```

### Record Custom Metrics

```rust
fn handle_data(&mut self, context: &mut Context, data: &BarData) -> Result<()> {
    // Store custom metrics
    let price = data.current_price(&self.asset)?;

    context.set("last_price".to_string(), price);
    context.set("trade_count".to_string(), self.trades);

    Ok(())
}
```

## Configuration

### Slippage Models

```rust
use zipline_rust::execution::*;

// Fixed slippage per share
let slippage = FixedSlippage::new(0.05);

// Percentage slippage
let slippage = VolumeShareSlippage::new(0.001); // 0.1%

let broker = SimulatedBroker::new(
    Box::new(slippage),
    Box::new(NoCommission),
);
```

### Commission Models

```rust
// Per share
let commission = PerShareCommission::new(0.01);

// Per trade
let commission = PerTradeCommission::new(1.0);

let broker = SimulatedBroker::new(
    Box::new(NoSlippage),
    Box::new(commission),
);
```

### Engine Configuration

```rust
let config = EngineConfig {
    starting_cash: 100_000.0,    // Initial capital
    max_history_len: 1000,       // Historical bars to keep
};
```

## Next Steps

1. **Examples**: Check out the [examples/](../examples/) directory
2. **API Docs**: Run `cargo doc --open`
3. **Architecture**: Read [ARCHITECTURE.md](ARCHITECTURE.md)
4. **Testing**: See [integration tests](../tests/)

## Common Issues

### Cargo not found

Install Rust from https://rustup.rs

### Missing data

Ensure you:
1. Create data source
2. Add assets
3. Add bars
4. Set date range

### Orders not filling

Check:
1. Asset has data for that timestamp
2. Order type constraints (limit price, etc.)
3. Sufficient cash available

## Resources

- [Rust Book](https://doc.rust-lang.org/book/)
- [Zipline Documentation](https://zipline.ml4trading.io/)
- [Algorithmic Trading](https://www.quantstart.com/)

## Getting Help

- GitHub Issues: Report bugs or ask questions
- Discussions: Community forum
- Examples: Check example strategies
