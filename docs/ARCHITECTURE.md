# Zipline-Rust Architecture

## Overview

Zipline-Rust is an event-driven backtesting system that simulates trading strategies against historical market data. The architecture follows a clean separation of concerns with well-defined module boundaries.

## Core Design Principles

1. **Event-Driven**: Sequential processing of market data events
2. **Type Safety**: Leverage Rust's type system for correctness
3. **Performance**: Zero-cost abstractions and efficient data structures
4. **Extensibility**: Trait-based design for custom implementations
5. **Realistic Simulation**: Accurate order execution with slippage and commission

## Module Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        User Algorithm                        │
│                    (implements Algorithm)                    │
└────────────────┬────────────────────────────────────────────┘
                 │
                 v
┌─────────────────────────────────────────────────────────────┐
│                      Context (API Layer)                     │
│  - Portfolio state                                           │
│  - Order management                                          │
│  - User variables                                            │
└────────────────┬────────────────────────────────────────────┘
                 │
                 v
┌─────────────────────────────────────────────────────────────┐
│                   Simulation Engine (Core)                   │
│  - Event loop orchestration                                  │
│  - Timestamp management                                      │
│  - Order processing                                          │
└──────┬──────────────────┬─────────────────┬────────────────┘
       │                  │                 │
       v                  v                 v
┌─────────────┐  ┌──────────────┐  ┌────────────────┐
│  Portfolio  │  │  Execution   │  │   BarData      │
│  Tracking   │  │  Engine      │  │   Provider     │
└─────────────┘  └──────────────┘  └────────────────┘
       │                  │                 │
       v                  v                 v
┌─────────────┐  ┌──────────────┐  ┌────────────────┐
│  Positions  │  │  Slippage/   │  │  Data Source   │
│  Finance    │  │  Commission  │  │  Interface     │
└─────────────┘  └──────────────┘  └────────────────┘
```

## Key Components

### 1. Algorithm Layer

**Purpose**: User-defined trading strategies

**Key Types**:
- `Algorithm` trait: Defines strategy interface
- `Context`: Provides trading API
- `BarData`: Market data access

**Responsibilities**:
- Initialize strategy state
- Handle market data events
- Make trading decisions
- Analyze results

### 2. Simulation Engine

**Purpose**: Event loop orchestration

**Key Types**:
- `SimulationEngine`: Main backtest coordinator
- `EngineConfig`: Configuration parameters

**Responsibilities**:
- Process historical data chronologically
- Call algorithm hooks at appropriate times
- Coordinate order execution
- Track performance metrics

### 3. Data Layer

**Purpose**: Market data management

**Key Types**:
- `BarData`: Current and historical bar access
- `DataSource` trait: Data provider interface
- `InMemoryDataSource`: Sample implementation
- `Bar`: OHLCV data structure

**Responsibilities**:
- Store and retrieve market data
- Provide historical lookback
- Handle missing data

### 4. Execution Layer

**Purpose**: Realistic order execution

**Key Types**:
- `SimulatedBroker`: Order execution engine
- `SlippageModel` trait: Price impact models
- `CommissionModel` trait: Trading costs

**Responsibilities**:
- Execute orders based on type and price
- Apply slippage to executions
- Calculate commissions
- Handle partial fills

### 5. Finance Layer

**Purpose**: Portfolio and position tracking

**Key Types**:
- `Portfolio`: Account state management
- `Position`: Single asset position
- `Order`: Trading order lifecycle

**Responsibilities**:
- Track cash and positions
- Calculate portfolio value
- Compute P&L and returns
- Maintain transaction history

### 6. Calendar Layer

**Purpose**: Trading day management

**Key Types**:
- `TradingCalendar` trait: Calendar interface
- `NYSECalendar`: US market implementation
- `SessionTimes`: Market hours

**Responsibilities**:
- Determine trading days
- Handle holidays
- Provide session times
- Calculate trading day sequences

### 7. Performance Layer

**Purpose**: Analytics and metrics

**Key Types**:
- `PerformanceTracker`: Metrics computation
- `PerformanceSummary`: Result aggregation

**Responsibilities**:
- Track portfolio value over time
- Calculate risk metrics
- Compute returns statistics
- Generate performance reports

## Data Flow

### Typical Backtest Flow

```
1. Initialize
   ├─> Create engine with config
   ├─> Setup data source
   ├─> Initialize algorithm
   └─> Create context

2. Main Loop (for each timestamp)
   ├─> Fetch market data
   ├─> Update BarData
   ├─> Call before_trading_start()
   ├─> Call handle_data()
   ├─> Process pending orders
   │   ├─> Execute via broker
   │   ├─> Apply slippage
   │   ├─> Calculate commission
   │   └─> Update portfolio
   ├─> Update portfolio value
   └─> Record performance

3. Finalize
   ├─> Call analyze()
   ├─> Generate summary
   └─> Return results
```

## Extension Points

### Custom Slippage Model

```rust
struct MySlippage;

impl SlippageModel for MySlippage {
    fn calculate_slippage(&self, order: &Order, price: Price) -> Price {
        // Custom logic
    }
}
```

### Custom Data Source

```rust
struct MyDataSource;

impl DataSource for MyDataSource {
    fn get_bars(&self, timestamp: Timestamp) -> Result<Vec<(u64, Bar)>> {
        // Fetch from database, API, etc.
    }

    fn get_assets(&self) -> Vec<Asset> {
        // Return available assets
    }

    fn get_date_range(&self) -> (Timestamp, Timestamp) {
        // Return data range
    }
}
```

### Custom Calendar

```rust
struct MyCalendar;

impl TradingCalendar for MyCalendar {
    fn is_trading_day(&self, date: NaiveDate) -> bool {
        // Custom trading day logic
    }

    fn session_times(&self, date: NaiveDate) -> Option<SessionTimes> {
        // Custom market hours
    }
}
```

## Performance Considerations

### Memory Management

- **Bar History**: Configurable limit (`max_history_len`)
- **HashMap Usage**: `hashbrown` for faster lookups
- **Clone Minimization**: Prefer references where possible

### Computational Efficiency

- **Parallel Potential**: Engine designed for future parallelization
- **Lazy Evaluation**: Metrics computed on-demand
- **Cache-Friendly**: Sequential data access patterns

### Type System Benefits

- **Zero-Cost Abstractions**: Trait dispatching optimized away
- **Compile-Time Guarantees**: No runtime type checking
- **RAII**: Automatic resource management

## Error Handling Strategy

### Error Types

- `ZiplineError`: Main error enum
- `Result<T>`: Type alias for `Result<T, ZiplineError>`

### Error Categories

1. **Asset Errors**: Asset not found
2. **Order Errors**: Invalid orders
3. **Data Errors**: Missing or invalid data
4. **Calendar Errors**: Invalid dates
5. **Execution Errors**: Order execution failures

### Error Propagation

- Use `?` operator for clean propagation
- Provide context in error messages
- Preserve error chains when appropriate

## Testing Strategy

### Unit Tests

- Each module has comprehensive unit tests
- Test files colocated with source
- Use `#[cfg(test)]` modules

### Integration Tests

- End-to-end backtest scenarios
- Located in `tests/` directory
- Test multiple components together

### Property-Based Testing

- Future: Use `proptest` for invariant checking
- Useful for order execution correctness
- Portfolio value consistency checks

### Benchmarks

- Located in `benches/` directory
- Use `criterion` for reliable benchmarking
- Track performance regressions

## Future Enhancements

### Planned Features

1. **Async Runtime**: Optional async data sources
2. **Parallel Backtesting**: Multi-strategy execution
3. **Advanced Orders**: Bracket, trailing stop
4. **Live Trading**: Real-time execution support
5. **Optimization Framework**: Parameter optimization
6. **Risk Management**: Position sizing, risk limits
7. **Multiple Assets**: True multi-asset portfolios

### Architecture Evolution

- **Plugin System**: Dynamic strategy loading
- **Event Sourcing**: Complete state reconstruction
- **Distributed Computing**: Cloud-based backtesting
- **Machine Learning**: Integrated feature engineering

## Dependencies

### Core Dependencies

- `polars`: High-performance DataFrames
- `chrono`: Date/time handling
- `serde`: Serialization
- `thiserror`: Error handling
- `hashbrown`: Fast HashMaps

### Optional Dependencies

- `tokio`: Async runtime (feature-gated)

### Development Dependencies

- `criterion`: Benchmarking
- `proptest`: Property testing

## Comparison with Python Zipline

| Aspect | Zipline-Rust | Python Zipline |
|--------|--------------|----------------|
| Type Safety | Compile-time | Runtime |
| Performance | 10-100x faster | Baseline |
| Memory | Explicit control | GC overhead |
| Concurrency | Native threads | GIL limited |
| Dependencies | Minimal | NumPy/Pandas heavy |
| Startup Time | Instant | Import overhead |

## Contributing Guidelines

See [CONTRIBUTING.md](../CONTRIBUTING.md) for:
- Code style guidelines
- Testing requirements
- Documentation standards
- Pull request process
