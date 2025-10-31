//! Portfolio and position tracking

use crate::asset::Asset;
use crate::order::{Order, OrderSide};
use crate::types::{Cash, Price, Quantity, Timestamp};
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

/// A position in a single asset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    /// Asset held
    pub asset: Asset,
    /// Quantity held (positive for long, negative for short)
    pub quantity: Quantity,
    /// Cost basis (total amount paid)
    pub cost_basis: Cash,
    /// Last known price
    pub last_price: Price,
}

impl Position {
    /// Create a new position
    pub fn new(asset: Asset, quantity: Quantity, cost_basis: Cash, last_price: Price) -> Self {
        Self {
            asset,
            quantity,
            cost_basis,
            last_price,
        }
    }

    /// Calculate current market value
    pub fn market_value(&self) -> Cash {
        self.quantity * self.last_price
    }

    /// Calculate profit/loss
    pub fn pnl(&self) -> Cash {
        self.market_value() - self.cost_basis
    }

    /// Calculate profit/loss percentage
    pub fn pnl_pct(&self) -> f64 {
        if self.cost_basis == 0.0 {
            0.0
        } else {
            self.pnl() / self.cost_basis
        }
    }

    /// Update last price
    pub fn update_price(&mut self, price: Price) {
        self.last_price = price;
    }

    /// Check if position is long
    pub fn is_long(&self) -> bool {
        self.quantity > 0.0
    }

    /// Check if position is short
    pub fn is_short(&self) -> bool {
        self.quantity < 0.0
    }

    /// Check if position is flat (closed)
    pub fn is_flat(&self) -> bool {
        self.quantity.abs() < f64::EPSILON
    }
}

/// Portfolio tracking account value and positions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Portfolio {
    /// Starting cash
    pub starting_cash: Cash,
    /// Current cash available
    pub cash: Cash,
    /// Current positions
    pub positions: HashMap<u64, Position>,
    /// Portfolio value history
    pub value_history: Vec<(Timestamp, Cash)>,
    /// Positions value (market value of all positions)
    pub positions_value: Cash,
    /// Total portfolio value (cash + positions)
    pub portfolio_value: Cash,
    /// Total profit/loss
    pub pnl: Cash,
    /// Total returns percentage
    pub returns: f64,
}

impl Portfolio {
    /// Create a new portfolio with starting cash
    pub fn new(starting_cash: Cash) -> Self {
        Self {
            starting_cash,
            cash: starting_cash,
            positions: HashMap::new(),
            value_history: Vec::new(),
            positions_value: 0.0,
            portfolio_value: starting_cash,
            pnl: 0.0,
            returns: 0.0,
        }
    }

    /// Get position for an asset
    pub fn get_position(&self, asset_id: u64) -> Option<&Position> {
        self.positions.get(&asset_id)
    }

    /// Get mutable position for an asset
    pub fn get_position_mut(&mut self, asset_id: u64) -> Option<&mut Position> {
        self.positions.get_mut(&asset_id)
    }

    /// Execute a fill on an order
    pub fn execute_order(&mut self, order: &Order, fill_price: Price, commission: Cash) {
        let cost = fill_price * order.filled;
        let total_cost = match order.side {
            OrderSide::Buy => cost + commission,
            OrderSide::Sell => -(cost - commission),
        };

        // Update cash
        self.cash -= total_cost;

        // Update or create position
        let position = self
            .positions
            .entry(order.asset.id)
            .or_insert_with(|| Position::new(order.asset.clone(), 0.0, 0.0, fill_price));

        match order.side {
            OrderSide::Buy => {
                position.quantity += order.filled;
                position.cost_basis += cost;
            }
            OrderSide::Sell => {
                position.quantity -= order.filled;
                position.cost_basis -= cost;
            }
        }

        position.last_price = fill_price;

        // Remove flat positions
        if position.is_flat() {
            self.positions.remove(&order.asset.id);
        }
    }

    /// Update portfolio value based on current prices
    pub fn update_value(&mut self, timestamp: Timestamp) {
        // Calculate positions value
        self.positions_value = self
            .positions
            .values()
            .map(|p| p.market_value())
            .sum();

        // Calculate total portfolio value
        self.portfolio_value = self.cash + self.positions_value;

        // Calculate PnL and returns
        self.pnl = self.portfolio_value - self.starting_cash;
        self.returns = if self.starting_cash > 0.0 {
            self.pnl / self.starting_cash
        } else {
            0.0
        };

        // Record value
        self.value_history.push((timestamp, self.portfolio_value));
    }

    /// Get number of open positions
    pub fn num_positions(&self) -> usize {
        self.positions.len()
    }

    /// Get total leverage
    pub fn leverage(&self) -> f64 {
        if self.portfolio_value == 0.0 {
            0.0
        } else {
            self.positions_value.abs() / self.portfolio_value
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::Asset;
    use chrono::Utc;

    #[test]
    fn test_position_calculations() {
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        let mut position = Position::new(asset, 100.0, 10000.0, 100.0);

        assert_eq!(position.market_value(), 10000.0);
        assert_eq!(position.pnl(), 0.0);
        assert!(position.is_long());

        position.update_price(110.0);
        assert_eq!(position.market_value(), 11000.0);
        assert_eq!(position.pnl(), 1000.0);
        assert_eq!(position.pnl_pct(), 0.1);
    }

    #[test]
    fn test_portfolio_execution() {
        let mut portfolio = Portfolio::new(100000.0);
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        let mut order = Order::market(asset.clone(), OrderSide::Buy, 100.0, Utc::now());
        order.fill(100.0, Utc::now());

        portfolio.execute_order(&order, 150.0, 1.0);

        assert_eq!(portfolio.cash, 100000.0 - 15000.0 - 1.0);
        assert_eq!(portfolio.num_positions(), 1);

        let position = portfolio.get_position(1).unwrap();
        assert_eq!(position.quantity, 100.0);
        assert_eq!(position.cost_basis, 15000.0);
    }

    #[test]
    fn test_portfolio_value_update() {
        let mut portfolio = Portfolio::new(100000.0);
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        let mut order = Order::market(asset.clone(), OrderSide::Buy, 100.0, Utc::now());
        order.fill(100.0, Utc::now());

        portfolio.execute_order(&order, 150.0, 0.0);
        portfolio.update_value(Utc::now());

        assert_eq!(portfolio.positions_value, 15000.0);
        assert_eq!(portfolio.portfolio_value, 100000.0);
        assert_eq!(portfolio.pnl, 0.0);

        // Update position price
        portfolio.get_position_mut(1).unwrap().update_price(160.0);
        portfolio.update_value(Utc::now());

        assert_eq!(portfolio.positions_value, 16000.0);
        assert_eq!(portfolio.portfolio_value, 101000.0);
        assert_eq!(portfolio.pnl, 1000.0);
        assert_eq!(portfolio.returns, 0.01);
    }
}
