//! Algorithm trait and context for trading strategies

use crate::asset::Asset;
use crate::data::BarData;
use crate::error::Result;
use crate::finance::Portfolio;
use crate::order::{Order, OrderSide};
use crate::types::{Quantity, Timestamp};
use hashbrown::HashMap;

/// Trading algorithm context
pub struct Context {
    /// Current simulation timestamp
    pub timestamp: Timestamp,
    /// Portfolio state
    pub portfolio: Portfolio,
    /// User-defined variables
    pub variables: HashMap<String, Box<dyn std::any::Any + Send>>,
    /// Pending orders
    pub pending_orders: Vec<Order>,
}

impl Context {
    /// Create a new context with starting cash
    pub fn new(starting_cash: f64) -> Self {
        Self {
            timestamp: Timestamp::default(),
            portfolio: Portfolio::new(starting_cash),
            variables: HashMap::new(),
            pending_orders: Vec::new(),
        }
    }

    /// Store a variable in the context
    pub fn set<T: 'static + Send>(&mut self, key: String, value: T) {
        self.variables.insert(key, Box::new(value));
    }

    /// Get a variable from the context
    pub fn get<T: 'static>(&self, key: &str) -> Option<&T> {
        self.variables
            .get(key)
            .and_then(|v| v.downcast_ref::<T>())
    }

    /// Order a target position in an asset
    pub fn order_target(&mut self, asset: Asset, target_quantity: Quantity) -> Result<OrderId> {
        let current_position = self
            .portfolio
            .get_position(asset.id)
            .map(|p| p.quantity)
            .unwrap_or(0.0);

        let delta = target_quantity - current_position;

        if delta.abs() < f64::EPSILON {
            // Already at target
            return Err(crate::error::ZiplineError::InvalidOrder(
                "Already at target position".to_string(),
            ));
        }

        let (side, quantity) = if delta > 0.0 {
            (OrderSide::Buy, delta)
        } else {
            (OrderSide::Sell, -delta)
        };

        let order = Order::market(asset, side, quantity, self.timestamp);
        let order_id = order.id;
        self.pending_orders.push(order);

        Ok(order_id)
    }

    /// Order a specific quantity of an asset
    pub fn order(&mut self, asset: Asset, quantity: Quantity) -> Result<OrderId> {
        if quantity.abs() < f64::EPSILON {
            return Err(crate::error::ZiplineError::InvalidOrder(
                "Quantity must be non-zero".to_string(),
            ));
        }

        let (side, qty) = if quantity > 0.0 {
            (OrderSide::Buy, quantity)
        } else {
            (OrderSide::Sell, -quantity)
        };

        let order = Order::market(asset, side, qty, self.timestamp);
        let order_id = order.id;
        self.pending_orders.push(order);

        Ok(order_id)
    }

    /// Get number of pending orders
    pub fn pending_orders_count(&self) -> usize {
        self.pending_orders.len()
    }
}

use uuid::Uuid;
type OrderId = Uuid;

/// Trading algorithm trait
pub trait Algorithm: Send {
    /// Initialize the algorithm (called once at start)
    fn initialize(&mut self, context: &mut Context) {
        // Default implementation does nothing
        let _ = context;
    }

    /// Handle data event (called for each bar)
    fn handle_data(&mut self, context: &mut Context, data: &BarData) -> Result<()>;

    /// Before trading starts each day (optional)
    fn before_trading_start(&mut self, context: &mut Context, data: &BarData) -> Result<()> {
        let _ = (context, data);
        Ok(())
    }

    /// Analyze results after backtest (optional)
    fn analyze(&mut self, context: &Context) -> Result<()> {
        let _ = context;
        Ok(())
    }
}

/// Example: Buy and hold strategy
pub struct BuyAndHold {
    pub asset: Asset,
    pub initialized: bool,
}

impl BuyAndHold {
    pub fn new(asset: Asset) -> Self {
        Self {
            asset,
            initialized: false,
        }
    }
}

impl Algorithm for BuyAndHold {
    fn initialize(&mut self, context: &mut Context) {
        println!("Initializing Buy and Hold strategy");
        println!("Starting cash: {}", context.portfolio.cash);
    }

    fn handle_data(&mut self, context: &mut Context, data: &BarData) -> Result<()> {
        if !self.initialized && data.has_data(&self.asset) {
            // Buy as much as we can on first bar
            let price = data.current_price(&self.asset)?;
            let quantity = (context.portfolio.cash / price).floor();

            if quantity > 0.0 {
                context.order(self.asset.clone(), quantity)?;
                self.initialized = true;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_creation() {
        let context = Context::new(100000.0);
        assert_eq!(context.portfolio.cash, 100000.0);
        assert_eq!(context.pending_orders_count(), 0);
    }

    #[test]
    fn test_context_variables() {
        let mut context = Context::new(100000.0);
        context.set("test_value".to_string(), 42i32);

        assert_eq!(context.get::<i32>("test_value"), Some(&42));
        assert_eq!(context.get::<i32>("nonexistent"), None);
    }

    #[test]
    fn test_order_creation() {
        let mut context = Context::new(100000.0);
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());

        let order_id = context.order(asset, 100.0).unwrap();
        assert_eq!(context.pending_orders_count(), 1);
        assert_eq!(context.pending_orders[0].id, order_id);
    }
}
