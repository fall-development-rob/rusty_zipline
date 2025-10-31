//! Order blotter and transaction tracking

use crate::asset::Asset;
use crate::error::{Result, ZiplineError};
use crate::order::{Order, OrderStatus};
use crate::types::{Cash, OrderId, Price, Timestamp};
use chrono::{DateTime, Utc};
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

/// Individual transaction record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    /// Asset traded
    pub asset: Asset,
    /// Quantity traded (positive for buys, negative for sells)
    pub amount: f64,
    /// Transaction timestamp
    pub dt: DateTime<Utc>,
    /// Execution price
    pub price: Price,
    /// Order ID that generated this transaction
    pub order_id: OrderId,
    /// Commission paid
    pub commission: Cash,
    /// Transaction ID
    pub id: uuid::Uuid,
}

impl Transaction {
    pub fn new(
        asset: Asset,
        amount: f64,
        dt: DateTime<Utc>,
        price: Price,
        order_id: OrderId,
        commission: Cash,
    ) -> Self {
        Self {
            asset,
            amount,
            dt,
            price,
            order_id,
            commission,
            id: uuid::Uuid::new_v4(),
        }
    }

    /// Get transaction value (price * amount)
    pub fn value(&self) -> Cash {
        self.price * self.amount.abs()
    }

    /// Get total cost including commission
    pub fn total_cost(&self) -> Cash {
        self.value() + self.commission
    }
}

/// Transaction log for tracking all executions
#[derive(Debug, Clone)]
pub struct TransactionLog {
    transactions: Vec<Transaction>,
}

impl TransactionLog {
    pub fn new() -> Self {
        Self {
            transactions: Vec::new(),
        }
    }

    /// Record a new transaction
    pub fn record(&mut self, transaction: Transaction) {
        self.transactions.push(transaction);
    }

    /// Get all transactions
    pub fn get_transactions(&self) -> &[Transaction] {
        &self.transactions
    }

    /// Get transactions for a specific asset
    pub fn get_transactions_for_asset(&self, asset_id: u64) -> Vec<&Transaction> {
        self.transactions
            .iter()
            .filter(|t| t.asset.id == asset_id)
            .collect()
    }

    /// Get transactions in a date range
    pub fn get_transactions_in_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Vec<&Transaction> {
        self.transactions
            .iter()
            .filter(|t| t.dt >= start && t.dt <= end)
            .collect()
    }

    /// Get total transaction count
    pub fn count(&self) -> usize {
        self.transactions.len()
    }

    /// Get total commission paid
    pub fn total_commission(&self) -> Cash {
        self.transactions.iter().map(|t| t.commission).sum()
    }
}

impl Default for TransactionLog {
    fn default() -> Self {
        Self::new()
    }
}

/// Fill information for an order
#[derive(Debug, Clone)]
pub struct Fill {
    /// Fill price
    pub price: Price,
    /// Quantity filled
    pub quantity: f64,
    /// Commission for this fill
    pub commission: Cash,
    /// Fill timestamp
    pub dt: DateTime<Utc>,
}

impl Fill {
    pub fn new(price: Price, quantity: f64, commission: Cash, dt: DateTime<Utc>) -> Self {
        Self {
            price,
            quantity,
            commission,
            dt,
        }
    }
}

/// Order management blotter
pub struct Blotter {
    /// Open orders (not yet filled)
    open_orders: HashMap<OrderId, Order>,
    /// Filled orders
    filled_orders: HashMap<OrderId, Order>,
    /// Cancelled orders
    cancelled_orders: HashMap<OrderId, Order>,
    /// Rejected orders
    rejected_orders: HashMap<OrderId, Order>,
    /// Transaction log
    transactions: TransactionLog,
}

impl Blotter {
    pub fn new() -> Self {
        Self {
            open_orders: HashMap::new(),
            filled_orders: HashMap::new(),
            cancelled_orders: HashMap::new(),
            rejected_orders: HashMap::new(),
            transactions: TransactionLog::new(),
        }
    }

    /// Place a new order
    pub fn place_order(&mut self, order: Order) -> OrderId {
        let order_id = order.id;
        self.open_orders.insert(order_id, order);
        order_id
    }

    /// Cancel an order
    pub fn cancel_order(&mut self, order_id: OrderId, dt: DateTime<Utc>) -> Result<()> {
        if let Some(mut order) = self.open_orders.remove(&order_id) {
            order.cancel(dt);
            self.cancelled_orders.insert(order_id, order);
            Ok(())
        } else {
            Err(ZiplineError::InvalidOrder(format!(
                "Order {} not found or already closed",
                order_id
            )))
        }
    }

    /// Reject an order
    pub fn reject_order(&mut self, order_id: OrderId, reason: String) -> Result<()> {
        if let Some(mut order) = self.open_orders.remove(&order_id) {
            order.status = OrderStatus::Rejected;
            self.rejected_orders.insert(order_id, order);
            log::warn!("Order {} rejected: {}", order_id, reason);
            Ok(())
        } else {
            Err(ZiplineError::InvalidOrder(format!(
                "Order {} not found",
                order_id
            )))
        }
    }

    /// Process a fill for an order
    pub fn process_fill(&mut self, order_id: OrderId, fill: Fill) -> Result<Transaction> {
        let order = self.open_orders.get_mut(&order_id).ok_or_else(|| {
            ZiplineError::InvalidOrder(format!("Order {} not found", order_id))
        })?;

        // Update order with fill
        order.fill(fill.quantity, fill.dt);

        // Create transaction
        let transaction = Transaction::new(
            order.asset.clone(),
            match order.side {
                crate::order::OrderSide::Buy => fill.quantity,
                crate::order::OrderSide::Sell => -fill.quantity,
            },
            fill.dt,
            fill.price,
            order_id,
            fill.commission,
        );

        // Record transaction
        self.transactions.record(transaction.clone());

        // Move order to filled if complete
        if order.is_filled() {
            let order = self.open_orders.remove(&order_id).unwrap();
            self.filled_orders.insert(order_id, order);
        }

        Ok(transaction)
    }

    /// Get an order by ID
    pub fn get_order(&self, order_id: OrderId) -> Option<&Order> {
        self.open_orders
            .get(&order_id)
            .or_else(|| self.filled_orders.get(&order_id))
            .or_else(|| self.cancelled_orders.get(&order_id))
            .or_else(|| self.rejected_orders.get(&order_id))
    }

    /// Get all open orders
    pub fn get_open_orders(&self) -> Vec<&Order> {
        self.open_orders.values().collect()
    }

    /// Get open orders for a specific asset
    pub fn get_open_orders_for_asset(&self, asset_id: u64) -> Vec<&Order> {
        self.open_orders
            .values()
            .filter(|o| o.asset.id == asset_id)
            .collect()
    }

    /// Get filled orders
    pub fn get_filled_orders(&self) -> Vec<&Order> {
        self.filled_orders.values().collect()
    }

    /// Get cancelled orders
    pub fn get_cancelled_orders(&self) -> Vec<&Order> {
        self.cancelled_orders.values().collect()
    }

    /// Get transaction log
    pub fn transactions(&self) -> &TransactionLog {
        &self.transactions
    }

    /// Get order counts by status
    pub fn order_counts(&self) -> (usize, usize, usize, usize) {
        (
            self.open_orders.len(),
            self.filled_orders.len(),
            self.cancelled_orders.len(),
            self.rejected_orders.len(),
        )
    }

    /// Clear all orders (for testing)
    #[cfg(test)]
    pub fn clear(&mut self) {
        self.open_orders.clear();
        self.filled_orders.clear();
        self.cancelled_orders.clear();
        self.rejected_orders.clear();
        self.transactions = TransactionLog::new();
    }
}

impl Default for Blotter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::Asset;
    use crate::order::{Order, OrderSide};
    use chrono::Utc;

    #[test]
    fn test_transaction_creation() {
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        let dt = Utc::now();
        let order_id = uuid::Uuid::new_v4();

        let txn = Transaction::new(asset, 100.0, dt, 150.0, order_id, 1.0);

        assert_eq!(txn.amount, 100.0);
        assert_eq!(txn.price, 150.0);
        assert_eq!(txn.commission, 1.0);
        assert_eq!(txn.value(), 15000.0);
        assert_eq!(txn.total_cost(), 15001.0);
    }

    #[test]
    fn test_transaction_log() {
        let mut log = TransactionLog::new();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        let dt = Utc::now();

        let txn1 = Transaction::new(asset.clone(), 100.0, dt, 150.0, uuid::Uuid::new_v4(), 1.0);
        let txn2 = Transaction::new(asset, 50.0, dt, 151.0, uuid::Uuid::new_v4(), 0.5);

        log.record(txn1);
        log.record(txn2);

        assert_eq!(log.count(), 2);
        assert_eq!(log.total_commission(), 1.5);
    }

    #[test]
    fn test_blotter_place_order() {
        let mut blotter = Blotter::new();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        let order = Order::market(asset, OrderSide::Buy, 100.0, Utc::now());
        let order_id = order.id;

        blotter.place_order(order);

        assert_eq!(blotter.order_counts(), (1, 0, 0, 0)); // 1 open, 0 filled, 0 cancelled, 0 rejected
        assert!(blotter.get_order(order_id).is_some());
    }

    #[test]
    fn test_blotter_fill_order() {
        let mut blotter = Blotter::new();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        let order = Order::market(asset, OrderSide::Buy, 100.0, Utc::now());
        let order_id = order.id;

        blotter.place_order(order);

        let fill = Fill::new(150.0, 100.0, 1.0, Utc::now());
        let txn = blotter.process_fill(order_id, fill).unwrap();

        assert_eq!(txn.amount, 100.0); // Buy is positive
        assert_eq!(txn.price, 150.0);
        assert_eq!(blotter.order_counts(), (0, 1, 0, 0)); // Moved to filled
        assert_eq!(blotter.transactions().count(), 1);
    }

    #[test]
    fn test_blotter_cancel_order() {
        let mut blotter = Blotter::new();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        let order = Order::market(asset, OrderSide::Buy, 100.0, Utc::now());
        let order_id = order.id;

        blotter.place_order(order);
        blotter.cancel_order(order_id, Utc::now()).unwrap();

        assert_eq!(blotter.order_counts(), (0, 0, 1, 0)); // Moved to cancelled
    }

    #[test]
    fn test_blotter_partial_fill() {
        let mut blotter = Blotter::new();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        let order = Order::market(asset, OrderSide::Buy, 100.0, Utc::now());
        let order_id = order.id;

        blotter.place_order(order);

        // First partial fill
        let fill1 = Fill::new(150.0, 50.0, 0.5, Utc::now());
        blotter.process_fill(order_id, fill1).unwrap();

        assert_eq!(blotter.order_counts(), (1, 0, 0, 0)); // Still open

        // Second fill completes the order
        let fill2 = Fill::new(150.5, 50.0, 0.5, Utc::now());
        blotter.process_fill(order_id, fill2).unwrap();

        assert_eq!(blotter.order_counts(), (0, 1, 0, 0)); // Now filled
        assert_eq!(blotter.transactions().count(), 2);
    }
}
