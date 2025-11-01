//! Transaction - represents executed orders
//!
//! A Transaction is created when an Order is filled (executed).
//! It records the actual price, quantity, and costs of the trade.

use crate::error::Result;
use crate::order::OrderSide;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Transaction ID
pub type TransactionId = Uuid;

/// Transaction represents an executed trade
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    /// Unique transaction ID
    pub id: TransactionId,
    /// Asset ID
    pub asset_id: u64,
    /// Order ID that generated this transaction
    pub order_id: Uuid,
    /// Transaction timestamp
    pub dt: DateTime<Utc>,
    /// Number of shares traded (positive for buy, negative for sell)
    pub amount: f64,
    /// Price per share
    pub price: f64,
    /// Commission paid
    pub commission: f64,
    /// Order side (Buy/Sell)
    pub side: OrderSide,
}

impl Transaction {
    /// Create a new transaction
    pub fn new(
        asset_id: u64,
        order_id: Uuid,
        dt: DateTime<Utc>,
        amount: f64,
        price: f64,
        commission: f64,
        side: OrderSide,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            asset_id,
            order_id,
            dt,
            amount,
            price,
            commission,
            side,
        }
    }

    /// Get total transaction value (price * amount)
    pub fn value(&self) -> f64 {
        self.price * self.amount.abs()
    }

    /// Get total cost including commission
    pub fn total_cost(&self) -> f64 {
        self.value() + self.commission
    }

    /// Check if this is a buy transaction
    pub fn is_buy(&self) -> bool {
        matches!(self.side, OrderSide::Buy)
    }

    /// Check if this is a sell transaction
    pub fn is_sell(&self) -> bool {
        matches!(self.side, OrderSide::Sell)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_creation() {
        let order_id = Uuid::new_v4();
        let dt = Utc::now();

        let txn = Transaction::new(1, order_id, dt, 100.0, 150.0, 5.0, OrderSide::Buy);

        assert_eq!(txn.asset_id, 1);
        assert_eq!(txn.order_id, order_id);
        assert_eq!(txn.amount, 100.0);
        assert_eq!(txn.price, 150.0);
        assert_eq!(txn.commission, 5.0);
        assert!(txn.is_buy());
        assert!(!txn.is_sell());
    }

    #[test]
    fn test_transaction_value() {
        let txn = Transaction::new(
            1,
            Uuid::new_v4(),
            Utc::now(),
            100.0,
            150.0,
            5.0,
            OrderSide::Buy,
        );

        assert_eq!(txn.value(), 15000.0); // 100 shares * $150
        assert_eq!(txn.total_cost(), 15005.0); // value + $5 commission
    }

    #[test]
    fn test_sell_transaction() {
        let txn = Transaction::new(
            1,
            Uuid::new_v4(),
            Utc::now(),
            -50.0,
            200.0,
            3.0,
            OrderSide::Sell,
        );

        assert_eq!(txn.value(), 10000.0); // abs(-50) * $200
        assert_eq!(txn.total_cost(), 10003.0);
        assert!(txn.is_sell());
        assert!(!txn.is_buy());
    }

    #[test]
    fn test_transaction_side() {
        let buy_txn = Transaction::new(
            1,
            Uuid::new_v4(),
            Utc::now(),
            100.0,
            150.0,
            0.0,
            OrderSide::Buy,
        );

        let sell_txn = Transaction::new(
            1,
            Uuid::new_v4(),
            Utc::now(),
            -100.0,
            150.0,
            0.0,
            OrderSide::Sell,
        );

        assert!(buy_txn.is_buy());
        assert!(!buy_txn.is_sell());

        assert!(sell_txn.is_sell());
        assert!(!sell_txn.is_buy());
    }
}
