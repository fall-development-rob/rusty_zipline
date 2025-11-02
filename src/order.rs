//! Order types and management

use crate::asset::Asset;
use crate::types::{Cash, OrderId, Price, Quantity, Timestamp};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Order side (buy or sell)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

/// Order type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    /// Market order - execute at current market price
    Market,
    /// Limit order - execute at specified price or better
    Limit,
    /// Stop order - trigger market order when price reached
    Stop,
    /// Stop-limit order - trigger limit order when price reached
    StopLimit,
}

/// Order status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    /// Order created but not yet submitted
    Created,
    /// Order submitted to broker
    Submitted,
    /// Order partially filled
    PartiallyFilled,
    /// Order completely filled
    Filled,
    /// Order cancelled
    Cancelled,
    /// Order rejected
    Rejected,
}

/// Trading order
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    /// Unique order identifier
    pub id: OrderId,
    /// Asset to trade
    pub asset: Asset,
    /// Order side (buy/sell)
    pub side: OrderSide,
    /// Order type
    pub order_type: OrderType,
    /// Requested quantity
    pub quantity: Quantity,
    /// Filled quantity
    pub filled: Quantity,
    /// Limit price (for limit orders)
    pub limit_price: Option<Price>,
    /// Stop price (for stop orders)
    pub stop_price: Option<Price>,
    /// Order status
    pub status: OrderStatus,
    /// Creation timestamp
    pub created_at: Timestamp,
    /// Creation datetime (alternative field)
    pub created: DateTime<Utc>,
    /// Last update timestamp
    pub updated_at: Timestamp,
    /// Order amount in cash terms
    pub amount: Cash,
}

impl Order {
    /// Create a new market order
    pub fn market(asset: Asset, side: OrderSide, quantity: Quantity, timestamp: Timestamp) -> Self {
        Self {
            id: OrderId::new_v4(),
            asset,
            side,
            order_type: OrderType::Market,
            quantity,
            filled: 0.0,
            limit_price: None,
            stop_price: None,
            status: OrderStatus::Created,
            created_at: timestamp,
            created: timestamp,
            updated_at: timestamp,
            amount: 0.0, // Will be calculated when order is filled
        }
    }

    /// Create a new limit order
    pub fn limit(
        asset: Asset,
        side: OrderSide,
        quantity: Quantity,
        limit_price: Price,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            id: OrderId::new_v4(),
            asset,
            side,
            order_type: OrderType::Limit,
            quantity,
            filled: 0.0,
            limit_price: Some(limit_price),
            stop_price: None,
            status: OrderStatus::Created,
            created_at: timestamp,
            created: timestamp,
            updated_at: timestamp,
            amount: quantity * limit_price, // Calculate expected amount
        }
    }

    /// Create a new stop order
    pub fn stop(
        asset: Asset,
        side: OrderSide,
        quantity: Quantity,
        stop_price: Price,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            id: OrderId::new_v4(),
            asset,
            side,
            order_type: OrderType::Stop,
            quantity,
            filled: 0.0,
            limit_price: None,
            stop_price: Some(stop_price),
            status: OrderStatus::Created,
            created_at: timestamp,
            created: timestamp,
            updated_at: timestamp,
            amount: quantity * stop_price, // Calculate expected amount
        }
    }

    /// Create a new stop-limit order
    pub fn stop_limit(
        asset: Asset,
        side: OrderSide,
        quantity: Quantity,
        stop_price: Price,
        limit_price: Price,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            id: OrderId::new_v4(),
            asset,
            side,
            order_type: OrderType::StopLimit,
            quantity,
            filled: 0.0,
            limit_price: Some(limit_price),
            stop_price: Some(stop_price),
            status: OrderStatus::Created,
            created_at: timestamp,
            created: timestamp,
            updated_at: timestamp,
            amount: quantity * limit_price, // Calculate expected amount at limit price
        }
    }

    /// Get remaining quantity to fill
    pub fn remaining(&self) -> Quantity {
        self.quantity - self.filled
    }

    /// Check if order is completely filled
    pub fn is_filled(&self) -> bool {
        self.filled >= self.quantity
    }

    /// Check if order is open (can still be filled)
    pub fn is_open(&self) -> bool {
        matches!(
            self.status,
            OrderStatus::Created | OrderStatus::Submitted | OrderStatus::PartiallyFilled
        )
    }

    /// Check if order is closed (terminal state)
    pub fn is_closed(&self) -> bool {
        matches!(
            self.status,
            OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected
        )
    }

    /// Fill order (partial or complete)
    pub fn fill(&mut self, quantity: Quantity, timestamp: Timestamp) {
        self.filled += quantity;
        self.updated_at = timestamp;

        if self.is_filled() {
            self.status = OrderStatus::Filled;
        } else {
            self.status = OrderStatus::PartiallyFilled;
        }
    }

    /// Cancel order
    pub fn cancel(&mut self, timestamp: Timestamp) {
        self.status = OrderStatus::Cancelled;
        self.updated_at = timestamp;
    }

    /// Get filled quantity
    pub fn filled_quantity(&self) -> Quantity {
        self.filled
    }

    /// Get open (unfilled) quantity
    pub fn open_quantity(&self) -> Quantity {
        self.quantity - self.filled
    }
}

impl fmt::Display for Order {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Order({:?}, {}, {:?}, {}/{}, {:?})",
            self.side, self.asset.symbol, self.order_type, self.filled, self.quantity, self.status
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::Asset;
    use chrono::{NaiveDate, Utc};

    #[test]
    fn test_market_order() {
        let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
        let order = Order::market(asset, OrderSide::Buy, 100.0, Utc::now());

        assert_eq!(order.side, OrderSide::Buy);
        assert_eq!(order.quantity, 100.0);
        assert_eq!(order.filled, 0.0);
        assert_eq!(order.remaining(), 100.0);
        assert!(!order.is_filled());
        assert!(order.is_open());
    }

    #[test]
    fn test_order_filling() {
        let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
        let mut order = Order::market(asset, OrderSide::Buy, 100.0, Utc::now());

        order.fill(50.0, Utc::now());
        assert_eq!(order.filled, 50.0);
        assert_eq!(order.status, OrderStatus::PartiallyFilled);
        assert!(!order.is_filled());

        order.fill(50.0, Utc::now());
        assert_eq!(order.filled, 100.0);
        assert_eq!(order.status, OrderStatus::Filled);
        assert!(order.is_filled());
        assert!(order.is_closed());
    }
}
