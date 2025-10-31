//! Order execution and slippage models

use crate::error::Result;
use crate::order::{Order, OrderStatus, OrderType};
use crate::types::{Cash, Price, Timestamp};

/// Slippage model trait
pub trait SlippageModel: Send + Sync {
    /// Calculate slippage for an order
    fn calculate_slippage(&self, order: &Order, current_price: Price) -> Price;
}

/// No slippage model
#[derive(Debug, Clone, Copy)]
pub struct NoSlippage;

impl SlippageModel for NoSlippage {
    fn calculate_slippage(&self, _order: &Order, _current_price: Price) -> Price {
        0.0
    }
}

/// Fixed slippage model (adds fixed amount to price)
#[derive(Debug, Clone, Copy)]
pub struct FixedSlippage {
    pub slippage: Price,
}

impl FixedSlippage {
    pub fn new(slippage: Price) -> Self {
        Self { slippage }
    }
}

impl SlippageModel for FixedSlippage {
    fn calculate_slippage(&self, order: &Order, _current_price: Price) -> Price {
        match order.side {
            crate::order::OrderSide::Buy => self.slippage,
            crate::order::OrderSide::Sell => -self.slippage,
        }
    }
}

/// Volume share slippage model (percentage of price)
#[derive(Debug, Clone, Copy)]
pub struct VolumeShareSlippage {
    /// Slippage as percentage of price
    pub percentage: f64,
}

impl VolumeShareSlippage {
    pub fn new(percentage: f64) -> Self {
        Self { percentage }
    }
}

impl SlippageModel for VolumeShareSlippage {
    fn calculate_slippage(&self, order: &Order, current_price: Price) -> Price {
        let slippage = current_price * self.percentage;
        match order.side {
            crate::order::OrderSide::Buy => slippage,
            crate::order::OrderSide::Sell => -slippage,
        }
    }
}

/// Commission model trait
pub trait CommissionModel: Send + Sync {
    /// Calculate commission for an order fill
    fn calculate_commission(&self, order: &Order, fill_price: Price) -> Cash;
}

/// No commission model
#[derive(Debug, Clone, Copy)]
pub struct NoCommission;

impl CommissionModel for NoCommission {
    fn calculate_commission(&self, _order: &Order, _fill_price: Price) -> Cash {
        0.0
    }
}

/// Per-share commission model
#[derive(Debug, Clone, Copy)]
pub struct PerShareCommission {
    pub cost_per_share: Cash,
}

impl PerShareCommission {
    pub fn new(cost_per_share: Cash) -> Self {
        Self { cost_per_share }
    }
}

impl CommissionModel for PerShareCommission {
    fn calculate_commission(&self, order: &Order, _fill_price: Price) -> Cash {
        order.filled * self.cost_per_share
    }
}

/// Per-trade commission model (flat fee)
#[derive(Debug, Clone, Copy)]
pub struct PerTradeCommission {
    pub cost_per_trade: Cash,
}

impl PerTradeCommission {
    pub fn new(cost_per_trade: Cash) -> Self {
        Self { cost_per_trade }
    }
}

impl CommissionModel for PerTradeCommission {
    fn calculate_commission(&self, _order: &Order, _fill_price: Price) -> Cash {
        self.cost_per_trade
    }
}

/// Simulated broker for backtesting
pub struct SimulatedBroker {
    slippage_model: Box<dyn SlippageModel>,
    commission_model: Box<dyn CommissionModel>,
}

impl SimulatedBroker {
    /// Create a new simulated broker
    pub fn new(
        slippage_model: Box<dyn SlippageModel>,
        commission_model: Box<dyn CommissionModel>,
    ) -> Self {
        Self {
            slippage_model,
            commission_model,
        }
    }

    /// Create a broker with no slippage or commission
    pub fn default_broker() -> Self {
        Self::new(Box::new(NoSlippage), Box::new(NoCommission))
    }

    /// Execute an order at current price
    pub fn execute_order(
        &self,
        order: &mut Order,
        current_price: Price,
        timestamp: Timestamp,
    ) -> Result<ExecutionResult> {
        // Calculate slippage
        let slippage = self.slippage_model.calculate_slippage(order, current_price);
        let execution_price = current_price + slippage;

        // Check if order can be filled based on type
        let can_fill = match order.order_type {
            OrderType::Market => true,
            OrderType::Limit => {
                if let Some(limit) = order.limit_price {
                    match order.side {
                        crate::order::OrderSide::Buy => execution_price <= limit,
                        crate::order::OrderSide::Sell => execution_price >= limit,
                    }
                } else {
                    false
                }
            }
            OrderType::Stop => {
                if let Some(stop) = order.stop_price {
                    match order.side {
                        crate::order::OrderSide::Buy => current_price >= stop,
                        crate::order::OrderSide::Sell => current_price <= stop,
                    }
                } else {
                    false
                }
            }
            OrderType::StopLimit => {
                // Simplified: check stop first, then limit
                if let (Some(stop), Some(limit)) = (order.stop_price, order.limit_price) {
                    let stop_triggered = match order.side {
                        crate::order::OrderSide::Buy => current_price >= stop,
                        crate::order::OrderSide::Sell => current_price <= stop,
                    };

                    if stop_triggered {
                        match order.side {
                            crate::order::OrderSide::Buy => execution_price <= limit,
                            crate::order::OrderSide::Sell => execution_price >= limit,
                        }
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        };

        if !can_fill {
            return Ok(ExecutionResult::NotFilled);
        }

        // Update order status
        if order.status == OrderStatus::Created {
            order.status = OrderStatus::Submitted;
        }

        // Fill the order
        let fill_quantity = order.remaining();
        order.fill(fill_quantity, timestamp);

        // Calculate commission
        let commission = self.commission_model.calculate_commission(order, execution_price);

        Ok(ExecutionResult::Filled {
            price: execution_price,
            quantity: fill_quantity,
            commission,
        })
    }
}

/// Result of order execution
#[derive(Debug, Clone)]
pub enum ExecutionResult {
    Filled {
        price: Price,
        quantity: f64,
        commission: Cash,
    },
    NotFilled,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::Asset;
    use crate::order::{Order, OrderSide};
    use chrono::Utc;

    #[test]
    fn test_fixed_slippage() {
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        let order = Order::market(asset, OrderSide::Buy, 100.0, Utc::now());
        let slippage = FixedSlippage::new(0.05);

        let slip = slippage.calculate_slippage(&order, 150.0);
        assert_eq!(slip, 0.05);
    }

    #[test]
    fn test_volume_share_slippage() {
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        let order = Order::market(asset, OrderSide::Buy, 100.0, Utc::now());
        let slippage = VolumeShareSlippage::new(0.001); // 0.1%

        let slip = slippage.calculate_slippage(&order, 150.0);
        assert_eq!(slip, 0.15);
    }

    #[test]
    fn test_per_share_commission() {
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        let mut order = Order::market(asset, OrderSide::Buy, 100.0, Utc::now());
        order.fill(100.0, Utc::now());

        let commission = PerShareCommission::new(0.01);
        let cost = commission.calculate_commission(&order, 150.0);
        assert_eq!(cost, 1.0);
    }

    #[test]
    fn test_simulated_broker_execution() {
        let broker = SimulatedBroker::default_broker();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        let mut order = Order::market(asset, OrderSide::Buy, 100.0, Utc::now());

        let result = broker.execute_order(&mut order, 150.0, Utc::now()).unwrap();

        match result {
            ExecutionResult::Filled { price, quantity, commission } => {
                assert_eq!(price, 150.0);
                assert_eq!(quantity, 100.0);
                assert_eq!(commission, 0.0);
                assert!(order.is_filled());
            }
            ExecutionResult::NotFilled => panic!("Order should have been filled"),
        }
    }
}
