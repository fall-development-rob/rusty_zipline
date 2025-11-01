//! Trading controls - restrictions and validations on trading
//!
//! This module provides controls to restrict trading behavior,
//! such as position limits, order value limits, and leverage constraints.

use crate::error::{Result, ZiplineError};
use crate::finance::Portfolio;
use crate::order::Order;
use std::collections::HashMap;

/// Trading control trait - validates orders before execution
pub trait TradingControl: Send + Sync {
    /// Validate an order
    ///
    /// Returns Ok(()) if order is valid, Err if it violates controls
    fn validate(&self, order: &Order, portfolio: &Portfolio) -> Result<()>;

    /// Get control name for debugging
    fn name(&self) -> &str {
        "TradingControl"
    }
}

/// MaxPositionSize - Limit maximum position size per asset
pub struct MaxPositionSize {
    /// Maximum absolute position size (in shares)
    max_shares: Option<f64>,
    /// Maximum position value (in dollars)
    max_value: Option<f64>,
}

impl MaxPositionSize {
    pub fn new(max_shares: Option<f64>, max_value: Option<f64>) -> Self {
        Self {
            max_shares,
            max_value,
        }
    }

    pub fn by_shares(max_shares: f64) -> Self {
        Self {
            max_shares: Some(max_shares),
            max_value: None,
        }
    }

    pub fn by_value(max_value: f64) -> Self {
        Self {
            max_shares: None,
            max_value: Some(max_value),
        }
    }
}

impl TradingControl for MaxPositionSize {
    fn validate(&self, order: &Order, portfolio: &Portfolio) -> Result<()> {
        let current_position = portfolio
            .get_position(order.asset.id)
            .map(|p| p.quantity)
            .unwrap_or(0.0);

        let new_position = current_position + order.filled_quantity();

        // Check share limit
        if let Some(max_shares) = self.max_shares {
            if new_position.abs() > max_shares {
                return Err(ZiplineError::MaxPositionSizeExceeded {
                    asset_id: order.asset.id,
                    current: current_position,
                    attempted: new_position,
                    limit: max_shares,
                });
            }
        }

        // Check value limit (would need price data)
        if let Some(max_value) = self.max_value {
            let estimated_value = new_position.abs() * order.limit_price.unwrap_or(0.0);
            if estimated_value > max_value {
                return Err(ZiplineError::MaxPositionSizeExceeded {
                    asset_id: order.asset.id,
                    current: current_position,
                    attempted: new_position,
                    limit: max_value,
                });
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "MaxPositionSize"
    }
}

/// MaxOrderSize - Limit individual order sizes
pub struct MaxOrderSize {
    /// Maximum order size in shares
    max_shares: f64,
}

impl MaxOrderSize {
    pub fn new(max_shares: f64) -> Self {
        Self { max_shares }
    }
}

impl TradingControl for MaxOrderSize {
    fn validate(&self, order: &Order, _portfolio: &Portfolio) -> Result<()> {
        if order.quantity.abs() > self.max_shares {
            return Err(ZiplineError::MaxOrderSizeExceeded {
                asset_id: order.asset.id,
                attempted: order.quantity,
                limit: self.max_shares,
            });
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "MaxOrderSize"
    }
}

/// MaxLeverage - Limit portfolio leverage
pub struct MaxLeverage {
    max_leverage: f64,
}

impl MaxLeverage {
    pub fn new(max_leverage: f64) -> Self {
        Self { max_leverage }
    }
}

impl TradingControl for MaxLeverage {
    fn validate(&self, _order: &Order, portfolio: &Portfolio) -> Result<()> {
        let leverage = portfolio.calculate_leverage();
        if leverage > self.max_leverage {
            return Err(ZiplineError::MaxLeverageExceeded {
                current: leverage,
                limit: self.max_leverage,
            });
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "MaxLeverage"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::Asset;
    use crate::order::OrderSide;
    use chrono::Utc;

    #[test]
    fn test_max_position_size_by_shares() {
        let control = MaxPositionSize::by_shares(1000.0);
        let portfolio = Portfolio::new(100000.0);

        let asset = Asset::equity(1, "TEST".to_string(), "NYSE".to_string());
        let order = Order::market(asset, OrderSide::Buy, 500.0, Utc::now());

        assert!(control.validate(&order, &portfolio).is_ok());
    }

    #[test]
    fn test_max_order_size() {
        let control = MaxOrderSize::new(100.0);
        let portfolio = Portfolio::new(100000.0);

        let asset = Asset::equity(1, "TEST".to_string(), "NYSE".to_string());
        let small_order = Order::market(asset.clone(), OrderSide::Buy, 50.0, Utc::now());
        let large_order = Order::market(asset, OrderSide::Buy, 150.0, Utc::now());

        assert!(control.validate(&small_order, &portfolio).is_ok());
        assert!(control.validate(&large_order, &portfolio).is_err());
    }

    #[test]
    fn test_trading_control_name() {
        let control = MaxOrderSize::new(100.0);
        assert_eq!(control.name(), "MaxOrderSize");
    }
}
