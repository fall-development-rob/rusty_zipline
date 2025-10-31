//! Trading controls and restrictions

use crate::algorithm::Context;
use crate::error::{Result, ZiplineError};
use crate::order::Order;
use chrono::Duration;
use hashbrown::HashSet;
use std::collections::VecDeque;

/// Trait for order-level trading controls
pub trait TradingControl: Send + Sync {
    /// Validate an order before submission
    fn validate_order(&self, order: &Order, context: &Context) -> Result<()>;

    /// Get control name for error messages
    fn name(&self) -> &str;
}

/// Trait for account-level controls
pub trait AccountControl: Send + Sync {
    /// Validate account state
    fn validate_account(&self, context: &Context) -> Result<()>;

    /// Get control name
    fn name(&self) -> &str;
}

/// Restrict maximum order size
pub struct MaxOrderSize {
    /// Maximum number of shares per order
    pub max_shares: Option<f64>,
    /// Maximum dollar value per order
    pub max_notional: Option<f64>,
}

impl MaxOrderSize {
    pub fn shares(max_shares: f64) -> Self {
        Self {
            max_shares: Some(max_shares),
            max_notional: None,
        }
    }

    pub fn notional(max_notional: f64) -> Self {
        Self {
            max_shares: None,
            max_notional: Some(max_notional),
        }
    }

    pub fn both(max_shares: f64, max_notional: f64) -> Self {
        Self {
            max_shares: Some(max_shares),
            max_notional: Some(max_notional),
        }
    }
}

impl TradingControl for MaxOrderSize {
    fn validate_order(&self, order: &Order, context: &Context) -> Result<()> {
        if let Some(max) = self.max_shares {
            if order.quantity > max {
                return Err(ZiplineError::InvalidOrder(format!(
                    "Order size {} exceeds maximum of {}",
                    order.quantity, max
                )));
            }
        }

        if let Some(max) = self.max_notional {
            // Estimate notional value (would need current price in real implementation)
            let estimated_notional = order.quantity * context.portfolio.portfolio_value / 100.0;
            if estimated_notional > max {
                return Err(ZiplineError::InvalidOrder(format!(
                    "Order notional value ~{} exceeds maximum of {}",
                    estimated_notional, max
                )));
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "MaxOrderSize"
    }
}

/// Limit number of orders per time period
pub struct MaxOrderCount {
    pub max_count: usize,
    pub period: Duration,
    order_times: VecDeque<chrono::DateTime<chrono::Utc>>,
}

impl MaxOrderCount {
    pub fn new(max_count: usize, period: Duration) -> Self {
        Self {
            max_count,
            period,
            order_times: VecDeque::new(),
        }
    }

    pub fn per_day(max_count: usize) -> Self {
        Self::new(max_count, Duration::days(1))
    }

    pub fn per_hour(max_count: usize) -> Self {
        Self::new(max_count, Duration::hours(1))
    }
}

impl TradingControl for MaxOrderCount {
    fn validate_order(&self, order: &Order, context: &Context) -> Result<()> {
        let cutoff = context.timestamp - self.period;
        let recent_orders: usize = self
            .order_times
            .iter()
            .filter(|&&t| t > cutoff)
            .count();

        if recent_orders >= self.max_count {
            return Err(ZiplineError::InvalidOrder(format!(
                "Order count {} in last {:?} exceeds maximum of {}",
                recent_orders, self.period, self.max_count
            )));
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "MaxOrderCount"
    }
}

/// Restrict maximum position size
pub struct MaxPositionSize {
    /// Maximum shares per position
    pub max_shares: Option<f64>,
    /// Maximum percentage of portfolio per position
    pub max_pct_portfolio: Option<f64>,
}

impl MaxPositionSize {
    pub fn shares(max_shares: f64) -> Self {
        Self {
            max_shares: Some(max_shares),
            max_pct_portfolio: None,
        }
    }

    pub fn percent(max_pct: f64) -> Self {
        Self {
            max_shares: None,
            max_pct_portfolio: Some(max_pct),
        }
    }

    pub fn both(max_shares: f64, max_pct: f64) -> Self {
        Self {
            max_shares: Some(max_shares),
            max_pct_portfolio: Some(max_pct),
        }
    }
}

impl TradingControl for MaxPositionSize {
    fn validate_order(&self, order: &Order, context: &Context) -> Result<()> {
        let current_position = context
            .portfolio
            .get_position(order.asset.id)
            .map(|p| p.quantity)
            .unwrap_or(0.0);

        let new_position = match order.side {
            crate::order::OrderSide::Buy => current_position + order.quantity,
            crate::order::OrderSide::Sell => current_position - order.quantity,
        };

        if let Some(max) = self.max_shares {
            if new_position.abs() > max {
                return Err(ZiplineError::InvalidOrder(format!(
                    "New position size {} would exceed maximum of {}",
                    new_position.abs(),
                    max
                )));
            }
        }

        if let Some(max_pct) = self.max_pct_portfolio {
            // Estimate position value as percentage of portfolio
            let estimated_value = new_position.abs() * 100.0; // Placeholder
            let pct = estimated_value / context.portfolio.portfolio_value;
            if pct > max_pct {
                return Err(ZiplineError::InvalidOrder(format!(
                    "New position would be {:.1}% of portfolio, exceeding maximum of {:.1}%",
                    pct * 100.0,
                    max_pct * 100.0
                )));
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "MaxPositionSize"
    }
}

/// Prevent trading specific assets
pub struct RestrictedList {
    pub restricted_assets: HashSet<u64>,
}

impl RestrictedList {
    pub fn new() -> Self {
        Self {
            restricted_assets: HashSet::new(),
        }
    }

    pub fn add_asset(&mut self, asset_id: u64) {
        self.restricted_assets.insert(asset_id);
    }

    pub fn remove_asset(&mut self, asset_id: u64) {
        self.restricted_assets.remove(&asset_id);
    }

    pub fn is_restricted(&self, asset_id: u64) -> bool {
        self.restricted_assets.contains(&asset_id)
    }
}

impl TradingControl for RestrictedList {
    fn validate_order(&self, order: &Order, _context: &Context) -> Result<()> {
        if self.is_restricted(order.asset.id) {
            return Err(ZiplineError::InvalidOrder(format!(
                "Asset {} ({}) is on the restricted list",
                order.asset.symbol, order.asset.id
            )));
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "RestrictedList"
    }
}

impl Default for RestrictedList {
    fn default() -> Self {
        Self::new()
    }
}

/// Prevent short selling (long-only strategy)
pub struct LongOnly;

impl TradingControl for LongOnly {
    fn validate_order(&self, order: &Order, context: &Context) -> Result<()> {
        let current_position = context
            .portfolio
            .get_position(order.asset.id)
            .map(|p| p.quantity)
            .unwrap_or(0.0);

        match order.side {
            crate::order::OrderSide::Sell => {
                if order.quantity > current_position {
                    return Err(ZiplineError::InvalidOrder(
                        "Long-only mode: cannot short sell".to_string(),
                    ));
                }
            }
            crate::order::OrderSide::Buy => {
                // Buying is always allowed in long-only
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "LongOnly"
    }
}

/// Restrict maximum account leverage
pub struct MaxLeverage {
    pub max_leverage: f64,
}

impl MaxLeverage {
    pub fn new(max_leverage: f64) -> Self {
        Self { max_leverage }
    }
}

impl AccountControl for MaxLeverage {
    fn validate_account(&self, context: &Context) -> Result<()> {
        let leverage = context.portfolio.leverage();
        if leverage > self.max_leverage {
            return Err(ZiplineError::InvalidOrder(format!(
                "Account leverage {:.2} exceeds maximum of {:.2}",
                leverage, self.max_leverage
            )));
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "MaxLeverage"
    }
}

/// Restrict minimum account leverage
pub struct MinLeverage {
    pub min_leverage: f64,
}

impl MinLeverage {
    pub fn new(min_leverage: f64) -> Self {
        Self { min_leverage }
    }
}

impl AccountControl for MinLeverage {
    fn validate_account(&self, context: &Context) -> Result<()> {
        let leverage = context.portfolio.leverage();
        if leverage < self.min_leverage {
            return Err(ZiplineError::InvalidOrder(format!(
                "Account leverage {:.2} is below minimum of {:.2}",
                leverage, self.min_leverage
            )));
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "MinLeverage"
    }
}

/// Manager for all trading controls
pub struct ControlManager {
    order_controls: Vec<Box<dyn TradingControl>>,
    account_controls: Vec<Box<dyn AccountControl>>,
}

impl ControlManager {
    pub fn new() -> Self {
        Self {
            order_controls: Vec::new(),
            account_controls: Vec::new(),
        }
    }

    /// Add an order-level control
    pub fn add_order_control(&mut self, control: Box<dyn TradingControl>) {
        self.order_controls.push(control);
    }

    /// Add an account-level control
    pub fn add_account_control(&mut self, control: Box<dyn AccountControl>) {
        self.account_controls.push(control);
    }

    /// Validate an order against all controls
    pub fn validate_order(&self, order: &Order, context: &Context) -> Result<()> {
        for control in &self.order_controls {
            control.validate_order(order, context)?;
        }
        Ok(())
    }

    /// Validate account state against all controls
    pub fn validate_account(&self, context: &Context) -> Result<()> {
        for control in &self.account_controls {
            control.validate_account(context)?;
        }
        Ok(())
    }

    /// Get count of active controls
    pub fn control_count(&self) -> (usize, usize) {
        (self.order_controls.len(), self.account_controls.len())
    }
}

impl Default for ControlManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm::Context;
    use crate::asset::Asset;
    use crate::order::{Order, OrderSide};
    use chrono::Utc;

    #[test]
    fn test_max_order_size() {
        let control = MaxOrderSize::shares(100.0);
        let context = Context::new(100000.0);
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());

        let valid_order = Order::market(asset.clone(), OrderSide::Buy, 50.0, Utc::now());
        assert!(control.validate_order(&valid_order, &context).is_ok());

        let invalid_order = Order::market(asset, OrderSide::Buy, 200.0, Utc::now());
        assert!(control.validate_order(&invalid_order, &context).is_err());
    }

    #[test]
    fn test_long_only() {
        let control = LongOnly;
        let mut context = Context::new(100000.0);
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());

        // Buying is allowed
        let buy_order = Order::market(asset.clone(), OrderSide::Buy, 100.0, Utc::now());
        assert!(control.validate_order(&buy_order, &context).is_ok());

        // Selling more than owned is not allowed (short selling)
        let short_order = Order::market(asset, OrderSide::Sell, 100.0, Utc::now());
        assert!(control.validate_order(&short_order, &context).is_err());
    }

    #[test]
    fn test_restricted_list() {
        let mut control = RestrictedList::new();
        control.add_asset(1);

        let context = Context::new(100000.0);
        let restricted_asset = Asset::equity(1, "BANNED".to_string(), "NYSE".to_string());
        let allowed_asset = Asset::equity(2, "AAPL".to_string(), "NASDAQ".to_string());

        let restricted_order = Order::market(restricted_asset, OrderSide::Buy, 100.0, Utc::now());
        assert!(control.validate_order(&restricted_order, &context).is_err());

        let allowed_order = Order::market(allowed_asset, OrderSide::Buy, 100.0, Utc::now());
        assert!(control.validate_order(&allowed_order, &context).is_ok());
    }

    #[test]
    fn test_control_manager() {
        let mut manager = ControlManager::new();
        manager.add_order_control(Box::new(LongOnly));
        manager.add_order_control(Box::new(MaxOrderSize::shares(100.0)));
        manager.add_account_control(Box::new(MaxLeverage::new(2.0)));

        assert_eq!(manager.control_count(), (2, 1));

        let context = Context::new(100000.0);
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        let order = Order::market(asset, OrderSide::Buy, 50.0, Utc::now());

        assert!(manager.validate_order(&order, &context).is_ok());
        assert!(manager.validate_account(&context).is_ok());
    }
}
