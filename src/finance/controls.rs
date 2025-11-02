//! Trading controls and restrictions

use crate::algorithm::Context;
use crate::error::{Result, ZiplineError};
use crate::order::Order;
use chrono::Duration;
use chrono::NaiveDate;
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
                return Err(ZiplineError::MaxOrderSizeExceeded {
                    asset: order.asset.id,
                    order_size: order.quantity,
                    max_size: max,
                });
            }
        }

        if let Some(max) = self.max_notional {
            // Estimate notional value (would need current price in real implementation)
            let estimated_notional = order.quantity * context.portfolio.portfolio_value / 100.0;
            if estimated_notional > max {
                return Err(ZiplineError::MaxOrderSizeExceeded {
                    asset: order.asset.id,
                    order_size: estimated_notional,
                    max_size: max,
                });
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
    fn validate_order(&self, _order: &Order, context: &Context) -> Result<()> {
        let cutoff = context.timestamp - self.period;
        let recent_orders: usize = self
            .order_times
            .iter()
            .filter(|&&t| t > cutoff)
            .count();

        if recent_orders >= self.max_count {
            return Err(ZiplineError::MaxOrderCountExceeded {
                current_count: recent_orders,
                max_count: self.max_count,
                date: context.timestamp,
            });
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
                return Err(ZiplineError::MaxPositionSizeExceeded {
                    asset: order.asset.id,
                    symbol: order.asset.symbol.clone(),
                    attempted_order: order.quantity,
                    max_shares: self.max_shares,
                    max_notional: None,
                });
            }
        }

        if let Some(max_pct) = self.max_pct_portfolio {
            // Estimate position value as percentage of portfolio
            let estimated_value = new_position.abs() * 100.0; // Placeholder
            let pct = estimated_value / context.portfolio.portfolio_value;
            let max_notional = max_pct * context.portfolio.portfolio_value;
            if pct > max_pct {
                return Err(ZiplineError::MaxPositionSizeExceeded {
                    asset: order.asset.id,
                    symbol: order.asset.symbol.clone(),
                    attempted_order: order.quantity,
                    max_shares: None,
                    max_notional: Some(max_notional),
                });
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
            return Err(ZiplineError::MaxLeverageExceeded {
                current_leverage: leverage,
                max_leverage: self.max_leverage,
            });
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

/// Limit exposure to specific sectors
pub struct SectorExposure {
    /// Maximum percentage of portfolio per sector
    max_sector_exposure: f64,
    /// Map of asset ID to sector
    asset_sectors: std::collections::HashMap<u64, String>,
}

impl SectorExposure {
    pub fn new(max_sector_exposure: f64) -> Self {
        Self {
            max_sector_exposure,
            asset_sectors: std::collections::HashMap::new(),
        }
    }

    /// Register an asset's sector
    pub fn register_asset(&mut self, asset_id: u64, sector: String) {
        self.asset_sectors.insert(asset_id, sector);
    }

    /// Get sector for an asset
    pub fn get_sector(&self, asset_id: u64) -> Option<&str> {
        self.asset_sectors.get(&asset_id).map(|s| s.as_str())
    }

    /// Calculate current sector exposures
    pub fn calculate_exposures(&self, context: &Context) -> std::collections::HashMap<String, f64> {
        let mut sector_values: std::collections::HashMap<String, f64> = std::collections::HashMap::new();

        for position in context.portfolio.positions.values() {
            if let Some(sector) = self.asset_sectors.get(&position.asset.id) {
                let value = position.quantity * position.cost_basis; // Approximation
                *sector_values.entry(sector.clone()).or_insert(0.0) += value;
            }
        }

        // Convert to percentages
        let portfolio_value = context.portfolio.portfolio_value;
        sector_values
            .into_iter()
            .map(|(sector, value)| (sector, value / portfolio_value))
            .collect()
    }
}

impl TradingControl for SectorExposure {
    fn validate_order(&self, order: &Order, context: &Context) -> Result<()> {
        if let Some(sector) = self.get_sector(order.asset.id) {
            let exposures = self.calculate_exposures(context);
            let current_exposure = exposures.get(sector).copied().unwrap_or(0.0);

            // Estimate new exposure (simplified)
            let order_value = order.quantity * 100.0; // Placeholder price
            let new_exposure = current_exposure + (order_value / context.portfolio.portfolio_value);

            if new_exposure > self.max_sector_exposure {
                return Err(ZiplineError::InvalidOrder(format!(
                    "Order would increase {} sector exposure to {:.1}%, exceeding maximum of {:.1}%",
                    sector,
                    new_exposure * 100.0,
                    self.max_sector_exposure * 100.0
                )));
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "SectorExposure"
    }
}

/// Limit trading based on asset volatility
pub struct VolatilityLimit {
    /// Maximum allowed volatility (annualized)
    max_volatility: f64,
    /// Volatility values for assets
    asset_volatilities: std::collections::HashMap<u64, f64>,
}

impl VolatilityLimit {
    pub fn new(max_volatility: f64) -> Self {
        Self {
            max_volatility,
            asset_volatilities: std::collections::HashMap::new(),
        }
    }

    /// Update volatility for an asset
    pub fn update_volatility(&mut self, asset_id: u64, volatility: f64) {
        self.asset_volatilities.insert(asset_id, volatility);
    }

    /// Get volatility for an asset
    pub fn get_volatility(&self, asset_id: u64) -> Option<f64> {
        self.asset_volatilities.get(&asset_id).copied()
    }
}

impl TradingControl for VolatilityLimit {
    fn validate_order(&self, order: &Order, _context: &Context) -> Result<()> {
        if let Some(volatility) = self.get_volatility(order.asset.id) {
            if volatility > self.max_volatility {
                return Err(ZiplineError::InvalidOrder(format!(
                    "Asset {} has volatility {:.2}%, exceeding maximum of {:.2}%",
                    order.asset.symbol,
                    volatility * 100.0,
                    self.max_volatility * 100.0
                )));
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "VolatilityLimit"
    }
}

/// Position concentration limit
pub struct PositionConcentration {
    /// Maximum percentage of portfolio in single position
    max_concentration: f64,
}

impl PositionConcentration {
    pub fn new(max_concentration: f64) -> Self {
        Self { max_concentration }
    }
}

impl TradingControl for PositionConcentration {
    fn validate_order(&self, order: &Order, context: &Context) -> Result<()> {
        let current_position = context
            .portfolio
            .get_position(order.asset.id)
            .map(|p| p.quantity * p.cost_basis)
            .unwrap_or(0.0);

        // Estimate new position value
        let order_value = order.quantity * 100.0; // Placeholder
        let new_position_value = match order.side {
            crate::order::OrderSide::Buy => current_position + order_value,
            crate::order::OrderSide::Sell => current_position - order_value,
        };

        let concentration = new_position_value / context.portfolio.portfolio_value;

        if concentration > self.max_concentration {
            return Err(ZiplineError::InvalidOrder(format!(
                "Position would be {:.1}% of portfolio, exceeding concentration limit of {:.1}%",
                concentration * 100.0,
                self.max_concentration * 100.0
            )));
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "PositionConcentration"
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
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);

        let valid_order = Order::market(asset.clone(), OrderSide::Buy, 50.0, Utc::now());
        assert!(control.validate_order(&valid_order, &context).is_ok());

        let invalid_order = Order::market(asset, OrderSide::Buy, 200.0, Utc::now());
        assert!(control.validate_order(&invalid_order, &context).is_err());
    }

    #[test]
    fn test_long_only() {
        let control = LongOnly;
        let mut context = Context::new(100000.0);
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);

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
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let restricted_asset = Asset::equity(1, "BANNED".to_string(), "NYSE".to_string(), start_date);
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let allowed_asset = Asset::equity(2, "AAPL".to_string(), "NASDAQ".to_string(), start_date);

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
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
        let order = Order::market(asset, OrderSide::Buy, 50.0, Utc::now());

        assert!(manager.validate_order(&order, &context).is_ok());
        assert!(manager.validate_account(&context).is_ok());
    }

    #[test]
    fn test_sector_exposure() {
        let mut control = SectorExposure::new(0.30); // 30% max per sector
        control.register_asset(1, "Technology".to_string());
        control.register_asset(2, "Healthcare".to_string());

        let context = Context::new(100000.0);
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let tech_asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);

        let order = Order::market(tech_asset, OrderSide::Buy, 100.0, Utc::now());
        // Should pass for reasonable order
        assert!(control.validate_order(&order, &context).is_ok());

        assert_eq!(control.get_sector(1), Some("Technology"));
        assert_eq!(control.get_sector(2), Some("Healthcare"));
    }

    #[test]
    fn test_volatility_limit() {
        let mut control = VolatilityLimit::new(0.50); // 50% max volatility
        control.update_volatility(1, 0.30); // Low volatility
        control.update_volatility(2, 0.60); // High volatility

        let context = Context::new(100000.0);

        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let low_vol_asset = Asset::equity(1, "STABLE".to_string(), "NYSE".to_string(), start_date);
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let high_vol_asset = Asset::equity(2, "VOLATILE".to_string(), "NYSE".to_string(), start_date);

        let low_vol_order = Order::market(low_vol_asset, OrderSide::Buy, 100.0, Utc::now());
        assert!(control.validate_order(&low_vol_order, &context).is_ok());

        let high_vol_order = Order::market(high_vol_asset, OrderSide::Buy, 100.0, Utc::now());
        assert!(control.validate_order(&high_vol_order, &context).is_err());

        assert_eq!(control.get_volatility(1), Some(0.30));
        assert_eq!(control.get_volatility(2), Some(0.60));
    }

    #[test]
    fn test_position_concentration() {
        let control = PositionConcentration::new(0.25); // 25% max concentration
        let context = Context::new(100000.0);
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);

        let order = Order::market(asset, OrderSide::Buy, 100.0, Utc::now());
        // Should validate concentration limits
        assert!(control.validate_order(&order, &context).is_ok());
    }
}
