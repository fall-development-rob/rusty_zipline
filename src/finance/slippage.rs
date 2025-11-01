//! Slippage models for realistic trade simulation

use crate::asset::Asset;
use crate::order::{Order, OrderSide};
use crate::types::Price;

/// Slippage model trait
pub trait SlippageModel: Send + Sync {
    /// Calculate execution price given order and market conditions
    fn calculate_price(
        &self,
        order: &Order,
        market_price: Price,
        volume: f64,
    ) -> Price;

    /// Get model name
    fn name(&self) -> &str;
}

/// Fixed slippage model - constant basis points
#[derive(Debug, Clone)]
pub struct FixedBasisPointsSlippage {
    /// Basis points (1 bp = 0.01%)
    basis_points: f64,
}

impl FixedBasisPointsSlippage {
    /// Create new fixed slippage model
    pub fn new(basis_points: f64) -> Self {
        Self { basis_points }
    }
}

impl SlippageModel for FixedBasisPointsSlippage {
    fn calculate_price(
        &self,
        order: &Order,
        market_price: Price,
        _volume: f64,
    ) -> Price {
        let slippage_factor = self.basis_points / 10000.0; // Convert basis points to decimal

        match order.side {
            OrderSide::Buy => market_price * (1.0 + slippage_factor), // Pay more
            OrderSide::Sell => market_price * (1.0 - slippage_factor), // Receive less
        }
    }

    fn name(&self) -> &str {
        "FixedBasisPointsSlippage"
    }
}

/// Volume share slippage - slippage based on order size relative to volume
#[derive(Debug, Clone)]
pub struct VolumeShareSlippage {
    /// Price impact coefficient
    price_impact: f64,
    /// Volume limit (fraction of daily volume)
    volume_limit: f64,
}

impl VolumeShareSlippage {
    /// Create new volume share slippage model
    pub fn new(price_impact: f64, volume_limit: f64) -> Self {
        Self {
            price_impact,
            volume_limit,
        }
    }

    /// Default model (0.1 price impact, 25% volume limit)
    pub fn default_model() -> Self {
        Self::new(0.1, 0.25)
    }
}

impl SlippageModel for VolumeShareSlippage {
    fn calculate_price(
        &self,
        order: &Order,
        market_price: Price,
        daily_volume: f64,
    ) -> Price {
        let order_size = order.quantity.abs();

        // Calculate volume share
        let volume_share = if daily_volume > 0.0 {
            (order_size / daily_volume).min(self.volume_limit)
        } else {
            self.volume_limit
        };

        // Calculate slippage based on volume share
        let slippage = self.price_impact * volume_share;

        match order.side {
            OrderSide::Buy => market_price * (1.0 + slippage),
            OrderSide::Sell => market_price * (1.0 - slippage),
        }
    }

    fn name(&self) -> &str {
        "VolumeShareSlippage"
    }
}

/// Zero slippage model (for testing or perfect execution scenarios)
#[derive(Debug, Clone)]
pub struct NoSlippage;

impl SlippageModel for NoSlippage {
    fn calculate_price(
        &self,
        _order: &Order,
        market_price: Price,
        _volume: f64,
    ) -> Price {
        market_price
    }

    fn name(&self) -> &str {
        "NoSlippage"
    }
}

/// Square root impact model - realistic for large orders
#[derive(Debug, Clone)]
pub struct SquareRootImpact {
    /// Impact coefficient
    coefficient: f64,
}

impl SquareRootImpact {
    /// Create new square root impact model
    pub fn new(coefficient: f64) -> Self {
        Self { coefficient }
    }
}

impl SlippageModel for SquareRootImpact {
    fn calculate_price(
        &self,
        order: &Order,
        market_price: Price,
        daily_volume: f64,
    ) -> Price {
        let order_size = order.quantity.abs();

        // Square root impact: slippage = coefficient * sqrt(order_size / daily_volume)
        let volume_share = if daily_volume > 0.0 {
            order_size / daily_volume
        } else {
            1.0
        };

        let slippage = self.coefficient * volume_share.sqrt();

        match order.side {
            OrderSide::Buy => market_price * (1.0 + slippage),
            OrderSide::Sell => market_price * (1.0 - slippage),
        }
    }

    fn name(&self) -> &str {
        "SquareRootImpact"
    }
}

/// Linear impact model - slippage proportional to order size
#[derive(Debug, Clone)]
pub struct LinearImpact {
    /// Impact coefficient per share
    coefficient: f64,
}

impl LinearImpact {
    /// Create new linear impact model
    pub fn new(coefficient: f64) -> Self {
        Self { coefficient }
    }
}

impl SlippageModel for LinearImpact {
    fn calculate_price(
        &self,
        order: &Order,
        market_price: Price,
        _volume: f64,
    ) -> Price {
        let order_size = order.quantity.abs();
        let slippage = self.coefficient * order_size / market_price;

        match order.side {
            OrderSide::Buy => market_price * (1.0 + slippage),
            OrderSide::Sell => market_price * (1.0 - slippage),
        }
    }

    fn name(&self) -> &str {
        "LinearImpact"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::Asset;
    use chrono::Utc;

    fn create_buy_order(quantity: f64) -> Order {
        let asset = Asset::equity(1, "TEST".to_string(), "TEST".to_string());
        Order::market(asset, OrderSide::Buy, quantity, Utc::now())
    }

    fn create_sell_order(quantity: f64) -> Order {
        let asset = Asset::equity(1, "TEST".to_string(), "TEST".to_string());
        Order::market(asset, OrderSide::Sell, quantity, Utc::now())
    }

    #[test]
    fn test_fixed_slippage() {
        let model = FixedBasisPointsSlippage::new(10.0); // 10 basis points = 0.1%
        let buy_order = create_buy_order(100.0);
        let sell_order = create_sell_order(100.0);

        let buy_price = model.calculate_price(&buy_order, 100.0, 1000.0);
        assert_eq!(buy_price, 100.1); // 100 * 1.001

        let sell_price = model.calculate_price(&sell_order, 100.0, 1000.0);
        assert_eq!(sell_price, 99.9); // 100 * 0.999
    }

    #[test]
    fn test_no_slippage() {
        let model = NoSlippage;
        let order = create_buy_order(100.0);

        let price = model.calculate_price(&order, 100.0, 1000.0);
        assert_eq!(price, 100.0);
    }

    #[test]
    fn test_volume_share_slippage() {
        let model = VolumeShareSlippage::new(0.1, 0.25);
        let order = create_buy_order(250.0); // 25% of daily volume

        let price = model.calculate_price(&order, 100.0, 1000.0);
        // 25% volume share, 0.1 impact = 0.025 slippage
        assert_eq!(price, 102.5); // 100 * 1.025
    }

    #[test]
    fn test_volume_share_slippage_limit() {
        let model = VolumeShareSlippage::new(0.1, 0.25);
        let large_order = create_buy_order(500.0); // 50% of daily volume, but capped at 25%

        let price = model.calculate_price(&large_order, 100.0, 1000.0);
        // Capped at 25% volume share
        assert_eq!(price, 102.5); // 100 * 1.025
    }

    #[test]
    fn test_square_root_impact() {
        let model = SquareRootImpact::new(0.1);
        let order = create_buy_order(100.0);

        let price = model.calculate_price(&order, 100.0, 10000.0);
        // sqrt(100/10000) = sqrt(0.01) = 0.1
        // slippage = 0.1 * 0.1 = 0.01 = 1%
        assert_eq!(price, 101.0);
    }

    #[test]
    fn test_linear_impact() {
        let model = LinearImpact::new(0.001);
        let order = create_buy_order(1000.0);

        let price = model.calculate_price(&order, 100.0, 10000.0);
        // slippage = 0.001 * 1000 / 100 = 0.01 = 1%
        assert_eq!(price, 101.0);
    }

    #[test]
    fn test_sell_slippage() {
        let model = FixedBasisPointsSlippage::new(50.0); // 50 bp = 0.5%
        let sell_order = create_sell_order(100.0);

        let price = model.calculate_price(&sell_order, 100.0, 1000.0);
        assert_eq!(price, 99.5); // Seller receives less
    }
}
