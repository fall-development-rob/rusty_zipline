//! Commission models for calculating trading costs

use crate::order::Order;
use crate::types::Cash;

/// Commission model trait
pub trait CommissionModel: Send + Sync {
    /// Calculate commission for an order
    fn calculate(&self, order: &Order, fill_price: f64, fill_quantity: f64) -> Cash;

    /// Get model name
    fn name(&self) -> &str;
}

/// Per-share commission model
#[derive(Debug, Clone)]
pub struct PerShare {
    /// Cost per share
    pub cost_per_share: f64,
    /// Minimum commission
    pub min_commission: f64,
}

impl PerShare {
    /// Create new per-share commission model
    pub fn new(cost_per_share: f64) -> Self {
        Self {
            cost_per_share,
            min_commission: 0.0,
        }
    }

    /// Create with minimum commission
    pub fn with_min(cost_per_share: f64, min_commission: f64) -> Self {
        Self {
            cost_per_share,
            min_commission,
        }
    }
}

impl CommissionModel for PerShare {
    fn calculate(&self, _order: &Order, _fill_price: f64, fill_quantity: f64) -> Cash {
        let commission = self.cost_per_share * fill_quantity.abs();
        commission.max(self.min_commission)
    }

    fn name(&self) -> &str {
        "PerShare"
    }
}

/// Per-trade commission model
#[derive(Debug, Clone)]
pub struct PerTrade {
    /// Cost per trade
    pub cost: f64,
}

impl PerTrade {
    /// Create new per-trade commission model
    pub fn new(cost: f64) -> Self {
        Self { cost }
    }
}

impl CommissionModel for PerTrade {
    fn calculate(&self, _order: &Order, _fill_price: f64, _fill_quantity: f64) -> Cash {
        self.cost
    }

    fn name(&self) -> &str {
        "PerTrade"
    }
}

/// Per-dollar commission model
#[derive(Debug, Clone)]
pub struct PerDollar {
    /// Cost per dollar traded (as percentage)
    pub cost_per_dollar: f64,
    /// Minimum commission
    pub min_commission: f64,
}

impl PerDollar {
    /// Create new per-dollar commission model
    pub fn new(cost_per_dollar: f64) -> Self {
        Self {
            cost_per_dollar,
            min_commission: 0.0,
        }
    }

    /// Create with minimum commission
    pub fn with_min(cost_per_dollar: f64, min_commission: f64) -> Self {
        Self {
            cost_per_dollar,
            min_commission,
        }
    }
}

impl CommissionModel for PerDollar {
    fn calculate(&self, _order: &Order, fill_price: f64, fill_quantity: f64) -> Cash {
        let dollar_value = (fill_price * fill_quantity).abs();
        let commission = dollar_value * self.cost_per_dollar;
        commission.max(self.min_commission)
    }

    fn name(&self) -> &str {
        "PerDollar"
    }
}

/// Zero commission model (for testing or commission-free brokers)
#[derive(Debug, Clone)]
pub struct ZeroCommission;

impl CommissionModel for ZeroCommission {
    fn calculate(&self, _order: &Order, _fill_price: f64, _fill_quantity: f64) -> Cash {
        0.0
    }

    fn name(&self) -> &str {
        "ZeroCommission"
    }
}

/// Tiered commission model based on trade volume
#[derive(Debug, Clone)]
pub struct TieredCommission {
    /// Tiers: (threshold, cost_per_share)
    tiers: Vec<(f64, f64)>,
    /// Minimum commission
    min_commission: f64,
}

impl TieredCommission {
    /// Create new tiered commission model
    pub fn new(tiers: Vec<(f64, f64)>, min_commission: f64) -> Self {
        let mut sorted_tiers = tiers;
        sorted_tiers.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        Self {
            tiers: sorted_tiers,
            min_commission,
        }
    }
}

impl CommissionModel for TieredCommission {
    fn calculate(&self, _order: &Order, _fill_price: f64, fill_quantity: f64) -> Cash {
        let quantity = fill_quantity.abs();

        // Find applicable tier
        let cost_per_share = self
            .tiers
            .iter()
            .rev()
            .find(|(threshold, _)| quantity >= *threshold)
            .map(|(_, cost)| *cost)
            .unwrap_or(0.01); // Default to $0.01 per share

        let commission = cost_per_share * quantity;
        commission.max(self.min_commission)
    }

    fn name(&self) -> &str {
        "TieredCommission"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::{Asset, AssetType};
    use chrono::Utc;
use chrono::NaiveDate;

    fn create_test_order(quantity: f64) -> Order {
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "TEST".to_string(), "TEST".to_string(), start_date);
        Order::market(asset, OrderSide::Buy, quantity, Utc::now())
    }

    #[test]
    fn test_per_share_commission() {
        let model = PerShare::new(0.01);
        let order = create_test_order(100.0);

        let commission = model.calculate(&order, 50.0, 100.0);
        assert_eq!(commission, 1.0); // 100 shares * $0.01 = $1
    }

    #[test]
    fn test_per_share_with_min() {
        let model = PerShare::with_min(0.01, 5.0);
        let order = create_test_order(10.0);

        let commission = model.calculate(&order, 50.0, 10.0);
        assert_eq!(commission, 5.0); // Min commission of $5
    }

    #[test]
    fn test_per_trade_commission() {
        let model = PerTrade::new(9.99);
        let order = create_test_order(100.0);

        let commission = model.calculate(&order, 50.0, 100.0);
        assert_eq!(commission, 9.99);
    }

    #[test]
    fn test_per_dollar_commission() {
        let model = PerDollar::new(0.001); // 0.1%
        let order = create_test_order(100.0);

        let commission = model.calculate(&order, 50.0, 100.0);
        assert_eq!(commission, 5.0); // $5000 * 0.001 = $5
    }

    #[test]
    fn test_zero_commission() {
        let model = ZeroCommission;
        let order = create_test_order(100.0);

        let commission = model.calculate(&order, 50.0, 100.0);
        assert_eq!(commission, 0.0);
    }

    #[test]
    fn test_tiered_commission() {
        let tiers = vec![
            (0.0, 0.01),     // 0-999 shares: $0.01/share
            (1000.0, 0.005), // 1000+ shares: $0.005/share
            (5000.0, 0.002), // 5000+ shares: $0.002/share
        ];
        let model = TieredCommission::new(tiers, 1.0);

        let order1 = create_test_order(500.0);
        assert_eq!(model.calculate(&order1, 50.0, 500.0), 5.0); // 500 * 0.01

        let order2 = create_test_order(2000.0);
        assert_eq!(model.calculate(&order2, 50.0, 2000.0), 10.0); // 2000 * 0.005

        let order3 = create_test_order(10000.0);
        assert_eq!(model.calculate(&order3, 50.0, 10000.0), 20.0); // 10000 * 0.002
    }
}
