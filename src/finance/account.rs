//! Account object - tracks account-level financial state
//!
//! The Account object maintains account-level metrics separate from Portfolio.
//! While Portfolio tracks positions, Account tracks overall financial health,
//! margin requirements, buying power, and regulatory metrics.

use crate::finance::Portfolio;
use chrono::{NaiveDate, DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Account tracks account-level financial state
///
/// Unlike Portfolio which tracks positions, Account tracks:
/// - Cash and buying power
/// - Margin requirements
/// - Leverage metrics
/// - Regulatory T equity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    /// Cash that has settled (available for withdrawal)
    pub settled_cash: f64,

    /// Interest accrued on cash balances
    pub accrued_interest: f64,

    /// Account buying power (cash + margin)
    pub buying_power: f64,

    /// Equity with loan value (for margin accounts)
    pub equity_with_loan: f64,

    /// Total market value of all positions
    pub total_positions_value: f64,

    /// Total positions exposure (sum of absolute position values)
    pub total_positions_exposure: f64,

    /// Regulatory T equity
    pub regt_equity: f64,

    /// Regulatory T margin used
    pub regt_margin: f64,

    /// Initial margin requirement
    pub initial_margin_requirement: f64,

    /// Maintenance margin requirement
    pub maintenance_margin_requirement: f64,

    /// Available funds for trading
    pub available_funds: f64,

    /// Excess liquidity (funds above maintenance margin)
    pub excess_liquidity: f64,

    /// Cushion ratio (excess liquidity / net liquidation)
    pub cushion: f64,

    /// Number of day trades remaining in rolling window
    pub day_trades_remaining: i32,

    /// Total leverage (total exposure / net liquidation)
    pub leverage: f64,

    /// Net liquidation value (total account value)
    pub net_liquidation: f64,

    /// Net leverage ((long - short) / net liquidation)
    pub net_leverage: f64,
}

impl Account {
    /// Create a new account with initial capital
    pub fn new(initial_capital: f64) -> Self {
        Account {
            settled_cash: initial_capital,
            accrued_interest: 0.0,
            buying_power: initial_capital,
            equity_with_loan: initial_capital,
            total_positions_value: 0.0,
            total_positions_exposure: 0.0,
            regt_equity: initial_capital,
            regt_margin: 0.0,
            initial_margin_requirement: 0.0,
            maintenance_margin_requirement: 0.0,
            available_funds: initial_capital,
            excess_liquidity: initial_capital,
            cushion: 1.0,
            day_trades_remaining: 3,
            leverage: 0.0,
            net_liquidation: initial_capital,
            net_leverage: 0.0,
        }
    }

    /// Update account metrics from portfolio state
    ///
    /// This should be called after each bar or order execution to keep
    /// account metrics synchronized with portfolio state.
    pub fn update(&mut self, portfolio: &Portfolio, _current_dt: DateTime<Utc>) {
        // Calculate total positions value and exposure
        self.total_positions_value = portfolio.positions_value;
        self.total_positions_exposure = portfolio
            .positions
            .values()
            .map(|p| p.market_value().abs())
            .sum();

        // Update net liquidation (total account value)
        self.net_liquidation = portfolio.portfolio_value;

        // Update settled cash from portfolio
        self.settled_cash = portfolio.cash;

        // Calculate leverage metrics
        if self.net_liquidation > 0.0 {
            self.leverage = self.total_positions_exposure / self.net_liquidation;

            // Calculate net exposure (long - short)
            let net_exposure = self.calculate_net_exposure(portfolio);
            self.net_leverage = net_exposure / self.net_liquidation;
        } else {
            self.leverage = 0.0;
            self.net_leverage = 0.0;
        }

        // Update margin requirements based on positions
        self.update_margin_requirements();

        // Calculate buying power
        self.buying_power = self.calculate_buying_power();

        // Calculate excess liquidity
        self.excess_liquidity = self.calculate_excess_liquidity();

        // Calculate cushion ratio
        self.cushion = self.calculate_cushion();

        // Update regulatory T equity
        self.regt_equity = self.net_liquidation;

        // Update equity with loan
        self.equity_with_loan = self.net_liquidation;

        // Available funds
        self.available_funds = self.settled_cash.max(0.0);
    }

    /// Calculate net exposure (long value - short value)
    fn calculate_net_exposure(&self, portfolio: &Portfolio) -> f64 {
        portfolio
            .positions
            .values()
            .map(|p| p.market_value())
            .sum()
    }

    /// Update margin requirements based on current positions
    ///
    /// Uses simplified margin requirements:
    /// - Initial margin: 50% of position value (Reg T requirement)
    /// - Maintenance margin: 25% of position value (typical requirement)
    fn update_margin_requirements(&mut self) {
        // Simplified margin calculations
        // In production, these would be asset-specific and more complex
        self.initial_margin_requirement = self.total_positions_exposure * 0.50;
        self.maintenance_margin_requirement = self.total_positions_exposure * 0.25;
        self.regt_margin = self.initial_margin_requirement;
    }

    /// Calculate buying power
    ///
    /// Buying power = available cash + available margin
    /// For a 2:1 margin account, this would be cash * 2
    fn calculate_buying_power(&self) -> f64 {
        // Simple calculation: available cash + margin capacity
        let margin_capacity = (self.equity_with_loan - self.initial_margin_requirement).max(0.0);
        self.settled_cash + margin_capacity
    }

    /// Calculate excess liquidity
    ///
    /// Excess liquidity is funds available above maintenance margin
    fn calculate_excess_liquidity(&self) -> f64 {
        (self.net_liquidation - self.maintenance_margin_requirement).max(0.0)
    }

    /// Calculate cushion ratio
    ///
    /// Cushion = excess liquidity / net liquidation
    /// Higher cushion = more buffer before margin call
    fn calculate_cushion(&self) -> f64 {
        if self.net_liquidation > 0.0 {
            self.excess_liquidity / self.net_liquidation
        } else {
            0.0
        }
    }

    /// Check if account has sufficient buying power for a trade
    pub fn has_buying_power(&self, required_cash: f64) -> bool {
        self.buying_power >= required_cash
    }

    /// Check if account would be margin called after a trade
    pub fn would_trigger_margin_call(&self, additional_margin: f64) -> bool {
        let new_margin_used = self.maintenance_margin_requirement + additional_margin;
        new_margin_used > self.net_liquidation
    }

    /// Get account summary as string
    pub fn summary(&self) -> String {
        format!(
            "Account Summary:\n\
             Net Liquidation: ${:.2}\n\
             Settled Cash: ${:.2}\n\
             Buying Power: ${:.2}\n\
             Total Positions: ${:.2}\n\
             Leverage: {:.2}x\n\
             Net Leverage: {:.2}x\n\
             Cushion: {:.2}%\n\
             Excess Liquidity: ${:.2}",
            self.net_liquidation,
            self.settled_cash,
            self.buying_power,
            self.total_positions_value,
            self.leverage,
            self.net_leverage,
            self.cushion * 100.0,
            self.excess_liquidity
        )
    }
}

impl Default for Account {
    fn default() -> Self {
        Self::new(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::Asset;
    use crate::finance::Position;

    #[test]
    fn test_account_creation() {
        let account = Account::new(100000.0);

        assert_eq!(account.settled_cash, 100000.0);
        assert_eq!(account.buying_power, 100000.0);
        assert_eq!(account.net_liquidation, 100000.0);
        assert_eq!(account.leverage, 0.0);
        assert_eq!(account.cushion, 1.0);
    }

    #[test]
    fn test_account_default() {
        let account = Account::default();
        assert_eq!(account.settled_cash, 0.0);
    }

    #[test]
    fn test_account_update_with_portfolio() {
        let mut account = Account::new(100000.0);
        let mut portfolio = Portfolio::new(100000.0);

        // Add a position
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
        let position = Position::new(asset, 100.0, 15000.0, 150.0);
        portfolio.positions.insert(1, position);
        portfolio.cash = 85000.0;
        portfolio.positions_value = 15000.0;
        portfolio.portfolio_value = 100000.0;

        account.update(&portfolio, Utc::now());

        assert_eq!(account.total_positions_value, 15000.0);
        assert_eq!(account.total_positions_exposure, 15000.0);
        assert_eq!(account.net_liquidation, 100000.0);
        assert_eq!(account.settled_cash, 85000.0);
    }

    #[test]
    fn test_leverage_calculation() {
        let mut account = Account::new(100000.0);
        let mut portfolio = Portfolio::new(100000.0);

        // Add a position worth 50% of portfolio
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
        let position = Position::new(asset, 500.0, 50000.0, 100.0);
        portfolio.positions.insert(1, position);
        portfolio.cash = 50000.0;
        portfolio.positions_value = 50000.0;
        portfolio.portfolio_value = 100000.0;

        account.update(&portfolio, Utc::now());

        // Leverage should be 0.5 (50k positions / 100k total)
        assert!((account.leverage - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_net_leverage_calculation() {
        let mut account = Account::new(100000.0);
        let mut portfolio = Portfolio::new(100000.0);

        // Add long position
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset1 = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
        let position1 = Position::new(asset1, 500.0, 50000.0, 100.0);
        portfolio.positions.insert(1, position1);

        portfolio.cash = 50000.0;
        portfolio.positions_value = 50000.0;
        portfolio.portfolio_value = 100000.0;

        account.update(&portfolio, Utc::now());

        // Net leverage should equal leverage for all long positions
        assert!((account.net_leverage - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_margin_requirements() {
        let mut account = Account::new(100000.0);
        let mut portfolio = Portfolio::new(100000.0);

        // Add position
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
        let position = Position::new(asset, 1000.0, 100000.0, 100.0);
        portfolio.positions.insert(1, position);
        portfolio.cash = 0.0;
        portfolio.positions_value = 100000.0;
        portfolio.portfolio_value = 100000.0;

        account.update(&portfolio, Utc::now());

        // Initial margin should be 50% of position value
        assert_eq!(account.initial_margin_requirement, 50000.0);
        // Maintenance margin should be 25% of position value
        assert_eq!(account.maintenance_margin_requirement, 25000.0);
    }

    #[test]
    fn test_buying_power_calculation() {
        let mut account = Account::new(100000.0);
        let portfolio = Portfolio::new(100000.0);

        account.update(&portfolio, Utc::now());

        // With no positions, buying power should equal cash
        assert_eq!(account.buying_power, 100000.0);
    }

    #[test]
    fn test_excess_liquidity() {
        let mut account = Account::new(100000.0);
        let mut portfolio = Portfolio::new(100000.0);

        // Add position with 40k value
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
        let position = Position::new(asset, 400.0, 40000.0, 100.0);
        portfolio.positions.insert(1, position);
        portfolio.cash = 60000.0;
        portfolio.positions_value = 40000.0;
        portfolio.portfolio_value = 100000.0;

        account.update(&portfolio, Utc::now());

        // Excess liquidity = net_liq - maintenance_margin
        // = 100000 - (40000 * 0.25) = 100000 - 10000 = 90000
        assert_eq!(account.excess_liquidity, 90000.0);
    }

    #[test]
    fn test_cushion_calculation() {
        let mut account = Account::new(100000.0);
        let mut portfolio = Portfolio::new(100000.0);

        // Add position
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
        let position = Position::new(asset, 400.0, 40000.0, 100.0);
        portfolio.positions.insert(1, position);
        portfolio.cash = 60000.0;
        portfolio.positions_value = 40000.0;
        portfolio.portfolio_value = 100000.0;

        account.update(&portfolio, Utc::now());

        // Cushion = excess_liquidity / net_liquidation
        // = 90000 / 100000 = 0.9
        assert!((account.cushion - 0.9).abs() < 0.01);
    }

    #[test]
    fn test_has_buying_power() {
        let account = Account::new(100000.0);

        assert!(account.has_buying_power(50000.0));
        assert!(account.has_buying_power(100000.0));
        assert!(!account.has_buying_power(150000.0));
    }

    #[test]
    fn test_would_trigger_margin_call() {
        let mut account = Account::new(100000.0);
        let mut portfolio = Portfolio::new(100000.0);

        // Setup account with some positions
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
        let position = Position::new(asset, 400.0, 40000.0, 100.0);
        portfolio.positions.insert(1, position);
        portfolio.cash = 60000.0;
        portfolio.positions_value = 40000.0;
        portfolio.portfolio_value = 100000.0;

        account.update(&portfolio, Utc::now());

        // Small additional margin should be fine
        assert!(!account.would_trigger_margin_call(1000.0));

        // Large additional margin should trigger call
        assert!(account.would_trigger_margin_call(200000.0));
    }
}
