//! Ledger System - Transaction tracking and P&L calculation
//!
//! This module provides a comprehensive ledger system for tracking all
//! trading activity, calculating P&L, and maintaining cost basis.

use crate::error::{Result, ZiplineError};
use crate::finance::transaction::Transaction;
use crate::order::OrderSide;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

/// Cost basis calculation method
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CostBasisMethod {
    /// First In, First Out
    FIFO,
    /// Last In, First Out
    LIFO,
    /// Average Cost
    Average,
}

/// Lot - represents a purchase of shares with specific cost basis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lot {
    /// Quantity of shares in this lot
    pub quantity: f64,
    /// Cost basis per share
    pub cost_basis: f64,
    /// Acquisition date
    pub acquired_at: DateTime<Utc>,
    /// Transaction ID that created this lot
    pub transaction_id: uuid::Uuid,
}

impl Lot {
    pub fn new(quantity: f64, cost_basis: f64, acquired_at: DateTime<Utc>, transaction_id: uuid::Uuid) -> Self {
        Self {
            quantity,
            cost_basis,
            acquired_at,
            transaction_id,
        }
    }

    /// Total cost of this lot
    pub fn total_cost(&self) -> f64 {
        self.quantity * self.cost_basis
    }
}

/// Position with lot tracking for cost basis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerPosition {
    /// Asset ID
    pub asset_id: u64,
    /// Total quantity across all lots
    pub quantity: f64,
    /// Individual lots (for FIFO/LIFO)
    lots: VecDeque<Lot>,
    /// Average cost basis
    average_cost: f64,
    /// Cost basis method
    cost_basis_method: CostBasisMethod,
}

impl LedgerPosition {
    pub fn new(asset_id: u64, cost_basis_method: CostBasisMethod) -> Self {
        Self {
            asset_id,
            quantity: 0.0,
            lots: VecDeque::new(),
            average_cost: 0.0,
            cost_basis_method,
        }
    }

    /// Add a buy transaction (creates new lot)
    pub fn add_shares(&mut self, quantity: f64, price: f64, dt: DateTime<Utc>, txn_id: uuid::Uuid) {
        let lot = Lot::new(quantity, price, dt, txn_id);

        // Update average cost
        let total_cost = self.quantity * self.average_cost + lot.total_cost();
        self.quantity += quantity;
        self.average_cost = if self.quantity > 0.0 {
            total_cost / self.quantity
        } else {
            0.0
        };

        self.lots.push_back(lot);
    }

    /// Remove shares (sell transaction) and calculate realized P&L
    pub fn remove_shares(&mut self, quantity: f64, sale_price: f64) -> Result<f64> {
        if quantity > self.quantity {
            return Err(ZiplineError::InvalidOrder(format!(
                "Cannot sell {} shares, only have {}",
                quantity, self.quantity
            )));
        }

        let mut realized_pnl = 0.0;
        let mut remaining = quantity;

        match self.cost_basis_method {
            CostBasisMethod::FIFO => {
                // Remove from front (oldest first)
                while remaining > 0.0 && !self.lots.is_empty() {
                    let lot = self.lots.front_mut().unwrap();

                    if lot.quantity <= remaining {
                        // Use entire lot
                        realized_pnl += lot.quantity * (sale_price - lot.cost_basis);
                        remaining -= lot.quantity;
                        self.lots.pop_front();
                    } else {
                        // Partial lot
                        realized_pnl += remaining * (sale_price - lot.cost_basis);
                        lot.quantity -= remaining;
                        remaining = 0.0;
                    }
                }
            }
            CostBasisMethod::LIFO => {
                // Remove from back (newest first)
                while remaining > 0.0 && !self.lots.is_empty() {
                    let lot = self.lots.back_mut().unwrap();

                    if lot.quantity <= remaining {
                        // Use entire lot
                        realized_pnl += lot.quantity * (sale_price - lot.cost_basis);
                        remaining -= lot.quantity;
                        self.lots.pop_back();
                    } else {
                        // Partial lot
                        realized_pnl += remaining * (sale_price - lot.cost_basis);
                        lot.quantity -= remaining;
                        remaining = 0.0;
                    }
                }
            }
            CostBasisMethod::Average => {
                // Use average cost
                realized_pnl = quantity * (sale_price - self.average_cost);

                // Remove quantity proportionally from lots
                let removal_ratio = quantity / self.quantity;
                for lot in &mut self.lots {
                    lot.quantity *= 1.0 - removal_ratio;
                }
                // Clean up zero-quantity lots
                self.lots.retain(|lot| lot.quantity > f64::EPSILON);
            }
        }

        self.quantity -= quantity;
        Ok(realized_pnl)
    }

    /// Calculate unrealized P&L at current price
    pub fn unrealized_pnl(&self, current_price: f64) -> f64 {
        self.quantity * (current_price - self.average_cost)
    }

    /// Get current cost basis
    pub fn cost_basis(&self) -> f64 {
        self.average_cost
    }

    /// Get market value at current price
    pub fn market_value(&self, current_price: f64) -> f64 {
        self.quantity * current_price
    }
}

/// P&L Summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnLSummary {
    /// Total realized P&L
    pub realized_pnl: f64,
    /// Total unrealized P&L
    pub unrealized_pnl: f64,
    /// Total P&L (realized + unrealized)
    pub total_pnl: f64,
    /// Number of winning trades
    pub winning_trades: usize,
    /// Number of losing trades
    pub losing_trades: usize,
    /// Total number of trades
    pub total_trades: usize,
    /// Win rate
    pub win_rate: f64,
}

impl PnLSummary {
    pub fn new() -> Self {
        Self {
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            total_pnl: 0.0,
            winning_trades: 0,
            losing_trades: 0,
            total_trades: 0,
            win_rate: 0.0,
        }
    }

    pub fn add_trade(&mut self, pnl: f64) {
        self.realized_pnl += pnl;
        self.total_trades += 1;

        if pnl > 0.0 {
            self.winning_trades += 1;
        } else if pnl < 0.0 {
            self.losing_trades += 1;
        }

        self.win_rate = if self.total_trades > 0 {
            self.winning_trades as f64 / self.total_trades as f64
        } else {
            0.0
        };

        self.total_pnl = self.realized_pnl + self.unrealized_pnl;
    }

    pub fn update_unrealized(&mut self, unrealized_pnl: f64) {
        self.unrealized_pnl = unrealized_pnl;
        self.total_pnl = self.realized_pnl + self.unrealized_pnl;
    }
}

impl Default for PnLSummary {
    fn default() -> Self {
        Self::new()
    }
}

/// Ledger - Complete transaction and P&L tracking system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ledger {
    /// All transactions
    transactions: Vec<Transaction>,
    /// Positions with lot tracking
    positions: HashMap<u64, LedgerPosition>,
    /// Cost basis method
    cost_basis_method: CostBasisMethod,
    /// P&L summary
    pnl_summary: PnLSummary,
    /// Transaction index by asset
    transactions_by_asset: HashMap<u64, Vec<usize>>,
}

impl Ledger {
    /// Create new ledger with specified cost basis method
    pub fn new(cost_basis_method: CostBasisMethod) -> Self {
        Self {
            transactions: Vec::new(),
            positions: HashMap::new(),
            cost_basis_method,
            pnl_summary: PnLSummary::new(),
            transactions_by_asset: HashMap::new(),
        }
    }

    /// Record a transaction
    pub fn record_transaction(&mut self, transaction: Transaction) -> Result<()> {
        let asset_id = transaction.asset_id;
        let txn_index = self.transactions.len();

        // Get or create position
        let position = self
            .positions
            .entry(asset_id)
            .or_insert_with(|| LedgerPosition::new(asset_id, self.cost_basis_method));

        // Process transaction
        match transaction.side {
            OrderSide::Buy => {
                position.add_shares(
                    transaction.amount,
                    transaction.price,
                    transaction.dt,
                    transaction.id,
                );
            }
            OrderSide::Sell => {
                let realized_pnl = position.remove_shares(
                    transaction.amount.abs(),
                    transaction.price,
                )?;
                self.pnl_summary.add_trade(realized_pnl);
            }
        }

        // Record transaction
        self.transactions.push(transaction);
        self.transactions_by_asset
            .entry(asset_id)
            .or_insert_with(Vec::new)
            .push(txn_index);

        Ok(())
    }

    /// Get position for an asset
    pub fn get_position(&self, asset_id: u64) -> Option<&LedgerPosition> {
        self.positions.get(&asset_id)
    }

    /// Get all positions
    pub fn get_all_positions(&self) -> &HashMap<u64, LedgerPosition> {
        &self.positions
    }

    /// Calculate unrealized P&L for a position
    pub fn unrealized_pnl(&self, asset_id: u64, current_price: f64) -> f64 {
        self.positions
            .get(&asset_id)
            .map(|pos| pos.unrealized_pnl(current_price))
            .unwrap_or(0.0)
    }

    /// Calculate total unrealized P&L across all positions
    pub fn total_unrealized_pnl(&self, prices: &HashMap<u64, f64>) -> f64 {
        self.positions
            .iter()
            .map(|(asset_id, pos)| {
                let price = prices.get(asset_id).copied().unwrap_or(0.0);
                pos.unrealized_pnl(price)
            })
            .sum()
    }

    /// Get P&L summary
    pub fn get_pnl_summary(&self) -> &PnLSummary {
        &self.pnl_summary
    }

    /// Get transactions for an asset
    pub fn get_transactions_for_asset(&self, asset_id: u64) -> Vec<&Transaction> {
        self.transactions_by_asset
            .get(&asset_id)
            .map(|indices| {
                indices
                    .iter()
                    .filter_map(|&idx| self.transactions.get(idx))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get all transactions
    pub fn get_all_transactions(&self) -> &[Transaction] {
        &self.transactions
    }

    /// Get transactions in date range
    pub fn get_transactions_in_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Vec<&Transaction> {
        self.transactions
            .iter()
            .filter(|txn| txn.dt >= start && txn.dt <= end)
            .collect()
    }

    /// Calculate average entry price for a position
    pub fn average_entry_price(&self, asset_id: u64) -> Option<f64> {
        self.positions.get(&asset_id).map(|pos| pos.cost_basis())
    }

    /// Get number of transactions
    pub fn transaction_count(&self) -> usize {
        self.transactions.len()
    }

    /// Get number of open positions
    pub fn open_position_count(&self) -> usize {
        self.positions.iter().filter(|(_, pos)| pos.quantity > f64::EPSILON).count()
    }

    /// Update P&L summary with current prices
    pub fn update_pnl(&mut self, prices: &HashMap<u64, f64>) {
        let unrealized = self.total_unrealized_pnl(prices);
        self.pnl_summary.update_unrealized(unrealized);
    }
}

impl Default for Ledger {
    fn default() -> Self {
        Self::new(CostBasisMethod::FIFO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_transaction(
        asset_id: u64,
        amount: f64,
        price: f64,
        side: OrderSide,
    ) -> Transaction {
        Transaction::new(
            asset_id,
            uuid::Uuid::new_v4(),
            Utc::now(),
            amount,
            price,
            0.0,
            side,
        )
    }

    #[test]
    fn test_lot_creation() {
        let lot = Lot::new(100.0, 50.0, Utc::now(), uuid::Uuid::new_v4());
        assert_eq!(lot.quantity, 100.0);
        assert_eq!(lot.cost_basis, 50.0);
        assert_eq!(lot.total_cost(), 5000.0);
    }

    #[test]
    fn test_position_add_shares() {
        let mut position = LedgerPosition::new(1, CostBasisMethod::FIFO);

        position.add_shares(100.0, 50.0, Utc::now(), uuid::Uuid::new_v4());
        assert_eq!(position.quantity, 100.0);
        assert_eq!(position.cost_basis(), 50.0);

        position.add_shares(100.0, 60.0, Utc::now(), uuid::Uuid::new_v4());
        assert_eq!(position.quantity, 200.0);
        assert_eq!(position.cost_basis(), 55.0); // Average of 50 and 60
    }

    #[test]
    fn test_position_remove_shares_fifo() {
        let mut position = LedgerPosition::new(1, CostBasisMethod::FIFO);

        position.add_shares(100.0, 50.0, Utc::now(), uuid::Uuid::new_v4());
        position.add_shares(100.0, 60.0, Utc::now(), uuid::Uuid::new_v4());

        // Sell 150 shares at $70 (should use FIFO)
        let pnl = position.remove_shares(150.0, 70.0).unwrap();

        // First 100 at $50: profit = 100 * (70-50) = 2000
        // Next 50 at $60: profit = 50 * (70-60) = 500
        // Total = 2500
        assert_eq!(pnl, 2500.0);
        assert_eq!(position.quantity, 50.0);
    }

    #[test]
    fn test_position_remove_shares_lifo() {
        let mut position = LedgerPosition::new(1, CostBasisMethod::LIFO);

        position.add_shares(100.0, 50.0, Utc::now(), uuid::Uuid::new_v4());
        position.add_shares(100.0, 60.0, Utc::now(), uuid::Uuid::new_v4());

        // Sell 150 shares at $70 (should use LIFO)
        let pnl = position.remove_shares(150.0, 70.0).unwrap();

        // First 100 at $60: profit = 100 * (70-60) = 1000
        // Next 50 at $50: profit = 50 * (70-50) = 1000
        // Total = 2000
        assert_eq!(pnl, 2000.0);
        assert_eq!(position.quantity, 50.0);
    }

    #[test]
    fn test_position_unrealized_pnl() {
        let mut position = LedgerPosition::new(1, CostBasisMethod::Average);

        position.add_shares(100.0, 50.0, Utc::now(), uuid::Uuid::new_v4());
        position.add_shares(100.0, 60.0, Utc::now(), uuid::Uuid::new_v4());

        // Average cost = 55, quantity = 200
        // At price 70: unrealized = 200 * (70 - 55) = 3000
        assert_eq!(position.unrealized_pnl(70.0), 3000.0);
    }

    #[test]
    fn test_ledger_record_buy() {
        let mut ledger = Ledger::new(CostBasisMethod::FIFO);

        let txn = create_test_transaction(1, 100.0, 50.0, OrderSide::Buy);
        ledger.record_transaction(txn).unwrap();

        assert_eq!(ledger.transaction_count(), 1);
        assert_eq!(ledger.open_position_count(), 1);

        let position = ledger.get_position(1).unwrap();
        assert_eq!(position.quantity, 100.0);
        assert_eq!(position.cost_basis(), 50.0);
    }

    #[test]
    fn test_ledger_record_buy_sell() {
        let mut ledger = Ledger::new(CostBasisMethod::FIFO);

        // Buy 100 at $50
        let buy_txn = create_test_transaction(1, 100.0, 50.0, OrderSide::Buy);
        ledger.record_transaction(buy_txn).unwrap();

        // Sell 50 at $60
        let sell_txn = create_test_transaction(1, 50.0, 60.0, OrderSide::Sell);
        ledger.record_transaction(sell_txn).unwrap();

        let summary = ledger.get_pnl_summary();
        assert_eq!(summary.realized_pnl, 500.0); // 50 * (60 - 50)
        assert_eq!(summary.total_trades, 1);
        assert_eq!(summary.winning_trades, 1);
    }

    #[test]
    fn test_ledger_pnl_summary() {
        let mut ledger = Ledger::new(CostBasisMethod::FIFO);

        // Winning trade
        ledger.record_transaction(create_test_transaction(1, 100.0, 50.0, OrderSide::Buy)).unwrap();
        ledger.record_transaction(create_test_transaction(1, 100.0, 60.0, OrderSide::Sell)).unwrap();

        // Losing trade
        ledger.record_transaction(create_test_transaction(2, 100.0, 50.0, OrderSide::Buy)).unwrap();
        ledger.record_transaction(create_test_transaction(2, 100.0, 40.0, OrderSide::Sell)).unwrap();

        let summary = ledger.get_pnl_summary();
        assert_eq!(summary.total_trades, 2);
        assert_eq!(summary.winning_trades, 1);
        assert_eq!(summary.losing_trades, 1);
        assert_eq!(summary.win_rate, 0.5);
        assert_eq!(summary.realized_pnl, 0.0); // 1000 - 1000
    }

    #[test]
    fn test_ledger_transactions_by_asset() {
        let mut ledger = Ledger::new(CostBasisMethod::FIFO);

        ledger.record_transaction(create_test_transaction(1, 100.0, 50.0, OrderSide::Buy)).unwrap();
        ledger.record_transaction(create_test_transaction(2, 100.0, 60.0, OrderSide::Buy)).unwrap();
        ledger.record_transaction(create_test_transaction(1, 50.0, 55.0, OrderSide::Sell)).unwrap();

        let asset1_txns = ledger.get_transactions_for_asset(1);
        assert_eq!(asset1_txns.len(), 2);

        let asset2_txns = ledger.get_transactions_for_asset(2);
        assert_eq!(asset2_txns.len(), 1);
    }

    #[test]
    fn test_ledger_average_entry_price() {
        let mut ledger = Ledger::new(CostBasisMethod::Average);

        ledger.record_transaction(create_test_transaction(1, 100.0, 50.0, OrderSide::Buy)).unwrap();
        ledger.record_transaction(create_test_transaction(1, 100.0, 60.0, OrderSide::Buy)).unwrap();

        assert_eq!(ledger.average_entry_price(1), Some(55.0));
    }
}
