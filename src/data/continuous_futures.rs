//! Continuous Futures - Synthetic perpetual contracts
//!
//! This module provides continuous futures construction by stitching together
//! individual futures contracts as they expire, creating a synthetic "perpetual"
//! contract for backtesting and analysis.
//!
//! Key concepts:
//! - Roll schedules: When to switch from front contract to back contract
//! - Price adjustments: How to handle discontinuities at roll points
//! - Contract chains: Ordered sequence of futures contracts by expiration

use crate::data::bar_reader::{Bar, BarReader};
use crate::error::{Result, ZiplineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Roll style determines when to switch from front to back month
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RollStyle {
    /// Roll on specific calendar dates (e.g., 5 days before expiration)
    Calendar,
    /// Roll when back month volume exceeds front month
    Volume,
    /// Roll when back month open interest exceeds front month
    OpenInterest,
}

impl Default for RollStyle {
    fn default() -> Self {
        RollStyle::Calendar
    }
}

/// Adjustment style for handling price discontinuities at roll points
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AdjustmentStyle {
    /// No adjustment - prices jump at rolls (actual prices)
    None,
    /// Multiplicative ratio adjustment (Panama Canal method)
    /// Multiply all historical prices by ratio to make continuous
    PanamaCanal,
    /// Backward-looking ratio adjustment
    /// Adjust only past data, keep current contract at actual prices
    BackwardRatio,
    /// Additive adjustment (difference-based)
    Add,
}

impl Default for AdjustmentStyle {
    fn default() -> Self {
        AdjustmentStyle::None
    }
}

/// Represents a single futures contract with expiration metadata
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FutureContract {
    /// Asset identifier (symbol)
    pub symbol: String,
    /// Root symbol (e.g., "ES" for E-mini S&P 500)
    pub root_symbol: String,
    /// Expiration date
    pub expiration: DateTime<Utc>,
    /// Contract month code (e.g., "H3" for March 2023)
    pub contract_code: String,
    /// Asset ID for data lookup
    pub asset_id: u64,
}

impl FutureContract {
    /// Create a new futures contract
    pub fn new(
        symbol: String,
        root_symbol: String,
        expiration: DateTime<Utc>,
        contract_code: String,
        asset_id: u64,
    ) -> Self {
        Self {
            symbol,
            root_symbol,
            expiration,
            contract_code,
            asset_id,
        }
    }

    /// Check if contract is expired at given date
    pub fn is_expired(&self, dt: DateTime<Utc>) -> bool {
        dt >= self.expiration
    }

    /// Days until expiration
    pub fn days_until_expiration(&self, dt: DateTime<Utc>) -> i64 {
        (self.expiration - dt).num_days()
    }
}

/// Contract chain - ordered sequence of futures contracts
#[derive(Debug, Clone)]
pub struct ContractChain {
    /// Root symbol for this chain
    pub root_symbol: String,
    /// Contracts sorted by expiration date
    contracts: Vec<FutureContract>,
    /// Index by expiration date for fast lookup
    expiration_index: BTreeMap<DateTime<Utc>, usize>,
}

impl ContractChain {
    /// Create a new contract chain
    pub fn new(root_symbol: String) -> Self {
        Self {
            root_symbol,
            contracts: Vec::new(),
            expiration_index: BTreeMap::new(),
        }
    }

    /// Add a contract to the chain
    pub fn add_contract(&mut self, contract: FutureContract) -> Result<()> {
        if contract.root_symbol != self.root_symbol {
            return Err(ZiplineError::InvalidData(format!(
                "Contract root symbol {} does not match chain root {}",
                contract.root_symbol, self.root_symbol
            )));
        }

        let expiration = contract.expiration;
        self.contracts.push(contract);

        // Re-sort and rebuild index
        self.contracts.sort_by_key(|c| c.expiration);
        self.rebuild_index();

        Ok(())
    }

    /// Rebuild the expiration index
    fn rebuild_index(&mut self) {
        self.expiration_index.clear();
        for (idx, contract) in self.contracts.iter().enumerate() {
            self.expiration_index.insert(contract.expiration, idx);
        }
    }

    /// Get active contract at a given date with offset
    /// offset=0 is front month, offset=1 is second month, etc.
    pub fn get_contract_at(&self, dt: DateTime<Utc>, offset: i32) -> Option<&FutureContract> {
        // Find first non-expired contract
        let front_idx = self.contracts.iter().position(|c| !c.is_expired(dt))?;

        // Apply offset
        let target_idx = (front_idx as i32 + offset) as usize;

        if target_idx < self.contracts.len() {
            Some(&self.contracts[target_idx])
        } else {
            None
        }
    }

    /// Get all contracts
    pub fn contracts(&self) -> &[FutureContract] {
        &self.contracts
    }

    /// Get contract count
    pub fn len(&self) -> usize {
        self.contracts.len()
    }

    /// Check if chain is empty
    pub fn is_empty(&self) -> bool {
        self.contracts.is_empty()
    }

    /// Get date range covered by this chain
    pub fn date_range(&self) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        if self.contracts.is_empty() {
            return None;
        }

        let first = self.contracts.first().unwrap();
        let last = self.contracts.last().unwrap();

        Some((first.expiration, last.expiration))
    }
}

/// Roll schedule determines when to switch contracts
#[derive(Debug, Clone)]
pub struct RollSchedule {
    /// Style of roll
    pub style: RollStyle,
    /// Days before expiration to roll (for Calendar style)
    pub days_before_expiration: i32,
}

impl Default for RollSchedule {
    fn default() -> Self {
        Self {
            style: RollStyle::Calendar,
            days_before_expiration: 5, // Roll 5 days before expiration
        }
    }
}

impl RollSchedule {
    /// Create a calendar-based roll schedule
    pub fn calendar(days_before_expiration: i32) -> Self {
        Self {
            style: RollStyle::Calendar,
            days_before_expiration,
        }
    }

    /// Create a volume-based roll schedule
    pub fn volume() -> Self {
        Self {
            style: RollStyle::Volume,
            days_before_expiration: 0,
        }
    }

    /// Create an open interest-based roll schedule
    pub fn open_interest() -> Self {
        Self {
            style: RollStyle::OpenInterest,
            days_before_expiration: 0,
        }
    }

    /// Determine if we should roll at this date
    pub fn should_roll(&self, contract: &FutureContract, dt: DateTime<Utc>, bars: &[Bar]) -> bool {
        match self.style {
            RollStyle::Calendar => {
                let days_to_exp = contract.days_until_expiration(dt);
                days_to_exp <= self.days_before_expiration as i64
            }
            RollStyle::Volume => {
                // Would need current and next contract volumes
                // Simplified: always false for now
                false
            }
            RollStyle::OpenInterest => {
                // Would need open interest data
                // Simplified: always false for now
                false
            }
        }
    }
}

/// Continuous futures reader trait
pub trait ContinuousFutureReader: Send + Sync {
    /// Get continuous futures prices for a date range
    fn get_continuous_prices(
        &self,
        root_symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        offset: i32,
        roll_style: RollStyle,
        adjustment: AdjustmentStyle,
    ) -> Result<Vec<Bar>>;

    /// Get active contract at a specific date
    fn get_active_contract(
        &self,
        root_symbol: &str,
        dt: DateTime<Utc>,
        offset: i32,
    ) -> Result<Option<FutureContract>>;
}

/// Default implementation of continuous futures reader
pub struct DefaultContinuousFutureReader {
    /// Bar reader for individual contracts
    bar_reader: Arc<dyn BarReader>,
    /// Contract chains by root symbol
    chains: BTreeMap<String, ContractChain>,
    /// Roll schedule
    roll_schedule: RollSchedule,
}

impl DefaultContinuousFutureReader {
    /// Create a new continuous futures reader
    pub fn new(bar_reader: Arc<dyn BarReader>) -> Self {
        Self {
            bar_reader,
            chains: BTreeMap::new(),
            roll_schedule: RollSchedule::default(),
        }
    }

    /// Add a contract chain
    pub fn add_chain(&mut self, chain: ContractChain) {
        self.chains.insert(chain.root_symbol.clone(), chain);
    }

    /// Set roll schedule
    pub fn set_roll_schedule(&mut self, schedule: RollSchedule) {
        self.roll_schedule = schedule;
    }

    /// Get chain for root symbol
    pub fn get_chain(&self, root_symbol: &str) -> Option<&ContractChain> {
        self.chains.get(root_symbol)
    }

    /// Build continuous series from individual contracts
    fn build_continuous_series(
        &self,
        chain: &ContractChain,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        offset: i32,
        adjustment: AdjustmentStyle,
    ) -> Result<Vec<Bar>> {
        let mut result = Vec::new();
        let mut current_date = start;
        let mut adjustment_ratio = 1.0;

        while current_date <= end {
            // Get active contract for this date
            if let Some(contract) = chain.get_contract_at(current_date, offset) {
                // Create mock asset for bar reader
                let asset = crate::asset::Asset::new(
                    contract.asset_id,
                    contract.symbol.clone(),
                    "FUTURES".to_string(),
                    crate::asset::AssetType::Future,
                );

                // Get bar for this contract
                if let Ok(mut bar) = self.bar_reader.get_bar(&asset, current_date) {
                    // Apply adjustment if needed
                    if adjustment != AdjustmentStyle::None {
                        bar = self.apply_adjustment(bar, adjustment_ratio);
                    }

                    result.push(bar);

                    // Check if we need to roll
                    if self.roll_schedule.should_roll(contract, current_date, &[bar]) {
                        // Calculate adjustment for next contract
                        if adjustment == AdjustmentStyle::PanamaCanal
                            || adjustment == AdjustmentStyle::BackwardRatio
                        {
                            if let Some(next_contract) = chain.get_contract_at(current_date, offset + 1) {
                                let next_asset = crate::asset::Asset::new(
                                    next_contract.asset_id,
                                    next_contract.symbol.clone(),
                                    "FUTURES".to_string(),
                                    crate::asset::AssetType::Future,
                                );

                                if let Ok(next_bar) = self.bar_reader.get_bar(&next_asset, current_date) {
                                    // Calculate ratio
                                    let ratio = next_bar.close / bar.close;
                                    adjustment_ratio *= ratio;
                                }
                            }
                        }
                    }
                }
            }

            // Move to next day (simplified - should use trading calendar)
            current_date = current_date + chrono::Duration::days(1);
        }

        Ok(result)
    }

    /// Apply adjustment to a bar
    fn apply_adjustment(&self, mut bar: Bar, ratio: f64) -> Bar {
        bar.open *= ratio;
        bar.high *= ratio;
        bar.low *= ratio;
        bar.close *= ratio;
        // Volume typically not adjusted
        bar
    }
}

impl ContinuousFutureReader for DefaultContinuousFutureReader {
    fn get_continuous_prices(
        &self,
        root_symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        offset: i32,
        _roll_style: RollStyle,
        adjustment: AdjustmentStyle,
    ) -> Result<Vec<Bar>> {
        let chain = self
            .chains
            .get(root_symbol)
            .ok_or_else(|| {
                ZiplineError::DataNotFound(format!(
                    "No contract chain found for root symbol: {}",
                    root_symbol
                ))
            })?;

        self.build_continuous_series(chain, start, end, offset, adjustment)
    }

    fn get_active_contract(
        &self,
        root_symbol: &str,
        dt: DateTime<Utc>,
        offset: i32,
    ) -> Result<Option<FutureContract>> {
        let chain = self
            .chains
            .get(root_symbol)
            .ok_or_else(|| {
                ZiplineError::DataNotFound(format!(
                    "No contract chain found for root symbol: {}",
                    root_symbol
                ))
            })?;

        Ok(chain.get_contract_at(dt, offset).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::{Asset, AssetType};

    // Mock bar reader for testing
    struct MockBarReader {
        data: BTreeMap<(u64, DateTime<Utc>), Bar>,
    }

    impl MockBarReader {
        fn new() -> Self {
            Self {
                data: BTreeMap::new(),
            }
        }

        fn insert(&mut self, asset_id: u64, bar: Bar) {
            self.data.insert((asset_id, bar.dt), bar);
        }
    }

    impl BarReader for MockBarReader {
        fn get_bar(&self, asset: &Asset, dt: DateTime<Utc>) -> Result<Bar> {
            self.data
                .get(&(asset.id, dt))
                .copied()
                .ok_or_else(|| {
                    ZiplineError::DataNotFound(format!(
                        "No bar for asset {} at {:?}",
                        asset.id, dt
                    ))
                })
        }

        fn get_bars(
            &self,
            _asset: &Asset,
            _start: DateTime<Utc>,
            _end: DateTime<Utc>,
        ) -> Result<Vec<Bar>> {
            Ok(Vec::new())
        }

        fn last_available_dt(&self, _asset: &Asset) -> Result<DateTime<Utc>> {
            Ok(Utc::now())
        }

        fn first_available_dt(&self, _asset: &Asset) -> Result<DateTime<Utc>> {
            Ok(Utc::now())
        }
    }

    fn create_test_bar(dt: DateTime<Utc>, close: f64) -> Bar {
        Bar::new(close * 0.98, close * 1.02, close * 0.97, close, 1000.0, dt)
    }

    #[test]
    fn test_roll_style_default() {
        assert_eq!(RollStyle::default(), RollStyle::Calendar);
    }

    #[test]
    fn test_adjustment_style_default() {
        assert_eq!(AdjustmentStyle::default(), AdjustmentStyle::None);
    }

    #[test]
    fn test_future_contract_creation() {
        use chrono::TimeZone;
        let expiration = Utc.with_ymd_and_hms(2023, 3, 17, 0, 0, 0).unwrap();

        let contract = FutureContract::new(
            "ESH3".to_string(),
            "ES".to_string(),
            expiration,
            "H3".to_string(),
            1001,
        );

        assert_eq!(contract.symbol, "ESH3");
        assert_eq!(contract.root_symbol, "ES");
        assert_eq!(contract.contract_code, "H3");
        assert_eq!(contract.asset_id, 1001);
    }

    #[test]
    fn test_future_contract_expiration() {
        use chrono::TimeZone;
        let expiration = Utc.with_ymd_and_hms(2023, 3, 17, 0, 0, 0).unwrap();
        let contract = FutureContract::new(
            "ESH3".to_string(),
            "ES".to_string(),
            expiration,
            "H3".to_string(),
            1001,
        );

        let before = Utc.with_ymd_and_hms(2023, 3, 10, 0, 0, 0).unwrap();
        let after = Utc.with_ymd_and_hms(2023, 3, 20, 0, 0, 0).unwrap();

        assert!(!contract.is_expired(before));
        assert!(contract.is_expired(after));
        assert_eq!(contract.days_until_expiration(before), 7);
    }

    #[test]
    fn test_contract_chain_creation() {
        let mut chain = ContractChain::new("ES".to_string());
        assert!(chain.is_empty());
        assert_eq!(chain.len(), 0);
        assert_eq!(chain.root_symbol, "ES");
    }

    #[test]
    fn test_contract_chain_add() {
        use chrono::TimeZone;
        let mut chain = ContractChain::new("ES".to_string());

        let exp1 = Utc.with_ymd_and_hms(2023, 3, 17, 0, 0, 0).unwrap();
        let exp2 = Utc.with_ymd_and_hms(2023, 6, 16, 0, 0, 0).unwrap();
        let exp3 = Utc.with_ymd_and_hms(2023, 9, 15, 0, 0, 0).unwrap();

        // Add contracts out of order
        chain.add_contract(FutureContract::new(
            "ESU3".to_string(),
            "ES".to_string(),
            exp3,
            "U3".to_string(),
            1003,
        )).unwrap();

        chain.add_contract(FutureContract::new(
            "ESH3".to_string(),
            "ES".to_string(),
            exp1,
            "H3".to_string(),
            1001,
        )).unwrap();

        chain.add_contract(FutureContract::new(
            "ESM3".to_string(),
            "ES".to_string(),
            exp2,
            "M3".to_string(),
            1002,
        )).unwrap();

        assert_eq!(chain.len(), 3);

        // Verify sorting by expiration
        let contracts = chain.contracts();
        assert_eq!(contracts[0].contract_code, "H3");
        assert_eq!(contracts[1].contract_code, "M3");
        assert_eq!(contracts[2].contract_code, "U3");
    }

    #[test]
    fn test_contract_chain_get_at() {
        use chrono::TimeZone;
        let mut chain = ContractChain::new("ES".to_string());

        let exp1 = Utc.with_ymd_and_hms(2023, 3, 17, 0, 0, 0).unwrap();
        let exp2 = Utc.with_ymd_and_hms(2023, 6, 16, 0, 0, 0).unwrap();

        chain.add_contract(FutureContract::new(
            "ESH3".to_string(),
            "ES".to_string(),
            exp1,
            "H3".to_string(),
            1001,
        )).unwrap();

        chain.add_contract(FutureContract::new(
            "ESM3".to_string(),
            "ES".to_string(),
            exp2,
            "M3".to_string(),
            1002,
        )).unwrap();

        // Date before first expiration - front month should be H3
        let dt = Utc.with_ymd_and_hms(2023, 3, 10, 0, 0, 0).unwrap();
        let front = chain.get_contract_at(dt, 0);
        assert!(front.is_some());
        assert_eq!(front.unwrap().contract_code, "H3");

        // Second month should be M3
        let second = chain.get_contract_at(dt, 1);
        assert!(second.is_some());
        assert_eq!(second.unwrap().contract_code, "M3");

        // After first expiration - front month should be M3
        let dt2 = Utc.with_ymd_and_hms(2023, 3, 20, 0, 0, 0).unwrap();
        let front2 = chain.get_contract_at(dt2, 0);
        assert!(front2.is_some());
        assert_eq!(front2.unwrap().contract_code, "M3");
    }

    #[test]
    fn test_contract_chain_date_range() {
        use chrono::TimeZone;
        let mut chain = ContractChain::new("CL".to_string());

        let exp1 = Utc.with_ymd_and_hms(2023, 1, 20, 0, 0, 0).unwrap();
        let exp2 = Utc.with_ymd_and_hms(2023, 12, 19, 0, 0, 0).unwrap();

        chain.add_contract(FutureContract::new(
            "CLF3".to_string(),
            "CL".to_string(),
            exp1,
            "F3".to_string(),
            2001,
        )).unwrap();

        chain.add_contract(FutureContract::new(
            "CLZ3".to_string(),
            "CL".to_string(),
            exp2,
            "Z3".to_string(),
            2002,
        )).unwrap();

        let range = chain.date_range();
        assert!(range.is_some());
        let (start, end) = range.unwrap();
        assert_eq!(start, exp1);
        assert_eq!(end, exp2);
    }

    #[test]
    fn test_roll_schedule_calendar() {
        let schedule = RollSchedule::calendar(5);
        assert_eq!(schedule.style, RollStyle::Calendar);
        assert_eq!(schedule.days_before_expiration, 5);
    }

    #[test]
    fn test_roll_schedule_should_roll() {
        use chrono::TimeZone;
        let schedule = RollSchedule::calendar(5);

        let expiration = Utc.with_ymd_and_hms(2023, 3, 17, 0, 0, 0).unwrap();
        let contract = FutureContract::new(
            "ESH3".to_string(),
            "ES".to_string(),
            expiration,
            "H3".to_string(),
            1001,
        );

        // 10 days before expiration - should not roll
        let dt1 = Utc.with_ymd_and_hms(2023, 3, 7, 0, 0, 0).unwrap();
        assert!(!schedule.should_roll(&contract, dt1, &[]));

        // 3 days before expiration - should roll
        let dt2 = Utc.with_ymd_and_hms(2023, 3, 14, 0, 0, 0).unwrap();
        assert!(schedule.should_roll(&contract, dt2, &[]));
    }

    #[test]
    fn test_continuous_futures_reader_creation() {
        let bar_reader = Arc::new(MockBarReader::new());
        let reader = DefaultContinuousFutureReader::new(bar_reader);

        assert_eq!(reader.chains.len(), 0);
    }

    #[test]
    fn test_continuous_futures_reader_add_chain() {
        let bar_reader = Arc::new(MockBarReader::new());
        let mut reader = DefaultContinuousFutureReader::new(bar_reader);

        let chain = ContractChain::new("GC".to_string());
        reader.add_chain(chain);

        assert_eq!(reader.chains.len(), 1);
        assert!(reader.get_chain("GC").is_some());
    }

    #[test]
    fn test_get_active_contract() {
        use chrono::TimeZone;
        let bar_reader = Arc::new(MockBarReader::new());
        let mut reader = DefaultContinuousFutureReader::new(bar_reader);

        let mut chain = ContractChain::new("ES".to_string());
        let exp = Utc.with_ymd_and_hms(2023, 3, 17, 0, 0, 0).unwrap();

        chain.add_contract(FutureContract::new(
            "ESH3".to_string(),
            "ES".to_string(),
            exp,
            "H3".to_string(),
            1001,
        )).unwrap();

        reader.add_chain(chain);

        let dt = Utc.with_ymd_and_hms(2023, 3, 10, 0, 0, 0).unwrap();
        let contract = reader.get_active_contract("ES", dt, 0).unwrap();

        assert!(contract.is_some());
        assert_eq!(contract.unwrap().symbol, "ESH3");
    }

    #[test]
    fn test_adjustment_application() {
        let bar_reader = Arc::new(MockBarReader::new());
        let reader = DefaultContinuousFutureReader::new(bar_reader);

        use chrono::TimeZone;
        let dt = Utc.with_ymd_and_hms(2023, 3, 10, 0, 0, 0).unwrap();
        let bar = Bar::new(100.0, 105.0, 99.0, 103.0, 1000.0, dt);

        let adjusted = reader.apply_adjustment(bar, 1.1);

        assert_eq!(adjusted.open, 110.0);
        assert_eq!(adjusted.high, 115.5);
        assert_eq!(adjusted.low, 108.9);
        assert_eq!(adjusted.close, 113.3);
        assert_eq!(adjusted.volume, 1000.0); // Volume unchanged
    }
}
