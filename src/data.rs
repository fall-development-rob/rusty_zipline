//! Market data handling

use crate::asset::Asset;
use crate::error::{Result, ZiplineError};
use crate::types::{Bar, Price, Timestamp};
use hashbrown::HashMap;
use std::collections::VecDeque;

/// Bar data provider for algorithm
#[derive(Debug, Clone)]
pub struct BarData {
    /// Current bars by asset ID
    current_bars: HashMap<u64, Bar>,
    /// Historical bars by asset ID
    history: HashMap<u64, VecDeque<Bar>>,
    /// Maximum history length
    max_history_len: usize,
}

impl BarData {
    /// Create a new BarData
    pub fn new(max_history_len: usize) -> Self {
        Self {
            current_bars: HashMap::new(),
            history: HashMap::new(),
            max_history_len,
        }
    }

    /// Update current bar for an asset
    pub fn update(&mut self, asset_id: u64, bar: Bar) {
        // Store in history
        let history = self.history.entry(asset_id).or_insert_with(VecDeque::new);
        history.push_back(bar.clone());

        // Limit history length
        while history.len() > self.max_history_len {
            history.pop_front();
        }

        // Update current bar
        self.current_bars.insert(asset_id, bar);
    }

    /// Get current bar for an asset
    pub fn current(&self, asset: &Asset) -> Result<&Bar> {
        self.current_bars
            .get(&asset.id)
            .ok_or_else(|| ZiplineError::DataError(format!("No data for {}", asset.symbol)))
    }

    /// Get current price for an asset
    pub fn current_price(&self, asset: &Asset) -> Result<Price> {
        Ok(self.current(asset)?.close)
    }

    /// Get historical bars for an asset
    pub fn history(&self, asset: &Asset, bars: usize) -> Result<Vec<Bar>> {
        let history = self
            .history
            .get(&asset.id)
            .ok_or_else(|| ZiplineError::DataError(format!("No history for {}", asset.symbol)))?;

        let start_idx = if history.len() > bars {
            history.len() - bars
        } else {
            0
        };

        Ok(history.iter().skip(start_idx).cloned().collect())
    }

    /// Get historical prices for an asset
    pub fn history_prices(&self, asset: &Asset, bars: usize) -> Result<Vec<Price>> {
        Ok(self.history(asset, bars)?.iter().map(|b| b.close).collect())
    }

    /// Check if asset has data
    pub fn has_data(&self, asset: &Asset) -> bool {
        self.current_bars.contains_key(&asset.id)
    }

    /// Get number of historical bars available
    pub fn history_len(&self, asset: &Asset) -> usize {
        self.history
            .get(&asset.id)
            .map(|h| h.len())
            .unwrap_or(0)
    }
}

/// Data source trait for providing market data
pub trait DataSource: Send + Sync {
    /// Get bars for a specific timestamp
    fn get_bars(&self, timestamp: Timestamp) -> Result<Vec<(u64, Bar)>>;

    /// Get available assets
    fn get_assets(&self) -> Vec<Asset>;

    /// Get date range available
    fn get_date_range(&self) -> (Timestamp, Timestamp);
}

/// In-memory data source for backtesting
#[derive(Debug)]
pub struct InMemoryDataSource {
    /// Bars indexed by timestamp then asset_id
    bars: HashMap<Timestamp, HashMap<u64, Bar>>,
    /// Available assets
    assets: Vec<Asset>,
    /// Date range
    date_range: (Timestamp, Timestamp),
}

impl InMemoryDataSource {
    /// Create a new in-memory data source
    pub fn new() -> Self {
        Self {
            bars: HashMap::new(),
            assets: Vec::new(),
            date_range: (Timestamp::default(), Timestamp::default()),
        }
    }

    /// Add a bar to the data source
    pub fn add_bar(&mut self, asset_id: u64, bar: Bar) {
        let timestamp_bars = self.bars.entry(bar.timestamp).or_insert_with(HashMap::new);
        timestamp_bars.insert(asset_id, bar);
    }

    /// Add an asset
    pub fn add_asset(&mut self, asset: Asset) {
        if !self.assets.iter().any(|a| a.id == asset.id) {
            self.assets.push(asset);
        }
    }

    /// Set date range
    pub fn set_date_range(&mut self, start: Timestamp, end: Timestamp) {
        self.date_range = (start, end);
    }
}

impl Default for InMemoryDataSource {
    fn default() -> Self {
        Self::new()
    }
}

impl DataSource for InMemoryDataSource {
    fn get_bars(&self, timestamp: Timestamp) -> Result<Vec<(u64, Bar)>> {
        match self.bars.get(&timestamp) {
            Some(bars) => Ok(bars.iter().map(|(id, bar)| (*id, bar.clone())).collect()),
            None => Ok(Vec::new()),
        }
    }

    fn get_assets(&self) -> Vec<Asset> {
        self.assets.clone()
    }

    fn get_date_range(&self) -> (Timestamp, Timestamp) {
        self.date_range
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::Asset;
    use chrono::Utc;

    #[test]
    fn test_bar_data() {
        let mut bar_data = BarData::new(100);
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        let bar = Bar::new(Utc::now(), 100.0, 105.0, 99.0, 103.0, 1000.0);

        bar_data.update(1, bar.clone());

        assert!(bar_data.has_data(&asset));
        assert_eq!(bar_data.current_price(&asset).unwrap(), 103.0);
        assert_eq!(bar_data.history_len(&asset), 1);
    }

    #[test]
    fn test_history() {
        let mut bar_data = BarData::new(100);
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());

        for i in 0..10 {
            let bar = Bar::new(Utc::now(), 100.0 + i as f64, 105.0, 99.0, 103.0, 1000.0);
            bar_data.update(1, bar);
        }

        let history = bar_data.history(&asset, 5).unwrap();
        assert_eq!(history.len(), 5);

        let prices = bar_data.history_prices(&asset, 5).unwrap();
        assert_eq!(prices.len(), 5);
    }
}
