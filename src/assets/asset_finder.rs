//! Asset Finder - Symbol lookup and asset retrieval
//!
//! Provides efficient symbol → asset resolution with support for:
//! - Point-in-time symbol lookups
//! - Symbol history (ticker changes)
//! - Batch lookups
//! - SID-based retrieval

use crate::asset::{Asset, AssetType};
use crate::error::{Result, ZiplineError};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Symbol entry with validity period
#[derive(Debug, Clone)]
pub struct SymbolEntry {
    pub sid: u64,
    pub symbol: String,
    pub start_date: DateTime<Utc>,
    pub end_date: Option<DateTime<Utc>>,
}

/// AssetFinder - Central asset lookup and management
///
/// Provides fast lookups for:
/// - Symbol → Asset (with point-in-time resolution)
/// - SID → Asset
/// - Batch symbol lookups
pub struct AssetFinder {
    /// Assets by SID
    assets: Arc<RwLock<HashMap<u64, Asset>>>,

    /// Symbol history index: symbol → [(start_date, sid)]
    /// Sorted by start_date for efficient point-in-time lookup
    symbol_index: Arc<RwLock<HashMap<String, Vec<SymbolEntry>>>>,

    /// Current symbols (latest mapping)
    current_symbols: Arc<RwLock<HashMap<String, u64>>>,

    /// Next available SID
    next_sid: Arc<RwLock<u64>>,
}

impl AssetFinder {
    /// Create a new AssetFinder
    pub fn new() -> Self {
        Self {
            assets: Arc::new(RwLock::new(HashMap::new())),
            symbol_index: Arc::new(RwLock::new(HashMap::new())),
            current_symbols: Arc::new(RwLock::new(HashMap::new())),
            next_sid: Arc::new(RwLock::new(1)),
        }
    }

    /// Insert an asset into the finder
    pub fn insert_asset(&self, asset: Asset) -> Result<()> {
        let sid = asset.sid;
        let symbol = asset.symbol.clone();
        let start_date = asset.start_date;
        let end_date = asset.end_date;

        // Insert into assets map
        self.assets.write().unwrap().insert(sid, asset);

        // Update symbol index
        let entry = SymbolEntry {
            sid,
            symbol: symbol.clone(),
            start_date,
            end_date,
        };

        let mut symbol_index = self.symbol_index.write().unwrap();
        symbol_index
            .entry(symbol.clone())
            .or_insert_with(Vec::new)
            .push(entry);

        // Sort by start_date for efficient lookup
        if let Some(entries) = symbol_index.get_mut(&symbol) {
            entries.sort_by_key(|e| e.start_date);
        }

        // Update current symbols if this is the latest
        if end_date.is_none() || end_date.unwrap() > Utc::now() {
            self.current_symbols
                .write()
                .unwrap()
                .insert(symbol, sid);
        }

        Ok(())
    }

    /// Look up a symbol at a specific point in time
    ///
    /// # Arguments
    /// * `symbol` - Symbol to look up (case-insensitive)
    /// * `as_of_date` - Point in time for lookup (None = current)
    pub fn lookup_symbol(
        &self,
        symbol: &str,
        as_of_date: Option<DateTime<Utc>>,
    ) -> Result<Asset> {
        let symbol_upper = symbol.to_uppercase();
        let lookup_date = as_of_date.unwrap_or_else(Utc::now);

        // Get symbol history
        let symbol_index = self.symbol_index.read().unwrap();
        let entries = symbol_index
            .get(&symbol_upper)
            .ok_or_else(|| ZiplineError::SymbolNotFound {
                symbol: symbol.to_string(),
            })?;

        // Find entry valid at as_of_date
        let mut matching_entry: Option<&SymbolEntry> = None;

        for entry in entries {
            if entry.start_date <= lookup_date {
                if let Some(end) = entry.end_date {
                    if end >= lookup_date {
                        matching_entry = Some(entry);
                        break;
                    }
                } else {
                    // No end date = currently valid
                    matching_entry = Some(entry);
                }
            }
        }

        let entry = matching_entry.ok_or_else(|| ZiplineError::SymbolNotFound {
            symbol: symbol.to_string(),
        })?;

        // Retrieve asset
        self.retrieve_asset(entry.sid)
    }

    /// Retrieve asset by SID
    pub fn retrieve_asset(&self, sid: u64) -> Result<Asset> {
        self.assets
            .read()
            .unwrap()
            .get(&sid)
            .cloned()
            .ok_or(ZiplineError::AssetNotFound(sid))
    }

    /// Look up multiple symbols at once
    pub fn lookup_symbols(
        &self,
        symbols: &[&str],
        as_of_date: Option<DateTime<Utc>>,
    ) -> Result<Vec<Asset>> {
        let mut assets = Vec::with_capacity(symbols.len());

        for &symbol in symbols {
            assets.push(self.lookup_symbol(symbol, as_of_date)?);
        }

        Ok(assets)
    }

    /// Get all assets of a specific type
    pub fn get_assets_by_type(&self, asset_type: AssetType) -> Vec<Asset> {
        self.assets
            .read()
            .unwrap()
            .values()
            .filter(|a| a.asset_type == asset_type)
            .cloned()
            .collect()
    }

    /// Get all equities
    pub fn equities(&self) -> Vec<Asset> {
        self.get_assets_by_type(AssetType::Equity)
    }

    /// Get all futures
    pub fn futures(&self) -> Vec<Asset> {
        self.get_assets_by_type(AssetType::Future)
    }

    /// Get symbol history for a symbol
    pub fn get_symbol_history(&self, symbol: &str) -> Vec<SymbolEntry> {
        let symbol_upper = symbol.to_uppercase();
        self.symbol_index
            .read()
            .unwrap()
            .get(&symbol_upper)
            .cloned()
            .unwrap_or_default()
    }

    /// Check if a symbol exists
    pub fn symbol_exists(&self, symbol: &str) -> bool {
        let symbol_upper = symbol.to_uppercase();
        self.symbol_index
            .read()
            .unwrap()
            .contains_key(&symbol_upper)
    }

    /// Get total number of assets
    pub fn asset_count(&self) -> usize {
        self.assets.read().unwrap().len()
    }

    /// Get next available SID
    pub fn next_sid(&self) -> u64 {
        let mut next = self.next_sid.write().unwrap();
        let sid = *next;
        *next += 1;
        sid
    }

    /// Bulk insert assets
    pub fn insert_assets(&self, assets: Vec<Asset>) -> Result<()> {
        for asset in assets {
            self.insert_asset(asset)?;
        }
        Ok(())
    }
}

impl Default for AssetFinder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_asset(sid: u64, symbol: &str, start: DateTime<Utc>) -> Asset {
        Asset {
            sid,
            symbol: symbol.to_string(),
            asset_type: AssetType::Equity,
            exchange: "NYSE".to_string(),
            start_date: start,
            end_date: None,
            auto_close_date: None,
        }
    }

    #[test]
    fn test_asset_finder_creation() {
        let finder = AssetFinder::new();
        assert_eq!(finder.asset_count(), 0);
    }

    #[test]
    fn test_insert_and_retrieve_asset() {
        let finder = AssetFinder::new();
        let asset = create_test_asset(1, "AAPL", Utc::now());

        finder.insert_asset(asset.clone()).unwrap();

        let retrieved = finder.retrieve_asset(1).unwrap();
        assert_eq!(retrieved.symbol, "AAPL");
        assert_eq!(retrieved.sid, 1);
    }

    #[test]
    fn test_lookup_symbol() {
        let finder = AssetFinder::new();
        let asset = create_test_asset(1, "AAPL", Utc::now());

        finder.insert_asset(asset).unwrap();

        let found = finder.lookup_symbol("AAPL", None).unwrap();
        assert_eq!(found.sid, 1);

        // Case insensitive
        let found = finder.lookup_symbol("aapl", None).unwrap();
        assert_eq!(found.sid, 1);
    }

    #[test]
    fn test_lookup_nonexistent_symbol() {
        let finder = AssetFinder::new();

        let result = finder.lookup_symbol("INVALID", None);
        assert!(result.is_err());

        if let Err(ZiplineError::SymbolNotFound { symbol }) = result {
            assert_eq!(symbol, "INVALID");
        } else {
            panic!("Expected SymbolNotFound error");
        }
    }

    #[test]
    fn test_retrieve_nonexistent_sid() {
        let finder = AssetFinder::new();

        let result = finder.retrieve_asset(999);
        assert!(result.is_err());

        if let Err(ZiplineError::AssetNotFound(sid)) = result {
            assert_eq!(sid, 999);
        } else {
            panic!("Expected AssetNotFound error");
        }
    }

    #[test]
    fn test_lookup_symbols_batch() {
        let finder = AssetFinder::new();

        let asset1 = create_test_asset(1, "AAPL", Utc::now());
        let asset2 = create_test_asset(2, "GOOGL", Utc::now());

        finder.insert_asset(asset1).unwrap();
        finder.insert_asset(asset2).unwrap();

        let assets = finder.lookup_symbols(&["AAPL", "GOOGL"], None).unwrap();
        assert_eq!(assets.len(), 2);
        assert_eq!(assets[0].symbol, "AAPL");
        assert_eq!(assets[1].symbol, "GOOGL");
    }

    #[test]
    fn test_symbol_history() {
        let finder = AssetFinder::new();

        use chrono::Duration;
        let now = Utc::now();
        let past = now - Duration::days(365);

        // Old symbol mapping
        let mut old_asset = create_test_asset(1, "FB", past);
        old_asset.end_date = Some(now - Duration::days(30));
        finder.insert_asset(old_asset).unwrap();

        // New symbol mapping
        let new_asset = create_test_asset(2, "META", now - Duration::days(29));
        finder.insert_asset(new_asset).unwrap();

        // Lookup in the past should find FB
        let found = finder.lookup_symbol("FB", Some(past + Duration::days(10))).unwrap();
        assert_eq!(found.sid, 1);

        // Lookup now should find META (or fail if FB is looked up)
        let result = finder.lookup_symbol("META", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_assets_by_type() {
        let finder = AssetFinder::new();

        let equity = create_test_asset(1, "AAPL", Utc::now());
        let mut future = create_test_asset(2, "CLZ23", Utc::now());
        future.asset_type = AssetType::Future;

        finder.insert_asset(equity).unwrap();
        finder.insert_asset(future).unwrap();

        let equities = finder.equities();
        let futures = finder.futures();

        assert_eq!(equities.len(), 1);
        assert_eq!(futures.len(), 1);
        assert_eq!(equities[0].symbol, "AAPL");
        assert_eq!(futures[0].symbol, "CLZ23");
    }

    #[test]
    fn test_symbol_exists() {
        let finder = AssetFinder::new();
        let asset = create_test_asset(1, "AAPL", Utc::now());

        assert!(!finder.symbol_exists("AAPL"));

        finder.insert_asset(asset).unwrap();

        assert!(finder.symbol_exists("AAPL"));
        assert!(finder.symbol_exists("aapl")); // Case insensitive
        assert!(!finder.symbol_exists("INVALID"));
    }

    #[test]
    fn test_asset_count() {
        let finder = AssetFinder::new();
        assert_eq!(finder.asset_count(), 0);

        finder.insert_asset(create_test_asset(1, "AAPL", Utc::now())).unwrap();
        assert_eq!(finder.asset_count(), 1);

        finder.insert_asset(create_test_asset(2, "GOOGL", Utc::now())).unwrap();
        assert_eq!(finder.asset_count(), 2);
    }

    #[test]
    fn test_next_sid() {
        let finder = AssetFinder::new();

        let sid1 = finder.next_sid();
        let sid2 = finder.next_sid();
        let sid3 = finder.next_sid();

        assert_eq!(sid1, 1);
        assert_eq!(sid2, 2);
        assert_eq!(sid3, 3);
    }

    #[test]
    fn test_bulk_insert() {
        let finder = AssetFinder::new();

        let assets = vec![
            create_test_asset(1, "AAPL", Utc::now()),
            create_test_asset(2, "GOOGL", Utc::now()),
            create_test_asset(3, "MSFT", Utc::now()),
        ];

        finder.insert_assets(assets).unwrap();
        assert_eq!(finder.asset_count(), 3);
    }
}
