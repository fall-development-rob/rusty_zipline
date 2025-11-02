//! Bar Readers - OHLCV data access
//!
//! Provides efficient access to daily and minute-level bar data.

use crate::asset::Asset;
use crate::error::{Result, ZiplineError};
use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// OHLCV bar data
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Bar {
    /// Open price
    pub open: f64,
    /// High price
    pub high: f64,
    /// Low price
    pub low: f64,
    /// Close price
    pub close: f64,
    /// Volume
    pub volume: f64,
    /// Timestamp
    pub dt: DateTime<Utc>,
}

impl Bar {
    pub fn new(open: f64, high: f64, low: f64, close: f64, volume: f64, dt: DateTime<Utc>) -> Self {
        Self {
            open,
            high,
            low,
            close,
            volume,
            dt,
        }
    }

    /// Typical price (HLC/3)
    pub fn typical_price(&self) -> f64 {
        (self.high + self.low + self.close) / 3.0
    }

    /// True range (for ATR calculation)
    pub fn true_range(&self, prev_close: Option<f64>) -> f64 {
        let high_low = self.high - self.low;
        match prev_close {
            Some(pc) => {
                let high_prev_close = (self.high - pc).abs();
                let low_prev_close = (self.low - pc).abs();
                high_low.max(high_prev_close).max(low_prev_close)
            }
            None => high_low,
        }
    }

    /// Check if bar is valid (OHLC relationships)
    pub fn is_valid(&self) -> bool {
        self.high >= self.low
            && self.high >= self.open
            && self.high >= self.close
            && self.low <= self.open
            && self.low <= self.close
            && self.volume >= 0.0
    }
}

/// Session label for trading sessions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionLabel {
    pub year: i32,
    pub month: u32,
    pub day: u32,
}

impl SessionLabel {
    pub fn from_datetime(dt: DateTime<Utc>) -> Self {
        Self {
            year: dt.year(),
            month: dt.month(),
            day: dt.day(),
        }
    }

    pub fn to_datetime(&self) -> Result<DateTime<Utc>> {
        use chrono::TimeZone;
        Utc.with_ymd_and_hms(self.year, self.month, self.day, 0, 0, 0)
            .single()
            .ok_or_else(|| {
                ZiplineError::InvalidData(format!(
                    "Invalid session date: {}-{}-{}",
                    self.year, self.month, self.day
                ))
            })
    }
}

/// Trait for reading bar data
pub trait BarReader: Send + Sync {
    /// Get bar data for a single asset and date
    fn get_bar(&self, asset: &Asset, dt: DateTime<Utc>) -> Result<Bar>;

    /// Get multiple bars for an asset in a date range
    fn get_bars(&self, asset: &Asset, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Bar>>;

    /// Get last available date for an asset
    fn last_available_dt(&self, asset: &Asset) -> Result<DateTime<Utc>>;

    /// Get first available date for an asset
    fn first_available_dt(&self, asset: &Asset) -> Result<DateTime<Utc>>;

    /// Check if data is available for asset at date
    fn has_data(&self, asset: &Asset, dt: DateTime<Utc>) -> bool {
        self.get_bar(asset, dt).is_ok()
    }

    /// Get available sessions (trading days)
    fn sessions(&self) -> Result<Vec<SessionLabel>>;
}

/// Daily bar reader for EOD data
#[derive(Debug, Clone)]
pub struct DailyBarReader {
    /// Data storage: asset_id -> (date -> bar)
    data: HashMap<u64, HashMap<DateTime<Utc>, Bar>>,
    /// Trading sessions
    sessions: Vec<SessionLabel>,
    /// Data directory path
    data_dir: Option<PathBuf>,
}

impl DailyBarReader {
    /// Create new daily bar reader
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            sessions: Vec::new(),
            data_dir: None,
        }
    }

    /// Create with data directory
    pub fn with_data_dir(data_dir: PathBuf) -> Self {
        Self {
            data: HashMap::new(),
            sessions: Vec::new(),
            data_dir: Some(data_dir),
        }
    }

    /// Load data from memory (for testing/small datasets)
    pub fn load_from_memory(&mut self, asset_id: u64, bars: Vec<Bar>) -> Result<()> {
        let asset_data = self.data.entry(asset_id).or_insert_with(HashMap::new);

        for bar in bars {
            if !bar.is_valid() {
                return Err(ZiplineError::InvalidData(format!(
                    "Invalid bar data for asset {} at {:?}",
                    asset_id, bar.dt
                )));
            }

            // Update sessions
            let session = SessionLabel::from_datetime(bar.dt);
            if !self.sessions.contains(&session) {
                self.sessions.push(session);
            }

            asset_data.insert(bar.dt, bar);
        }

        // Sort sessions
        self.sessions.sort_by_key(|s| (s.year, s.month, s.day));

        Ok(())
    }

    /// Get all bars for an asset
    pub fn get_all_bars(&self, asset_id: u64) -> Result<Vec<Bar>> {
        let asset_data = self
            .data
            .get(&asset_id)
            .ok_or_else(|| ZiplineError::AssetNotFound(asset_id))?;

        let mut bars: Vec<Bar> = asset_data.values().copied().collect();
        bars.sort_by_key(|b| b.dt);

        Ok(bars)
    }

    /// Get bar count for an asset
    pub fn bar_count(&self, asset_id: u64) -> usize {
        self.data
            .get(&asset_id)
            .map(|data| data.len())
            .unwrap_or(0)
    }

    /// Get number of assets
    pub fn asset_count(&self) -> usize {
        self.data.len()
    }

    /// Load sessions from existing data
    pub fn compute_sessions(&mut self) {
        let mut sessions: Vec<SessionLabel> = Vec::new();

        for asset_data in self.data.values() {
            for dt in asset_data.keys() {
                let session = SessionLabel::from_datetime(*dt);
                if !sessions.contains(&session) {
                    sessions.push(session);
                }
            }
        }

        sessions.sort_by_key(|s| (s.year, s.month, s.day));
        self.sessions = sessions;
    }
}

impl Default for DailyBarReader {
    fn default() -> Self {
        Self::new()
    }
}

impl BarReader for DailyBarReader {
    fn get_bar(&self, asset: &Asset, dt: DateTime<Utc>) -> Result<Bar> {
        self.data
            .get(&asset.id)
            .and_then(|data| data.get(&dt).copied())
            .ok_or_else(|| {
                ZiplineError::DataNotFound(format!(
                    "No bar data for asset {} at {:?}",
                    asset.symbol, dt
                ))
            })
    }

    fn get_bars(&self, asset: &Asset, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Bar>> {
        let asset_data = self
            .data
            .get(&asset.id)
            .ok_or_else(|| ZiplineError::AssetNotFound(asset.id))?;

        let mut bars: Vec<Bar> = asset_data
            .iter()
            .filter(|(dt, _)| **dt >= start && **dt <= end)
            .map(|(_, bar)| *bar)
            .collect();

        bars.sort_by_key(|b| b.dt);

        Ok(bars)
    }

    fn last_available_dt(&self, asset: &Asset) -> Result<DateTime<Utc>> {
        self.data
            .get(&asset.id)
            .and_then(|data| data.keys().max().copied())
            .ok_or_else(|| ZiplineError::AssetNotFound(asset.id))
    }

    fn first_available_dt(&self, asset: &Asset) -> Result<DateTime<Utc>> {
        self.data
            .get(&asset.id)
            .and_then(|data| data.keys().min().copied())
            .ok_or_else(|| ZiplineError::AssetNotFound(asset.id))
    }

    fn sessions(&self) -> Result<Vec<SessionLabel>> {
        Ok(self.sessions.clone())
    }
}

/// Minute bar reader for intraday data
#[derive(Debug, Clone)]
pub struct MinuteBarReader {
    /// Data storage: asset_id -> (datetime -> bar)
    data: HashMap<u64, HashMap<DateTime<Utc>, Bar>>,
    /// Trading sessions
    sessions: Vec<SessionLabel>,
    /// Minutes per session (e.g., 390 for US equities)
    minutes_per_session: usize,
}

impl MinuteBarReader {
    /// Create new minute bar reader
    pub fn new(minutes_per_session: usize) -> Self {
        Self {
            data: HashMap::new(),
            sessions: Vec::new(),
            minutes_per_session,
        }
    }

    /// Standard US equity market (390 minutes = 6.5 hours)
    pub fn us_equity() -> Self {
        Self::new(390)
    }

    /// Load minute data from memory
    pub fn load_from_memory(&mut self, asset_id: u64, bars: Vec<Bar>) -> Result<()> {
        let asset_data = self.data.entry(asset_id).or_insert_with(HashMap::new);

        for bar in bars {
            if !bar.is_valid() {
                return Err(ZiplineError::InvalidData(format!(
                    "Invalid bar data for asset {} at {:?}",
                    asset_id, bar.dt
                )));
            }

            // Update sessions
            let session = SessionLabel::from_datetime(bar.dt);
            if !self.sessions.contains(&session) {
                self.sessions.push(session);
            }

            asset_data.insert(bar.dt, bar);
        }

        // Sort sessions
        self.sessions.sort_by_key(|s| (s.year, s.month, s.day));

        Ok(())
    }

    /// Get all bars for a session
    pub fn get_session_bars(&self, asset: &Asset, session: SessionLabel) -> Result<Vec<Bar>> {
        let session_dt = session.to_datetime()?;
        let next_session = session_dt + chrono::Duration::days(1);

        self.get_bars(asset, session_dt, next_session)
    }

    /// Get bar count for an asset
    pub fn bar_count(&self, asset_id: u64) -> usize {
        self.data
            .get(&asset_id)
            .map(|data| data.len())
            .unwrap_or(0)
    }

    /// Get number of assets
    pub fn asset_count(&self) -> usize {
        self.data.len()
    }

    /// Expected bars per session
    pub fn minutes_per_session(&self) -> usize {
        self.minutes_per_session
    }
}

impl BarReader for MinuteBarReader {
    fn get_bar(&self, asset: &Asset, dt: DateTime<Utc>) -> Result<Bar> {
        self.data
            .get(&asset.id)
            .and_then(|data| data.get(&dt).copied())
            .ok_or_else(|| {
                ZiplineError::DataNotFound(format!(
                    "No bar data for asset {} at {:?}",
                    asset.symbol, dt
                ))
            })
    }

    fn get_bars(&self, asset: &Asset, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Bar>> {
        let asset_data = self
            .data
            .get(&asset.id)
            .ok_or_else(|| ZiplineError::AssetNotFound(asset.id))?;

        let mut bars: Vec<Bar> = asset_data
            .iter()
            .filter(|(dt, _)| **dt >= start && **dt <= end)
            .map(|(_, bar)| *bar)
            .collect();

        bars.sort_by_key(|b| b.dt);

        Ok(bars)
    }

    fn last_available_dt(&self, asset: &Asset) -> Result<DateTime<Utc>> {
        self.data
            .get(&asset.id)
            .and_then(|data| data.keys().max().copied())
            .ok_or_else(|| ZiplineError::AssetNotFound(asset.id))
    }

    fn first_available_dt(&self, asset: &Asset) -> Result<DateTime<Utc>> {
        self.data
            .get(&asset.id)
            .and_then(|data| data.keys().min().copied())
            .ok_or_else(|| ZiplineError::AssetNotFound(asset.id))
    }

    fn sessions(&self) -> Result<Vec<SessionLabel>> {
        Ok(self.sessions.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_asset() -> Asset {
        Asset {
            id: 1,
            symbol: "AAPL".to_string(),
            exchange: "NYSE".to_string(),
            asset_type: crate::asset::AssetType::Equity,
        }
    }

    fn create_test_bar(dt: DateTime<Utc>) -> Bar {
        Bar::new(100.0, 105.0, 99.0, 102.0, 1000000.0, dt)
    }

    #[test]
    fn test_bar_creation() {
        let dt = Utc::now();
        let bar = Bar::new(100.0, 105.0, 99.0, 102.0, 1000000.0, dt);

        assert_eq!(bar.open, 100.0);
        assert_eq!(bar.high, 105.0);
        assert_eq!(bar.low, 99.0);
        assert_eq!(bar.close, 102.0);
        assert_eq!(bar.volume, 1000000.0);
    }

    #[test]
    fn test_bar_typical_price() {
        let dt = Utc::now();
        let bar = Bar::new(100.0, 105.0, 99.0, 102.0, 1000000.0, dt);

        let typical = bar.typical_price();
        assert_eq!(typical, (105.0 + 99.0 + 102.0) / 3.0);
    }

    #[test]
    fn test_bar_true_range() {
        let dt = Utc::now();
        let bar = Bar::new(100.0, 105.0, 99.0, 102.0, 1000000.0, dt);

        // Without previous close
        let tr = bar.true_range(None);
        assert_eq!(tr, 105.0 - 99.0);

        // With previous close
        let tr_with_prev = bar.true_range(Some(110.0));
        assert!(tr_with_prev > tr);
    }

    #[test]
    fn test_bar_validation() {
        let dt = Utc::now();

        // Valid bar
        let valid_bar = Bar::new(100.0, 105.0, 99.0, 102.0, 1000000.0, dt);
        assert!(valid_bar.is_valid());

        // Invalid bar (high < low)
        let invalid_bar = Bar::new(100.0, 99.0, 105.0, 102.0, 1000000.0, dt);
        assert!(!invalid_bar.is_valid());

        // Invalid bar (negative volume)
        let invalid_volume = Bar::new(100.0, 105.0, 99.0, 102.0, -1000.0, dt);
        assert!(!invalid_volume.is_valid());
    }

    #[test]
    fn test_session_label() {
        let dt = Utc::now();
        let session = SessionLabel::from_datetime(dt);

        assert_eq!(session.year, dt.year());
        assert_eq!(session.month, dt.month());
        assert_eq!(session.day, dt.day());
    }

    #[test]
    fn test_daily_bar_reader() {
        let mut reader = DailyBarReader::new();
        let asset = create_test_asset();

        let dt1 = Utc::now();
        let dt2 = dt1 + chrono::Duration::days(1);

        let bars = vec![create_test_bar(dt1), create_test_bar(dt2)];

        reader.load_from_memory(asset.id, bars).unwrap();

        // Test get_bar
        let bar = reader.get_bar(&asset, dt1).unwrap();
        assert_eq!(bar.open, 100.0);

        // Test get_bars
        let range_bars = reader.get_bars(&asset, dt1, dt2).unwrap();
        assert_eq!(range_bars.len(), 2);

        // Test first/last available
        assert_eq!(reader.first_available_dt(&asset).unwrap(), dt1);
        assert_eq!(reader.last_available_dt(&asset).unwrap(), dt2);
    }

    #[test]
    fn test_minute_bar_reader() {
        let mut reader = MinuteBarReader::us_equity();
        let asset = create_test_asset();

        let dt1 = Utc::now();
        let dt2 = dt1 + chrono::Duration::minutes(1);
        let dt3 = dt1 + chrono::Duration::minutes(2);

        let bars = vec![
            create_test_bar(dt1),
            create_test_bar(dt2),
            create_test_bar(dt3),
        ];

        reader.load_from_memory(asset.id, bars).unwrap();

        // Test get_bar
        let bar = reader.get_bar(&asset, dt1).unwrap();
        assert_eq!(bar.open, 100.0);

        // Test get_bars
        let range_bars = reader.get_bars(&asset, dt1, dt3).unwrap();
        assert_eq!(range_bars.len(), 3);

        assert_eq!(reader.minutes_per_session(), 390);
    }

    #[test]
    fn test_sessions() {
        let mut reader = DailyBarReader::new();

        let dt1 = Utc::now();
        let dt2 = dt1 + chrono::Duration::days(1);

        let bars = vec![create_test_bar(dt1), create_test_bar(dt2)];

        reader.load_from_memory(1, bars).unwrap();

        let sessions = reader.sessions().unwrap();
        assert_eq!(sessions.len(), 2);
    }
}
