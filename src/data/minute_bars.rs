//! Minute-resolution bar data storage and retrieval

use crate::error::{Result, ZiplineError};
use crate::types::Bar;
use chrono::{DateTime, Timelike, Utc};
use hashbrown::HashMap;
use std::sync::Arc;

/// Minute bar reader for intraday data
#[derive(Debug, Clone)]
pub struct MinuteBarReader {
    /// Asset ID to minute bars mapping
    data: Arc<HashMap<u64, Vec<MinuteBar>>>,
    /// Trading calendar for session awareness
    market_open_hour: u32,
    market_close_hour: u32,
}

/// Minute-resolution OHLCV bar
#[derive(Debug, Clone)]
pub struct MinuteBar {
    pub timestamp: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl MinuteBar {
    /// Create a new minute bar
    pub fn new(
        timestamp: DateTime<Utc>,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
    ) -> Self {
        Self {
            timestamp,
            open,
            high,
            low,
            close,
            volume,
        }
    }

    /// Convert to generic Bar type
    pub fn to_bar(&self) -> Bar {
        Bar {
            timestamp: self.timestamp,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
        }
    }
}

impl MinuteBarReader {
    /// Create new minute bar reader with data
    pub fn new(data: HashMap<u64, Vec<MinuteBar>>) -> Self {
        Self {
            data: Arc::new(data),
            market_open_hour: 9, // 9:30 AM (handle minutes separately)
            market_close_hour: 16, // 4:00 PM
        }
    }

    /// Create empty minute bar reader
    pub fn empty() -> Self {
        Self::new(HashMap::new())
    }

    /// Get minute bars for an asset within a time range
    pub fn get_bars(
        &self,
        asset_id: u64,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<MinuteBar>> {
        let bars = self
            .data
            .get(&asset_id)
            .ok_or_else(|| ZiplineError::AssetNotFound(asset_id))?;

        let filtered: Vec<MinuteBar> = bars
            .iter()
            .filter(|bar| bar.timestamp >= start && bar.timestamp <= end)
            .cloned()
            .collect();

        Ok(filtered)
    }

    /// Get the most recent minute bar before or at the given timestamp
    pub fn get_latest_bar(&self, asset_id: u64, timestamp: DateTime<Utc>) -> Result<MinuteBar> {
        let bars = self
            .data
            .get(&asset_id)
            .ok_or_else(|| ZiplineError::AssetNotFound(asset_id))?;

        bars.iter()
            .rev()
            .find(|bar| bar.timestamp <= timestamp)
            .cloned()
            .ok_or_else(|| ZiplineError::NoDataAvailable)
    }

    /// Get minute bars for multiple assets
    pub fn get_bars_multi(
        &self,
        asset_ids: &[u64],
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<HashMap<u64, Vec<MinuteBar>>> {
        let mut result = HashMap::new();

        for &asset_id in asset_ids {
            if let Ok(bars) = self.get_bars(asset_id, start, end) {
                result.insert(asset_id, bars);
            }
        }

        Ok(result)
    }

    /// Check if we have data for an asset
    pub fn has_data(&self, asset_id: u64) -> bool {
        self.data.contains_key(&asset_id)
    }

    /// Get the first available timestamp for an asset
    pub fn first_timestamp(&self, asset_id: u64) -> Option<DateTime<Utc>> {
        self.data
            .get(&asset_id)
            .and_then(|bars| bars.first())
            .map(|bar| bar.timestamp)
    }

    /// Get the last available timestamp for an asset
    pub fn last_timestamp(&self, asset_id: u64) -> Option<DateTime<Utc>> {
        self.data
            .get(&asset_id)
            .and_then(|bars| bars.last())
            .map(|bar| bar.timestamp)
    }

    /// Get number of available bars for an asset
    pub fn bar_count(&self, asset_id: u64) -> usize {
        self.data.get(&asset_id).map(|bars| bars.len()).unwrap_or(0)
    }

    /// Check if timestamp is within market hours
    pub fn is_market_hours(&self, timestamp: DateTime<Utc>) -> bool {
        let hour = timestamp.hour();
        let minute = timestamp.minute();

        // Market open: 9:30 AM
        let after_open = hour > self.market_open_hour
            || (hour == self.market_open_hour && minute >= 30);

        // Market close: 4:00 PM
        let before_close = hour < self.market_close_hour;

        after_open && before_close
    }

    /// Filter bars to only include market hours
    pub fn filter_market_hours(&self, bars: Vec<MinuteBar>) -> Vec<MinuteBar> {
        bars.into_iter()
            .filter(|bar| self.is_market_hours(bar.timestamp))
            .collect()
    }

    /// Aggregate minute bars to a higher frequency
    pub fn aggregate_to_daily(&self, minute_bars: &[MinuteBar]) -> Option<Bar> {
        if minute_bars.is_empty() {
            return None;
        }

        let open = minute_bars.first()?.open;
        let close = minute_bars.last()?.close;
        let high = minute_bars
            .iter()
            .map(|b| b.high)
            .fold(f64::NEG_INFINITY, f64::max);
        let low = minute_bars
            .iter()
            .map(|b| b.low)
            .fold(f64::INFINITY, f64::min);
        let volume: f64 = minute_bars.iter().map(|b| b.volume).sum();
        let timestamp = minute_bars.first()?.timestamp;

        Some(Bar {
            timestamp,
            open,
            high,
            low,
            close,
            volume,
        })
    }
}

/// Builder for creating minute bar data
pub struct MinuteBarBuilder {
    data: HashMap<u64, Vec<MinuteBar>>,
}

impl MinuteBarBuilder {
    /// Create new builder
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Add a single minute bar
    pub fn add_bar(&mut self, asset_id: u64, bar: MinuteBar) -> &mut Self {
        self.data.entry(asset_id).or_insert_with(Vec::new).push(bar);
        self
    }

    /// Add multiple bars for an asset
    pub fn add_bars(&mut self, asset_id: u64, mut bars: Vec<MinuteBar>) -> &mut Self {
        self.data
            .entry(asset_id)
            .or_insert_with(Vec::new)
            .append(&mut bars);
        self
    }

    /// Sort all bars by timestamp
    pub fn sort(&mut self) -> &mut Self {
        for bars in self.data.values_mut() {
            bars.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        }
        self
    }

    /// Build the minute bar reader
    pub fn build(self) -> MinuteBarReader {
        MinuteBarReader::new(self.data)
    }
}

impl Default for MinuteBarBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn create_test_bars() -> Vec<MinuteBar> {
        vec![
            MinuteBar::new(
                Utc.with_ymd_and_hms(2025, 1, 15, 9, 30, 0).unwrap(),
                100.0,
                101.0,
                99.5,
                100.5,
                1000.0,
            ),
            MinuteBar::new(
                Utc.with_ymd_and_hms(2025, 1, 15, 9, 31, 0).unwrap(),
                100.5,
                102.0,
                100.0,
                101.5,
                1500.0,
            ),
            MinuteBar::new(
                Utc.with_ymd_and_hms(2025, 1, 15, 9, 32, 0).unwrap(),
                101.5,
                103.0,
                101.0,
                102.0,
                2000.0,
            ),
        ]
    }

    #[test]
    fn test_minute_bar_creation() {
        let bar = MinuteBar::new(
            Utc.with_ymd_and_hms(2025, 1, 15, 10, 30, 0).unwrap(),
            100.0,
            101.0,
            99.0,
            100.5,
            1000.0,
        );

        assert_eq!(bar.open, 100.0);
        assert_eq!(bar.high, 101.0);
        assert_eq!(bar.low, 99.0);
        assert_eq!(bar.close, 100.5);
        assert_eq!(bar.volume, 1000.0);
    }

    #[test]
    fn test_minute_bar_reader_creation() {
        let mut data = HashMap::new();
        data.insert(1, create_test_bars());

        let reader = MinuteBarReader::new(data);
        assert!(reader.has_data(1));
        assert!(!reader.has_data(2));
    }

    #[test]
    fn test_get_bars() {
        let mut data = HashMap::new();
        data.insert(1, create_test_bars());
        let reader = MinuteBarReader::new(data);

        let start = Utc.with_ymd_and_hms(2025, 1, 15, 9, 30, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2025, 1, 15, 9, 32, 0).unwrap();

        let bars = reader.get_bars(1, start, end).unwrap();
        assert_eq!(bars.len(), 3);
    }

    #[test]
    fn test_get_latest_bar() {
        let mut data = HashMap::new();
        data.insert(1, create_test_bars());
        let reader = MinuteBarReader::new(data);

        let timestamp = Utc.with_ymd_and_hms(2025, 1, 15, 9, 31, 30).unwrap();
        let bar = reader.get_latest_bar(1, timestamp).unwrap();

        assert_eq!(bar.close, 101.5); // Should get the 9:31 bar
    }

    #[test]
    fn test_is_market_hours() {
        let reader = MinuteBarReader::empty();

        // Market hours (9:30 AM - 4:00 PM)
        assert!(reader.is_market_hours(Utc.with_ymd_and_hms(2025, 1, 15, 9, 30, 0).unwrap()));
        assert!(reader.is_market_hours(Utc.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap()));
        assert!(reader.is_market_hours(Utc.with_ymd_and_hms(2025, 1, 15, 15, 59, 0).unwrap()));

        // Outside market hours
        assert!(!reader.is_market_hours(Utc.with_ymd_and_hms(2025, 1, 15, 9, 29, 0).unwrap()));
        assert!(!reader.is_market_hours(Utc.with_ymd_and_hms(2025, 1, 15, 16, 0, 0).unwrap()));
        assert!(!reader.is_market_hours(Utc.with_ymd_and_hms(2025, 1, 15, 8, 0, 0).unwrap()));
    }

    #[test]
    fn test_aggregate_to_daily() {
        let reader = MinuteBarReader::empty();
        let minute_bars = create_test_bars();

        let daily_bar = reader.aggregate_to_daily(&minute_bars).unwrap();

        assert_eq!(daily_bar.open, 100.0); // First bar's open
        assert_eq!(daily_bar.close, 102.0); // Last bar's close
        assert_eq!(daily_bar.high, 103.0); // Highest high
        assert_eq!(daily_bar.low, 99.5); // Lowest low
        assert_eq!(daily_bar.volume, 4500.0); // Sum of volumes
    }

    #[test]
    fn test_minute_bar_builder() {
        let mut builder = MinuteBarBuilder::new();
        let bars = create_test_bars();

        builder.add_bars(1, bars).sort();

        let reader = builder.build();
        assert_eq!(reader.bar_count(1), 3);
    }

    #[test]
    fn test_bar_count() {
        let mut data = HashMap::new();
        data.insert(1, create_test_bars());
        let reader = MinuteBarReader::new(data);

        assert_eq!(reader.bar_count(1), 3);
        assert_eq!(reader.bar_count(999), 0);
    }

    #[test]
    fn test_first_last_timestamp() {
        let mut data = HashMap::new();
        data.insert(1, create_test_bars());
        let reader = MinuteBarReader::new(data);

        let first = reader.first_timestamp(1).unwrap();
        let last = reader.last_timestamp(1).unwrap();

        assert_eq!(first, Utc.with_ymd_and_hms(2025, 1, 15, 9, 30, 0).unwrap());
        assert_eq!(last, Utc.with_ymd_and_hms(2025, 1, 15, 9, 32, 0).unwrap());
    }
}
