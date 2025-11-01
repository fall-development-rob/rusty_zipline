//! Multi-frequency data dispatch reader

use crate::data::frequency::DataFrequency;
use crate::data::minute_bars::{MinuteBarReader, MinuteBar};
use crate::error::{Result, ZiplineError};
use crate::types::Bar;
use chrono::{DateTime, Utc};
use hashbrown::HashMap;

/// Dispatch bar reader that routes requests to appropriate frequency reader
pub struct DispatchBarReader {
    /// Minute bar reader
    minute_reader: Option<MinuteBarReader>,
    /// Daily bar reader (using generic Bar type for now)
    daily_bars: HashMap<u64, Vec<Bar>>,
    /// Default frequency
    default_frequency: DataFrequency,
}

impl DispatchBarReader {
    /// Create new dispatch reader
    pub fn new(default_frequency: DataFrequency) -> Self {
        Self {
            minute_reader: None,
            daily_bars: HashMap::new(),
            default_frequency,
        }
    }

    /// Set minute bar reader
    pub fn with_minute_reader(mut self, reader: MinuteBarReader) -> Self {
        self.minute_reader = Some(reader);
        self
    }

    /// Set daily bars
    pub fn with_daily_bars(mut self, bars: HashMap<u64, Vec<Bar>>) -> Self {
        self.daily_bars = bars;
        self
    }

    /// Get bars at specified frequency
    pub fn get_bars(
        &self,
        asset_id: u64,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        frequency: DataFrequency,
    ) -> Result<Vec<Bar>> {
        match frequency {
            DataFrequency::Minute => {
                let reader = self.minute_reader.as_ref()
                    .ok_or_else(|| ZiplineError::DataError("Minute data not available".to_string()))?;

                let minute_bars = reader.get_bars(asset_id, start, end)?;
                Ok(minute_bars.iter().map(|mb| mb.to_bar()).collect())
            }
            DataFrequency::Daily => {
                self.get_daily_bars(asset_id, start, end)
            }
            DataFrequency::Second => {
                Err(ZiplineError::DataError("Second frequency not yet implemented".to_string()))
            }
        }
    }

    /// Get daily bars
    fn get_daily_bars(
        &self,
        asset_id: u64,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Bar>> {
        let bars = self.daily_bars.get(&asset_id)
            .ok_or_else(|| ZiplineError::AssetNotFound(asset_id))?;

        let filtered: Vec<Bar> = bars.iter()
            .filter(|bar| bar.timestamp >= start && bar.timestamp <= end)
            .cloned()
            .collect();

        Ok(filtered)
    }

    /// Get the latest bar for an asset
    pub fn get_latest_bar(
        &self,
        asset_id: u64,
        timestamp: DateTime<Utc>,
        frequency: DataFrequency,
    ) -> Result<Bar> {
        match frequency {
            DataFrequency::Minute => {
                let reader = self.minute_reader.as_ref()
                    .ok_or_else(|| ZiplineError::DataError("Minute data not available".to_string()))?;

                let minute_bar = reader.get_latest_bar(asset_id, timestamp)?;
                Ok(minute_bar.to_bar())
            }
            DataFrequency::Daily => {
                let bars = self.daily_bars.get(&asset_id)
                    .ok_or_else(|| ZiplineError::AssetNotFound(asset_id))?;

                bars.iter()
                    .rev()
                    .find(|bar| bar.timestamp <= timestamp)
                    .cloned()
                    .ok_or_else(|| ZiplineError::NoDataAvailable)
            }
            DataFrequency::Second => {
                Err(ZiplineError::DataError("Second frequency not yet implemented".to_string()))
            }
        }
    }

    /// Check if data is available for an asset at given frequency
    pub fn has_data(&self, asset_id: u64, frequency: DataFrequency) -> bool {
        match frequency {
            DataFrequency::Minute => {
                self.minute_reader
                    .as_ref()
                    .map(|reader| reader.has_data(asset_id))
                    .unwrap_or(false)
            }
            DataFrequency::Daily => {
                self.daily_bars.contains_key(&asset_id)
            }
            DataFrequency::Second => false,
        }
    }

    /// Get available frequencies for an asset
    pub fn available_frequencies(&self, asset_id: u64) -> Vec<DataFrequency> {
        let mut frequencies = Vec::new();

        if self.has_data(asset_id, DataFrequency::Daily) {
            frequencies.push(DataFrequency::Daily);
        }

        if self.has_data(asset_id, DataFrequency::Minute) {
            frequencies.push(DataFrequency::Minute);
        }

        frequencies
    }

    /// Convert minute bars to daily bars
    pub fn minute_to_daily(
        &self,
        asset_id: u64,
        date: DateTime<Utc>,
    ) -> Result<Bar> {
        let reader = self.minute_reader.as_ref()
            .ok_or_else(|| ZiplineError::DataError("Minute data not available".to_string()))?;

        // Get all minute bars for the day
        let start = date.date_naive().and_hms_opt(0, 0, 0)
            .map(|dt| DateTime::from_naive_utc_and_offset(dt, Utc))
            .unwrap_or(date);

        let end = date.date_naive().and_hms_opt(23, 59, 59)
            .map(|dt| DateTime::from_naive_utc_and_offset(dt, Utc))
            .unwrap_or(date);

        let minute_bars = reader.get_bars(asset_id, start, end)?;

        reader.aggregate_to_daily(&minute_bars)
            .ok_or_else(|| ZiplineError::DataError("Failed to aggregate minute bars".to_string()))
    }

    /// Get the default frequency
    pub fn default_frequency(&self) -> DataFrequency {
        self.default_frequency
    }

    /// Set the default frequency
    pub fn set_default_frequency(&mut self, frequency: DataFrequency) {
        self.default_frequency = frequency;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::minute_bars::MinuteBarBuilder;
    use chrono::TimeZone;

    fn create_test_minute_bars() -> Vec<MinuteBar> {
        vec![
            MinuteBar::new(
                Utc.with_ymd_and_hms(2025, 1, 15, 9, 30, 0).unwrap(),
                100.0, 101.0, 99.5, 100.5, 1000.0,
            ),
            MinuteBar::new(
                Utc.with_ymd_and_hms(2025, 1, 15, 9, 31, 0).unwrap(),
                100.5, 102.0, 100.0, 101.5, 1500.0,
            ),
        ]
    }

    fn create_test_daily_bars() -> Vec<Bar> {
        vec![
            Bar {
                timestamp: Utc.with_ymd_and_hms(2025, 1, 15, 0, 0, 0).unwrap(),
                open: 100.0,
                high: 105.0,
                low: 98.0,
                close: 103.0,
                volume: 10000.0,
            },
        ]
    }

    #[test]
    fn test_dispatch_reader_creation() {
        let dispatch = DispatchBarReader::new(DataFrequency::Daily);
        assert_eq!(dispatch.default_frequency(), DataFrequency::Daily);
    }

    #[test]
    fn test_with_minute_reader() {
        let mut builder = MinuteBarBuilder::new();
        builder.add_bars(1, create_test_minute_bars());
        let minute_reader = builder.build();

        let dispatch = DispatchBarReader::new(DataFrequency::Minute)
            .with_minute_reader(minute_reader);

        assert!(dispatch.has_data(1, DataFrequency::Minute));
    }

    #[test]
    fn test_with_daily_bars() {
        let mut daily_data = HashMap::new();
        daily_data.insert(1, create_test_daily_bars());

        let dispatch = DispatchBarReader::new(DataFrequency::Daily)
            .with_daily_bars(daily_data);

        assert!(dispatch.has_data(1, DataFrequency::Daily));
    }

    #[test]
    fn test_get_daily_bars() {
        let mut daily_data = HashMap::new();
        daily_data.insert(1, create_test_daily_bars());

        let dispatch = DispatchBarReader::new(DataFrequency::Daily)
            .with_daily_bars(daily_data);

        let start = Utc.with_ymd_and_hms(2025, 1, 14, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2025, 1, 16, 0, 0, 0).unwrap();

        let bars = dispatch.get_bars(1, start, end, DataFrequency::Daily).unwrap();
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].close, 103.0);
    }

    #[test]
    fn test_get_minute_bars() {
        let mut builder = MinuteBarBuilder::new();
        builder.add_bars(1, create_test_minute_bars());
        let minute_reader = builder.build();

        let dispatch = DispatchBarReader::new(DataFrequency::Minute)
            .with_minute_reader(minute_reader);

        let start = Utc.with_ymd_and_hms(2025, 1, 15, 9, 30, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2025, 1, 15, 9, 31, 0).unwrap();

        let bars = dispatch.get_bars(1, start, end, DataFrequency::Minute).unwrap();
        assert_eq!(bars.len(), 2);
    }

    #[test]
    fn test_available_frequencies() {
        let mut builder = MinuteBarBuilder::new();
        builder.add_bars(1, create_test_minute_bars());
        let minute_reader = builder.build();

        let mut daily_data = HashMap::new();
        daily_data.insert(1, create_test_daily_bars());

        let dispatch = DispatchBarReader::new(DataFrequency::Daily)
            .with_minute_reader(minute_reader)
            .with_daily_bars(daily_data);

        let frequencies = dispatch.available_frequencies(1);
        assert_eq!(frequencies.len(), 2);
        assert!(frequencies.contains(&DataFrequency::Daily));
        assert!(frequencies.contains(&DataFrequency::Minute));
    }

    #[test]
    fn test_minute_to_daily_aggregation() {
        let mut builder = MinuteBarBuilder::new();
        builder.add_bars(1, create_test_minute_bars());
        let minute_reader = builder.build();

        let dispatch = DispatchBarReader::new(DataFrequency::Minute)
            .with_minute_reader(minute_reader);

        let date = Utc.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap();
        let daily_bar = dispatch.minute_to_daily(1, date).unwrap();

        assert_eq!(daily_bar.open, 100.0);
        assert_eq!(daily_bar.close, 101.5);
        assert_eq!(daily_bar.high, 102.0);
        assert_eq!(daily_bar.low, 99.5);
    }
}
