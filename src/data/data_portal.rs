//! Data Portal - Unified data access interface
//!
//! The DataPortal provides a single interface for accessing all market data,
//! handling multi-frequency data, adjustments, and current/historical queries.

use crate::asset::Asset;
use crate::data::adjustments::{Adjustment, AdjustmentReader};
use crate::data::frequency::DataFrequency;
use crate::error::{Result, ZiplineError};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;

/// Represents a single bar of OHLCV data
#[derive(Debug, Clone)]
pub struct Bar {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// DataFrame-like structure for historical data
#[derive(Debug, Clone)]
pub struct DataFrame {
    pub data: HashMap<u64, Vec<f64>>, // asset_id -> values
    pub index: Vec<DateTime<Utc>>,    // timestamps
    pub columns: Vec<String>,         // field names
}

impl DataFrame {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            index: Vec::new(),
            columns: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}

impl Default for DataFrame {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for reading bar data (daily or minute)
pub trait BarReader: Send + Sync {
    fn get_value(
        &self,
        asset_id: u64,
        dt: DateTime<Utc>,
        field: &str,
    ) -> Result<Option<f64>>;

    fn get_last_traded_dt(&self, asset_id: u64, dt: DateTime<Utc>) -> Result<Option<DateTime<Utc>>>;

    fn get_bars(&self, asset_id: u64, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Bar>>;
}

/// Data Portal - Unified data access
///
/// Provides a single interface for:
/// - Current data (spot prices, latest values)
/// - Historical data (windowed queries)
/// - Adjustments (splits, dividends)
/// - Multi-frequency data (minute and daily)
pub struct DataPortal {
    /// Daily bar reader
    daily_reader: Option<Arc<dyn BarReader>>,

    /// Minute bar reader (optional)
    minute_reader: Option<Arc<dyn BarReader>>,

    /// Adjustment reader
    adjustment_reader: Option<Arc<AdjustmentReader>>,

    /// Default frequency
    default_frequency: DataFrequency,

    /// Trading calendar (for business days)
    trading_days: Vec<DateTime<Utc>>,
}

impl std::fmt::Debug for DataPortal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataPortal")
            .field("daily_reader", &if self.daily_reader.is_some() { "<Some(BarReader)>" } else { "None" })
            .field("minute_reader", &if self.minute_reader.is_some() { "<Some(BarReader)>" } else { "None" })
            .field("adjustment_reader", &if self.adjustment_reader.is_some() { "<Some(AdjustmentReader)>" } else { "None" })
            .field("default_frequency", &self.default_frequency)
            .field("trading_days", &format!("{} days", self.trading_days.len()))
            .finish()
    }
}

impl DataPortal {
    /// Create a new DataPortal
    pub fn new(
        daily_reader: Option<Arc<dyn BarReader>>,
        minute_reader: Option<Arc<dyn BarReader>>,
        adjustment_reader: Option<Arc<AdjustmentReader>>,
        trading_days: Vec<DateTime<Utc>>,
    ) -> Self {
        Self {
            daily_reader,
            minute_reader,
            adjustment_reader,
            default_frequency: DataFrequency::Daily,
            trading_days,
        }
    }

    /// Get current value for a single asset and field
    ///
    /// # Arguments
    /// * `asset` - Asset to query
    /// * `field` - Field name ("open", "high", "low", "close", "volume", "price")
    /// * `dt` - Current datetime
    pub fn current_value(
        &self,
        asset: &Asset,
        field: &str,
        dt: DateTime<Utc>,
    ) -> Result<Option<f64>> {
        // Map "price" to "close"
        let actual_field = if field == "price" { "close" } else { field };

        // Try minute reader first if available
        if let Some(minute_reader) = &self.minute_reader {
            if let Ok(Some(value)) = minute_reader.get_value(asset.id, dt, actual_field) {
                return Ok(Some(value));
            }
        }

        // Fall back to daily reader
        if let Some(daily_reader) = &self.daily_reader {
            return daily_reader.get_value(asset.id, dt, actual_field);
        }

        Err(ZiplineError::NoDataAvailable)
    }

    /// Get current values for multiple assets and fields
    ///
    /// # Arguments
    /// * `assets` - Assets to query
    /// * `fields` - Field names
    /// * `dt` - Current datetime
    ///
    /// # Returns
    /// HashMap<AssetId, HashMap<Field, Value>>
    pub fn current(
        &self,
        assets: &[Asset],
        fields: &[&str],
        dt: DateTime<Utc>,
    ) -> Result<HashMap<u64, HashMap<String, f64>>> {
        let mut result = HashMap::new();

        for asset in assets {
            let mut asset_data = HashMap::new();

            for &field in fields {
                if let Ok(Some(value)) = self.current_value(asset, field, dt) {
                    asset_data.insert(field.to_string(), value);
                }
            }

            if !asset_data.is_empty() {
                result.insert(asset.id, asset_data);
            }
        }

        Ok(result)
    }

    /// Get historical data for assets
    ///
    /// # Arguments
    /// * `assets` - Assets to query
    /// * `fields` - Field names
    /// * `bar_count` - Number of bars to retrieve
    /// * `frequency` - Data frequency (minute or daily)
    /// * `dt` - End datetime
    pub fn history(
        &self,
        assets: &[Asset],
        fields: &[&str],
        bar_count: usize,
        frequency: DataFrequency,
        dt: DateTime<Utc>,
    ) -> Result<HashMap<String, DataFrame>> {
        let mut result = HashMap::new();

        // Get the appropriate reader
        let reader = match frequency {
            DataFrequency::Minute => self.minute_reader.as_ref(),
            DataFrequency::Daily => self.daily_reader.as_ref(),
            DataFrequency::Second => {
                return Err(ZiplineError::UnsupportedFrequency {
                    frequency: "second".to_string(),
                    supported: vec!["daily".to_string(), "minute".to_string()],
                })
            }
        };

        let reader = reader.ok_or_else(|| {
            let asset_ids: Vec<u64> = assets.iter().map(|a| a.id).collect();
            ZiplineError::PricingDataNotLoaded { assets: asset_ids }
        })?;

        // Calculate start datetime
        let start = self.get_history_start(dt, bar_count, frequency)?;

        for &field in fields {
            let mut df = DataFrame::new();
            df.columns = vec![field.to_string()];

            for asset in assets {
                // Get bars for this asset
                let bars = reader.get_bars(asset.id, start, dt)?;

                // Extract field values
                let values: Vec<f64> = bars
                    .iter()
                    .map(|bar| match field {
                        "open" => bar.open,
                        "high" => bar.high,
                        "low" => bar.low,
                        "close" | "price" => bar.close,
                        "volume" => bar.volume,
                        _ => 0.0,
                    })
                    .collect();

                df.data.insert(asset.id, values);
            }

            // Use first asset's bar timestamps as index
            if let Some(first_asset) = assets.first() {
                let bars = reader.get_bars(first_asset.id, start, dt)?;
                // Timestamps would come from bars - simplified here
                df.index = vec![dt; bars.len()];
            }

            result.insert(field.to_string(), df);
        }

        Ok(result)
    }

    /// Get spot value (single point-in-time lookup)
    pub fn get_spot_value(
        &self,
        asset: &Asset,
        field: &str,
        dt: DateTime<Utc>,
    ) -> Result<Option<f64>> {
        self.current_value(asset, field, dt)
    }

    /// Check if asset can be traded at this time
    pub fn can_trade(&self, asset: &Asset, dt: DateTime<Utc>) -> bool {
        // Check if there's recent data available
        if let Ok(Some(_last_traded)) = self.get_last_traded_dt(asset, dt) {
            // Could add more sophisticated staleness checks here
            return true;
        }
        false
    }

    /// Check if asset data is stale
    pub fn is_stale(&self, asset: &Asset, dt: DateTime<Utc>) -> bool {
        !self.can_trade(asset, dt)
    }

    /// Get last traded datetime for asset
    pub fn get_last_traded_dt(
        &self,
        asset: &Asset,
        dt: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>> {
        // Try minute reader first
        if let Some(minute_reader) = &self.minute_reader {
            if let Ok(last_dt) = minute_reader.get_last_traded_dt(asset.id, dt) {
                return Ok(last_dt);
            }
        }

        // Fall back to daily reader
        if let Some(daily_reader) = &self.daily_reader {
            return daily_reader.get_last_traded_dt(asset.id, dt);
        }

        Ok(None)
    }

    /// Get adjustments for assets in date range
    pub fn get_adjustments(
        &self,
        assets: &[Asset],
        field: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Adjustment>> {
        if let Some(adjustment_reader) = &self.adjustment_reader {
            let mut all_adjustments = Vec::new();

            for asset in assets {
                let adjustments = adjustment_reader.get_adjustments(asset.id, start, end);
                all_adjustments.extend(adjustments.into_iter().cloned());
            }

            Ok(all_adjustments)
        } else {
            Ok(Vec::new())
        }
    }

    /// Set default frequency
    pub fn set_default_frequency(&mut self, frequency: DataFrequency) {
        self.default_frequency = frequency;
    }

    /// Get default frequency
    pub fn default_frequency(&self) -> DataFrequency {
        self.default_frequency
    }

    // Helper: Calculate history start datetime
    fn get_history_start(
        &self,
        end: DateTime<Utc>,
        bar_count: usize,
        frequency: DataFrequency,
    ) -> Result<DateTime<Utc>> {
        match frequency {
            DataFrequency::Daily => {
                // Go back bar_count trading days
                let end_idx = self
                    .trading_days
                    .iter()
                    .position(|&d| d >= end)
                    .unwrap_or(self.trading_days.len() - 1);

                let start_idx = end_idx.saturating_sub(bar_count);

                // Check if we have enough data
                if start_idx == 0 && end_idx < bar_count {
                    let first_available = self.trading_days.first().copied().unwrap_or(end);
                    return Err(ZiplineError::HistoryWindowBeforeFirstData {
                        asset: 0, // Generic - specific asset would be passed in real implementation
                        requested_start: end - chrono::Duration::days(bar_count as i64),
                        first_available,
                    });
                }

                Ok(self.trading_days[start_idx])
            }
            DataFrequency::Minute => {
                // Go back bar_count minutes
                // Simplified - real implementation would use trading calendar
                use chrono::Duration;
                Ok(end - Duration::minutes(bar_count as i64))
            }
            DataFrequency::Second => Err(ZiplineError::UnsupportedFrequency {
                frequency: "second".to_string(),
                supported: vec!["daily".to_string(), "minute".to_string()],
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::AssetType;

    struct MockBarReader {
        data: HashMap<u64, HashMap<String, f64>>,
    }

    impl BarReader for MockBarReader {
        fn get_value(
            &self,
            asset_id: u64,
            _dt: DateTime<Utc>,
            field: &str,
        ) -> Result<Option<f64>> {
            Ok(self
                .data
                .get(&asset_id)
                .and_then(|fields| fields.get(field).copied()))
        }

        fn get_last_traded_dt(
            &self,
            _asset_id: u64,
            dt: DateTime<Utc>,
        ) -> Result<Option<DateTime<Utc>>> {
            Ok(Some(dt))
        }

        fn get_bars(
            &self,
            _asset_id: u64,
            _start: DateTime<Utc>,
            _end: DateTime<Utc>,
        ) -> Result<Vec<Bar>> {
            Ok(vec![Bar {
                open: 100.0,
                high: 105.0,
                low: 99.0,
                close: 103.0,
                volume: 1000000.0,
            }])
        }
    }

    fn create_test_portal() -> DataPortal {
        let mut data = HashMap::new();
        let mut asset_data = HashMap::new();
        asset_data.insert("close".to_string(), 100.0);
        asset_data.insert("volume".to_string(), 1000000.0);
        data.insert(1, asset_data);

        let reader = Arc::new(MockBarReader { data });

        DataPortal::new(Some(reader), None, None, vec![Utc::now()])
    }

    fn create_test_asset() -> Asset {
        Asset {
            id: 1,
            symbol: "AAPL".to_string(),
            asset_type: AssetType::Equity,
            exchange: "NASDAQ".to_string(),
            name: None,
        }
    }

    #[test]
    fn test_data_portal_creation() {
        let portal = create_test_portal();
        assert_eq!(portal.default_frequency(), DataFrequency::Daily);
    }

    #[test]
    fn test_current_value() {
        let portal = create_test_portal();
        let asset = create_test_asset();
        let dt = Utc::now();

        let value = portal.current_value(&asset, "close", dt).unwrap();
        assert_eq!(value, Some(100.0));
    }

    #[test]
    fn test_current_price_alias() {
        let portal = create_test_portal();
        let asset = create_test_asset();
        let dt = Utc::now();

        // "price" should map to "close"
        let value = portal.current_value(&asset, "price", dt).unwrap();
        assert_eq!(value, Some(100.0));
    }

    #[test]
    fn test_current_multiple_assets() {
        let portal = create_test_portal();
        let asset = create_test_asset();
        let dt = Utc::now();

        let result = portal.current(&[asset], &["close", "volume"], dt).unwrap();
        assert!(result.contains_key(&1));

        let asset_data = &result[&1];
        assert_eq!(asset_data.get("close"), Some(&100.0));
        assert_eq!(asset_data.get("volume"), Some(&1000000.0));
    }

    #[test]
    fn test_can_trade() {
        let portal = create_test_portal();
        let asset = create_test_asset();
        let dt = Utc::now();

        assert!(portal.can_trade(&asset, dt));
    }

    #[test]
    fn test_is_stale() {
        let portal = create_test_portal();
        let asset = create_test_asset();
        let dt = Utc::now();

        assert!(!portal.is_stale(&asset, dt));
    }

    #[test]
    fn test_dataframe_creation() {
        let df = DataFrame::new();
        assert!(df.is_empty());
        assert_eq!(df.len(), 0);
    }

    #[test]
    fn test_set_default_frequency() {
        let mut portal = create_test_portal();
        portal.set_default_frequency(DataFrequency::Minute);
        assert_eq!(portal.default_frequency(), DataFrequency::Minute);
    }
}
