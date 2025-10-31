//! Data bundle system for ingesting historical data from CSV files

use crate::asset::{Asset, AssetType};
use crate::error::{Result, ZiplineError};
use crate::types::{Bar, Price, Timestamp, Volume};
use chrono::{DateTime, NaiveDate, Utc};
use csv::ReaderBuilder;
use hashbrown::HashMap;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Bundle metadata and data holder
#[derive(Debug, Clone)]
pub struct BundleData {
    /// Asset ID to bars mapping
    data: HashMap<u64, Vec<Bar>>,
    /// Asset metadata (symbol -> Asset)
    assets: HashMap<String, Asset>,
    /// Data start date
    start_date: Option<NaiveDate>,
    /// Data end date
    end_date: Option<NaiveDate>,
    /// Total bars loaded
    bar_count: usize,
}

impl BundleData {
    /// Create new empty bundle
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            assets: HashMap::new(),
            start_date: None,
            end_date: None,
            bar_count: 0,
        }
    }

    /// Add bar data for an asset
    pub fn add_bar(&mut self, asset_id: u64, bar: Bar) {
        self.data.entry(asset_id).or_insert_with(Vec::new).push(bar);
        self.bar_count += 1;
    }

    /// Register an asset
    pub fn add_asset(&mut self, symbol: String, asset: Asset) {
        self.assets.insert(symbol, asset);
    }

    /// Get bars for an asset
    pub fn get_bars(&self, asset_id: u64) -> Option<&[Bar]> {
        self.data.get(&asset_id).map(|v| v.as_slice())
    }

    /// Get asset by symbol
    pub fn get_asset(&self, symbol: &str) -> Option<&Asset> {
        self.assets.get(symbol)
    }

    /// Get all assets
    pub fn assets(&self) -> &HashMap<String, Asset> {
        &self.assets
    }

    /// Get date range
    pub fn date_range(&self) -> Option<(NaiveDate, NaiveDate)> {
        match (self.start_date, self.end_date) {
            (Some(start), Some(end)) => Some((start, end)),
            _ => None,
        }
    }

    /// Finalize bundle (sort and validate)
    pub fn finalize(&mut self) -> Result<()> {
        // Sort all bars by timestamp
        for bars in self.data.values_mut() {
            bars.sort_by_key(|b| b.timestamp);
        }

        // Calculate date range
        let mut min_date: Option<NaiveDate> = None;
        let mut max_date: Option<NaiveDate> = None;

        for bars in self.data.values() {
            if let Some(first) = bars.first() {
                let first_date = first.timestamp.date_naive();
                min_date = Some(match min_date {
                    Some(d) => d.min(first_date),
                    None => first_date,
                });
            }
            if let Some(last) = bars.last() {
                let last_date = last.timestamp.date_naive();
                max_date = Some(match max_date {
                    Some(d) => d.max(last_date),
                    None => last_date,
                });
            }
        }

        self.start_date = min_date;
        self.end_date = max_date;

        Ok(())
    }

    /// Get statistics
    pub fn stats(&self) -> BundleStats {
        BundleStats {
            asset_count: self.assets.len(),
            bar_count: self.bar_count,
            start_date: self.start_date,
            end_date: self.end_date,
        }
    }
}

impl Default for BundleData {
    fn default() -> Self {
        Self::new()
    }
}

/// Bundle statistics
#[derive(Debug, Clone)]
pub struct BundleStats {
    pub asset_count: usize,
    pub bar_count: usize,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
}

/// CSV data format configuration
#[derive(Debug, Clone)]
pub struct CSVFormat {
    /// Date column name
    pub date_column: String,
    /// Symbol column name
    pub symbol_column: String,
    /// Open price column name
    pub open_column: String,
    /// High price column name
    pub high_column: String,
    /// Low price column name
    pub low_column: String,
    /// Close price column name
    pub close_column: String,
    /// Volume column name
    pub volume_column: String,
    /// Date format string (e.g., "%Y-%m-%d")
    pub date_format: String,
    /// Has header row
    pub has_headers: bool,
}

impl Default for CSVFormat {
    fn default() -> Self {
        Self {
            date_column: "date".to_string(),
            symbol_column: "symbol".to_string(),
            open_column: "open".to_string(),
            high_column: "high".to_string(),
            low_column: "low".to_string(),
            close_column: "close".to_string(),
            volume_column: "volume".to_string(),
            date_format: "%Y-%m-%d".to_string(),
            has_headers: true,
        }
    }
}

/// CSV bundle reader
pub struct CSVBundleReader {
    format: CSVFormat,
    next_asset_id: u64,
}

impl CSVBundleReader {
    /// Create new CSV reader with default format
    pub fn new() -> Self {
        Self {
            format: CSVFormat::default(),
            next_asset_id: 1,
        }
    }

    /// Create with custom format
    pub fn with_format(format: CSVFormat) -> Self {
        Self {
            format,
            next_asset_id: 1,
        }
    }

    /// Load CSV file into bundle
    pub fn load_csv(&mut self, path: &Path) -> Result<BundleData> {
        let mut bundle = BundleData::new();
        let mut rdr = ReaderBuilder::new()
            .has_headers(self.format.has_headers)
            .from_path(path)
            .map_err(|e| ZiplineError::DataError(format!("Failed to open CSV: {}", e)))?;

        // Get headers
        let headers = rdr.headers()
            .map_err(|e| ZiplineError::DataError(format!("Failed to read headers: {}", e)))?
            .clone();

        // Find column indices
        let date_idx = Self::find_column(&headers, &self.format.date_column)?;
        let symbol_idx = Self::find_column(&headers, &self.format.symbol_column)?;
        let open_idx = Self::find_column(&headers, &self.format.open_column)?;
        let high_idx = Self::find_column(&headers, &self.format.high_column)?;
        let low_idx = Self::find_column(&headers, &self.format.low_column)?;
        let close_idx = Self::find_column(&headers, &self.format.close_column)?;
        let volume_idx = Self::find_column(&headers, &self.format.volume_column)?;

        // Symbol to asset ID mapping
        let mut symbol_to_id: HashMap<String, u64> = HashMap::new();

        // Read records
        for result in rdr.records() {
            let record = result
                .map_err(|e| ZiplineError::DataError(format!("Failed to read record: {}", e)))?;

            // Parse fields
            let symbol = record.get(symbol_idx)
                .ok_or_else(|| ZiplineError::DataError("Missing symbol".to_string()))?
                .to_string();

            let date_str = record.get(date_idx)
                .ok_or_else(|| ZiplineError::DataError("Missing date".to_string()))?;

            let open: f64 = record.get(open_idx)
                .ok_or_else(|| ZiplineError::DataError("Missing open".to_string()))?
                .parse()
                .map_err(|_| ZiplineError::DataError("Invalid open price".to_string()))?;

            let high: f64 = record.get(high_idx)
                .ok_or_else(|| ZiplineError::DataError("Missing high".to_string()))?
                .parse()
                .map_err(|_| ZiplineError::DataError("Invalid high price".to_string()))?;

            let low: f64 = record.get(low_idx)
                .ok_or_else(|| ZiplineError::DataError("Missing low".to_string()))?
                .parse()
                .map_err(|_| ZiplineError::DataError("Invalid low price".to_string()))?;

            let close: f64 = record.get(close_idx)
                .ok_or_else(|| ZiplineError::DataError("Missing close".to_string()))?
                .parse()
                .map_err(|_| ZiplineError::DataError("Invalid close price".to_string()))?;

            let volume: u64 = record.get(volume_idx)
                .ok_or_else(|| ZiplineError::DataError("Missing volume".to_string()))?
                .parse()
                .map_err(|_| ZiplineError::DataError("Invalid volume".to_string()))?;

            // Parse date
            let date = NaiveDate::parse_from_str(date_str, &self.format.date_format)
                .map_err(|_| ZiplineError::DataError(format!("Invalid date format: {}", date_str)))?;

            let timestamp = date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| ZiplineError::DataError("Invalid time".to_string()))?
                .and_utc();

            // Get or create asset ID
            let asset_id = *symbol_to_id.entry(symbol.clone()).or_insert_with(|| {
                let id = self.next_asset_id;
                self.next_asset_id += 1;

                // Create and register asset
                let asset = Asset::equity(id, symbol.clone(), "CSV".to_string());
                bundle.add_asset(symbol.clone(), asset);

                id
            });

            // Validate OHLC
            if high < low || open > high || open < low || close > high || close < low {
                return Err(ZiplineError::DataError(format!(
                    "Invalid OHLC data for {} on {}: O={} H={} L={} C={}",
                    symbol, date, open, high, low, close
                )));
            }

            // Create bar
            let bar = Bar {
                timestamp,
                open,
                high,
                low,
                close,
                volume: volume as f64,
            };

            bundle.add_bar(asset_id, bar);
        }

        // Finalize bundle (sort and calculate date range)
        bundle.finalize()?;

        Ok(bundle)
    }

    /// Find column index by name
    fn find_column(headers: &csv::StringRecord, name: &str) -> Result<usize> {
        headers
            .iter()
            .position(|h| h.eq_ignore_ascii_case(name))
            .ok_or_else(|| ZiplineError::DataError(format!("Column '{}' not found", name)))
    }
}

impl Default for CSVBundleReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Bundle registry for managing multiple named bundles
pub struct BundleRegistry {
    bundles: HashMap<String, BundleData>,
    cache_dir: Option<PathBuf>,
}

impl BundleRegistry {
    /// Create new bundle registry
    pub fn new() -> Self {
        Self {
            bundles: HashMap::new(),
            cache_dir: None,
        }
    }

    /// Set cache directory for bundles
    pub fn with_cache_dir(cache_dir: PathBuf) -> Self {
        Self {
            bundles: HashMap::new(),
            cache_dir: Some(cache_dir),
        }
    }

    /// Register a bundle
    pub fn register(&mut self, name: String, bundle: BundleData) {
        self.bundles.insert(name, bundle);
    }

    /// Get a bundle by name
    pub fn get(&self, name: &str) -> Option<&BundleData> {
        self.bundles.get(name)
    }

    /// Load bundle from CSV
    pub fn load_csv(&mut self, name: String, path: &Path) -> Result<()> {
        let mut reader = CSVBundleReader::new();
        let bundle = reader.load_csv(path)?;
        self.register(name, bundle);
        Ok(())
    }

    /// Load bundle from CSV with custom format
    pub fn load_csv_with_format(
        &mut self,
        name: String,
        path: &Path,
        format: CSVFormat,
    ) -> Result<()> {
        let mut reader = CSVBundleReader::with_format(format);
        let bundle = reader.load_csv(path)?;
        self.register(name, bundle);
        Ok(())
    }

    /// List all registered bundles
    pub fn list_bundles(&self) -> Vec<&str> {
        self.bundles.keys().map(|s| s.as_str()).collect()
    }

    /// Get bundle statistics
    pub fn bundle_stats(&self, name: &str) -> Option<BundleStats> {
        self.bundles.get(name).map(|b| b.stats())
    }

    /// Clear all bundles
    pub fn clear(&mut self) {
        self.bundles.clear();
    }
}

impl Default for BundleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_csv() -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            "date,symbol,open,high,low,close,volume\n\
             2020-01-02,AAPL,300.35,300.58,298.32,300.35,33911800\n\
             2020-01-03,AAPL,297.15,300.58,297.14,297.43,36028600\n\
             2020-01-02,MSFT,160.62,160.73,159.98,160.62,22622100\n\
             2020-01-03,MSFT,158.32,159.94,158.06,158.62,21116200"
        )
        .unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_bundle_data_creation() {
        let bundle = BundleData::new();
        assert_eq!(bundle.bar_count, 0);
        assert_eq!(bundle.assets.len(), 0);
        assert!(bundle.start_date.is_none());
        assert!(bundle.end_date.is_none());
    }

    #[test]
    fn test_csv_reader_load() {
        let file = create_test_csv();
        let mut reader = CSVBundleReader::new();
        let bundle = reader.load_csv(file.path()).unwrap();

        let stats = bundle.stats();
        assert_eq!(stats.asset_count, 2); // AAPL and MSFT
        assert_eq!(stats.bar_count, 4); // 2 bars each
        assert!(stats.start_date.is_some());
        assert!(stats.end_date.is_some());
    }

    #[test]
    fn test_bundle_finalization() {
        let mut bundle = BundleData::new();

        let asset = Asset::equity(1, "TEST".to_string(), "TEST".to_string());
        bundle.add_asset("TEST".to_string(), asset);

        let bar1 = Bar {
            timestamp: DateTime::parse_from_rfc3339("2020-01-03T00:00:00Z").unwrap().into(),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 1000.0,
        };

        let bar2 = Bar {
            timestamp: DateTime::parse_from_rfc3339("2020-01-02T00:00:00Z").unwrap().into(),
            open: 99.0,
            high: 100.0,
            low: 98.0,
            close: 99.5,
            volume: 900.0,
        };

        // Add in reverse order
        bundle.add_bar(1, bar1);
        bundle.add_bar(1, bar2);

        bundle.finalize().unwrap();

        // Verify sorting
        let bars = bundle.get_bars(1).unwrap();
        assert_eq!(bars.len(), 2);
        assert!(bars[0].timestamp < bars[1].timestamp);

        // Verify date range
        let (start, end) = bundle.date_range().unwrap();
        assert_eq!(start, NaiveDate::from_ymd_opt(2020, 1, 2).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2020, 1, 3).unwrap());
    }

    #[test]
    fn test_bundle_registry() {
        let file = create_test_csv();
        let mut registry = BundleRegistry::new();

        registry.load_csv("test".to_string(), file.path()).unwrap();

        assert_eq!(registry.list_bundles().len(), 1);
        assert!(registry.get("test").is_some());

        let stats = registry.bundle_stats("test").unwrap();
        assert_eq!(stats.asset_count, 2);
        assert_eq!(stats.bar_count, 4);
    }

    #[test]
    fn test_invalid_ohlc_validation() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            "date,symbol,open,high,low,close,volume\n\
             2020-01-02,AAPL,300.35,298.00,299.00,300.35,33911800"
        )
        .unwrap(); // high < low - invalid
        file.flush().unwrap();

        let mut reader = CSVBundleReader::new();
        let result = reader.load_csv(file.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_custom_csv_format() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            "Date,Ticker,O,H,L,C,Vol\n\
             01/02/2020,AAPL,300.35,300.58,298.32,300.35,33911800"
        )
        .unwrap();
        file.flush().unwrap();

        let format = CSVFormat {
            date_column: "Date".to_string(),
            symbol_column: "Ticker".to_string(),
            open_column: "O".to_string(),
            high_column: "H".to_string(),
            low_column: "L".to_string(),
            close_column: "C".to_string(),
            volume_column: "Vol".to_string(),
            date_format: "%m/%d/%Y".to_string(),
            has_headers: true,
        };

        let mut reader = CSVBundleReader::with_format(format);
        let bundle = reader.load_csv(file.path()).unwrap();

        let stats = bundle.stats();
        assert_eq!(stats.asset_count, 1);
        assert_eq!(stats.bar_count, 1);
    }
}
