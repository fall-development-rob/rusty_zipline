//! Bcolz Daily Bar Reader
//!
//! Reads daily OHLCV bar data from Zipline bcolz bundles.
//! Provides efficient access to end-of-day market data with caching.

use crate::asset::Asset;
use crate::calendar::TradingCalendar;
use crate::data::bar_reader::{Bar, BarReader, SessionLabel};
use crate::data::readers::bcolz_utils::{find_asset_sids, read_column_f64, read_column_i64};
use crate::error::{Result, ZiplineError};
use chrono::{NaiveDate, DateTime, Datelike, TimeZone, Utc};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// LRU cache entry for daily bars
#[derive(Debug, Clone)]
struct CachedDailyBars {
    bars: Vec<Bar>,
    dates: Vec<DateTime<Utc>>,
}

/// Bcolz daily bar reader
///
/// Reads daily OHLCV data from a Zipline bcolz bundle directory structure:
/// ```text
/// <bundle_root>/
///   daily_equities/
///     <sid>/
///       open.00000
///       high.00000
///       low.00000
///       close.00000
///       volume.00000
///       day.00000
///       meta/
///         attrs
/// ```
pub struct BcolzDailyBarReader {
    /// Root directory of the bundle
    root_dir: PathBuf,
    /// Trading calendar for date alignment
    calendar: Option<Arc<dyn TradingCalendar>>,
    /// Available asset SIDs
    sids: Vec<u64>,
    /// First trading day in the bundle
    first_trading_day: Option<DateTime<Utc>>,
    /// Last trading day in the bundle
    last_trading_day: Option<DateTime<Utc>>,
    /// Cached bar data (LRU cache)
    cache: Arc<RwLock<HashMap<u64, CachedDailyBars>>>,
    /// Maximum cache size (number of assets)
    max_cache_size: usize,
    /// All sessions available
    sessions: Vec<SessionLabel>,
}

impl std::fmt::Debug for BcolzDailyBarReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cache_size = self.cache.read().map(|c| c.len()).unwrap_or(0);
        f.debug_struct("BcolzDailyBarReader")
            .field("root_dir", &self.root_dir)
            .field("calendar", &if self.calendar.is_some() { "<Some(TradingCalendar)>" } else { "None" })
            .field("sids", &format!("{} assets", self.sids.len()))
            .field("first_trading_day", &self.first_trading_day)
            .field("last_trading_day", &self.last_trading_day)
            .field("cache_size", &cache_size)
            .field("max_cache_size", &self.max_cache_size)
            .field("sessions", &format!("{} sessions", self.sessions.len()))
            .finish()
    }
}

impl BcolzDailyBarReader {
    /// Create a new BcolzDailyBarReader
    ///
    /// # Arguments
    /// * `root_dir` - Path to the bundle root (containing daily_equities/)
    /// * `calendar` - Optional trading calendar for date alignment
    pub fn new<P: AsRef<Path>>(root_dir: P, calendar: Option<Arc<dyn TradingCalendar>>) -> Result<Self> {
        let root_path = root_dir.as_ref().to_path_buf();
        let daily_path = root_path.join("daily_equities");

        if !daily_path.exists() {
            return Err(ZiplineError::InvalidData(format!(
                "Daily equities directory not found: {:?}",
                daily_path
            )));
        }

        // Find all asset SIDs
        let sids = find_asset_sids(&daily_path)?;

        if sids.is_empty() {
            return Err(ZiplineError::InvalidData(
                "No asset data found in bundle".to_string(),
            ));
        }

        // Read first asset to determine date range
        let first_sid = sids[0];
        let (first_day, last_day) = Self::read_date_range(&daily_path, first_sid)?;

        // Build sessions list
        let sessions = Self::build_sessions(&daily_path, &sids)?;

        Ok(Self {
            root_dir: root_path,
            calendar,
            sids,
            first_trading_day: Some(first_day),
            last_trading_day: Some(last_day),
            cache: Arc::new(RwLock::new(HashMap::new())),
            max_cache_size: 100, // Cache up to 100 assets
            sessions,
        })
    }

    /// Get the daily equities directory path
    fn daily_equities_path(&self) -> PathBuf {
        self.root_dir.join("daily_equities")
    }

    /// Get the path for a specific asset
    fn asset_path(&self, sid: u64) -> PathBuf {
        self.daily_equities_path().join(sid.to_string())
    }

    /// Read date range from an asset's data
    fn read_date_range(daily_path: &Path, sid: u64) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
        let asset_path = daily_path.join(sid.to_string());

        // Read the 'day' column which contains dates as epoch days or timestamps
        let days = read_column_i64(&asset_path, "day")?;

        if days.is_empty() {
            return Err(ZiplineError::InvalidData(format!(
                "No date data for asset {}",
                sid
            )));
        }

        // Convert to DateTime (assuming days since epoch or nanoseconds)
        let first_dt = Self::convert_day_to_datetime(days[0])?;
        let last_dt = Self::convert_day_to_datetime(days[days.len() - 1])?;

        Ok((first_dt, last_dt))
    }

    /// Convert day value to DateTime
    /// Handles both epoch days and nanosecond timestamps
    fn convert_day_to_datetime(day_value: i64) -> Result<DateTime<Utc>> {
        // If value is > 1e12, it's likely nanoseconds
        if day_value > 1_000_000_000_000 {
            // Nanoseconds since epoch
            let secs = day_value / 1_000_000_000;
            let nsecs = (day_value % 1_000_000_000) as u32;
            Utc.timestamp_opt(secs, nsecs)
                .single()
                .ok_or_else(|| ZiplineError::InvalidData(format!("Invalid timestamp: {}", day_value)))
        } else if day_value > 1_000_000 {
            // Seconds since epoch (old format)
            Utc.timestamp_opt(day_value, 0)
                .single()
                .ok_or_else(|| ZiplineError::InvalidData(format!("Invalid timestamp: {}", day_value)))
        } else {
            // Days since Unix epoch (1970-01-01)
            let base = Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0)
                .single()
                .ok_or_else(|| ZiplineError::InvalidData("Invalid base date".to_string()))?;

            Ok(base + chrono::Duration::days(day_value))
        }
    }

    /// Build sessions list from all assets
    fn build_sessions(daily_path: &Path, sids: &[u64]) -> Result<Vec<SessionLabel>> {
        let mut session_set = std::collections::HashSet::new();

        // Sample first asset to get sessions
        if let Some(&sid) = sids.first() {
            let asset_path = daily_path.join(sid.to_string());
            let days = read_column_i64(&asset_path, "day")?;

            for day_value in days {
                let dt = Self::convert_day_to_datetime(day_value)?;
                session_set.insert(SessionLabel::from_datetime(dt));
            }
        }

        let mut sessions: Vec<SessionLabel> = session_set.into_iter().collect();
        sessions.sort_by_key(|s| (s.year, s.month, s.day));

        Ok(sessions)
    }

    /// Load and cache bars for an asset
    fn load_asset_data(&self, sid: u64) -> Result<CachedDailyBars> {
        // Check cache first
        {
            let cache = self.cache.read().unwrap();
            if let Some(cached) = cache.get(&sid) {
                return Ok(cached.clone());
            }
        }

        // Load from disk
        let asset_path = self.asset_path(sid);

        if !asset_path.exists() {
            return Err(ZiplineError::AssetNotFound(sid));
        }

        // Read all columns
        let days = read_column_i64(&asset_path, "day")?;
        let opens = read_column_f64(&asset_path, "open")?;
        let highs = read_column_f64(&asset_path, "high")?;
        let lows = read_column_f64(&asset_path, "low")?;
        let closes = read_column_f64(&asset_path, "close")?;
        let volumes = read_column_f64(&asset_path, "volume")?;

        // Validate all columns have same length
        let n = days.len();
        if opens.len() != n || highs.len() != n || lows.len() != n || closes.len() != n || volumes.len() != n {
            return Err(ZiplineError::InvalidData(format!(
                "Column length mismatch for asset {}",
                sid
            )));
        }

        // Build bars
        let mut bars = Vec::with_capacity(n);
        let mut dates = Vec::with_capacity(n);

        for i in 0..n {
            let dt = Self::convert_day_to_datetime(days[i])?;
            dates.push(dt);

            let bar = Bar::new(
                opens[i],
                highs[i],
                lows[i],
                closes[i],
                volumes[i],
                dt,
            );

            if !bar.is_valid() {
                log::warn!("Invalid bar for asset {} at {:?}", sid, dt);
            }

            bars.push(bar);
        }

        let cached = CachedDailyBars { bars, dates };

        // Store in cache
        {
            let mut cache = self.cache.write().unwrap();

            // Implement simple LRU by removing oldest if cache is full
            if cache.len() >= self.max_cache_size {
                // Remove a random entry (simple eviction)
                if let Some(&key) = cache.keys().next() {
                    cache.remove(&key);
                }
            }

            cache.insert(sid, cached.clone());
        }

        Ok(cached)
    }

    /// Find bar index for a given date
    fn find_bar_index(dates: &[DateTime<Utc>], target_dt: DateTime<Utc>) -> Option<usize> {
        // Binary search for exact match or closest
        dates.binary_search_by(|dt| dt.date_naive().cmp(&target_dt.date_naive()))
            .ok()
            .or_else(|| {
                // If not exact match, find closest previous date
                let idx = dates.partition_point(|dt| dt.date_naive() < target_dt.date_naive());
                if idx > 0 {
                    Some(idx - 1)
                } else {
                    None
                }
            })
    }

    /// Get available asset SIDs
    pub fn sids(&self) -> &[u64] {
        &self.sids
    }

    /// Get first trading day
    pub fn first_trading_day(&self) -> Option<DateTime<Utc>> {
        self.first_trading_day
    }

    /// Get last trading day
    pub fn last_trading_day(&self) -> Option<DateTime<Utc>> {
        self.last_trading_day
    }

    /// Clear the cache
    pub fn clear_cache(&self) {
        let mut cache = self.cache.write().unwrap();
        cache.clear();
    }

    /// Get cache size
    pub fn cache_size(&self) -> usize {
        let cache = self.cache.read().unwrap();
        cache.len()
    }
}

impl BarReader for BcolzDailyBarReader {
    fn get_bar(&self, asset: &Asset, dt: DateTime<Utc>) -> Result<Bar> {
        let cached = self.load_asset_data(asset.id)?;

        if let Some(idx) = Self::find_bar_index(&cached.dates, dt) {
            Ok(cached.bars[idx])
        } else {
            Err(ZiplineError::DataNotFound(format!(
                "No bar data for asset {} at {:?}",
                asset.symbol, dt
            )))
        }
    }

    fn get_bars(&self, asset: &Asset, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Bar>> {
        let cached = self.load_asset_data(asset.id)?;

        let start_idx = Self::find_bar_index(&cached.dates, start).unwrap_or(0);
        let end_idx = Self::find_bar_index(&cached.dates, end)
            .map(|i| (i + 1).min(cached.bars.len()))
            .unwrap_or(cached.bars.len());

        if start_idx >= end_idx {
            return Ok(Vec::new());
        }

        Ok(cached.bars[start_idx..end_idx].to_vec())
    }

    fn last_available_dt(&self, asset: &Asset) -> Result<DateTime<Utc>> {
        let cached = self.load_asset_data(asset.id)?;

        cached.dates.last().copied().ok_or_else(|| {
            ZiplineError::DataNotFound(format!("No data for asset {}", asset.symbol))
        })
    }

    fn first_available_dt(&self, asset: &Asset) -> Result<DateTime<Utc>> {
        let cached = self.load_asset_data(asset.id)?;

        cached.dates.first().copied().ok_or_else(|| {
            ZiplineError::DataNotFound(format!("No data for asset {}", asset.symbol))
        })
    }

    fn sessions(&self) -> Result<Vec<SessionLabel>> {
        Ok(self.sessions.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_bcolz_asset(path: &Path, sid: u64, num_days: usize) -> Result<()> {
        let asset_path = path.join(sid.to_string());
        fs::create_dir_all(&asset_path)?;

        let meta_path = asset_path.join("meta");
        fs::create_dir_all(&meta_path)?;

        // Create sample data
        let mut days = Vec::new();
        let mut opens = Vec::new();
        let mut highs = Vec::new();
        let mut lows = Vec::new();
        let mut closes = Vec::new();
        let mut volumes = Vec::new();

        let base_date = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();

        for i in 0..num_days {
            let dt = base_date + chrono::Duration::days(i as i64);
            let day_value = (dt.timestamp_nanos_opt().unwrap_or(0) / 1_000_000_000) as i64;

            days.extend_from_slice(&day_value.to_le_bytes());
            opens.extend_from_slice(&(100.0 + i as f64).to_le_bytes());
            highs.extend_from_slice(&(105.0 + i as f64).to_le_bytes());
            lows.extend_from_slice(&(99.0 + i as f64).to_le_bytes());
            closes.extend_from_slice(&(102.0 + i as f64).to_le_bytes());
            volumes.extend_from_slice(&(1000000.0 + i as f64 * 1000.0).to_le_bytes());
        }

        // Write chunk files (uncompressed for testing)
        fs::write(asset_path.join("day.00000"), days)?;
        fs::write(asset_path.join("open.00000"), opens)?;
        fs::write(asset_path.join("high.00000"), highs)?;
        fs::write(asset_path.join("low.00000"), lows)?;
        fs::write(asset_path.join("close.00000"), closes)?;
        fs::write(asset_path.join("volume.00000"), volumes)?;

        Ok(())
    }

    #[test]
    fn test_bcolz_daily_bar_reader() {
        let temp_dir = TempDir::new().unwrap();
        let bundle_path = temp_dir.path();
        let daily_path = bundle_path.join("daily_equities");
        fs::create_dir_all(&daily_path).unwrap();

        // Create test data for 2 assets
        create_test_bcolz_asset(&daily_path, 1, 10).unwrap();
        create_test_bcolz_asset(&daily_path, 2, 10).unwrap();

        // Create reader
        let reader = BcolzDailyBarReader::new(bundle_path, None).unwrap();

        assert_eq!(reader.sids(), &[1, 2]);
        assert_eq!(reader.cache_size(), 0);

        // Test get_bar
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "TEST".to_string(), "NYSE".to_string(), start_date);
        let dt = Utc.with_ymd_and_hms(2020, 1, 5, 0, 0, 0).unwrap();

        let bar = reader.get_bar(&asset, dt).unwrap();
        assert!(bar.is_valid());
        assert_eq!(reader.cache_size(), 1);

        // Test get_bars
        let start = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2020, 1, 10, 0, 0, 0).unwrap();

        let bars = reader.get_bars(&asset, start, end).unwrap();
        assert!(bars.len() > 0);
    }

    #[test]
    fn test_convert_day_to_datetime() {
        // Test epoch days
        let days_since_epoch = 18262; // 2020-01-01
        let dt = BcolzDailyBarReader::convert_day_to_datetime(days_since_epoch).unwrap();
        assert_eq!(dt.year(), 2020);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1);

        // Test nanoseconds
        let nanos = 1577836800_000_000_000i64; // 2020-01-01 00:00:00 UTC
        let dt = BcolzDailyBarReader::convert_day_to_datetime(nanos).unwrap();
        assert_eq!(dt.year(), 2020);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1);
    }
}
