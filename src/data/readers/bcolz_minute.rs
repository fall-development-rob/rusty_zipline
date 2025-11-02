//! Bcolz Minute Bar Reader
//!
//! Reads minute-level OHLCV bar data from Zipline bcolz bundles.
//! Provides efficient access to intraday market data with session-based caching.

use crate::asset::Asset;
use crate::calendar::TradingCalendar;
use crate::data::bar_reader::{Bar, BarReader, SessionLabel};
use crate::data::readers::bcolz_utils::{find_asset_sids, read_column_f64, read_column_i64};
use crate::error::{Result, ZiplineError};
use chrono::{NaiveDate, DateTime, Datelike, TimeZone, Utc};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// Cached minute bars for a single session (trading day)
#[derive(Debug, Clone)]
struct SessionBars {
    bars: Vec<Bar>,
    timestamps: Vec<DateTime<Utc>>,
}

/// Cache key: (asset_id, session_date)
type CacheKey = (u64, SessionLabel);

/// Bcolz minute bar reader
///
/// Reads minute-level OHLCV data from a Zipline bcolz bundle directory structure:
/// ```text
/// <bundle_root>/
///   minute_equities/
///     <sid>/
///       open.00000
///       high.00000
///       low.00000
///       close.00000
///       volume.00000
///       minute.00000  (or 'date' column)
///       meta/
///         attrs
/// ```
///
/// Due to the large volume of minute data (390 bars per day for US equities),
/// this reader uses session-based caching to minimize memory usage.
pub struct BcolzMinuteBarReader {
    /// Root directory of the bundle
    root_dir: PathBuf,
    /// Trading calendar for session alignment
    calendar: Option<Arc<dyn TradingCalendar>>,
    /// Available asset SIDs
    sids: Vec<u64>,
    /// Trading sessions available
    sessions: Vec<SessionLabel>,
    /// Session index for O(1) lookup
    session_idx: HashMap<SessionLabel, usize>,
    /// First trading minute in the bundle
    first_trading_minute: Option<DateTime<Utc>>,
    /// Last trading minute in the bundle
    last_trading_minute: Option<DateTime<Utc>>,
    /// Session-based cache: (asset_id, session) -> bars
    cache: Arc<RwLock<HashMap<CacheKey, SessionBars>>>,
    /// Maximum cache size (number of session-assets)
    max_cache_size: usize,
    /// Minutes per session (390 for US equities)
    minutes_per_session: usize,
}

impl std::fmt::Debug for BcolzMinuteBarReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cache_size = self.cache.read().map(|c| c.len()).unwrap_or(0);
        f.debug_struct("BcolzMinuteBarReader")
            .field("root_dir", &self.root_dir)
            .field("calendar", &if self.calendar.is_some() { "<Some(TradingCalendar)>" } else { "None" })
            .field("sids", &format!("{} assets", self.sids.len()))
            .field("sessions", &format!("{} sessions", self.sessions.len()))
            .field("first_trading_minute", &self.first_trading_minute)
            .field("last_trading_minute", &self.last_trading_minute)
            .field("cache_size", &cache_size)
            .field("max_cache_size", &self.max_cache_size)
            .field("minutes_per_session", &self.minutes_per_session)
            .finish()
    }
}

impl BcolzMinuteBarReader {
    /// Create a new BcolzMinuteBarReader
    ///
    /// # Arguments
    /// * `root_dir` - Path to the bundle root (containing minute_equities/)
    /// * `calendar` - Optional trading calendar for session alignment
    /// * `minutes_per_session` - Expected minutes per trading session (default: 390)
    pub fn new<P: AsRef<Path>>(
        root_dir: P,
        calendar: Option<Arc<dyn TradingCalendar>>,
        minutes_per_session: usize,
    ) -> Result<Self> {
        let root_path = root_dir.as_ref().to_path_buf();
        let minute_path = root_path.join("minute_equities");

        if !minute_path.exists() {
            return Err(ZiplineError::InvalidData(format!(
                "Minute equities directory not found: {:?}",
                minute_path
            )));
        }

        // Find all asset SIDs
        let sids = find_asset_sids(&minute_path)?;

        if sids.is_empty() {
            return Err(ZiplineError::InvalidData(
                "No asset data found in bundle".to_string(),
            ));
        }

        // Read first asset to determine time range and sessions
        let first_sid = sids[0];
        let (first_minute, last_minute, sessions) =
            Self::read_time_range_and_sessions(&minute_path, first_sid)?;

        // Build session index
        let session_idx: HashMap<SessionLabel, usize> = sessions
            .iter()
            .enumerate()
            .map(|(i, s)| (*s, i))
            .collect();

        Ok(Self {
            root_dir: root_path,
            calendar,
            sids,
            sessions,
            session_idx,
            first_trading_minute: Some(first_minute),
            last_trading_minute: Some(last_minute),
            cache: Arc::new(RwLock::new(HashMap::new())),
            max_cache_size: 50, // Cache 50 session-assets (e.g., 5 assets * 10 days)
            minutes_per_session,
        })
    }

    /// Create reader for US equities (390 minutes per session)
    pub fn us_equity<P: AsRef<Path>>(
        root_dir: P,
        calendar: Option<Arc<dyn TradingCalendar>>,
    ) -> Result<Self> {
        Self::new(root_dir, calendar, 390)
    }

    /// Get the minute equities directory path
    fn minute_equities_path(&self) -> PathBuf {
        self.root_dir.join("minute_equities")
    }

    /// Get the path for a specific asset
    fn asset_path(&self, sid: u64) -> PathBuf {
        self.minute_equities_path().join(sid.to_string())
    }

    /// Read time range and sessions from an asset's data
    fn read_time_range_and_sessions(
        minute_path: &Path,
        sid: u64,
    ) -> Result<(DateTime<Utc>, DateTime<Utc>, Vec<SessionLabel>)> {
        let asset_path = minute_path.join(sid.to_string());

        // Try 'minute' column first, fallback to 'date'
        let timestamps = read_column_i64(&asset_path, "minute")
            .or_else(|_| read_column_i64(&asset_path, "date"))?;

        if timestamps.is_empty() {
            return Err(ZiplineError::InvalidData(format!(
                "No timestamp data for asset {}",
                sid
            )));
        }

        // Convert to DateTime
        let first_dt = Self::convert_timestamp_to_datetime(timestamps[0])?;
        let last_dt = Self::convert_timestamp_to_datetime(timestamps[timestamps.len() - 1])?;

        // Extract unique sessions
        let mut session_set = std::collections::HashSet::new();
        for ts in timestamps {
            let dt = Self::convert_timestamp_to_datetime(ts)?;
            session_set.insert(SessionLabel::from_datetime(dt));
        }

        let mut sessions: Vec<SessionLabel> = session_set.into_iter().collect();
        sessions.sort_by_key(|s| (s.year, s.month, s.day));

        Ok((first_dt, last_dt, sessions))
    }

    /// Convert timestamp to DateTime
    fn convert_timestamp_to_datetime(timestamp: i64) -> Result<DateTime<Utc>> {
        // Handle different timestamp formats
        if timestamp > 1_000_000_000_000_000_000 {
            // Nanoseconds since epoch
            let secs = timestamp / 1_000_000_000;
            let nsecs = (timestamp % 1_000_000_000) as u32;
            Utc.timestamp_opt(secs, nsecs)
                .single()
                .ok_or_else(|| ZiplineError::InvalidData(format!("Invalid timestamp: {}", timestamp)))
        } else if timestamp > 1_000_000_000_000 {
            // Milliseconds since epoch
            let secs = timestamp / 1000;
            let millis = (timestamp % 1000) as u32;
            Utc.timestamp_opt(secs, millis * 1_000_000)
                .single()
                .ok_or_else(|| ZiplineError::InvalidData(format!("Invalid timestamp: {}", timestamp)))
        } else {
            // Seconds since epoch
            Utc.timestamp_opt(timestamp, 0)
                .single()
                .ok_or_else(|| ZiplineError::InvalidData(format!("Invalid timestamp: {}", timestamp)))
        }
    }

    /// Load all bars for an asset (not cached - used for range queries)
    fn load_all_asset_data(&self, sid: u64) -> Result<(Vec<Bar>, Vec<DateTime<Utc>>)> {
        let asset_path = self.asset_path(sid);

        if !asset_path.exists() {
            return Err(ZiplineError::AssetNotFound(sid));
        }

        // Read timestamp column
        let timestamps = read_column_i64(&asset_path, "minute")
            .or_else(|_| read_column_i64(&asset_path, "date"))?;

        // Read OHLCV columns
        let opens = read_column_f64(&asset_path, "open")?;
        let highs = read_column_f64(&asset_path, "high")?;
        let lows = read_column_f64(&asset_path, "low")?;
        let closes = read_column_f64(&asset_path, "close")?;
        let volumes = read_column_f64(&asset_path, "volume")?;

        // Validate lengths
        let n = timestamps.len();
        if opens.len() != n || highs.len() != n || lows.len() != n || closes.len() != n || volumes.len() != n {
            return Err(ZiplineError::InvalidData(format!(
                "Column length mismatch for asset {}",
                sid
            )));
        }

        // Build bars
        let mut bars = Vec::with_capacity(n);
        let mut dts = Vec::with_capacity(n);

        for i in 0..n {
            let dt = Self::convert_timestamp_to_datetime(timestamps[i])?;
            dts.push(dt);

            let bar = Bar::new(opens[i], highs[i], lows[i], closes[i], volumes[i], dt);

            if !bar.is_valid() {
                log::warn!("Invalid bar for asset {} at {:?}", sid, dt);
            }

            bars.push(bar);
        }

        Ok((bars, dts))
    }

    /// Load bars for a specific session (cached)
    fn load_session_data(&self, sid: u64, session: SessionLabel) -> Result<SessionBars> {
        let cache_key = (sid, session);

        // Check cache first
        {
            let cache = self.cache.read().unwrap();
            if let Some(cached) = cache.get(&cache_key) {
                return Ok(cached.clone());
            }
        }

        // Load all data for the asset
        let (all_bars, all_timestamps) = self.load_all_asset_data(sid)?;

        // Filter to the requested session
        let session_start = session.to_datetime()?;
        let session_end = session_start + chrono::Duration::days(1);

        let mut session_bars = Vec::new();
        let mut session_timestamps = Vec::new();

        for (bar, ts) in all_bars.iter().zip(all_timestamps.iter()) {
            if *ts >= session_start && *ts < session_end {
                session_bars.push(*bar);
                session_timestamps.push(*ts);
            }
        }

        let session_data = SessionBars {
            bars: session_bars,
            timestamps: session_timestamps,
        };

        // Store in cache
        {
            let mut cache = self.cache.write().unwrap();

            // Implement LRU eviction
            if cache.len() >= self.max_cache_size {
                // Remove oldest entry (simple FIFO)
                if let Some(&key) = cache.keys().next() {
                    cache.remove(&key);
                }
            }

            cache.insert(cache_key, session_data.clone());
        }

        Ok(session_data)
    }

    /// Find bar index for a given timestamp within a session
    fn find_bar_index(timestamps: &[DateTime<Utc>], target_dt: DateTime<Utc>) -> Option<usize> {
        timestamps
            .binary_search_by(|dt| dt.cmp(&target_dt))
            .ok()
            .or_else(|| {
                // Find closest previous timestamp
                let idx = timestamps.partition_point(|dt| dt < &target_dt);
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

    /// Get first trading minute
    pub fn first_trading_minute(&self) -> Option<DateTime<Utc>> {
        self.first_trading_minute
    }

    /// Get last trading minute
    pub fn last_trading_minute(&self) -> Option<DateTime<Utc>> {
        self.last_trading_minute
    }

    /// Get minutes per session
    pub fn minutes_per_session(&self) -> usize {
        self.minutes_per_session
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

    /// Get all bars for a specific session
    pub fn get_session_bars(&self, asset: &Asset, session: SessionLabel) -> Result<Vec<Bar>> {
        let session_data = self.load_session_data(asset.id, session)?;
        Ok(session_data.bars)
    }
}

impl BarReader for BcolzMinuteBarReader {
    fn get_bar(&self, asset: &Asset, dt: DateTime<Utc>) -> Result<Bar> {
        let session = SessionLabel::from_datetime(dt);
        let session_data = self.load_session_data(asset.id, session)?;

        if let Some(idx) = Self::find_bar_index(&session_data.timestamps, dt) {
            Ok(session_data.bars[idx])
        } else {
            Err(ZiplineError::DataNotFound(format!(
                "No bar data for asset {} at {:?}",
                asset.symbol, dt
            )))
        }
    }

    fn get_bars(&self, asset: &Asset, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Bar>> {
        // Determine which sessions are needed
        let start_session = SessionLabel::from_datetime(start);
        let end_session = SessionLabel::from_datetime(end);

        let start_idx = self.session_idx.get(&start_session).copied().unwrap_or(0);
        let end_idx = self
            .session_idx
            .get(&end_session)
            .copied()
            .unwrap_or(self.sessions.len().saturating_sub(1));

        let mut all_bars = Vec::new();

        // Load bars from each session in the range
        for session_idx in start_idx..=end_idx.min(self.sessions.len().saturating_sub(1)) {
            let session = self.sessions[session_idx];
            let session_data = self.load_session_data(asset.id, session)?;

            for (bar, ts) in session_data.bars.iter().zip(session_data.timestamps.iter()) {
                if *ts >= start && *ts <= end {
                    all_bars.push(*bar);
                }
            }
        }

        Ok(all_bars)
    }

    fn last_available_dt(&self, asset: &Asset) -> Result<DateTime<Utc>> {
        // Load data from last session
        if let Some(&last_session) = self.sessions.last() {
            let session_data = self.load_session_data(asset.id, last_session)?;
            session_data.timestamps.last().copied().ok_or_else(|| {
                ZiplineError::DataNotFound(format!("No data for asset {}", asset.symbol))
            })
        } else {
            Err(ZiplineError::DataNotFound(
                "No sessions available".to_string(),
            ))
        }
    }

    fn first_available_dt(&self, asset: &Asset) -> Result<DateTime<Utc>> {
        // Load data from first session
        if let Some(&first_session) = self.sessions.first() {
            let session_data = self.load_session_data(asset.id, first_session)?;
            session_data.timestamps.first().copied().ok_or_else(|| {
                ZiplineError::DataNotFound(format!("No data for asset {}", asset.symbol))
            })
        } else {
            Err(ZiplineError::DataNotFound(
                "No sessions available".to_string(),
            ))
        }
    }

    fn sessions(&self) -> Result<Vec<SessionLabel>> {
        Ok(self.sessions.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_minute_data(
        path: &Path,
        sid: u64,
        session: SessionLabel,
        minutes: usize,
    ) -> Result<()> {
        let asset_path = path.join(sid.to_string());
        fs::create_dir_all(&asset_path)?;

        let meta_path = asset_path.join("meta");
        fs::create_dir_all(&meta_path)?;

        let session_dt = session.to_datetime()?;
        let market_open = session_dt
            .with_hour(9)
            .and_then(|dt| dt.with_minute(30))
            .unwrap();

        let mut timestamps = Vec::new();
        let mut opens = Vec::new();
        let mut highs = Vec::new();
        let mut lows = Vec::new();
        let mut closes = Vec::new();
        let mut volumes = Vec::new();

        for i in 0..minutes {
            let dt = market_open + chrono::Duration::minutes(i as i64);
            let ts = dt.timestamp();

            timestamps.extend_from_slice(&ts.to_le_bytes());
            opens.extend_from_slice(&(100.0 + i as f64 * 0.1).to_le_bytes());
            highs.extend_from_slice(&(100.5 + i as f64 * 0.1).to_le_bytes());
            lows.extend_from_slice(&(99.5 + i as f64 * 0.1).to_le_bytes());
            closes.extend_from_slice(&(100.2 + i as f64 * 0.1).to_le_bytes());
            volumes.extend_from_slice(&(10000.0 + i as f64 * 100.0).to_le_bytes());
        }

        fs::write(asset_path.join("minute.00000"), timestamps)?;
        fs::write(asset_path.join("open.00000"), opens)?;
        fs::write(asset_path.join("high.00000"), highs)?;
        fs::write(asset_path.join("low.00000"), lows)?;
        fs::write(asset_path.join("close.00000"), closes)?;
        fs::write(asset_path.join("volume.00000"), volumes)?;

        Ok(())
    }

    #[test]
    fn test_bcolz_minute_bar_reader() {
        let temp_dir = TempDir::new().unwrap();
        let bundle_path = temp_dir.path();
        let minute_path = bundle_path.join("minute_equities");
        fs::create_dir_all(&minute_path).unwrap();

        // Create test data
        let session = SessionLabel {
            year: 2020,
            month: 1,
            day: 2,
        };
        create_test_minute_data(&minute_path, 1, session, 390).unwrap();

        // Create reader
        let reader = BcolzMinuteBarReader::us_equity(bundle_path, None).unwrap();

        assert_eq!(reader.sids(), &[1]);
        assert_eq!(reader.minutes_per_session(), 390);

        // Test get_session_bars
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "TEST".to_string(), "NYSE".to_string(), start_date);
        let bars = reader.get_session_bars(&asset, session).unwrap();
        assert_eq!(bars.len(), 390);
    }

    #[test]
    fn test_convert_timestamp() {
        // Test seconds
        let secs = 1577836800i64; // 2020-01-01 00:00:00 UTC
        let dt = BcolzMinuteBarReader::convert_timestamp_to_datetime(secs).unwrap();
        assert_eq!(dt.year(), 2020);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1);

        // Test nanoseconds
        let nanos = 1577836800_000_000_000i64;
        let dt = BcolzMinuteBarReader::convert_timestamp_to_datetime(nanos).unwrap();
        assert_eq!(dt.year(), 2020);
    }
}
