//! History Loader - Historical window management with caching
//!
//! Efficiently loads and caches historical bar data for strategy execution.

use crate::asset::Asset;
use crate::data::bar_reader::{Bar, BarReader};
use crate::error::{Result, ZiplineError};
use chrono::{DateTime, Duration, Utc};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

/// Field types for historical data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HistoryField {
    Open,
    High,
    Low,
    Close,
    Volume,
}

impl HistoryField {
    /// Extract value from bar
    pub fn extract(&self, bar: &Bar) -> f64 {
        match self {
            HistoryField::Open => bar.open,
            HistoryField::High => bar.high,
            HistoryField::Low => bar.low,
            HistoryField::Close => bar.close,
            HistoryField::Volume => bar.volume,
        }
    }

    /// All available fields
    pub fn all_fields() -> Vec<HistoryField> {
        vec![
            HistoryField::Open,
            HistoryField::High,
            HistoryField::Low,
            HistoryField::Close,
            HistoryField::Volume,
        ]
    }
}

/// Frequency for historical data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Frequency {
    /// Daily data
    Daily,
    /// Minute data
    Minute,
}

impl Frequency {
    /// Duration for this frequency
    pub fn to_duration(&self) -> Duration {
        match self {
            Frequency::Daily => Duration::days(1),
            Frequency::Minute => Duration::minutes(1),
        }
    }
}

/// Historical data window for a single asset and field
#[derive(Debug, Clone)]
pub struct HistoryWindow {
    /// Asset ID
    asset_id: u64,
    /// Field type
    field: HistoryField,
    /// Frequency
    frequency: Frequency,
    /// Window size (number of bars)
    window_size: usize,
    /// Historical values (oldest first)
    values: VecDeque<f64>,
    /// Timestamps for each value
    timestamps: VecDeque<DateTime<Utc>>,
}

impl HistoryWindow {
    pub fn new(
        asset_id: u64,
        field: HistoryField,
        frequency: Frequency,
        window_size: usize,
    ) -> Self {
        Self {
            asset_id,
            field,
            frequency,
            window_size,
            values: VecDeque::with_capacity(window_size),
            timestamps: VecDeque::with_capacity(window_size),
        }
    }

    /// Add a new bar to the window
    pub fn update(&mut self, bar: &Bar) {
        let value = self.field.extract(bar);

        self.values.push_back(value);
        self.timestamps.push_back(bar.dt);

        // Remove oldest if exceeding window size
        if self.values.len() > self.window_size {
            self.values.pop_front();
            self.timestamps.pop_front();
        }
    }

    /// Get current window values
    pub fn values(&self) -> &VecDeque<f64> {
        &self.values
    }

    /// Get window as slice (if full)
    pub fn as_slice(&self) -> Option<Vec<f64>> {
        if self.is_full() {
            Some(self.values.iter().copied().collect())
        } else {
            None
        }
    }

    /// Check if window is full
    pub fn is_full(&self) -> bool {
        self.values.len() == self.window_size
    }

    /// Current window size
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if window is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get latest value
    pub fn latest(&self) -> Option<f64> {
        self.values.back().copied()
    }

    /// Get oldest value
    pub fn oldest(&self) -> Option<f64> {
        self.values.front().copied()
    }

    /// Clear the window
    pub fn clear(&mut self) {
        self.values.clear();
        self.timestamps.clear();
    }
}

/// Cache entry for historical data
#[derive(Debug, Clone)]
struct CacheEntry {
    /// Cached bars
    bars: Vec<Bar>,
    /// Last update time
    last_updated: DateTime<Utc>,
}

/// History Loader - manages historical data windows
pub struct HistoryLoader {
    /// Bar reader
    bar_reader: Arc<dyn BarReader>,
    /// Cache: (asset_id, start, end) -> CacheEntry
    cache: Arc<RwLock<HashMap<(u64, DateTime<Utc>, DateTime<Utc>), CacheEntry>>>,
    /// Maximum cache size (number of entries)
    max_cache_size: usize,
    /// Cache hit/miss statistics
    cache_hits: Arc<RwLock<usize>>,
    cache_misses: Arc<RwLock<usize>>,
}

impl std::fmt::Debug for HistoryLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cache_size = self.cache.read().map(|c| c.len()).unwrap_or(0);
        let hits = self.cache_hits.read().map(|h| *h).unwrap_or(0);
        let misses = self.cache_misses.read().map(|m| *m).unwrap_or(0);

        f.debug_struct("HistoryLoader")
            .field("bar_reader", &"<dyn BarReader>")
            .field("cache_size", &cache_size)
            .field("max_cache_size", &self.max_cache_size)
            .field("cache_hits", &hits)
            .field("cache_misses", &misses)
            .finish()
    }
}

impl HistoryLoader {
    /// Create new history loader
    pub fn new(bar_reader: Arc<dyn BarReader>) -> Self {
        Self {
            bar_reader,
            cache: Arc::new(RwLock::new(HashMap::new())),
            max_cache_size: 1000,
            cache_hits: Arc::new(RwLock::new(0)),
            cache_misses: Arc::new(RwLock::new(0)),
        }
    }

    /// Create with custom cache size
    pub fn with_cache_size(bar_reader: Arc<dyn BarReader>, max_cache_size: usize) -> Self {
        Self {
            bar_reader,
            cache: Arc::new(RwLock::new(HashMap::new())),
            max_cache_size,
            cache_hits: Arc::new(RwLock::new(0)),
            cache_misses: Arc::new(RwLock::new(0)),
        }
    }

    /// Load historical data for a single asset
    pub fn load_history(
        &self,
        asset: &Asset,
        field: HistoryField,
        window_size: usize,
        end_dt: DateTime<Utc>,
        frequency: Frequency,
    ) -> Result<Vec<f64>> {
        let start_dt = self.compute_start_date(end_dt, window_size, frequency);

        // Try cache first
        let cache_key = (asset.id, start_dt, end_dt);
        let bars = if let Some(cached) = self.get_from_cache(&cache_key) {
            cached
        } else {
            // Load from bar reader
            let bars = self.bar_reader.get_bars(asset, start_dt, end_dt)?;

            // Update cache
            self.update_cache(cache_key, bars.clone());

            bars
        };

        // Extract field values
        let values: Vec<f64> = bars.iter().map(|bar| field.extract(bar)).collect();

        if values.len() < window_size {
            return Err(ZiplineError::DataNotFound(format!(
                "Insufficient data: requested {} bars, got {}",
                window_size,
                values.len()
            )));
        }

        // Return last window_size values
        Ok(values.iter().rev().take(window_size).rev().copied().collect())
    }

    /// Load history for multiple assets
    pub fn load_history_multiple(
        &self,
        assets: &[Asset],
        field: HistoryField,
        window_size: usize,
        end_dt: DateTime<Utc>,
        frequency: Frequency,
    ) -> Result<HashMap<u64, Vec<f64>>> {
        let mut result = HashMap::new();

        for asset in assets {
            let values = self.load_history(asset, field, window_size, end_dt, frequency)?;
            result.insert(asset.id, values);
        }

        Ok(result)
    }

    /// Create a history window
    pub fn create_window(
        &self,
        asset: &Asset,
        field: HistoryField,
        window_size: usize,
        end_dt: DateTime<Utc>,
        frequency: Frequency,
    ) -> Result<HistoryWindow> {
        let mut window = HistoryWindow::new(asset.id, field, frequency, window_size);

        let start_dt = self.compute_start_date(end_dt, window_size, frequency);
        let bars = self.bar_reader.get_bars(asset, start_dt, end_dt)?;

        for bar in &bars {
            window.update(bar);
        }

        Ok(window)
    }

    /// Load multiple fields for an asset
    pub fn load_multi_field(
        &self,
        asset: &Asset,
        fields: &[HistoryField],
        window_size: usize,
        end_dt: DateTime<Utc>,
        frequency: Frequency,
    ) -> Result<HashMap<HistoryField, Vec<f64>>> {
        let mut result = HashMap::new();

        for field in fields {
            let values = self.load_history(asset, *field, window_size, end_dt, frequency)?;
            result.insert(*field, values);
        }

        Ok(result)
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> (usize, usize, f64) {
        let hits = *self.cache_hits.read().unwrap();
        let misses = *self.cache_misses.read().unwrap();
        let total = hits + misses;
        let hit_rate = if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        };

        (hits, misses, hit_rate)
    }

    /// Clear cache
    pub fn clear_cache(&self) {
        self.cache.write().unwrap().clear();
        *self.cache_hits.write().unwrap() = 0;
        *self.cache_misses.write().unwrap() = 0;
    }

    /// Get cache size
    pub fn cache_size(&self) -> usize {
        self.cache.read().unwrap().len()
    }

    fn compute_start_date(
        &self,
        end_dt: DateTime<Utc>,
        window_size: usize,
        frequency: Frequency,
    ) -> DateTime<Utc> {
        let duration = frequency.to_duration() * window_size as i32;
        end_dt - duration
    }

    fn get_from_cache(
        &self,
        key: &(u64, DateTime<Utc>, DateTime<Utc>),
    ) -> Option<Vec<Bar>> {
        let cache = self.cache.read().unwrap();
        if let Some(entry) = cache.get(key) {
            *self.cache_hits.write().unwrap() += 1;
            Some(entry.bars.clone())
        } else {
            *self.cache_misses.write().unwrap() += 1;
            None
        }
    }

    fn update_cache(&self, key: (u64, DateTime<Utc>, DateTime<Utc>), bars: Vec<Bar>) {
        let mut cache = self.cache.write().unwrap();

        // Evict oldest entry if cache is full
        if cache.len() >= self.max_cache_size && !cache.contains_key(&key) {
            if let Some(oldest_key) = cache.keys().next().copied() {
                cache.remove(&oldest_key);
            }
        }

        cache.insert(
            key,
            CacheEntry {
                bars,
                last_updated: Utc::now(),
            },
        );
    }
}

/// Batch history loader for efficient multi-asset loading
pub struct BatchHistoryLoader {
    loader: HistoryLoader,
    /// Pre-fetch window size
    prefetch_window: usize,
}

impl BatchHistoryLoader {
    pub fn new(bar_reader: Arc<dyn BarReader>) -> Self {
        Self {
            loader: HistoryLoader::new(bar_reader),
            prefetch_window: 100,
        }
    }

    pub fn with_prefetch(bar_reader: Arc<dyn BarReader>, prefetch_window: usize) -> Self {
        Self {
            loader: HistoryLoader::new(bar_reader),
            prefetch_window,
        }
    }

    /// Load history for all assets in batch
    pub fn load_batch(
        &self,
        assets: &[Asset],
        fields: &[HistoryField],
        window_size: usize,
        end_dt: DateTime<Utc>,
        frequency: Frequency,
    ) -> Result<HashMap<(u64, HistoryField), Vec<f64>>> {
        let mut result = HashMap::new();

        for asset in assets {
            for field in fields {
                let values =
                    self.loader
                        .load_history(asset, *field, window_size, end_dt, frequency)?;
                result.insert((asset.id, *field), values);
            }
        }

        Ok(result)
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> (usize, usize, f64) {
        self.loader.cache_stats()
    }

    /// Clear cache
    pub fn clear_cache(&self) {
        self.loader.clear_cache();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::bar_reader::{Bar, DailyBarReader};

    fn create_test_asset() -> Asset {
        Asset {
            id: 1,
            symbol: "AAPL".to_string(),
            exchange: "NYSE".to_string(),
            asset_type: crate::asset::AssetType::Equity,
        }
    }

    fn create_test_bars(count: usize) -> Vec<Bar> {
        let mut bars = Vec::new();
        let base_dt = Utc::now();

        for i in 0..count {
            let dt = base_dt + Duration::days(i as i64);
            bars.push(Bar::new(
                100.0 + i as f64,
                105.0 + i as f64,
                99.0 + i as f64,
                102.0 + i as f64,
                1000000.0,
                dt,
            ));
        }

        bars
    }

    #[test]
    fn test_history_field_extract() {
        let dt = Utc::now();
        let bar = Bar::new(100.0, 105.0, 99.0, 102.0, 1000000.0, dt);

        assert_eq!(HistoryField::Open.extract(&bar), 100.0);
        assert_eq!(HistoryField::High.extract(&bar), 105.0);
        assert_eq!(HistoryField::Low.extract(&bar), 99.0);
        assert_eq!(HistoryField::Close.extract(&bar), 102.0);
        assert_eq!(HistoryField::Volume.extract(&bar), 1000000.0);
    }

    #[test]
    fn test_history_window() {
        let mut window = HistoryWindow::new(1, HistoryField::Close, Frequency::Daily, 5);

        assert!(window.is_empty());
        assert!(!window.is_full());

        // Add bars
        let bars = create_test_bars(5);
        for bar in &bars {
            window.update(bar);
        }

        assert!(window.is_full());
        assert_eq!(window.len(), 5);
        assert_eq!(window.latest().unwrap(), 106.0); // 102 + 4
        assert_eq!(window.oldest().unwrap(), 102.0); // 102 + 0
    }

    #[test]
    fn test_history_window_overflow() {
        let mut window = HistoryWindow::new(1, HistoryField::Close, Frequency::Daily, 3);

        let bars = create_test_bars(5);
        for bar in &bars {
            window.update(bar);
        }

        // Should only keep last 3
        assert_eq!(window.len(), 3);
        assert_eq!(window.latest().unwrap(), 106.0); // Last bar
        assert_eq!(window.oldest().unwrap(), 104.0); // bars[2]
    }

    #[test]
    fn test_history_loader() {
        let mut bar_reader = DailyBarReader::new();
        let asset = create_test_asset();

        let bars = create_test_bars(10);
        bar_reader.load_from_memory(asset.id, bars.clone()).unwrap();

        let loader = HistoryLoader::new(Arc::new(bar_reader));

        let end_dt = bars.last().unwrap().dt;
        let values = loader
            .load_history(&asset, HistoryField::Close, 5, end_dt, Frequency::Daily)
            .unwrap();

        assert_eq!(values.len(), 5);
        // Last 5 closes: 107, 108, 109, 110, 111 (102 + 5,6,7,8,9)
        assert_eq!(values[0], 107.0);
        assert_eq!(values[4], 111.0);
    }

    #[test]
    fn test_history_loader_cache() {
        let mut bar_reader = DailyBarReader::new();
        let asset = create_test_asset();

        let bars = create_test_bars(10);
        bar_reader.load_from_memory(asset.id, bars.clone()).unwrap();

        let loader = HistoryLoader::new(Arc::new(bar_reader));

        let end_dt = bars.last().unwrap().dt;

        // First load - cache miss
        loader
            .load_history(&asset, HistoryField::Close, 5, end_dt, Frequency::Daily)
            .unwrap();

        let (hits, misses, _) = loader.cache_stats();
        assert_eq!(misses, 1);
        assert_eq!(hits, 0);

        // Second load - cache hit
        loader
            .load_history(&asset, HistoryField::Close, 5, end_dt, Frequency::Daily)
            .unwrap();

        let (hits, misses, hit_rate) = loader.cache_stats();
        assert_eq!(misses, 1);
        assert_eq!(hits, 1);
        assert_eq!(hit_rate, 0.5);
    }

    #[test]
    fn test_history_loader_multiple_assets() {
        let mut bar_reader = DailyBarReader::new();

        let asset1 = Asset {
            id: 1,
            symbol: "AAPL".to_string(),
            exchange: "NYSE".to_string(),
            asset_type: crate::asset::AssetType::Equity,
        };
        let asset2 = Asset {
            id: 2,
            symbol: "GOOGL".to_string(),
            exchange: "NYSE".to_string(),
            asset_type: crate::asset::AssetType::Equity,
        };

        let bars1 = create_test_bars(10);
        let bars2 = create_test_bars(10);

        bar_reader.load_from_memory(asset1.id, bars1.clone()).unwrap();
        bar_reader.load_from_memory(asset2.id, bars2).unwrap();

        let loader = HistoryLoader::new(Arc::new(bar_reader));

        let end_dt = bars1.last().unwrap().dt;
        let result = loader
            .load_history_multiple(
                &[asset1, asset2],
                HistoryField::Close,
                5,
                end_dt,
                Frequency::Daily,
            )
            .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result.get(&1).unwrap().len(), 5);
        assert_eq!(result.get(&2).unwrap().len(), 5);
    }

    #[test]
    fn test_batch_history_loader() {
        let mut bar_reader = DailyBarReader::new();
        let asset = create_test_asset();

        let bars = create_test_bars(10);
        bar_reader.load_from_memory(asset.id, bars.clone()).unwrap();

        let batch_loader = BatchHistoryLoader::new(Arc::new(bar_reader));

        let end_dt = bars.last().unwrap().dt;
        let fields = vec![HistoryField::Open, HistoryField::Close];

        let result = batch_loader
            .load_batch(&[asset], &fields, 5, end_dt, Frequency::Daily)
            .unwrap();

        assert_eq!(result.len(), 2); // 2 fields
        assert!(result.contains_key(&(1, HistoryField::Open)));
        assert!(result.contains_key(&(1, HistoryField::Close)));
    }

    #[test]
    fn test_create_window() {
        let mut bar_reader = DailyBarReader::new();
        let asset = create_test_asset();

        let bars = create_test_bars(10);
        bar_reader.load_from_memory(asset.id, bars.clone()).unwrap();

        let loader = HistoryLoader::new(Arc::new(bar_reader));

        let end_dt = bars.last().unwrap().dt;
        let window = loader
            .create_window(&asset, HistoryField::Close, 5, end_dt, Frequency::Daily)
            .unwrap();

        assert!(window.is_full());
        assert_eq!(window.len(), 5);
    }
}
