//! In-memory FX rate reader implementation
//!
//! Provides fast lookup of FX rates stored in memory using HashMap and BTreeMap.
//! Ideal for backtesting with pre-loaded historical rate data.

use super::base::{Currency, FXRateReader};
use crate::error::{Result, ZiplineError};
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

/// In-memory FX rate storage
///
/// Stores rates as: HashMap<(from, to), BTreeMap<DateTime, rate>>
/// BTreeMap allows efficient range queries and finding nearest rate by time.
///
/// # Example
/// ```
/// use rusty_zipline::data::fx::{InMemoryFXRateReader, Currency, FXRateReader};
/// use chrono::Utc;
///
/// let mut reader = InMemoryFXRateReader::new();
/// let dt = Utc::now();
///
/// // Add rate: 1 EUR = 1.20 USD
/// reader.add_rate(Currency::EUR, Currency::USD, dt, 1.20);
///
/// // Retrieve rate
/// let rate = reader.get_rate(Currency::EUR, Currency::USD, dt).unwrap();
/// assert_eq!(rate, 1.20);
/// ```
#[derive(Debug, Clone)]
pub struct InMemoryFXRateReader {
    /// Rates storage: (from_currency, to_currency) -> (timestamp -> rate)
    rates: Arc<RwLock<HashMap<(Currency, Currency), BTreeMap<DateTime<Utc>, f64>>>>,
    /// Cache for frequently accessed rates
    cache: Arc<RwLock<HashMap<(Currency, Currency, DateTime<Utc>), f64>>>,
    /// Enable automatic inverse rate lookup
    auto_inverse: bool,
    /// Enable cross-rate calculation
    auto_cross: bool,
    /// Base currency for cross-rate calculations (default: USD)
    base_currency: Currency,
}

impl InMemoryFXRateReader {
    /// Create new in-memory FX rate reader
    pub fn new() -> Self {
        Self {
            rates: Arc::new(RwLock::new(HashMap::new())),
            cache: Arc::new(RwLock::new(HashMap::new())),
            auto_inverse: true,
            auto_cross: false,
            base_currency: Currency::USD,
        }
    }

    /// Create with configuration
    pub fn with_config(auto_inverse: bool, auto_cross: bool, base_currency: Currency) -> Self {
        Self {
            rates: Arc::new(RwLock::new(HashMap::new())),
            cache: Arc::new(RwLock::new(HashMap::new())),
            auto_inverse,
            auto_cross,
            base_currency,
        }
    }

    /// Add a single FX rate
    pub fn add_rate(
        &mut self,
        from: Currency,
        to: Currency,
        dt: DateTime<Utc>,
        rate: f64,
    ) -> Result<()> {
        if rate <= 0.0 {
            return Err(ZiplineError::InvalidData(format!(
                "FX rate must be positive, got: {}",
                rate
            )));
        }

        let mut rates = self.rates.write().unwrap();
        rates
            .entry((from, to))
            .or_insert_with(BTreeMap::new)
            .insert(dt, rate);

        // Clear cache entry if it exists
        let mut cache = self.cache.write().unwrap();
        cache.remove(&(from, to, dt));

        Ok(())
    }

    /// Add multiple rates efficiently (batch insert)
    pub fn add_rates(
        &mut self,
        entries: Vec<(Currency, Currency, DateTime<Utc>, f64)>,
    ) -> Result<()> {
        let mut rates = self.rates.write().unwrap();
        let mut cache = self.cache.write().unwrap();

        for (from, to, dt, rate) in entries {
            if rate <= 0.0 {
                return Err(ZiplineError::InvalidData(format!(
                    "FX rate must be positive, got: {}",
                    rate
                )));
            }

            rates
                .entry((from, to))
                .or_insert_with(BTreeMap::new)
                .insert(dt, rate);

            cache.remove(&(from, to, dt));
        }

        Ok(())
    }

    /// Load rates from CSV data
    ///
    /// Expected format: timestamp,from_currency,to_currency,rate
    pub fn load_from_csv(&mut self, csv_data: &str) -> Result<usize> {
        let mut count = 0;
        let mut entries = Vec::new();

        for (line_num, line) in csv_data.lines().enumerate() {
            if line.trim().is_empty() || line.starts_with('#') {
                continue;
            }

            let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
            if parts.len() != 4 {
                return Err(ZiplineError::InvalidData(format!(
                    "Invalid CSV format at line {}: expected 4 columns, got {}",
                    line_num + 1,
                    parts.len()
                )));
            }

            let dt = DateTime::parse_from_rfc3339(parts[0])
                .map_err(|e| {
                    ZiplineError::InvalidData(format!("Invalid timestamp at line {}: {}", line_num + 1, e))
                })?
                .with_timezone(&Utc);

            let from = Currency::from_str(parts[1])?;
            let to = Currency::from_str(parts[2])?;
            let rate: f64 = parts[3].parse().map_err(|e| {
                ZiplineError::InvalidData(format!("Invalid rate at line {}: {}", line_num + 1, e))
            })?;

            entries.push((from, to, dt, rate));
            count += 1;
        }

        self.add_rates(entries)?;
        Ok(count)
    }

    /// Clear all rates
    pub fn clear(&mut self) {
        let mut rates = self.rates.write().unwrap();
        let mut cache = self.cache.write().unwrap();
        rates.clear();
        cache.clear();
    }

    /// Get number of stored rate pairs
    pub fn num_pairs(&self) -> usize {
        let rates = self.rates.read().unwrap();
        rates.len()
    }

    /// Get total number of rate entries (across all pairs and timestamps)
    pub fn num_entries(&self) -> usize {
        let rates = self.rates.read().unwrap();
        rates.values().map(|tree| tree.len()).sum()
    }

    /// Get rate from storage (exact timestamp match)
    fn get_exact_rate(&self, from: Currency, to: Currency, dt: DateTime<Utc>) -> Option<f64> {
        let rates = self.rates.read().unwrap();
        rates.get(&(from, to))?.get(&dt).copied()
    }

    /// Get nearest rate (forward-fill: use most recent rate before or at dt)
    fn get_nearest_rate(&self, from: Currency, to: Currency, dt: DateTime<Utc>) -> Option<f64> {
        let rates = self.rates.read().unwrap();
        let tree = rates.get(&(from, to))?;

        // Find the largest timestamp <= dt
        tree.range(..=dt).next_back().map(|(_, rate)| *rate)
    }

    /// Try to get inverse rate
    fn try_inverse(&self, from: Currency, to: Currency, dt: DateTime<Utc>) -> Option<f64> {
        if !self.auto_inverse {
            return None;
        }

        // Try to get rate in reverse direction
        self.get_nearest_rate(to, from, dt)
            .map(|rate| 1.0 / rate)
    }

    /// Try to calculate cross-rate via base currency
    fn try_cross_rate(&self, from: Currency, to: Currency, dt: DateTime<Utc>) -> Option<f64> {
        if !self.auto_cross {
            return None;
        }

        // Try: from -> base -> to
        let from_to_base = self.get_nearest_rate(from, self.base_currency, dt)?;
        let base_to_to = self.get_nearest_rate(self.base_currency, to, dt)?;

        Some(from_to_base * base_to_to)
    }
}

impl Default for InMemoryFXRateReader {
    fn default() -> Self {
        Self::new()
    }
}

impl FXRateReader for InMemoryFXRateReader {
    fn get_rate(&self, from: Currency, to: Currency, dt: DateTime<Utc>) -> Result<f64> {
        // Same currency always returns 1.0
        if from == to {
            return Ok(1.0);
        }

        // Check cache first
        {
            let cache = self.cache.read().unwrap();
            if let Some(&rate) = cache.get(&(from, to, dt)) {
                return Ok(rate);
            }
        }

        // Try direct lookup
        if let Some(rate) = self.get_nearest_rate(from, to, dt) {
            // Cache the result
            let mut cache = self.cache.write().unwrap();
            cache.insert((from, to, dt), rate);
            return Ok(rate);
        }

        // Try inverse rate
        if let Some(rate) = self.try_inverse(from, to, dt) {
            let mut cache = self.cache.write().unwrap();
            cache.insert((from, to, dt), rate);
            return Ok(rate);
        }

        // Try cross-rate
        if let Some(rate) = self.try_cross_rate(from, to, dt) {
            let mut cache = self.cache.write().unwrap();
            cache.insert((from, to, dt), rate);
            return Ok(rate);
        }

        Err(ZiplineError::MissingData(format!(
            "No FX rate available for {}/{} at {}",
            from.as_str(),
            to.as_str(),
            dt
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_basic_rate_storage() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        reader.add_rate(Currency::EUR, Currency::USD, dt, 1.20).unwrap();

        let rate = reader.get_rate(Currency::EUR, Currency::USD, dt).unwrap();
        assert_eq!(rate, 1.20);
    }

    #[test]
    fn test_same_currency() {
        let reader = InMemoryFXRateReader::new();
        let dt = Utc::now();

        let rate = reader.get_rate(Currency::USD, Currency::USD, dt).unwrap();
        assert_eq!(rate, 1.0);
    }

    #[test]
    fn test_auto_inverse() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        // Add EUR/USD = 1.20
        reader.add_rate(Currency::EUR, Currency::USD, dt, 1.20).unwrap();

        // Should automatically compute USD/EUR = 1/1.20
        let rate = reader.get_rate(Currency::USD, Currency::EUR, dt).unwrap();
        assert!((rate - 0.8333).abs() < 0.001);
    }

    #[test]
    fn test_forward_fill() {
        let mut reader = InMemoryFXRateReader::new();
        let dt1 = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let dt2 = Utc.with_ymd_and_hms(2024, 1, 2, 12, 0, 0).unwrap();
        let dt_query = Utc.with_ymd_and_hms(2024, 1, 1, 18, 0, 0).unwrap();

        reader.add_rate(Currency::EUR, Currency::USD, dt1, 1.20).unwrap();
        reader.add_rate(Currency::EUR, Currency::USD, dt2, 1.22).unwrap();

        // Query between dt1 and dt2 should return dt1's rate
        let rate = reader.get_rate(Currency::EUR, Currency::USD, dt_query).unwrap();
        assert_eq!(rate, 1.20);
    }

    #[test]
    fn test_batch_add() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        let entries = vec![
            (Currency::EUR, Currency::USD, dt, 1.20),
            (Currency::GBP, Currency::USD, dt, 1.30),
            (Currency::JPY, Currency::USD, dt, 0.0091),
        ];

        reader.add_rates(entries).unwrap();

        assert_eq!(reader.num_pairs(), 3);
        assert_eq!(reader.num_entries(), 3);
    }

    #[test]
    fn test_cache() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        reader.add_rate(Currency::EUR, Currency::USD, dt, 1.20).unwrap();

        // First call - should cache
        let rate1 = reader.get_rate(Currency::EUR, Currency::USD, dt).unwrap();

        // Second call - should use cache
        let rate2 = reader.get_rate(Currency::EUR, Currency::USD, dt).unwrap();

        assert_eq!(rate1, rate2);
    }

    #[test]
    fn test_invalid_rate() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        // Negative rate should fail
        let result = reader.add_rate(Currency::EUR, Currency::USD, dt, -1.0);
        assert!(result.is_err());

        // Zero rate should fail
        let result = reader.add_rate(Currency::EUR, Currency::USD, dt, 0.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_cross_rate() {
        let mut reader = InMemoryFXRateReader::with_config(true, true, Currency::USD);
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        // Add EUR/USD = 1.20 and GBP/USD = 1.30
        reader.add_rate(Currency::EUR, Currency::USD, dt, 1.20).unwrap();
        reader.add_rate(Currency::GBP, Currency::USD, dt, 1.30).unwrap();

        // Should calculate EUR/GBP via USD
        // EUR/GBP = (EUR/USD) * (USD/GBP) = 1.20 * (1/1.30) = 0.923
        let rate = reader.get_rate(Currency::EUR, Currency::GBP, dt).unwrap();
        assert!((rate - 0.923).abs() < 0.001);
    }

    #[test]
    fn test_clear() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        reader.add_rate(Currency::EUR, Currency::USD, dt, 1.20).unwrap();
        assert_eq!(reader.num_entries(), 1);

        reader.clear();
        assert_eq!(reader.num_entries(), 0);
    }
}
