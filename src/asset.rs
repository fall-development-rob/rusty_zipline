//! Asset representations

use crate::types::{AssetId, Symbol};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Type of asset
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AssetType {
    /// Common stock
    Equity,
    /// Futures contract
    Future,
    /// Options contract
    Option,
    /// Foreign exchange
    Forex,
    /// Cryptocurrency
    Crypto,
}

/// Asset representation
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Asset {
    /// Unique asset identifier
    pub id: AssetId,
    /// Trading symbol
    pub symbol: Symbol,
    /// Exchange where asset is traded
    pub exchange: String,
    /// Type of asset
    pub asset_type: AssetType,
    /// Asset name
    pub name: Option<String>,
    /// First date the asset was traded
    pub start_date: NaiveDate,
    /// Last date the asset was traded (None if still active)
    pub end_date: Option<NaiveDate>,
    /// Date when positions in this asset should be auto-closed
    pub auto_close_date: Option<NaiveDate>,
}

impl Asset {
    /// Create a new asset
    pub fn new(
        id: AssetId,
        symbol: Symbol,
        exchange: String,
        asset_type: AssetType,
        start_date: NaiveDate,
    ) -> Self {
        Self {
            id,
            symbol,
            exchange,
            asset_type,
            name: None,
            start_date,
            end_date: None,
            auto_close_date: None,
        }
    }

    /// Create a new asset with a name
    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    /// Set the end date for this asset
    pub fn with_end_date(mut self, end_date: NaiveDate) -> Self {
        self.end_date = Some(end_date);
        self
    }

    /// Set the auto close date for this asset
    pub fn with_auto_close_date(mut self, auto_close_date: NaiveDate) -> Self {
        self.auto_close_date = Some(auto_close_date);
        self
    }

    /// Create an equity asset
    pub fn equity(id: AssetId, symbol: Symbol, exchange: String, start_date: NaiveDate) -> Self {
        Self::new(id, symbol, exchange, AssetType::Equity, start_date)
    }

    /// Get the full identifier (symbol@exchange)
    pub fn full_id(&self) -> String {
        format!("{}@{}", self.symbol, self.exchange)
    }

    /// Check if the asset is alive (tradeable) for a given session date
    ///
    /// An asset is alive if:
    /// - The session date is on or after the start_date
    /// - The session date is before the end_date (if set)
    pub fn is_alive_for_session(&self, dt: NaiveDate) -> bool {
        // Check if session is after start date
        if dt < self.start_date {
            return false;
        }

        // Check if session is before end date (if set)
        if let Some(end_date) = self.end_date {
            if dt > end_date {
                return false;
            }
        }

        true
    }

    /// Check if the exchange is open at the given datetime
    ///
    /// This is a basic implementation that checks trading hours.
    /// For a production system, this should be enhanced with:
    /// - Exchange-specific trading calendars
    /// - Holiday schedules
    /// - Market hours per exchange
    pub fn is_exchange_open(&self, dt: DateTime<Utc>) -> bool {
        // Basic check: trading hours are Monday-Friday, 9:30 AM - 4:00 PM ET
        // This is a simplified implementation and should be enhanced with proper calendar support

        use chrono::Datelike;
        let weekday = dt.weekday();

        // Check if it's a weekday (Monday = 0, Sunday = 6)
        if weekday.num_days_from_monday() >= 5 {
            return false; // Weekend
        }

        // For a more sophisticated implementation, you would:
        // 1. Convert to exchange's timezone
        // 2. Check against exchange-specific market hours
        // 3. Check against holiday calendar
        // 4. Handle pre-market and after-hours sessions

        // Currently returns true for weekdays as a basic implementation
        true
    }
}

impl fmt::Display for Asset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Asset({}, {}, {:?})",
            self.symbol, self.exchange, self.asset_type
        )
    }
}

impl fmt::Display for AssetType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AssetType::Equity => write!(f, "Equity"),
            AssetType::Future => write!(f, "Future"),
            AssetType::Option => write!(f, "Option"),
            AssetType::Forex => write!(f, "Forex"),
            AssetType::Crypto => write!(f, "Crypto"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, TimeZone};

    #[test]
    fn test_asset_creation() {
        let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);
        assert_eq!(asset.symbol, "AAPL");
        assert_eq!(asset.exchange, "NASDAQ");
        assert_eq!(asset.asset_type, AssetType::Equity);
        assert_eq!(asset.full_id(), "AAPL@NASDAQ");
        assert_eq!(asset.start_date, start_date);
        assert_eq!(asset.end_date, None);
        assert_eq!(asset.auto_close_date, None);
    }

    #[test]
    fn test_asset_with_name() {
        let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date)
            .with_name("Apple Inc.".to_string());
        assert_eq!(asset.name, Some("Apple Inc.".to_string()));
    }

    #[test]
    fn test_asset_with_dates() {
        let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(2020, 12, 31).unwrap();
        let auto_close_date = NaiveDate::from_ymd_opt(2020, 12, 30).unwrap();

        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date)
            .with_end_date(end_date)
            .with_auto_close_date(auto_close_date);

        assert_eq!(asset.start_date, start_date);
        assert_eq!(asset.end_date, Some(end_date));
        assert_eq!(asset.auto_close_date, Some(auto_close_date));
    }

    #[test]
    fn test_is_alive_for_session() {
        let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(2020, 12, 31).unwrap();

        // Asset without end date
        let active_asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);

        // Should be alive for dates after start
        assert!(active_asset.is_alive_for_session(NaiveDate::from_ymd_opt(2010, 6, 15).unwrap()));
        assert!(active_asset.is_alive_for_session(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()));

        // Should not be alive before start
        assert!(!active_asset.is_alive_for_session(NaiveDate::from_ymd_opt(1999, 12, 31).unwrap()));

        // Asset with end date
        let delisted_asset = Asset::equity(2, "XYZ".to_string(), "NYSE".to_string(), start_date)
            .with_end_date(end_date);

        // Should be alive within range
        assert!(delisted_asset.is_alive_for_session(NaiveDate::from_ymd_opt(2010, 6, 15).unwrap()));

        // Should not be alive after end date
        assert!(!delisted_asset.is_alive_for_session(NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()));
    }

    #[test]
    fn test_is_exchange_open() {
        let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string(), start_date);

        // Monday
        let monday = Utc.with_ymd_and_hms(2025, 11, 3, 14, 30, 0).unwrap();
        assert!(asset.is_exchange_open(monday));

        // Friday
        let friday = Utc.with_ymd_and_hms(2025, 11, 7, 14, 30, 0).unwrap();
        assert!(asset.is_exchange_open(friday));

        // Saturday
        let saturday = Utc.with_ymd_and_hms(2025, 11, 8, 14, 30, 0).unwrap();
        assert!(!asset.is_exchange_open(saturday));

        // Sunday
        let sunday = Utc.with_ymd_and_hms(2025, 11, 9, 14, 30, 0).unwrap();
        assert!(!asset.is_exchange_open(sunday));
    }
}
