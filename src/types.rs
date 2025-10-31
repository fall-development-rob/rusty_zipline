//! Core types and constants

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Timestamp type used throughout the library
pub type Timestamp = DateTime<Utc>;

/// Symbol identifier for assets
pub type Symbol = String;

/// Price type (using f64 for precision)
pub type Price = f64;

/// Quantity/volume type
pub type Quantity = f64;

/// Money/cash type
pub type Cash = f64;

/// Percentage type (0.0 to 1.0)
pub type Percentage = f64;

/// Unique identifier for orders
pub type OrderId = uuid::Uuid;

/// Unique identifier for assets
pub type AssetId = u64;

/// OHLCV bar data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bar {
    pub timestamp: Timestamp,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: Quantity,
}

impl Bar {
    /// Create a new bar
    pub fn new(
        timestamp: Timestamp,
        open: Price,
        high: Price,
        low: Price,
        close: Price,
        volume: Quantity,
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

    /// Get typical price (HLC/3)
    pub fn typical_price(&self) -> Price {
        (self.high + self.low + self.close) / 3.0
    }

    /// Get price range (high - low)
    pub fn range(&self) -> Price {
        self.high - self.low
    }

    /// Check if bar is bullish
    pub fn is_bullish(&self) -> bool {
        self.close > self.open
    }

    /// Check if bar is bearish
    pub fn is_bearish(&self) -> bool {
        self.close < self.open
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_bar_calculations() {
        let bar = Bar::new(Utc::now(), 100.0, 105.0, 99.0, 103.0, 1000.0);

        assert_eq!(bar.typical_price(), (105.0 + 99.0 + 103.0) / 3.0);
        assert_eq!(bar.range(), 6.0);
        assert!(bar.is_bullish());
        assert!(!bar.is_bearish());
    }
}
