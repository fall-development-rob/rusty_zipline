//! Asset representations

use crate::types::{AssetId, Symbol};
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
}

impl Asset {
    /// Create a new asset
    pub fn new(id: AssetId, symbol: Symbol, exchange: String, asset_type: AssetType) -> Self {
        Self {
            id,
            symbol,
            exchange,
            asset_type,
            name: None,
        }
    }

    /// Create a new asset with a name
    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    /// Create an equity asset
    pub fn equity(id: AssetId, symbol: Symbol, exchange: String) -> Self {
        Self::new(id, symbol, exchange, AssetType::Equity)
    }

    /// Get the full identifier (symbol@exchange)
    pub fn full_id(&self) -> String {
        format!("{}@{}", self.symbol, self.exchange)
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

    #[test]
    fn test_asset_creation() {
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string());
        assert_eq!(asset.symbol, "AAPL");
        assert_eq!(asset.exchange, "NASDAQ");
        assert_eq!(asset.asset_type, AssetType::Equity);
        assert_eq!(asset.full_id(), "AAPL@NASDAQ");
    }

    #[test]
    fn test_asset_with_name() {
        let asset = Asset::equity(1, "AAPL".to_string(), "NASDAQ".to_string())
            .with_name("Apple Inc.".to_string());
        assert_eq!(asset.name, Some("Apple Inc.".to_string()));
    }
}
