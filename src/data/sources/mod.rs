//! External data source integrations
//!
//! This module provides integrations with popular financial data sources:
//! - Quandl: Historical datasets and economic indicators
//! - Yahoo Finance: Free historical OHLCV data
//! - Alpha Vantage: Intraday and daily market data

#[cfg(feature = "async")]
pub mod quandl;
#[cfg(feature = "async")]
pub mod yahoo;
#[cfg(feature = "async")]
pub mod alpha_vantage;

#[cfg(feature = "async")]
pub use quandl::QuandlDataSource;
#[cfg(feature = "async")]
pub use yahoo::YahooFinanceSource;
#[cfg(feature = "async")]
pub use alpha_vantage::AlphaVantageSource;

use crate::error::{Result, ZiplineError};
use crate::types::Bar;
use chrono::{DateTime, Utc};
use hashbrown::HashMap;
use std::sync::Arc;

/// Registry for managing multiple data sources
#[cfg(feature = "async")]
pub struct DataSourceRegistry {
    sources: HashMap<String, Arc<dyn ExternalDataSource>>,
}

#[cfg(feature = "async")]
impl DataSourceRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
        }
    }

    /// Register a data source
    pub fn register<S: ExternalDataSource + 'static>(
        &mut self,
        name: String,
        source: S,
    ) {
        self.sources.insert(name, Arc::new(source));
    }

    /// Get a registered data source
    pub fn get(&self, name: &str) -> Option<Arc<dyn ExternalDataSource>> {
        self.sources.get(name).cloned()
    }

    /// List all registered source names
    pub fn list_sources(&self) -> Vec<String> {
        self.sources.keys().cloned().collect()
    }
}

#[cfg(feature = "async")]
impl Default for DataSourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for external data sources
#[cfg(feature = "async")]
pub trait ExternalDataSource: Send + Sync {
    /// Fetch historical data for a single symbol
    fn fetch_historical(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> impl std::future::Future<Output = Result<Vec<Bar>>> + Send;

    /// Get the source name
    fn name(&self) -> &str;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "async")]
    fn test_registry() {
        let registry = DataSourceRegistry::new();
        assert_eq!(registry.list_sources().len(), 0);
    }
}
