//! Yahoo Finance data source integration
//!
//! Provides free access to historical OHLCV data with adjusted prices.

use crate::error::{Result, ZiplineError};
use crate::types::Bar;
use chrono::{DateTime, Utc};
use hashbrown::HashMap;
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;

const YAHOO_BASE_URL: &str = "https://query1.finance.yahoo.com/v7/finance/download";

/// Yahoo Finance data source (no API key required)
pub struct YahooFinanceSource {
    client: Client,
}

#[derive(Debug, Deserialize)]
struct YahooRow {
    #[serde(rename = "Date")]
    date: String,
    #[serde(rename = "Open")]
    open: f64,
    #[serde(rename = "High")]
    high: f64,
    #[serde(rename = "Low")]
    low: f64,
    #[serde(rename = "Close")]
    close: f64,
    #[serde(rename = "Adj Close")]
    adj_close: f64,
    #[serde(rename = "Volume")]
    volume: f64,
}

impl YahooFinanceSource {
    /// Create a new Yahoo Finance data source
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .build()
            .map_err(|e| ZiplineError::DataError(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self { client })
    }

    /// Fetch historical data for a single symbol
    pub async fn fetch_historical(
        &self,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Bar>> {
        let period1 = start.timestamp();
        let period2 = end.timestamp();

        let url = format!(
            "{}/{}?period1={}&period2={}&interval=1d&events=history",
            YAHOO_BASE_URL, symbol, period1, period2
        );

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| ZiplineError::DataError(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(ZiplineError::DataError(format!(
                "Yahoo Finance returned error: {}",
                response.status()
            )));
        }

        let text = response
            .text()
            .await
            .map_err(|e| ZiplineError::DataError(format!("Failed to read response: {}", e)))?;

        self.parse_csv_data(&text)
    }

    /// Fetch data for multiple symbols
    pub async fn fetch_multiple(
        &self,
        symbols: &[&str],
    ) -> Result<HashMap<String, Vec<Bar>>> {
        let mut results = HashMap::new();
        let start = Utc::now() - chrono::Duration::days(365);
        let end = Utc::now();

        for symbol in symbols {
            match self.fetch_historical(symbol, start, end).await {
                Ok(bars) => {
                    results.insert(symbol.to_string(), bars);
                }
                Err(e) => {
                    log::warn!("Failed to fetch data for {}: {}", symbol, e);
                }
            }
        }

        Ok(results)
    }

    fn parse_csv_data(&self, csv_text: &str) -> Result<Vec<Bar>> {
        let mut reader = csv::Reader::from_reader(csv_text.as_bytes());
        let mut bars = Vec::new();

        for result in reader.deserialize() {
            let row: YahooRow = result
                .map_err(|e| ZiplineError::DataError(format!("CSV parse error: {}", e)))?;

            let timestamp = chrono::NaiveDate::parse_from_str(&row.date, "%Y-%m-%d")
                .map_err(|e| ZiplineError::DataError(format!("Date parse error: {}", e)))?
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| ZiplineError::DataError("Invalid time".to_string()))?;
            let timestamp = DateTime::from_naive_utc_and_offset(timestamp, Utc);

            // Use adjusted close for more accurate backtesting
            bars.push(Bar::new(
                timestamp,
                row.open,
                row.high,
                row.low,
                row.adj_close,
                row.volume,
            ));
        }

        Ok(bars)
    }
}

impl Default for YahooFinanceSource {
    fn default() -> Self {
        Self::new().expect("Failed to create Yahoo Finance source")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_yahoo_source_creation() {
        let source = YahooFinanceSource::new();
        assert!(source.is_ok());
    }

    #[test]
    fn test_csv_parsing() {
        let source = YahooFinanceSource::new().unwrap();
        let csv_data = "Date,Open,High,Low,Close,Adj Close,Volume\n\
                        2023-01-03,100.0,105.0,99.0,103.0,103.0,1000000\n\
                        2023-01-04,103.0,106.0,102.0,105.0,105.0,1100000";

        let bars = source.parse_csv_data(csv_data).unwrap();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].close, 103.0);
        assert_eq!(bars[1].close, 105.0);
    }
}
