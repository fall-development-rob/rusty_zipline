//! Alpha Vantage data source integration
//!
//! Provides access to intraday and daily market data, plus fundamentals and technical indicators.

use crate::error::{Result, ZiplineError};
use crate::types::Bar;
use chrono::{DateTime, NaiveDateTime, Utc};
use std::collections::HashMap;
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;

const ALPHA_VANTAGE_BASE_URL: &str = "https://www.alphavantage.co/query";
const RATE_LIMIT_DELAY_MS: u64 = 12000; // 5 requests per minute = 12 seconds between requests

/// Alpha Vantage data source
pub struct AlphaVantageSource {
    api_key: String,
    client: Client,
    last_request_time: std::sync::Arc<std::sync::Mutex<Option<std::time::Instant>>>,
}

#[derive(Debug, Deserialize)]
struct AlphaVantageResponse {
    #[serde(rename = "Time Series (Daily)", default)]
    time_series_daily: Option<HashMap<String, TimeSeriesData>>,
    #[serde(flatten)]
    time_series_intraday: HashMap<String, HashMap<String, TimeSeriesData>>,
}

#[derive(Debug, Deserialize)]
struct TimeSeriesData {
    #[serde(rename = "1. open")]
    open: String,
    #[serde(rename = "2. high")]
    high: String,
    #[serde(rename = "3. low")]
    low: String,
    #[serde(rename = "4. close")]
    close: String,
    #[serde(rename = "5. volume")]
    volume: String,
}

impl AlphaVantageSource {
    /// Create a new Alpha Vantage data source
    pub fn new(api_key: String) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| ZiplineError::DataError(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            api_key,
            client,
            last_request_time: std::sync::Arc::new(std::sync::Mutex::new(None)),
        })
    }

    /// Fetch intraday data with rate limiting
    pub async fn fetch_intraday(&self, symbol: &str, interval: &str) -> Result<Vec<Bar>> {
        self.enforce_rate_limit().await;

        let url = format!(
            "{}?function=TIME_SERIES_INTRADAY&symbol={}&interval={}&apikey={}&outputsize=full",
            ALPHA_VANTAGE_BASE_URL, symbol, interval, self.api_key
        );

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| ZiplineError::DataError(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(ZiplineError::DataError(format!(
                "Alpha Vantage returned error: {}",
                response.status()
            )));
        }

        let data: AlphaVantageResponse = response
            .json()
            .await
            .map_err(|e| ZiplineError::DataError(format!("JSON parse error: {}", e)))?;

        self.parse_response(data, false)
    }

    /// Fetch daily data with rate limiting
    pub async fn fetch_daily(&self, symbol: &str) -> Result<Vec<Bar>> {
        self.enforce_rate_limit().await;

        let url = format!(
            "{}?function=TIME_SERIES_DAILY&symbol={}&apikey={}&outputsize=full",
            ALPHA_VANTAGE_BASE_URL, symbol, self.api_key
        );

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| ZiplineError::DataError(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(ZiplineError::DataError(format!(
                "Alpha Vantage returned error: {}",
                response.status()
            )));
        }

        let data: AlphaVantageResponse = response
            .json()
            .await
            .map_err(|e| ZiplineError::DataError(format!("JSON parse error: {}", e)))?;

        self.parse_response(data, true)
    }

    async fn enforce_rate_limit(&self) {
        let mut last_request = self.last_request_time.lock().unwrap();
        if let Some(last_time) = *last_request {
            let elapsed = last_time.elapsed();
            let required_delay = Duration::from_millis(RATE_LIMIT_DELAY_MS);
            if elapsed < required_delay {
                let sleep_duration = required_delay - elapsed;
                drop(last_request); // Release lock before sleeping
                tokio::time::sleep(sleep_duration).await;
                last_request = self.last_request_time.lock().unwrap();
            }
        }
        *last_request = Some(std::time::Instant::now());
    }

    fn parse_response(&self, data: AlphaVantageResponse, is_daily: bool) -> Result<Vec<Bar>> {
        let time_series = if is_daily {
            data.time_series_daily.ok_or_else(|| {
                ZiplineError::DataError("No daily time series data in response".to_string())
            })?
        } else {
            data.time_series_intraday
                .values()
                .next()
                .ok_or_else(|| {
                    ZiplineError::DataError("No intraday time series data in response".to_string())
                })?
                .clone()
        };

        let mut bars = Vec::new();
        for (timestamp_str, ts_data) in time_series {
            let timestamp = if is_daily {
                let date = chrono::NaiveDate::parse_from_str(&timestamp_str, "%Y-%m-%d")
                    .map_err(|e| ZiplineError::DataError(format!("Date parse error: {}", e)))?;
                date.and_hms_opt(0, 0, 0)
                    .ok_or_else(|| ZiplineError::DataError("Invalid time".to_string()))?
            } else {
                NaiveDateTime::parse_from_str(&timestamp_str, "%Y-%m-%d %H:%M:%S")
                    .map_err(|e| ZiplineError::DataError(format!("Datetime parse error: {}", e)))?
            };

            let timestamp = DateTime::from_naive_utc_and_offset(timestamp, Utc);
            let open = ts_data.open.parse::<f64>().unwrap_or(0.0);
            let high = ts_data.high.parse::<f64>().unwrap_or(0.0);
            let low = ts_data.low.parse::<f64>().unwrap_or(0.0);
            let close = ts_data.close.parse::<f64>().unwrap_or(0.0);
            let volume = ts_data.volume.parse::<f64>().unwrap_or(0.0);

            bars.push(Bar::new(timestamp, open, high, low, close, volume));
        }

        // Sort by timestamp (oldest first)
        bars.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        Ok(bars)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_alpha_vantage_source_creation() {
        let source = AlphaVantageSource::new("test_key".to_string());
        assert!(source.is_ok());
    }
}
