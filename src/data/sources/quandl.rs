//! Quandl data source integration
//!
//! Provides access to Quandl datasets including WIKI prices, futures, and economic indicators.

use crate::error::{Result, ZiplineError};
use crate::types::Bar;
use chrono::{DateTime, Utc};
use hashbrown::HashMap;
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;

const QUANDL_BASE_URL: &str = "https://www.quandl.com/api/v3";
const MAX_RETRIES: u32 = 3;
const RETRY_DELAY_MS: u64 = 1000;

/// Quandl data source
pub struct QuandlDataSource {
    api_key: String,
    base_url: String,
    client: Client,
}

#[derive(Debug, Deserialize)]
struct QuandlResponse {
    dataset: QuandlDataset,
}

#[derive(Debug, Deserialize)]
struct QuandlDataset {
    data: Vec<Vec<serde_json::Value>>,
    column_names: Vec<String>,
}

impl QuandlDataSource {
    /// Create a new Quandl data source
    pub fn new(api_key: String) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| ZiplineError::DataError(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            api_key,
            base_url: QUANDL_BASE_URL.to_string(),
            client,
        })
    }

    /// Fetch a single dataset with retry logic
    pub async fn fetch_dataset(
        &self,
        dataset: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Bar>> {
        let url = format!(
            "{}/datasets/{}.json?api_key={}&start_date={}&end_date={}",
            self.base_url,
            dataset,
            self.api_key,
            start.format("%Y-%m-%d"),
            end.format("%Y-%m-%d")
        );

        let mut retries = 0;
        loop {
            match self.fetch_with_retry(&url).await {
                Ok(response) => {
                    let quandl_data: QuandlResponse = response
                        .json()
                        .await
                        .map_err(|e| ZiplineError::DataError(format!("JSON parse error: {}", e)))?;

                    return self.parse_quandl_data(quandl_data);
                }
                Err(e) if retries < MAX_RETRIES => {
                    retries += 1;
                    tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS * retries as u64)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Fetch multiple datasets in bulk
    pub async fn fetch_bulk(&self, datasets: &[&str]) -> Result<HashMap<String, Vec<Bar>>> {
        let mut results = HashMap::new();

        for dataset in datasets {
            let start = Utc::now() - chrono::Duration::days(365);
            let end = Utc::now();

            match self.fetch_dataset(dataset, start, end).await {
                Ok(bars) => {
                    results.insert(dataset.to_string(), bars);
                }
                Err(e) => {
                    log::warn!("Failed to fetch dataset {}: {}", dataset, e);
                }
            }
        }

        Ok(results)
    }

    async fn fetch_with_retry(&self, url: &str) -> Result<reqwest::Response> {
        self.client
            .get(url)
            .send()
            .await
            .map_err(|e| ZiplineError::DataError(format!("HTTP request failed: {}", e)))
    }

    fn parse_quandl_data(&self, data: QuandlResponse) -> Result<Vec<Bar>> {
        let column_names = &data.dataset.column_names;
        let date_idx = column_names.iter().position(|n| n == "Date")
            .ok_or_else(|| ZiplineError::DataError("No Date column".to_string()))?;
        let open_idx = column_names.iter().position(|n| n == "Open")
            .ok_or_else(|| ZiplineError::DataError("No Open column".to_string()))?;
        let high_idx = column_names.iter().position(|n| n == "High")
            .ok_or_else(|| ZiplineError::DataError("No High column".to_string()))?;
        let low_idx = column_names.iter().position(|n| n == "Low")
            .ok_or_else(|| ZiplineError::DataError("No Low column".to_string()))?;
        let close_idx = column_names.iter().position(|n| n == "Close")
            .ok_or_else(|| ZiplineError::DataError("No Close column".to_string()))?;
        let volume_idx = column_names.iter().position(|n| n == "Volume")
            .ok_or_else(|| ZiplineError::DataError("No Volume column".to_string()))?;

        let mut bars = Vec::new();
        for row in data.dataset.data {
            let timestamp = row[date_idx].as_str()
                .ok_or_else(|| ZiplineError::DataError("Invalid date".to_string()))?;
            let timestamp = chrono::NaiveDate::parse_from_str(timestamp, "%Y-%m-%d")
                .map_err(|e| ZiplineError::DataError(format!("Date parse error: {}", e)))?
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| ZiplineError::DataError("Invalid time".to_string()))?;
            let timestamp = DateTime::from_naive_utc_and_offset(timestamp, Utc);

            let open = row[open_idx].as_f64().unwrap_or(0.0);
            let high = row[high_idx].as_f64().unwrap_or(0.0);
            let low = row[low_idx].as_f64().unwrap_or(0.0);
            let close = row[close_idx].as_f64().unwrap_or(0.0);
            let volume = row[volume_idx].as_f64().unwrap_or(0.0);

            bars.push(Bar::new(timestamp, open, high, low, close, volume));
        }

        Ok(bars)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_quandl_source_creation() {
        let source = QuandlDataSource::new("test_key".to_string());
        assert!(source.is_ok());
    }
}
