//! Data adjustments for corporate actions (splits, dividends, mergers)

use crate::error::{Result, ZiplineError};
use crate::types::{Bar, Price, Quantity};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Type of dividend
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum DividendKind {
    /// Cash dividend
    Cash,
    /// Stock dividend
    Stock,
}

/// Type of corporate action adjustment
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AdjustmentKind {
    /// Stock split (e.g., 2-for-1 split has ratio 2.0)
    Split { ratio: f64 },
    /// Dividend payment
    Dividend { amount: f64, kind: DividendKind },
    /// Merger into another asset
    Merger {
        ratio: f64,
        target_asset_id: u64,
    },
    /// Spin-off creating new asset
    SpinOff {
        ratio: f64,
        new_asset_id: u64,
    },
}

/// Corporate action adjustment
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Adjustment {
    /// Asset affected
    pub asset_id: u64,
    /// Effective date of adjustment
    pub effective_date: DateTime<Utc>,
    /// Type and parameters of adjustment
    pub kind: AdjustmentKind,
}

impl Adjustment {
    /// Create a new adjustment
    pub fn new(asset_id: u64, effective_date: DateTime<Utc>, kind: AdjustmentKind) -> Self {
        Self {
            asset_id,
            effective_date,
            kind,
        }
    }

    /// Apply this adjustment to a price
    pub fn adjust_price(&self, price: Price, as_of_date: DateTime<Utc>) -> Price {
        if as_of_date < self.effective_date {
            // Don't adjust prices from before the effective date
            return price;
        }

        match &self.kind {
            AdjustmentKind::Split { ratio } => price / (*ratio),
            AdjustmentKind::Dividend { amount, kind } => match kind {
                DividendKind::Cash => price - (*amount),
                DividendKind::Stock => price, // Stock dividends are handled via splits
            },
            AdjustmentKind::Merger { ratio, .. } => price * (*ratio),
            AdjustmentKind::SpinOff { .. } => price, // Spin-offs typically don't adjust price
        }
    }

    /// Apply this adjustment to volume
    pub fn adjust_volume(&self, volume: Quantity, as_of_date: DateTime<Utc>) -> Quantity {
        if as_of_date < self.effective_date {
            return volume;
        }

        match &self.kind {
            AdjustmentKind::Split { ratio } => volume * (*ratio),
            AdjustmentKind::Dividend { kind, .. } => match kind {
                DividendKind::Stock => volume, // Adjust via split if needed
                DividendKind::Cash => volume,
            },
            AdjustmentKind::Merger { ratio, .. } => volume / (*ratio),
            AdjustmentKind::SpinOff { .. } => volume,
        }
    }
}

/// Reader for adjustment data
pub struct AdjustmentReader {
    /// Adjustments by asset ID
    adjustments: HashMap<u64, Vec<Adjustment>>,
}

impl AdjustmentReader {
    /// Create a new empty adjustment reader
    pub fn new() -> Self {
        Self {
            adjustments: HashMap::new(),
        }
    }

    /// Add an adjustment
    pub fn add_adjustment(&mut self, adjustment: Adjustment) {
        self.adjustments
            .entry(adjustment.asset_id)
            .or_insert_with(Vec::new)
            .push(adjustment);
    }

    /// Get all adjustments for an asset in a date range
    pub fn get_adjustments(
        &self,
        asset_id: u64,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Vec<&Adjustment> {
        self.adjustments
            .get(&asset_id)
            .map(|adjs| {
                adjs.iter()
                    .filter(|adj| adj.effective_date >= start && adj.effective_date <= end)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Apply all adjustments to a bar
    pub fn apply_adjustments_to_bar(
        &self,
        bar: &mut Bar,
        asset_id: u64,
        as_of_date: DateTime<Utc>,
    ) {
        if let Some(adjustments) = self.adjustments.get(&asset_id) {
            for adj in adjustments {
                if adj.effective_date > bar.timestamp && adj.effective_date <= as_of_date {
                    bar.open = adj.adjust_price(bar.open, as_of_date);
                    bar.high = adj.adjust_price(bar.high, as_of_date);
                    bar.low = adj.adjust_price(bar.low, as_of_date);
                    bar.close = adj.adjust_price(bar.close, as_of_date);
                    bar.volume = adj.adjust_volume(bar.volume, as_of_date);
                }
            }
        }
    }

    /// Apply adjustments to multiple bars
    pub fn apply_adjustments(
        &self,
        bars: &mut [Bar],
        asset_id: u64,
        as_of_date: DateTime<Utc>,
    ) {
        for bar in bars.iter_mut() {
            self.apply_adjustments_to_bar(bar, asset_id, as_of_date);
        }
    }

    /// Load adjustments from CSV file
    /// CSV format: asset_id,date,type,value1,value2
    pub fn load_from_csv(&mut self, path: &Path) -> Result<()> {
        let mut reader = csv::Reader::from_path(path)
            .map_err(|e| ZiplineError::DataError(format!("Failed to read CSV: {}", e)))?;

        for result in reader.records() {
            let record = result
                .map_err(|e| ZiplineError::DataError(format!("Failed to parse CSV row: {}", e)))?;

            if record.len() < 3 {
                continue;
            }

            let asset_id: u64 = record[0]
                .parse()
                .map_err(|e| ZiplineError::DataError(format!("Invalid asset_id: {}", e)))?;

            let date_str = &record[1];
            let effective_date = DateTime::parse_from_rfc3339(date_str)
                .map(|dt| dt.with_timezone(&Utc))
                .map_err(|e| ZiplineError::DataError(format!("Invalid date: {}", e)))?;

            let adj_type = &record[2];

            let kind = match adj_type {
                "split" => {
                    let ratio: f64 = record
                        .get(3)
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| ZiplineError::DataError("Missing split ratio".into()))?;
                    AdjustmentKind::Split { ratio }
                }
                "dividend_cash" => {
                    let amount: f64 = record
                        .get(3)
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| ZiplineError::DataError("Missing dividend amount".into()))?;
                    AdjustmentKind::Dividend {
                        amount,
                        kind: DividendKind::Cash,
                    }
                }
                "dividend_stock" => {
                    let amount: f64 = record
                        .get(3)
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| {
                            ZiplineError::DataError("Missing stock dividend amount".into())
                        })?;
                    AdjustmentKind::Dividend {
                        amount,
                        kind: DividendKind::Stock,
                    }
                }
                "merger" => {
                    let ratio: f64 = record
                        .get(3)
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| ZiplineError::DataError("Missing merger ratio".into()))?;
                    let target_asset_id: u64 = record
                        .get(4)
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| ZiplineError::DataError("Missing target asset ID".into()))?;
                    AdjustmentKind::Merger { ratio, target_asset_id }
                }
                "spinoff" => {
                    let ratio: f64 = record
                        .get(3)
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| ZiplineError::DataError("Missing spinoff ratio".into()))?;
                    let new_asset_id: u64 = record
                        .get(4)
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| ZiplineError::DataError("Missing new asset ID".into()))?;
                    AdjustmentKind::SpinOff { ratio, new_asset_id }
                }
                _ => continue,
            };

            self.add_adjustment(Adjustment::new(asset_id, effective_date, kind));
        }

        Ok(())
    }

    /// Get count of adjustments for an asset
    pub fn adjustment_count(&self, asset_id: u64) -> usize {
        self.adjustments
            .get(&asset_id)
            .map(|adjs| adjs.len())
            .unwrap_or(0)
    }

    /// Get all unique asset IDs that have adjustments
    pub fn assets_with_adjustments(&self) -> Vec<u64> {
        self.adjustments.keys().copied().collect()
    }

    /// Remove adjustments for an asset
    pub fn remove_asset(&mut self, asset_id: u64) -> Option<Vec<Adjustment>> {
        self.adjustments.remove(&asset_id)
    }

    /// Get all adjustments (for all assets)
    pub fn all_adjustments(&self) -> Vec<&Adjustment> {
        self.adjustments
            .values()
            .flat_map(|adjs| adjs.iter())
            .collect()
    }

    /// Total adjustment count across all assets
    pub fn total_adjustment_count(&self) -> usize {
        self.adjustments
            .values()
            .map(|adjs| adjs.len())
            .sum()
    }
}

impl Default for AdjustmentReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Writer for adjustment data
pub struct AdjustmentWriter {
    /// Adjustments to write
    adjustments: Vec<Adjustment>,
}

impl AdjustmentWriter {
    /// Create new adjustment writer
    pub fn new() -> Self {
        Self {
            adjustments: Vec::new(),
        }
    }

    /// Add adjustment to write
    pub fn add(&mut self, adjustment: Adjustment) {
        self.adjustments.push(adjustment);
    }

    /// Add multiple adjustments
    pub fn add_many(&mut self, adjustments: Vec<Adjustment>) {
        self.adjustments.extend(adjustments);
    }

    /// Write adjustments to CSV file
    /// CSV format: asset_id,date,type,value1,value2
    pub fn write_to_csv(&self, path: &Path) -> Result<()> {
        let mut writer = csv::Writer::from_path(path)
            .map_err(|e| ZiplineError::DataError(format!("Failed to create CSV: {}", e)))?;

        // Write header
        writer
            .write_record(&["asset_id", "date", "type", "value1", "value2"])
            .map_err(|e| ZiplineError::DataError(format!("Failed to write header: {}", e)))?;

        // Write adjustments
        for adj in &self.adjustments {
            let date_str = adj.effective_date.to_rfc3339();

            match &adj.kind {
                AdjustmentKind::Split { ratio } => {
                    writer
                        .write_record(&[
                            adj.asset_id.to_string(),
                            date_str,
                            "split".to_string(),
                            ratio.to_string(),
                            "".to_string(),
                        ])
                        .map_err(|e| ZiplineError::DataError(format!("Failed to write split: {}", e)))?;
                }
                AdjustmentKind::Dividend { amount, kind } => {
                    let type_str = match kind {
                        DividendKind::Cash => "dividend_cash",
                        DividendKind::Stock => "dividend_stock",
                    };
                    writer
                        .write_record(&[
                            adj.asset_id.to_string(),
                            date_str,
                            type_str.to_string(),
                            amount.to_string(),
                            "".to_string(),
                        ])
                        .map_err(|e| ZiplineError::DataError(format!("Failed to write dividend: {}", e)))?;
                }
                AdjustmentKind::Merger { ratio, target_asset_id } => {
                    writer
                        .write_record(&[
                            adj.asset_id.to_string(),
                            date_str,
                            "merger".to_string(),
                            ratio.to_string(),
                            target_asset_id.to_string(),
                        ])
                        .map_err(|e| ZiplineError::DataError(format!("Failed to write merger: {}", e)))?;
                }
                AdjustmentKind::SpinOff { ratio, new_asset_id } => {
                    writer
                        .write_record(&[
                            adj.asset_id.to_string(),
                            date_str,
                            "spinoff".to_string(),
                            ratio.to_string(),
                            new_asset_id.to_string(),
                        ])
                        .map_err(|e| ZiplineError::DataError(format!("Failed to write spinoff: {}", e)))?;
                }
            }
        }

        writer
            .flush()
            .map_err(|e| ZiplineError::DataError(format!("Failed to flush CSV: {}", e)))?;

        Ok(())
    }

    /// Get count of pending adjustments
    pub fn count(&self) -> usize {
        self.adjustments.len()
    }

    /// Clear all pending adjustments
    pub fn clear(&mut self) {
        self.adjustments.clear();
    }
}

impl Default for AdjustmentWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Combined adjustment manager (reader + writer)
pub struct AdjustmentManager {
    reader: AdjustmentReader,
    writer: AdjustmentWriter,
}

impl AdjustmentManager {
    /// Create new adjustment manager
    pub fn new() -> Self {
        Self {
            reader: AdjustmentReader::new(),
            writer: AdjustmentWriter::new(),
        }
    }

    /// Get reader
    pub fn reader(&self) -> &AdjustmentReader {
        &self.reader
    }

    /// Get mutable reader
    pub fn reader_mut(&mut self) -> &mut AdjustmentReader {
        &mut self.reader
    }

    /// Get writer
    pub fn writer(&self) -> &AdjustmentWriter {
        &self.writer
    }

    /// Get mutable writer
    pub fn writer_mut(&mut self) -> &mut AdjustmentWriter {
        &mut self.writer
    }

    /// Load adjustments from CSV into reader
    pub fn load(&mut self, path: &Path) -> Result<()> {
        self.reader.load_from_csv(path)
    }

    /// Save pending adjustments from writer to CSV
    pub fn save(&self, path: &Path) -> Result<()> {
        self.writer.write_to_csv(path)
    }

    /// Add adjustment (goes to writer)
    pub fn add_adjustment(&mut self, adjustment: Adjustment) {
        self.writer.add(adjustment);
    }

    /// Commit pending adjustments from writer to reader
    pub fn commit(&mut self) {
        for adj in self.writer.adjustments.iter().cloned() {
            self.reader.add_adjustment(adj);
        }
        self.writer.clear();
    }
}

impl Default for AdjustmentManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_split_adjustment() {
        let adj = Adjustment::new(
            1,
            Utc::now(),
            AdjustmentKind::Split { ratio: 2.0 },
        );

        let price = 100.0;
        let adjusted = adj.adjust_price(price, Utc::now() + chrono::Duration::days(1));
        assert_eq!(adjusted, 50.0); // 2-for-1 split halves the price

        let volume = 1000.0;
        let adjusted_vol = adj.adjust_volume(volume, Utc::now() + chrono::Duration::days(1));
        assert_eq!(adjusted_vol, 2000.0); // Volume doubles
    }

    #[test]
    fn test_dividend_adjustment() {
        let adj = Adjustment::new(
            1,
            Utc::now(),
            AdjustmentKind::Dividend {
                amount: 1.50,
                kind: DividendKind::Cash,
            },
        );

        let price = 100.0;
        let adjusted = adj.adjust_price(price, Utc::now() + chrono::Duration::days(1));
        assert_eq!(adjusted, 98.50);
    }

    #[test]
    fn test_adjustment_reader() {
        let mut reader = AdjustmentReader::new();

        let adj1 = Adjustment::new(
            1,
            Utc::now(),
            AdjustmentKind::Split { ratio: 2.0 },
        );

        let adj2 = Adjustment::new(
            1,
            Utc::now() + chrono::Duration::days(30),
            AdjustmentKind::Dividend {
                amount: 1.0,
                kind: DividendKind::Cash,
            },
        );

        reader.add_adjustment(adj1);
        reader.add_adjustment(adj2);

        assert_eq!(reader.adjustment_count(1), 2);
        assert_eq!(reader.adjustment_count(2), 0);
    }

    #[test]
    fn test_apply_adjustments_to_bar() {
        let mut reader = AdjustmentReader::new();
        let now = Utc::now();

        reader.add_adjustment(Adjustment::new(
            1,
            now + chrono::Duration::days(5),
            AdjustmentKind::Split { ratio: 2.0 },
        ));

        let mut bar = Bar::new(now, 100.0, 105.0, 95.0, 102.0, 1000.0);

        // Adjust as of 10 days from now (after the split)
        reader.apply_adjustments_to_bar(&mut bar, 1, now + chrono::Duration::days(10));

        assert_eq!(bar.close, 51.0); // Split-adjusted
        assert_eq!(bar.volume, 2000.0); // Volume doubled
    }
}
