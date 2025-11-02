//! Volume-based indicators for pipeline analysis
//!
//! This module provides volume-related technical indicators:
//! - AverageDollarVolume: Liquidity screening (CRITICAL for production)
//! - VWAP: Volume-weighted average price
//! - OnBalanceVolume: Cumulative volume indicator
//! - ChaikinMoneyFlow: Money flow indicator
//! - MoneyFlowIndex: Volume-weighted RSI
//! - AccumulationDistribution: Volume flow indicator
//! - VolumeWeightedMA: Volume-weighted moving average

use crate::error::Result;
use crate::pipeline::engine::{Factor, FactorOutput, PipelineContext};
use chrono::{DateTime, Utc};
use hashbrown::HashMap;
use std::collections::VecDeque;

/// OBV - On-Balance Volume
#[derive(Debug, Clone)]
pub struct OnBalanceVolume {
    obv: f64,
    prev_close: Option<f64>,
}

impl OnBalanceVolume {
    /// Create new OBV indicator
    pub fn new() -> Self {
        Self {
            obv: 0.0,
            prev_close: None,
        }
    }

    /// Update with new close price and volume
    pub fn update(&mut self, close: f64, volume: f64) -> f64 {
        if let Some(prev) = self.prev_close {
            if close > prev {
                self.obv += volume;
            } else if close < prev {
                self.obv -= volume;
            }
            // If close == prev, OBV stays the same
        } else {
            self.obv = volume; // First value
        }

        self.prev_close = Some(close);
        self.obv
    }

    /// Reset OBV
    pub fn reset(&mut self) {
        self.obv = 0.0;
        self.prev_close = None;
    }
}

impl Default for OnBalanceVolume {
    fn default() -> Self {
        Self::new()
    }
}

/// Chaikin Money Flow
#[derive(Debug, Clone)]
pub struct ChaikinMoneyFlow {
    period: usize,
    mf_volumes: VecDeque<f64>,
    volumes: VecDeque<f64>,
}

impl ChaikinMoneyFlow {
    /// Create new Chaikin Money Flow with given period (typically 20 or 21)
    pub fn new(period: usize) -> Self {
        if period == 0 {
            panic!("Period must be greater than 0");
        }
        Self {
            period,
            mf_volumes: VecDeque::with_capacity(period),
            volumes: VecDeque::with_capacity(period),
        }
    }

    /// Update with new HLCV values
    pub fn update(&mut self, high: f64, low: f64, close: f64, volume: f64) -> Option<f64> {
        // Money Flow Multiplier
        let mf_multiplier = if high != low {
            ((close - low) - (high - close)) / (high - low)
        } else {
            0.0
        };

        let mf_volume = mf_multiplier * volume;

        self.mf_volumes.push_back(mf_volume);
        self.volumes.push_back(volume);

        if self.mf_volumes.len() > self.period {
            self.mf_volumes.pop_front();
            self.volumes.pop_front();
        }

        if self.mf_volumes.len() == self.period {
            let sum_mf_volume = self.mf_volumes.iter().sum::<f64>();
            let sum_volume = self.volumes.iter().sum::<f64>();

            if sum_volume != 0.0 {
                Some(sum_mf_volume / sum_volume)
            } else {
                Some(0.0)
            }
        } else {
            None
        }
    }
}

/// Money Flow Index (MFI)
#[derive(Debug, Clone)]
pub struct MoneyFlowIndex {
    period: usize,
    positive_flow: VecDeque<f64>,
    negative_flow: VecDeque<f64>,
    prev_typical_price: Option<f64>,
}

impl MoneyFlowIndex {
    /// Create new MFI with given period (typically 14)
    pub fn new(period: usize) -> Self {
        if period == 0 {
            panic!("Period must be greater than 0");
        }
        Self {
            period,
            positive_flow: VecDeque::with_capacity(period),
            negative_flow: VecDeque::with_capacity(period),
            prev_typical_price: None,
        }
    }

    /// Update with new HLCV values
    pub fn update(&mut self, high: f64, low: f64, close: f64, volume: f64) -> Option<f64> {
        let typical_price = (high + low + close) / 3.0;
        let money_flow = typical_price * volume;

        if let Some(prev_tp) = self.prev_typical_price {
            if typical_price > prev_tp {
                self.positive_flow.push_back(money_flow);
                self.negative_flow.push_back(0.0);
            } else if typical_price < prev_tp {
                self.positive_flow.push_back(0.0);
                self.negative_flow.push_back(money_flow);
            } else {
                self.positive_flow.push_back(0.0);
                self.negative_flow.push_back(0.0);
            }

            if self.positive_flow.len() > self.period {
                self.positive_flow.pop_front();
                self.negative_flow.pop_front();
            }

            if self.positive_flow.len() == self.period {
                let positive_sum = self.positive_flow.iter().sum::<f64>();
                let negative_sum = self.negative_flow.iter().sum::<f64>();

                if negative_sum == 0.0 {
                    self.prev_typical_price = Some(typical_price);
                    return Some(100.0);
                }

                let money_ratio = positive_sum / negative_sum;
                let mfi = 100.0 - (100.0 / (1.0 + money_ratio));

                self.prev_typical_price = Some(typical_price);
                return Some(mfi);
            }
        }

        self.prev_typical_price = Some(typical_price);
        None
    }
}

/// Accumulation/Distribution Line
#[derive(Debug, Clone)]
pub struct AccumulationDistribution {
    ad_line: f64,
}

impl AccumulationDistribution {
    /// Create new A/D Line indicator
    pub fn new() -> Self {
        Self { ad_line: 0.0 }
    }

    /// Update with new HLCV values
    pub fn update(&mut self, high: f64, low: f64, close: f64, volume: f64) -> f64 {
        let clv = if high != low {
            ((close - low) - (high - close)) / (high - low)
        } else {
            0.0
        };

        self.ad_line += clv * volume;
        self.ad_line
    }

    /// Reset A/D Line
    pub fn reset(&mut self) {
        self.ad_line = 0.0;
    }
}

impl Default for AccumulationDistribution {
    fn default() -> Self {
        Self::new()
    }
}

/// Volume-Weighted Moving Average
#[derive(Debug, Clone)]
pub struct VolumeWeightedMA {
    window: usize,
    price_volumes: VecDeque<f64>,
    volumes: VecDeque<f64>,
}

impl VolumeWeightedMA {
    /// Create new VWMA with given window
    pub fn new(window: usize) -> Self {
        if window == 0 {
            panic!("Window must be greater than 0");
        }
        Self {
            window,
            price_volumes: VecDeque::with_capacity(window),
            volumes: VecDeque::with_capacity(window),
        }
    }

    /// Update with new price and volume
    pub fn update(&mut self, price: f64, volume: f64) -> Option<f64> {
        self.price_volumes.push_back(price * volume);
        self.volumes.push_back(volume);

        if self.price_volumes.len() > self.window {
            self.price_volumes.pop_front();
            self.volumes.pop_front();
        }

        if self.price_volumes.len() == self.window {
            let sum_pv = self.price_volumes.iter().sum::<f64>();
            let sum_v = self.volumes.iter().sum::<f64>();

            if sum_v != 0.0 {
                Some(sum_pv / sum_v)
            } else {
                None
            }
        } else {
            None
        }
    }
}

/// Average Dollar Volume - CRITICAL for liquidity screening
///
/// Calculates average dollar volume over a rolling window.
/// Dollar volume = price * volume
///
/// This is the #1 most common liquidity filter in production strategies.
/// Typical usage: screen for stocks with ADV > $1M to ensure tradability.
///
/// # Example
/// ```rust,no_run
/// use rusty_zipline::pipeline::AverageDollarVolume;
///
/// // Filter for stocks with > $1M average daily dollar volume
/// let adv = AverageDollarVolume::new(20);
/// // Use with pipeline filtering
/// ```
#[derive(Debug, Clone)]
pub struct AverageDollarVolume {
    /// Number of trading days to average over (typically 20-30)
    window: usize,
    /// Rolling buffer of dollar volumes
    dollar_volumes: VecDeque<f64>,
}

impl AverageDollarVolume {
    /// Create new AverageDollarVolume factor
    ///
    /// # Arguments
    /// * `window` - Number of days to average (typically 20-30)
    ///
    /// # Panics
    /// Panics if window is 0
    pub fn new(window: usize) -> Self {
        assert!(window > 0, "Window must be positive");
        Self {
            window,
            dollar_volumes: VecDeque::with_capacity(window),
        }
    }

    /// Update with new price and volume data
    ///
    /// # Returns
    /// Some(average_dollar_volume) if window is full, None otherwise
    pub fn update(&mut self, close: f64, volume: f64) -> Option<f64> {
        let dollar_volume = close * volume;
        self.dollar_volumes.push_back(dollar_volume);

        if self.dollar_volumes.len() > self.window {
            self.dollar_volumes.pop_front();
        }

        if self.dollar_volumes.len() == self.window {
            let sum: f64 = self.dollar_volumes.iter().sum();
            Some(sum / self.window as f64)
        } else {
            None
        }
    }

    /// Get the window length
    pub fn window_length(&self) -> usize {
        self.window
    }
}

impl Factor for AverageDollarVolume {
    fn compute(&self, _timestamp: DateTime<Utc>, context: &PipelineContext) -> Result<FactorOutput> {
        let mut output = HashMap::new();

        for asset in context.assets() {
            // Get historical close prices
            let closes = context
                .data_provider()
                .get_prices(asset.id, self.window)?;

            // Get historical volumes
            let volumes = context
                .data_provider()
                .get_volumes(asset.id, self.window)?;

            // Calculate average dollar volume
            if closes.len() >= self.window && volumes.len() >= self.window {
                let dollar_volumes: Vec<f64> = closes
                    .iter()
                    .zip(volumes.iter())
                    .map(|(close, volume)| close * volume)
                    .collect();

                let avg_dollar_volume: f64 = dollar_volumes.iter().sum::<f64>() / self.window as f64;
                output.insert(asset.id, avg_dollar_volume);
            } else if !closes.is_empty() && !volumes.is_empty() {
                // Use available data if we don't have full window
                let available_len = closes.len().min(volumes.len());
                let dollar_volumes: Vec<f64> = closes
                    .iter()
                    .take(available_len)
                    .zip(volumes.iter().take(available_len))
                    .map(|(close, volume)| close * volume)
                    .collect();

                let avg_dollar_volume: f64 =
                    dollar_volumes.iter().sum::<f64>() / available_len as f64;
                output.insert(asset.id, avg_dollar_volume);
            } else {
                output.insert(asset.id, f64::NAN);
            }
        }

        Ok(output)
    }

    fn name(&self) -> &str {
        "AverageDollarVolume"
    }

    fn clone_box(&self) -> Box<dyn Factor> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn test_obv() {
        let mut obv = OnBalanceVolume::new();

        let v1 = obv.update(100.0, 1000.0);
        assert_eq!(v1, 1000.0); // First value

        let v2 = obv.update(105.0, 1500.0); // Price up
        assert_eq!(v2, 2500.0); // Add volume

        let v3 = obv.update(103.0, 1200.0); // Price down
        assert_eq!(v3, 1300.0); // Subtract volume

        let v4 = obv.update(103.0, 800.0); // Price unchanged
        assert_eq!(v4, 1300.0); // No change
    }

    #[test]
    fn test_chaikin_money_flow() {
        let mut cmf = ChaikinMoneyFlow::new(3);

        assert_eq!(cmf.update(110.0, 90.0, 105.0, 1000.0), None);
        assert_eq!(cmf.update(115.0, 95.0, 110.0, 1500.0), None);

        let result = cmf.update(112.0, 92.0, 106.0, 1200.0);
        assert!(result.is_some());
        assert!(result.unwrap().abs() <= 1.0); // Should be between -1 and 1
    }

    #[test]
    fn test_money_flow_index() {
        let mut mfi = MoneyFlowIndex::new(14);

        for i in 0..20 {
            let high = 110.0 + i as f64;
            let low = 90.0 + i as f64;
            let close = 100.0 + i as f64;
            let volume = 1000.0;

            let result = mfi.update(high, low, close, volume);
            if let Some(value) = result {
                assert!(value >= 0.0 && value <= 100.0);
            }
        }
    }

    #[test]
    fn test_accumulation_distribution() {
        let mut ad = AccumulationDistribution::new();

        let v1 = ad.update(110.0, 90.0, 105.0, 1000.0);
        assert!(v1 != 0.0);

        let v2 = ad.update(115.0, 95.0, 110.0, 1500.0);
        assert!(v2 > v1); // Positive money flow

        ad.reset();
        assert_eq!(ad.ad_line, 0.0);
    }

    #[test]
    fn test_vwma() {
        let mut vwma = VolumeWeightedMA::new(3);

        assert_eq!(vwma.update(100.0, 1000.0), None);
        assert_eq!(vwma.update(101.0, 1000.0), None);

        let result = vwma.update(102.0, 2000.0);
        assert!(result.is_some());
        // Should weight the 102.0 price more heavily due to higher volume
        assert_relative_eq!(result.unwrap(), 101.5, epsilon = 0.01);
    }

    #[test]
    fn test_average_dollar_volume() {
        let mut adv = AverageDollarVolume::new(5);

        // Test basic computation
        let closes = vec![100.0, 101.0, 102.0, 103.0, 104.0];
        let volumes = vec![1000.0, 1100.0, 1200.0, 1300.0, 1400.0];

        let mut results = vec![];
        for i in 0..5 {
            if let Some(result) = adv.update(closes[i], volumes[i]) {
                results.push(result);
            }
        }

        assert_eq!(results.len(), 1);
        // Average dollar volume should be around 116,000
        assert!(results[0] > 100_000.0 && results[0] < 130_000.0);
    }

    #[test]
    fn test_average_dollar_volume_partial_window() {
        let adv = AverageDollarVolume::new(10);

        // Only provide 3 days - should handle gracefully
        let mut adv_state = adv.clone();
        assert_eq!(adv_state.update(100.0, 1000.0), None);
        assert_eq!(adv_state.update(101.0, 1000.0), None);
        assert_eq!(adv_state.update(102.0, 1000.0), None);
    }

    #[test]
    fn test_average_dollar_volume_pipeline_integration() {
        // This test will be expanded when we integrate with the pipeline
        let adv = AverageDollarVolume::new(20);
        assert_eq!(adv.window_length(), 20);
        assert_eq!(adv.name(), "AverageDollarVolume");
    }
}
