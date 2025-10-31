//! Technical analysis factors for pipeline system

use crate::error::{Result, ZiplineError};
use statrs::statistics::{Data, OrderStatistics, Statistics};
use std::collections::VecDeque;

/// Simple Moving Average (SMA)
#[derive(Debug, Clone)]
pub struct SimpleMovingAverage {
    window: usize,
    values: VecDeque<f64>,
}

impl SimpleMovingAverage {
    /// Create new SMA with given window size
    pub fn new(window: usize) -> Self {
        if window == 0 {
            panic!("Window size must be greater than 0");
        }
        Self {
            window,
            values: VecDeque::with_capacity(window),
        }
    }

    /// Add a value and compute current SMA
    pub fn update(&mut self, value: f64) -> Option<f64> {
        self.values.push_back(value);

        if self.values.len() > self.window {
            self.values.pop_front();
        }

        if self.values.len() == self.window {
            Some(self.values.iter().sum::<f64>() / self.window as f64)
        } else {
            None
        }
    }

    /// Compute SMA for a slice of values
    pub fn compute(window: usize, values: &[f64]) -> Vec<Option<f64>> {
        let mut sma = Self::new(window);
        values.iter().map(|&v| sma.update(v)).collect()
    }

    /// Get current value (if window is full)
    pub fn current(&self) -> Option<f64> {
        if self.values.len() == self.window {
            Some(self.values.iter().sum::<f64>() / self.window as f64)
        } else {
            None
        }
    }
}

/// Exponential Moving Average (EMA)
#[derive(Debug, Clone)]
pub struct ExponentialMovingAverage {
    span: usize,
    alpha: f64,
    current_ema: Option<f64>,
}

impl ExponentialMovingAverage {
    /// Create new EMA with given span
    pub fn new(span: usize) -> Self {
        if span == 0 {
            panic!("Span must be greater than 0");
        }
        let alpha = 2.0 / (span as f64 + 1.0);
        Self {
            span,
            alpha,
            current_ema: None,
        }
    }

    /// Update with new value
    pub fn update(&mut self, value: f64) -> f64 {
        match self.current_ema {
            None => {
                self.current_ema = Some(value);
                value
            }
            Some(prev_ema) => {
                let ema = self.alpha * value + (1.0 - self.alpha) * prev_ema;
                self.current_ema = Some(ema);
                ema
            }
        }
    }

    /// Compute EMA for a slice of values
    pub fn compute(span: usize, values: &[f64]) -> Vec<f64> {
        let mut ema = Self::new(span);
        values.iter().map(|&v| ema.update(v)).collect()
    }

    /// Get current EMA value
    pub fn current(&self) -> Option<f64> {
        self.current_ema
    }
}

/// Relative Strength Index (RSI)
#[derive(Debug, Clone)]
pub struct RSI {
    period: usize,
    gains: VecDeque<f64>,
    losses: VecDeque<f64>,
    prev_value: Option<f64>,
}

impl RSI {
    /// Create new RSI with given period
    pub fn new(period: usize) -> Self {
        if period == 0 {
            panic!("Period must be greater than 0");
        }
        Self {
            period,
            gains: VecDeque::with_capacity(period),
            losses: VecDeque::with_capacity(period),
            prev_value: None,
        }
    }

    /// Update with new value and compute RSI
    pub fn update(&mut self, value: f64) -> Option<f64> {
        if let Some(prev) = self.prev_value {
            let change = value - prev;
            let gain = if change > 0.0 { change } else { 0.0 };
            let loss = if change < 0.0 { -change } else { 0.0 };

            self.gains.push_back(gain);
            self.losses.push_back(loss);

            if self.gains.len() > self.period {
                self.gains.pop_front();
                self.losses.pop_front();
            }

            if self.gains.len() == self.period {
                let avg_gain = self.gains.iter().sum::<f64>() / self.period as f64;
                let avg_loss = self.losses.iter().sum::<f64>() / self.period as f64;

                if avg_loss == 0.0 {
                    return Some(100.0);
                }

                let rs = avg_gain / avg_loss;
                let rsi = 100.0 - (100.0 / (1.0 + rs));
                self.prev_value = Some(value);
                return Some(rsi);
            }
        }

        self.prev_value = Some(value);
        None
    }

    /// Compute RSI for a slice of values
    pub fn compute(period: usize, values: &[f64]) -> Vec<Option<f64>> {
        let mut rsi = Self::new(period);
        values.iter().map(|&v| rsi.update(v)).collect()
    }
}

/// Moving Average Convergence Divergence (MACD)
#[derive(Debug, Clone)]
pub struct MACD {
    fast_ema: ExponentialMovingAverage,
    slow_ema: ExponentialMovingAverage,
    signal_ema: ExponentialMovingAverage,
}

impl MACD {
    /// Create new MACD with standard parameters (12, 26, 9)
    pub fn new() -> Self {
        Self::with_params(12, 26, 9)
    }

    /// Create MACD with custom parameters
    pub fn with_params(fast: usize, slow: usize, signal: usize) -> Self {
        Self {
            fast_ema: ExponentialMovingAverage::new(fast),
            slow_ema: ExponentialMovingAverage::new(slow),
            signal_ema: ExponentialMovingAverage::new(signal),
        }
    }

    /// Update with new value, returns (MACD line, signal line, histogram)
    pub fn update(&mut self, value: f64) -> (f64, f64, f64) {
        let fast = self.fast_ema.update(value);
        let slow = self.slow_ema.update(value);
        let macd_line = fast - slow;
        let signal_line = self.signal_ema.update(macd_line);
        let histogram = macd_line - signal_line;

        (macd_line, signal_line, histogram)
    }
}

impl Default for MACD {
    fn default() -> Self {
        Self::new()
    }
}

/// Bollinger Bands
#[derive(Debug, Clone)]
pub struct BollingerBands {
    window: usize,
    num_std_dev: f64,
    values: VecDeque<f64>,
}

impl BollingerBands {
    /// Create new Bollinger Bands with window and number of standard deviations
    pub fn new(window: usize, num_std_dev: f64) -> Self {
        if window == 0 {
            panic!("Window must be greater than 0");
        }
        Self {
            window,
            num_std_dev,
            values: VecDeque::with_capacity(window),
        }
    }

    /// Update with new value, returns (middle, upper, lower) bands
    pub fn update(&mut self, value: f64) -> Option<(f64, f64, f64)> {
        self.values.push_back(value);

        if self.values.len() > self.window {
            self.values.pop_front();
        }

        if self.values.len() == self.window {
            let values_vec: Vec<f64> = self.values.iter().copied().collect();
            let data = Data::new(values_vec);

            let middle = data.mean().unwrap_or(0.0);
            let std_dev = data.std_dev().unwrap_or(0.0);

            let upper = middle + (self.num_std_dev * std_dev);
            let lower = middle - (self.num_std_dev * std_dev);

            Some((middle, upper, lower))
        } else {
            None
        }
    }
}

/// Average True Range (ATR)
#[derive(Debug, Clone)]
pub struct AverageTrueRange {
    period: usize,
    tr_values: VecDeque<f64>,
    prev_close: Option<f64>,
}

impl AverageTrueRange {
    /// Create new ATR with given period
    pub fn new(period: usize) -> Self {
        if period == 0 {
            panic!("Period must be greater than 0");
        }
        Self {
            period,
            tr_values: VecDeque::with_capacity(period),
            prev_close: None,
        }
    }

    /// Update with new OHLC values
    pub fn update(&mut self, high: f64, low: f64, close: f64) -> Option<f64> {
        let tr = if let Some(prev_close) = self.prev_close {
            let h_l = high - low;
            let h_pc = (high - prev_close).abs();
            let l_pc = (low - prev_close).abs();
            h_l.max(h_pc).max(l_pc)
        } else {
            high - low
        };

        self.tr_values.push_back(tr);
        if self.tr_values.len() > self.period {
            self.tr_values.pop_front();
        }

        self.prev_close = Some(close);

        if self.tr_values.len() == self.period {
            Some(self.tr_values.iter().sum::<f64>() / self.period as f64)
        } else {
            None
        }
    }
}

/// Volume-Weighted Average Price (VWAP)
#[derive(Debug, Clone)]
pub struct VWAP {
    cumulative_price_volume: f64,
    cumulative_volume: f64,
}

impl VWAP {
    /// Create new VWAP
    pub fn new() -> Self {
        Self {
            cumulative_price_volume: 0.0,
            cumulative_volume: 0.0,
        }
    }

    /// Update with new price and volume
    pub fn update(&mut self, price: f64, volume: f64) -> f64 {
        self.cumulative_price_volume += price * volume;
        self.cumulative_volume += volume;

        if self.cumulative_volume == 0.0 {
            price
        } else {
            self.cumulative_price_volume / self.cumulative_volume
        }
    }

    /// Reset VWAP (for new trading session)
    pub fn reset(&mut self) {
        self.cumulative_price_volume = 0.0;
        self.cumulative_volume = 0.0;
    }
}

impl Default for VWAP {
    fn default() -> Self {
        Self::new()
    }
}

/// Momentum (Rate of Change)
#[derive(Debug, Clone)]
pub struct Momentum {
    period: usize,
    values: VecDeque<f64>,
}

impl Momentum {
    /// Create new Momentum indicator
    pub fn new(period: usize) -> Self {
        if period == 0 {
            panic!("Period must be greater than 0");
        }
        Self {
            period,
            values: VecDeque::with_capacity(period + 1),
        }
    }

    /// Update with new value
    pub fn update(&mut self, value: f64) -> Option<f64> {
        self.values.push_back(value);

        if self.values.len() > self.period + 1 {
            self.values.pop_front();
        }

        if self.values.len() == self.period + 1 {
            let oldest = self.values.front().unwrap();
            let current = self.values.back().unwrap();
            Some(((current - oldest) / oldest) * 100.0)
        } else {
            None
        }
    }
}

/// Historical Volatility (rolling standard deviation)
#[derive(Debug, Clone)]
pub struct HistoricalVolatility {
    window: usize,
    returns: VecDeque<f64>,
    prev_price: Option<f64>,
    annualization_factor: f64,
}

impl HistoricalVolatility {
    /// Create new Historical Volatility indicator
    /// periods_per_year: 252 for daily, 52 for weekly, etc.
    pub fn new(window: usize, periods_per_year: f64) -> Self {
        if window == 0 {
            panic!("Window must be greater than 0");
        }
        Self {
            window,
            returns: VecDeque::with_capacity(window),
            prev_price: None,
            annualization_factor: periods_per_year.sqrt(),
        }
    }

    /// Update with new price
    pub fn update(&mut self, price: f64) -> Option<f64> {
        if let Some(prev) = self.prev_price {
            let ret = (price / prev).ln();
            self.returns.push_back(ret);

            if self.returns.len() > self.window {
                self.returns.pop_front();
            }

            if self.returns.len() == self.window {
                let returns_vec: Vec<f64> = self.returns.iter().copied().collect();
                let data = Data::new(returns_vec);
                let std_dev = data.std_dev().unwrap_or(0.0);
                let annualized_vol = std_dev * self.annualization_factor;

                self.prev_price = Some(price);
                return Some(annualized_vol);
            }
        }

        self.prev_price = Some(price);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn test_sma() {
        let mut sma = SimpleMovingAverage::new(3);

        assert_eq!(sma.update(1.0), None);
        assert_eq!(sma.update(2.0), None);
        assert_eq!(sma.update(3.0), Some(2.0)); // (1+2+3)/3 = 2
        assert_eq!(sma.update(4.0), Some(3.0)); // (2+3+4)/3 = 3
    }

    #[test]
    fn test_sma_compute() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = SimpleMovingAverage::compute(3, &values);

        assert_eq!(result[0], None);
        assert_eq!(result[1], None);
        assert_eq!(result[2], Some(2.0)); // (1+2+3)/3
        assert_eq!(result[3], Some(3.0)); // (2+3+4)/3
        assert_eq!(result[4], Some(4.0)); // (3+4+5)/3
    }

    #[test]
    fn test_ema() {
        let mut ema = ExponentialMovingAverage::new(3);

        let v1 = ema.update(1.0);
        assert_eq!(v1, 1.0);

        let v2 = ema.update(2.0);
        assert!(v2 > 1.0 && v2 < 2.0);

        let v3 = ema.update(3.0);
        assert!(v3 > v2 && v3 < 3.0);
    }

    #[test]
    fn test_rsi() {
        let values = vec![44.0, 44.34, 44.09, 43.61, 44.33, 44.83, 45.10, 45.42, 45.84, 46.08, 45.89, 46.03, 45.61, 46.28, 46.28];
        let results = RSI::compute(14, &values);

        // First 14 values should be None
        assert!(results[13].is_none());
        // 15th value should have RSI
        assert!(results[14].is_some());
    }

    #[test]
    fn test_macd() {
        let mut macd = MACD::new();

        for _ in 0..50 {
            let price = 100.0 + (rand::random::<f64>() * 10.0);
            let (macd_line, signal, histogram) = macd.update(price);
            assert_eq!(histogram, macd_line - signal);
        }
    }

    #[test]
    fn test_bollinger_bands() {
        let mut bb = BollingerBands::new(20, 2.0);

        for i in 0..19 {
            assert_eq!(bb.update(100.0 + i as f64), None);
        }

        let result = bb.update(119.0);
        assert!(result.is_some());

        let (middle, upper, lower) = result.unwrap();
        assert!(upper > middle);
        assert!(lower < middle);
        assert_relative_eq!(middle, 109.5, epsilon = 0.1);
    }

    #[test]
    fn test_atr() {
        let mut atr = AverageTrueRange::new(14);

        for i in 0..13 {
            let high = 110.0 + i as f64;
            let low = 100.0 + i as f64;
            let close = 105.0 + i as f64;
            assert_eq!(atr.update(high, low, close), None);
        }

        let result = atr.update(123.0, 113.0, 118.0);
        assert!(result.is_some());
        assert!(result.unwrap() > 0.0);
    }

    #[test]
    fn test_vwap() {
        let mut vwap = VWAP::new();

        let v1 = vwap.update(100.0, 1000.0);
        assert_eq!(v1, 100.0);

        let v2 = vwap.update(101.0, 1000.0);
        assert_eq!(v2, 100.5); // (100*1000 + 101*1000) / 2000

        let v3 = vwap.update(102.0, 2000.0);
        assert_eq!(v3, 101.0); // (100*1000 + 101*1000 + 102*2000) / 4000
    }

    #[test]
    fn test_momentum() {
        let mut mom = Momentum::new(10);

        for i in 0..10 {
            assert_eq!(mom.update(100.0 + i as f64), None);
        }

        let result = mom.update(110.0);
        assert!(result.is_some());
        assert_relative_eq!(result.unwrap(), 10.0, epsilon = 0.01); // 10% increase
    }

    #[test]
    fn test_historical_volatility() {
        let mut vol = HistoricalVolatility::new(20, 252.0);

        let prices = (0..20).map(|i| 100.0 + (i as f64 * 0.5)).collect::<Vec<f64>>();

        for (i, &price) in prices.iter().enumerate() {
            let result = vol.update(price);
            if i < 19 {
                assert_eq!(result, None);
            } else {
                assert!(result.is_some());
                assert!(result.unwrap() > 0.0);
            }
        }
    }
}
