//! Advanced technical indicators for pipeline analysis
//!
//! This module provides additional technical analysis factors beyond the basics

use std::collections::VecDeque;

/// ADX - Average Directional Index
#[derive(Debug, Clone)]
pub struct ADX {
    period: usize,
    dx_values: VecDeque<f64>,
    prev_high: Option<f64>,
    prev_low: Option<f64>,
    prev_close: Option<f64>,
}

impl ADX {
    /// Create new ADX with given period (typically 14)
    pub fn new(period: usize) -> Self {
        if period == 0 {
            panic!("Period must be greater than 0");
        }
        Self {
            period,
            dx_values: VecDeque::with_capacity(period),
            prev_high: None,
            prev_low: None,
            prev_close: None,
        }
    }

    /// Update with new OHLC values
    pub fn update(&mut self, high: f64, low: f64, close: f64) -> Option<f64> {
        if let (Some(ph), Some(pl), Some(pc)) = (self.prev_high, self.prev_low, self.prev_close) {
            // Calculate directional movement
            let up_move = high - ph;
            let down_move = pl - low;

            let plus_dm = if up_move > down_move && up_move > 0.0 {
                up_move
            } else {
                0.0
            };

            let minus_dm = if down_move > up_move && down_move > 0.0 {
                down_move
            } else {
                0.0
            };

            // Calculate true range
            let tr = (high - low)
                .max((high - pc).abs())
                .max((low - pc).abs());

            // Calculate directional indicators
            let plus_di = if tr != 0.0 { (plus_dm / tr) * 100.0 } else { 0.0 };
            let minus_di = if tr != 0.0 { (minus_dm / tr) * 100.0 } else { 0.0 };

            // Calculate DX
            let di_sum = plus_di + minus_di;
            let dx = if di_sum != 0.0 {
                ((plus_di - minus_di).abs() / di_sum) * 100.0
            } else {
                0.0
            };

            self.dx_values.push_back(dx);

            if self.dx_values.len() > self.period {
                self.dx_values.pop_front();
            }

            // Calculate ADX as average of DX
            if self.dx_values.len() == self.period {
                let adx = self.dx_values.iter().sum::<f64>() / self.period as f64;
                self.prev_high = Some(high);
                self.prev_low = Some(low);
                self.prev_close = Some(close);
                return Some(adx);
            }
        }

        self.prev_high = Some(high);
        self.prev_low = Some(low);
        self.prev_close = Some(close);
        None
    }
}

/// CCI - Commodity Channel Index
#[derive(Debug, Clone)]
pub struct CCI {
    period: usize,
    tp_values: VecDeque<f64>, // Typical Price values
}

impl CCI {
    /// Create new CCI with given period (typically 20)
    pub fn new(period: usize) -> Self {
        if period == 0 {
            panic!("Period must be greater than 0");
        }
        Self {
            period,
            tp_values: VecDeque::with_capacity(period),
        }
    }

    /// Update with new HLC values
    pub fn update(&mut self, high: f64, low: f64, close: f64) -> Option<f64> {
        let typical_price = (high + low + close) / 3.0;
        self.tp_values.push_back(typical_price);

        if self.tp_values.len() > self.period {
            self.tp_values.pop_front();
        }

        if self.tp_values.len() == self.period {
            let sma = self.tp_values.iter().sum::<f64>() / self.period as f64;
            let mean_deviation = self
                .tp_values
                .iter()
                .map(|&tp| (tp - sma).abs())
                .sum::<f64>()
                / self.period as f64;

            if mean_deviation != 0.0 {
                Some((typical_price - sma) / (0.015 * mean_deviation))
            } else {
                Some(0.0)
            }
        } else {
            None
        }
    }
}

/// Stochastic Oscillator
#[derive(Debug, Clone)]
pub struct StochasticOscillator {
    period: usize,
    highs: VecDeque<f64>,
    lows: VecDeque<f64>,
    k_period: usize,
    d_period: usize,
    k_values: VecDeque<f64>,
}

impl StochasticOscillator {
    /// Create new Stochastic Oscillator
    /// period: lookback for high/low (typically 14)
    /// k_period: smoothing for %K (typically 3)
    /// d_period: smoothing for %D (typically 3)
    pub fn new(period: usize, k_period: usize, d_period: usize) -> Self {
        if period == 0 || k_period == 0 || d_period == 0 {
            panic!("Periods must be greater than 0");
        }
        Self {
            period,
            highs: VecDeque::with_capacity(period),
            lows: VecDeque::with_capacity(period),
            k_period,
            d_period,
            k_values: VecDeque::with_capacity(d_period),
        }
    }

    /// Update with new HLC values, returns (%K, %D)
    pub fn update(&mut self, high: f64, low: f64, close: f64) -> Option<(f64, f64)> {
        self.highs.push_back(high);
        self.lows.push_back(low);

        if self.highs.len() > self.period {
            self.highs.pop_front();
            self.lows.pop_front();
        }

        if self.highs.len() == self.period {
            let highest_high = self.highs.iter().fold(f64::MIN, |a, &b| a.max(b));
            let lowest_low = self.lows.iter().fold(f64::MAX, |a, &b| a.min(b));

            let k = if highest_high != lowest_low {
                ((close - lowest_low) / (highest_high - lowest_low)) * 100.0
            } else {
                50.0
            };

            self.k_values.push_back(k);

            if self.k_values.len() > self.d_period {
                self.k_values.pop_front();
            }

            if self.k_values.len() == self.d_period {
                let d = self.k_values.iter().sum::<f64>() / self.d_period as f64;
                return Some((k, d));
            }
        }

        None
    }
}

/// Williams %R
#[derive(Debug, Clone)]
pub struct WilliamsR {
    period: usize,
    highs: VecDeque<f64>,
    lows: VecDeque<f64>,
}

impl WilliamsR {
    /// Create new Williams %R with given period (typically 14)
    pub fn new(period: usize) -> Self {
        if period == 0 {
            panic!("Period must be greater than 0");
        }
        Self {
            period,
            highs: VecDeque::with_capacity(period),
            lows: VecDeque::with_capacity(period),
        }
    }

    /// Update with new HLC values
    pub fn update(&mut self, high: f64, low: f64, close: f64) -> Option<f64> {
        self.highs.push_back(high);
        self.lows.push_back(low);

        if self.highs.len() > self.period {
            self.highs.pop_front();
            self.lows.pop_front();
        }

        if self.highs.len() == self.period {
            let highest_high = self.highs.iter().fold(f64::MIN, |a, &b| a.max(b));
            let lowest_low = self.lows.iter().fold(f64::MAX, |a, &b| a.min(b));

            if highest_high != lowest_low {
                Some(((highest_high - close) / (highest_high - lowest_low)) * -100.0)
            } else {
                Some(-50.0)
            }
        } else {
            None
        }
    }
}

/// Aroon Indicator
#[derive(Debug, Clone)]
pub struct Aroon {
    period: usize,
    highs: VecDeque<f64>,
    lows: VecDeque<f64>,
}

impl Aroon {
    /// Create new Aroon indicator with given period (typically 25)
    pub fn new(period: usize) -> Self {
        if period == 0 {
            panic!("Period must be greater than 0");
        }
        Self {
            period,
            highs: VecDeque::with_capacity(period),
            lows: VecDeque::with_capacity(period),
        }
    }

    /// Update with new HL values, returns (Aroon Up, Aroon Down)
    pub fn update(&mut self, high: f64, low: f64) -> Option<(f64, f64)> {
        self.highs.push_back(high);
        self.lows.push_back(low);

        if self.highs.len() > self.period {
            self.highs.pop_front();
            self.lows.pop_front();
        }

        if self.highs.len() == self.period {
            // Find periods since highest high and lowest low
            let days_since_high = self
                .highs
                .iter()
                .enumerate()
                .rev()
                .find(|(_, &h)| h == *self.highs.iter().fold(f64::MIN, |a, &b| a.max(b)))
                .map(|(i, _)| self.period - 1 - i)
                .unwrap_or(0);

            let days_since_low = self
                .lows
                .iter()
                .enumerate()
                .rev()
                .find(|(_, &l)| l == *self.lows.iter().fold(f64::MAX, |a, &b| a.min(b)))
                .map(|(i, _)| self.period - 1 - i)
                .unwrap_or(0);

            let aroon_up = ((self.period - days_since_high) as f64 / self.period as f64) * 100.0;
            let aroon_down = ((self.period - days_since_low) as f64 / self.period as f64) * 100.0;

            Some((aroon_up, aroon_down))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adx() {
        let mut adx = ADX::new(14);

        for i in 0..20 {
            let high = 100.0 + i as f64;
            let low = 95.0 + i as f64;
            let close = 98.0 + i as f64;

            let result = adx.update(high, low, close);
            if i >= 14 {
                assert!(result.is_some());
                assert!(result.unwrap() >= 0.0 && result.unwrap() <= 100.0);
            }
        }
    }

    #[test]
    fn test_cci() {
        let mut cci = CCI::new(20);

        for i in 0..25 {
            let high = 105.0 + i as f64 * 0.5;
            let low = 95.0 + i as f64 * 0.5;
            let close = 100.0 + i as f64 * 0.5;

            let result = cci.update(high, low, close);
            if i >= 19 {
                assert!(result.is_some());
            }
        }
    }

    #[test]
    fn test_stochastic() {
        let mut stoch = StochasticOscillator::new(14, 3, 3);

        for i in 0..20 {
            let high = 110.0 + i as f64;
            let low = 90.0 + i as f64;
            let close = 100.0 + i as f64;

            let result = stoch.update(high, low, close);
            if let Some((k, d)) = result {
                assert!(k >= 0.0 && k <= 100.0);
                assert!(d >= 0.0 && d <= 100.0);
            }
        }
    }

    #[test]
    fn test_williams_r() {
        let mut wr = WilliamsR::new(14);

        for i in 0..20 {
            let high = 110.0 + i as f64;
            let low = 90.0 + i as f64;
            let close = 95.0 + i as f64;

            let result = wr.update(high, low, close);
            if let Some(value) = result {
                assert!(value >= -100.0 && value <= 0.0);
            }
        }
    }

    #[test]
    fn test_aroon() {
        let mut aroon = Aroon::new(25);

        for i in 0..30 {
            let high = 100.0 + i as f64;
            let low = 90.0 + i as f64;

            let result = aroon.update(high, low);
            if let Some((up, down)) = result {
                assert!(up >= 0.0 && up <= 100.0);
                assert!(down >= 0.0 && down <= 100.0);
            }
        }
    }
}
