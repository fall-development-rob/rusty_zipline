//! Exploding FX rate reader - testing stub
//!
//! This reader panics on any usage. It's used to ensure that FX conversion
//! isn't accidentally triggered in single-currency backtests or tests that
//! shouldn't require FX rates.

use super::base::{Currency, FXRateReader};
use crate::error::Result;
use chrono::{DateTime, Utc};

/// Exploding FX rate reader
///
/// Panics on any attempt to get rates. Use this in tests to ensure
/// FX conversion is not accidentally triggered.
///
/// # Example
/// ```should_panic
/// use rusty_zipline::data::fx::{ExplodingFXRateReader, Currency, FXRateReader};
/// use chrono::Utc;
///
/// let reader = ExplodingFXRateReader::new();
///
/// // This will panic!
/// reader.get_rate(Currency::EUR, Currency::USD, Utc::now()).unwrap();
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct ExplodingFXRateReader {
    /// Custom panic message
    message: Option<&'static str>,
}

impl ExplodingFXRateReader {
    /// Create new exploding FX rate reader with default message
    pub fn new() -> Self {
        Self { message: None }
    }

    /// Create with custom panic message
    pub fn with_message(message: &'static str) -> Self {
        Self {
            message: Some(message),
        }
    }

    /// Get the panic message
    fn panic_message(&self) -> String {
        self.message.unwrap_or(
            "FX rate access not allowed! This backtest should be single-currency. \
             If multi-currency support is needed, use InMemoryFXRateReader or HDF5FXRateReader.",
        ).to_string()
    }
}

impl FXRateReader for ExplodingFXRateReader {
    fn get_rate(&self, from: Currency, to: Currency, dt: DateTime<Utc>) -> Result<f64> {
        // Allow same-currency conversion (always 1.0)
        if from == to {
            return Ok(1.0);
        }

        // Panic on any cross-currency conversion
        panic!(
            "{}\n\nAttempted conversion: {} -> {} at {}",
            self.panic_message(),
            from.as_str(),
            to.as_str(),
            dt
        );
    }

    fn get_rates(
        &self,
        currency_pairs: &[(Currency, Currency)],
        dt: DateTime<Utc>,
    ) -> Result<Vec<f64>> {
        // Check if any pair is cross-currency
        for (from, to) in currency_pairs {
            if from != to {
                panic!(
                    "{}\n\nAttempted batch conversion including: {} -> {} at {}",
                    self.panic_message(),
                    from.as_str(),
                    to.as_str(),
                    dt
                );
            }
        }

        // All same-currency - safe to return 1.0s
        Ok(vec![1.0; currency_pairs.len()])
    }

    fn has_rate(&self, from: Currency, to: Currency, _dt: DateTime<Utc>) -> bool {
        // Only same-currency "rates" are available
        from == to
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_same_currency_allowed() {
        let reader = ExplodingFXRateReader::new();
        let dt = Utc::now();

        // Same currency should work (returns 1.0)
        let rate = reader.get_rate(Currency::USD, Currency::USD, dt).unwrap();
        assert_eq!(rate, 1.0);

        let rate = reader.get_rate(Currency::EUR, Currency::EUR, dt).unwrap();
        assert_eq!(rate, 1.0);
    }

    #[test]
    #[should_panic(expected = "FX rate access not allowed")]
    fn test_cross_currency_panics() {
        let reader = ExplodingFXRateReader::new();
        let dt = Utc::now();

        // This should panic
        let _ = reader.get_rate(Currency::EUR, Currency::USD, dt);
    }

    #[test]
    #[should_panic(expected = "Custom error message")]
    fn test_custom_message() {
        let reader = ExplodingFXRateReader::with_message("Custom error message");
        let dt = Utc::now();

        // This should panic with custom message
        let _ = reader.get_rate(Currency::EUR, Currency::USD, dt);
    }

    #[test]
    fn test_batch_same_currency() {
        let reader = ExplodingFXRateReader::new();
        let dt = Utc::now();

        let pairs = vec![
            (Currency::USD, Currency::USD),
            (Currency::EUR, Currency::EUR),
        ];

        let rates = reader.get_rates(&pairs, dt).unwrap();
        assert_eq!(rates, vec![1.0, 1.0]);
    }

    #[test]
    #[should_panic(expected = "FX rate access not allowed")]
    fn test_batch_cross_currency_panics() {
        let reader = ExplodingFXRateReader::new();
        let dt = Utc::now();

        let pairs = vec![
            (Currency::USD, Currency::USD),
            (Currency::EUR, Currency::USD), // This triggers panic
        ];

        let _ = reader.get_rates(&pairs, dt);
    }

    #[test]
    fn test_has_rate() {
        let reader = ExplodingFXRateReader::new();
        let dt = Utc::now();

        // Same currency - has rate
        assert!(reader.has_rate(Currency::USD, Currency::USD, dt));

        // Cross currency - no rate
        assert!(!reader.has_rate(Currency::EUR, Currency::USD, dt));
    }
}
