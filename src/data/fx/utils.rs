//! FX utilities - currency pair operations and conversions

use super::base::{Currency, FXRateReader};
use crate::error::{Result, ZiplineError};
use chrono::{DateTime, Utc};

/// Currency pair representation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CurrencyPair {
    pub base: Currency,
    pub quote: Currency,
}

impl CurrencyPair {
    /// Create new currency pair
    pub fn new(base: Currency, quote: Currency) -> Self {
        Self { base, quote }
    }

    /// Normalize to canonical form (e.g., always USD as quote for major pairs)
    /// This helps with storage efficiency and lookup consistency
    pub fn normalize(&self) -> (Self, bool) {
        // Major currency hierarchy for normalization:
        // EUR > GBP > AUD > NZD > USD > CAD > CHF > JPY
        let priority = |c: Currency| -> i32 {
            match c {
                Currency::EUR => 8,
                Currency::GBP => 7,
                Currency::AUD => 6,
                Currency::NZD => 5,
                Currency::USD => 4,
                Currency::CAD => 3,
                Currency::CHF => 2,
                Currency::JPY => 1,
                _ => 0,
            }
        };

        let base_priority = priority(self.base);
        let quote_priority = priority(self.quote);

        if base_priority < quote_priority {
            // Swap to put higher priority currency as base
            (Self::new(self.quote, self.base), true)
        } else {
            (*self, false)
        }
    }

    /// Get inverse pair
    pub fn inverse(&self) -> Self {
        Self {
            base: self.quote,
            quote: self.base,
        }
    }

    /// Invert rate (for inverse pair)
    pub fn invert_rate(rate: f64) -> Result<f64> {
        if rate <= 0.0 {
            return Err(ZiplineError::InvalidData(format!(
                "Cannot invert non-positive rate: {}",
                rate
            )));
        }
        Ok(1.0 / rate)
    }

    /// Calculate cross rate via intermediate currency
    ///
    /// Example: EUR/GBP via USD
    /// EUR/GBP = (EUR/USD) / (GBP/USD)
    pub fn cross_rate(
        base_to_mid: f64,
        quote_to_mid: f64,
    ) -> Result<f64> {
        if quote_to_mid <= 0.0 {
            return Err(ZiplineError::InvalidData(
                "Cannot calculate cross rate with zero quote rate".to_string(),
            ));
        }
        Ok(base_to_mid / quote_to_mid)
    }

    /// Format as string (e.g., "EUR/USD")
    pub fn to_string(&self) -> String {
        format!("{}/{}", self.base.as_str(), self.quote.as_str())
    }

    /// Parse from string (e.g., "EUR/USD" or "EURUSD")
    pub fn from_string(s: &str) -> Result<Self> {
        if s.contains('/') {
            let parts: Vec<&str> = s.split('/').collect();
            if parts.len() != 2 {
                return Err(ZiplineError::InvalidData(format!(
                    "Invalid currency pair format: {}",
                    s
                )));
            }
            Ok(Self {
                base: Currency::from_str(parts[0])?,
                quote: Currency::from_str(parts[1])?,
            })
        } else if s.len() == 6 {
            // Format: EURUSD (3 chars each)
            Ok(Self {
                base: Currency::from_str(&s[0..3])?,
                quote: Currency::from_str(&s[3..6])?,
            })
        } else {
            Err(ZiplineError::InvalidData(format!(
                "Invalid currency pair format: {}",
                s
            )))
        }
    }
}

/// Convert amount from one currency to another
///
/// # Example
/// ```
/// use rusty_zipline::data::fx::{convert_amount, InMemoryFXRateReader, Currency, FXRateReader};
/// use chrono::Utc;
///
/// let mut reader = InMemoryFXRateReader::new();
/// let dt = Utc::now();
/// reader.add_rate(Currency::EUR, Currency::USD, dt, 1.20);
///
/// let usd_amount = convert_amount(&reader, 100.0, Currency::EUR, Currency::USD, dt).unwrap();
/// assert_eq!(usd_amount, 120.0);
/// ```
pub fn convert_amount<R: FXRateReader + ?Sized>(
    reader: &R,
    amount: f64,
    from: Currency,
    to: Currency,
    dt: DateTime<Utc>,
) -> Result<f64> {
    let rate = reader.get_rate(from, to, dt)?;
    Ok(amount * rate)
}

/// Batch convert multiple amounts at once
pub fn convert_amounts<R: FXRateReader + ?Sized>(
    reader: &R,
    amounts: &[(f64, Currency)],
    to: Currency,
    dt: DateTime<Utc>,
) -> Result<Vec<f64>> {
    amounts
        .iter()
        .map(|(amount, from)| convert_amount(reader, *amount, *from, to, dt))
        .collect()
}

/// Calculate portfolio value in target currency
///
/// Converts all positions to target currency and sums them
pub fn portfolio_value<R: FXRateReader + ?Sized>(
    reader: &R,
    positions: &[(f64, Currency)],
    target_currency: Currency,
    dt: DateTime<Utc>,
) -> Result<f64> {
    let converted = convert_amounts(reader, positions, target_currency, dt)?;
    Ok(converted.iter().sum())
}

/// Find triangular arbitrage opportunities
///
/// Returns (profit_ratio, path) if arbitrage exists
/// Example: USD -> EUR -> GBP -> USD with profit
pub fn find_triangular_arbitrage<R: FXRateReader + ?Sized>(
    reader: &R,
    base: Currency,
    intermediate1: Currency,
    intermediate2: Currency,
    dt: DateTime<Utc>,
) -> Result<Option<(f64, Vec<Currency>)>> {
    // Get rates for the triangle
    let rate1 = reader.get_rate(base, intermediate1, dt)?;
    let rate2 = reader.get_rate(intermediate1, intermediate2, dt)?;
    let rate3 = reader.get_rate(intermediate2, base, dt)?;

    // Calculate profit ratio
    let final_amount = rate1 * rate2 * rate3;

    if final_amount > 1.0 {
        // Arbitrage opportunity exists
        let path = vec![base, intermediate1, intermediate2, base];
        Ok(Some((final_amount, path)))
    } else {
        Ok(None)
    }
}

/// Calculate effective exchange rate over a time range (average)
pub fn average_rate<R: FXRateReader + ?Sized>(
    reader: &R,
    from: Currency,
    to: Currency,
    timestamps: &[DateTime<Utc>],
) -> Result<f64> {
    if timestamps.is_empty() {
        return Err(ZiplineError::InvalidData(
            "Cannot calculate average rate with empty timestamp list".to_string(),
        ));
    }

    let mut sum = 0.0;
    for dt in timestamps {
        sum += reader.get_rate(from, to, *dt)?;
    }

    Ok(sum / timestamps.len() as f64)
}

/// Calculate rate volatility over time period (standard deviation)
pub fn rate_volatility<R: FXRateReader + ?Sized>(
    reader: &R,
    from: Currency,
    to: Currency,
    timestamps: &[DateTime<Utc>],
) -> Result<f64> {
    if timestamps.len() < 2 {
        return Err(ZiplineError::InvalidData(
            "Need at least 2 timestamps to calculate volatility".to_string(),
        ));
    }

    // Get all rates
    let rates: Result<Vec<f64>> = timestamps
        .iter()
        .map(|dt| reader.get_rate(from, to, *dt))
        .collect();
    let rates = rates?;

    // Calculate mean
    let mean: f64 = rates.iter().sum::<f64>() / rates.len() as f64;

    // Calculate variance
    let variance: f64 = rates.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / rates.len() as f64;

    // Return standard deviation
    Ok(variance.sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::fx::InMemoryFXRateReader;
    use chrono::TimeZone;

    #[test]
    fn test_currency_pair_creation() {
        let pair = CurrencyPair::new(Currency::EUR, Currency::USD);
        assert_eq!(pair.base, Currency::EUR);
        assert_eq!(pair.quote, Currency::USD);
    }

    #[test]
    fn test_currency_pair_inverse() {
        let pair = CurrencyPair::new(Currency::EUR, Currency::USD);
        let inverse = pair.inverse();
        assert_eq!(inverse.base, Currency::USD);
        assert_eq!(inverse.quote, Currency::EUR);
    }

    #[test]
    fn test_currency_pair_normalize() {
        let pair = CurrencyPair::new(Currency::USD, Currency::EUR);
        let (normalized, inverted) = pair.normalize();

        // EUR has higher priority, so should be base
        assert_eq!(normalized.base, Currency::EUR);
        assert_eq!(normalized.quote, Currency::USD);
        assert!(inverted);
    }

    #[test]
    fn test_currency_pair_to_string() {
        let pair = CurrencyPair::new(Currency::EUR, Currency::USD);
        assert_eq!(pair.to_string(), "EUR/USD");
    }

    #[test]
    fn test_currency_pair_from_string() {
        let pair = CurrencyPair::from_string("EUR/USD").unwrap();
        assert_eq!(pair.base, Currency::EUR);
        assert_eq!(pair.quote, Currency::USD);

        let pair2 = CurrencyPair::from_string("GBPJPY").unwrap();
        assert_eq!(pair2.base, Currency::GBP);
        assert_eq!(pair2.quote, Currency::JPY);
    }

    #[test]
    fn test_invert_rate() {
        let rate = 1.20;
        let inverted = CurrencyPair::invert_rate(rate).unwrap();
        assert!((inverted - 0.8333).abs() < 0.001);
    }

    #[test]
    fn test_cross_rate() {
        // EUR/USD = 1.20, GBP/USD = 1.30
        // EUR/GBP = 1.20 / 1.30 = 0.923
        let rate = CurrencyPair::cross_rate(1.20, 1.30).unwrap();
        assert!((rate - 0.923).abs() < 0.001);
    }

    #[test]
    fn test_convert_amount() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        reader.add_rate(Currency::EUR, Currency::USD, dt, 1.20).unwrap();

        let usd_amount = convert_amount(&reader, 100.0, Currency::EUR, Currency::USD, dt).unwrap();
        assert_eq!(usd_amount, 120.0);
    }

    #[test]
    fn test_convert_amounts() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        reader.add_rate(Currency::EUR, Currency::USD, dt, 1.20).unwrap();
        reader.add_rate(Currency::GBP, Currency::USD, dt, 1.30).unwrap();

        let amounts = vec![
            (100.0, Currency::EUR),
            (100.0, Currency::GBP),
        ];

        let converted = convert_amounts(&reader, &amounts, Currency::USD, dt).unwrap();
        assert_eq!(converted[0], 120.0);
        assert_eq!(converted[1], 130.0);
    }

    #[test]
    fn test_portfolio_value() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        reader.add_rate(Currency::EUR, Currency::USD, dt, 1.20).unwrap();
        reader.add_rate(Currency::GBP, Currency::USD, dt, 1.30).unwrap();

        let positions = vec![
            (100.0, Currency::EUR),
            (100.0, Currency::GBP),
            (100.0, Currency::USD),
        ];

        let total = portfolio_value(&reader, &positions, Currency::USD, dt).unwrap();
        assert_eq!(total, 350.0); // 120 + 130 + 100
    }

    #[test]
    fn test_average_rate() {
        let mut reader = InMemoryFXRateReader::new();
        let dt1 = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let dt2 = Utc.with_ymd_and_hms(2024, 1, 2, 12, 0, 0).unwrap();
        let dt3 = Utc.with_ymd_and_hms(2024, 1, 3, 12, 0, 0).unwrap();

        reader.add_rate(Currency::EUR, Currency::USD, dt1, 1.20).unwrap();
        reader.add_rate(Currency::EUR, Currency::USD, dt2, 1.22).unwrap();
        reader.add_rate(Currency::EUR, Currency::USD, dt3, 1.24).unwrap();

        let timestamps = vec![dt1, dt2, dt3];
        let avg = average_rate(&reader, Currency::EUR, Currency::USD, &timestamps).unwrap();

        assert!((avg - 1.22).abs() < 0.001);
    }

    #[test]
    fn test_rate_volatility() {
        let mut reader = InMemoryFXRateReader::new();
        let dt1 = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let dt2 = Utc.with_ymd_and_hms(2024, 1, 2, 12, 0, 0).unwrap();
        let dt3 = Utc.with_ymd_and_hms(2024, 1, 3, 12, 0, 0).unwrap();

        reader.add_rate(Currency::EUR, Currency::USD, dt1, 1.20).unwrap();
        reader.add_rate(Currency::EUR, Currency::USD, dt2, 1.22).unwrap();
        reader.add_rate(Currency::EUR, Currency::USD, dt3, 1.24).unwrap();

        let timestamps = vec![dt1, dt2, dt3];
        let vol = rate_volatility(&reader, Currency::EUR, Currency::USD, &timestamps).unwrap();

        assert!(vol > 0.0);
        assert!(vol < 0.1); // Should be relatively small
    }

    #[test]
    fn test_triangular_arbitrage_no_opportunity() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        // Fair rates (no arbitrage)
        reader.add_rate(Currency::USD, Currency::EUR, dt, 0.85).unwrap();
        reader.add_rate(Currency::EUR, Currency::GBP, dt, 0.88).unwrap();
        reader.add_rate(Currency::GBP, Currency::USD, dt, 1.33).unwrap();

        // 0.85 * 0.88 * 1.33 ≈ 0.995 < 1.0 (no arbitrage)
        let result = find_triangular_arbitrage(
            &reader,
            Currency::USD,
            Currency::EUR,
            Currency::GBP,
            dt,
        ).unwrap();

        assert!(result.is_none());
    }
}
