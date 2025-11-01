//! Currency types and FX conversion

use serde::{Deserialize, Serialize};
use std::fmt;

/// Currency enumeration (ISO 4217 codes)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Currency {
    /// US Dollar
    USD,
    /// Euro
    EUR,
    /// British Pound Sterling
    GBP,
    /// Japanese Yen
    JPY,
    /// Swiss Franc
    CHF,
    /// Australian Dollar
    AUD,
    /// Canadian Dollar
    CAD,
    /// Chinese Yuan
    CNY,
    /// Hong Kong Dollar
    HKD,
    /// Singapore Dollar
    SGD,
}

impl Currency {
    /// Get ISO 4217 code
    pub fn code(&self) -> &'static str {
        match self {
            Currency::USD => "USD",
            Currency::EUR => "EUR",
            Currency::GBP => "GBP",
            Currency::JPY => "JPY",
            Currency::CHF => "CHF",
            Currency::AUD => "AUD",
            Currency::CAD => "CAD",
            Currency::CNY => "CNY",
            Currency::HKD => "HKD",
            Currency::SGD => "SGD",
        }
    }

    /// Get currency symbol
    pub fn symbol(&self) -> &'static str {
        match self {
            Currency::USD => "$",
            Currency::EUR => "€",
            Currency::GBP => "£",
            Currency::JPY => "¥",
            Currency::CHF => "CHF",
            Currency::AUD => "A$",
            Currency::CAD => "C$",
            Currency::CNY => "¥",
            Currency::HKD => "HK$",
            Currency::SGD => "S$",
        }
    }

    /// Parse from ISO code
    pub fn from_code(code: &str) -> Option<Self> {
        match code.to_uppercase().as_str() {
            "USD" => Some(Currency::USD),
            "EUR" => Some(Currency::EUR),
            "GBP" => Some(Currency::GBP),
            "JPY" => Some(Currency::JPY),
            "CHF" => Some(Currency::CHF),
            "AUD" => Some(Currency::AUD),
            "CAD" => Some(Currency::CAD),
            "CNY" => Some(Currency::CNY),
            "HKD" => Some(Currency::HKD),
            "SGD" => Some(Currency::SGD),
            _ => None,
        }
    }

    /// Get all supported currencies
    pub fn all() -> Vec<Currency> {
        vec![
            Currency::USD,
            Currency::EUR,
            Currency::GBP,
            Currency::JPY,
            Currency::CHF,
            Currency::AUD,
            Currency::CAD,
            Currency::CNY,
            Currency::HKD,
            Currency::SGD,
        ]
    }
}

impl fmt::Display for Currency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.code())
    }
}

/// Currency pair for exchange rates
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

    /// Get the inverse pair
    pub fn inverse(&self) -> Self {
        Self {
            base: self.quote,
            quote: self.base,
        }
    }

    /// Convert rate to inverse rate
    pub fn invert_rate(&self, rate: f64) -> f64 {
        if rate > 0.0 {
            1.0 / rate
        } else {
            0.0
        }
    }
}

impl fmt::Display for CurrencyPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.base, self.quote)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_currency_code() {
        assert_eq!(Currency::USD.code(), "USD");
        assert_eq!(Currency::EUR.code(), "EUR");
        assert_eq!(Currency::GBP.code(), "GBP");
    }

    #[test]
    fn test_currency_symbol() {
        assert_eq!(Currency::USD.symbol(), "$");
        assert_eq!(Currency::EUR.symbol(), "€");
        assert_eq!(Currency::GBP.symbol(), "£");
    }

    #[test]
    fn test_currency_from_code() {
        assert_eq!(Currency::from_code("USD"), Some(Currency::USD));
        assert_eq!(Currency::from_code("usd"), Some(Currency::USD));
        assert_eq!(Currency::from_code("INVALID"), None);
    }

    #[test]
    fn test_currency_display() {
        assert_eq!(format!("{}", Currency::USD), "USD");
        assert_eq!(format!("{}", Currency::JPY), "JPY");
    }

    #[test]
    fn test_currency_pair() {
        let pair = CurrencyPair::new(Currency::EUR, Currency::USD);
        assert_eq!(pair.base, Currency::EUR);
        assert_eq!(pair.quote, Currency::USD);
        assert_eq!(format!("{}", pair), "EUR/USD");
    }

    #[test]
    fn test_currency_pair_inverse() {
        let pair = CurrencyPair::new(Currency::EUR, Currency::USD);
        let inverse = pair.inverse();

        assert_eq!(inverse.base, Currency::USD);
        assert_eq!(inverse.quote, Currency::EUR);
    }

    #[test]
    fn test_invert_rate() {
        let pair = CurrencyPair::new(Currency::EUR, Currency::USD);
        let rate = 1.2; // 1 EUR = 1.2 USD
        let inverse_rate = pair.invert_rate(rate);

        assert!((inverse_rate - 0.8333).abs() < 0.001); // 1 USD = 0.8333 EUR
    }

    #[test]
    fn test_all_currencies() {
        let currencies = Currency::all();
        assert!(currencies.len() >= 10);
        assert!(currencies.contains(&Currency::USD));
        assert!(currencies.contains(&Currency::EUR));
    }
}
