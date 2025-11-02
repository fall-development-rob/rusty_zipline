//! Base FX system - Currency and FXRateReader trait

use crate::error::{Result, ZiplineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// ISO 4217 currency code
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Currency {
    USD, // US Dollar
    EUR, // Euro
    GBP, // British Pound
    JPY, // Japanese Yen
    CHF, // Swiss Franc
    CAD, // Canadian Dollar
    AUD, // Australian Dollar
    NZD, // New Zealand Dollar
    CNY, // Chinese Yuan
    HKD, // Hong Kong Dollar
    SGD, // Singapore Dollar
    KRW, // South Korean Won
    INR, // Indian Rupee
    BRL, // Brazilian Real
    MXN, // Mexican Peso
    ZAR, // South African Rand
    RUB, // Russian Ruble
    TRY, // Turkish Lira
}

impl Currency {
    /// Parse currency from string
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_uppercase().as_str() {
            "USD" => Ok(Currency::USD),
            "EUR" => Ok(Currency::EUR),
            "GBP" => Ok(Currency::GBP),
            "JPY" => Ok(Currency::JPY),
            "CHF" => Ok(Currency::CHF),
            "CAD" => Ok(Currency::CAD),
            "AUD" => Ok(Currency::AUD),
            "NZD" => Ok(Currency::NZD),
            "CNY" => Ok(Currency::CNY),
            "HKD" => Ok(Currency::HKD),
            "SGD" => Ok(Currency::SGD),
            "KRW" => Ok(Currency::KRW),
            "INR" => Ok(Currency::INR),
            "BRL" => Ok(Currency::BRL),
            "MXN" => Ok(Currency::MXN),
            "ZAR" => Ok(Currency::ZAR),
            "RUB" => Ok(Currency::RUB),
            "TRY" => Ok(Currency::TRY),
            _ => Err(ZiplineError::InvalidData(format!(
                "Unknown currency: {}",
                s
            ))),
        }
    }

    /// Get currency code as string
    pub fn as_str(&self) -> &'static str {
        match self {
            Currency::USD => "USD",
            Currency::EUR => "EUR",
            Currency::GBP => "GBP",
            Currency::JPY => "JPY",
            Currency::CHF => "CHF",
            Currency::CAD => "CAD",
            Currency::AUD => "AUD",
            Currency::NZD => "NZD",
            Currency::CNY => "CNY",
            Currency::HKD => "HKD",
            Currency::SGD => "SGD",
            Currency::KRW => "KRW",
            Currency::INR => "INR",
            Currency::BRL => "BRL",
            Currency::MXN => "MXN",
            Currency::ZAR => "ZAR",
            Currency::RUB => "RUB",
            Currency::TRY => "TRY",
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
            Currency::CAD => "C$",
            Currency::AUD => "A$",
            Currency::NZD => "NZ$",
            Currency::CNY => "¥",
            Currency::HKD => "HK$",
            Currency::SGD => "S$",
            Currency::KRW => "₩",
            Currency::INR => "₹",
            Currency::BRL => "R$",
            Currency::MXN => "MX$",
            Currency::ZAR => "R",
            Currency::RUB => "₽",
            Currency::TRY => "₺",
        }
    }

    /// Major currencies (high liquidity)
    pub fn is_major(&self) -> bool {
        matches!(
            self,
            Currency::USD
                | Currency::EUR
                | Currency::GBP
                | Currency::JPY
                | Currency::CHF
                | Currency::CAD
                | Currency::AUD
        )
    }
}

impl fmt::Display for Currency {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Trait for reading FX rates
pub trait FXRateReader: Send + Sync {
    /// Get exchange rate from one currency to another at a specific time
    /// Returns the rate such that: to_amount = from_amount * rate
    fn get_rate(
        &self,
        from_currency: Currency,
        to_currency: Currency,
        dt: DateTime<Utc>,
    ) -> Result<f64>;

    /// Get multiple rates efficiently (batch query)
    fn get_rates(
        &self,
        currency_pairs: &[(Currency, Currency)],
        dt: DateTime<Utc>,
    ) -> Result<Vec<f64>> {
        currency_pairs
            .iter()
            .map(|(from, to)| self.get_rate(*from, *to, dt))
            .collect()
    }

    /// Check if rate is available
    fn has_rate(
        &self,
        from_currency: Currency,
        to_currency: Currency,
        dt: DateTime<Utc>,
    ) -> bool {
        self.get_rate(from_currency, to_currency, dt).is_ok()
    }

    /// Get inverse rate (to/from instead of from/to)
    fn get_inverse_rate(
        &self,
        from_currency: Currency,
        to_currency: Currency,
        dt: DateTime<Utc>,
    ) -> Result<f64> {
        self.get_rate(to_currency, from_currency, dt)
            .map(|rate| 1.0 / rate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_currency_from_str() {
        assert_eq!(Currency::from_str("USD").unwrap(), Currency::USD);
        assert_eq!(Currency::from_str("eur").unwrap(), Currency::EUR);
        assert_eq!(Currency::from_str("GBP").unwrap(), Currency::GBP);
        assert!(Currency::from_str("XXX").is_err());
    }

    #[test]
    fn test_currency_display() {
        assert_eq!(Currency::USD.to_string(), "USD");
        assert_eq!(Currency::EUR.to_string(), "EUR");
        assert_eq!(Currency::GBP.as_str(), "GBP");
    }

    #[test]
    fn test_currency_symbol() {
        assert_eq!(Currency::USD.symbol(), "$");
        assert_eq!(Currency::EUR.symbol(), "€");
        assert_eq!(Currency::GBP.symbol(), "£");
    }

    #[test]
    fn test_is_major() {
        assert!(Currency::USD.is_major());
        assert!(Currency::EUR.is_major());
        assert!(!Currency::TRY.is_major());
        assert!(!Currency::INR.is_major());
    }
}
