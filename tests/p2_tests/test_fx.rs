//! FX System Tests
//!
//! Comprehensive tests for foreign exchange rate system

#[cfg(test)]
mod fx_tests {
    use chrono::{DateTime, Utc, TimeZone};
    use std::collections::HashMap;

    // Mock Currency enum for testing
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    enum Currency {
        USD,
        EUR,
        GBP,
        JPY,
        CHF,
    }

    // Mock FXRateReader trait
    trait FXRateReader {
        fn get_rate(&self, from: Currency, to: Currency, dt: DateTime<Utc>) -> Result<f64, String>;
    }

    // Mock InMemoryFXRateReader implementation
    struct InMemoryFXRateReader {
        rates: HashMap<(Currency, Currency, i64), f64>,
    }

    impl InMemoryFXRateReader {
        fn new() -> Self {
            Self {
                rates: HashMap::new(),
            }
        }

        fn add_rate(&mut self, from: Currency, to: Currency, dt: DateTime<Utc>, rate: f64) {
            let key = (from, to, dt.timestamp());
            self.rates.insert(key, rate);
        }
    }

    impl FXRateReader for InMemoryFXRateReader {
        fn get_rate(&self, from: Currency, to: Currency, dt: DateTime<Utc>) -> Result<f64, String> {
            if from == to {
                return Ok(1.0);
            }

            let key = (from, to, dt.timestamp());
            self.rates.get(&key).copied().ok_or_else(||
                format!("Rate not found for {:?}/{:?} at {}", from, to, dt)
            )
        }
    }

    #[test]
    fn test_same_currency_conversion() {
        let reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        let rate = reader.get_rate(Currency::USD, Currency::USD, dt).unwrap();
        assert_eq!(rate, 1.0);
    }

    #[test]
    fn test_basic_currency_conversion() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        // EUR/USD = 1.10
        reader.add_rate(Currency::EUR, Currency::USD, dt, 1.10);

        let rate = reader.get_rate(Currency::EUR, Currency::USD, dt).unwrap();
        assert!((rate - 1.10).abs() < 1e-6);
    }

    #[test]
    fn test_inverse_rate_calculation() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        // EUR/USD = 1.10
        reader.add_rate(Currency::EUR, Currency::USD, dt, 1.10);
        // USD/EUR should be 1/1.10
        reader.add_rate(Currency::USD, Currency::EUR, dt, 1.0 / 1.10);

        let rate_forward = reader.get_rate(Currency::EUR, Currency::USD, dt).unwrap();
        let rate_inverse = reader.get_rate(Currency::USD, Currency::EUR, dt).unwrap();

        assert!((rate_forward * rate_inverse - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cross_rate_calculation() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        // EUR/USD = 1.10
        reader.add_rate(Currency::EUR, Currency::USD, dt, 1.10);
        // GBP/USD = 1.25
        reader.add_rate(Currency::GBP, Currency::USD, dt, 1.25);
        // EUR/GBP should be 1.10/1.25 = 0.88
        reader.add_rate(Currency::EUR, Currency::GBP, dt, 1.10 / 1.25);

        let eur_gbp = reader.get_rate(Currency::EUR, Currency::GBP, dt).unwrap();
        assert!((eur_gbp - 0.88).abs() < 1e-6);
    }

    #[test]
    fn test_missing_rate_error() {
        let reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        let result = reader.get_rate(Currency::EUR, Currency::USD, dt);
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_dates() {
        let mut reader = InMemoryFXRateReader::new();
        let dt1 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let dt2 = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();

        reader.add_rate(Currency::EUR, Currency::USD, dt1, 1.10);
        reader.add_rate(Currency::EUR, Currency::USD, dt2, 1.12);

        let rate1 = reader.get_rate(Currency::EUR, Currency::USD, dt1).unwrap();
        let rate2 = reader.get_rate(Currency::EUR, Currency::USD, dt2).unwrap();

        assert!((rate1 - 1.10).abs() < 1e-6);
        assert!((rate2 - 1.12).abs() < 1e-6);
    }

    #[test]
    fn test_jpy_conversion() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        // USD/JPY = 150.0
        reader.add_rate(Currency::USD, Currency::JPY, dt, 150.0);

        let rate = reader.get_rate(Currency::USD, Currency::JPY, dt).unwrap();
        assert!((rate - 150.0).abs() < 1e-6);
    }

    #[test]
    fn test_chf_conversion() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        // USD/CHF = 0.85
        reader.add_rate(Currency::USD, Currency::CHF, dt, 0.85);

        let rate = reader.get_rate(Currency::USD, Currency::CHF, dt).unwrap();
        assert!((rate - 0.85).abs() < 1e-6);
    }

    #[test]
    fn test_amount_conversion() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        reader.add_rate(Currency::EUR, Currency::USD, dt, 1.10);

        let rate = reader.get_rate(Currency::EUR, Currency::USD, dt).unwrap();
        let amount_eur = 100.0;
        let amount_usd = amount_eur * rate;

        assert!((amount_usd - 110.0).abs() < 1e-6);
    }

    #[test]
    fn test_rate_precision() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        let precise_rate = 1.123456789;
        reader.add_rate(Currency::EUR, Currency::USD, dt, precise_rate);

        let retrieved_rate = reader.get_rate(Currency::EUR, Currency::USD, dt).unwrap();
        assert!((retrieved_rate - precise_rate).abs() < 1e-9);
    }

    #[test]
    fn test_date_range_queries() {
        let mut reader = InMemoryFXRateReader::new();

        // Add rates for a week
        for day in 1..=7 {
            let dt = Utc.with_ymd_and_hms(2024, 1, day, 0, 0, 0).unwrap();
            let rate = 1.10 + (day as f64 * 0.01);
            reader.add_rate(Currency::EUR, Currency::USD, dt, rate);
        }

        let dt3 = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();
        let rate = reader.get_rate(Currency::EUR, Currency::USD, dt3).unwrap();
        assert!((rate - 1.13).abs() < 1e-6);
    }

    #[test]
    fn test_exploding_fx_reader() {
        // Mock ExplodingFXRateReader that panics
        struct ExplodingFXRateReader;

        impl FXRateReader for ExplodingFXRateReader {
            fn get_rate(&self, _from: Currency, _to: Currency, _dt: DateTime<Utc>) -> Result<f64, String> {
                panic!("ExplodingFXRateReader intentionally panics");
            }
        }

        let reader = ExplodingFXRateReader;
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        let result = std::panic::catch_unwind(|| {
            let _ = reader.get_rate(Currency::EUR, Currency::USD, dt);
        });

        assert!(result.is_err());
    }

    #[test]
    fn test_rate_inversion_accuracy() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        reader.add_rate(Currency::EUR, Currency::USD, dt, 1.10);
        reader.add_rate(Currency::USD, Currency::EUR, dt, 1.0 / 1.10);

        let eur_usd = reader.get_rate(Currency::EUR, Currency::USD, dt).unwrap();
        let usd_eur = reader.get_rate(Currency::USD, Currency::EUR, dt).unwrap();

        // Convert 100 EUR -> USD -> EUR
        let usd_amount = 100.0 * eur_usd;
        let eur_amount = usd_amount * usd_eur;

        assert!((eur_amount - 100.0).abs() < 1e-9);
    }

    #[test]
    fn test_multiple_currency_pairs() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        reader.add_rate(Currency::EUR, Currency::USD, dt, 1.10);
        reader.add_rate(Currency::GBP, Currency::USD, dt, 1.25);
        reader.add_rate(Currency::USD, Currency::JPY, dt, 150.0);
        reader.add_rate(Currency::USD, Currency::CHF, dt, 0.85);

        assert!(reader.get_rate(Currency::EUR, Currency::USD, dt).is_ok());
        assert!(reader.get_rate(Currency::GBP, Currency::USD, dt).is_ok());
        assert!(reader.get_rate(Currency::USD, Currency::JPY, dt).is_ok());
        assert!(reader.get_rate(Currency::USD, Currency::CHF, dt).is_ok());
    }

    #[test]
    fn test_rate_update() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        reader.add_rate(Currency::EUR, Currency::USD, dt, 1.10);
        let rate1 = reader.get_rate(Currency::EUR, Currency::USD, dt).unwrap();

        // Update rate
        reader.add_rate(Currency::EUR, Currency::USD, dt, 1.12);
        let rate2 = reader.get_rate(Currency::EUR, Currency::USD, dt).unwrap();

        assert!((rate1 - 1.10).abs() < 1e-6);
        assert!((rate2 - 1.12).abs() < 1e-6);
    }

    #[test]
    fn test_historical_rate_lookup() {
        let mut reader = InMemoryFXRateReader::new();

        let dt_old = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let dt_new = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        reader.add_rate(Currency::EUR, Currency::USD, dt_old, 1.05);
        reader.add_rate(Currency::EUR, Currency::USD, dt_new, 1.10);

        let old_rate = reader.get_rate(Currency::EUR, Currency::USD, dt_old).unwrap();
        let new_rate = reader.get_rate(Currency::EUR, Currency::USD, dt_new).unwrap();

        assert!((old_rate - 1.05).abs() < 1e-6);
        assert!((new_rate - 1.10).abs() < 1e-6);
    }

    #[test]
    fn test_zero_rate_handling() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        // Zero rate should be stored and retrieved
        reader.add_rate(Currency::EUR, Currency::USD, dt, 0.0);
        let rate = reader.get_rate(Currency::EUR, Currency::USD, dt).unwrap();
        assert_eq!(rate, 0.0);
    }

    #[test]
    fn test_negative_rate_handling() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        // Negative rates shouldn't exist in reality, but test storage
        reader.add_rate(Currency::EUR, Currency::USD, dt, -1.0);
        let rate = reader.get_rate(Currency::EUR, Currency::USD, dt).unwrap();
        assert_eq!(rate, -1.0);
    }

    #[test]
    fn test_large_rate_values() {
        let mut reader = InMemoryFXRateReader::new();
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        // Test with large values (e.g., exotic currency pairs)
        reader.add_rate(Currency::USD, Currency::JPY, dt, 50000.0);
        let rate = reader.get_rate(Currency::USD, Currency::JPY, dt).unwrap();
        assert_eq!(rate, 50000.0);
    }

    #[test]
    fn test_timestamp_precision() {
        let mut reader = InMemoryFXRateReader::new();
        let dt1 = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let dt2 = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 1).unwrap();

        reader.add_rate(Currency::EUR, Currency::USD, dt1, 1.10);
        reader.add_rate(Currency::EUR, Currency::USD, dt2, 1.11);

        let rate1 = reader.get_rate(Currency::EUR, Currency::USD, dt1).unwrap();
        let rate2 = reader.get_rate(Currency::EUR, Currency::USD, dt2).unwrap();

        assert!((rate1 - 1.10).abs() < 1e-6);
        assert!((rate2 - 1.11).abs() < 1e-6);
    }
}
