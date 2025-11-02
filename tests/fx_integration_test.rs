//! Integration tests for FX system
//!
//! Tests cross-module interactions and real-world usage scenarios

use chrono::{TimeZone, Utc};
use rusty_zipline::data::fx::{
    convert_amount, convert_amounts, portfolio_value, Currency, ExplodingFXRateReader,
    FXRateReader, InMemoryFXRateReader,
};

#[test]
fn test_multi_currency_portfolio() {
    let mut reader = InMemoryFXRateReader::new();
    let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

    // Setup rates
    reader
        .add_rate(Currency::EUR, Currency::USD, dt, 1.20)
        .unwrap();
    reader
        .add_rate(Currency::GBP, Currency::USD, dt, 1.30)
        .unwrap();
    reader
        .add_rate(Currency::JPY, Currency::USD, dt, 0.0091)
        .unwrap();

    // Portfolio in multiple currencies
    let positions = vec![
        (1000.0, Currency::USD), // $1,000
        (500.0, Currency::EUR),  // €500 = $600
        (200.0, Currency::GBP),  // £200 = $260
        (10000.0, Currency::JPY), // ¥10,000 = $91
    ];

    let total_usd = portfolio_value(&reader, &positions, Currency::USD, dt).unwrap();

    assert!((total_usd - 1951.0).abs() < 0.1);
}

#[test]
fn test_currency_conversion_chain() {
    let mut reader = InMemoryFXRateReader::new();
    let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

    // Setup rates for conversion chain
    reader
        .add_rate(Currency::EUR, Currency::USD, dt, 1.20)
        .unwrap();
    reader
        .add_rate(Currency::GBP, Currency::EUR, dt, 1.15)
        .unwrap();

    // Convert GBP -> EUR
    let eur_amount = convert_amount(&reader, 100.0, Currency::GBP, Currency::EUR, dt).unwrap();
    assert!((eur_amount - 115.0).abs() < 0.01);

    // Convert EUR -> USD
    let usd_amount = convert_amount(&reader, eur_amount, Currency::EUR, Currency::USD, dt).unwrap();
    assert!((usd_amount - 138.0).abs() < 0.01);
}

#[test]
fn test_batch_conversion() {
    let mut reader = InMemoryFXRateReader::new();
    let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

    reader
        .add_rate(Currency::EUR, Currency::USD, dt, 1.20)
        .unwrap();
    reader
        .add_rate(Currency::GBP, Currency::USD, dt, 1.30)
        .unwrap();
    reader
        .add_rate(Currency::CHF, Currency::USD, dt, 1.10)
        .unwrap();

    let amounts = vec![
        (100.0, Currency::EUR),
        (100.0, Currency::GBP),
        (100.0, Currency::CHF),
    ];

    let converted = convert_amounts(&reader, &amounts, Currency::USD, dt).unwrap();

    assert_eq!(converted.len(), 3);
    assert_eq!(converted[0], 120.0);
    assert_eq!(converted[1], 130.0);
    assert_eq!(converted[2], 110.0);
}

#[test]
fn test_time_series_conversion() {
    let mut reader = InMemoryFXRateReader::new();

    // Add rates over multiple days
    let dates = vec![
        Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2024, 1, 2, 12, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2024, 1, 3, 12, 0, 0).unwrap(),
    ];

    reader
        .add_rate(Currency::EUR, Currency::USD, dates[0], 1.20)
        .unwrap();
    reader
        .add_rate(Currency::EUR, Currency::USD, dates[1], 1.22)
        .unwrap();
    reader
        .add_rate(Currency::EUR, Currency::USD, dates[2], 1.24)
        .unwrap();

    // Convert amounts at different times
    for (i, dt) in dates.iter().enumerate() {
        let rate = reader
            .get_rate(Currency::EUR, Currency::USD, *dt)
            .unwrap();
        let expected_rate = 1.20 + (i as f64 * 0.02);
        assert!((rate - expected_rate).abs() < 0.001);
    }
}

#[test]
fn test_auto_inverse_rates() {
    let mut reader = InMemoryFXRateReader::new();
    let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

    // Only add EUR/USD
    reader
        .add_rate(Currency::EUR, Currency::USD, dt, 1.20)
        .unwrap();

    // Should automatically calculate USD/EUR
    let usd_eur = reader
        .get_rate(Currency::USD, Currency::EUR, dt)
        .unwrap();
    assert!((usd_eur - 0.8333).abs() < 0.001);

    // Convert using inverse rate
    let eur_amount = convert_amount(&reader, 120.0, Currency::USD, Currency::EUR, dt).unwrap();
    assert!((eur_amount - 100.0).abs() < 0.1);
}

#[test]
fn test_cross_rate_calculation() {
    let mut reader = InMemoryFXRateReader::with_config(true, true, Currency::USD);
    let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

    // Setup rates via USD
    reader
        .add_rate(Currency::EUR, Currency::USD, dt, 1.20)
        .unwrap();
    reader
        .add_rate(Currency::GBP, Currency::USD, dt, 1.30)
        .unwrap();
    reader
        .add_rate(Currency::JPY, Currency::USD, dt, 0.0091)
        .unwrap();

    // Test EUR/GBP cross rate
    let eur_gbp = reader
        .get_rate(Currency::EUR, Currency::GBP, dt)
        .unwrap();
    // EUR/GBP = (EUR/USD) / (GBP/USD) = 1.20 / 1.30 ≈ 0.923
    assert!((eur_gbp - 0.923).abs() < 0.001);

    // Test EUR/JPY cross rate
    let eur_jpy = reader
        .get_rate(Currency::EUR, Currency::JPY, dt)
        .unwrap();
    // EUR/JPY = (EUR/USD) / (JPY/USD) = 1.20 / 0.0091 ≈ 131.87
    assert!((eur_jpy - 131.87).abs() < 0.1);
}

#[test]
fn test_forward_fill_semantics() {
    let mut reader = InMemoryFXRateReader::new();

    let dt1 = Utc.with_ymd_and_hms(2024, 1, 1, 9, 0, 0).unwrap();
    let dt2 = Utc.with_ymd_and_hms(2024, 1, 1, 15, 0, 0).unwrap();
    let dt3 = Utc.with_ymd_and_hms(2024, 1, 2, 9, 0, 0).unwrap();

    // Add rates at 9 AM and 3 PM on day 1
    reader
        .add_rate(Currency::EUR, Currency::USD, dt1, 1.20)
        .unwrap();
    reader
        .add_rate(Currency::EUR, Currency::USD, dt2, 1.22)
        .unwrap();

    // Query at noon should use 9 AM rate (forward-fill)
    let dt_noon = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
    let rate = reader
        .get_rate(Currency::EUR, Currency::USD, dt_noon)
        .unwrap();
    assert_eq!(rate, 1.20);

    // Query at 6 PM should use 3 PM rate
    let dt_evening = Utc.with_ymd_and_hms(2024, 1, 1, 18, 0, 0).unwrap();
    let rate = reader
        .get_rate(Currency::EUR, Currency::USD, dt_evening)
        .unwrap();
    assert_eq!(rate, 1.22);

    // Query on day 2 should use last available rate (3 PM day 1)
    let rate = reader
        .get_rate(Currency::EUR, Currency::USD, dt3)
        .unwrap();
    assert_eq!(rate, 1.22);
}

#[test]
#[should_panic(expected = "FX rate access not allowed")]
fn test_exploding_reader_prevents_fx() {
    let reader = ExplodingFXRateReader::new();
    let dt = Utc::now();

    // This should panic
    let _ = reader.get_rate(Currency::EUR, Currency::USD, dt);
}

#[test]
fn test_exploding_reader_allows_same_currency() {
    let reader = ExplodingFXRateReader::new();
    let dt = Utc::now();

    // Same currency should work
    let rate = reader
        .get_rate(Currency::USD, Currency::USD, dt)
        .unwrap();
    assert_eq!(rate, 1.0);
}

#[test]
fn test_csv_loading() {
    let mut reader = InMemoryFXRateReader::new();

    let csv_data = r#"2024-01-01T12:00:00Z,EUR,USD,1.20
2024-01-02T12:00:00Z,EUR,USD,1.22
2024-01-03T12:00:00Z,EUR,USD,1.24
2024-01-01T12:00:00Z,GBP,USD,1.30
2024-01-02T12:00:00Z,GBP,USD,1.32"#;

    let count = reader.load_from_csv(csv_data).unwrap();
    assert_eq!(count, 5);

    let dt = Utc.with_ymd_and_hms(2024, 1, 2, 12, 0, 0).unwrap();
    let eur_usd = reader
        .get_rate(Currency::EUR, Currency::USD, dt)
        .unwrap();
    assert_eq!(eur_usd, 1.22);

    let gbp_usd = reader
        .get_rate(Currency::GBP, Currency::USD, dt)
        .unwrap();
    assert_eq!(gbp_usd, 1.32);
}

#[test]
fn test_cache_performance() {
    let mut reader = InMemoryFXRateReader::new();
    let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

    reader
        .add_rate(Currency::EUR, Currency::USD, dt, 1.20)
        .unwrap();

    // First access - cache miss
    let rate1 = reader
        .get_rate(Currency::EUR, Currency::USD, dt)
        .unwrap();

    // Subsequent accesses - cache hits (should be same value)
    for _ in 0..100 {
        let rate = reader
            .get_rate(Currency::EUR, Currency::USD, dt)
            .unwrap();
        assert_eq!(rate, rate1);
    }
}

#[test]
fn test_major_currency_pairs() {
    let mut reader = InMemoryFXRateReader::new();
    let dt = Utc::now();

    // Major currency pairs
    let pairs = vec![
        (Currency::EUR, Currency::USD, 1.20),
        (Currency::GBP, Currency::USD, 1.30),
        (Currency::USD, Currency::JPY, 110.0),
        (Currency::USD, Currency::CHF, 0.91),
        (Currency::AUD, Currency::USD, 0.75),
        (Currency::USD, Currency::CAD, 1.25),
    ];

    for (from, to, rate) in pairs {
        reader.add_rate(from, to, dt, rate).unwrap();
    }

    // Verify all rates are retrievable
    assert_eq!(
        reader
            .get_rate(Currency::EUR, Currency::USD, dt)
            .unwrap(),
        1.20
    );
    assert_eq!(
        reader
            .get_rate(Currency::GBP, Currency::USD, dt)
            .unwrap(),
        1.30
    );
    assert_eq!(
        reader
            .get_rate(Currency::USD, Currency::JPY, dt)
            .unwrap(),
        110.0
    );
}

#[test]
fn test_error_handling() {
    let reader = InMemoryFXRateReader::new();
    let dt = Utc::now();

    // No rate available
    let result = reader.get_rate(Currency::EUR, Currency::USD, dt);
    assert!(result.is_err());

    // Invalid rate
    let mut reader = InMemoryFXRateReader::new();
    let result = reader.add_rate(Currency::EUR, Currency::USD, dt, -1.0);
    assert!(result.is_err());

    let result = reader.add_rate(Currency::EUR, Currency::USD, dt, 0.0);
    assert!(result.is_err());
}
