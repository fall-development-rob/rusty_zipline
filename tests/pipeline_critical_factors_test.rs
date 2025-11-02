//! Integration tests for critical P0 pipeline factors
//!
//! Tests AverageDollarVolume and MaxDrawdown factors in realistic pipeline scenarios

use chrono::{NaiveDate, Utc};
use rusty_zipline::asset::Asset;
use rusty_zipline::error::Result;
use rusty_zipline::pipeline::{
    engine::{DataProvider, Factor, OHLCVBar, Pipeline, PipelineContext},
    AverageDollarVolume, MaxDrawdown,
};
use std::sync::Arc;

/// Mock data provider for testing
struct MockDataProvider {
    /// Price data for each asset
    prices: std::collections::HashMap<u64, Vec<f64>>,
    /// Volume data for each asset
    volumes: std::collections::HashMap<u64, Vec<f64>>,
}

impl MockDataProvider {
    fn new() -> Self {
        let mut prices = std::collections::HashMap::new();
        let mut volumes = std::collections::HashMap::new();

        // Asset 1: High liquidity, low volatility
        prices.insert(
            1,
            vec![
                100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0,
                111.0, 112.0, 113.0, 114.0, 115.0, 116.0, 117.0, 118.0, 119.0, 120.0,
            ],
        );
        volumes.insert(
            1,
            vec![
                10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0,
                10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0,
                10000.0, 10000.0, 10000.0,
            ],
        );

        // Asset 2: Low liquidity, high volatility
        prices.insert(
            2,
            vec![
                50.0, 55.0, 45.0, 60.0, 40.0, 65.0, 35.0, 70.0, 30.0, 75.0, 25.0, 80.0, 20.0,
                85.0, 15.0, 90.0, 10.0, 95.0, 5.0, 100.0, 50.0,
            ],
        );
        volumes.insert(
            2,
            vec![
                100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0,
                100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0,
            ],
        );

        // Asset 3: Medium liquidity, moderate volatility
        prices.insert(
            3,
            vec![
                100.0, 102.0, 101.0, 103.0, 102.0, 104.0, 103.0, 105.0, 104.0, 106.0, 105.0,
                107.0, 106.0, 108.0, 107.0, 109.0, 108.0, 110.0, 109.0, 111.0, 110.0,
            ],
        );
        volumes.insert(
            3,
            vec![
                1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0,
                1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0,
                1000.0,
            ],
        );

        MockDataProvider { prices, volumes }
    }
}

impl DataProvider for MockDataProvider {
    fn get_prices(&self, asset_id: u64, lookback: usize) -> Result<Vec<f64>> {
        Ok(self
            .prices
            .get(&asset_id)
            .map(|p| {
                let start = p.len().saturating_sub(lookback);
                p[start..].to_vec()
            })
            .unwrap_or_default())
    }

    fn get_volumes(&self, asset_id: u64, lookback: usize) -> Result<Vec<f64>> {
        Ok(self
            .volumes
            .get(&asset_id)
            .map(|v| {
                let start = v.len().saturating_sub(lookback);
                v[start..].to_vec()
            })
            .unwrap_or_default())
    }

    fn get_ohlcv(&self, asset_id: u64, lookback: usize) -> Result<Vec<OHLCVBar>> {
        let prices = self.get_prices(asset_id, lookback)?;
        let volumes = self.get_volumes(asset_id, lookback)?;

        Ok(prices
            .iter()
            .zip(volumes.iter())
            .map(|(&close, &volume)| OHLCVBar {
                timestamp: Utc::now(),
                open: close,
                high: close * 1.01,
                low: close * 0.99,
                close,
                volume,
            })
            .collect())
    }

    fn get_latest_price(&self, asset_id: u64) -> Result<f64> {
        Ok(*self
            .prices
            .get(&asset_id)
            .and_then(|p| p.last())
            .unwrap_or(&100.0))
    }
}

#[test]
fn test_average_dollar_volume_factor() {
    let adv = AverageDollarVolume::new(20);
    let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();

    let assets = vec![
        Asset::equity(1, "HIGH_LIQ".to_string(), "HIGH_LIQ".to_string(), start_date),
        Asset::equity(2, "LOW_LIQ".to_string(), "LOW_LIQ".to_string(), start_date),
        Asset::equity(3, "MED_LIQ".to_string(), "MED_LIQ".to_string(), start_date),
    ];

    let data_provider = Arc::new(MockDataProvider::new());
    let context = PipelineContext::new(assets, data_provider, Utc::now());

    let result = adv.compute(Utc::now(), &context).unwrap();

    // Asset 1: High liquidity (~1.1M average dollar volume)
    let adv1 = result.get(&1).unwrap();
    assert!(*adv1 > 1_000_000.0, "Asset 1 should have high liquidity");

    // Asset 2: Low liquidity (~5K average dollar volume)
    let adv2 = result.get(&2).unwrap();
    assert!(*adv2 < 10_000.0, "Asset 2 should have low liquidity");

    // Asset 3: Medium liquidity (~100K average dollar volume)
    let adv3 = result.get(&3).unwrap();
    assert!(
        *adv3 > 90_000.0 && *adv3 < 120_000.0,
        "Asset 3 should have medium liquidity"
    );

    // Verify liquidity ranking
    assert!(adv1 > adv3, "High liquidity > Medium liquidity");
    assert!(adv3 > adv2, "Medium liquidity > Low liquidity");
}

#[test]
fn test_max_drawdown_factor() {
    let mdd = MaxDrawdown::new(20);
    let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();

    let assets = vec![
        Asset::equity(1, "STABLE".to_string(), "STABLE".to_string(), start_date),
        Asset::equity(2, "VOLATILE".to_string(), "VOLATILE".to_string(), start_date),
        Asset::equity(3, "MODERATE".to_string(), "MODERATE".to_string(), start_date),
    ];

    let data_provider = Arc::new(MockDataProvider::new());
    let context = PipelineContext::new(assets, data_provider, Utc::now());

    let result = mdd.compute(Utc::now(), &context).unwrap();

    // Asset 1: Low volatility, minimal drawdown
    let dd1 = result.get(&1).unwrap();
    assert!(*dd1 < 0.05, "Asset 1 should have minimal drawdown");

    // Asset 2: High volatility, large drawdown
    let dd2 = result.get(&2).unwrap();
    assert!(*dd2 > 0.50, "Asset 2 should have large drawdown");

    // Asset 3: Moderate volatility, moderate drawdown
    let dd3 = result.get(&3).unwrap();
    assert!(
        *dd3 > 0.01 && *dd3 < 0.20,
        "Asset 3 should have moderate drawdown"
    );

    // Verify risk ranking
    assert!(dd2 > dd3, "Volatile > Moderate drawdown");
    assert!(dd3 > dd1, "Moderate > Stable drawdown");
}

#[test]
fn test_pipeline_with_both_factors() {
    // Create pipeline with both critical factors
    let mut pipeline = Pipeline::new();

    let adv = Box::new(AverageDollarVolume::new(20));
    let mdd = Box::new(MaxDrawdown::new(20));

    pipeline.add_factor("adv".to_string(), adv);
    pipeline.add_factor("mdd".to_string(), mdd);

    let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
    let assets = vec![
        Asset::equity(1, "HIGH_LIQ_LOW_VOL".to_string(), "GOOD".to_string(), start_date),
        Asset::equity(2, "LOW_LIQ_HIGH_VOL".to_string(), "BAD".to_string(), start_date),
        Asset::equity(3, "MED_LIQ_MED_VOL".to_string(), "OKAY".to_string(), start_date),
    ];

    pipeline.set_universe(assets);

    let data_provider = Arc::new(MockDataProvider::new());
    let output = pipeline.run(Utc::now(), data_provider).unwrap();

    // Verify both factors computed
    assert!(output.factors.contains_key("adv"));
    assert!(output.factors.contains_key("mdd"));

    // Asset 1: Good candidate (high liquidity, low drawdown)
    let adv1 = output.get_factor_value("adv", 1).unwrap();
    let dd1 = output.get_factor_value("mdd", 1).unwrap();
    assert!(adv1 > 1_000_000.0, "Asset 1 should have high liquidity");
    assert!(dd1 < 0.05, "Asset 1 should have low risk");

    // Asset 2: Bad candidate (low liquidity, high drawdown)
    let adv2 = output.get_factor_value("adv", 2).unwrap();
    let dd2 = output.get_factor_value("mdd", 2).unwrap();
    assert!(adv2 < 10_000.0, "Asset 2 should have low liquidity");
    assert!(dd2 > 0.50, "Asset 2 should have high risk");

    // Asset 3: Okay candidate (medium on both)
    let adv3 = output.get_factor_value("adv", 3).unwrap();
    let dd3 = output.get_factor_value("mdd", 3).unwrap();
    assert!(
        adv3 > 90_000.0 && adv3 < 120_000.0,
        "Asset 3 should have medium liquidity"
    );
    assert!(
        dd3 > 0.01 && dd3 < 0.20,
        "Asset 3 should have moderate risk"
    );
}

#[test]
fn test_liquidity_filtering_use_case() {
    // Realistic use case: Filter for stocks with > $500K ADV
    let adv = AverageDollarVolume::new(20);
    let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();

    let assets = vec![
        Asset::equity(1, "AAPL".to_string(), "Apple".to_string(), start_date),
        Asset::equity(2, "PENNY".to_string(), "Penny Stock".to_string(), start_date),
        Asset::equity(3, "MID".to_string(), "Mid Cap".to_string(), start_date),
    ];

    let data_provider = Arc::new(MockDataProvider::new());
    let context = PipelineContext::new(assets, data_provider, Utc::now());

    let result = adv.compute(Utc::now(), &context).unwrap();

    // Filter for ADV > $500K
    let liquid_assets: Vec<u64> = result
        .iter()
        .filter(|(_, &adv_value)| adv_value > 500_000.0)
        .map(|(&asset_id, _)| asset_id)
        .collect();

    // Only asset 1 should pass (high liquidity)
    assert_eq!(liquid_assets.len(), 1);
    assert!(liquid_assets.contains(&1));
}

#[test]
fn test_risk_filtering_use_case() {
    // Realistic use case: Filter out stocks with > 30% drawdown
    let mdd = MaxDrawdown::new(20);
    let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();

    let assets = vec![
        Asset::equity(1, "STABLE".to_string(), "Stable Stock".to_string(), start_date),
        Asset::equity(2, "RISKY".to_string(), "Risky Stock".to_string(), start_date),
        Asset::equity(3, "MODERATE".to_string(), "Moderate Stock".to_string(), start_date),
    ];

    let data_provider = Arc::new(MockDataProvider::new());
    let context = PipelineContext::new(assets, data_provider, Utc::now());

    let result = mdd.compute(Utc::now(), &context).unwrap();

    // Filter for drawdown < 30%
    let safe_assets: Vec<u64> = result
        .iter()
        .filter(|(_, &dd_value)| dd_value < 0.30)
        .map(|(&asset_id, _)| asset_id)
        .collect();

    // Assets 1 and 3 should pass (low/moderate risk)
    assert!(safe_assets.len() >= 2);
    assert!(safe_assets.contains(&1));
    assert!(safe_assets.contains(&3));
    assert!(!safe_assets.contains(&2)); // Risky asset should be filtered out
}

#[test]
fn test_combined_filtering_use_case() {
    // Realistic use case: Filter for liquid AND safe stocks
    let adv = AverageDollarVolume::new(20);
    let mdd = MaxDrawdown::new(20);
    let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();

    let assets = vec![
        Asset::equity(1, "IDEAL".to_string(), "Ideal Stock".to_string(), start_date),
        Asset::equity(2, "RISKY".to_string(), "Risky Stock".to_string(), start_date),
        Asset::equity(3, "OKAY".to_string(), "Okay Stock".to_string(), start_date),
    ];

    let data_provider = Arc::new(MockDataProvider::new());
    let context = PipelineContext::new(assets.clone(), Arc::clone(&data_provider), Utc::now());

    let adv_result = adv.compute(Utc::now(), &context).unwrap();

    let context2 = PipelineContext::new(assets, data_provider, Utc::now());
    let mdd_result = mdd.compute(Utc::now(), &context2).unwrap();

    // Filter for ADV > $500K AND drawdown < 30%
    let ideal_assets: Vec<u64> = adv_result
        .iter()
        .filter(|(&asset_id, &adv_value)| {
            let dd_value = mdd_result.get(&asset_id).unwrap_or(&1.0);
            adv_value > 500_000.0 && *dd_value < 0.30
        })
        .map(|(&asset_id, _)| asset_id)
        .collect();

    // Only asset 1 should pass both filters
    assert_eq!(ideal_assets.len(), 1);
    assert!(ideal_assets.contains(&1));
}
