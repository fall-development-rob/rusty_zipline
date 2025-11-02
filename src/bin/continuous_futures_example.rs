//! Continuous Futures Example
//!
//! Demonstrates how to construct and use continuous futures contracts
//! for backtesting with realistic E-mini S&P 500 (ES) futures data.

use chrono::{TimeZone, Utc};
use rusty_zipline::asset::{Asset, AssetType};
use rusty_zipline::data::bar_reader::{Bar, BarReader, DailyBarReader};
use rusty_zipline::data::continuous_futures::{
    AdjustmentStyle, ContractChain, DefaultContinuousFutureReader, FutureContract, RollSchedule,
    RollStyle,
};
use rusty_zipline::error::Result;
use std::sync::Arc;

fn main() -> Result<()> {
    println!("=== Continuous Futures Example ===\n");

    // 1. Create mock bar reader with ES futures data
    let mut bar_reader = DailyBarReader::new();

    // Simulate E-mini S&P 500 futures contracts
    // ESH3 (March 2023) - expires March 17, 2023
    // ESM3 (June 2023) - expires June 16, 2023
    // ESU3 (September 2023) - expires September 15, 2023

    println!("1. Loading futures contract data...");

    // ESH3 data (March contract)
    let esh3_asset = Asset::new(1001, "ESH3".to_string(), "CME".to_string(), AssetType::Future);
    let esh3_bars = create_sample_bars(
        Utc.with_ymd_and_hms(2023, 1, 3, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2023, 3, 17, 0, 0, 0).unwrap(),
        4000.0,
        4200.0,
    );
    bar_reader.load_from_memory(1001, esh3_bars)?;

    // ESM3 data (June contract)
    let esm3_asset = Asset::new(1002, "ESM3".to_string(), "CME".to_string(), AssetType::Future);
    let esm3_bars = create_sample_bars(
        Utc.with_ymd_and_hms(2023, 1, 3, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2023, 6, 16, 0, 0, 0).unwrap(),
        4010.0,
        4250.0,
    );
    bar_reader.load_from_memory(1002, esm3_bars)?;

    // ESU3 data (September contract)
    let esu3_asset = Asset::new(1003, "ESU3".to_string(), "CME".to_string(), AssetType::Future);
    let esu3_bars = create_sample_bars(
        Utc.with_ymd_and_hms(2023, 1, 3, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2023, 9, 15, 0, 0, 0).unwrap(),
        4020.0,
        4300.0,
    );
    bar_reader.load_from_memory(1003, esu3_bars)?;

    println!("  ✓ Loaded {} contracts", 3);
    println!("  ✓ Total bars: {}\n", bar_reader.bar_count(1001) + bar_reader.bar_count(1002) + bar_reader.bar_count(1003));

    // 2. Create contract chain for ES
    println!("2. Building contract chain...");
    let mut es_chain = ContractChain::new("ES".to_string());

    es_chain.add_contract(FutureContract::new(
        "ESH3".to_string(),
        "ES".to_string(),
        Utc.with_ymd_and_hms(2023, 3, 17, 0, 0, 0).unwrap(),
        "H3".to_string(),
        1001,
    ))?;

    es_chain.add_contract(FutureContract::new(
        "ESM3".to_string(),
        "ES".to_string(),
        Utc.with_ymd_and_hms(2023, 6, 16, 0, 0, 0).unwrap(),
        "M3".to_string(),
        1002,
    ))?;

    es_chain.add_contract(FutureContract::new(
        "ESU3".to_string(),
        "ES".to_string(),
        Utc.with_ymd_and_hms(2023, 9, 15, 0, 0, 0).unwrap(),
        "U3".to_string(),
        1003,
    ))?;

    println!("  ✓ Chain contains {} contracts", es_chain.len());
    if let Some((start, end)) = es_chain.date_range() {
        println!("  ✓ Date range: {} to {}\n", start.format("%Y-%m-%d"), end.format("%Y-%m-%d"));
    }

    // 3. Create continuous futures reader
    println!("3. Initializing continuous futures reader...");
    let mut reader = DefaultContinuousFutureReader::new(Arc::new(bar_reader));
    reader.add_chain(es_chain);

    // Set calendar roll schedule (roll 5 days before expiration)
    let roll_schedule = RollSchedule::calendar(5);
    reader.set_roll_schedule(roll_schedule);
    println!("  ✓ Roll schedule: Calendar (5 days before expiration)\n");

    // 4. Get active contract at different dates
    println!("4. Testing active contract selection...");

    let test_dates = vec![
        Utc.with_ymd_and_hms(2023, 2, 1, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2023, 3, 20, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2023, 6, 20, 0, 0, 0).unwrap(),
    ];

    for dt in test_dates {
        if let Ok(Some(contract)) = reader.get_active_contract("ES", dt, 0) {
            println!(
                "  Date: {} → Front month: {} (expires: {})",
                dt.format("%Y-%m-%d"),
                contract.symbol,
                contract.expiration.format("%Y-%m-%d")
            );
        }
    }
    println!();

    // 5. Demonstrate different adjustment styles
    println!("5. Comparing adjustment styles...");

    let start = Utc.with_ymd_and_hms(2023, 1, 3, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2023, 3, 20, 0, 0, 0).unwrap();

    let styles = vec![
        ("No Adjustment", AdjustmentStyle::None),
        ("Panama Canal", AdjustmentStyle::PanamaCanal),
        ("Backward Ratio", AdjustmentStyle::BackwardRatio),
    ];

    for (name, style) in styles {
        println!("\n  {} ({:?}):", name, style);

        match reader.get_continuous_prices("ES", start, end, 0, RollStyle::Calendar, style) {
            Ok(bars) => {
                if bars.len() > 0 {
                    println!("    ✓ Generated {} continuous bars", bars.len());
                    println!("    ✓ First close: {:.2}", bars[0].close);
                    if bars.len() > 1 {
                        println!("    ✓ Last close: {:.2}", bars[bars.len() - 1].close);
                    }
                } else {
                    println!("    ⚠ No bars generated");
                }
            }
            Err(e) => println!("    ✗ Error: {}", e),
        }
    }

    // 6. Front month vs back month comparison
    println!("\n6. Front month vs second month (offset comparison)...");

    let test_date = Utc.with_ymd_and_hms(2023, 2, 15, 0, 0, 0).unwrap();

    if let Ok(Some(front)) = reader.get_active_contract("ES", test_date, 0) {
        println!("  Front month (offset=0): {}", front.symbol);
        println!("    Expires: {}", front.expiration.format("%Y-%m-%d"));
    }

    if let Ok(Some(second)) = reader.get_active_contract("ES", test_date, 1) {
        println!("  Second month (offset=1): {}", second.symbol);
        println!("    Expires: {}", second.expiration.format("%Y-%m-%d"));
    }

    // 7. Summary statistics
    println!("\n7. Summary:");
    println!("  ✓ Continuous futures allow seamless backtesting across contract rolls");
    println!("  ✓ Multiple adjustment styles handle price discontinuities");
    println!("  ✓ Calendar-based rolls automate contract switching");
    println!("  ✓ Offsets enable trading deferred contracts\n");

    println!("=== Example Complete ===");

    Ok(())
}

/// Create sample bars with gradual price movement
fn create_sample_bars(
    start: chrono::DateTime<Utc>,
    end: chrono::DateTime<Utc>,
    start_price: f64,
    end_price: f64,
) -> Vec<Bar> {
    let mut bars = Vec::new();
    let mut current_date = start;
    let days = (end - start).num_days() as f64;
    let price_increment = (end_price - start_price) / days;

    let mut price = start_price;

    while current_date <= end {
        // Skip weekends (simplified)
        let weekday = current_date.weekday();
        if weekday != chrono::Weekday::Sat && weekday != chrono::Weekday::Sun {
            let bar = Bar::new(
                price,
                price + 10.0,
                price - 10.0,
                price + price_increment,
                100000.0,
                current_date,
            );
            bars.push(bar);
            price += price_increment;
        }

        current_date = current_date + chrono::Duration::days(1);
    }

    bars
}
