//! Example usage of external data sources
//!
//! This example demonstrates how to use the data source integrations.
//!
//! Run with: cargo run --example data_sources_example --features async

#[cfg(feature = "async")]
use rusty_zipline::data::sources::{
    AlphaVantageSource, DataSourceRegistry, QuandlDataSource, YahooFinanceSource,
};
use chrono::{TimeZone, Utc};

#[cfg(feature = "async")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    println!("=== External Data Sources Example ===\n");

    // Example 1: Yahoo Finance (Free, no API key)
    println!("1. Yahoo Finance Source");
    println!("   - Free historical data");
    println!("   - No API key required");

    let yahoo = YahooFinanceSource::new()?;
    let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2023, 12, 31, 0, 0, 0).unwrap();

    println!("   Fetching AAPL data from {} to {}", start.format("%Y-%m-%d"), end.format("%Y-%m-%d"));

    // Uncomment to fetch real data (requires internet connection)
    // match yahoo.fetch_historical("AAPL", start, end).await {
    //     Ok(bars) => println!("   ✓ Fetched {} bars", bars.len()),
    //     Err(e) => println!("   ✗ Error: {}", e),
    // }

    println!("   (Set RUST_LOG=debug for detailed logs)\n");

    // Example 2: Quandl (Requires API key)
    println!("2. Quandl Source");
    println!("   - WIKI prices, futures, economic data");
    println!("   - API key required (set QUANDL_API_KEY env var)");

    if let Ok(api_key) = std::env::var("QUANDL_API_KEY") {
        let quandl = QuandlDataSource::new(api_key)?;
        println!("   Fetching WIKI/AAPL dataset");

        // Uncomment to fetch real data
        // match quandl.fetch_dataset("WIKI/AAPL", start, end).await {
        //     Ok(bars) => println!("   ✓ Fetched {} bars", bars.len()),
        //     Err(e) => println!("   ✗ Error: {}", e),
        // }
    } else {
        println!("   ⚠ QUANDL_API_KEY not set, skipping example");
    }
    println!();

    // Example 3: Alpha Vantage (Free tier available)
    println!("3. Alpha Vantage Source");
    println!("   - Intraday and daily data");
    println!("   - Free tier: 5 requests/min, 500/day");
    println!("   - API key required (set ALPHA_VANTAGE_API_KEY env var)");

    if let Ok(api_key) = std::env::var("ALPHA_VANTAGE_API_KEY") {
        let alpha = AlphaVantageSource::new(api_key)?;
        println!("   Fetching daily data for AAPL");

        // Uncomment to fetch real data
        // match alpha.fetch_daily("AAPL").await {
        //     Ok(bars) => println!("   ✓ Fetched {} bars", bars.len()),
        //     Err(e) => println!("   ✗ Error: {}", e),
        // }
    } else {
        println!("   ⚠ ALPHA_VANTAGE_API_KEY not set, skipping example");
    }
    println!();

    // Example 4: Using the Data Source Registry
    println!("4. Data Source Registry");
    println!("   - Manage multiple data sources");
    println!("   - Switch between sources dynamically");

    let mut registry = DataSourceRegistry::new();
    registry.register("yahoo".to_string(), yahoo);

    println!("   Registered sources: {:?}", registry.list_sources());

    if let Some(source) = registry.get("yahoo") {
        println!("   ✓ Retrieved 'yahoo' source from registry");
    }
    println!();

    // Example 5: Bulk fetching
    println!("5. Bulk Data Fetching");
    println!("   - Fetch multiple symbols efficiently");

    let yahoo = YahooFinanceSource::new()?;
    let symbols = vec!["AAPL", "GOOGL", "MSFT"];

    println!("   Fetching data for: {:?}", symbols);

    // Uncomment to fetch real data
    // match yahoo.fetch_multiple(&symbols).await {
    //     Ok(results) => {
    //         println!("   ✓ Fetched data for {} symbols", results.len());
    //         for (symbol, bars) in results {
    //             println!("     - {}: {} bars", symbol, bars.len());
    //         }
    //     }
    //     Err(e) => println!("   ✗ Error: {}", e),
    // }

    println!("\n=== Example Complete ===");
    println!("To fetch real data, uncomment the fetch calls in the example code.");
    println!("Make sure to set API keys as environment variables:");
    println!("  export QUANDL_API_KEY='your_key'");
    println!("  export ALPHA_VANTAGE_API_KEY='your_key'");

    Ok(())
}

#[cfg(not(feature = "async"))]
fn main() {
    println!("This example requires the 'async' feature.");
    println!("Run with: cargo run --example data_sources_example --features async");
}
