//! Comprehensive tests for P0 critical errors
//!
//! Tests all 15 P0 error types to ensure proper error creation,
//! message formatting, and debugging information.

use chrono::Utc;
use rusty_zipline::error::ZiplineError;
use uuid::Uuid;

#[cfg(test)]
mod p0_error_tests {
    use super::*;

    // ========== Category 1: Trading Control Errors ==========

    #[test]
    fn test_max_position_size_exceeded() {
        let err = ZiplineError::MaxPositionSizeExceeded {
            asset: 123,
            symbol: "AAPL".to_string(),
            attempted_order: 1500.0,
            max_shares: Some(1000.0),
            max_notional: Some(100_000.0),
        };

        let msg = err.to_string();
        assert!(msg.contains("Max position size exceeded"));
        assert!(msg.contains("AAPL"));
        assert!(msg.contains("123"));
        assert!(msg.contains("1500"));
        assert!(msg.contains("1000"));
        assert!(msg.contains("100000"));
    }

    #[test]
    fn test_max_position_size_exceeded_shares_only() {
        let err = ZiplineError::MaxPositionSizeExceeded {
            asset: 456,
            symbol: "GOOGL".to_string(),
            attempted_order: 2000.0,
            max_shares: Some(1500.0),
            max_notional: None,
        };

        let msg = err.to_string();
        assert!(msg.contains("Max position size exceeded"));
        assert!(msg.contains("GOOGL"));
        assert!(msg.contains("2000"));
        assert!(msg.contains("1500"));
        assert!(msg.contains("None"));
    }

    #[test]
    fn test_max_order_count_exceeded() {
        let date = Utc::now();
        let err = ZiplineError::MaxOrderCountExceeded {
            current_count: 15,
            max_count: 10,
            date,
        };

        let msg = err.to_string();
        assert!(msg.contains("Max order count exceeded"));
        assert!(msg.contains("15 orders"));
        assert!(msg.contains("10"));
    }

    #[test]
    fn test_max_order_size_exceeded() {
        let err = ZiplineError::MaxOrderSizeExceeded {
            asset: 789,
            order_size: 5000.0,
            max_size: 3000.0,
        };

        let msg = err.to_string();
        assert!(msg.contains("Max order size exceeded"));
        assert!(msg.contains("789"));
        assert!(msg.contains("5000"));
        assert!(msg.contains("3000"));
    }

    #[test]
    fn test_max_leverage_exceeded() {
        let err = ZiplineError::MaxLeverageExceeded {
            current_leverage: 3.5,
            max_leverage: 2.0,
        };

        let msg = err.to_string();
        assert!(msg.contains("Max leverage exceeded"));
        assert!(msg.contains("3.5"));
        assert!(msg.contains("2.0"));
        assert!(msg.contains("3.50x")); // Verify formatting
    }

    // ========== Category 2: Data Availability Errors ==========

    #[test]
    fn test_history_window_before_first_data() {
        let requested = Utc::now();
        let first_available = requested + chrono::Duration::days(10);

        let err = ZiplineError::HistoryWindowBeforeFirstData {
            asset: 101,
            requested_start: requested,
            first_available,
        };

        let msg = err.to_string();
        assert!(msg.contains("History window starts before first available data"));
        assert!(msg.contains("101"));
        assert!(msg.contains("requested"));
        assert!(msg.contains("first available"));
    }

    #[test]
    fn test_asset_non_existent() {
        let requested_dt = Utc::now();
        let start_date = requested_dt - chrono::Duration::days(365);
        let end_date = requested_dt - chrono::Duration::days(30);

        let err = ZiplineError::AssetNonExistent {
            asset: 202,
            requested_dt,
            start_date: Some(start_date),
            end_date: Some(end_date),
        };

        let msg = err.to_string();
        assert!(msg.contains("Asset 202 does not exist"));
        assert!(msg.contains("Asset trading dates"));
    }

    #[test]
    fn test_asset_non_existent_no_dates() {
        let requested_dt = Utc::now();

        let err = ZiplineError::AssetNonExistent {
            asset: 303,
            requested_dt,
            start_date: None,
            end_date: None,
        };

        let msg = err.to_string();
        assert!(msg.contains("Asset 303 does not exist"));
        assert!(msg.contains("None"));
    }

    #[test]
    fn test_pricing_data_not_loaded() {
        let err = ZiplineError::PricingDataNotLoaded {
            assets: vec![1, 2, 3, 4, 5],
        };

        let msg = err.to_string();
        assert!(msg.contains("Pricing data not loaded"));
        assert!(msg.contains("1"));
        assert!(msg.contains("5"));
        assert!(msg.contains("load_pricing()"));
    }

    #[test]
    fn test_no_further_data() {
        let current = Utc::now();
        let requested = current + chrono::Duration::hours(1);

        let err = ZiplineError::NoFurtherData {
            current_dt: current,
            requested_dt: requested,
        };

        let msg = err.to_string();
        assert!(msg.contains("Cannot request data beyond current simulation time"));
        assert!(msg.contains("Current:"));
        assert!(msg.contains("requested:"));
    }

    // ========== Category 3: Pipeline Errors ==========

    #[test]
    fn test_unsupported_pipeline_output() {
        let err = ZiplineError::UnsupportedPipelineOutput {
            column: "momentum".to_string(),
            expected: "Float64".to_string(),
            actual: "String".to_string(),
        };

        let msg = err.to_string();
        assert!(msg.contains("Pipeline produced unsupported output type"));
        assert!(msg.contains("momentum"));
        assert!(msg.contains("Float64"));
        assert!(msg.contains("String"));
    }

    #[test]
    fn test_term_not_in_graph() {
        let err = ZiplineError::TermNotInGraph {
            term_name: "missing_factor".to_string(),
            available_terms: vec!["price".to_string(), "volume".to_string(), "returns".to_string()],
        };

        let msg = err.to_string();
        assert!(msg.contains("Term 'missing_factor' not found"));
        assert!(msg.contains("Available terms:"));
        assert!(msg.contains("price"));
        assert!(msg.contains("volume"));
        assert!(msg.contains("returns"));
    }

    // ========== Category 4: Order Management Errors ==========

    #[test]
    fn test_order_id_not_found() {
        let order_id = Uuid::new_v4();
        let err = ZiplineError::OrderIdNotFound { order_id };

        let msg = err.to_string();
        assert!(msg.contains("Order ID"));
        assert!(msg.contains("not found in order tracker"));
        assert!(msg.contains(&order_id.to_string()));
    }

    #[test]
    fn test_order_after_session_end() {
        let session_end = Utc::now();
        let attempted = session_end + chrono::Duration::minutes(30);

        let err = ZiplineError::OrderAfterSessionEnd {
            session_end,
            attempted_at: attempted,
        };

        let msg = err.to_string();
        assert!(msg.contains("Cannot place order after session end"));
        assert!(msg.contains("Session ended at"));
        assert!(msg.contains("order attempted at"));
    }

    // ========== Category 5: Configuration Errors ==========

    #[test]
    fn test_unsupported_frequency() {
        let err = ZiplineError::UnsupportedFrequency {
            frequency: "nanosecond".to_string(),
            supported: vec!["daily".to_string(), "minute".to_string(), "hourly".to_string()],
        };

        let msg = err.to_string();
        assert!(msg.contains("Unsupported data frequency: nanosecond"));
        assert!(msg.contains("Supported frequencies:"));
        assert!(msg.contains("daily"));
        assert!(msg.contains("minute"));
    }

    #[test]
    fn test_invalid_calendar_name() {
        let err = ZiplineError::InvalidCalendarName {
            calendar: "MARS".to_string(),
            available: vec!["NYSE".to_string(), "NASDAQ".to_string(), "LSE".to_string()],
        };

        let msg = err.to_string();
        assert!(msg.contains("Invalid trading calendar name: 'MARS'"));
        assert!(msg.contains("Available calendars:"));
        assert!(msg.contains("NYSE"));
        assert!(msg.contains("NASDAQ"));
        assert!(msg.contains("LSE"));
    }

    // ========== Category 6: Financial Errors ==========

    #[test]
    fn test_negative_portfolio_value() {
        let timestamp = Utc::now();
        let err = ZiplineError::NegativePortfolioValue {
            portfolio_value: -5000.50,
            timestamp,
        };

        let msg = err.to_string();
        assert!(msg.contains("Portfolio value became negative"));
        assert!(msg.contains("-5000.50"));
        assert!(msg.contains("critical error"));
    }

    // ========== Integration Tests ==========

    #[test]
    fn test_error_debug_formatting() {
        let err = ZiplineError::MaxLeverageExceeded {
            current_leverage: 4.2,
            max_leverage: 2.0,
        };

        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("MaxLeverageExceeded"));
        assert!(debug_str.contains("4.2"));
        assert!(debug_str.contains("2.0"));
    }

    #[test]
    fn test_error_chaining_context() {
        // Test that errors contain enough context for debugging
        let errors = vec![
            ZiplineError::MaxOrderCountExceeded {
                current_count: 100,
                max_count: 50,
                date: Utc::now(),
            },
            ZiplineError::PricingDataNotLoaded {
                assets: vec![1, 2, 3],
            },
            ZiplineError::OrderIdNotFound {
                order_id: Uuid::new_v4(),
            },
        ];

        for err in errors {
            let msg = err.to_string();
            // All P0 errors should have descriptive messages
            assert!(msg.len() > 20, "Error message too short: {}", msg);
        }
    }

    #[test]
    fn test_all_p0_errors_have_structured_fields() {
        // Ensure all P0 errors use structured fields, not just String
        let now = Utc::now();
        let order_id = Uuid::new_v4();

        // Trading Control Errors
        let _ = ZiplineError::MaxPositionSizeExceeded {
            asset: 1,
            symbol: "TEST".to_string(),
            attempted_order: 100.0,
            max_shares: Some(50.0),
            max_notional: None,
        };

        let _ = ZiplineError::MaxOrderCountExceeded {
            current_count: 10,
            max_count: 5,
            date: now,
        };

        let _ = ZiplineError::MaxOrderSizeExceeded {
            asset: 1,
            order_size: 1000.0,
            max_size: 500.0,
        };

        let _ = ZiplineError::MaxLeverageExceeded {
            current_leverage: 3.0,
            max_leverage: 2.0,
        };

        // Data Availability Errors
        let _ = ZiplineError::HistoryWindowBeforeFirstData {
            asset: 1,
            requested_start: now,
            first_available: now,
        };

        let _ = ZiplineError::AssetNonExistent {
            asset: 1,
            requested_dt: now,
            start_date: Some(now),
            end_date: Some(now),
        };

        let _ = ZiplineError::PricingDataNotLoaded { assets: vec![1] };

        let _ = ZiplineError::NoFurtherData {
            current_dt: now,
            requested_dt: now,
        };

        // Pipeline Errors
        let _ = ZiplineError::UnsupportedPipelineOutput {
            column: "test".to_string(),
            expected: "f64".to_string(),
            actual: "String".to_string(),
        };

        let _ = ZiplineError::TermNotInGraph {
            term_name: "test".to_string(),
            available_terms: vec![],
        };

        // Order Management Errors
        let _ = ZiplineError::OrderIdNotFound { order_id };

        let _ = ZiplineError::OrderAfterSessionEnd {
            session_end: now,
            attempted_at: now,
        };

        // Configuration Errors
        let _ = ZiplineError::UnsupportedFrequency {
            frequency: "test".to_string(),
            supported: vec![],
        };

        let _ = ZiplineError::InvalidCalendarName {
            calendar: "test".to_string(),
            available: vec![],
        };

        // Financial Errors
        let _ = ZiplineError::NegativePortfolioValue {
            portfolio_value: -100.0,
            timestamp: now,
        };

        // If we got here, all errors compiled successfully
        assert!(true);
    }
}
