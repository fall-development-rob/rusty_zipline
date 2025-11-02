//! Cancel policies - determine when orders should be auto-cancelled
//!
//! Cancel policies define rules for automatically cancelling open orders,
//! such as at end of day (EOD) or never.

use crate::order::Order;
use chrono::{NaiveDate, DateTime, Timelike, Utc};

/// Cancel policy trait
pub trait CancelPolicy: Send + Sync {
    /// Check if an order should be cancelled at the given time
    fn should_cancel(&self, order: &Order, dt: DateTime<Utc>) -> bool;

    /// Get policy name
    fn name(&self) -> &str {
        "CancelPolicy"
    }
}

/// NeverCancel - Never automatically cancel orders
///
/// Orders remain open indefinitely until filled or manually cancelled
pub struct NeverCancel;

impl CancelPolicy for NeverCancel {
    fn should_cancel(&self, _order: &Order, _dt: DateTime<Utc>) -> bool {
        false
    }

    fn name(&self) -> &str {
        "NeverCancel"
    }
}

/// EODCancel - Cancel orders at end of day
///
/// All unfilled orders are automatically cancelled at the end of each trading day
pub struct EODCancel {
    /// Market close time (hour in UTC)
    market_close_hour: u32,
}

impl EODCancel {
    pub fn new() -> Self {
        Self {
            market_close_hour: 20, // 4:00 PM EST = 20:00 UTC (approximate)
        }
    }

    pub fn with_close_hour(market_close_hour: u32) -> Self {
        Self { market_close_hour }
    }

    /// Check if the given time is at or after market close
    fn is_after_close(&self, dt: DateTime<Utc>) -> bool {
        dt.hour() >= self.market_close_hour
    }

    /// Check if order and current time are on different days
    fn is_different_day(&self, order_time: DateTime<Utc>, current_time: DateTime<Utc>) -> bool {
        order_time.date_naive() != current_time.date_naive()
    }
}

impl Default for EODCancel {
    fn default() -> Self {
        Self::new()
    }
}

impl CancelPolicy for EODCancel {
    fn should_cancel(&self, order: &Order, dt: DateTime<Utc>) -> bool {
        // Cancel if we're after market close on the same day
        if self.is_after_close(dt) && !self.is_different_day(order.created_at, dt) {
            return true;
        }

        // Cancel if we're on a different day
        if self.is_different_day(order.created_at, dt) {
            return true;
        }

        false
    }

    fn name(&self) -> &str {
        "EODCancel"
    }
}

/// EODCancelNext - Cancel at next market open
///
/// Similar to EODCancel but orders are cancelled at the next market open
pub struct EODCancelNext {
    market_open_hour: u32,
}

impl EODCancelNext {
    pub fn new() -> Self {
        Self {
            market_open_hour: 14, // 9:30 AM EST = 14:30 UTC (approximate)
        }
    }

    pub fn with_open_hour(market_open_hour: u32) -> Self {
        Self { market_open_hour }
    }
}

impl Default for EODCancelNext {
    fn default() -> Self {
        Self::new()
    }
}

impl CancelPolicy for EODCancelNext {
    fn should_cancel(&self, order: &Order, dt: DateTime<Utc>) -> bool {
        // Cancel if we're at or past market open on a different day
        if dt.date_naive() > order.created_at.date_naive() && dt.hour() >= self.market_open_hour {
            return true;
        }

        false
    }

    fn name(&self) -> &str {
        "EODCancelNext"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::Asset;
    use crate::order::OrderSide;
    use chrono::TimeZone;

    #[test]
    fn test_never_cancel() {
        let policy = NeverCancel;
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "TEST".to_string(), "NYSE".to_string(), start_date);

        let order_time = Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap();
        let check_time = Utc.with_ymd_and_hms(2024, 1, 16, 10, 0, 0).unwrap();

        let order = Order::market(asset, OrderSide::Buy, 100.0, order_time);

        assert!(!policy.should_cancel(&order, check_time));
        assert_eq!(policy.name(), "NeverCancel");
    }

    #[test]
    fn test_eod_cancel_same_day() {
        let policy = EODCancel::with_close_hour(16);
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "TEST".to_string(), "NYSE".to_string(), start_date);

        let order_time = Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap();
        let before_close = Utc.with_ymd_and_hms(2024, 1, 15, 15, 0, 0).unwrap();
        let after_close = Utc.with_ymd_and_hms(2024, 1, 15, 17, 0, 0).unwrap();

        let order = Order::market(asset, OrderSide::Buy, 100.0, order_time);

        assert!(!policy.should_cancel(&order, before_close));
        assert!(policy.should_cancel(&order, after_close));
    }

    #[test]
    fn test_eod_cancel_next_day() {
        let policy = EODCancel::new();
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "TEST".to_string(), "NYSE".to_string(), start_date);

        let order_time = Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap();
        let next_day = Utc.with_ymd_and_hms(2024, 1, 16, 10, 0, 0).unwrap();

        let order = Order::market(asset, OrderSide::Buy, 100.0, order_time);

        assert!(policy.should_cancel(&order, next_day));
    }

    #[test]
    fn test_eod_cancel_next() {
        let policy = EODCancelNext::with_open_hour(9);
        let start_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let asset = Asset::equity(1, "TEST".to_string(), "NYSE".to_string(), start_date);

        let order_time = Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap();
        let next_day_before_open = Utc.with_ymd_and_hms(2024, 1, 16, 8, 0, 0).unwrap();
        let next_day_after_open = Utc.with_ymd_and_hms(2024, 1, 16, 10, 0, 0).unwrap();

        let order = Order::market(asset, OrderSide::Buy, 100.0, order_time);

        assert!(!policy.should_cancel(&order, next_day_before_open));
        assert!(policy.should_cancel(&order, next_day_after_open));
    }

    #[test]
    fn test_policy_names() {
        assert_eq!(NeverCancel.name(), "NeverCancel");
        assert_eq!(EODCancel::new().name(), "EODCancel");
        assert_eq!(EODCancelNext::new().name(), "EODCancelNext");
    }
}
