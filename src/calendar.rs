//! Trading calendar implementation

use crate::error::{Result, ZiplineError};
use chrono::{Datelike, Duration, NaiveDate, NaiveTime, TimeZone, Weekday};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

/// Trading session times
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SessionTimes {
    pub market_open: NaiveTime,
    pub market_close: NaiveTime,
}

/// Trading calendar trait
pub trait TradingCalendar: Send + Sync {
    /// Get the timezone for this calendar
    fn timezone(&self) -> Tz;

    /// Check if a date is a trading day
    fn is_trading_day(&self, date: NaiveDate) -> bool;

    /// Get session times for a date
    fn session_times(&self, date: NaiveDate) -> Option<SessionTimes>;

    /// Get the next trading day after the given date
    fn next_trading_day(&self, date: NaiveDate) -> Result<NaiveDate> {
        let mut current = date + Duration::days(1);
        for _ in 0..365 {
            if self.is_trading_day(current) {
                return Ok(current);
            }
            current = current + Duration::days(1);
        }
        Err(ZiplineError::CalendarError(
            "No trading day found within 365 days".to_string(),
        ))
    }

    /// Get the previous trading day before the given date
    fn previous_trading_day(&self, date: NaiveDate) -> Result<NaiveDate> {
        let mut current = date - Duration::days(1);
        for _ in 0..365 {
            if self.is_trading_day(current) {
                return Ok(current);
            }
            current = current - Duration::days(1);
        }
        Err(ZiplineError::CalendarError(
            "No trading day found within 365 days".to_string(),
        ))
    }

    /// Get all trading days between two dates (inclusive)
    fn trading_days_between(&self, start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
        let mut days = Vec::new();
        let mut current = start;

        while current <= end {
            if self.is_trading_day(current) {
                days.push(current);
            }
            current = current + Duration::days(1);
        }

        days
    }

    /// Get the number of trading days between two dates
    fn trading_days_count(&self, start: NaiveDate, end: NaiveDate) -> usize {
        self.trading_days_between(start, end).len()
    }
}

/// NYSE trading calendar
#[derive(Debug, Clone)]
pub struct NYSECalendar {
    /// List of holiday dates
    holidays: Vec<NaiveDate>,
}

impl NYSECalendar {
    /// Create a new NYSE calendar
    pub fn new() -> Self {
        Self {
            holidays: Self::default_holidays(),
        }
    }

    /// Get default US market holidays (simplified)
    fn default_holidays() -> Vec<NaiveDate> {
        // This is a simplified list - in production, use a comprehensive holiday calendar
        vec![
            // 2024 holidays
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),   // New Year's Day
            NaiveDate::from_ymd_opt(2024, 7, 4).unwrap(),   // Independence Day
            NaiveDate::from_ymd_opt(2024, 12, 25).unwrap(), // Christmas
            // 2025 holidays
            NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),   // New Year's Day
            NaiveDate::from_ymd_opt(2025, 7, 4).unwrap(),   // Independence Day
            NaiveDate::from_ymd_opt(2025, 12, 25).unwrap(), // Christmas
        ]
    }

    /// Add a custom holiday
    pub fn add_holiday(&mut self, date: NaiveDate) {
        if !self.holidays.contains(&date) {
            self.holidays.push(date);
            self.holidays.sort();
        }
    }

    /// Check if date is a weekend
    fn is_weekend(date: NaiveDate) -> bool {
        matches!(date.weekday(), Weekday::Sat | Weekday::Sun)
    }
}

impl Default for NYSECalendar {
    fn default() -> Self {
        Self::new()
    }
}

impl TradingCalendar for NYSECalendar {
    fn timezone(&self) -> Tz {
        chrono_tz::America::New_York
    }

    fn is_trading_day(&self, date: NaiveDate) -> bool {
        !Self::is_weekend(date) && !self.holidays.contains(&date)
    }

    fn session_times(&self, date: NaiveDate) -> Option<SessionTimes> {
        if self.is_trading_day(date) {
            Some(SessionTimes {
                market_open: NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
                market_close: NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nyse_calendar() {
        let calendar = NYSECalendar::new();

        // Test weekday
        let monday = NaiveDate::from_ymd_opt(2024, 1, 8).unwrap();
        assert!(calendar.is_trading_day(monday));

        // Test weekend
        let saturday = NaiveDate::from_ymd_opt(2024, 1, 6).unwrap();
        assert!(!calendar.is_trading_day(saturday));

        // Test holiday
        let new_years = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert!(!calendar.is_trading_day(new_years));
    }

    #[test]
    fn test_next_trading_day() {
        let calendar = NYSECalendar::new();
        let friday = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        let next = calendar.next_trading_day(friday).unwrap();

        // Should be Monday (skipping weekend)
        let monday = NaiveDate::from_ymd_opt(2024, 1, 8).unwrap();
        assert_eq!(next, monday);
    }

    #[test]
    fn test_trading_days_between() {
        let calendar = NYSECalendar::new();
        let start = NaiveDate::from_ymd_opt(2024, 1, 8).unwrap(); // Monday
        let end = NaiveDate::from_ymd_opt(2024, 1, 12).unwrap(); // Friday

        let days = calendar.trading_days_between(start, end);
        assert_eq!(days.len(), 5); // Mon-Fri
    }

    #[test]
    fn test_session_times() {
        let calendar = NYSECalendar::new();
        let trading_day = NaiveDate::from_ymd_opt(2024, 1, 8).unwrap();

        let times = calendar.session_times(trading_day).unwrap();
        assert_eq!(times.market_open.hour(), 9);
        assert_eq!(times.market_open.minute(), 30);
        assert_eq!(times.market_close.hour(), 16);
    }
}
