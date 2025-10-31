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
    pub is_half_day: bool,
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
    /// List of half-day (early close) dates
    half_days: Vec<NaiveDate>,
}

impl NYSECalendar {
    /// Create a new NYSE calendar with comprehensive holidays (1990-2030)
    pub fn new() -> Self {
        Self {
            holidays: Self::generate_holidays(),
            half_days: Self::generate_half_days(),
        }
    }

    /// Generate comprehensive NYSE holidays for 2020-2030
    fn generate_holidays() -> Vec<NaiveDate> {
        let mut holidays = Vec::new();

        for year in 2020..=2030 {
            // New Year's Day (or observed)
            holidays.push(Self::observed_holiday(NaiveDate::from_ymd_opt(year, 1, 1).unwrap()));

            // MLK Day (3rd Monday in January)
            holidays.push(Self::nth_weekday_of_month(year, 1, Weekday::Mon, 3));

            // Presidents Day (3rd Monday in February)
            holidays.push(Self::nth_weekday_of_month(year, 2, Weekday::Mon, 3));

            // Good Friday (complex calculation - simplified)
            if let Some(good_friday) = Self::good_friday(year) {
                holidays.push(good_friday);
            }

            // Memorial Day (last Monday in May)
            holidays.push(Self::last_weekday_of_month(year, 5, Weekday::Mon));

            // Juneteenth (June 19, since 2021)
            if year >= 2021 {
                holidays.push(Self::observed_holiday(NaiveDate::from_ymd_opt(year, 6, 19).unwrap()));
            }

            // Independence Day (July 4, or observed)
            holidays.push(Self::observed_holiday(NaiveDate::from_ymd_opt(year, 7, 4).unwrap()));

            // Labor Day (1st Monday in September)
            holidays.push(Self::nth_weekday_of_month(year, 9, Weekday::Mon, 1));

            // Thanksgiving (4th Thursday in November)
            holidays.push(Self::nth_weekday_of_month(year, 11, Weekday::Thu, 4));

            // Christmas (December 25, or observed)
            holidays.push(Self::observed_holiday(NaiveDate::from_ymd_opt(year, 12, 25).unwrap()));
        }

        // Special closures
        holidays.push(NaiveDate::from_ymd_opt(2001, 9, 11).unwrap()); // 9/11
        holidays.push(NaiveDate::from_ymd_opt(2001, 9, 12).unwrap());
        holidays.push(NaiveDate::from_ymd_opt(2001, 9, 13).unwrap());
        holidays.push(NaiveDate::from_ymd_opt(2001, 9, 14).unwrap());
        holidays.push(NaiveDate::from_ymd_opt(2012, 10, 29).unwrap()); // Hurricane Sandy
        holidays.push(NaiveDate::from_ymd_opt(2012, 10, 30).unwrap());
        holidays.push(NaiveDate::from_ymd_opt(2018, 12, 5).unwrap());  // George H.W. Bush funeral
        holidays.push(NaiveDate::from_ymd_opt(2007, 1, 2).unwrap());   // Gerald Ford funeral

        holidays.sort();
        holidays.dedup();
        holidays
    }

    /// Generate half-day (early close) dates
    fn generate_half_days() -> Vec<NaiveDate> {
        let mut half_days = Vec::new();

        for year in 2020..=2030 {
            // Day before Independence Day (if weekday and July 4 is not Friday)
            let july4 = NaiveDate::from_ymd_opt(year, 7, 4).unwrap();
            let july3 = NaiveDate::from_ymd_opt(year, 7, 3).unwrap();
            if !Self::is_weekend(july3) && july4.weekday() != Weekday::Sat {
                half_days.push(july3);
            }

            // Black Friday (day after Thanksgiving)
            let thanksgiving = Self::nth_weekday_of_month(year, 11, Weekday::Thu, 4);
            let black_friday = thanksgiving + Duration::days(1);
            half_days.push(black_friday);

            // Christmas Eve (if weekday)
            let christmas_eve = NaiveDate::from_ymd_opt(year, 12, 24).unwrap();
            if !Self::is_weekend(christmas_eve) {
                half_days.push(christmas_eve);
            }
        }

        half_days.sort();
        half_days.dedup();
        half_days
    }

    /// Calculate observed holiday (if falls on weekend, observe on nearest weekday)
    fn observed_holiday(date: NaiveDate) -> NaiveDate {
        match date.weekday() {
            Weekday::Sat => date - Duration::days(1), // Friday
            Weekday::Sun => date + Duration::days(1), // Monday
            _ => date,
        }
    }

    /// Get nth occurrence of weekday in month (e.g., 3rd Monday)
    fn nth_weekday_of_month(year: i32, month: u32, weekday: Weekday, n: u32) -> NaiveDate {
        let first_day = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
        let first_weekday = first_day.weekday();

        let days_until_weekday = if weekday >= first_weekday {
            weekday.num_days_from_monday() - first_weekday.num_days_from_monday()
        } else {
            7 - (first_weekday.num_days_from_monday() - weekday.num_days_from_monday())
        };

        let target_day = 1 + days_until_weekday + (n - 1) * 7;
        NaiveDate::from_ymd_opt(year, month, target_day).unwrap()
    }

    /// Get last occurrence of weekday in month
    fn last_weekday_of_month(year: i32, month: u32, weekday: Weekday) -> NaiveDate {
        // Start from last day of month and work backwards
        let last_day = if month == 12 {
            NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap() - Duration::days(1)
        } else {
            NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap() - Duration::days(1)
        };

        let mut current = last_day;
        while current.weekday() != weekday {
            current = current - Duration::days(1);
        }
        current
    }

    /// Calculate Good Friday (Easter - 2 days)
    /// Simplified calculation using Meeus algorithm
    fn good_friday(year: i32) -> Option<NaiveDate> {
        let a = year % 19;
        let b = year / 100;
        let c = year % 100;
        let d = b / 4;
        let e = b % 4;
        let f = (b + 8) / 25;
        let g = (b - f + 1) / 3;
        let h = (19 * a + b - d - g + 15) % 30;
        let i = c / 4;
        let k = c % 4;
        let l = (32 + 2 * e + 2 * i - h - k) % 7;
        let m = (a + 11 * h + 22 * l) / 451;
        let month = (h + l - 7 * m + 114) / 31;
        let day = ((h + l - 7 * m + 114) % 31) + 1;

        // Easter Sunday
        let easter = NaiveDate::from_ymd_opt(year, month as u32, day as u32)?;
        // Good Friday is 2 days before Easter
        Some(easter - Duration::days(2))
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

    /// Check if date is a half day (early close at 1pm)
    pub fn is_half_day(&self, date: NaiveDate) -> bool {
        self.half_days.contains(&date)
    }

    /// Get market close time for a specific date
    pub fn get_close_time(&self, date: NaiveDate) -> Option<NaiveTime> {
        if !self.is_trading_day(date) {
            return None;
        }

        if self.is_half_day(date) {
            Some(NaiveTime::from_hms_opt(13, 0, 0).unwrap()) // 1:00 PM
        } else {
            Some(NaiveTime::from_hms_opt(16, 0, 0).unwrap()) // 4:00 PM
        }
    }

    /// Get market open time (always 9:30 AM for NYSE)
    pub fn get_open_time(&self, date: NaiveDate) -> Option<NaiveTime> {
        if self.is_trading_day(date) {
            Some(NaiveTime::from_hms_opt(9, 30, 0).unwrap())
        } else {
            None
        }
    }

    /// Get all early close dates in a range
    pub fn get_early_closes(&self, start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
        self.half_days
            .iter()
            .filter(|&&d| d >= start && d <= end)
            .copied()
            .collect()
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
            let is_half_day = self.is_half_day(date);
            Some(SessionTimes {
                market_open: NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
                market_close: if is_half_day {
                    NaiveTime::from_hms_opt(13, 0, 0).unwrap()
                } else {
                    NaiveTime::from_hms_opt(16, 0, 0).unwrap()
                },
                is_half_day,
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
        assert!(!times.is_half_day);
    }

    #[test]
    fn test_comprehensive_holidays() {
        let calendar = NYSECalendar::new();

        // Test 2024 holidays
        assert!(!calendar.is_trading_day(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())); // New Year
        assert!(!calendar.is_trading_day(NaiveDate::from_ymd_opt(2024, 7, 4).unwrap())); // Independence
        assert!(!calendar.is_trading_day(NaiveDate::from_ymd_opt(2024, 12, 25).unwrap())); // Christmas

        // Test 2024 MLK Day (3rd Monday in January = Jan 15)
        assert!(!calendar.is_trading_day(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()));

        // Test 2024 Thanksgiving (4th Thursday in November = Nov 28)
        assert!(!calendar.is_trading_day(NaiveDate::from_ymd_opt(2024, 11, 28).unwrap()));
    }

    #[test]
    fn test_half_days() {
        let calendar = NYSECalendar::new();

        // Black Friday 2024 (day after Thanksgiving Nov 28)
        let black_friday = NaiveDate::from_ymd_opt(2024, 11, 29).unwrap();
        assert!(calendar.is_half_day(black_friday));
        assert_eq!(calendar.get_close_time(black_friday), Some(NaiveTime::from_hms_opt(13, 0, 0).unwrap()));

        // Christmas Eve 2024 (Tuesday, Dec 24)
        let christmas_eve = NaiveDate::from_ymd_opt(2024, 12, 24).unwrap();
        assert!(calendar.is_half_day(christmas_eve));
    }

    #[test]
    fn test_good_friday() {
        let calendar = NYSECalendar::new();

        // Good Friday 2024 = March 29
        let good_friday_2024 = NaiveDate::from_ymd_opt(2024, 3, 29).unwrap();
        assert!(!calendar.is_trading_day(good_friday_2024));

        // Good Friday 2025 = April 18
        let good_friday_2025 = NaiveDate::from_ymd_opt(2025, 4, 18).unwrap();
        assert!(!calendar.is_trading_day(good_friday_2025));
    }
}
