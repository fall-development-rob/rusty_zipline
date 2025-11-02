//! Data frequency resampling
//!
//! Provides resampling functionality to convert bar data from one frequency to another.
//! Supports minute-to-daily, daily-to-weekly, and daily-to-monthly conversions.

use crate::calendar::TradingCalendar;
use crate::data::bar_reader::Bar;
use crate::data::frequency::DataFrequency;
use crate::error::{Result, ZiplineError};
use chrono::{Datelike, DateTime, Duration, Utc};
use std::sync::Arc;

/// Trait for resampling bar data between frequencies
pub trait Resampler: Send + Sync {
    /// Resample data from one frequency to another
    fn resample(
        &self,
        data: &[Bar],
        from_freq: DataFrequency,
        to_freq: DataFrequency,
    ) -> Result<Vec<Bar>>;
}

/// Resampling rules and validation
#[derive(Debug, Clone, Copy)]
pub struct ResampleRules;

impl ResampleRules {
    /// Check if resampling from one frequency to another is valid
    /// Only downsampling (high freq -> low freq) is allowed
    pub fn is_valid_conversion(from: DataFrequency, to: DataFrequency) -> bool {
        from.is_higher_than(to)
    }

    /// Validate resampling request
    pub fn validate(from: DataFrequency, to: DataFrequency) -> Result<()> {
        if !Self::is_valid_conversion(from, to) {
            return Err(ZiplineError::InvalidFrequency(format!(
                "Cannot resample from {:?} to {:?}: only downsampling is supported",
                from, to
            )));
        }
        Ok(())
    }
}

/// OHLCV aggregation helper
#[derive(Debug, Default)]
struct OHLCVAggregator {
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    volume: f64,
    first_dt: Option<DateTime<Utc>>,
    last_dt: Option<DateTime<Utc>>,
}

impl OHLCVAggregator {
    /// Create a new aggregator
    fn new() -> Self {
        Self::default()
    }

    /// Add a bar to the aggregation
    fn add_bar(&mut self, bar: &Bar) {
        // Open: first bar's open
        if self.open.is_none() {
            self.open = Some(bar.open);
            self.first_dt = Some(bar.dt);
        }

        // High: maximum of all highs
        self.high = Some(self.high.map_or(bar.high, |h| h.max(bar.high)));

        // Low: minimum of all lows
        self.low = Some(self.low.map_or(bar.low, |l| l.min(bar.low)));

        // Close: last bar's close
        self.close = Some(bar.close);
        self.last_dt = Some(bar.dt);

        // Volume: sum of all volumes
        self.volume += bar.volume;
    }

    /// Build the aggregated bar
    fn build(&self, dt: DateTime<Utc>) -> Result<Bar> {
        Ok(Bar {
            open: self.open.ok_or_else(|| {
                ZiplineError::DataError("No data to aggregate".to_string())
            })?,
            high: self.high.ok_or_else(|| {
                ZiplineError::DataError("No data to aggregate".to_string())
            })?,
            low: self.low.ok_or_else(|| {
                ZiplineError::DataError("No data to aggregate".to_string())
            })?,
            close: self.close.ok_or_else(|| {
                ZiplineError::DataError("No data to aggregate".to_string())
            })?,
            volume: self.volume,
            dt,
        })
    }

    /// Check if aggregator is empty
    fn is_empty(&self) -> bool {
        self.open.is_none()
    }
}

/// Minute to daily resampler
#[derive(Clone)]
pub struct MinuteToDaily {
    calendar: Option<Arc<dyn TradingCalendar>>,
}

impl std::fmt::Debug for MinuteToDaily {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MinuteToDaily")
            .field("calendar", &if self.calendar.is_some() { "<Some(TradingCalendar)>" } else { "None" })
            .finish()
    }
}

impl MinuteToDaily {
    /// Create a new minute to daily resampler
    pub fn new() -> Self {
        Self { calendar: None }
    }

    /// Create with trading calendar for alignment
    pub fn with_calendar(calendar: Arc<dyn TradingCalendar>) -> Self {
        Self {
            calendar: Some(calendar),
        }
    }

    /// Group minute bars by trading day
    fn group_by_day(&self, bars: &[Bar]) -> Vec<(DateTime<Utc>, Vec<Bar>)> {
        let mut groups: Vec<(DateTime<Utc>, Vec<Bar>)> = Vec::new();
        let mut current_date: Option<(i32, u32, u32)> = None;
        let mut current_bars: Vec<Bar> = Vec::new();

        for bar in bars {
            let bar_date = (bar.dt.year(), bar.dt.month(), bar.dt.day());

            match current_date {
                None => {
                    current_date = Some(bar_date);
                    current_bars.push(*bar);
                }
                Some(date) if date == bar_date => {
                    current_bars.push(*bar);
                }
                Some(_) => {
                    // New day started, save previous day
                    if !current_bars.is_empty() {
                        let day_dt = current_bars[0].dt.date_naive()
                            .and_hms_opt(0, 0, 0)
                            .map(|dt| DateTime::from_naive_utc_and_offset(dt, Utc))
                            .unwrap_or(current_bars[0].dt);
                        groups.push((day_dt, current_bars.clone()));
                    }

                    // Start new day
                    current_date = Some(bar_date);
                    current_bars.clear();
                    current_bars.push(*bar);
                }
            }
        }

        // Add last day
        if !current_bars.is_empty() {
            let day_dt = current_bars[0].dt.date_naive()
                .and_hms_opt(0, 0, 0)
                .map(|dt| DateTime::from_naive_utc_and_offset(dt, Utc))
                .unwrap_or(current_bars[0].dt);
            groups.push((day_dt, current_bars));
        }

        groups
    }

    /// Aggregate minute bars into a single daily bar
    fn aggregate_bars(&self, bars: &[Bar], dt: DateTime<Utc>) -> Result<Bar> {
        if bars.is_empty() {
            return Err(ZiplineError::DataError("No bars to aggregate".to_string()));
        }

        let mut aggregator = OHLCVAggregator::new();
        for bar in bars {
            aggregator.add_bar(bar);
        }

        aggregator.build(dt)
    }
}

impl Default for MinuteToDaily {
    fn default() -> Self {
        Self::new()
    }
}

impl Resampler for MinuteToDaily {
    fn resample(
        &self,
        data: &[Bar],
        from_freq: DataFrequency,
        to_freq: DataFrequency,
    ) -> Result<Vec<Bar>> {
        // Validate conversion
        ResampleRules::validate(from_freq, to_freq)?;

        if data.is_empty() {
            return Ok(Vec::new());
        }

        // Group by day and aggregate
        let groups = self.group_by_day(data);
        let mut daily_bars = Vec::with_capacity(groups.len());

        for (dt, bars) in groups {
            let daily_bar = self.aggregate_bars(&bars, dt)?;
            daily_bars.push(daily_bar);
        }

        Ok(daily_bars)
    }
}

/// Daily to weekly resampler
#[derive(Clone)]
pub struct DailyToWeekly {
    calendar: Option<Arc<dyn TradingCalendar>>,
}

impl std::fmt::Debug for DailyToWeekly {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DailyToWeekly")
            .field("calendar", &if self.calendar.is_some() { "<Some(TradingCalendar)>" } else { "None" })
            .finish()
    }
}

impl DailyToWeekly {
    /// Create a new daily to weekly resampler
    pub fn new() -> Self {
        Self { calendar: None }
    }

    /// Create with trading calendar
    pub fn with_calendar(calendar: Arc<dyn TradingCalendar>) -> Self {
        Self {
            calendar: Some(calendar),
        }
    }

    /// Get week start date (Monday) for a given date
    fn week_start(&self, dt: DateTime<Utc>) -> DateTime<Utc> {
        let weekday = dt.weekday();
        let days_from_monday = weekday.num_days_from_monday();
        let monday = dt - Duration::days(days_from_monday as i64);
        monday.date_naive()
            .and_hms_opt(0, 0, 0)
            .map(|dt| DateTime::from_naive_utc_and_offset(dt, Utc))
            .unwrap_or(monday)
    }

    /// Group bars by week
    fn group_by_week(&self, bars: &[Bar]) -> Vec<(DateTime<Utc>, Vec<Bar>)> {
        let mut groups: Vec<(DateTime<Utc>, Vec<Bar>)> = Vec::new();
        let mut current_week: Option<DateTime<Utc>> = None;
        let mut current_bars: Vec<Bar> = Vec::new();

        for bar in bars {
            let week_start = self.week_start(bar.dt);

            match current_week {
                None => {
                    current_week = Some(week_start);
                    current_bars.push(*bar);
                }
                Some(week) if week == week_start => {
                    current_bars.push(*bar);
                }
                Some(_) => {
                    // New week started
                    if !current_bars.is_empty() {
                        let week_dt = current_week.unwrap();
                        groups.push((week_dt, current_bars.clone()));
                    }

                    current_week = Some(week_start);
                    current_bars.clear();
                    current_bars.push(*bar);
                }
            }
        }

        // Add last week
        if !current_bars.is_empty() && current_week.is_some() {
            groups.push((current_week.unwrap(), current_bars));
        }

        groups
    }
}

impl Default for DailyToWeekly {
    fn default() -> Self {
        Self::new()
    }
}

impl Resampler for DailyToWeekly {
    fn resample(
        &self,
        data: &[Bar],
        _from_freq: DataFrequency,
        _to_freq: DataFrequency,
    ) -> Result<Vec<Bar>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        let groups = self.group_by_week(data);
        let mut weekly_bars = Vec::with_capacity(groups.len());

        for (dt, bars) in groups {
            let mut aggregator = OHLCVAggregator::new();
            for bar in &bars {
                aggregator.add_bar(bar);
            }
            weekly_bars.push(aggregator.build(dt)?);
        }

        Ok(weekly_bars)
    }
}

/// Daily to monthly resampler
#[derive(Clone)]
pub struct DailyToMonthly {
    calendar: Option<Arc<dyn TradingCalendar>>,
}

impl std::fmt::Debug for DailyToMonthly {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DailyToMonthly")
            .field("calendar", &if self.calendar.is_some() { "<Some(TradingCalendar)>" } else { "None" })
            .finish()
    }
}

impl DailyToMonthly {
    /// Create a new daily to monthly resampler
    pub fn new() -> Self {
        Self { calendar: None }
    }

    /// Create with trading calendar
    pub fn with_calendar(calendar: Arc<dyn TradingCalendar>) -> Self {
        Self {
            calendar: Some(calendar),
        }
    }

    /// Get month start date
    fn month_start(&self, dt: DateTime<Utc>) -> DateTime<Utc> {
        dt.date_naive()
            .with_day(1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .map(|dt| DateTime::from_naive_utc_and_offset(dt, Utc))
            .unwrap_or(dt)
    }

    /// Group bars by month
    fn group_by_month(&self, bars: &[Bar]) -> Vec<(DateTime<Utc>, Vec<Bar>)> {
        let mut groups: Vec<(DateTime<Utc>, Vec<Bar>)> = Vec::new();
        let mut current_month: Option<(i32, u32)> = None;
        let mut current_bars: Vec<Bar> = Vec::new();

        for bar in bars {
            let month = (bar.dt.year(), bar.dt.month());

            match current_month {
                None => {
                    current_month = Some(month);
                    current_bars.push(*bar);
                }
                Some(m) if m == month => {
                    current_bars.push(*bar);
                }
                Some(_) => {
                    // New month started
                    if !current_bars.is_empty() {
                        let month_dt = self.month_start(current_bars[0].dt);
                        groups.push((month_dt, current_bars.clone()));
                    }

                    current_month = Some(month);
                    current_bars.clear();
                    current_bars.push(*bar);
                }
            }
        }

        // Add last month
        if !current_bars.is_empty() {
            let month_dt = self.month_start(current_bars[0].dt);
            groups.push((month_dt, current_bars));
        }

        groups
    }
}

impl Default for DailyToMonthly {
    fn default() -> Self {
        Self::new()
    }
}

impl Resampler for DailyToMonthly {
    fn resample(
        &self,
        data: &[Bar],
        _from_freq: DataFrequency,
        _to_freq: DataFrequency,
    ) -> Result<Vec<Bar>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        let groups = self.group_by_month(data);
        let mut monthly_bars = Vec::with_capacity(groups.len());

        for (dt, bars) in groups {
            let mut aggregator = OHLCVAggregator::new();
            for bar in &bars {
                aggregator.add_bar(bar);
            }
            monthly_bars.push(aggregator.build(dt)?);
        }

        Ok(monthly_bars)
    }
}

/// Generic resampler with flexible configuration
#[derive(Clone)]
pub struct GenericResampler {
    calendar: Option<Arc<dyn TradingCalendar>>,
}

impl std::fmt::Debug for GenericResampler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericResampler")
            .field("calendar", &if self.calendar.is_some() { "<Some(TradingCalendar)>" } else { "None" })
            .finish()
    }
}

impl GenericResampler {
    /// Create a new generic resampler
    pub fn new() -> Self {
        Self { calendar: None }
    }

    /// Create with trading calendar
    pub fn with_calendar(calendar: Arc<dyn TradingCalendar>) -> Self {
        Self {
            calendar: Some(calendar),
        }
    }

    /// Select appropriate resampler based on frequencies
    fn select_resampler(
        &self,
        from: DataFrequency,
        to: DataFrequency,
    ) -> Result<Box<dyn Resampler>> {
        match (from, to) {
            (DataFrequency::Minute, DataFrequency::Daily) => {
                if let Some(ref cal) = self.calendar {
                    Ok(Box::new(MinuteToDaily::with_calendar(cal.clone())))
                } else {
                    Ok(Box::new(MinuteToDaily::new()))
                }
            }
            (DataFrequency::Daily, DataFrequency::Daily) => {
                // No resampling needed
                Ok(Box::new(NoOpResampler))
            }
            _ => Err(ZiplineError::InvalidFrequency(format!(
                "Unsupported resampling from {:?} to {:?}",
                from, to
            ))),
        }
    }
}

impl Default for GenericResampler {
    fn default() -> Self {
        Self::new()
    }
}

impl Resampler for GenericResampler {
    fn resample(
        &self,
        data: &[Bar],
        from_freq: DataFrequency,
        to_freq: DataFrequency,
    ) -> Result<Vec<Bar>> {
        let resampler = self.select_resampler(from_freq, to_freq)?;
        resampler.resample(data, from_freq, to_freq)
    }
}

/// No-op resampler (when frequencies are the same)
#[derive(Debug, Clone)]
struct NoOpResampler;

impl Resampler for NoOpResampler {
    fn resample(
        &self,
        data: &[Bar],
        _from_freq: DataFrequency,
        _to_freq: DataFrequency,
    ) -> Result<Vec<Bar>> {
        Ok(data.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn create_test_bar(dt: DateTime<Utc>, close: f64) -> Bar {
        Bar {
            open: close - 1.0,
            high: close + 1.0,
            low: close - 2.0,
            close,
            volume: 1000.0,
            dt,
        }
    }

    #[test]
    fn test_resample_rules() {
        // Valid conversions (downsampling)
        assert!(ResampleRules::is_valid_conversion(
            DataFrequency::Minute,
            DataFrequency::Daily
        ));
        assert!(ResampleRules::is_valid_conversion(
            DataFrequency::Second,
            DataFrequency::Minute
        ));

        // Invalid conversions (upsampling)
        assert!(!ResampleRules::is_valid_conversion(
            DataFrequency::Daily,
            DataFrequency::Minute
        ));
    }

    #[test]
    fn test_ohlcv_aggregator() {
        let mut agg = OHLCVAggregator::new();

        let dt1 = Utc.with_ymd_and_hms(2025, 1, 15, 9, 30, 0).unwrap();
        let bar1 = Bar::new(100.0, 105.0, 99.0, 102.0, 1000.0, dt1);

        let dt2 = Utc.with_ymd_and_hms(2025, 1, 15, 9, 31, 0).unwrap();
        let bar2 = Bar::new(102.0, 108.0, 101.0, 107.0, 1500.0, dt2);

        agg.add_bar(&bar1);
        agg.add_bar(&bar2);

        let result = agg.build(dt1).unwrap();

        assert_eq!(result.open, 100.0); // First bar's open
        assert_eq!(result.high, 108.0); // Max of highs
        assert_eq!(result.low, 99.0);   // Min of lows
        assert_eq!(result.close, 107.0); // Last bar's close
        assert_eq!(result.volume, 2500.0); // Sum of volumes
    }

    #[test]
    fn test_minute_to_daily_empty() {
        let resampler = MinuteToDaily::new();
        let result = resampler
            .resample(&[], DataFrequency::Minute, DataFrequency::Daily)
            .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_minute_to_daily_single_day() {
        let resampler = MinuteToDaily::new();

        // Create 3 minute bars for same day
        let base_dt = Utc.with_ymd_and_hms(2025, 1, 15, 9, 30, 0).unwrap();
        let bars = vec![
            create_test_bar(base_dt, 100.0),
            create_test_bar(base_dt + Duration::minutes(1), 101.0),
            create_test_bar(base_dt + Duration::minutes(2), 102.0),
        ];

        let result = resampler
            .resample(&bars, DataFrequency::Minute, DataFrequency::Daily)
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].open, 99.0); // First bar's open (100-1)
        assert_eq!(result[0].close, 102.0); // Last bar's close
        assert_eq!(result[0].high, 103.0); // Max high (102+1)
        assert_eq!(result[0].low, 98.0); // Min low (100-2)
        assert_eq!(result[0].volume, 3000.0); // Sum volumes
    }

    #[test]
    fn test_minute_to_daily_multiple_days() {
        let resampler = MinuteToDaily::new();

        let day1 = Utc.with_ymd_and_hms(2025, 1, 15, 9, 30, 0).unwrap();
        let day2 = Utc.with_ymd_and_hms(2025, 1, 16, 9, 30, 0).unwrap();

        let bars = vec![
            create_test_bar(day1, 100.0),
            create_test_bar(day1 + Duration::minutes(1), 101.0),
            create_test_bar(day2, 105.0),
            create_test_bar(day2 + Duration::minutes(1), 106.0),
        ];

        let result = resampler
            .resample(&bars, DataFrequency::Minute, DataFrequency::Daily)
            .unwrap();

        assert_eq!(result.len(), 2);

        // Day 1
        assert_eq!(result[0].open, 99.0);
        assert_eq!(result[0].close, 101.0);

        // Day 2
        assert_eq!(result[1].open, 104.0);
        assert_eq!(result[1].close, 106.0);
    }

    #[test]
    fn test_minute_to_daily_gaps() {
        let resampler = MinuteToDaily::new();

        // Create bars with gap (missing day)
        let day1 = Utc.with_ymd_and_hms(2025, 1, 15, 9, 30, 0).unwrap();
        let day3 = Utc.with_ymd_and_hms(2025, 1, 17, 9, 30, 0).unwrap();

        let bars = vec![
            create_test_bar(day1, 100.0),
            create_test_bar(day3, 105.0),
        ];

        let result = resampler
            .resample(&bars, DataFrequency::Minute, DataFrequency::Daily)
            .unwrap();

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_daily_to_weekly() {
        let resampler = DailyToWeekly::new();

        // Create bars for one week (Mon-Fri)
        let monday = Utc.with_ymd_and_hms(2025, 1, 13, 0, 0, 0).unwrap(); // Monday
        let bars = vec![
            create_test_bar(monday, 100.0),
            create_test_bar(monday + Duration::days(1), 101.0),
            create_test_bar(monday + Duration::days(2), 102.0),
            create_test_bar(monday + Duration::days(3), 103.0),
            create_test_bar(monday + Duration::days(4), 104.0),
        ];

        let result = resampler
            .resample(&bars, DataFrequency::Daily, DataFrequency::Daily)
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].open, 99.0); // First bar's open
        assert_eq!(result[0].close, 104.0); // Last bar's close
        assert_eq!(result[0].volume, 5000.0); // Sum of volumes
    }

    #[test]
    fn test_daily_to_weekly_multiple_weeks() {
        let resampler = DailyToWeekly::new();

        let week1 = Utc.with_ymd_and_hms(2025, 1, 13, 0, 0, 0).unwrap(); // Monday
        let week2 = Utc.with_ymd_and_hms(2025, 1, 20, 0, 0, 0).unwrap(); // Next Monday

        let bars = vec![
            create_test_bar(week1, 100.0),
            create_test_bar(week1 + Duration::days(1), 101.0),
            create_test_bar(week2, 105.0),
            create_test_bar(week2 + Duration::days(1), 106.0),
        ];

        let result = resampler
            .resample(&bars, DataFrequency::Daily, DataFrequency::Daily)
            .unwrap();

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_daily_to_monthly() {
        let resampler = DailyToMonthly::new();

        let jan_1 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let bars = vec![
            create_test_bar(jan_1, 100.0),
            create_test_bar(jan_1 + Duration::days(1), 101.0),
            create_test_bar(jan_1 + Duration::days(2), 102.0),
        ];

        let result = resampler
            .resample(&bars, DataFrequency::Daily, DataFrequency::Daily)
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].open, 99.0);
        assert_eq!(result[0].close, 102.0);
        assert_eq!(result[0].volume, 3000.0);
    }

    #[test]
    fn test_daily_to_monthly_multiple_months() {
        let resampler = DailyToMonthly::new();

        let jan_1 = Utc.with_ymd_and_hms(2025, 1, 15, 0, 0, 0).unwrap();
        let feb_1 = Utc.with_ymd_and_hms(2025, 2, 15, 0, 0, 0).unwrap();

        let bars = vec![
            create_test_bar(jan_1, 100.0),
            create_test_bar(jan_1 + Duration::days(1), 101.0),
            create_test_bar(feb_1, 105.0),
            create_test_bar(feb_1 + Duration::days(1), 106.0),
        ];

        let result = resampler
            .resample(&bars, DataFrequency::Daily, DataFrequency::Daily)
            .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].close, 101.0); // Jan
        assert_eq!(result[1].close, 106.0); // Feb
    }

    #[test]
    fn test_generic_resampler() {
        let resampler = GenericResampler::new();

        let day1 = Utc.with_ymd_and_hms(2025, 1, 15, 9, 30, 0).unwrap();
        let bars = vec![
            create_test_bar(day1, 100.0),
            create_test_bar(day1 + Duration::minutes(1), 101.0),
        ];

        let result = resampler
            .resample(&bars, DataFrequency::Minute, DataFrequency::Daily)
            .unwrap();

        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_invalid_resampling() {
        // Try to upsample (should fail)
        let result = ResampleRules::validate(DataFrequency::Daily, DataFrequency::Minute);
        assert!(result.is_err());
    }

    #[test]
    fn test_partial_day() {
        // Test early close scenario
        let resampler = MinuteToDaily::new();
        let base_dt = Utc.with_ymd_and_hms(2025, 1, 15, 9, 30, 0).unwrap();

        // Only 2 hours of trading instead of full day
        let bars = vec![
            create_test_bar(base_dt, 100.0),
            create_test_bar(base_dt + Duration::minutes(60), 105.0),
            create_test_bar(base_dt + Duration::minutes(120), 103.0),
        ];

        let result = resampler
            .resample(&bars, DataFrequency::Minute, DataFrequency::Daily)
            .unwrap();

        assert_eq!(result.len(), 1);
        assert!(result[0].is_valid());
    }
}
