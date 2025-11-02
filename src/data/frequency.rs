//! Data frequency types and conversion utilities

use crate::error::{Result, ZiplineError};
use chrono::{DateTime, Duration, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Data frequency enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataFrequency {
    /// Daily bar data (one bar per trading day)
    Daily,
    /// Minute bar data (one bar per minute)
    Minute,
    /// Second bar data (one bar per second)
    Second,
}

impl DataFrequency {
    /// Get the duration represented by this frequency
    pub fn duration(&self) -> Duration {
        match self {
            DataFrequency::Daily => Duration::days(1),
            DataFrequency::Minute => Duration::minutes(1),
            DataFrequency::Second => Duration::seconds(1),
        }
    }

    /// Get the number of bars per day for this frequency
    pub fn bars_per_day(&self) -> usize {
        match self {
            DataFrequency::Daily => 1,
            DataFrequency::Minute => 390, // 6.5 hours * 60 minutes (9:30 AM - 4:00 PM)
            DataFrequency::Second => 23400, // 390 minutes * 60 seconds
        }
    }

    /// Check if this frequency is higher (more frequent) than another
    pub fn is_higher_than(&self, other: DataFrequency) -> bool {
        self.bars_per_day() > other.bars_per_day()
    }

    /// Get the next timestamp for this frequency
    pub fn next_timestamp(&self, current: DateTime<Utc>) -> DateTime<Utc> {
        current + self.duration()
    }

    /// Convert to string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            DataFrequency::Daily => "daily",
            DataFrequency::Minute => "minute",
            DataFrequency::Second => "second",
        }
    }

    /// Parse from string
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "daily" | "d" | "1d" => Ok(DataFrequency::Daily),
            "minute" | "min" | "1min" => Ok(DataFrequency::Minute),
            "second" | "sec" | "1s" => Ok(DataFrequency::Second),
            _ => Err(ZiplineError::InvalidFrequency(s.to_string())),
        }
    }
}

impl fmt::Display for DataFrequency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Convert data from one frequency to another
pub struct FrequencyConverter;

impl FrequencyConverter {
    /// Align timestamp to frequency boundary
    pub fn align_timestamp(timestamp: DateTime<Utc>, frequency: DataFrequency) -> DateTime<Utc> {
        match frequency {
            DataFrequency::Daily => {
                // Align to start of day (midnight UTC)
                timestamp.date_naive().and_hms_opt(0, 0, 0)
                    .map(|dt| DateTime::from_naive_utc_and_offset(dt, Utc))
                    .unwrap_or(timestamp)
            }
            DataFrequency::Minute => {
                // Align to start of minute
                timestamp.date_naive()
                    .and_hms_opt(timestamp.hour(), timestamp.minute(), 0)
                    .map(|dt| DateTime::from_naive_utc_and_offset(dt, Utc))
                    .unwrap_or(timestamp)
            }
            DataFrequency::Second => {
                // Already at second precision
                timestamp
            }
        }
    }

    /// Check if timestamp is aligned to frequency
    pub fn is_aligned(timestamp: DateTime<Utc>, frequency: DataFrequency) -> bool {
        timestamp == Self::align_timestamp(timestamp, frequency)
    }

    /// Calculate number of bars between two timestamps
    pub fn bars_between(
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        frequency: DataFrequency,
    ) -> usize {
        let duration = end.signed_duration_since(start);
        match frequency {
            DataFrequency::Daily => duration.num_days().max(0) as usize,
            DataFrequency::Minute => duration.num_minutes().max(0) as usize,
            DataFrequency::Second => duration.num_seconds().max(0) as usize,
        }
    }

    /// Generate timestamps at given frequency between start and end
    pub fn generate_timestamps(
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        frequency: DataFrequency,
    ) -> Vec<DateTime<Utc>> {
        let mut timestamps = Vec::new();
        let mut current = Self::align_timestamp(start, frequency);

        while current <= end {
            timestamps.push(current);
            current = frequency.next_timestamp(current);
        }

        timestamps
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_frequency_duration() {
        assert_eq!(DataFrequency::Daily.duration(), Duration::days(1));
        assert_eq!(DataFrequency::Minute.duration(), Duration::minutes(1));
        assert_eq!(DataFrequency::Second.duration(), Duration::seconds(1));
    }

    #[test]
    fn test_bars_per_day() {
        assert_eq!(DataFrequency::Daily.bars_per_day(), 1);
        assert_eq!(DataFrequency::Minute.bars_per_day(), 390);
        assert_eq!(DataFrequency::Second.bars_per_day(), 23400);
    }

    #[test]
    fn test_is_higher_than() {
        assert!(DataFrequency::Minute.is_higher_than(DataFrequency::Daily));
        assert!(DataFrequency::Second.is_higher_than(DataFrequency::Minute));
        assert!(DataFrequency::Second.is_higher_than(DataFrequency::Daily));
        assert!(!DataFrequency::Daily.is_higher_than(DataFrequency::Minute));
    }

    #[test]
    fn test_frequency_from_str() {
        assert_eq!(DataFrequency::from_str("daily").unwrap(), DataFrequency::Daily);
        assert_eq!(DataFrequency::from_str("minute").unwrap(), DataFrequency::Minute);
        assert_eq!(DataFrequency::from_str("second").unwrap(), DataFrequency::Second);
        assert_eq!(DataFrequency::from_str("1d").unwrap(), DataFrequency::Daily);
        assert_eq!(DataFrequency::from_str("1min").unwrap(), DataFrequency::Minute);
        assert!(DataFrequency::from_str("invalid").is_err());
    }

    #[test]
    fn test_align_timestamp() {
        let timestamp = Utc.with_ymd_and_hms(2025, 1, 15, 10, 35, 42).unwrap();

        let daily_aligned = FrequencyConverter::align_timestamp(timestamp, DataFrequency::Daily);
        assert_eq!(daily_aligned.hour(), 0);
        assert_eq!(daily_aligned.minute(), 0);
        assert_eq!(daily_aligned.second(), 0);

        let minute_aligned = FrequencyConverter::align_timestamp(timestamp, DataFrequency::Minute);
        assert_eq!(minute_aligned.hour(), 10);
        assert_eq!(minute_aligned.minute(), 35);
        assert_eq!(minute_aligned.second(), 0);

        let second_aligned = FrequencyConverter::align_timestamp(timestamp, DataFrequency::Second);
        assert_eq!(second_aligned, timestamp);
    }

    #[test]
    fn test_is_aligned() {
        let aligned = Utc.with_ymd_and_hms(2025, 1, 15, 10, 35, 0).unwrap();
        let not_aligned = Utc.with_ymd_and_hms(2025, 1, 15, 10, 35, 42).unwrap();

        assert!(FrequencyConverter::is_aligned(aligned, DataFrequency::Minute));
        assert!(!FrequencyConverter::is_aligned(not_aligned, DataFrequency::Minute));
    }

    #[test]
    fn test_bars_between() {
        let start = Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2025, 1, 15, 11, 30, 0).unwrap();

        assert_eq!(
            FrequencyConverter::bars_between(start, end, DataFrequency::Minute),
            90
        );

        let start_day = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let end_day = Utc.with_ymd_and_hms(2025, 1, 5, 0, 0, 0).unwrap();

        assert_eq!(
            FrequencyConverter::bars_between(start_day, end_day, DataFrequency::Daily),
            4
        );
    }

    #[test]
    fn test_generate_timestamps() {
        let start = Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2025, 1, 15, 10, 5, 0).unwrap();

        let timestamps = FrequencyConverter::generate_timestamps(start, end, DataFrequency::Minute);

        assert_eq!(timestamps.len(), 6); // 10:00, 10:01, 10:02, 10:03, 10:04, 10:05
        assert_eq!(timestamps[0], start);
        assert_eq!(timestamps[5], end);
    }

    #[test]
    fn test_next_timestamp() {
        let current = Utc.with_ymd_and_hms(2025, 1, 15, 10, 35, 0).unwrap();

        let next_minute = DataFrequency::Minute.next_timestamp(current);
        assert_eq!(next_minute, Utc.with_ymd_and_hms(2025, 1, 15, 10, 36, 0).unwrap());

        let next_day = DataFrequency::Daily.next_timestamp(current);
        assert_eq!(next_day, Utc.with_ymd_and_hms(2025, 1, 16, 10, 35, 0).unwrap());
    }
}
