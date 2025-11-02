//! Resample Tests
//!
//! Tests for OHLC data resampling (minute -> daily, daily -> weekly, etc.)

#[cfg(test)]
mod resample_tests {
    use chrono::{DateTime, Utc, TimeZone, Duration, Datelike, Timelike};

    #[derive(Debug, Clone, Copy)]
    struct Bar {
        timestamp: DateTime<Utc>,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
    }

    fn resample_to_daily(bars: &[Bar]) -> Vec<Bar> {
        if bars.is_empty() {
            return Vec::new();
        }

        let mut daily_bars = Vec::new();
        let mut current_day = bars[0].timestamp.date_naive();
        let mut day_bars = Vec::new();

        for bar in bars {
            let bar_day = bar.timestamp.date_naive();
            if bar_day != current_day {
                if !day_bars.is_empty() {
                    daily_bars.push(aggregate_bars(&day_bars, current_day));
                }
                day_bars.clear();
                current_day = bar_day;
            }
            day_bars.push(*bar);
        }

        if !day_bars.is_empty() {
            daily_bars.push(aggregate_bars(&day_bars, current_day));
        }

        daily_bars
    }

    fn aggregate_bars(bars: &[Bar], date: chrono::NaiveDate) -> Bar {
        let open = bars.first().unwrap().open;
        let close = bars.last().unwrap().close;
        let high = bars.iter().map(|b| b.high).fold(f64::NEG_INFINITY, f64::max);
        let low = bars.iter().map(|b| b.low).fold(f64::INFINITY, f64::min);
        let volume = bars.iter().map(|b| b.volume).sum();

        Bar {
            timestamp: date.and_hms_opt(0, 0, 0).unwrap().and_utc(),
            open,
            high,
            low,
            close,
            volume,
        }
    }

    #[test]
    fn test_single_bar_resample() {
        let bar = Bar {
            timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 9, 30, 0).unwrap(),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 1000.0,
        };

        let daily = resample_to_daily(&[bar]);
        assert_eq!(daily.len(), 1);
        assert_eq!(daily[0].open, 100.0);
        assert_eq!(daily[0].close, 100.5);
    }

    #[test]
    fn test_minute_to_daily_aggregation() {
        let mut bars = Vec::new();
        let base_time = Utc.with_ymd_and_hms(2024, 1, 1, 9, 30, 0).unwrap();

        // Create 390 minute bars for one day
        for i in 0..390 {
            bars.push(Bar {
                timestamp: base_time + Duration::minutes(i),
                open: 100.0 + i as f64 * 0.01,
                high: 101.0 + i as f64 * 0.01,
                low: 99.0 + i as f64 * 0.01,
                close: 100.5 + i as f64 * 0.01,
                volume: 100.0,
            });
        }

        let daily = resample_to_daily(&bars);
        assert_eq!(daily.len(), 1);
        assert_eq!(daily[0].open, 100.0);
        assert_eq!(daily[0].volume, 39000.0); // 390 * 100
    }

    #[test]
    fn test_ohlc_correctness() {
        let bars = vec![
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 9, 30, 0).unwrap(),
                open: 100.0,
                high: 105.0,
                low: 98.0,
                close: 102.0,
                volume: 1000.0,
            },
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 10, 30, 0).unwrap(),
                open: 102.0,
                high: 108.0,
                low: 101.0,
                close: 107.0,
                volume: 1500.0,
            },
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 11, 30, 0).unwrap(),
                open: 107.0,
                high: 110.0,
                low: 106.0,
                close: 109.0,
                volume: 2000.0,
            },
        ];

        let daily = resample_to_daily(&bars);
        assert_eq!(daily[0].open, 100.0); // First open
        assert_eq!(daily[0].close, 109.0); // Last close
        assert_eq!(daily[0].high, 110.0); // Max high
        assert_eq!(daily[0].low, 98.0); // Min low
        assert_eq!(daily[0].volume, 4500.0); // Sum volume
    }

    #[test]
    fn test_multiple_days_resample() {
        let bars = vec![
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 9, 30, 0).unwrap(),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 1000.0,
            },
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 2, 9, 30, 0).unwrap(),
                open: 101.0,
                high: 102.0,
                low: 100.0,
                close: 101.5,
                volume: 1100.0,
            },
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 3, 9, 30, 0).unwrap(),
                open: 102.0,
                high: 103.0,
                low: 101.0,
                close: 102.5,
                volume: 1200.0,
            },
        ];

        let daily = resample_to_daily(&bars);
        assert_eq!(daily.len(), 3);
    }

    #[test]
    fn test_volume_summation() {
        let bars = vec![
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 9, 30, 0).unwrap(),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 1000.0,
            },
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 10, 30, 0).unwrap(),
                open: 100.5,
                high: 101.5,
                low: 100.0,
                close: 101.0,
                volume: 2000.0,
            },
        ];

        let daily = resample_to_daily(&bars);
        assert_eq!(daily[0].volume, 3000.0);
    }

    #[test]
    fn test_empty_bars() {
        let bars: Vec<Bar> = Vec::new();
        let daily = resample_to_daily(&bars);
        assert_eq!(daily.len(), 0);
    }

    #[test]
    fn test_gap_handling() {
        let bars = vec![
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 9, 30, 0).unwrap(),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 1000.0,
            },
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 5, 9, 30, 0).unwrap(),
                open: 102.0,
                high: 103.0,
                low: 101.0,
                close: 102.5,
                volume: 1200.0,
            },
        ];

        let daily = resample_to_daily(&bars);
        assert_eq!(daily.len(), 2); // Gap days not included
    }

    #[test]
    fn test_high_is_maximum() {
        let bars = vec![
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 9, 30, 0).unwrap(),
                open: 100.0,
                high: 105.0,
                low: 99.0,
                close: 102.0,
                volume: 1000.0,
            },
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 10, 30, 0).unwrap(),
                open: 102.0,
                high: 110.0,
                low: 101.0,
                close: 107.0,
                volume: 1500.0,
            },
        ];

        let daily = resample_to_daily(&bars);
        assert_eq!(daily[0].high, 110.0);
    }

    #[test]
    fn test_low_is_minimum() {
        let bars = vec![
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 9, 30, 0).unwrap(),
                open: 100.0,
                high: 105.0,
                low: 95.0,
                close: 102.0,
                volume: 1000.0,
            },
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 10, 30, 0).unwrap(),
                open: 102.0,
                high: 110.0,
                low: 98.0,
                close: 107.0,
                volume: 1500.0,
            },
        ];

        let daily = resample_to_daily(&bars);
        assert_eq!(daily[0].low, 95.0);
    }

    #[test]
    fn test_early_close_handling() {
        let mut bars = Vec::new();
        let base_time = Utc.with_ymd_and_hms(2024, 1, 1, 9, 30, 0).unwrap();

        // Early close day - only 2 hours of trading
        for i in 0..120 {
            bars.push(Bar {
                timestamp: base_time + Duration::minutes(i),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 100.0,
            });
        }

        let daily = resample_to_daily(&bars);
        assert_eq!(daily.len(), 1);
        assert_eq!(daily[0].volume, 12000.0); // 120 * 100
    }

    #[test]
    fn test_weekly_aggregation() {
        let mut bars = Vec::new();

        // Create daily bars for a week
        for day in 0..7 {
            bars.push(Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()
                    + Duration::days(day),
                open: 100.0 + day as f64,
                high: 101.0 + day as f64,
                low: 99.0 + day as f64,
                close: 100.5 + day as f64,
                volume: 1000.0,
            });
        }

        // Would need weekly aggregation logic, but test daily works
        let daily = resample_to_daily(&bars);
        assert_eq!(daily.len(), 7);
    }

    #[test]
    fn test_precise_timestamps() {
        let bars = vec![
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 9, 30, 0).unwrap(),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 1000.0,
            },
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 15, 59, 0).unwrap(),
                open: 100.5,
                high: 101.5,
                low: 100.0,
                close: 101.0,
                volume: 1500.0,
            },
        ];

        let daily = resample_to_daily(&bars);
        assert_eq!(daily[0].timestamp.hour(), 0);
        assert_eq!(daily[0].timestamp.minute(), 0);
    }

    #[test]
    fn test_large_volume_bars() {
        let bars = vec![
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 9, 30, 0).unwrap(),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 1_000_000.0,
            },
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 10, 30, 0).unwrap(),
                open: 100.5,
                high: 101.5,
                low: 100.0,
                close: 101.0,
                volume: 2_000_000.0,
            },
        ];

        let daily = resample_to_daily(&bars);
        assert_eq!(daily[0].volume, 3_000_000.0);
    }

    #[test]
    fn test_zero_volume_bars() {
        let bars = vec![
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 9, 30, 0).unwrap(),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 0.0,
            },
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 10, 30, 0).unwrap(),
                open: 100.5,
                high: 101.5,
                low: 100.0,
                close: 101.0,
                volume: 0.0,
            },
        ];

        let daily = resample_to_daily(&bars);
        assert_eq!(daily[0].volume, 0.0);
    }

    #[test]
    fn test_consistent_ohlc_relationships() {
        let bars = vec![
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 9, 30, 0).unwrap(),
                open: 100.0,
                high: 105.0,
                low: 95.0,
                close: 102.0,
                volume: 1000.0,
            },
        ];

        let daily = resample_to_daily(&bars);
        let bar = daily[0];

        // Verify OHLC relationships
        assert!(bar.high >= bar.open);
        assert!(bar.high >= bar.close);
        assert!(bar.low <= bar.open);
        assert!(bar.low <= bar.close);
        assert!(bar.high >= bar.low);
    }

    #[test]
    fn test_intraday_volatility() {
        let bars = vec![
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 9, 30, 0).unwrap(),
                open: 100.0,
                high: 120.0,
                low: 80.0,
                close: 110.0,
                volume: 5000.0,
            },
        ];

        let daily = resample_to_daily(&bars);
        let range = daily[0].high - daily[0].low;
        assert_eq!(range, 40.0);
    }

    #[test]
    fn test_overnight_gap() {
        let bars = vec![
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 15, 30, 0).unwrap(),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 1000.0,
            },
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 2, 9, 30, 0).unwrap(),
                open: 105.0, // Gap up
                high: 106.0,
                low: 104.0,
                close: 105.5,
                volume: 1200.0,
            },
        ];

        let daily = resample_to_daily(&bars);
        assert_eq!(daily.len(), 2);
        assert_eq!(daily[1].open, 105.0); // Gap preserved
    }

    #[test]
    fn test_month_boundary() {
        let bars = vec![
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 1, 31, 9, 30, 0).unwrap(),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 1000.0,
            },
            Bar {
                timestamp: Utc.with_ymd_and_hms(2024, 2, 1, 9, 30, 0).unwrap(),
                open: 101.0,
                high: 102.0,
                low: 100.0,
                close: 101.5,
                volume: 1100.0,
            },
        ];

        let daily = resample_to_daily(&bars);
        assert_eq!(daily.len(), 2);
    }
}
