//! Scheduling and event system for time-based strategy callbacks

use chrono::{DateTime, Datelike, NaiveTime, Timelike, Utc, Weekday};
use std::fmt;

/// Trait for determining when an event should fire
pub trait EventRule: Send + Sync {
    /// Check if event should trigger at given timestamp
    fn should_trigger(&self, timestamp: DateTime<Utc>, last_trigger: Option<DateTime<Utc>>)
        -> bool;

    /// Get name of this event rule
    fn name(&self) -> &str;
}

/// Event fires every trading day
#[derive(Debug, Clone)]
pub struct EveryDay;

impl EventRule for EveryDay {
    fn should_trigger(
        &self,
        timestamp: DateTime<Utc>,
        last_trigger: Option<DateTime<Utc>>,
    ) -> bool {
        match last_trigger {
            None => true,
            Some(last) => timestamp.date_naive() != last.date_naive(),
        }
    }

    fn name(&self) -> &str {
        "EveryDay"
    }
}

/// Event fires every N days
#[derive(Debug, Clone)]
pub struct EveryNthDay {
    n: u32,
    day_count: std::sync::Arc<std::sync::Mutex<u32>>,
}

impl EveryNthDay {
    pub fn new(n: u32) -> Self {
        Self {
            n,
            day_count: std::sync::Arc::new(std::sync::Mutex::new(0)),
        }
    }
}

impl EventRule for EveryNthDay {
    fn should_trigger(
        &self,
        timestamp: DateTime<Utc>,
        last_trigger: Option<DateTime<Utc>>,
    ) -> bool {
        match last_trigger {
            None => true,
            Some(last) => {
                if timestamp.date_naive() != last.date_naive() {
                    let mut count = self.day_count.lock().unwrap();
                    *count += 1;
                    if *count >= self.n {
                        *count = 0;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        }
    }

    fn name(&self) -> &str {
        "EveryNthDay"
    }
}

/// Event fires at start of week (Monday)
#[derive(Debug, Clone)]
pub struct WeekStart;

impl EventRule for WeekStart {
    fn should_trigger(
        &self,
        timestamp: DateTime<Utc>,
        last_trigger: Option<DateTime<Utc>>,
    ) -> bool {
        let is_monday = timestamp.weekday() == Weekday::Mon;
        match last_trigger {
            None => is_monday,
            Some(last) => {
                is_monday && timestamp.date_naive() != last.date_naive()
            }
        }
    }

    fn name(&self) -> &str {
        "WeekStart"
    }
}

/// Event fires at end of week (Friday)
#[derive(Debug, Clone)]
pub struct WeekEnd;

impl EventRule for WeekEnd {
    fn should_trigger(
        &self,
        timestamp: DateTime<Utc>,
        last_trigger: Option<DateTime<Utc>>,
    ) -> bool {
        let is_friday = timestamp.weekday() == Weekday::Fri;
        match last_trigger {
            None => is_friday,
            Some(last) => {
                is_friday && timestamp.date_naive() != last.date_naive()
            }
        }
    }

    fn name(&self) -> &str {
        "WeekEnd"
    }
}

/// Event fires at start of month (first trading day)
#[derive(Debug, Clone)]
pub struct MonthStart;

impl EventRule for MonthStart {
    fn should_trigger(
        &self,
        timestamp: DateTime<Utc>,
        last_trigger: Option<DateTime<Utc>>,
    ) -> bool {
        let is_first_day = timestamp.day() == 1;
        match last_trigger {
            None => is_first_day,
            Some(last) => {
                timestamp.month() != last.month() && is_first_day
            }
        }
    }

    fn name(&self) -> &str {
        "MonthStart"
    }
}

/// Event fires at end of month (last trading day)
#[derive(Debug, Clone)]
pub struct MonthEnd;

impl EventRule for MonthEnd {
    fn should_trigger(
        &self,
        timestamp: DateTime<Utc>,
        last_trigger: Option<DateTime<Utc>>,
    ) -> bool {
        match last_trigger {
            None => false,
            Some(last) => {
                timestamp.month() != last.month()
            }
        }
    }

    fn name(&self) -> &str {
        "MonthEnd"
    }
}

/// Trait for determining time of day for event
pub trait TimeRule: Send + Sync {
    /// Get the time for this rule
    fn get_time(&self, date: DateTime<Utc>) -> NaiveTime;

    /// Get name of this time rule
    fn name(&self) -> &str;
}

/// Market open time
#[derive(Debug, Clone)]
pub struct MarketOpen {
    /// Offset in minutes from market open
    pub offset_minutes: i32,
}

impl MarketOpen {
    pub fn new() -> Self {
        Self { offset_minutes: 0 }
    }

    pub fn with_offset(offset_minutes: i32) -> Self {
        Self { offset_minutes }
    }
}

impl Default for MarketOpen {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeRule for MarketOpen {
    fn get_time(&self, _date: DateTime<Utc>) -> NaiveTime {
        // NYSE opens at 9:30 AM ET (14:30 UTC)
        let base = NaiveTime::from_hms_opt(14, 30, 0).unwrap();
        if self.offset_minutes == 0 {
            base
        } else {
            let signed_secs = self.offset_minutes as i64 * 60;
            base.signed_duration_since(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
                .checked_add(&chrono::Duration::seconds(signed_secs))
                .and_then(|d| NaiveTime::from_num_seconds_from_midnight_opt(d.num_seconds() as u32, 0))
                .unwrap_or(base)
        }
    }

    fn name(&self) -> &str {
        "MarketOpen"
    }
}

/// Market close time
#[derive(Debug, Clone)]
pub struct MarketClose {
    /// Offset in minutes from market close (negative = before close)
    pub offset_minutes: i32,
}

impl MarketClose {
    pub fn new() -> Self {
        Self { offset_minutes: 0 }
    }

    pub fn with_offset(offset_minutes: i32) -> Self {
        Self { offset_minutes }
    }
}

impl Default for MarketClose {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeRule for MarketClose {
    fn get_time(&self, _date: DateTime<Utc>) -> NaiveTime {
        // NYSE closes at 4:00 PM ET (21:00 UTC)
        let base = NaiveTime::from_hms_opt(21, 0, 0).unwrap();
        if self.offset_minutes == 0 {
            base
        } else {
            let signed_secs = self.offset_minutes as i64 * 60;
            base.signed_duration_since(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
                .checked_add(&chrono::Duration::seconds(signed_secs))
                .and_then(|d| NaiveTime::from_num_seconds_from_midnight_opt(d.num_seconds() as u32, 0))
                .unwrap_or(base)
        }
    }

    fn name(&self) -> &str {
        "MarketClose"
    }
}

/// Specific time of day
#[derive(Debug, Clone)]
pub struct SpecificTime {
    time: NaiveTime,
}

impl SpecificTime {
    pub fn new(hour: u32, minute: u32) -> Self {
        Self {
            time: NaiveTime::from_hms_opt(hour, minute, 0).unwrap(),
        }
    }
}

impl TimeRule for SpecificTime {
    fn get_time(&self, _date: DateTime<Utc>) -> NaiveTime {
        self.time
    }

    fn name(&self) -> &str {
        "SpecificTime"
    }
}

/// Function pointer type for scheduled callbacks
pub type ScheduledCallback = fn(&mut crate::algorithm::Context) -> crate::error::Result<()>;

/// Scheduled function with event and time rules
pub struct ScheduledFunction {
    /// Callback function
    callback: ScheduledCallback,
    /// Event rule (when to fire)
    event_rule: Box<dyn EventRule>,
    /// Time rule (what time to fire)
    time_rule: Box<dyn TimeRule>,
    /// Last time this function was triggered
    last_trigger: Option<DateTime<Utc>>,
    /// Function name/description
    name: String,
}

impl ScheduledFunction {
    /// Create new scheduled function
    pub fn new(
        callback: ScheduledCallback,
        event_rule: Box<dyn EventRule>,
        time_rule: Box<dyn TimeRule>,
        name: String,
    ) -> Self {
        Self {
            callback,
            event_rule,
            time_rule,
            last_trigger: None,
            name,
        }
    }

    /// Check if should trigger at given time
    pub fn should_trigger(&self, current_time: DateTime<Utc>) -> bool {
        // Check if event rule says to trigger
        if !self.event_rule.should_trigger(current_time, self.last_trigger) {
            return false;
        }

        // Check if we're at or past the target time
        let target_time = self.time_rule.get_time(current_time);
        let current_naive_time = current_time.time();

        current_naive_time >= target_time
    }

    /// Execute the callback
    pub fn execute(&mut self, context: &mut crate::algorithm::Context) -> crate::error::Result<()> {
        self.last_trigger = Some(context.timestamp);
        (self.callback)(context)
    }

    /// Get function name
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Debug for ScheduledFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScheduledFunction")
            .field("name", &self.name)
            .field("event_rule", &self.event_rule.name())
            .field("time_rule", &self.time_rule.name())
            .field("last_trigger", &self.last_trigger)
            .finish()
    }
}

/// Scheduler manages all scheduled functions
pub struct Scheduler {
    functions: Vec<ScheduledFunction>,
}

impl Scheduler {
    /// Create new scheduler
    pub fn new() -> Self {
        Self {
            functions: Vec::new(),
        }
    }

    /// Schedule a function
    pub fn schedule_function(
        &mut self,
        callback: ScheduledCallback,
        event_rule: Box<dyn EventRule>,
        time_rule: Box<dyn TimeRule>,
        name: String,
    ) {
        let func = ScheduledFunction::new(callback, event_rule, time_rule, name);
        self.functions.push(func);
    }

    /// Get functions that should trigger at current time
    pub fn get_pending(&self, current_time: DateTime<Utc>) -> Vec<usize> {
        self.functions
            .iter()
            .enumerate()
            .filter(|(_, f)| f.should_trigger(current_time))
            .map(|(i, _)| i)
            .collect()
    }

    /// Execute all pending scheduled functions
    pub fn execute_pending(
        &mut self,
        context: &mut crate::algorithm::Context,
    ) -> crate::error::Result<()> {
        let pending_indices = self.get_pending(context.timestamp);

        for idx in pending_indices {
            if let Some(func) = self.functions.get_mut(idx) {
                func.execute(context)?;
            }
        }

        Ok(())
    }

    /// Get number of scheduled functions
    pub fn len(&self) -> usize {
        self.functions.len()
    }

    /// Check if scheduler is empty
    pub fn is_empty(&self) -> bool {
        self.functions.is_empty()
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_every_day_rule() {
        let rule = EveryDay;
        let date1 = Utc.with_ymd_and_hms(2024, 1, 15, 14, 30, 0).unwrap();
        let date2 = Utc.with_ymd_and_hms(2024, 1, 16, 14, 30, 0).unwrap();
        let date3 = Utc.with_ymd_and_hms(2024, 1, 16, 18, 0, 0).unwrap();

        assert!(rule.should_trigger(date1, None));
        assert!(rule.should_trigger(date2, Some(date1)));
        assert!(!rule.should_trigger(date3, Some(date2)));
    }

    #[test]
    fn test_week_start_rule() {
        let rule = WeekStart;
        // Monday
        let monday = Utc.with_ymd_and_hms(2024, 1, 15, 14, 30, 0).unwrap();
        // Tuesday
        let tuesday = Utc.with_ymd_and_hms(2024, 1, 16, 14, 30, 0).unwrap();
        // Next Monday
        let next_monday = Utc.with_ymd_and_hms(2024, 1, 22, 14, 30, 0).unwrap();

        assert!(rule.should_trigger(monday, None));
        assert!(!rule.should_trigger(tuesday, Some(monday)));
        assert!(rule.should_trigger(next_monday, Some(monday)));
    }

    #[test]
    fn test_market_open_time() {
        let rule = MarketOpen::new();
        let date = Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap();
        let time = rule.get_time(date);

        assert_eq!(time.hour(), 14);
        assert_eq!(time.minute(), 30);
    }

    #[test]
    fn test_market_open_with_offset() {
        let rule = MarketOpen::with_offset(30); // 30 minutes after open
        let date = Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap();
        let time = rule.get_time(date);

        assert_eq!(time.hour(), 15);
        assert_eq!(time.minute(), 0);
    }

    #[test]
    fn test_market_close_time() {
        let rule = MarketClose::new();
        let date = Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap();
        let time = rule.get_time(date);

        assert_eq!(time.hour(), 21);
        assert_eq!(time.minute(), 0);
    }

    #[test]
    fn test_specific_time() {
        let rule = SpecificTime::new(12, 30);
        let date = Utc.with_ymd_and_hms(2024, 1, 15, 0, 0, 0).unwrap();
        let time = rule.get_time(date);

        assert_eq!(time.hour(), 12);
        assert_eq!(time.minute(), 30);
    }

    #[test]
    fn test_scheduler() {
        fn test_callback(_ctx: &mut crate::algorithm::Context) -> crate::error::Result<()> {
            Ok(())
        }

        let mut scheduler = Scheduler::new();
        scheduler.schedule_function(
            test_callback,
            Box::new(EveryDay),
            Box::new(MarketOpen::new()),
            "test_func".to_string(),
        );

        assert_eq!(scheduler.len(), 1);
        assert!(!scheduler.is_empty());
    }
}
