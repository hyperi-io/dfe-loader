//! Timestamp validation and correction
//!
//! Validates timestamps and corrects known bad values.
//!
//! ## ClickHouse DateTime64 Bounds
//!
//! ClickHouse DateTime64 has absolute limits:
//! - Minimum: 1900-01-01 00:00:00 UTC
//! - Maximum: 2299-12-31 23:59:59 UTC (precision 8), or 2262-04-11 for precision 9
//!
//! Timestamps outside this range will cause insert errors.

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

/// Minimum timestamp for ClickHouse DateTime64 (1900-01-01 00:00:00 UTC)
pub const MIN_DATETIME64_MS: i64 = -2_208_988_800_000;

/// Maximum timestamp for ClickHouse DateTime64 precision 8 (2299-12-31 23:59:59 UTC)
pub const MAX_DATETIME64_MS: i64 = 10_413_791_999_000;

/// Maximum timestamp for ClickHouse DateTime64 precision 9 (2262-04-11 23:47:16 UTC)
pub const MAX_DATETIME64_NANO_MS: i64 = 9_223_339_708_000;

/// Clamp a millisecond timestamp to ClickHouse DateTime64 bounds.
///
/// This is a cheap operation (~2ns) that prevents DateTime64 overflow errors.
#[inline]
pub fn clamp_timestamp_ms(ts_ms: i64) -> i64 {
    ts_ms.clamp(MIN_DATETIME64_MS, MAX_DATETIME64_MS)
}

/// Clamp a millisecond timestamp to DateTime64(9) nanosecond precision bounds.
#[inline]
pub fn clamp_timestamp_ms_nano(ts_ms: i64) -> i64 {
    ts_ms.clamp(MIN_DATETIME64_MS, MAX_DATETIME64_NANO_MS)
}

/// Check if a millisecond timestamp is within ClickHouse DateTime64 bounds.
#[inline]
pub fn is_valid_datetime64_ms(ts_ms: i64) -> bool {
    ts_ms >= MIN_DATETIME64_MS && ts_ms <= MAX_DATETIME64_MS
}

use crate::config::TimestampDqConfig;

/// Timestamp validation result
#[derive(Debug, Clone, PartialEq)]
pub enum TimestampResult {
    /// Valid timestamp
    Valid(DateTime<Utc>),
    /// Invalid but corrected
    Corrected(DateTime<Utc>, String),
    /// Invalid and rejected
    Invalid(String),
}

/// Timestamp validator with configurable rules
pub struct TimestampValidator {
    max_future_seconds: i64,
    max_past_seconds: i64,
    correct_known_bad: bool,
}

impl TimestampValidator {
    pub fn new(config: &TimestampDqConfig) -> Self {
        Self {
            max_future_seconds: config.max_future_seconds,
            max_past_seconds: config.max_past_seconds,
            correct_known_bad: config.correct_known_bad,
        }
    }

    /// Validate and potentially correct a timestamp string
    pub fn validate(&self, ts: &str) -> TimestampResult {
        self.validate_with_now(ts, Utc::now())
    }

    /// Validate with a pre-cached current time (avoids syscall in hot path)
    #[inline]
    pub fn validate_with_now(&self, ts: &str, now: DateTime<Utc>) -> TimestampResult {
        // Try to parse as various formats
        let parsed = self.parse_timestamp(ts);

        match parsed {
            Some(dt) => self.check_bounds_with_now(dt, now),
            None => {
                // Check for known bad formats
                if self.correct_known_bad {
                    if let Some(corrected) = self.correct_known_bad_format(ts) {
                        return TimestampResult::Corrected(
                            corrected,
                            format!("Corrected from: {}", ts),
                        );
                    }
                }
                TimestampResult::Invalid(format!("Failed to parse: {}", ts))
            }
        }
    }

    /// Validate a Unix timestamp (seconds or milliseconds)
    pub fn validate_unix(&self, ts: i64) -> TimestampResult {
        self.validate_unix_with_now(ts, Utc::now())
    }

    /// Validate Unix timestamp with a pre-cached current time (avoids syscall in hot path)
    #[inline]
    pub fn validate_unix_with_now(&self, ts: i64, now: DateTime<Utc>) -> TimestampResult {
        // Determine if seconds or milliseconds based on magnitude
        let dt = if ts > 1_000_000_000_000 {
            // Milliseconds
            Utc.timestamp_millis_opt(ts).single()
        } else {
            // Seconds
            Utc.timestamp_opt(ts, 0).single()
        };

        match dt {
            Some(dt) => self.check_bounds_with_now(dt, now),
            None => TimestampResult::Invalid(format!("Invalid Unix timestamp: {}", ts)),
        }
    }

    fn parse_timestamp(&self, ts: &str) -> Option<DateTime<Utc>> {
        // Try RFC3339/ISO8601 first (most common)
        if let Ok(dt) = DateTime::parse_from_rfc3339(ts) {
            return Some(dt.with_timezone(&Utc));
        }

        // Try common formats
        let formats = [
            "%Y-%m-%dT%H:%M:%S%.fZ",   // 2024-01-15T10:30:00.123Z
            "%Y-%m-%dT%H:%M:%SZ",      // 2024-01-15T10:30:00Z
            "%Y-%m-%d %H:%M:%S%.f",    // 2024-01-15 10:30:00.123
            "%Y-%m-%d %H:%M:%S",       // 2024-01-15 10:30:00
            "%Y-%m-%dT%H:%M:%S%.f%:z", // 2024-01-15T10:30:00.123+00:00
            "%Y-%m-%dT%H:%M:%S%:z",    // 2024-01-15T10:30:00+00:00
        ];

        for fmt in &formats {
            if let Ok(dt) = NaiveDateTime::parse_from_str(ts, fmt) {
                return Some(Utc.from_utc_datetime(&dt));
            }
        }

        None
    }

    /// Check bounds with a pre-cached current time (avoids syscall in hot path)
    #[inline]
    fn check_bounds_with_now(&self, dt: DateTime<Utc>, now: DateTime<Utc>) -> TimestampResult {
        let diff_secs = (dt - now).num_seconds();

        // Check future bound
        if self.max_future_seconds > 0 && diff_secs > self.max_future_seconds {
            return TimestampResult::Invalid(format!(
                "Timestamp {} is {} seconds in the future (max: {})",
                dt, diff_secs, self.max_future_seconds
            ));
        }

        // Check past bound
        if self.max_past_seconds > 0 && -diff_secs > self.max_past_seconds {
            return TimestampResult::Invalid(format!(
                "Timestamp {} is {} seconds in the past (max: {})",
                dt, -diff_secs, self.max_past_seconds
            ));
        }

        TimestampResult::Valid(dt)
    }

    fn correct_known_bad_format(&self, ts: &str) -> Option<DateTime<Utc>> {
        // Known bad: milliseconds as string
        if let Ok(ms) = ts.parse::<i64>() {
            if ms > 1_000_000_000_000 {
                return Utc.timestamp_millis_opt(ms).single();
            } else if ms > 1_000_000_000 {
                return Utc.timestamp_opt(ms, 0).single();
            }
        }

        // Known bad: space instead of T separator with timezone
        if ts.contains(' ') && !ts.contains('T') {
            let fixed = ts.replacen(' ', "T", 1);
            if let Ok(dt) = DateTime::parse_from_rfc3339(&fixed) {
                return Some(dt.with_timezone(&Utc));
            }
        }

        None
    }
}

impl Default for TimestampValidator {
    fn default() -> Self {
        Self {
            max_future_seconds: 600, // 10 minutes
            max_past_seconds: 0,     // No limit
            correct_known_bad: true,
        }
    }
}

/// Validate and correct timestamps (simple API)
pub fn validate_timestamp(ts: &str) -> Option<DateTime<Utc>> {
    let validator = TimestampValidator::default();
    match validator.validate(ts) {
        TimestampResult::Valid(dt) | TimestampResult::Corrected(dt, _) => Some(dt),
        TimestampResult::Invalid(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_valid_rfc3339() {
        let ts = "2024-12-24T10:30:00Z";
        let validator = TimestampValidator {
            max_future_seconds: 0,
            max_past_seconds: 0,
            correct_known_bad: true,
        };
        match validator.validate(ts) {
            TimestampResult::Valid(dt) => {
                assert_eq!(dt.year(), 2024);
                assert_eq!(dt.month(), 12);
            }
            _ => panic!("Expected valid timestamp"),
        }
    }

    #[test]
    fn test_valid_with_millis() {
        let ts = "2024-12-24T10:30:00.123Z";
        let validator = TimestampValidator::default();
        match validator.validate(ts) {
            TimestampResult::Valid(_) | TimestampResult::Corrected(_, _) => {}
            TimestampResult::Invalid(e) => panic!("Expected valid: {}", e),
        }
    }

    #[test]
    fn test_unix_milliseconds() {
        let validator = TimestampValidator::default();
        let result = validator.validate_unix(1703412600000); // Dec 24, 2024
        match result {
            TimestampResult::Valid(_) | TimestampResult::Corrected(_, _) => {}
            TimestampResult::Invalid(e) => panic!("Expected valid: {}", e),
        }
    }

    #[test]
    fn test_unix_seconds() {
        let validator = TimestampValidator::default();
        let result = validator.validate_unix(1703412600);
        match result {
            TimestampResult::Valid(_) | TimestampResult::Corrected(_, _) => {}
            TimestampResult::Invalid(e) => panic!("Expected valid: {}", e),
        }
    }

    #[test]
    fn test_future_rejection() {
        let validator = TimestampValidator {
            max_future_seconds: 60,
            max_past_seconds: 0,
            correct_known_bad: false,
        };

        // 1 hour in future
        let future = Utc::now() + chrono::Duration::hours(1);
        let ts = future.to_rfc3339();

        match validator.validate(&ts) {
            TimestampResult::Invalid(_) => {} // Expected
            _ => panic!("Expected rejection of future timestamp"),
        }
    }

    #[test]
    fn test_correct_numeric_string() {
        let validator = TimestampValidator::default();
        // Milliseconds as string
        let result = validator.validate("1703412600000");
        match result {
            TimestampResult::Corrected(_, reason) => {
                assert!(reason.contains("Corrected"));
            }
            _ => panic!("Expected correction"),
        }
    }

    #[test]
    fn test_invalid_format() {
        let result = validate_timestamp("not-a-timestamp");
        assert!(result.is_none());
    }

    #[test]
    fn test_space_separator_correction() {
        let validator = TimestampValidator::default();
        // Space instead of T (common in some systems)
        let result = validator.validate("2024-12-24 10:30:00Z");
        match result {
            TimestampResult::Valid(_) | TimestampResult::Corrected(_, _) => {}
            TimestampResult::Invalid(e) => panic!("Expected valid or corrected: {}", e),
        }
    }

    #[test]
    fn test_clamp_timestamp_within_bounds() {
        // Normal timestamp (2024-01-01) should pass through unchanged
        let ts = 1704067200000_i64;
        assert_eq!(clamp_timestamp_ms(ts), ts);
    }

    #[test]
    fn test_clamp_timestamp_too_old() {
        // Year 1800 should clamp to 1900
        let ts = -5_364_662_400_000_i64; // ~1800
        assert_eq!(clamp_timestamp_ms(ts), MIN_DATETIME64_MS);
    }

    #[test]
    fn test_clamp_timestamp_too_new() {
        // Year 2500 should clamp to 2299
        let ts = 16_725_225_600_000_i64; // ~2500
        assert_eq!(clamp_timestamp_ms(ts), MAX_DATETIME64_MS);
    }

    #[test]
    fn test_is_valid_datetime64_bounds() {
        // Valid: 2024-01-01
        assert!(is_valid_datetime64_ms(1704067200000));

        // Invalid: 1800
        assert!(!is_valid_datetime64_ms(-5_364_662_400_000));

        // Invalid: 2500
        assert!(!is_valid_datetime64_ms(16_725_225_600_000));

        // Edge: exactly at min
        assert!(is_valid_datetime64_ms(MIN_DATETIME64_MS));

        // Edge: exactly at max
        assert!(is_valid_datetime64_ms(MAX_DATETIME64_MS));
    }
}
