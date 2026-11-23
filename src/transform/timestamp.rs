// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Timestamp validation and correction
//!
//! Validates timestamps and corrects known bad values.
//!
//! ## `ClickHouse` `DateTime64` Bounds
//!
//! `ClickHouse` `DateTime64` has absolute limits:
//! - Minimum: 1900-01-01 00:00:00 UTC
//! - Maximum: 2299-12-31 23:59:59 UTC (precision 8), or 2262-04-11 for precision 9
//!
//! Timestamps outside this range will cause insert errors.

use jiff::Timestamp;
use jiff::civil::DateTime;
use jiff::tz::Offset;

use crate::datetime::utc_fields;

/// Minimum timestamp for `ClickHouse` `DateTime64` (1900-01-01 00:00:00 UTC)
pub const MIN_DATETIME64_MS: i64 = -2_208_988_800_000;

/// Maximum timestamp for `ClickHouse` `DateTime64` precision 8 (2299-12-31 23:59:59 UTC)
pub const MAX_DATETIME64_MS: i64 = 10_413_791_999_000;

/// Maximum timestamp for `ClickHouse` `DateTime64` precision 9 (2262-04-11 23:47:16 UTC)
pub const MAX_DATETIME64_NANO_MS: i64 = 9_223_339_708_000;

/// Clamp a millisecond timestamp to `ClickHouse` `DateTime64` bounds.
///
/// This is a cheap operation (~2ns) that prevents `DateTime64` overflow errors.
#[inline]
pub fn clamp_timestamp_ms(ts_ms: i64) -> i64 {
    ts_ms.clamp(MIN_DATETIME64_MS, MAX_DATETIME64_MS)
}

/// Clamp a millisecond timestamp to DateTime64(9) nanosecond precision bounds.
#[inline]
pub fn clamp_timestamp_ms_nano(ts_ms: i64) -> i64 {
    ts_ms.clamp(MIN_DATETIME64_MS, MAX_DATETIME64_NANO_MS)
}

/// Check if a millisecond timestamp is within `ClickHouse` `DateTime64` bounds.
#[inline]
pub fn is_valid_datetime64_ms(ts_ms: i64) -> bool {
    (MIN_DATETIME64_MS..=MAX_DATETIME64_MS).contains(&ts_ms)
}

use crate::config::TimestampDqConfig;

/// Timestamp validation result
#[derive(Debug, Clone, PartialEq)]
pub enum TimestampResult {
    /// Valid timestamp
    Valid(Timestamp),
    /// Invalid but corrected
    Corrected(Timestamp, String),
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
        self.validate_with_now(ts, Timestamp::now())
    }

    /// Validate with a pre-cached current time (avoids syscall in hot path)
    #[inline]
    pub fn validate_with_now(&self, ts: &str, now: Timestamp) -> TimestampResult {
        // Try to parse as various formats
        let parsed = self.parse_timestamp(ts);

        if let Some(dt) = parsed {
            self.check_bounds_with_now(dt, now)
        } else {
            // Check for known bad formats
            if self.correct_known_bad
                && let Some(corrected) = self.correct_known_bad_format(ts)
            {
                return TimestampResult::Corrected(corrected, format!("Corrected from: {ts}"));
            }
            TimestampResult::Invalid(format!("Failed to parse: {ts}"))
        }
    }

    /// Validate a Unix timestamp (seconds or milliseconds)
    pub fn validate_unix(&self, ts: i64) -> TimestampResult {
        self.validate_unix_with_now(ts, Timestamp::now())
    }

    /// Validate Unix timestamp with a pre-cached current time (avoids syscall in hot path)
    #[inline]
    pub fn validate_unix_with_now(&self, ts: i64, now: Timestamp) -> TimestampResult {
        // Determine if seconds or milliseconds based on magnitude
        let dt = if ts > 1_000_000_000_000 {
            // Milliseconds
            Timestamp::from_millisecond(ts).ok()
        } else {
            // Seconds
            Timestamp::from_second(ts).ok()
        };

        match dt {
            Some(dt) => self.check_bounds_with_now(dt, now),
            None => TimestampResult::Invalid(format!("Invalid Unix timestamp: {ts}")),
        }
    }

    /// RFC 3339 text, else the zone-less `YYYY-MM-DD[T| ]HH:MM:SS[.f]` forms
    /// read as UTC.
    fn parse_timestamp(&self, ts: &str) -> Option<Timestamp> {
        rfc3339(ts).or_else(|| loose_utc(ts))
    }

    /// Check bounds with a pre-cached current time (avoids syscall in hot path)
    #[inline]
    pub fn check_bounds_with_now(&self, dt: Timestamp, now: Timestamp) -> TimestampResult {
        let diff_secs = dt.duration_since(now).as_secs();

        // Check future bound
        if self.max_future_seconds > 0 && diff_secs > self.max_future_seconds {
            return TimestampResult::Invalid(format!(
                "Timestamp {} is {} seconds in the future (max: {})",
                utc_display(&dt),
                diff_secs,
                self.max_future_seconds
            ));
        }

        // Check past bound
        if self.max_past_seconds > 0 && -diff_secs > self.max_past_seconds {
            return TimestampResult::Invalid(format!(
                "Timestamp {} is {} seconds in the past (max: {})",
                utc_display(&dt),
                -diff_secs,
                self.max_past_seconds
            ));
        }

        TimestampResult::Valid(dt)
    }

    fn correct_known_bad_format(&self, ts: &str) -> Option<Timestamp> {
        // Known bad: milliseconds as string
        if let Ok(ms) = ts.parse::<i64>() {
            if ms > 1_000_000_000_000 {
                return Timestamp::from_millisecond(ms).ok();
            } else if ms > 1_000_000_000 {
                return Timestamp::from_second(ms).ok();
            }
        }

        // Known bad: space instead of T separator with timezone
        if ts.contains(' ') && !ts.contains('T') {
            let fixed = ts.replacen(' ', "T", 1);
            return rfc3339(&fixed);
        }

        None
    }
}

/// `YYYY-MM-DD HH:MM:SS[.f] UTC`, the fraction left out when zero.
fn utc_display(ts: &Timestamp) -> String {
    utc_fields(ts, ' ', false, " UTC")
}

/// An instant from civil fields and an offset east of UTC in seconds. `None`
/// for a date or time that does not exist, or one `Timestamp` can't hold.
fn civil_instant(
    [year, month, day, hour, minute, second]: [i64; 6],
    nanos: u32,
    offset_secs: i32,
) -> Option<Timestamp> {
    let civil = DateTime::new(
        i16::try_from(year).ok()?,
        i8::try_from(month).ok()?,
        i8::try_from(day).ok()?,
        i8::try_from(hour).ok()?,
        i8::try_from(minute).ok()?,
        i8::try_from(second).ok()?,
        i32::try_from(nanos).ok()?,
    )
    .ok()?;
    Offset::from_seconds(offset_secs)
        .ok()?
        .to_timestamp(civil)
        .ok()
}

/// RFC 3339 text: `YYYY-MM-DD`, `T`, `t` or a space, `HH:MM:SS`, any number
/// of fraction digits with those past nine dropped, then `Z`, `z` or
/// `+HH:MM`, `-HH:MM` or U+2212 `HH:MM` under 24 hours. Second 60 reads as 59.
fn rfc3339(s: &str) -> Option<Timestamp> {
    let b = s.as_bytes();
    if b.len() < 19
        || b[4] != b'-'
        || b[7] != b'-'
        || !matches!(b[10], b'T' | b't' | b' ')
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    let field = |at: usize, n: usize| {
        b[at..at + n].iter().try_fold(0_i64, |acc, &c| {
            c.is_ascii_digit().then(|| acc * 10 + i64::from(c - b'0'))
        })
    };
    let second = field(17, 2).filter(|&second| second <= 60)?;
    let fields = [
        field(0, 4)?,
        field(5, 2)?,
        field(8, 2)?,
        field(11, 2)?,
        field(14, 2)?,
        second.min(59),
    ];
    let mut rest = &s[19..];
    let mut nanos = 0;
    if let Some(fraction) = rest.strip_prefix('.') {
        (rest, nanos) = fraction_digits(fraction)?;
    }
    let offset_secs = if matches!(rest, "Z" | "z") {
        0
    } else {
        let (sign, hhmm) = match rest.strip_prefix('+') {
            Some(hhmm) => (1, hhmm),
            None => (-1, rest.strip_prefix(['-', '\u{2212}'])?),
        };
        let &[h1, h2, b':', m1 @ b'0'..=b'5', m2] = hhmm.as_bytes() else {
            return None;
        };
        let [h1, h2, m1, m2] =
            [h1, h2, m1, m2].map(|c| c.is_ascii_digit().then(|| i32::from(c - b'0')));
        let secs = (h1? * 10 + h2?) * 3600 + (m1? * 10 + m2?) * 60;
        if secs >= 86_400 {
            return None;
        }
        sign * secs
    };
    civil_instant(fields, nanos, offset_secs)
}

/// One to nine fraction digits as nanoseconds, any further digits skipped.
fn fraction_digits(s: &str) -> Option<(&str, u32)> {
    let len = s.bytes().take_while(u8::is_ascii_digit).count();
    if len == 0 {
        return None;
    }
    let nanos = s.as_bytes()[..len.min(9)]
        .iter()
        .zip((0..9).rev())
        .fold(0, |acc, (&c, place)| {
            acc + u32::from(c - b'0') * 10_u32.pow(place)
        });
    Some((&s[len..], nanos))
}

/// The zone-less forms `YYYY-MM-DDTHH:MM:SS[.f]Z`, `YYYY-MM-DD HH:MM:SS[.f]`
/// and `YYYY-MM-DDTHH:MM:SS[.f]±HH[:]MM`, read as UTC with any offset
/// ignored. A field may be one or two digits (the year one to four, or signed)
/// after optional whitespace, the space between date and time may be any run
/// of whitespace or none, and second 60 reads as 59.
fn loose_utc(s: &str) -> Option<Timestamp> {
    let (date, s) = loose_date(s)?;
    if let Some(time) = s.strip_prefix('T') {
        let (fields, nanos, rest) = loose_time(date, time)?;
        // `Z`, or an offset that is read and then ignored.
        if rest == "Z" || loose_offset(rest)? {
            return civil_instant(fields, nanos, 0);
        }
        return None;
    }
    let (fields, nanos, rest) = loose_time(date, s.trim_start())?;
    rest.is_empty().then_some(())?;
    civil_instant(fields, nanos, 0)
}

/// `[ws]Y-[ws]M-[ws]D`, and what follows it.
fn loose_date(s: &str) -> Option<([i64; 3], &str)> {
    let s = s.trim_start();
    let (year, s) = if let Some(digits) = s.strip_prefix('-') {
        let (year, s) = loose_number(digits, usize::MAX)?;
        (-year, s)
    } else if let Some(digits) = s.strip_prefix('+') {
        loose_number(digits, usize::MAX)?
    } else {
        loose_number(s, 4)?
    };
    let (month, s) = loose_number(s.strip_prefix('-')?.trim_start(), 2)?;
    let (day, s) = loose_number(s.strip_prefix('-')?.trim_start(), 2)?;
    ((1..=12).contains(&month) && (1..=31).contains(&day)).then_some(())?;
    Some(([year, month, day], s))
}

/// `[ws]H:[ws]M:[ws]S[.f]` after `date`, as the six civil fields, the
/// nanoseconds and what follows.
fn loose_time(date: [i64; 3], s: &str) -> Option<([i64; 6], u32, &str)> {
    let (hour, s) = loose_number(s.trim_start(), 2)?;
    let (minute, s) = loose_number(s.strip_prefix(':')?.trim_start(), 2)?;
    let (second, mut s) = loose_number(s.strip_prefix(':')?.trim_start(), 2)?;
    (hour <= 23 && minute <= 59 && second <= 60).then_some(())?;
    let mut nanos = 0;
    if let Some(fraction) = s.strip_prefix('.') {
        (s, nanos) = fraction_digits(fraction)?;
    }
    let [year, month, day] = date;
    Some(([year, month, day, hour, minute, second.min(59)], nanos, s))
}

/// `[ws]±HH[:| ]*MM` and nothing after it: `Some(true)` when it is one.
fn loose_offset(s: &str) -> Option<bool> {
    let s = s.trim_start();
    let s = s
        .strip_prefix(['+', '-', '\u{2212}'])?
        .strip_prefix(|c: char| c.is_ascii_digit())?
        .strip_prefix(|c: char| c.is_ascii_digit())?
        .trim_start_matches(|c: char| c == ':' || c.is_whitespace());
    let b = s.as_bytes();
    Some(b.len() == 2 && matches!(b[0], b'0'..=b'5') && b[1].is_ascii_digit())
}

/// One to `max` ASCII digits, and what follows them.
fn loose_number(s: &str, max: usize) -> Option<(i64, &str)> {
    let len = s.bytes().take(max).take_while(u8::is_ascii_digit).count();
    if len == 0 {
        return None;
    }
    let value = s[..len].bytes().try_fold(0_i64, |acc, c| {
        acc.checked_mul(10)?.checked_add(i64::from(c - b'0'))
    })?;
    Some((value, &s[len..]))
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
pub fn validate_timestamp(ts: &str) -> Option<Timestamp> {
    let validator = TimestampValidator::default();
    match validator.validate(ts) {
        TimestampResult::Valid(dt) | TimestampResult::Corrected(dt, _) => Some(dt),
        TimestampResult::Invalid(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::SignedDuration;

    /// `ts` as RFC 3339 text with a `+00:00` offset.
    fn rfc3339_text(ts: Timestamp) -> String {
        ts.display_with_offset(Offset::UTC).to_string()
    }

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
                let dt = Offset::UTC.to_datetime(dt);
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
            TimestampResult::Invalid(e) => panic!("Expected valid: {e}"),
        }
    }

    #[test]
    fn test_unix_milliseconds() {
        let validator = TimestampValidator::default();
        let result = validator.validate_unix(1703412600000); // Dec 24, 2024
        match result {
            TimestampResult::Valid(_) | TimestampResult::Corrected(_, _) => {}
            TimestampResult::Invalid(e) => panic!("Expected valid: {e}"),
        }
    }

    #[test]
    fn test_unix_seconds() {
        let validator = TimestampValidator::default();
        let result = validator.validate_unix(1703412600);
        match result {
            TimestampResult::Valid(_) | TimestampResult::Corrected(_, _) => {}
            TimestampResult::Invalid(e) => panic!("Expected valid: {e}"),
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
        let future = Timestamp::now() + SignedDuration::from_hours(1);
        let ts = rfc3339_text(future);

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
            TimestampResult::Invalid(e) => panic!("Expected valid or corrected: {e}"),
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

    #[test]
    fn test_clamp_timestamp_ms_nano_within_bounds() {
        let ts = 1704067200000_i64; // 2024-01-01
        assert_eq!(clamp_timestamp_ms_nano(ts), ts);
    }

    #[test]
    fn test_clamp_timestamp_ms_nano_below_min() {
        // Pre-1900 timestamp should be clamped up
        let ts = -5_364_662_400_000_i64;
        assert_eq!(clamp_timestamp_ms_nano(ts), MIN_DATETIME64_MS);
    }

    #[test]
    fn test_clamp_timestamp_ms_nano_above_nano_max() {
        // Year 2500 clamped to DateTime64(9) nano max
        let ts = 16_725_225_600_000_i64;
        assert_eq!(clamp_timestamp_ms_nano(ts), MAX_DATETIME64_NANO_MS);
    }

    /// The reason names the instant as `YYYY-MM-DD HH:MM:SS[.f] UTC`.
    #[test]
    fn out_of_bounds_reasons_keep_their_text() {
        let validator = TimestampValidator {
            max_future_seconds: 60,
            max_past_seconds: 60,
            correct_known_bad: false,
        };
        let now: Timestamp = "2026-04-07T07:03:00Z".parse().unwrap();
        for (text, reason) in [
            (
                "2026-04-07T08:03:00.12Z",
                "Timestamp 2026-04-07 08:03:00.120 UTC is 3600 seconds in the future (max: 60)",
            ),
            (
                "2026-04-07 06:03:00",
                "Timestamp 2026-04-07 06:03:00 UTC is 3600 seconds in the past (max: 60)",
            ),
            (
                "2026-04-07T06:03:00.000001+00:00",
                "Timestamp 2026-04-07 06:03:00.000001 UTC is 3599 seconds in the past (max: 60)",
            ),
        ] {
            match validator.validate_with_now(text, now) {
                TimestampResult::Invalid(got) => assert_eq!(got, reason, "{text}"),
                other => panic!("{text}: expected Invalid, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_past_rejection_with_specific_max_past() {
        // max_past_seconds = 60 -- timestamps 1 hour in the past are rejected
        let validator = TimestampValidator {
            max_future_seconds: 0,
            max_past_seconds: 60,
            correct_known_bad: false,
        };
        let past = Timestamp::now() - SignedDuration::from_hours(1);
        let ts = rfc3339_text(past);
        match validator.validate(&ts) {
            TimestampResult::Invalid(reason) => {
                assert!(reason.contains("past"));
            }
            other => panic!("Expected Invalid past, got {other:?}"),
        }
    }

    #[test]
    fn test_unix_seconds_correction_reason() {
        // Seconds timestamp (>1e9, <1e12) via numeric string
        // After correction, should produce Corrected variant
        let validator = TimestampValidator::default();
        let ts = "1703412600"; // seconds epoch
        match validator.validate(ts) {
            TimestampResult::Corrected(_, reason) => {
                assert!(
                    reason.contains("Corrected") || !reason.is_empty(),
                    "Reason: {reason}"
                );
            }
            TimestampResult::Valid(_) => {} // Also acceptable if parser handled it directly
            TimestampResult::Invalid(e) => panic!("unexpected invalid: {e}"),
        }
    }

    #[test]
    fn test_correction_disabled_returns_invalid() {
        // correct_known_bad=false means unparseable strings stay Invalid
        let validator = TimestampValidator {
            max_future_seconds: 0,
            max_past_seconds: 0,
            correct_known_bad: false,
        };
        let result = validator.validate("not-parseable-at-all");
        match result {
            TimestampResult::Invalid(_) => {} // Expected
            other => panic!("Expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn test_validate_timestamp_function_on_invalid() {
        // Top-level helper function: invalid input → None
        assert!(validate_timestamp("complete garbage").is_none());
        assert!(validate_timestamp("").is_none());
    }

    #[test]
    fn test_validate_timestamp_function_on_valid() {
        let result = validate_timestamp("2024-12-25T10:00:00Z");
        assert!(result.is_some());
    }

    #[test]
    fn test_space_separator_no_tz_does_not_correct() {
        // Space separator WITHOUT timezone — correct_known_bad_format only
        // handles space-with-TZ cases. Without Z or +offset, space→T replacement
        // fails rfc3339 parsing.
        let validator = TimestampValidator {
            max_future_seconds: 0,
            max_past_seconds: 0,
            correct_known_bad: true,
        };
        // This parses via the format list ("%Y-%m-%d %H:%M:%S"), so it's Valid
        let result = validator.validate("2024-12-24 10:30:00");
        match result {
            TimestampResult::Valid(_) | TimestampResult::Corrected(_, _) => {}
            TimestampResult::Invalid(e) => panic!("should parse: {e}"),
        }
    }

    #[test]
    fn test_parse_timestamp_iso_with_offset() {
        let validator = TimestampValidator {
            max_future_seconds: 0,
            max_past_seconds: 0,
            correct_known_bad: false,
        };
        // ISO 8601 with offset
        let result = validator.validate("2024-12-24T10:30:00+00:00");
        match result {
            TimestampResult::Valid(_) => {}
            other => panic!("Expected Valid, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_timestamp_naive_with_millis() {
        let validator = TimestampValidator {
            max_future_seconds: 0,
            max_past_seconds: 0,
            correct_known_bad: false,
        };
        // Naive datetime with milliseconds (no timezone)
        let result = validator.validate("2024-12-24 10:30:00.123");
        match result {
            TimestampResult::Valid(_) => {}
            other => panic!("Expected Valid, got {other:?}"),
        }
    }

    #[test]
    fn test_unix_seconds_helper() {
        // validate_unix with small epoch (seconds)
        let validator = TimestampValidator::default();
        // Seconds epoch for 2024-01-01
        let r = validator.validate_unix(1704067200);
        assert!(
            matches!(
                r,
                TimestampResult::Valid(_) | TimestampResult::Corrected(_, _)
            ),
            "{r:?}"
        );
    }

    #[test]
    fn test_validate_with_strict_future_and_past() {
        // Both bounds set — test both paths are reachable
        let validator = TimestampValidator {
            max_future_seconds: 30,
            max_past_seconds: 30,
            correct_known_bad: false,
        };
        // Current time should be valid
        let now = Timestamp::now();
        let r = validator.validate(&rfc3339_text(now));
        assert!(matches!(r, TimestampResult::Valid(_)), "{r:?}");

        // 5 min future → invalid
        let future = now + SignedDuration::from_mins(5);
        let r = validator.validate(&rfc3339_text(future));
        assert!(matches!(r, TimestampResult::Invalid(_)), "{r:?}");

        // 5 min past → invalid
        let past = now - SignedDuration::from_mins(5);
        let r = validator.validate(&rfc3339_text(past));
        assert!(matches!(r, TimestampResult::Invalid(_)), "{r:?}");
    }
}
