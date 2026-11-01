// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Date-time text from sources, read once and written per insert path.
//!
//! [`parse`] reads `YYYY-MM-DD[T| ]HH:MM:SS[.fraction][Z|±HH:MM]`. Text with no
//! zone is read as UTC, and text that names one is moved to UTC. The extractor
//! and transformer write it as RFC 3339 UTC text ending in `Z`
//! ([`normalise_zoned`]), which keeps the zone for the insert: the `RowBinary`
//! encoder writes ticks, and the `JSONEachRow` shaping step epoch text, because
//! `ClickHouse` reads zone-less text in the column's timezone, else the
//! server's.

use chrono::{DateTime, Utc};
use serde_json::Value;

/// An instant read from date-time text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UtcInstant {
    /// Whole seconds since the Unix epoch, floored.
    pub secs: i64,
    /// Nanoseconds past `secs`.
    pub nanos: u32,
    /// Whether the text named its zone.
    pub zoned: bool,
}

/// Byte length of `YYYY-MM-DD HH:MM:SS`.
const WHOLE_SECONDS_LEN: usize = 19;

/// Read date-time text in the form the module doc names.
///
/// Fraction digits past nine are dropped, as `ClickHouse` drops them.
///
/// # Errors
///
/// What is wrong with text that is not that form, names a date or time that
/// does not exist, or carries an offset past 23:59.
pub fn parse(text: &str) -> Result<UtcInstant, &'static str> {
    const INVALID: &str = "invalid DateTime string";
    let b = text.as_bytes();
    if b.len() < WHOLE_SECONDS_LEN
        || b[4] != b'-'
        || b[7] != b'-'
        || !matches!(b[10], b'T' | b't' | b' ')
        || b[13] != b':'
        || b[16] != b':'
    {
        return Err(INVALID);
    }
    let (Some(year), Some(month), Some(day), Some(hour), Some(min), Some(sec)) = (
        digits(b, 0, 4),
        digits(b, 5, 2),
        digits(b, 8, 2),
        digits(b, 11, 2),
        digits(b, 14, 2),
        digits(b, 17, 2),
    ) else {
        return Err(INVALID);
    };
    if !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || min > 59
        || sec > 59
    {
        return Err(INVALID);
    }

    let mut pos = WHOLE_SECONDS_LEN;
    let mut nanos = 0_u32;
    if b.get(pos) == Some(&b'.') {
        let start = pos + 1;
        let len = b[start..].iter().take_while(|c| c.is_ascii_digit()).count();
        if len == 0 {
            return Err("invalid fractional seconds");
        }
        for (place, &c) in (0_u32..).zip(&b[start..start + len.min(9)]) {
            nanos += u32::from(c - b'0') * 10_u32.pow(8 - place);
        }
        pos = start + len;
    }

    let offset = match b.get(pos) {
        None => None,
        Some(b'Z' | b'z') => {
            pos += 1;
            Some(0)
        }
        Some(&sign @ (b'+' | b'-')) => {
            let (hh, mm, len) = match b.len() - pos - 1 {
                5 if b[pos + 3] == b':' => (digits(b, pos + 1, 2), digits(b, pos + 4, 2), 6),
                4 => (digits(b, pos + 1, 2), digits(b, pos + 3, 2), 5),
                2 => (digits(b, pos + 1, 2), Some(0), 3),
                _ => return Err("invalid timezone offset"),
            };
            let (Some(hh), Some(mm)) = (hh, mm) else {
                return Err("invalid timezone offset");
            };
            if hh > 23 || mm > 59 {
                return Err("invalid timezone offset");
            }
            pos += len;
            let magnitude = i64::from(hh * 3600 + mm * 60);
            Some(if sign == b'-' { -magnitude } else { magnitude })
        }
        Some(_) => return Err(INVALID),
    };
    if pos != b.len() {
        return Err(INVALID);
    }

    // Four digits, so the year always fits.
    let year = i32::try_from(year).map_err(|_| INVALID)?;
    let local =
        civil_days_from_epoch(year, month, day) * 86_400 + i64::from(hour * 3600 + min * 60 + sec);
    Ok(UtcInstant {
        secs: local - offset.unwrap_or(0),
        nanos,
        zoned: offset.is_some(),
    })
}

impl UtcInstant {
    /// The instant as UTC text. `None` outside the dates chrono holds.
    #[must_use]
    pub fn utc_text(self) -> Option<String> {
        DateTime::<Utc>::from_timestamp(self.secs, self.nanos).map(|dt| utc_text(&dt))
    }
}

/// `YYYY-MM-DDTHH:MM:SS.fffZ` in UTC, widened to six or nine fraction digits
/// where the instant carries them.
#[must_use]
pub fn utc_text(dt: &DateTime<Utc>) -> String {
    let nanos = dt.timestamp_subsec_nanos();
    let format = if nanos.is_multiple_of(1_000_000) {
        "%Y-%m-%dT%H:%M:%S%.3fZ"
    } else if nanos.is_multiple_of(1_000) {
        "%Y-%m-%dT%H:%M:%S%.6fZ"
    } else {
        "%Y-%m-%dT%H:%M:%S%.9fZ"
    };
    dt.format(format).to_string()
}

/// Whether text could end in a zone: a `Z`, or a sign in the last six bytes.
#[must_use]
pub fn may_name_zone(text: &str) -> bool {
    let b = text.as_bytes();
    b.len() > WHOLE_SECONDS_LEN
        && (matches!(b[b.len() - 1], b'Z' | b'z')
            || b[b.len() - 6..].iter().any(|c| matches!(c, b'+' | b'-')))
}

/// Rewrite a string value that names its zone as UTC text, keeping every
/// fraction digit, and leave every other value as it is.
pub fn normalise_zoned(value: &mut Value) {
    let Value::String(text) = value else {
        return;
    };
    if !may_name_zone(text) {
        return;
    }
    if let Some(utc) = parse(text)
        .ok()
        .filter(|instant| instant.zoned)
        .and_then(UtcInstant::utc_text)
    {
        *text = utc;
    }
}

/// The ASCII digits at `b[at..at + n]` as a number.
fn digits(b: &[u8], at: usize, n: usize) -> Option<u32> {
    b.get(at..at + n)?.iter().try_fold(0_u32, |acc, &c| {
        c.is_ascii_digit().then(|| acc * 10 + u32::from(c - b'0'))
    })
}

fn days_in_month(year: u32, month: u32) -> u32 {
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days since 1970-01-01 for a civil date (Howard Hinnant's algorithm).
#[must_use]
fn civil_days_from_epoch(year: i32, month: u32, day: u32) -> i64 {
    let y = i64::from(if month <= 2 { year - 1 } else { year });
    let m = i64::from(if month <= 2 { month + 9 } else { month - 3 });
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * m + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-04-07 07:03:00 UTC.
    const SECS: i64 = 1_775_545_380;

    fn instant(text: &str) -> UtcInstant {
        parse(text).unwrap_or_else(|e| panic!("{text}: {e}"))
    }

    #[test]
    fn every_zone_form_reads_as_the_same_instant() {
        for text in [
            "2026-04-07T17:03:00.123456789+10:00",
            "2026-04-07T17:03:00.123456789+1000",
            "2026-04-07T17:03:00.123456789+10",
            "2026-04-07T01:33:00.123456789-05:30",
            "2026-04-07 07:03:00.123456789Z",
            "2026-04-07t07:03:00.123456789z",
            "2026-04-07T07:03:00.123456789+00:00",
            "2026-04-07T07:03:00.123456789-00:00",
        ] {
            assert_eq!(
                instant(text),
                UtcInstant {
                    secs: SECS,
                    nanos: 123_456_789,
                    zoned: true
                },
                "{text}"
            );
        }
    }

    #[test]
    fn zone_less_text_reads_as_utc_and_says_so() {
        for text in ["2026-04-07 07:03:00", "2026-04-07T07:03:00"] {
            assert_eq!(
                instant(text),
                UtcInstant {
                    secs: SECS,
                    nanos: 0,
                    zoned: false
                },
                "{text}"
            );
        }
    }

    #[test]
    fn an_offset_that_crosses_midnight_or_a_year_moves_the_date() {
        // 2026-04-06 16:00:00 UTC.
        assert_eq!(instant("2026-04-07 02:00:00+10:00").secs, 1_775_491_200);
        // 2026-01-01 00:30:00 UTC.
        assert_eq!(instant("2025-12-31T23:30:00-01:00").secs, 1_767_227_400);
    }

    #[test]
    fn fraction_digits_past_nine_are_dropped() {
        assert_eq!(
            instant("2026-04-07 07:03:00.1234567891234Z").nanos,
            123_456_789
        );
        assert_eq!(instant("2026-04-07 07:03:00.1Z").nanos, 100_000_000);
    }

    #[test]
    fn malformed_text_is_refused() {
        for text in [
            "",
            "2026-04-07",
            "2026-04-07X07:03:00",
            "2026-13-07 07:03:00",
            "2026-02-29 07:03:00",
            "2026-04-31 07:03:00",
            "2026-04-07 24:00:00",
            "2026-04-07 07:03:60",
            "2026-04-07 07:03:00.",
            "2026-04-07 07:03:00+24:00",
            "2026-04-07 07:03:00+10:60",
            "2026-04-07 07:03:00+1",
            "2026-04-07 07:03:00+10:00 ",
            "2026-04-07 07:03:00 UTC",
            "2026-1\u{e9}-7 07:03:00",
            "\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}\u{e9}",
        ] {
            assert!(parse(text).is_err(), "{text:?} must be refused");
        }
        assert!(parse("2024-02-29 07:03:00").is_ok(), "2024 is a leap year");
    }

    #[test]
    fn utc_text_keeps_every_digit_the_instant_carries() {
        for (text, utc) in [
            ("2026-04-07T17:03:00+10:00", "2026-04-07T07:03:00.000Z"),
            ("2026-04-07T17:03:00.12+10:00", "2026-04-07T07:03:00.120Z"),
            (
                "2026-04-07T17:03:00.1234+10:00",
                "2026-04-07T07:03:00.123400Z",
            ),
            (
                "2026-04-07T17:03:00.123456789+10:00",
                "2026-04-07T07:03:00.123456789Z",
            ),
        ] {
            assert_eq!(instant(text).utc_text().as_deref(), Some(utc), "{text}");
        }
    }

    #[test]
    fn the_zone_guard_admits_only_text_that_can_end_in_a_zone() {
        assert!(may_name_zone("2026-09-07T05:53:53.385Z"));
        assert!(may_name_zone("2026-09-07t05:53:53z"));
        assert!(may_name_zone("2026-09-07 05:53:53.385+10:00"));
        assert!(may_name_zone("2026-09-07 05:53:53-0530"));
        assert!(!may_name_zone("2026-09-07 05:53:53.385"));
        assert!(!may_name_zone("2026-09-07T05:53:53"));
        assert!(!may_name_zone("2026-09-07"));
        assert!(!may_name_zone(""));
    }

    #[test]
    fn normalise_zoned_rewrites_only_a_zoned_string() {
        let mut zoned = Value::from("2026-04-07T01:33:00.5-05:30");
        normalise_zoned(&mut zoned);
        assert_eq!(zoned, Value::from("2026-04-07T07:03:00.500Z"));

        for unchanged in [
            Value::from("2026-04-07T07:03:00.123Z"),
            Value::from("not a time at all Z"),
            Value::from(1_775_545_380_123_i64),
            Value::Null,
        ] {
            let mut value = unchanged.clone();
            normalise_zoned(&mut value);
            assert_eq!(value, unchanged);
        }
    }
}
