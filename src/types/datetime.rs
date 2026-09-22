// Copyright 2026 Krzysztof Duśko.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Date/time conversions for Netezza binary formats.
//!
//! Netezza stores DATE as days since 2000-01-01, TIMESTAMP as microseconds
//! since 2000-01-01, TIME as microseconds (with a float8 legacy layout).
//! Calendar math uses Howard Hinnant's `civil_from_days` algorithm so the
//! driver needs no external date library.

use std::fmt::Write as _;

pub const POSTGRES_EPOCH_DAYS: i64 = 10_957; // days from 1970-01-01 to 2000-01-01
const MS_PER_DAY: i64 = 86_400_000;
const US_PER_DAY: i64 = 86_400_000_000;

/// Convert days since 2000-01-01 to (year, month, day).
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Convert (year, month, day) to days since 1970-01-01 (`days_from_civil`).
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64; // [0, 399]
    let mp = if m > 2 { m - 3 } else { m + 9 } as u64;
    let doy = (153 * mp + 2) / 5 + d as u64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i64 - 719_468
}

/// Format a DATE (days since 2000-01-01) as `YYYY-MM-DD`.
pub fn date_from_4bytes(days: i32) -> String {
    let mut out = String::new();
    date_from_4bytes_into(days, &mut out);
    out
}

pub(crate) fn date_from_4bytes_into(days: i32, out: &mut String) {
    let (y, m, d) = civil_from_days(days as i64 + POSTGRES_EPOCH_DAYS);
    out.clear();
    append_date(y, m, d, out);
}

/// Format a TIMESTAMP (microseconds since 2000-01-01) as
/// `YYYY-MM-DD HH:MM:SS[.ffffff]` (fraction omitted when zero).
pub fn timestamp_from_8bytes(micros: i64) -> String {
    let mut out = String::new();
    timestamp_from_8bytes_into(micros, &mut out);
    out
}

pub(crate) fn timestamp_from_8bytes_into(micros: i64, out: &mut String) {
    let days = micros.div_euclid(US_PER_DAY);
    let rem = micros.rem_euclid(US_PER_DAY);
    let (y, m, d) = civil_from_days(days + POSTGRES_EPOCH_DAYS);
    let secs = rem / 1_000_000;
    let frac = rem % 1_000_000;
    let (hh, mm, ss) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    out.clear();
    append_date(y, m, d, out);
    out.push(' ');
    append_hms(hh, mm, ss, out);
    if frac != 0 {
        out.push('.');
        append_fixed_digits(frac, 6, out);
    }
}

/// Format TIME (microseconds) as `HH:MM:SS[.ffffff]`.
pub fn time_from_8bytes(micros: i64) -> String {
    let mut out = String::new();
    time_from_8bytes_into(micros, &mut out);
    out
}

pub(crate) fn time_from_8bytes_into(micros: i64, out: &mut String) {
    out.clear();
    append_time_from_8bytes(micros, out);
}

fn append_time_from_8bytes(micros: i64, out: &mut String) {
    let secs = micros.div_euclid(1_000_000);
    let frac = micros.rem_euclid(1_000_000);
    let (hh, mm, ss) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    append_hms(hh, mm, ss, out);
    if frac != 0 {
        out.push('.');
        append_fixed_digits(frac, 6, out);
    }
}

#[inline]
fn append_date(year: i64, month: u32, day: u32, out: &mut String) {
    if (0..=9999).contains(&year) {
        append_fixed_digits(year, 4, out);
    } else {
        let _ = write!(out, "{year}");
    }
    out.push('-');
    append_fixed_digits(month as i64, 2, out);
    out.push('-');
    append_fixed_digits(day as i64, 2, out);
}

#[inline]
fn append_hms(hours: i64, minutes: i64, seconds: i64, out: &mut String) {
    append_fixed_digits(hours, 2, out);
    out.push(':');
    append_fixed_digits(minutes, 2, out);
    out.push(':');
    append_fixed_digits(seconds, 2, out);
}

#[inline]
fn append_fixed_digits(value: i64, width: usize, out: &mut String) {
    let mut divisor = 1i64;
    for _ in 1..width {
        divisor *= 10;
    }
    let mut remaining = value;
    for _ in 0..width {
        let digit = (remaining / divisor).rem_euclid(10) as u8;
        out.push((b'0' + digit) as char);
        remaining %= divisor;
        if divisor > 1 {
            divisor /= 10;
        }
    }
}

/// Format TIMETZ (8-byte time + 4-byte zone seconds at `zone_offset`).
pub fn timetz_from_bytes(data: &[u8], fld_len: usize) -> String {
    let mut out = String::new();
    timetz_from_bytes_into(data, fld_len, &mut out);
    out
}

pub(crate) fn timetz_from_bytes_into(data: &[u8], fld_len: usize, out: &mut String) {
    let micros = i64::from_le_bytes(data[0..8].try_into().unwrap());
    let zone_seconds = i32::from_le_bytes(data[fld_len - 4..fld_len].try_into().unwrap());
    out.clear();
    append_time_from_8bytes(micros, out);
    let sign = if zone_seconds < 0 { '+' } else { '-' };
    let abs = zone_seconds.unsigned_abs();
    let tz_h = abs / 3600;
    let tz_m = (abs % 3600) / 60;
    let tz_s = abs % 60;
    if tz_s != 0 {
        let _ = write!(out, "{sign}{tz_h:02}:{tz_m:02}:{tz_s:02}");
    } else if tz_m != 0 {
        let _ = write!(out, "{sign}{tz_h:02}:{tz_m:02}");
    } else {
        let _ = write!(out, "{sign}{tz_h:02}");
    }
}

/// Format INTERVAL (8-byte micros + 4-byte months) as a Netezza interval text.
pub fn interval_from_12bytes(data: &[u8]) -> String {
    let mut out = String::new();
    interval_from_12bytes_into(data, &mut out);
    out
}

pub(crate) fn interval_from_12bytes_into(data: &[u8], out: &mut String) {
    let micros = i64::from_le_bytes(data[0..8].try_into().unwrap());
    let months = i32::from_le_bytes(data[8..12].try_into().unwrap());
    out.clear();
    if months == 0 {
        append_time_from_8bytes(micros, out);
        return;
    }
    let years = months / 12;
    let remaining = months % 12;
    if years > 0 {
        let _ = write!(out, "{years} years {remaining} mons ");
    } else {
        let _ = write!(out, "{remaining} mons ");
    }
    append_time_from_8bytes(micros, out);
}

/// Parse `HH:MM:SS[.f]` (or `MM:SS`) text into (hours, minutes, seconds, micros).
pub fn parse_time_text(s: &str) -> (i64, i64, i64, i64) {
    let parts: Vec<&str> = s.split(':').collect();
    let h = parts.first().and_then(|p| p.parse().ok()).unwrap_or(0i64);
    let m = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0i64);
    let sec_parts: Vec<&str> = parts.get(2).unwrap_or(&"0").split('.').collect();
    let sec = sec_parts
        .first()
        .and_then(|p| p.parse().ok())
        .unwrap_or(0i64);
    let micros = sec_parts
        .get(1)
        .map(|frac| {
            let padded = format!("{:0<6}", &frac[..frac.len().min(6)]);
            padded.parse().unwrap_or(0)
        })
        .unwrap_or(0);
    (h, m, sec, micros)
}

/// Normalize a TIME text value (used on the text path).
pub fn normalize_time_text(s: &str) -> String {
    let (h, m, sec, micros) = parse_time_text(s.trim());
    if micros > 0 {
        format!("{h:02}:{m:02}:{sec:02}.{micros:06}")
    } else {
        format!("{h:02}:{m:02}:{sec:02}")
    }
}

/// Convert a DATE text (`YYYY-MM-DD`) to days since 2000-01-01 — used to give
/// the editor round-trippable values. Returns None when the text is not a date.
pub fn date_text_to_days(s: &str) -> Option<i64> {
    let s = s.trim();
    let mut it = s.split('-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: u32 = it.next()?.parse().ok()?;
    let d: u32 = it.next()?.parse().ok()?;
    Some(days_from_civil(y, m, d) - POSTGRES_EPOCH_DAYS)
}

/// Milliseconds since Unix epoch for a TIMESTAMP micros value (Node `Date`
/// parity — the JS driver stores millisecond precision).
pub fn timestamp_micros_to_epoch_ms(micros: i64) -> i64 {
    micros.div_euclid(1000) + (POSTGRES_EPOCH_DAYS * MS_PER_DAY)
}

/// Milliseconds since Unix epoch for a DATE days value (Node `Date` parity).
pub fn date_days_to_epoch_ms(days: i32) -> i64 {
    (days as i64 + POSTGRES_EPOCH_DAYS) * MS_PER_DAY
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_roundtrip() {
        for &(y, m, d) in &[
            (2000, 1, 1),
            (1970, 1, 1),
            (2024, 2, 29),
            (2024, 12, 11),
            (1900, 3, 1),
        ] {
            let days = days_from_civil(y, m, d);
            assert_eq!(civil_from_days(days), (y, m, d));
        }
    }

    #[test]
    fn date_epoch() {
        // 2000-01-01 is day 0
        assert_eq!(date_from_4bytes(0), "2000-01-01");
        assert_eq!(date_from_4bytes(POSTGRES_EPOCH_DAYS as i32), "2029-12-31");
        assert_eq!(date_from_4bytes(-1), "1999-12-31");
    }

    #[test]
    fn timestamp_formatting() {
        // 2025-01-06 08:00:00 UTC = 789,465,600s after 2000-01-01
        let micros = 789_465_600i64 * 1_000_000;
        assert_eq!(timestamp_from_8bytes(micros), "2025-01-06 08:00:00");
        assert_eq!(
            timestamp_from_8bytes(micros + 123_456),
            "2025-01-06 08:00:00.123456"
        );
    }

    #[test]
    fn time_formatting() {
        assert_eq!(time_from_8bytes(0), "00:00:00");
        assert_eq!(time_from_8bytes(43_260_000_000), "12:01:00");
        assert_eq!(time_from_8bytes(43_260_123_456), "12:01:00.123456");
    }

    #[test]
    fn timetz_formatting() {
        let mut data = vec![0u8; 12];
        data[0..8].copy_from_slice(&43_260_000_000i64.to_le_bytes());
        data[8..12].copy_from_slice(&(-41_100i32).to_le_bytes());
        assert_eq!(timetz_from_bytes(&data, 12), "12:01:00+11:25");
    }

    #[test]
    fn interval_formatting() {
        let mut data = vec![0u8; 12];
        data[8..12].copy_from_slice(&26i32.to_le_bytes()); // 26 months
        data[0..8].copy_from_slice(&3_600_000_000i64.to_le_bytes());
        assert_eq!(interval_from_12bytes(&data), "2 years 2 mons 01:00:00");
    }

    #[test]
    fn parse_time_text_works() {
        assert_eq!(parse_time_text("02:00:00"), (2, 0, 0, 0));
        assert_eq!(parse_time_text("10:12:13.5"), (10, 12, 13, 500_000));
    }
}
