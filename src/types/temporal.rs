//! Numeric temporal values; binary getters avoid text formatting and allocation.
use super::datetime;
use crate::{FromSql, FromSqlRaw, NzError, NzResult, NzValue, RawValue, ToSql};
use std::fmt;
const DAY: i64 = 86_400_000_000;
fn invalid() -> NzError {
    NzError::Config("invalid temporal value or SQL type".into())
}
fn binary<'a>(value: RawValue<'a>, expected: i32, width: usize) -> NzResult<Option<&'a [u8]>> {
    if value.is_null() {
        return Err(invalid());
    }
    if let Some((descriptor, row, offset, index)) = value.dbos {
        if descriptor.field_type[index] != expected {
            return Err(invalid());
        }
        return offset
            .checked_add(width)
            .and_then(|end| row.get(offset..end))
            .map(Some)
            .ok_or_else(|| NzError::Protocol("truncated temporal field".into()));
    }
    Ok(None)
}
fn parse_date(input: &str) -> NzResult<i32> {
    let (year, rest) = input.rsplit_once('-').ok_or_else(invalid)?;
    let day: u32 = rest.parse().map_err(|_| invalid())?;
    let (year, month) = year.rsplit_once('-').ok_or_else(invalid)?;
    let year: i64 = year.parse().map_err(|_| invalid())?;
    let month: u32 = month.parse().map_err(|_| invalid())?;
    if !(-6_000_000..=6_000_000).contains(&year)
        || !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
    {
        return Err(invalid());
    }
    let unix_days = datetime::days_from_civil(year, month, day);
    if datetime::civil_from_days(unix_days) != (year, month, day) {
        return Err(invalid());
    }
    (unix_days - datetime::POSTGRES_EPOCH_DAYS)
        .try_into()
        .map_err(|_| invalid())
}
fn parse_clock(input: &str) -> NzResult<i64> {
    let (negative, input) = if let Some(rest) = input.strip_prefix('-') {
        (true, rest)
    } else {
        (false, input.strip_prefix('+').unwrap_or(input))
    };
    let mut parts = input.split(':');
    let hours: i64 = parts
        .next()
        .ok_or_else(invalid)?
        .parse()
        .map_err(|_| invalid())?;
    let minutes: i64 = parts
        .next()
        .ok_or_else(invalid)?
        .parse()
        .map_err(|_| invalid())?;
    let seconds = parts.next().ok_or_else(invalid)?;
    if parts.next().is_some() || hours < 0 || !(0..60).contains(&minutes) {
        return Err(invalid());
    }
    let (seconds, fraction) = seconds.split_once('.').unwrap_or((seconds, ""));
    let seconds: i64 = seconds.parse().map_err(|_| invalid())?;
    if !(0..60).contains(&seconds)
        || fraction.len() > 6
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(invalid());
    }
    let fraction = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<i64>().map_err(|_| invalid())? * 10i64.pow(6 - fraction.len() as u32)
    };
    let micros = hours
        .checked_mul(3600)
        .and_then(|v| v.checked_add(minutes * 60 + seconds))
        .and_then(|v| v.checked_mul(1_000_000))
        .and_then(|v| v.checked_add(fraction))
        .ok_or_else(invalid)?;
    Ok(if negative { -micros } else { micros })
}
/// Days since 2000-01-01.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NzDate {
    pub days: i32,
}
/// Microseconds since midnight, without a time zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NzTime {
    microseconds: i64,
}
impl NzTime {
    pub fn new(microseconds: i64) -> NzResult<Self> {
        if !(0..DAY).contains(&microseconds) {
            return Err(invalid());
        }
        Ok(Self { microseconds })
    }
    pub fn microseconds(self) -> i64 {
        self.microseconds
    }
}
/// Microseconds since 2000-01-01, without a time zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NzTimestamp {
    pub microseconds: i64,
}
/// Calendar months and signed microseconds, preserving mixed-sign intervals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NzInterval {
    pub months: i32,
    pub microseconds: i64,
}
impl fmt::Display for NzDate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&datetime::date_from_4bytes(self.days))
    }
}
impl fmt::Display for NzTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&datetime::time_from_8bytes(self.microseconds))
    }
}
impl fmt::Display for NzTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&datetime::timestamp_from_8bytes(self.microseconds))
    }
}
impl fmt::Display for NzInterval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut bytes = [0; 12];
        bytes[..8].copy_from_slice(&self.microseconds.to_le_bytes());
        bytes[8..].copy_from_slice(&self.months.to_le_bytes());
        f.write_str(&datetime::interval_from_12bytes(&bytes))
    }
}
impl FromSql for NzDate {
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        if let Some(bytes) = binary(value, 6, 4)? {
            return Ok(Self {
                days: i32::from_le_bytes(bytes.try_into().unwrap()),
            });
        }
        Self::from_sql(&value.to_nz_value()?)
    }
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        if let NzValue::Date(text) = value {
            Ok(Self {
                days: parse_date(text)?,
            })
        } else {
            Err(invalid())
        }
    }
}
impl FromSql for NzTime {
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        if let Some(bytes) = binary(value, 8, 8)? {
            return Self::new(i64::from_le_bytes(bytes.try_into().unwrap()));
        }
        Self::from_sql(&value.to_nz_value()?)
    }
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        if let NzValue::Time(text) = value {
            Self::new(parse_clock(text)?)
        } else {
            Err(invalid())
        }
    }
}
impl FromSql for NzTimestamp {
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        if let Some(bytes) = binary(value, 9, 8)? {
            return Ok(Self {
                microseconds: i64::from_le_bytes(bytes.try_into().unwrap()),
            });
        }
        Self::from_sql(&value.to_nz_value()?)
    }
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        let NzValue::Timestamp(text) = value else {
            return Err(invalid());
        };
        let (date, time) = text.split_once(' ').ok_or_else(invalid)?;
        let time = NzTime::new(parse_clock(time)?)?;
        let micros = i64::from(parse_date(date)?)
            .checked_mul(DAY)
            .and_then(|v| v.checked_add(time.microseconds))
            .ok_or_else(invalid)?;
        Ok(Self {
            microseconds: micros,
        })
    }
}
impl FromSql for NzInterval {
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        if let Some(bytes) = binary(value, 10, 12)? {
            return Ok(Self {
                microseconds: i64::from_le_bytes(bytes[..8].try_into().unwrap()),
                months: i32::from_le_bytes(bytes[8..].try_into().unwrap()),
            });
        }
        Self::from_sql(&value.to_nz_value()?)
    }
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        let NzValue::Interval(text) = value else {
            return Err(invalid());
        };
        let mut months = 0i64;
        let mut micros = 0i64;
        let mut tokens = text.split_whitespace();
        while let Some(token) = tokens.next() {
            if token.contains(':') {
                micros = micros
                    .checked_add(parse_clock(token)?)
                    .ok_or_else(invalid)?;
                continue;
            }
            let number: i64 = token.parse().map_err(|_| invalid())?;
            match tokens.next().ok_or_else(invalid)? {
                "year" | "years" => {
                    months = months
                        .checked_add(number.checked_mul(12).ok_or_else(invalid)?)
                        .ok_or_else(invalid)?
                }
                "mon" | "mons" | "month" | "months" => {
                    months = months.checked_add(number).ok_or_else(invalid)?
                }
                "day" | "days" => {
                    micros = micros
                        .checked_add(number.checked_mul(DAY).ok_or_else(invalid)?)
                        .ok_or_else(invalid)?
                }
                _ => return Err(invalid()),
            }
        }
        Ok(Self {
            months: months.try_into().map_err(|_| invalid())?,
            microseconds: micros,
        })
    }
}
/// A time and UTC offset in seconds, without inventing a calendar date.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NzTimetz {
    time: NzTime,
    offset_seconds: i32,
}
impl NzTimetz {
    pub fn time(self) -> NzTime {
        self.time
    }
    pub fn offset_seconds(self) -> i32 {
        self.offset_seconds
    }
    pub fn new(time: NzTime, offset_seconds: i32) -> NzResult<Self> {
        if !(-86_400..86_400).contains(&offset_seconds) {
            return Err(invalid());
        }
        Ok(Self {
            time,
            offset_seconds,
        })
    }
}
impl fmt::Display for NzTimetz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut bytes = [0; 12];
        bytes[..8].copy_from_slice(&self.time.microseconds.to_le_bytes());
        bytes[8..].copy_from_slice(&self.offset_seconds.saturating_neg().to_le_bytes());
        f.write_str(&datetime::timetz_from_bytes(&bytes, 12))
    }
}
impl FromSql for NzTimetz {
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        if let Some(bytes) = binary(value, 11, 12)? {
            let time = NzTime::new(i64::from_le_bytes(bytes[..8].try_into().unwrap()))?;
            let offset = i32::from_le_bytes(bytes[8..].try_into().unwrap())
                .checked_neg()
                .ok_or_else(invalid)?;
            return Self::new(time, offset);
        }
        Self::from_sql(&value.to_nz_value()?)
    }
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        let NzValue::Timetz(text) = value else {
            return Err(invalid());
        };
        let index = text
            .char_indices()
            .find(|(index, ch)| *index > 0 && matches!(ch, '+' | '-'))
            .map(|(index, _)| index)
            .ok_or_else(invalid)?;
        let time = NzTime::new(parse_clock(&text[..index])?)?;
        let zone = &text[index + 1..];
        let mut parts = zone.split(':');
        let hours: i32 = parts
            .next()
            .ok_or_else(invalid)?
            .parse()
            .map_err(|_| invalid())?;
        let minutes: i32 = parts.next().unwrap_or("0").parse().map_err(|_| invalid())?;
        let seconds: i32 = parts.next().unwrap_or("0").parse().map_err(|_| invalid())?;
        if parts.next().is_some()
            || !(0..24).contains(&hours)
            || !(0..60).contains(&minutes)
            || !(0..60).contains(&seconds)
        {
            return Err(invalid());
        }
        let offset = hours * 3600 + minutes * 60 + seconds;
        Self::new(
            time,
            if text.as_bytes()[index] == b'-' {
                -offset
            } else {
                offset
            },
        )
    }
}

macro_rules! temporal_traits {
    ($ty:ty, $variant:ident) => {
        impl<'a> FromSqlRaw<'a> for $ty {
            fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
                Self::from_raw(value)
            }
        }
        impl ToSql for $ty {
            fn to_nz_value(&self) -> NzValue {
                NzValue::$variant(self.to_string())
            }
        }
    };
}
temporal_traits!(NzDate, Date);
temporal_traits!(NzTime, Time);
temporal_traits!(NzTimestamp, Timestamp);
temporal_traits!(NzInterval, Interval);
temporal_traits!(NzTimetz, Timetz);
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn timetz_preserves_microseconds_and_signed_offsets() {
        for text in ["12:34:56.123456+05:30", "00:00:00.000001-03:30"] {
            let value = NzTimetz::from_sql(&NzValue::Timetz(text.into())).unwrap();
            assert_eq!(
                NzTimetz::from_sql(&NzValue::Timetz(value.to_string())).unwrap(),
                value
            );
        }
        assert!(NzTimetz::from_sql(&NzValue::Timetz("12:00:00+24".into())).is_err());
        assert!(NzTimetz::new(NzTime::new(0).unwrap(), i32::MIN).is_err());
    }
    #[test]
    fn rejects_invalid_dates_and_clocks() {
        for date in [
            "2024-02-30",
            "2024-0-1",
            "2024-1-0",
            "2024-1-1-extra",
            "999999999999999-1-1",
        ] {
            assert!(parse_date(date).is_err());
        }
        for time in [
            "12:60:00",
            "12:00:60",
            "1:2",
            "-01:00:00",
            "24:00:00",
            "00:00:00.1234567",
        ] {
            assert!(NzTime::from_sql(&NzValue::Time(time.into())).is_err());
        }
        assert_eq!(parse_date("2000-01-01").unwrap(), 0);
    }
    #[test]
    fn intervals_preserve_negative_months_and_microseconds() {
        let interval =
            NzInterval::from_sql(&NzValue::Interval("-1 year -1 mon -00:00:00.000001".into()))
                .unwrap();
        assert_eq!(
            interval,
            NzInterval {
                months: -13,
                microseconds: -1
            }
        );
        assert_eq!(
            NzInterval::from_sql(&NzValue::Interval(interval.to_string())).unwrap(),
            interval
        );
        assert_eq!(
            NzInterval::from_sql(&NzValue::Interval("4 days 04:00:00".into()))
                .unwrap()
                .microseconds,
            360_000_000_000
        );
    }
}
