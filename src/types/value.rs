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

//! Runtime value type — the Rust analog of tokio-postgres' `Value`.
//!
//! Every cell returned by the driver is an [`NzValue`]; typed access goes
//! through the [`FromSql`] trait (impls for primitives and `Option<T>`).
//! Temporal values keep their text rendering (`YYYY-MM-DD`,
//! `HH:MM:SS[.ffffff]`, …) exactly as Netezza presents them, so results
//! round-trip losslessly and match the Node driver's output when canonicalized.

use crate::error::{NzError, NzResult};
#[cfg(feature = "chrono")]
use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::{prelude::ToPrimitive, Decimal};

/// A Netezza `TIMETZ` value without inventing a calendar date.
#[cfg(feature = "chrono")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NzTimeTz {
    pub time: NaiveTime,
    pub offset: FixedOffset,
}

/// A single Netezza value.
#[derive(Debug, Clone, PartialEq)]
pub enum NzValue {
    Null,
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    /// IEEE 754 single precision (REAL).
    Float4(f32),
    /// IEEE 754 double precision (DOUBLE / FLOAT8).
    Float8(f64),
    /// NUMERIC — exact decimal text, precision-preserving (may exceed f64).
    Numeric(String),
    /// NUMERIC represented as an exact fixed-point decimal when its 96-bit
    /// coefficient and scale fit `rust_decimal`; scale/trailing zeroes are
    /// preserved without storing the value as a heap-allocated string.
    Decimal(Decimal),
    /// CHAR / VARCHAR / NCHAR / NVARCHAR and all other textual types.
    Text(String),
    /// DATE as `YYYY-MM-DD`.
    Date(String),
    /// TIME as `HH:MM:SS[.ffffff]`.
    Time(String),
    /// TIMETZ as `HH:MM:SS[.ffffff]±HH[:MM[:SS]]`.
    Timetz(String),
    /// TIMESTAMP as `YYYY-MM-DD HH:MM:SS[.ffffff]`.
    Timestamp(String),
    /// INTERVAL in Netezza text form.
    Interval(String),
    /// BYTEA / binary payloads.
    Bytea(Vec<u8>),
}

/// String-backed value variant used by the protocol decoders to retain the
/// allocation for a column while rows are streamed. This is crate-private so
/// the public value model stays a simple enum.
#[derive(Clone, Copy)]
pub(crate) enum StringValueKind {
    Text,
    Date,
    Time,
    Timetz,
    Timestamp,
    Interval,
}

impl StringValueKind {
    fn empty(self) -> NzValue {
        match self {
            Self::Text => NzValue::Text(String::new()),
            Self::Date => NzValue::Date(String::with_capacity(10)),
            Self::Time => NzValue::Time(String::with_capacity(15)),
            Self::Timetz => NzValue::Timetz(String::with_capacity(24)),
            Self::Timestamp => NzValue::Timestamp(String::with_capacity(26)),
            Self::Interval => NzValue::Interval(String::new()),
        }
    }
}

/// Return the reusable string buffer for `kind`, changing the enum variant
/// only when the column's logical value kind changed.
pub(crate) fn string_value_slot(value: &mut NzValue, kind: StringValueKind) -> &mut String {
    let matches_kind = matches!(
        (kind, &*value),
        (StringValueKind::Text, NzValue::Text(_))
            | (StringValueKind::Date, NzValue::Date(_))
            | (StringValueKind::Time, NzValue::Time(_))
            | (StringValueKind::Timetz, NzValue::Timetz(_))
            | (StringValueKind::Timestamp, NzValue::Timestamp(_))
            | (StringValueKind::Interval, NzValue::Interval(_))
    );
    if !matches_kind {
        *value = kind.empty();
    }
    match value {
        NzValue::Text(text)
        | NzValue::Date(text)
        | NzValue::Time(text)
        | NzValue::Timetz(text)
        | NzValue::Timestamp(text)
        | NzValue::Interval(text) => text,
        _ => unreachable!("string_value_slot installed the requested variant"),
    }
}

impl NzValue {
    /// Human/grid display form (short, no type decoration).
    pub fn to_display_string(&self) -> String {
        match self {
            NzValue::Null => "NULL".into(),
            NzValue::Bool(b) => b.to_string(),
            NzValue::Int2(v) => v.to_string(),
            NzValue::Int4(v) => v.to_string(),
            NzValue::Int8(v) => v.to_string(),
            NzValue::Float4(v) => fmt_f64(*v as f64),
            NzValue::Float8(v) => fmt_f64(*v),
            NzValue::Numeric(s)
            | NzValue::Text(s)
            | NzValue::Date(s)
            | NzValue::Time(s)
            | NzValue::Timetz(s)
            | NzValue::Timestamp(s)
            | NzValue::Interval(s) => s.clone(),
            NzValue::Decimal(value) => value.to_string(),
            NzValue::Bytea(b) => {
                let mut out = String::from("0x");
                push_hex(&mut out, b);
                out
            }
        }
    }

    /// Canonical string used for cross-implementation comparisons.
    ///
    /// Mirrors how the Node driver's JSON dump represents values:
    /// numbers via JS `String()`, `Date` via `toISOString()`, `BigInt` as
    /// decimal text, time objects via their `toString()`.
    pub fn to_node_canonical(&self) -> String {
        match self {
            NzValue::Null => "null".into(),
            NzValue::Bool(b) => b.to_string(),
            NzValue::Int2(v) => v.to_string(),
            NzValue::Int4(v) => v.to_string(),
            NzValue::Int8(v) => v.to_string(),
            NzValue::Float4(v) => fmt_f64(*v as f64),
            NzValue::Float8(v) => fmt_f64(*v),
            NzValue::Numeric(s) | NzValue::Text(s) | NzValue::Interval(s) => s.clone(),
            NzValue::Decimal(value) => value.to_string(),
            NzValue::Date(s) => format!("{s}T00:00:00.000Z"),
            // Node Date has ms precision: drop sub-ms digits to match.
            NzValue::Timestamp(s) => {
                // "YYYY-MM-DD HH:MM:SS.ffffff" → "YYYY-MM-DDTHH:MM:SS.mmmZ"
                let (date_part, time_part) = match s.split_once(' ') {
                    Some((d, t)) => (d, t),
                    None => (s.as_str(), ""),
                };
                let (hms, frac) = match time_part.split_once('.') {
                    Some((h, f)) => (h, f),
                    None => (time_part, ""),
                };
                let ms = if frac.is_empty() {
                    "000".to_string()
                } else {
                    format!("{:0<3}", &frac[..frac.len().min(3)])
                };
                format!("{date_part}T{hms}.{ms}Z")
            }
            NzValue::Time(s) => s.clone(),
            NzValue::Timetz(s) => s.clone(),
            NzValue::Bytea(b) => {
                let mut out = String::from("E'\\\\x");
                push_hex(&mut out, b);
                out.push('\'');
                out
            }
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, NzValue::Null)
    }
}

/// Shortest round-trip float formatting (matches JS `String(number)` for
/// finite doubles, which is the same shortest-representation algorithm).
fn fmt_f64(v: f64) -> String {
    if v == v.trunc() && v.abs() < 1e16 {
        // JS prints integral doubles without ".0" — Rust does the same via {}.
        format!("{v}")
    } else {
        format!("{v}")
    }
}

/// Hex lookup table: `format!("{byte:02x}")` per byte costs a formatting
/// machinery call per byte; a table push is a few instructions (C# port
/// renders hex the same way in its hot text paths).
const HEX: &[u8; 16] = b"0123456789abcdef";

pub(crate) fn push_hex(out: &mut String, bytes: &[u8]) {
    out.reserve(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xF) as usize] as char);
    }
}

/// A borrowed value from a result row.
///
/// The payload is kept in the row's owned frame buffer.  The optional decoded
/// value is used by compatibility rows constructed from an already materialized
/// [`NzValue`].  User implementations can inspect `as_bytes()` and the wire
/// metadata without forcing conversion to the compatibility enum.
///
/// Eagerly decoded rows (buffered `query()` and `Row::new`) expose `as_bytes()`
/// only for cells whose decoded value preserves the wire bytes exactly
/// (untrimmed text and binary payloads); other types return `None` and must be
/// read through the typed interface.
#[derive(Debug, Clone, Copy)]
pub struct RawValue<'a> {
    bytes: Option<&'a [u8]>,
    decoded: Option<&'a NzValue>,
    type_oid: i32,
    type_mod: i32,
    format: u8,
    pub(crate) dbos: Option<(&'a crate::tuple_desc::DbosTupleDesc, &'a [u8], usize, usize)>,
}

impl<'a> RawValue<'a> {
    #[cfg(test)]
    pub(crate) fn from_decoded(
        value: &'a NzValue,
        type_oid: i32,
        type_mod: i32,
        format: u8,
    ) -> Self {
        Self {
            bytes: None,
            decoded: Some(value),
            type_oid,
            type_mod,
            format,
            dbos: None,
        }
    }

    pub(crate) fn from_parts(
        bytes: Option<&'a [u8]>,
        decoded: Option<&'a NzValue>,
        type_oid: i32,
        type_mod: i32,
        format: u8,
    ) -> Self {
        Self {
            bytes,
            decoded,
            type_oid,
            type_mod,
            format,
            dbos: None,
        }
    }

    pub fn is_null(&self) -> bool {
        match self.decoded {
            Some(NzValue::Null) => true,
            Some(_) => false,
            None => self.bytes.is_none(),
        }
    }

    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        self.bytes
    }

    /// Borrow the already-decoded compatibility value, if the row was decoded
    /// eagerly (buffered `query()` rows and `Row::new` rows). Lazy rows return
    /// `None` and expose wire state (`as_bytes()` / `dbos`) instead.
    pub(crate) fn decoded(&self) -> Option<&'a NzValue> {
        self.decoded
    }

    pub fn type_oid(&self) -> i32 {
        self.type_oid
    }

    pub fn type_mod(&self) -> i32 {
        self.type_mod
    }

    pub fn format(&self) -> u8 {
        self.format
    }

    pub(crate) fn with_dbos(
        mut self,
        descriptor: &'a crate::tuple_desc::DbosTupleDesc,
        row: &'a [u8],
        offset: usize,
        index: usize,
    ) -> Self {
        self.dbos = Some((descriptor, row, offset, index));
        self.format = 1;
        self
    }

    pub(crate) fn to_nz_value(self) -> NzResult<NzValue> {
        if let Some(value) = self.decoded {
            return Ok(value.clone());
        }
        if self.is_null() {
            return Ok(NzValue::Null);
        }
        if let Some((descriptor, row, offset, index)) = self.dbos {
            let mut value = NzValue::Null;
            descriptor.parse_field_into(row, offset, index, &mut value)?;
            return Ok(value);
        }
        let bytes = self
            .bytes
            .ok_or_else(|| NzError::Config("cannot decode SQL NULL as a value".into()))?;
        if self.format != 0 {
            return Err(NzError::Unsupported(
                "direct binary FromSql decoding requires a typed Row context".into(),
            ));
        }
        let text = std::str::from_utf8(bytes)
            .map_err(|e| NzError::Protocol(format!("invalid UTF-8 field: {e}")))?;
        crate::types::text::try_parse_text_value(text, self.type_oid, self.type_mod)
    }
}

/// Typed extraction from an already decoded Netezza value.
///
/// This is the original public typed-access interface. It intentionally
/// remains borrowed from [`NzValue`] so downstream implementations written
/// against earlier releases continue to compile.
pub trait FromSql: Sized {
    fn from_sql(value: &NzValue) -> NzResult<Self>;
    /// Decode only the selected field. Existing implementations receive its value.
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        Self::from_sql(&value.to_nz_value()?)
    }
}

/// Typed extraction from raw Netezza field bytes.
///
/// This opt-in interface complements [`FromSql`] for consumers that need
/// access to the lazy row representation. Use [`crate::connection::Row::try_get_raw_typed`]
/// rather than changing an existing [`FromSql`] implementation.
pub trait FromSqlRaw<'a>: Sized {
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self>;
}

impl FromSql for NzValue {
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        Ok(value.clone())
    }
}

impl FromSql for bool {
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        match value {
            NzValue::Bool(value) => Ok(*value),
            other => Err(unexpected(other, "BOOL")),
        }
    }
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        if let Some(decoded) = value.decoded {
            return Self::from_sql(decoded);
        }
        if let Some(result) = decode_bool_dbos(&value) {
            return result;
        }
        Self::from_sql(&value.to_nz_value()?)
    }
}

/// Direct binary BOOL decode for lazy DBOS rows (no intermediate `NzValue`).
fn decode_bool_dbos(value: &RawValue<'_>) -> Option<NzResult<bool>> {
    if value.is_null() {
        return None;
    }
    let (descriptor, row, offset, index) = value.dbos?;
    if descriptor.field_type.get(index).copied()? != crate::messages::nz_type::NZ_TYPE_BOOL {
        return None;
    }
    let byte = *row.get(offset)?;
    Some(match byte {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(NzError::Protocol("invalid binary BOOLEAN".into())),
    })
}

/// Direct binary integer decode for lazy DBOS rows. Returns `None` when the
/// field is not a fixed-width integer family member (caller falls back to
/// the `NzValue` path, preserving NUMERIC/text coercions).
fn decode_int_dbos(value: &RawValue<'_>) -> Option<NzResult<i128>> {
    if value.is_null() {
        return None;
    }
    let (descriptor, row, offset, index) = value.dbos?;
    let field_type = descriptor.field_type.get(index).copied()?;
    use crate::messages::nz_type as t;
    let raw: i128 = match field_type {
        t::NZ_TYPE_INT8 => {
            i64::from_le_bytes(row.get(offset..offset + 8)?.try_into().ok()?) as i128
        }
        t::NZ_TYPE_INT => i32::from_le_bytes(row.get(offset..offset + 4)?.try_into().ok()?) as i128,
        t::NZ_TYPE_INT2 => {
            i16::from_le_bytes(row.get(offset..offset + 2)?.try_into().ok()?) as i128
        }
        t::NZ_TYPE_INT1 => *row.get(offset)? as i8 as i128,
        _ => return None,
    };
    Some(Ok(raw))
}

/// Direct binary float decode for lazy DBOS rows. Integers coerce like the
/// `NzValue` path; other types return `None` for fallback.
fn decode_float_dbos(value: &RawValue<'_>) -> Option<NzResult<f64>> {
    if value.is_null() {
        return None;
    }
    let (descriptor, row, offset, index) = value.dbos?;
    let field_type = descriptor.field_type.get(index).copied()?;
    use crate::messages::nz_type as t;
    let number: f64 = match field_type {
        t::NZ_TYPE_DOUBLE => f64::from_le_bytes(row.get(offset..offset + 8)?.try_into().ok()?),
        t::NZ_TYPE_FLOAT => {
            f32::from_le_bytes(row.get(offset..offset + 4)?.try_into().ok()?) as f64
        }
        t::NZ_TYPE_INT8 => i64::from_le_bytes(row.get(offset..offset + 8)?.try_into().ok()?) as f64,
        t::NZ_TYPE_INT => i32::from_le_bytes(row.get(offset..offset + 4)?.try_into().ok()?) as f64,
        t::NZ_TYPE_INT2 => i16::from_le_bytes(row.get(offset..offset + 2)?.try_into().ok()?) as f64,
        t::NZ_TYPE_INT1 => *row.get(offset)? as i8 as f64,
        _ => return None,
    };
    Some(Ok(number))
}

fn decode_int<T>(value: &NzValue, name: &str) -> NzResult<T>
where
    T: TryFrom<i128>,
{
    match value {
        NzValue::Int2(v) => T::try_from(*v as i128)
            .map_err(|_| NzError::Config(format!("value {v} out of range for {name}"))),
        NzValue::Int4(v) => T::try_from(*v as i128)
            .map_err(|_| NzError::Config(format!("value {v} out of range for {name}"))),
        NzValue::Int8(v) => T::try_from(*v as i128)
            .map_err(|_| NzError::Config(format!("value {v} out of range for {name}"))),
        NzValue::Numeric(v) | NzValue::Text(v) => v
            .trim()
            .parse::<i128>()
            .ok()
            .and_then(|n| T::try_from(n).ok())
            .ok_or_else(|| NzError::Config(format!("value {v} out of range for {name}"))),
        NzValue::Decimal(v) => v
            .fract()
            .is_zero()
            .then(|| v.to_i128())
            .flatten()
            .and_then(|n| T::try_from(n).ok())
            .ok_or_else(|| NzError::Config(format!("value {v} out of range for {name}"))),
        other => Err(unexpected(other, "integer")),
    }
}

macro_rules! from_sql_int {
    ($($t:ty),*) => {
        $(
            impl FromSql for $t {
                fn from_sql(value: &NzValue) -> NzResult<Self> {
                    decode_int(value, stringify!($t))
                }
                fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
                    if let Some(decoded) = value.decoded {
                        return decode_int(decoded, stringify!($t));
                    }
                    if let Some(raw) = decode_int_dbos(&value) {
                        let number = raw?;
                        return <$t>::try_from(number).map_err(|_| {
                            NzError::Config(format!("value {number} out of range for {}", stringify!($t)))
                        });
                    }
                    decode_int(&value.to_nz_value()?, stringify!($t))
                }
            }
        )*
    };
}

from_sql_int!(i16, i32, i64, u8, u16, u32);

impl FromSql for f32 {
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        match value {
            NzValue::Float4(v) => Ok(*v),
            NzValue::Float8(v) => Ok(*v as f32),
            NzValue::Int2(v) => Ok(*v as f32),
            NzValue::Int4(v) => Ok(*v as f32),
            NzValue::Int8(v) => Ok(*v as f32),
            NzValue::Numeric(v) | NzValue::Text(v) => v
                .trim()
                .parse::<f32>()
                .map_err(|_| unexpected(value, "float")),
            NzValue::Decimal(v) => v.to_f32().ok_or_else(|| unexpected(value, "float")),
            other => Err(unexpected(other, "float")),
        }
    }
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        if let Some(decoded) = value.decoded {
            return Self::from_sql(decoded);
        }
        if let Some(number) = decode_float_dbos(&value) {
            return Ok(number? as f32);
        }
        Self::from_sql(&value.to_nz_value()?)
    }
}

impl FromSql for f64 {
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        match value {
            NzValue::Float4(v) => Ok(*v as f64),
            NzValue::Float8(v) => Ok(*v),
            NzValue::Int2(v) => Ok(*v as f64),
            NzValue::Int4(v) => Ok(*v as f64),
            NzValue::Int8(v) => Ok(*v as f64),
            NzValue::Numeric(v) | NzValue::Text(v) => v
                .trim()
                .parse::<f64>()
                .map_err(|_| unexpected(value, "float")),
            NzValue::Decimal(v) => v.to_f64().ok_or_else(|| unexpected(value, "float")),
            other => Err(unexpected(other, "float")),
        }
    }
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        if let Some(decoded) = value.decoded {
            return Self::from_sql(decoded);
        }
        if let Some(number) = decode_float_dbos(&value) {
            return number;
        }
        Self::from_sql(&value.to_nz_value()?)
    }
}

impl FromSql for Decimal {
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        match value {
            NzValue::Decimal(value) => Ok(*value),
            NzValue::Numeric(text) | NzValue::Text(text) => text
                .trim()
                .parse::<Decimal>()
                .map_err(|_| unexpected(value, "Decimal")),
            NzValue::Int2(value) => Ok(Decimal::from(*value)),
            NzValue::Int4(value) => Ok(Decimal::from(*value)),
            NzValue::Int8(value) => Ok(Decimal::from(*value)),
            other => Err(unexpected(other, "Decimal")),
        }
    }
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        if let Some(decoded) = value.decoded {
            return Self::from_sql(decoded);
        }
        Self::from_sql(&value.to_nz_value()?)
    }
}

#[cfg(feature = "chrono")]
impl FromSql for NaiveDate {
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        match value {
            NzValue::Date(text) => {
                NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(|_| unexpected(value, "DATE"))
            }
            other => Err(unexpected(other, "DATE")),
        }
    }
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        if let Some(decoded) = value.decoded {
            return Self::from_sql(decoded);
        }
        Self::from_sql(&value.to_nz_value()?)
    }
}

#[cfg(feature = "chrono")]
impl FromSql for NaiveTime {
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        match value {
            NzValue::Time(text) => parse_naive_time(text).map_err(|_| unexpected(value, "TIME")),
            other => Err(unexpected(other, "TIME")),
        }
    }
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        if let Some(decoded) = value.decoded {
            return Self::from_sql(decoded);
        }
        Self::from_sql(&value.to_nz_value()?)
    }
}

#[cfg(feature = "chrono")]
impl FromSql for NaiveDateTime {
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        match value {
            NzValue::Timestamp(text) => NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S"))
                .map_err(|_| unexpected(value, "TIMESTAMP")),
            other => Err(unexpected(other, "TIMESTAMP")),
        }
    }
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        if let Some(decoded) = value.decoded {
            return Self::from_sql(decoded);
        }
        Self::from_sql(&value.to_nz_value()?)
    }
}

#[cfg(feature = "chrono")]
fn parse_naive_time(text: &str) -> Result<NaiveTime, chrono::ParseError> {
    NaiveTime::parse_from_str(text, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(text, "%H:%M:%S"))
}

#[cfg(feature = "chrono")]
impl FromSql for NzTimeTz {
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        if let Some(decoded) = value.decoded {
            return Self::from_sql(decoded);
        }
        Self::from_sql(&value.to_nz_value()?)
    }
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        let NzValue::Timetz(text) = value else {
            return Err(unexpected(value, "TIMETZ"));
        };
        let Some(index) = text
            .char_indices()
            .skip(1)
            .find_map(|(i, c)| (c == '+' || c == '-').then_some(i))
        else {
            return Err(unexpected(value, "TIMETZ"));
        };
        let time = parse_naive_time(&text[..index]).map_err(|_| unexpected(value, "TIMETZ"))?;
        let offset_text = &text[index + 1..];
        let parts: Vec<&str> = offset_text.split(':').collect();
        if parts.is_empty() || parts.len() > 3 {
            return Err(unexpected(value, "TIMETZ"));
        }
        let numbers: Vec<i32> = parts
            .iter()
            .map(|part| part.parse::<i32>())
            .collect::<Result<_, _>>()
            .map_err(|_| unexpected(value, "TIMETZ"))?;
        let hours = numbers[0];
        let minutes = *numbers.get(1).unwrap_or(&0);
        let seconds = *numbers.get(2).unwrap_or(&0);
        if hours > 23 || !(0..60).contains(&minutes) || !(0..60).contains(&seconds) {
            return Err(unexpected(value, "TIMETZ"));
        }
        let sign = if text.as_bytes()[index] == b'-' {
            -1
        } else {
            1
        };
        let offset = FixedOffset::east_opt(sign * (hours * 3600 + minutes * 60 + seconds))
            .ok_or_else(|| unexpected(value, "TIMETZ"))?;
        Ok(NzTimeTz { time, offset })
    }
}

impl FromSql for String {
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        // Eager/compatibility rows carry the decoded value: single clone,
        // no intermediate `NzValue` allocation.
        if value.as_bytes().is_none() {
            if let Some(decoded) = value.decoded {
                return Self::from_sql(decoded);
            }
        }
        if let Some(bytes) = value.as_bytes() {
            let textual = value
                .dbos
                .map(|(d, _, _, i)| matches!(d.field_type[i], 15 | 16 | 21 | 25 | 26 | 30 | 32))
                .unwrap_or(matches!(
                    value.type_oid,
                    18 | 19 | 25 | 1042 | 1043 | 2522 | 2530
                ));
            if textual {
                return Ok(<&str as FromSqlRaw>::from_sql_raw(value)?.to_owned());
            }
            let _ = bytes;
        }
        Self::from_sql(&value.to_nz_value()?)
    }
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        match value {
            NzValue::Text(s)
            | NzValue::Numeric(s)
            | NzValue::Date(s)
            | NzValue::Time(s)
            | NzValue::Timetz(s)
            | NzValue::Timestamp(s)
            | NzValue::Interval(s) => Ok(s.clone()),
            NzValue::Decimal(value) => Ok(value.to_string()),
            other => Err(unexpected(other, "text")),
        }
    }
}

impl FromSql for Vec<u8> {
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        match value {
            NzValue::Bytea(bytes) => Ok(bytes.clone()),
            other => Err(unexpected(other, "bytea")),
        }
    }
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        if let Some(decoded) = value.decoded {
            return Self::from_sql(decoded);
        }
        Self::from_sql(&value.to_nz_value()?)
    }
}

impl<T: FromSql> FromSql for Option<T> {
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        if value.is_null() {
            Ok(None)
        } else {
            T::from_raw(value).map(Some)
        }
    }
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        if value.is_null() {
            Ok(None)
        } else {
            T::from_sql(value).map(Some)
        }
    }
}

impl<'a> FromSqlRaw<'a> for NzValue {
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
        value.to_nz_value()
    }
}

impl<'a> FromSqlRaw<'a> for bool {
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
        if let Some(decoded) = value.decoded {
            return <Self as FromSql>::from_sql(decoded);
        }
        if let Some(result) = decode_bool_dbos(&value) {
            return result;
        }
        let decoded = value.to_nz_value()?;
        <Self as FromSql>::from_sql(&decoded)
    }
}

macro_rules! from_sql_raw_int {
    ($($t:ty),*) => {
        $(
            impl<'a> FromSqlRaw<'a> for $t {
                fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
                    // Reuse the `FromSql::from_raw` fast paths (decoded
                    // borrow + direct binary decode) instead of cloning.
                    <Self as FromSql>::from_raw(value)
                }
            }
        )*
    };
}

from_sql_raw_int!(i16, i32, i64, u8, u16, u32);

impl<'a> FromSqlRaw<'a> for f32 {
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
        <Self as FromSql>::from_raw(value)
    }
}

impl<'a> FromSqlRaw<'a> for f64 {
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
        <Self as FromSql>::from_raw(value)
    }
}

macro_rules! from_sql_raw_decoded {
    ($($t:ty),*) => {
        $(
            impl<'a> FromSqlRaw<'a> for $t {
                fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
                    <Self as FromSql>::from_raw(value)
                }
            }
        )*
    };
}

from_sql_raw_decoded!(Decimal);
#[cfg(feature = "chrono")]
from_sql_raw_decoded!(NaiveDate, NaiveTime, NaiveDateTime, NzTimeTz);

impl<'a> FromSqlRaw<'a> for String {
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
        <Self as FromSql>::from_raw(value)
    }
}

impl<'a> FromSqlRaw<'a> for &'a str {
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
        if let Some(bytes) = value.as_bytes() {
            let text = std::str::from_utf8(bytes)
                .map_err(|e| NzError::Config(format!("invalid UTF-8 SQL text: {e}")))?;
            if let Some((descriptor, _, _, index)) = value.dbos {
                return match descriptor.field_type[index] {
                    15 => Ok(text.trim_end_matches(' ')),
                    25 | 26 => Ok(text.trim_end_matches('\0')),
                    16 | 21 | 30 | 32 => Ok(text),
                    _ => Err(NzError::Config("binary field is not text".into())),
                };
            }
            return Ok(text);
        }
        match value.decoded {
            Some(NzValue::Text(s))
            | Some(NzValue::Date(s))
            | Some(NzValue::Time(s))
            | Some(NzValue::Timetz(s))
            | Some(NzValue::Timestamp(s))
            | Some(NzValue::Interval(s)) => Ok(s),
            Some(NzValue::Numeric(s)) => Ok(s),
            Some(other) => Err(unexpected(other, "text")),
            None => Err(NzError::Config("cannot decode SQL NULL as text".into())),
        }
    }
}

impl<'a> FromSqlRaw<'a> for &'a [u8] {
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
        if let Some(bytes) = value.as_bytes() {
            return Ok(bytes);
        }
        match value.decoded {
            Some(NzValue::Bytea(bytes)) => Ok(bytes),
            _ => Err(NzError::Config(
                "field does not contain binary bytes".into(),
            )),
        }
    }
}

impl<'a> FromSqlRaw<'a> for Vec<u8> {
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
        <Self as FromSql>::from_raw(value)
    }
}

impl<'a, T> FromSqlRaw<'a> for Option<T>
where
    T: FromSqlRaw<'a>,
{
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
        if value.is_null() {
            Ok(None)
        } else {
            Ok(Some(T::from_sql_raw(value)?))
        }
    }
}

fn unexpected(value: &NzValue, expected: &str) -> NzError {
    NzError::Config(format!(
        "unexpected SQL type {expected:?} for value {value:?}"
    ))
}

// Parameter convenience conversions (the ToSql-analog surface).
//
// `ToSql` mirrors `tokio-postgres::types::ToSql`: any `&dyn ToSql` can be
// bound as `$1, $2, …` and is escaped client-side (Netezza simple-query path
// has no server-side bind — see `crate::params`).
pub trait ToSql: std::fmt::Debug + Sync {
    fn to_nz_value(&self) -> NzValue;

    /// Append this value as a Netezza SQL literal.
    ///
    /// The default preserves compatibility for downstream implementations.
    /// Built-in primitive and string types override it to append directly,
    /// avoiding an intermediate [`NzValue`].
    fn write_sql(&self, output: &mut String) -> Result<(), String> {
        crate::params::write_nz_value_sql(&self.to_nz_value(), output)
    }
}

impl ToSql for NzValue {
    fn to_nz_value(&self) -> NzValue {
        self.clone()
    }

    fn write_sql(&self, output: &mut String) -> Result<(), String> {
        crate::params::write_nz_value_sql(self, output)
    }
}

macro_rules! to_sql_integer {
    ($($t:ty),*) => {
        $(
            impl ToSql for $t {
                fn to_nz_value(&self) -> NzValue {
                    self.clone().into()
                }

                fn write_sql(&self, output: &mut String) -> Result<(), String> {
                    use std::fmt::Write as _;
                    write!(output, "{self}").expect("writing to String cannot fail");
                    Ok(())
                }
            }
        )*
    };
}

to_sql_integer!(i16, i32, i64);

impl ToSql for bool {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Bool(*self)
    }

    fn write_sql(&self, output: &mut String) -> Result<(), String> {
        output.push_str(if *self { "'t'" } else { "'f'" });
        Ok(())
    }
}

impl ToSql for f32 {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Float4(*self)
    }

    fn write_sql(&self, output: &mut String) -> Result<(), String> {
        let value = *self as f64;
        if !value.is_finite() {
            return Err(format!("Cannot bind non-finite number: {value}"));
        }
        use std::fmt::Write as _;
        write!(output, "{value}").expect("writing to String cannot fail");
        Ok(())
    }
}

impl ToSql for f64 {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Float8(*self)
    }

    fn write_sql(&self, output: &mut String) -> Result<(), String> {
        if !self.is_finite() {
            return Err(format!("Cannot bind non-finite number: {self}"));
        }
        use std::fmt::Write as _;
        write!(output, "{self}").expect("writing to String cannot fail");
        Ok(())
    }
}

impl ToSql for Decimal {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Decimal(*self)
    }

    fn write_sql(&self, output: &mut String) -> Result<(), String> {
        use std::fmt::Write as _;
        write!(output, "{self}").expect("writing to String cannot fail");
        Ok(())
    }
}

impl ToSql for Vec<u8> {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Bytea(self.clone())
    }

    fn write_sql(&self, _output: &mut String) -> Result<(), String> {
        Err("Binary SQL parameters are unsupported; use an external-table reader".into())
    }
}

impl ToSql for String {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Text(self.clone())
    }

    fn write_sql(&self, output: &mut String) -> Result<(), String> {
        crate::params::write_text_literal(self, output)
    }
}

impl ToSql for i8 {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Int2(*self as i16)
    }

    fn write_sql(&self, output: &mut String) -> Result<(), String> {
        use std::fmt::Write as _;
        write!(output, "{self}").expect("writing to String cannot fail");
        Ok(())
    }
}
impl ToSql for u8 {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Int2(*self as i16)
    }

    fn write_sql(&self, output: &mut String) -> Result<(), String> {
        use std::fmt::Write as _;
        write!(output, "{}", *self as i16).expect("writing to String cannot fail");
        Ok(())
    }
}
impl ToSql for u16 {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Int4(*self as i32)
    }

    fn write_sql(&self, output: &mut String) -> Result<(), String> {
        use std::fmt::Write as _;
        write!(output, "{}", *self as i32).expect("writing to String cannot fail");
        Ok(())
    }
}
impl ToSql for u32 {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Int8(*self as i64)
    }

    fn write_sql(&self, output: &mut String) -> Result<(), String> {
        use std::fmt::Write as _;
        write!(output, "{}", *self as i64).expect("writing to String cannot fail");
        Ok(())
    }
}

impl ToSql for &str {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Text((*self).into())
    }

    fn write_sql(&self, output: &mut String) -> Result<(), String> {
        crate::params::write_text_literal(self, output)
    }
}
impl ToSql for str {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Text(self.into())
    }

    fn write_sql(&self, output: &mut String) -> Result<(), String> {
        crate::params::write_text_literal(self, output)
    }
}
impl ToSql for &[u8] {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Bytea(self.to_vec())
    }

    fn write_sql(&self, _output: &mut String) -> Result<(), String> {
        Err("Binary SQL parameters are unsupported; use an external-table reader".into())
    }
}
impl<T: ToSql> ToSql for Option<T> {
    fn to_nz_value(&self) -> NzValue {
        match self {
            Some(v) => v.to_nz_value(),
            None => NzValue::Null,
        }
    }

    fn write_sql(&self, output: &mut String) -> Result<(), String> {
        match self {
            Some(value) => value.write_sql(output),
            None => {
                output.push_str("NULL");
                Ok(())
            }
        }
    }
}
impl<T: ToSql> ToSql for &T {
    fn to_nz_value(&self) -> NzValue {
        (*self).to_nz_value()
    }

    fn write_sql(&self, output: &mut String) -> Result<(), String> {
        (*self).write_sql(output)
    }
}

/// Collect `&[&dyn ToSql]` (tokio-postgres style) into owned [`NzValue`]s.
pub fn to_nz_values(params: &[&dyn ToSql]) -> Vec<NzValue> {
    params.iter().map(|p| p.to_nz_value()).collect()
}

impl From<bool> for NzValue {
    fn from(v: bool) -> Self {
        NzValue::Bool(v)
    }
}
impl From<i16> for NzValue {
    fn from(v: i16) -> Self {
        NzValue::Int2(v)
    }
}
impl From<i8> for NzValue {
    fn from(v: i8) -> Self {
        NzValue::Int2(v as i16)
    }
}
impl From<u8> for NzValue {
    fn from(v: u8) -> Self {
        NzValue::Int2(v as i16)
    }
}
impl From<u16> for NzValue {
    fn from(v: u16) -> Self {
        NzValue::Int4(v as i32)
    }
}
impl From<u32> for NzValue {
    fn from(v: u32) -> Self {
        NzValue::Int8(v as i64)
    }
}
impl From<i32> for NzValue {
    fn from(v: i32) -> Self {
        NzValue::Int4(v)
    }
}
impl From<i64> for NzValue {
    fn from(v: i64) -> Self {
        NzValue::Int8(v)
    }
}
impl From<f32> for NzValue {
    fn from(v: f32) -> Self {
        NzValue::Float4(v)
    }
}
impl From<f64> for NzValue {
    fn from(v: f64) -> Self {
        NzValue::Float8(v)
    }
}
impl From<Decimal> for NzValue {
    fn from(v: Decimal) -> Self {
        NzValue::Decimal(v)
    }
}
impl From<String> for NzValue {
    fn from(v: String) -> Self {
        NzValue::Text(v)
    }
}
impl From<&str> for NzValue {
    fn from(v: &str) -> Self {
        NzValue::Text(v.into())
    }
}
impl From<Vec<u8>> for NzValue {
    fn from(v: Vec<u8>) -> Self {
        NzValue::Bytea(v)
    }
}
impl<T: Into<NzValue>> From<Option<T>> for NzValue {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(x) => x.into(),
            None => NzValue::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_timestamp_drops_sub_ms() {
        let v = NzValue::Timestamp("2024-12-12 10:30:00.123456".into());
        assert_eq!(v.to_node_canonical(), "2024-12-12T10:30:00.123Z");
        let v = NzValue::Timestamp("2024-12-12 10:30:00".into());
        assert_eq!(v.to_node_canonical(), "2024-12-12T10:30:00.000Z");
    }

    #[test]
    fn canonical_date_and_null() {
        assert_eq!(
            NzValue::Date("2024-02-29".into()).to_node_canonical(),
            "2024-02-29T00:00:00.000Z"
        );
        assert_eq!(NzValue::Null.to_node_canonical(), "null");
    }

    #[test]
    fn from_sql_typed_access() {
        assert_eq!(
            i32::from_sql_raw(RawValue::from_decoded(&NzValue::Int4(7), 23, -1, 1)).unwrap(),
            7
        );
        assert_eq!(
            String::from_sql_raw(RawValue::from_decoded(
                &NzValue::Text("x".into()),
                25,
                -1,
                0
            ))
            .unwrap(),
            "x"
        );
        assert_eq!(
            Option::<i32>::from_sql_raw(RawValue::from_decoded(&NzValue::Null, 23, -1, 1)).unwrap(),
            None
        );
        assert_eq!(
            Option::<i32>::from_sql_raw(RawValue::from_decoded(&NzValue::Int4(3), 23, -1, 1))
                .unwrap(),
            Some(3)
        );
        assert!(i32::from_sql_raw(RawValue::from_decoded(
            &NzValue::Text("no".into()),
            25,
            -1,
            0
        ))
        .is_err());
        // Numeric text is readable as String and f64 where finite.
        assert_eq!(
            String::from_sql_raw(RawValue::from_decoded(
                &NzValue::Numeric("3.1400".into()),
                1700,
                -1,
                0
            ))
            .unwrap(),
            "3.1400"
        );
        let decimal = "3.1400".parse::<Decimal>().unwrap();
        assert_eq!(
            String::from_sql_raw(RawValue::from_decoded(
                &NzValue::Decimal(decimal),
                1700,
                -1,
                1
            ))
            .unwrap(),
            "3.1400"
        );
        assert_eq!(
            f64::from_sql_raw(RawValue::from_decoded(
                &NzValue::Decimal(decimal),
                1700,
                -1,
                1
            ))
            .unwrap(),
            3.0 + 0.14
        );
        assert!(i32::from_sql_raw(RawValue::from_decoded(
            &NzValue::Decimal("3.14".parse().unwrap()),
            1700,
            -1,
            1
        ))
        .is_err());
        assert_eq!(
            f64::from_sql_raw(RawValue::from_decoded(&NzValue::Int8(9), 20, -1, 1)).unwrap(),
            9.0
        );
        assert!(
            i64::from_sql_raw(RawValue::from_decoded(&NzValue::Int8(i64::MAX), 20, -1, 1)).is_ok()
        );
        assert!(
            i32::from_sql_raw(RawValue::from_decoded(&NzValue::Int8(i64::MAX), 20, -1, 1)).is_err()
        );
    }

    #[test]
    fn legacy_from_sql_interface_remains_available() {
        assert_eq!(i32::from_sql(&NzValue::Int4(7)).unwrap(), 7);

        struct LegacyMarker;

        impl FromSql for LegacyMarker {
            fn from_sql(value: &NzValue) -> NzResult<Self> {
                if matches!(value, NzValue::Text(text) if text == "legacy") {
                    Ok(Self)
                } else {
                    Err(NzError::Config("unexpected legacy test value".into()))
                }
            }
        }

        assert!(LegacyMarker::from_sql(&NzValue::Text("legacy".into())).is_ok());
    }

    #[test]
    fn display_forms() {
        assert_eq!(NzValue::Null.to_display_string(), "NULL");
        assert_eq!(NzValue::Bool(false).to_display_string(), "false");
        assert_eq!(NzValue::Float8(1.5).to_display_string(), "1.5");
        assert_eq!(
            NzValue::Decimal("3.1400".parse().unwrap()).to_display_string(),
            "3.1400"
        );
        assert_eq!(
            NzValue::Bytea(vec![0xde, 0xad]).to_display_string(),
            "0xdead"
        );
    }

    #[test]
    fn to_sql_wraps_values() {
        assert_eq!(42i32.to_nz_value(), NzValue::Int4(42));
        assert_eq!("text".to_nz_value(), NzValue::Text("text".into()));
        assert_eq!(Option::<i32>::None.to_nz_value(), NzValue::Null);
        assert_eq!(Some(7i64).to_nz_value(), NzValue::Int8(7));
        assert_eq!(vec![1u8, 2].to_nz_value(), NzValue::Bytea(vec![1, 2]));
    }

    #[test]
    fn from_optionals_and_conversions() {
        assert_eq!(NzValue::from(Some(5i32)), NzValue::Int4(5));
        assert_eq!(NzValue::from(Option::<i32>::None), NzValue::Null);
        assert_eq!(NzValue::from(true), NzValue::Bool(true));
    }

    #[test]
    fn to_sql_writes_numeric_literals_without_changing_formatting() {
        let mut output = String::new();
        42i32.write_sql(&mut output).unwrap();
        assert_eq!(output, "42");

        output.clear();
        let single = 0.1f32;
        single.write_sql(&mut output).unwrap();
        assert_eq!(output, NzValue::Float4(single).to_display_string());

        output.clear();
        let double = 1.25f64;
        double.write_sql(&mut output).unwrap();
        assert_eq!(output, NzValue::Float8(double).to_display_string());
        assert!(f64::INFINITY.write_sql(&mut output).is_err());

        output.clear();
        let decimal = "123.4500".parse::<Decimal>().unwrap();
        decimal.write_sql(&mut output).unwrap();
        assert_eq!(output, "123.4500");

        let bytes = vec![1, 2, 3];
        assert!(bytes.write_sql(&mut output).is_err());
    }
}
