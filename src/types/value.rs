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
use rust_decimal::{prelude::ToPrimitive, Decimal};

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
                let mut out = String::with_capacity(2 + b.len() * 2);
                out.push_str("0x");
                for byte in b {
                    out.push_str(&format!("{byte:02x}"));
                }
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
            NzValue::Bytea(b) => format!(
                "E'\\\\x{}'",
                b.iter().map(|x| format!("{x:02x}")).collect::<String>()
            ),
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

/// A borrowed value from a result row.
///
/// The payload is kept in the row's owned frame buffer.  The optional decoded
/// value is used by compatibility rows constructed from an already materialized
/// [`NzValue`].  User implementations can inspect `as_bytes()` and the wire
/// metadata without forcing conversion to the compatibility enum.
#[derive(Debug, Clone, Copy)]
pub struct RawValue<'a> {
    bytes: Option<&'a [u8]>,
    decoded: Option<&'a NzValue>,
    type_oid: i32,
    type_mod: i32,
    format: u8,
}

impl<'a> RawValue<'a> {
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

    pub fn type_oid(&self) -> i32 {
        self.type_oid
    }

    pub fn type_mod(&self) -> i32 {
        self.type_mod
    }

    pub fn format(&self) -> u8 {
        self.format
    }

    fn to_nz_value(self) -> NzResult<NzValue> {
        if let Some(value) = self.decoded {
            return Ok(value.clone());
        }
        let bytes = self
            .bytes
            .ok_or_else(|| NzError::Config("cannot decode SQL NULL as a value".into()))?;
        if self.format != 0 {
            return Err(NzError::Unsupported(
                "direct binary FromSql decoding requires a typed Row context".into(),
            ));
        }
        let text = String::from_utf8_lossy(bytes);
        Ok(crate::types::text::parse_text_value(
            &text,
            self.type_oid,
            self.type_mod,
        ))
    }
}

/// Typed extraction from an already decoded Netezza value.
///
/// This is the original public typed-access interface. It intentionally
/// remains borrowed from [`NzValue`] so downstream implementations written
/// against earlier releases continue to compile.
pub trait FromSql: Sized {
    fn from_sql(value: &NzValue) -> NzResult<Self>;
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
}

impl FromSql for String {
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
}

macro_rules! from_sql_option {
    ($($t:ty),*) => {
        $(
            impl FromSql for Option<$t> {
                fn from_sql(value: &NzValue) -> NzResult<Self> {
                    match value {
                        NzValue::Null => Ok(None),
                        other => Ok(Some(<$t as FromSql>::from_sql(other)?)),
                    }
                }
            }
        )*
    };
}

from_sql_option!(NzValue, bool, i16, i32, i64, f32, f64, String, Vec<u8>);

impl<'a> FromSqlRaw<'a> for NzValue {
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
        value.to_nz_value()
    }
}

impl<'a> FromSqlRaw<'a> for bool {
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
        let decoded = value.to_nz_value()?;
        <Self as FromSql>::from_sql(&decoded)
    }
}

macro_rules! from_sql_raw_int {
    ($($t:ty),*) => {
        $(
            impl<'a> FromSqlRaw<'a> for $t {
                fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
                    let decoded = value.to_nz_value()?;
                    <Self as FromSql>::from_sql(&decoded)
                }
            }
        )*
    };
}

from_sql_raw_int!(i16, i32, i64, u8, u16, u32);

impl<'a> FromSqlRaw<'a> for f32 {
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
        let decoded = value.to_nz_value()?;
        <Self as FromSql>::from_sql(&decoded)
    }
}

impl<'a> FromSqlRaw<'a> for f64 {
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
        let decoded = value.to_nz_value()?;
        <Self as FromSql>::from_sql(&decoded)
    }
}

impl<'a> FromSqlRaw<'a> for String {
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
        let decoded = value.to_nz_value()?;
        <Self as FromSql>::from_sql(&decoded)
    }
}

impl<'a> FromSqlRaw<'a> for &'a str {
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
        if let Some(bytes) = value.as_bytes() {
            return std::str::from_utf8(bytes)
                .map_err(|e| NzError::Config(format!("invalid UTF-8 SQL text: {e}")));
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

impl<'a> FromSqlRaw<'a> for Vec<u8> {
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
        let decoded = value.to_nz_value()?;
        <Self as FromSql>::from_sql(&decoded)
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
pub trait ToSql: std::fmt::Debug {
    fn to_nz_value(&self) -> NzValue;
}

impl ToSql for NzValue {
    fn to_nz_value(&self) -> NzValue {
        self.clone()
    }
}

macro_rules! to_sql_direct {
    ($($t:ty),*) => {
        $(
            impl ToSql for $t
            where
                $t: Into<NzValue> + Clone + std::fmt::Debug,
            {
                fn to_nz_value(&self) -> NzValue {
                    self.clone().into()
                }
            }
        )*
    };
}

to_sql_direct!(bool, i16, i32, i64, f32, f64, Decimal, String, Vec<u8>);

impl ToSql for i8 {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Int2(*self as i16)
    }
}
impl ToSql for u8 {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Int2(*self as i16)
    }
}
impl ToSql for u16 {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Int4(*self as i32)
    }
}
impl ToSql for u32 {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Int8(*self as i64)
    }
}

impl ToSql for &str {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Text((*self).into())
    }
}
impl ToSql for str {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Text(self.into())
    }
}
impl ToSql for &[u8] {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Bytea(self.to_vec())
    }
}
impl<T: ToSql> ToSql for Option<T> {
    fn to_nz_value(&self) -> NzValue {
        match self {
            Some(v) => v.to_nz_value(),
            None => NzValue::Null,
        }
    }
}
impl<T: ToSql> ToSql for &T {
    fn to_nz_value(&self) -> NzValue {
        (*self).to_nz_value()
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
}
