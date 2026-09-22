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

//! Text-path (`T`/`D`) value parsing — port of C# `NzConnectionHelpers.cs`
//! plus the Node driver `TypeConversions.createTextValueParser`.
//!
//! The binary path (`X`/`Y`) is handled by [`crate::tuple_desc::DbosTupleDesc`];
//! this module covers the UTF-8 text rows the appliance sends for system
//! catalogs, `SELECT` literals and anything without a binary descriptor.
//!
//! Mapping (mirrors both reference drivers):
//! - 16 BOOL → [`NzValue::Bool`] (`t`/`true`/`1`)
//! - 21 INT2 / 2500 BYTEINT → [`NzValue::Int2`]
//! - 23 INT4 / 26 OID / 28 XID → [`NzValue::Int4`]
//! - 20 INT8 → [`NzValue::Int8`]
//! - 700 REAL → [`NzValue::Float4`], 701 DOUBLE → [`NzValue::Float8`]
//! - 1700 NUMERIC → [`NzValue::Float8`] when it round-trips and precision ≤ 15,
//!   [`NzValue::Decimal`] when its exact coefficient fits `rust_decimal`, else
//!   [`NzValue::Numeric`] (exact decimal text)
//! - 1082 DATE → [`NzValue::Date`], 1083 TIME → [`NzValue::Time`],
//!   1114/1184/702 TIMESTAMP family → [`NzValue::Timestamp`],
//!   1266 TIMETZ → [`NzValue::Timetz`], 1186 INTERVAL → [`NzValue::Interval`]
//! - 17 BYTEA → [`NzValue::Bytea`] (hex `\\x…` decoded, else raw UTF-8 bytes)
//! - everything else (CHAR/VARCHAR/TEXT/NAME/UUID/…) → [`NzValue::Text`]

use crate::types::datetime::normalize_time_text;
use crate::types::numeric::{parse_numeric_text, NumericDecoded};
use crate::types::value::NzValue;
use std::ops::Range;

/// Build a Netezza simple-query (`P`) packet: `'P' + cmdNum(BE) + sql + NUL`.
///
/// Port of the Node driver `buildSimpleQueryPacket` (C# `Core.IPack` equivalent).
pub fn build_simple_query_packet(sql: &str, command_number: i32) -> Vec<u8> {
    let bytes = sql.as_bytes();
    let mut buf = Vec::with_capacity(1 + 4 + bytes.len() + 1);
    buf.push(b'P');
    buf.extend_from_slice(&command_number.to_be_bytes());
    buf.extend_from_slice(bytes);
    buf.push(0);
    buf
}

/// Parse one text cell into an [`NzValue`].
pub fn parse_text_value(raw: &str, type_oid: i32, type_mod: i32) -> NzValue {
    match type_oid {
        16 => NzValue::Bool(parse_bool_text(raw)),
        21 | 2500 => match raw.trim().parse::<i16>() {
            Ok(v) => NzValue::Int2(v),
            Err(_) => NzValue::Text(raw.to_string()),
        },
        23 | 26 | 28 => match raw.trim().parse::<i32>() {
            Ok(v) => NzValue::Int4(v),
            Err(_) => NzValue::Text(raw.to_string()),
        },
        20 => match raw.trim().parse::<i64>() {
            Ok(v) => NzValue::Int8(v),
            Err(_) => NzValue::Numeric(raw.trim().to_string()),
        },
        700 => match raw.trim().parse::<f32>() {
            Ok(v) => NzValue::Float4(v),
            Err(_) => NzValue::Text(raw.to_string()),
        },
        701 => match raw.trim().parse::<f64>() {
            Ok(v) if v.is_finite() => NzValue::Float8(v),
            _ => NzValue::Text(raw.to_string()),
        },
        1700 => match parse_numeric_text(raw, type_mod) {
            NumericDecoded::Number(n) => NzValue::Float8(n),
            NumericDecoded::Decimal(value) => NzValue::Decimal(value),
            NumericDecoded::Exact(s) => NzValue::Numeric(s),
        },
        1082 => NzValue::Date(raw.trim().to_string()),
        1083 => NzValue::Time(normalize_time_text(raw)),
        1114 | 1184 | 702 => NzValue::Timestamp(raw.trim().to_string()),
        1266 => NzValue::Timetz(raw.trim().to_string()),
        1186 => NzValue::Interval(raw.trim().to_string()),
        17 => NzValue::Bytea(decode_bytea_text(raw)),
        _ => NzValue::Text(raw.to_string()),
    }
}

fn parse_bool_text(raw: &str) -> bool {
    let t = raw.trim();
    t.eq_ignore_ascii_case("t")
        || t.eq_ignore_ascii_case("true")
        || t == "1"
        || t.eq_ignore_ascii_case("yes")
        || t.eq_ignore_ascii_case("y")
}

/// Decode a text-path BYTEA cell.
///
/// Netezza/Postgres text bytea arrives as `\\x<hex>` (modern) or as escaped
/// octal/as-is text (legacy). Hex is decoded; anything else is kept as the
/// raw UTF-8 bytes so no data is lost.
pub fn decode_bytea_text(raw: &str) -> Vec<u8> {
    let t = raw.trim();
    let hex = t.strip_prefix("\\x").or_else(|| t.strip_prefix("\\X"));
    if let Some(h) = hex {
        if h.len() % 2 == 0 && !h.is_empty() && h.bytes().all(|b| b.is_ascii_hexdigit()) {
            let mut out = Vec::with_capacity(h.len() / 2);
            let bytes = h.as_bytes();
            let mut i = 0;
            while i < bytes.len() {
                let hi = (bytes[i] as char).to_digit(16).unwrap_or(0);
                let lo = (bytes[i + 1] as char).to_digit(16).unwrap_or(0);
                out.push(((hi << 4) | lo) as u8);
                i += 2;
            }
            return out;
        }
    }
    t.as_bytes().to_vec()
}

/// Parse one text-protocol DataRow payload (`D`) into row values.
///
/// Layout (C# `HandleDataRow`, Node `_parseDataRow`): `bitmap + per-column
/// (len(i32 BE, includes self) + bytes)`. A cleared bitmap bit or a `vlen < 4`
/// length means SQL NULL. A present cell with `vlen == 4` is a genuine empty
/// string; this distinction is required for parity with the C# reader.
pub fn parse_text_data_row(
    data: &[u8],
    columns: &[crate::tuple_desc::ColumnDesc],
) -> Result<Vec<NzValue>, String> {
    let n = columns.len();
    let bitmap_len = n.div_ceil(8);
    if data.len() < bitmap_len {
        return Err("Invalid DataRow payload: null bitmap is truncated".into());
    }
    let mut row = Vec::with_capacity(n);
    parse_text_data_row_into(data, columns, &mut row)?;
    Ok(row)
}

/// Locate text fields without converting them to `NzValue`. `None` marks a
/// SQL NULL; an empty range is a present empty string.
pub(crate) fn text_row_layout(
    data: &[u8],
    columns: &[crate::tuple_desc::ColumnDesc],
) -> Result<Vec<Option<Range<usize>>>, String> {
    let n = columns.len();
    let bitmap_len = n.div_ceil(8);
    if data.len() < bitmap_len {
        return Err("Invalid DataRow payload: null bitmap is truncated".into());
    }
    let mut layout = Vec::with_capacity(n);
    let mut idx = bitmap_len;
    for col_no in 0..n {
        let byte = data[col_no / 8];
        let bit = 7 - (col_no % 8);
        if byte & (1 << bit) == 0 {
            layout.push(None);
            continue;
        }
        if idx + 4 > data.len() {
            return Err(format!(
                "Invalid DataRow payload: column {col_no} length is truncated"
            ));
        }
        let vlen = i32::from_be_bytes(data[idx..idx + 4].try_into().unwrap());
        idx += 4;
        if vlen < 4 {
            layout.push(None);
            continue;
        }
        let actual = (vlen - 4) as usize;
        if idx.checked_add(actual).is_none_or(|end| end > data.len()) {
            return Err(format!(
                "Invalid DataRow payload: column {col_no} value length is invalid"
            ));
        }
        layout.push(Some(idx..idx + actual));
        idx += actual;
    }
    Ok(layout)
}

/// Parse a text DataRow into a reusable value vector.
pub fn parse_text_data_row_into(
    data: &[u8],
    columns: &[crate::tuple_desc::ColumnDesc],
    row: &mut Vec<NzValue>,
) -> Result<(), String> {
    let n = columns.len();
    let bitmap_len = n.div_ceil(8);
    if data.len() < bitmap_len {
        return Err("Invalid DataRow payload: null bitmap is truncated".into());
    }
    row.clear();
    if row.capacity() < n {
        row.reserve(n - row.capacity());
    }
    let mut idx = bitmap_len;
    for (col_no, col) in columns.iter().enumerate() {
        let byte = data[col_no / 8];
        let bit = 7 - (col_no % 8);
        if byte & (1 << bit) == 0 {
            row.push(NzValue::Null);
            continue;
        }
        if idx + 4 > data.len() {
            return Err(format!(
                "Invalid DataRow payload: column {col_no} length is truncated"
            ));
        }
        let vlen = i32::from_be_bytes(data[idx..idx + 4].try_into().unwrap());
        idx += 4;
        if vlen < 4 {
            row.push(NzValue::Null);
            continue;
        }
        let actual = (vlen - 4) as usize;
        if idx + actual > data.len() {
            return Err(format!(
                "Invalid DataRow payload: column {col_no} value length is invalid"
            ));
        }
        if actual == 0 {
            row.push(parse_text_value("", col.type_oid, col.type_mod));
            continue;
        }
        let text = String::from_utf8_lossy(&data[idx..idx + actual]);
        idx += actual;
        row.push(parse_text_value(&text, col.type_oid, col.type_mod));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_packet_layout() {
        let p = build_simple_query_packet("SELECT 1", 7);
        assert_eq!(p[0], b'P');
        assert_eq!(&p[1..5], &7i32.to_be_bytes());
        assert_eq!(&p[5..13], b"SELECT 1");
        assert_eq!(p[p.len() - 1], 0);
    }

    #[test]
    fn parses_scalars() {
        assert_eq!(parse_text_value("t", 16, -1), NzValue::Bool(true));
        assert_eq!(parse_text_value("f", 16, -1), NzValue::Bool(false));
        assert_eq!(parse_text_value("42", 23, -1), NzValue::Int4(42));
        assert_eq!(parse_text_value("-5", 21, -1), NzValue::Int2(-5));
        assert_eq!(
            parse_text_value("9223372036854775807", 20, -1),
            NzValue::Int8(i64::MAX)
        );
        assert_eq!(
            parse_text_value("2024-12-12", 1082, -1),
            NzValue::Date("2024-12-12".into())
        );
        assert_eq!(
            parse_text_value("12:01:00", 1083, -1),
            NzValue::Time("12:01:00".into())
        );
        // NUMERIC(10,4) "3.1400": trailing zeros break the f64 round-trip, so
        // the exact Decimal representation preserves the scale.
        assert_eq!(
            parse_text_value("3.1400", 1700, (10 << 16) | (4 + 16)),
            NzValue::Decimal("3.1400".parse().unwrap())
        );
        assert_eq!(
            parse_text_value("3.14", 1700, (10 << 16) | (4 + 16)),
            NzValue::Float8(3.0 + 0.14)
        );
        // high precision stays exact
        assert!(matches!(
            parse_text_value("923281625142643375987.43950777", 1700, -1),
            NzValue::Numeric(_)
        ));
    }

    #[test]
    fn bytea_hex_decoded() {
        assert_eq!(decode_bytea_text("\\xdead"), vec![0xde, 0xad]);
        assert_eq!(
            parse_text_value("\\xdead", 17, -1),
            NzValue::Bytea(vec![0xde, 0xad])
        );
    }

    #[test]
    fn data_row_bitmap_and_lengths() {
        use crate::tuple_desc::ColumnDesc;
        let cols = vec![
            ColumnDesc {
                name: "a".into(),
                type_oid: 23,
                type_len: 4,
                type_mod: -1,
                format: 0,
            },
            ColumnDesc {
                name: "b".into(),
                type_oid: 1043,
                type_len: -1,
                type_mod: -1,
                format: 0,
            },
        ];
        // bitmap: col0 present (bit7), col1 null (bit6 clear) → 0b1000_0000
        let mut payload = vec![0x80];
        payload.extend_from_slice(&6i32.to_be_bytes()); // vlen = 4 + 2
        payload.extend_from_slice(b"42");
        let row = parse_text_data_row(&payload, &cols).unwrap();
        assert_eq!(row, vec![NzValue::Int4(42), NzValue::Null]);
    }

    #[test]
    fn present_zero_length_text_is_not_sql_null() {
        use crate::tuple_desc::ColumnDesc;
        let cols = vec![ColumnDesc {
            name: "text".into(),
            type_oid: 1043,
            type_len: -1,
            type_mod: -1,
            format: 0,
        }];
        let mut payload = vec![0x80];
        payload.extend_from_slice(&4i32.to_be_bytes());
        assert_eq!(
            parse_text_data_row(&payload, &cols).unwrap(),
            vec![NzValue::Text(String::new())]
        );
    }
}
