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

/// Checked text decoding. Malformed scalar values never become NULL or false.
pub fn try_parse_text_value(raw: &str, type_oid: i32, type_mod: i32) -> crate::NzResult<NzValue> {
    let invalid = || crate::NzError::Protocol(format!("invalid text scalar for OID {type_oid}"));
    Ok(match type_oid {
        16 => {
            let text = raw.trim();
            if parse_bool_text(text) {
                NzValue::Bool(true)
            } else if text == "0"
                || ["f", "false", "no", "n"]
                    .iter()
                    .any(|value| text.eq_ignore_ascii_case(value))
            {
                NzValue::Bool(false)
            } else {
                return Err(invalid());
            }
        }
        21 | 2500 => NzValue::Int2(raw.trim().parse().map_err(|_| invalid())?),
        23 => NzValue::Int4(raw.trim().parse().map_err(|_| invalid())?),
        26 | 28 => NzValue::Int8(i64::from(raw.trim().parse::<u32>().map_err(|_| invalid())?)),
        20 => NzValue::Int8(raw.trim().parse().map_err(|_| invalid())?),
        700 => NzValue::Float4(raw.trim().parse().map_err(|_| invalid())?),
        701 => NzValue::Float8(raw.trim().parse().map_err(|_| invalid())?),
        1700 => {
            raw.trim()
                .parse::<crate::NzNumeric>()
                .map_err(|_| invalid())?;
            parse_text_value(raw, type_oid, type_mod)
        }
        // Preserve the exact wire text; typed temporal getters validate it.
        1083 => NzValue::Time(raw.trim().to_owned()),
        _ => parse_text_value(raw, type_oid, type_mod),
    })
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
/// (len(i32 BE, includes self) + bytes)`. A cleared bitmap bit means SQL NULL;
/// a present cell whose `vlen < 4` is a protocol error. A present cell with
/// `vlen == 4` is a genuine empty string; this distinction is required for
/// parity with the C# reader.
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

/// Validate text-row framing without retaining one range per column.
pub(crate) fn validate_text_row(
    data: &[u8],
    columns: &[crate::tuple_desc::ColumnDesc],
) -> Result<(), String> {
    let bitmap_len = columns.len().div_ceil(8);
    if data.len() < bitmap_len {
        return Err("Invalid DataRow payload: null bitmap is truncated".into());
    }
    let mut index = bitmap_len;
    for col_no in 0..columns.len() {
        let byte = data[col_no / 8];
        let bit = 7 - (col_no % 8);
        if byte & (1 << bit) == 0 {
            continue;
        }
        if index.checked_add(4).is_none_or(|end| end > data.len()) {
            return Err(format!(
                "Invalid DataRow payload: column {col_no} length is truncated"
            ));
        }
        let encoded = i32::from_be_bytes(data[index..index + 4].try_into().unwrap());
        index += 4;
        if encoded < 4 {
            return Err(format!(
                "Invalid DataRow payload: column {col_no} length is smaller than its prefix"
            ));
        }
        let value_len = (encoded - 4) as usize;
        index = index
            .checked_add(value_len)
            .filter(|end| *end <= data.len())
            .ok_or_else(|| {
                format!("Invalid DataRow payload: column {col_no} value length is invalid")
            })?;
    }
    Ok(())
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
            return Err(format!(
                "Invalid DataRow payload: column {col_no} length is smaller than its prefix"
            ));
        }
        let actual = (vlen - 4) as usize;
        if idx + actual > data.len() {
            return Err(format!(
                "Invalid DataRow payload: column {col_no} value length is invalid"
            ));
        }
        if actual == 0 {
            row.push(
                try_parse_text_value("", col.type_oid, col.type_mod)
                    .map_err(|error| error.to_string())?,
            );
            continue;
        }
        let text = std::str::from_utf8(&data[idx..idx + actual])
            .map_err(|_| "Invalid UTF-8 DataRow field".to_owned())?;
        idx += actual;
        row.push(
            try_parse_text_value(text, col.type_oid, col.type_mod)
                .map_err(|error| error.to_string())?,
        );
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

    /// `validate_text_row` guards the lazy row path; it must accept exactly the
    /// payloads the eager parser accepts, at every bitmap byte boundary and for
    /// every truncation of the payload.
    #[test]
    fn validate_text_row_agrees_with_eager_parser_at_bitmap_boundaries() {
        use crate::tuple_desc::ColumnDesc;
        for n in [7usize, 8, 9, 15, 16, 17, 31, 32, 33] {
            let columns: Vec<ColumnDesc> = (0..n)
                .map(|i| ColumnDesc {
                    name: format!("c{i}"),
                    type_oid: 1043,
                    type_len: -1,
                    type_mod: -1,
                    format: 0,
                })
                .collect();
            let mut masks: Vec<Vec<bool>> = vec![vec![true; n], vec![false; n]];
            masks.push((0..n).map(|i| i % 2 == 0).collect());
            for i in 0..n {
                let mut mask = vec![true; n];
                mask[i] = false;
                masks.push(mask);
                let mut mask = vec![false; n];
                mask[i] = true;
                masks.push(mask);
            }
            for mask in masks {
                let mut payload = vec![0u8; n.div_ceil(8)];
                for (i, present) in mask.iter().enumerate() {
                    if *present {
                        payload[i / 8] |= 1 << (7 - i % 8);
                    }
                }
                for (i, present) in mask.iter().enumerate() {
                    if *present {
                        let cell = "v".repeat(i % 4 + 1);
                        payload.extend_from_slice(&((cell.len() + 4) as i32).to_be_bytes());
                        payload.extend_from_slice(cell.as_bytes());
                    }
                }
                assert!(validate_text_row(&payload, &columns).is_ok());
                for cut in 0..payload.len() {
                    let truncated = &payload[..cut];
                    assert_eq!(
                        validate_text_row(truncated, &columns).is_ok(),
                        parse_text_data_row(truncated, &columns).is_ok(),
                        "n={n} mask={mask:?} cut={cut}"
                    );
                }
            }
        }
    }
}
