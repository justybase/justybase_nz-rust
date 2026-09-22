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

//! Streaming result reader — port of C# `NzDataReader.cs` / Node `NzDataReader.ts`.
//!
//! The Rust connection buffers the full response (sync driver), so the reader
//! navigates the buffered [`QueryResult`]: [`NzDataReader::read`] advances
//! within the current result set, [`NzDataReader::next_result`] moves to the
//! next statement's set. Typed access goes through [`FromSql`], like
//! `tokio-postgres` rows.

use crate::connection::{QueryResult, ResultSet, Row, RowIndex};
use crate::error::{NzError, NzResult};
use crate::tuple_desc::ColumnDesc;
use crate::types::value::{FromSql, NzValue};

// ---------------------------------------------------------------------------
// Column metadata (port of Node `NzDataReader` ColumnMetadata / SchemaRow)
// ---------------------------------------------------------------------------

/// Closest Rust analog of the JS/.NET CLR type a column maps to. The Node
/// reader exposes the constructor (`Boolean`, `Number`, `BigInt`, `Date`,
/// `Object`, `String`); this enum is the Rust-idiomatic equivalent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnDataType {
    Bool,
    Number,
    BigInt,
    Date,
    Object,
    String,
}

impl ColumnDataType {
    pub fn name(self) -> &'static str {
        match self {
            ColumnDataType::Bool => "Boolean",
            ColumnDataType::Number => "Number",
            ColumnDataType::BigInt => "BigInt",
            ColumnDataType::Date => "Date",
            ColumnDataType::Object => "Object",
            ColumnDataType::String => "String",
        }
    }
}

/// Resolved metadata for one column, mirroring Node's `getColumnMetadata(i)`
/// / C# `NzDbColumn`.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnMetadata {
    pub index: usize,
    pub name: String,
    pub provider_type: i32,
    pub type_modifier: i32,
    pub type_length: i16,
    pub type_name: String,
    pub declared_type_name: String,
    pub declared_length: Option<i32>,
    pub numeric_precision: i32,
    pub numeric_scale: i32,
    pub data_type: ColumnDataType,
    pub column_size: i64,
    pub is_long: bool,
}

/// One `getSchemaTable()` row (Node `SchemaRow` / ADO.NET `DataTable` row).
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaRow {
    pub column_name: String,
    /// 1-based ordinal (Node `ColumnOrdinal`).
    pub column_ordinal: i32,
    pub column_size: i64,
    pub numeric_precision: i32,
    pub numeric_scale: i32,
    pub data_type: ColumnDataType,
    pub provider_type: i32,
    pub allow_db_null: bool,
    pub is_read_only: bool,
    pub is_long: bool,
}

/// Schema table returned by [`NzDataReader::get_schema_table`].
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaTable {
    pub rows: Vec<SchemaRow>,
    pub columns_count: usize,
}

const TYPE_MOD_OFFSET: i32 = 16;

/// OIDs referenced by the metadata resolver (Node `Oid`).
mod oid {
    pub const BOOL: i32 = 16;
    pub const BYTEA: i32 = 17;
    pub const CHAR: i32 = 18;
    pub const NAME: i32 = 19;
    pub const INT8: i32 = 20;
    pub const INT2: i32 = 21;
    pub const INT4: i32 = 23;
    pub const TEXT: i32 = 25;
    pub const OID: i32 = 26;
    pub const ABS_TIME: i32 = 702;
    pub const BYTEINT: i32 = 2500;
    pub const NCHAR: i32 = 2522;
    pub const NVARCHAR: i32 = 2530;
    pub const BPCHAR: i32 = 1042;
    pub const VARCHAR: i32 = 1043;
    pub const DATE: i32 = 1082;
    pub const TIME: i32 = 1083;
    pub const TIMESTAMP: i32 = 1114;
    pub const TIMESTAMPTZ: i32 = 1184;
    pub const INTERVAL: i32 = 1186;
    pub const TIMETZ: i32 = 1266;
    pub const NUMERIC: i32 = 1700;
    pub const FLOAT4: i32 = 700;
    pub const FLOAT8: i32 = 701;
    pub const NZ_CHAR: i32 = 15;
}

fn type_name_from_oid(oid: i32) -> String {
    match oid {
        x if x == oid::BOOL => "BOOL".into(),
        x if x == oid::BYTEA => "BYTEA".into(),
        x if x == oid::BYTEINT => "BYTEINT".into(),
        x if x == oid::CHAR => "CHAR".into(),
        x if x == oid::NAME => "NAME".into(),
        x if x == oid::INT8 => "INT8".into(),
        x if x == oid::INT2 => "INT2".into(),
        x if x == oid::INT4 => "INT4".into(),
        x if x == oid::OID => "OID".into(),
        x if x == oid::TEXT => "TEXT".into(),
        x if x == oid::NCHAR => "NCHAR".into(),
        x if x == oid::BPCHAR => "CHAR".into(),
        x if x == oid::VARCHAR => "VARCHAR".into(),
        x if x == oid::NVARCHAR => "NVARCHAR".into(),
        x if x == oid::ABS_TIME => "ABSTIME".into(),
        x if x == oid::DATE => "DATE".into(),
        x if x == oid::TIME => "TIME".into(),
        x if x == oid::TIMESTAMP => "TIMESTAMP".into(),
        x if x == oid::TIMESTAMPTZ => "TIMESTAMPTZ".into(),
        x if x == oid::INTERVAL => "INTERVAL".into(),
        x if x == oid::TIMETZ => "TIMETZ".into(),
        x if x == oid::NUMERIC => "NUMERIC".into(),
        x if x == oid::FLOAT4 => "FLOAT4".into(),
        x if x == oid::FLOAT8 => "FLOAT8".into(),
        x if x == oid::NZ_CHAR => "CHAR".into(),
        other => format!("UNKNOWN({other})"),
    }
}

fn is_character_type(oid: i32) -> bool {
    oid == oid::NZ_CHAR
        || oid == oid::CHAR
        || oid == oid::NAME
        || oid == oid::TEXT
        || oid == oid::BPCHAR
        || oid == oid::VARCHAR
        || oid == oid::NCHAR
        || oid == oid::NVARCHAR
}

fn numeric_precision_scale(type_mod: i32) -> (i32, i32) {
    if type_mod > TYPE_MOD_OFFSET {
        let normalized = type_mod - TYPE_MOD_OFFSET;
        (normalized >> 16, normalized & 0xffff)
    } else {
        (0, 0)
    }
}

fn character_declared_length(col: &ColumnDesc) -> Option<i32> {
    if !is_character_type(col.type_oid) {
        return None;
    }
    if col.type_mod > TYPE_MOD_OFFSET {
        Some(col.type_mod - TYPE_MOD_OFFSET)
    } else {
        None
    }
}

fn format_declared_type_name(
    oid: i32,
    type_name: &str,
    declared_length: Option<i32>,
    precision: i32,
    scale: i32,
) -> String {
    if oid == oid::BPCHAR || oid == oid::VARCHAR || oid == oid::NCHAR || oid == oid::NVARCHAR {
        match declared_length {
            Some(len) => format!("{type_name}({len})"),
            None => type_name.to_string(),
        }
    } else if oid == oid::NUMERIC {
        if precision > 0 {
            format!("NUMERIC({precision},{scale})")
        } else {
            type_name.to_string()
        }
    } else {
        type_name.to_string()
    }
}

/// Resolve one column's metadata (Node `_getResolvedColumnMetadata`).
fn resolve_column_metadata(col: &ColumnDesc, index: usize) -> ColumnMetadata {
    let oid_value = col.type_oid;
    let type_name = type_name_from_oid(oid_value);
    let declared_length = character_declared_length(col);
    let (mut numeric_precision, mut numeric_scale) = numeric_precision_scale(col.type_mod);
    let type_len = col.type_len as i32;

    let (data_type, column_size): (ColumnDataType, i64) = if oid_value == oid::BOOL {
        (ColumnDataType::Bool, 1)
    } else if oid_value == oid::BYTEINT {
        (ColumnDataType::Number, 1)
    } else if oid_value == oid::INT2 {
        (ColumnDataType::Number, 2)
    } else if oid_value == oid::INT4 {
        (ColumnDataType::Number, 4)
    } else if oid_value == oid::INT8 {
        (ColumnDataType::BigInt, 8)
    } else if oid_value == oid::OID {
        (
            ColumnDataType::Number,
            if type_len > 0 { type_len as i64 } else { 4 },
        )
    } else if oid_value == oid::FLOAT4 || oid_value == oid::FLOAT8 {
        let is_double = oid_value == oid::FLOAT8;
        numeric_precision = if is_double { 53 } else { 24 };
        numeric_scale = 0;
        (ColumnDataType::Number, if is_double { 8 } else { 4 })
    } else if oid_value == oid::NUMERIC {
        let size = if numeric_precision > 0 {
            let base = (numeric_precision / 2 + 1) as i64;
            if base < type_len as i64 && type_len > 0 {
                type_len as i64
            } else {
                base
            }
        } else if type_len > 0 {
            type_len as i64
        } else {
            -1
        };
        (ColumnDataType::Number, size)
    } else if oid_value == oid::DATE
        || oid_value == oid::ABS_TIME
        || oid_value == oid::TIMESTAMP
        || oid_value == oid::TIMESTAMPTZ
        || oid_value == oid::TIME
        || oid_value == oid::TIMETZ
    {
        let is_time = oid_value == oid::TIME || oid_value == oid::TIMETZ;
        let size = if type_len > 0 {
            type_len as i64
        } else if oid_value == oid::ABS_TIME {
            4
        } else {
            -1
        };
        (
            if is_time {
                ColumnDataType::Object
            } else {
                ColumnDataType::Date
            },
            size,
        )
    } else if is_character_type(oid_value) {
        let size = declared_length
            .map(|l| l as i64)
            .unwrap_or(if type_len > 0 { type_len as i64 } else { -1 });
        (ColumnDataType::String, size)
    } else {
        (
            ColumnDataType::String,
            if type_len > 0 { type_len as i64 } else { -1 },
        )
    };

    let declared_type_name = format_declared_type_name(
        oid_value,
        &type_name,
        declared_length,
        numeric_precision,
        numeric_scale,
    );

    ColumnMetadata {
        index,
        name: col.name.clone(),
        provider_type: oid_value,
        type_modifier: col.type_mod,
        type_length: col.type_len,
        type_name,
        declared_type_name,
        declared_length,
        numeric_precision,
        numeric_scale,
        data_type,
        column_size,
        is_long: column_size > 8000,
    }
}

/// Forward-only reader over a buffered [`QueryResult`].
///
/// ```no_run
/// # use nz_rust::NzConnection;
/// # fn f(conn: &mut NzConnection) -> nz_rust::error::NzResult<()> {
/// let mut reader = conn.execute_reader("SELECT 1 AS ONE", &[])?;
/// while reader.read()? {
///     let one: i32 = reader.try_get(0)?;
///     println!("{one}");
/// }
/// # Ok(())
/// # }
/// ```
pub struct NzDataReader {
    result: QueryResult,
    set_idx: usize,
    /// Row position within the current set: `None` = before first / after last.
    row_idx: Option<usize>,
    /// True once the current set's `CommandComplete` was observed (no more rows).
    result_complete: bool,
    closed: bool,
}

impl NzDataReader {
    pub fn from_result(mut result: QueryResult) -> Self {
        // A statement that returns no result set (DDL, `DELETE`, …) still forms
        // one reader result in the ADO.NET/Node model, so `has_rows()` reports
        // `false` rather than the reader appearing to have no results at all
        // (Node `NzDataReader` / C# `NzDataReader` parity).
        if result.result_sets.is_empty() {
            result
                .result_sets
                .push(ResultSet::new(Vec::new(), Vec::new()));
        }
        NzDataReader {
            result,
            set_idx: 0,
            row_idx: None,
            result_complete: false,
            closed: false,
        }
    }

    /// Columns of the current result set.
    pub fn columns(&self) -> &[ColumnDesc] {
        self.result
            .result_sets
            .get(self.set_idx)
            .map(|s| s.columns.as_slice())
            .unwrap_or(&[])
    }

    pub fn field_count(&self) -> usize {
        self.columns().len()
    }

    /// Current row (after a successful [`NzDataReader::read`]).
    pub fn current_row(&self) -> Option<&Row> {
        let set = self.result.result_sets.get(self.set_idx)?;
        let idx = self.row_idx?;
        set.rows.get(idx)
    }

    fn require_row(&self) -> NzResult<&Row> {
        self.current_row()
            .ok_or_else(|| NzError::Config("No current row. Did you call read()?".into()))
    }

    /// Advance to the next row in the current set.
    ///
    /// Returns `false` at the end of the set (mirrors `DbDataReader.Read()`).
    /// A further `read()` stays `false` until [`NzDataReader::next_result`].
    pub fn read(&mut self) -> NzResult<bool> {
        if self.closed || self.result_complete {
            return Ok(false);
        }
        let Some(set) = self.result.result_sets.get(self.set_idx) else {
            return Ok(false);
        };
        let next = match self.row_idx {
            None => 0,
            Some(i) => i + 1,
        };
        if next < set.rows.len() {
            self.row_idx = Some(next);
            Ok(true)
        } else {
            self.row_idx = None;
            self.result_complete = true;
            Ok(false)
        }
    }

    /// Advance to the next statement's result set. Returns `false` when no
    /// more sets exist (mirrors `DbDataReader.NextResult()`).
    pub fn next_result(&mut self) -> NzResult<bool> {
        if self.closed {
            return Ok(false);
        }
        if self.set_idx + 1 >= self.result.result_sets.len() {
            return Ok(false);
        }
        self.set_idx += 1;
        self.row_idx = None;
        self.result_complete = false;
        Ok(true)
    }

    pub fn has_rows(&self) -> bool {
        self.result
            .result_sets
            .get(self.set_idx)
            .map(|s| !s.rows.is_empty())
            .unwrap_or(false)
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn close(&mut self) {
        self.closed = true;
    }

    // -- metadata (C#/Node parity) -------------------------------------------

    pub fn get_name(&self, i: usize) -> NzResult<&str> {
        self.columns()
            .get(i)
            .map(|c| c.name.as_str())
            .ok_or_else(|| NzError::Config(format!("Column ordinal {i} is out of range")))
    }

    pub fn get_ordinal(&self, name: &str) -> NzResult<usize> {
        self.columns()
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| NzError::Config(format!("Column '{name}' not found")))
    }

    pub fn get_type_name(&self, i: usize) -> NzResult<String> {
        self.get_column_metadata(i).map(|m| m.type_name)
    }

    pub fn get_declared_type_name(&self, i: usize) -> NzResult<String> {
        self.get_column_metadata(i).map(|m| m.declared_type_name)
    }

    pub fn get_provider_type(&self, i: usize) -> NzResult<i32> {
        self.get_column_metadata(i).map(|m| m.provider_type)
    }

    /// Raw PostgreSQL/Netezza type modifier for a column.
    pub fn get_type_modifier(&self, i: usize) -> NzResult<i32> {
        self.get_column_metadata(i).map(|m| m.type_modifier)
    }

    /// Declared storage length for a column (`-1` = variable).
    pub fn get_type_length(&self, i: usize) -> NzResult<i16> {
        self.get_column_metadata(i).map(|m| m.type_length)
    }

    /// Wire format of the column's values (0 = text, 1 = binary).
    pub fn get_format(&self, i: usize) -> NzResult<u8> {
        self.column(i).map(|c| c.format)
    }

    /// Whether the column accepts SQL NULL. The binary tuple descriptor
    /// reports this; on the text path (no nullability information) it is
    /// assumed `true`, matching the Node/C# readers.
    pub fn get_column_allows_null(&self, i: usize) -> bool {
        self.nullability()
            .and_then(|n| n.get(i).copied())
            .unwrap_or(true)
    }

    /// Fully resolved metadata for one column (Node `getColumnMetadata`).
    pub fn get_column_metadata(&self, i: usize) -> NzResult<ColumnMetadata> {
        let col = self.column(i)?;
        Ok(resolve_column_metadata(col, i))
    }

    /// ADO.NET-style schema table over the current result set's columns
    /// (Node `getSchemaTable` / C# `GetSchemaTable`).
    pub fn get_schema_table(&self) -> NzResult<SchemaTable> {
        let count = self.columns().len();
        let mut rows = Vec::with_capacity(count);
        for i in 0..count {
            let m = self.get_column_metadata(i)?;
            rows.push(SchemaRow {
                column_name: m.name,
                column_ordinal: (i + 1) as i32,
                column_size: m.column_size,
                numeric_precision: m.numeric_precision,
                numeric_scale: m.numeric_scale,
                data_type: m.data_type,
                provider_type: m.provider_type,
                allow_db_null: self.get_column_allows_null(i),
                is_read_only: true,
                is_long: m.is_long,
            });
        }
        Ok(SchemaTable {
            rows,
            columns_count: count,
        })
    }

    fn nullability(&self) -> Option<&[bool]> {
        self.result
            .result_sets
            .get(self.set_idx)
            .and_then(|s| s.nullability.as_deref())
    }

    fn column(&self, i: usize) -> NzResult<&ColumnDesc> {
        self.columns()
            .get(i)
            .ok_or_else(|| NzError::Config(format!("Column ordinal {i} is out of range")))
    }

    // -- value access ----------------------------------------------------------

    fn value(&self, i: usize) -> NzResult<&NzValue> {
        let row = self.require_row()?;
        row.try_get_value(i)
    }

    pub fn get_value(&self, i: usize) -> NzResult<NzValue> {
        Ok(self.value(i)?.clone())
    }

    pub fn get_value_by_name(&self, name: &str) -> NzResult<NzValue> {
        let i = self.get_ordinal(name)?;
        self.get_value(i)
    }

    pub fn try_get<I: RowIndex, T: FromSql>(&self, idx: I) -> NzResult<T> {
        let row = self.require_row()?;
        row.try_get(idx)
    }

    pub fn is_db_null(&self, i: usize) -> NzResult<bool> {
        Ok(self.value(i)?.is_null())
    }

    pub fn get_bool(&self, i: usize) -> NzResult<bool> {
        let v = self.value(i)?;
        match v {
            NzValue::Null => Ok(false),
            NzValue::Bool(b) => Ok(*b),
            NzValue::Text(s) => Ok(matches!(
                s.trim().to_ascii_lowercase().as_str(),
                "t" | "true" | "1" | "y" | "yes"
            )),
            NzValue::Int2(n) => Ok(*n != 0),
            NzValue::Int4(n) => Ok(*n != 0),
            NzValue::Int8(n) => Ok(*n != 0),
            NzValue::Decimal(value) => Ok(!value.is_zero()),
            other => Err(NzError::Config(format!("cannot convert {other:?} to bool"))),
        }
    }

    pub fn get_i16(&self, i: usize) -> NzResult<i16> {
        let value = self.value(i)?;
        i16::from_sql(value)
    }
    pub fn get_i32(&self, i: usize) -> NzResult<i32> {
        let value = self.value(i)?;
        i32::from_sql(value)
    }
    pub fn get_i64(&self, i: usize) -> NzResult<i64> {
        let value = self.value(i)?;
        i64::from_sql(value)
    }
    pub fn get_u8(&self, i: usize) -> NzResult<u8> {
        let value = self.value(i)?;
        u8::from_sql(value)
    }
    pub fn get_u16(&self, i: usize) -> NzResult<u16> {
        let value = self.value(i)?;
        u16::from_sql(value)
    }
    pub fn get_u32(&self, i: usize) -> NzResult<u32> {
        let value = self.value(i)?;
        u32::from_sql(value)
    }
    pub fn get_f32(&self, i: usize) -> NzResult<f32> {
        let value = self.value(i)?;
        f32::from_sql(value)
    }
    pub fn get_f64(&self, i: usize) -> NzResult<f64> {
        let value = self.value(i)?;
        f64::from_sql(value)
    }

    pub fn get_string(&self, i: usize) -> NzResult<Option<String>> {
        match self.value(i)? {
            NzValue::Null => Ok(None),
            NzValue::Text(s)
            | NzValue::Numeric(s)
            | NzValue::Date(s)
            | NzValue::Time(s)
            | NzValue::Timetz(s)
            | NzValue::Timestamp(s)
            | NzValue::Interval(s) => Ok(Some(s.clone())),
            NzValue::Decimal(value) => Ok(Some(value.to_string())),
            NzValue::Bool(b) => Ok(Some(b.to_string())),
            NzValue::Int2(n) => Ok(Some(n.to_string())),
            NzValue::Int4(n) => Ok(Some(n.to_string())),
            NzValue::Int8(n) => Ok(Some(n.to_string())),
            NzValue::Float4(n) => Ok(Some(n.to_string())),
            NzValue::Float8(n) => Ok(Some(n.to_string())),
            NzValue::Bytea(b) => Ok(Some(format!(
                "E'\\\\x{}'",
                b.iter().map(|x| format!("{x:02x}")).collect::<String>()
            ))),
        }
    }

    pub fn get_bytes(&self, i: usize) -> NzResult<Option<Vec<u8>>> {
        match self.value(i)? {
            NzValue::Null => Ok(None),
            NzValue::Bytea(b) => Ok(Some(b.clone())),
            NzValue::Text(s)
            | NzValue::Numeric(s)
            | NzValue::Date(s)
            | NzValue::Time(s)
            | NzValue::Timetz(s)
            | NzValue::Timestamp(s)
            | NzValue::Interval(s) => Ok(Some(s.as_bytes().to_vec())),
            NzValue::Decimal(value) => Ok(Some(value.to_string().into_bytes())),
            other => Err(NzError::Config(format!(
                "cannot convert {other:?} to bytes"
            ))),
        }
    }

    /// Textual counterpart of ADO.NET `GetChars`.
    pub fn get_chars(&self, i: usize) -> NzResult<Option<String>> {
        self.get_string(i)
    }

    /// Current row as `name → value` pairs.
    pub fn get_row_map(&self) -> NzResult<Vec<(String, NzValue)>> {
        Ok(self.require_row()?.as_map())
    }

    /// Drain remaining rows of all sets into a `Vec`.
    pub fn drain_rows(&mut self) -> NzResult<Vec<Row>> {
        let mut out = Vec::new();
        loop {
            while self.read()? {
                out.push(self.require_row()?.clone());
            }
            if !self.next_result()? {
                break;
            }
        }
        Ok(out)
    }

    /// Drain remaining rows of all sets into a `Vec`.
    ///
    /// Use [`Self::drain_rows`] in new code. This compatibility spelling is
    /// retained because the method consumes the reader's remaining rows.
    #[deprecated(note = "use drain_rows; this method drains the reader")]
    #[allow(clippy::wrong_self_convention)]
    pub fn to_rows(&mut self) -> NzResult<Vec<Row>> {
        self.drain_rows()
    }
}

impl Iterator for NzDataReader {
    type Item = NzResult<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read() {
            Ok(true) => self.current_row().cloned().map(Ok),
            Ok(false) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ResultSet;

    fn sample() -> QueryResult {
        use crate::tuple_desc::ColumnDesc;
        let cols = vec![ColumnDesc {
            name: "ONE".into(),
            type_oid: 23,
            type_len: 4,
            type_mod: -1,
            format: 0,
        }];
        QueryResult {
            result_sets: vec![ResultSet::new(
                cols.clone(),
                vec![
                    Row::new(cols.clone(), vec![NzValue::Int4(1)]),
                    Row::new(cols.clone(), vec![NzValue::Int4(2)]),
                ],
            )],
            rows_affected: 2,
            notices: vec![],
        }
    }

    #[test]
    fn reads_rows_and_metadata() {
        let mut r = NzDataReader::from_result(sample());
        assert!(r.read().unwrap());
        assert_eq!(r.try_get::<_, i32>(0).unwrap(), 1);
        assert_eq!(r.get_name(0).unwrap(), "ONE");
        assert_eq!(r.get_ordinal("one").unwrap(), 0);
        assert!(r.read().unwrap());
        assert_eq!(r.get_i32(0).unwrap(), 2);
        assert!(!r.read().unwrap());
        assert!(!r.next_result().unwrap());
    }

    #[test]
    fn multi_set_navigation() {
        let mut q = sample();
        q.result_sets.push(q.result_sets[0].clone());
        let mut r = NzDataReader::from_result(q);
        assert!(r.read().unwrap());
        assert!(r.next_result().unwrap());
        assert!(r.read().unwrap());
    }

    fn col(name: &str, oid: i32, type_len: i16, type_mod: i32) -> ColumnDesc {
        ColumnDesc {
            name: name.into(),
            type_oid: oid,
            type_len,
            type_mod,
            format: 0,
        }
    }

    fn reader_with(columns: Vec<ColumnDesc>) -> NzDataReader {
        NzDataReader::from_result(QueryResult {
            result_sets: vec![ResultSet::new(columns, vec![])],
            rows_affected: 0,
            notices: vec![],
        })
    }

    #[test]
    fn column_metadata_matches_reference_types() {
        let numeric_mod = (10 << 16 | 2) + 16;
        let varchar_mod = 5 + 16;
        let r = reader_with(vec![
            col("INT_COL", 23, 4, -1),
            col("DATE_COL", 1082, 8, -1),
            col("NUMERIC_COL", 1700, 8, numeric_mod),
            col("VAR_CHAR", 1043, -1, varchar_mod),
            col("TEXT_COL", 25, -1, -1),
        ]);

        let int_col = r.get_column_metadata(0).unwrap();
        assert_eq!(int_col.type_name, "INT4");
        assert_eq!(int_col.data_type, ColumnDataType::Number);
        assert_eq!(int_col.column_size, 4);

        let date_col = r.get_column_metadata(1).unwrap();
        assert_eq!(date_col.data_type, ColumnDataType::Date);

        let numeric_col = r.get_column_metadata(2).unwrap();
        assert_eq!(numeric_col.numeric_precision, 10);
        assert_eq!(numeric_col.numeric_scale, 2);
        assert_eq!(numeric_col.declared_type_name, "NUMERIC(10,2)");

        let varchar = r.get_column_metadata(3).unwrap();
        assert_eq!(varchar.declared_length, Some(5));
        assert_eq!(varchar.declared_type_name, "VARCHAR(5)");
        assert_eq!(varchar.column_size, 5);

        let text = r.get_column_metadata(4).unwrap();
        assert_eq!(text.type_name, "TEXT");
        assert_eq!(text.column_size, -1);
    }

    #[test]
    fn schema_table_reports_rows_and_nullability() {
        let r = reader_with(vec![col("A", 23, 4, -1), col("B", 20, 8, -1)]);
        let schema = r.get_schema_table().unwrap();
        assert_eq!(schema.columns_count, 2);
        assert_eq!(schema.rows[0].column_name, "A");
        assert_eq!(schema.rows[0].column_ordinal, 1);
        assert_eq!(schema.rows[0].data_type, ColumnDataType::Number);
        assert!(schema.rows[0].is_read_only);
        // Text path reports no nullability → assumed nullable.
        assert!(schema.rows[0].allow_db_null);
        assert_eq!(schema.rows[1].column_ordinal, 2);
        assert_eq!(schema.rows[1].data_type, ColumnDataType::BigInt);
    }

    #[test]
    fn nullability_from_binary_descriptor() {
        let columns = vec![col("A", 23, 4, -1), col("B", 23, 4, -1)];
        let mut set = ResultSet::new(columns, vec![]);
        set.nullability = Some(vec![false, true]);
        let r = NzDataReader::from_result(QueryResult {
            result_sets: vec![set],
            rows_affected: 0,
            notices: vec![],
        });
        assert!(!r.get_column_allows_null(0));
        assert!(r.get_column_allows_null(1));
        let schema = r.get_schema_table().unwrap();
        assert!(!schema.rows[0].allow_db_null);
    }

    #[test]
    fn unknown_oid_and_float_precision() {
        let r = reader_with(vec![
            col("F", 701, 8, -1),
            col("F4", 700, 4, -1),
            col("W", 999, 4, -1),
        ]);
        assert_eq!(r.get_type_name(0).unwrap(), "FLOAT8");
        assert_eq!(r.get_column_metadata(0).unwrap().numeric_precision, 53);
        assert_eq!(r.get_column_metadata(1).unwrap().numeric_precision, 24);
        assert_eq!(r.get_type_name(2).unwrap(), "UNKNOWN(999)");
    }

    #[test]
    fn out_of_range_ordinal_errors() {
        let r = reader_with(vec![col("A", 23, 4, -1)]);
        assert!(r.get_column_metadata(5).is_err());
        assert!(r.get_type_name(5).is_err());
    }
}
