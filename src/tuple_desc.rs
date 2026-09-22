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

//! Binary tuple descriptor — port of C# `DbosTupleDesc.cs` / Node
//! `DbosTupleDesc.ts` plus the shared [`ColumnDesc`] metadata type.

use crate::error::{validate_protocol_length, NzError, NzResult};
use crate::messages::nz_type;
use crate::types::datetime;
use crate::types::numeric::{
    field_precision, field_scale, get_cs_numeric_into, numeric_digit_count, NumericDecodedInto,
};
use crate::types::value::{string_value_slot, NzValue, StringValueKind};

/// Column metadata from a text (`T`) or binary (`X`) row description.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDesc {
    pub name: String,
    pub type_oid: i32,
    pub type_len: i16,
    pub type_mod: i32,
    pub format: u8,
}

/// Offset Netezza adds to the declared length when it packs it into `type_mod`.
const TYPE_MOD_OFFSET: i32 = 16;

impl ColumnDesc {
    /// Canonical base type name — port of the C# `GetDataTypeName(FieldDescription)`
    /// switch (itself mirrored by the Node driver's `_getTypeNameFromOid`).
    ///
    /// The two references agree on the OID set but not on every spelling:
    /// C# reports `SMALLINT`/`INTEGER`/`BIGINT`/`REAL`/`DOUBLE`, Node reports
    /// `INT2`/`INT4`/`INT8`/`FLOAT4`/`FLOAT8`. This table follows C#; the Node
    /// spellings are available on [`crate::reader::ColumnMetadata`]'s
    /// `type_name` field.
    ///
    /// Unknown OIDs fall back to `OID(n)` — the C# falls back to the server's
    /// own type name, which is not part of the row description on this wire
    /// protocol.
    pub fn type_name(&self) -> String {
        match self.type_oid {
            15 | 18 | 1042 => "CHAR".into(),
            16 => "BOOL".into(),
            17 => "BYTEA".into(),
            19 => "NAME".into(),
            20 => "BIGINT".into(),
            21 => "SMALLINT".into(),
            23 => "INTEGER".into(),
            25 => "TEXT".into(),
            26 => "OID".into(),
            700 => "REAL".into(),
            701 => "DOUBLE".into(),
            702 => "ABSTIME".into(),
            1043 => "VARCHAR".into(),
            1082 => "DATE".into(),
            1083 => "TIME".into(),
            1114 => "TIMESTAMP".into(),
            1184 => "TIMESTAMPTZ".into(),
            1186 => "INTERVAL".into(),
            1266 => "TIMETZ".into(),
            1700 => "NUMERIC".into(),
            2500 => "BYTEINT".into(),
            2522 => "NCHAR".into(),
            2530 => "NVARCHAR".into(),
            other => format!("OID({other})"),
        }
    }

    /// Declared type name with its length/precision, e.g. `VARCHAR(32)`,
    /// `NVARCHAR(20)` or `NUMERIC(10,4)`.
    ///
    /// The declared length is **not** in `type_len` on the wire: Netezza
    /// reports `type_len = -1` for every varying/character type and packs the
    /// size into `type_mod` as `length + 16` (verified against a live
    /// appliance). Fixed-width types keep `type_mod = -1` and no suffix.
    pub fn declared_type_name(&self) -> String {
        let base = self.type_name();
        if self.type_mod <= TYPE_MOD_OFFSET {
            return base;
        }
        let normalized = self.type_mod - TYPE_MOD_OFFSET;
        if self.type_oid == 1700 {
            let precision = normalized >> 16;
            let scale = normalized & 0xffff;
            if precision > 0 {
                return format!("NUMERIC({precision},{scale})");
            }
            return base;
        }
        if matches!(self.type_oid, 1042 | 1043 | 2522 | 2530) {
            return format!("{base}({normalized})");
        }
        base
    }
}

/// Parses text RowDescription (`T`) payload: `count(u16)`, then per column
/// `name(NUL) + oid(i32) + len(i16) + mod(i32) + format(u8)`.
pub fn parse_row_description(data: &[u8]) -> NzResult<Vec<ColumnDesc>> {
    if data.len() < 2 {
        return Err(NzError::Protocol(
            "Invalid RowDescription payload: column count is truncated; reconnect is required."
                .into(),
        ));
    }
    let count = u16::from_be_bytes(data[0..2].try_into().unwrap()) as usize;
    let mut columns = Vec::with_capacity(count);
    let mut offset = 2usize;

    for i in 0..count {
        let name_start = offset;
        while offset < data.len() && data[offset] != 0 {
            offset += 1;
        }
        if offset >= data.len() {
            return Err(NzError::Protocol(format!(
                "Invalid RowDescription payload: column {i} name is not terminated; reconnect is required."
            )));
        }
        let name = String::from_utf8_lossy(&data[name_start..offset]).to_string();
        offset += 1;

        if offset + 11 > data.len() {
            return Err(NzError::Protocol(format!(
                "Invalid RowDescription payload: column {i} metadata is truncated; reconnect is required."
            )));
        }
        let type_oid = i32::from_be_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let type_len = i16::from_be_bytes(data[offset..offset + 2].try_into().unwrap());
        offset += 2;
        let type_mod = i32::from_be_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let format = data[offset];
        offset += 1;

        columns.push(ColumnDesc {
            name,
            type_oid,
            type_len,
            type_mod,
            format,
        });
    }

    Ok(columns)
}

/// Binary tuple descriptor (`X` RowDescriptionStandard payload).
#[derive(Debug, Default, Clone)]
pub struct DbosTupleDesc {
    pub version: i32,
    pub nulls_allowed: i32,
    pub size_word: i32,
    pub size_word_size: i32,
    pub num_fixed_fields: i32,
    pub num_varying_fields: i32,
    pub fixed_fields_size: i32,
    pub max_record_size: i32,
    pub num_fields: usize,
    pub field_type: Vec<i32>,
    pub field_size: Vec<i32>,
    pub field_true_size: Vec<i32>,
    pub field_offset: Vec<i32>,
    pub field_phys_field: Vec<i32>,
    pub field_log_field: Vec<i32>,
    pub field_null_allowed: Vec<bool>,
    pub field_null_byte_offset: Vec<usize>,
    pub field_null_bit_mask: Vec<u8>,
    pub field_fixed_size: Vec<i32>,
    pub field_spring_field: Vec<i32>,
    pub date_style: i32,
    pub euro_dates: i32,
}

/// Validated field locations for one DBOS row. The descriptor remains shared;
/// only varying-field offsets and the row's NULL bits are row-specific.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DbosFieldLayout {
    pub start: usize,
    pub is_null: bool,
}

impl DbosTupleDesc {
    /// Parse the descriptor. `cached_columns` supplies the text-path column
    /// metadata (used for the abstime OID 702 fix, nzpy issue #61).
    pub fn parse(data: &[u8], cached_columns: Option<&[ColumnDesc]>) -> NzResult<Self> {
        if data.len() < 36 {
            return Err(NzError::Protocol(
                "Invalid RowDescriptionStandard payload: descriptor header is truncated; reconnect is required."
                    .into(),
            ));
        }
        let be32 =
            |off: usize| -> i32 { i32::from_be_bytes(data[off..off + 4].try_into().unwrap()) };

        let mut desc = DbosTupleDesc {
            version: be32(0),
            nulls_allowed: be32(4),
            size_word: be32(8),
            size_word_size: be32(12),
            num_fixed_fields: be32(16),
            num_varying_fields: be32(20),
            fixed_fields_size: be32(24),
            max_record_size: be32(28),
            num_fields: 0,
            ..Default::default()
        };
        let num_fields_i = be32(32);
        if !(0..=100_000).contains(&num_fields_i) {
            return Err(NzError::Protocol(format!(
                "Invalid RowDescriptionStandard payload: field count {num_fields_i} is out of range; \
                 reconnect is required."
            )));
        }
        desc.num_fields = num_fields_i as usize;

        let descriptor_len = 36 + desc.num_fields * 36 + 8;
        validate_protocol_length(
            descriptor_len as i32,
            "rowDescriptionStandardDescriptor",
            true,
        )?;
        if data.len() < descriptor_len {
            return Err(NzError::Protocol(format!(
                "Invalid RowDescriptionStandard payload: expected at least {descriptor_len} bytes, received {}; \
                 reconnect is required.",
                data.len()
            )));
        }

        let mut idx = 36usize;
        for ix in 0..desc.num_fields {
            let mut ft = be32(idx);
            // Abstime (OID 702) comes back typed as INT — restore the fixed type.
            if ft == nz_type::NZ_TYPE_INT
                && cached_columns.and_then(|c| c.get(ix)).map(|c| c.type_oid) == Some(702)
            {
                ft = nz_type::NZ_TYPE_INTVS_ABS_TIME_FIX;
            }
            desc.field_type.push(ft);
            desc.field_size.push(be32(idx + 4));
            desc.field_true_size.push(be32(idx + 8));
            desc.field_offset.push(be32(idx + 12));
            let phys_field = be32(idx + 16);
            desc.field_phys_field.push(phys_field);
            desc.field_log_field.push(be32(idx + 20));
            desc.field_null_allowed.push(be32(idx + 24) != 0);
            desc.field_null_byte_offset
                .push(2 + (phys_field / 8) as usize);
            desc.field_null_bit_mask.push(1u8 << (phys_field % 8));
            desc.field_fixed_size.push(be32(idx + 28));
            desc.field_spring_field.push(be32(idx + 32));
            idx += 36;
        }

        desc.date_style = be32(idx);
        desc.euro_dates = be32(idx + 4);
        Ok(desc)
    }

    /// Build [`ColumnDesc`] list for binary result sets when no cached text
    /// description exists (best-effort naming `col1..colN`).
    pub fn to_column_descs(&self) -> Vec<ColumnDesc> {
        (0..self.num_fields)
            .map(|i| ColumnDesc {
                name: format!("col{}", i + 1),
                type_oid: self.field_type[i],
                type_len: self.field_size[i] as i16,
                type_mod: self.field_size[i],
                format: 1,
            })
            .collect()
    }

    fn is_null(&self, row: &[u8], base: usize, field_ix: usize) -> bool {
        if self.nulls_allowed == 0 {
            return false;
        }
        let byte_off = base + self.field_null_byte_offset[field_ix];
        if byte_off >= row.len() {
            return false;
        }
        (row[byte_off] & self.field_null_bit_mask[field_ix]) != 0
    }

    /// Validate and locate every field without decoding its value. This is
    /// the lazy-row counterpart of `parse_row_into_with_scratch`.
    pub(crate) fn row_layout(&self, row: &[u8]) -> NzResult<Vec<DbosFieldLayout>> {
        let num_varying = self.num_varying_fields.max(0) as usize;
        let mut var_starts = Vec::with_capacity(num_varying);
        if num_varying > 0 {
            let fixed_size = self.fixed_fields_size;
            if fixed_size < 0 {
                return Err(NzError::Protocol(
                    "Invalid RowStandard payload: fixed-field area offset is invalid; reconnect is required.".into(),
                ));
            }
            let mut voff = fixed_size as usize;
            for j in 0..num_varying {
                if voff + 2 > row.len() {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: varying field {j} length prefix is truncated; reconnect is required."
                    )));
                }
                var_starts.push(voff);
                let vlen = u16::from_le_bytes(row[voff..voff + 2].try_into().unwrap()) as usize;
                if vlen < 2 || voff + vlen > row.len() {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: varying field {j} length is invalid; reconnect is required."
                    )));
                }
                voff += vlen;
                if !vlen.is_multiple_of(2) {
                    voff += 1;
                }
                if voff > row.len() {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: varying field {j} padding is truncated; reconnect is required."
                    )));
                }
            }
        }

        let mut layout = Vec::with_capacity(self.num_fields);
        for i in 0..self.num_fields {
            let is_null = self.is_null(row, 0, i);
            if is_null {
                layout.push(DbosFieldLayout { start: 0, is_null });
                continue;
            }
            let fixed_size = self.field_fixed_size[i];
            let field_start = if fixed_size != 0 {
                if fixed_size < 0 {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: fixed field {i} size is invalid; reconnect is required."
                    )));
                }
                let off = self.field_offset[i];
                if off < 0 {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: fixed field {i} offset is invalid; reconnect is required."
                    )));
                }
                let off = off as usize;
                if off
                    .checked_add(fixed_size as usize)
                    .is_none_or(|end| end > row.len())
                {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: fixed field {i} extends beyond the row; reconnect is required."
                    )));
                }
                off
            } else if !var_starts.is_empty() {
                let var_index = self.field_offset[i];
                if var_index < 0 || var_index as usize >= num_varying {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: varying field {i} index is invalid; reconnect is required."
                    )));
                }
                let start = var_starts[var_index as usize];
                let encoded =
                    u16::from_le_bytes(row[start..start + 2].try_into().unwrap()) as usize;
                if encoded < 2 || start.checked_add(encoded).is_none_or(|end| end > row.len()) {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: varying field {i} extends beyond the row; reconnect is required."
                    )));
                }
                start
            } else {
                let fixed = self.fixed_fields_size;
                if fixed < 0 || fixed as usize > row.len() {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: field {i} starts outside the row; reconnect is required."
                    )));
                }
                fixed as usize
            };
            layout.push(DbosFieldLayout {
                start: field_start,
                is_null,
            });
        }
        Ok(layout)
    }

    /// Parse one binary row (`Y` payload, after the 8-byte DBOS header).
    ///
    /// Layout: `[2 reserved bytes][null bitmap][fixed fields][varying fields]`.
    /// Varying fields carry a `u16 LE` length that includes the length bytes,
    /// padded to even size.
    pub fn parse_row(&self, row: &[u8]) -> NzResult<Vec<NzValue>> {
        let num_fields = self.num_fields;
        let mut out = Vec::with_capacity(num_fields);
        self.parse_row_into(row, &mut out)?;
        Ok(out)
    }

    /// Parse a binary row into a reusable value vector.
    ///
    /// The streaming API decodes one row at a time. Clearing and reusing the
    /// vector keeps the hot path allocation-free for fixed-width values and
    /// matches the row-buffering strategy used by the reference drivers.
    pub fn parse_row_into(&self, row: &[u8], out: &mut Vec<NzValue>) -> NzResult<()> {
        let mut var_starts = Vec::new();
        self.parse_row_into_with_scratch(row, out, &mut var_starts)
    }

    /// Parse a binary row while reusing the varying-field offset scratch
    /// vector as well as the decoded value vector.
    pub fn parse_row_into_with_scratch(
        &self,
        row: &[u8],
        out: &mut Vec<NzValue>,
        var_starts: &mut Vec<usize>,
    ) -> NzResult<()> {
        let num_fields = self.num_fields;
        if out.capacity() < num_fields {
            out.reserve(num_fields - out.capacity());
        }
        if out.len() < num_fields {
            out.resize_with(num_fields, || NzValue::Null);
        } else {
            out.truncate(num_fields);
        }

        // Pre-compute varying field start offsets.
        let num_varying = self.num_varying_fields.max(0) as usize;
        var_starts.clear();
        if var_starts.capacity() < num_varying {
            var_starts.reserve(num_varying - var_starts.capacity());
        }
        if num_varying > 0 {
            let fixed_size = self.fixed_fields_size;
            if fixed_size < 0 {
                return Err(NzError::Protocol(
                    "Invalid RowStandard payload: fixed-field area offset is invalid; reconnect is required.".into(),
                ));
            }
            let mut voff = fixed_size as usize;
            for j in 0..num_varying {
                if voff + 2 > row.len() {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: varying field {j} length prefix is truncated; \
                         reconnect is required."
                    )));
                }
                var_starts.push(voff);
                let vlen = u16::from_le_bytes(row[voff..voff + 2].try_into().unwrap()) as usize;
                if vlen < 2 || voff + vlen > row.len() {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: varying field {j} length is invalid; reconnect is required."
                    )));
                }
                voff += vlen;
                if !vlen.is_multiple_of(2) {
                    voff += 1;
                }
                if voff > row.len() {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: varying field {j} padding is truncated; reconnect is required."
                    )));
                }
            }
        }

        for (i, out_value) in out.iter_mut().enumerate().take(num_fields) {
            if self.is_null(row, 0, i) {
                *out_value = NzValue::Null;
                continue;
            }

            let field_start: usize;
            let fixed_size = self.field_fixed_size[i];
            if fixed_size != 0 {
                if fixed_size < 0 {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: fixed field {i} size is invalid; reconnect is required."
                    )));
                }
                let off = self.field_offset[i];
                if off < 0 {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: fixed field {i} offset is invalid; reconnect is required."
                    )));
                }
                field_start = off as usize;
                if field_start + fixed_size as usize > row.len() {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: fixed field {i} extends beyond the row; reconnect is required."
                    )));
                }
            } else if !var_starts.is_empty() {
                let var_index = self.field_offset[i];
                if var_index < 0 || var_index as usize >= num_varying {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: varying field {i} index is invalid; reconnect is required."
                    )));
                }
                let start = var_starts[var_index as usize];
                let encoded =
                    u16::from_le_bytes(row[start..start + 2].try_into().unwrap()) as usize;
                if encoded < 2 || start + encoded > row.len() {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: varying field {i} extends beyond the row; reconnect is required."
                    )));
                }
                field_start = start;
            } else {
                let fixed = self.fixed_fields_size;
                if fixed < 0 {
                    return Err(NzError::Protocol(
                        "Invalid RowStandard payload: fixed-field area offset is invalid; reconnect is required.".into(),
                    ));
                }
                field_start = fixed as usize;
                if field_start > row.len() {
                    return Err(NzError::Protocol(format!(
                        "Invalid RowStandard payload: field {i} starts outside the row; reconnect is required."
                    )));
                }
            }

            self.parse_field_into(row, field_start, i, out_value)?;
        }

        Ok(())
    }

    pub(crate) fn parse_field_into(
        &self,
        row: &[u8],
        off: usize,
        i: usize,
        out: &mut NzValue,
    ) -> NzResult<()> {
        let fld_type = self.field_type[i];
        let fld_len = self.field_size[i] as usize;

        match fld_type {
            nz_type::NZ_TYPE_CHAR => {
                let s = slice_of(row, off, fld_len);
                let text = String::from_utf8_lossy(s);
                let target = string_value_slot(out, StringValueKind::Text);
                target.clear();
                target.push_str(text.trim_end_matches(' '));
            }
            nz_type::NZ_TYPE_NCHAR | nz_type::NZ_TYPE_NVARCHAR => {
                if off + 2 > row.len() {
                    return Err(protocol_trunc(i));
                }
                let cursize = i16::from_le_bytes(row[off..off + 2].try_into().unwrap()) as i32 - 2;
                if cursize < 0 || off + 2 + cursize as usize > row.len() {
                    return Err(protocol_trunc(i));
                }
                let text = String::from_utf8_lossy(slice_of(row, off + 2, cursize as usize));
                let target = string_value_slot(out, StringValueKind::Text);
                target.clear();
                target.push_str(text.trim_end_matches('\0'));
            }
            nz_type::NZ_TYPE_VARCHAR | nz_type::NZ_TYPE_VAR_FIXED_CHAR => {
                if off + 2 > row.len() {
                    return Err(protocol_trunc(i));
                }
                let cursize = i16::from_le_bytes(row[off..off + 2].try_into().unwrap()) as i32 - 2;
                if cursize < 0 || off + 2 + cursize as usize > row.len() {
                    return Err(protocol_trunc(i));
                }
                let text = String::from_utf8_lossy(slice_of(row, off + 2, cursize as usize));
                let target = string_value_slot(out, StringValueKind::Text);
                target.clear();
                target.push_str(&text);
            }
            nz_type::NZ_TYPE_INT8 => {
                if off + 8 > row.len() {
                    return Err(protocol_trunc(i));
                }
                *out = NzValue::Int8(i64::from_le_bytes(row[off..off + 8].try_into().unwrap()));
            }
            nz_type::NZ_TYPE_INT => {
                if off + 4 > row.len() {
                    return Err(protocol_trunc(i));
                }
                *out = NzValue::Int4(i32::from_le_bytes(row[off..off + 4].try_into().unwrap()));
            }
            nz_type::NZ_TYPE_INT2 => {
                if off + 2 > row.len() {
                    return Err(protocol_trunc(i));
                }
                *out = NzValue::Int2(i16::from_le_bytes(row[off..off + 2].try_into().unwrap()));
            }
            nz_type::NZ_TYPE_INT1 => {
                if off >= row.len() {
                    return Err(protocol_trunc(i));
                }
                *out = NzValue::Int2(row[off] as i8 as i16);
            }
            nz_type::NZ_TYPE_DOUBLE => {
                if off + 8 > row.len() {
                    return Err(protocol_trunc(i));
                }
                *out = NzValue::Float8(f64::from_le_bytes(row[off..off + 8].try_into().unwrap()));
            }
            nz_type::NZ_TYPE_FLOAT => {
                if off + 4 > row.len() {
                    return Err(protocol_trunc(i));
                }
                *out = NzValue::Float4(f32::from_le_bytes(row[off..off + 4].try_into().unwrap()));
            }
            nz_type::NZ_TYPE_DATE => {
                if off + 4 > row.len() {
                    return Err(protocol_trunc(i));
                }
                let days = i32::from_le_bytes(row[off..off + 4].try_into().unwrap());
                let target = string_value_slot(out, StringValueKind::Date);
                datetime::date_from_4bytes_into(days, target);
            }
            nz_type::NZ_TYPE_TIME => {
                if off + 8 > row.len() {
                    return Err(protocol_trunc(i));
                }
                let micros = i64::from_le_bytes(row[off..off + 8].try_into().unwrap());
                let target = string_value_slot(out, StringValueKind::Time);
                datetime::time_from_8bytes_into(micros, target);
            }
            nz_type::NZ_TYPE_INTERVAL => {
                if off + 12 > row.len() {
                    return Err(protocol_trunc(i));
                }
                let target = string_value_slot(out, StringValueKind::Interval);
                datetime::interval_from_12bytes_into(&row[off..off + 12], target);
            }
            nz_type::NZ_TYPE_TIME_TZ => {
                if off + fld_len > row.len() || fld_len < 12 {
                    return Err(protocol_trunc(i));
                }
                let target = string_value_slot(out, StringValueKind::Timetz);
                datetime::timetz_from_bytes_into(&row[off..off + fld_len], fld_len, target);
            }
            nz_type::NZ_TYPE_TIMESTAMP => {
                if off + 8 > row.len() {
                    return Err(protocol_trunc(i));
                }
                let micros = i64::from_le_bytes(row[off..off + 8].try_into().unwrap());
                let target = string_value_slot(out, StringValueKind::Timestamp);
                datetime::timestamp_from_8bytes_into(micros, target);
            }
            nz_type::NZ_TYPE_BOOL => {
                if off >= row.len() {
                    return Err(protocol_trunc(i));
                }
                *out = NzValue::Bool(row[off] == 0x01);
            }
            nz_type::NZ_TYPE_NUMERIC => {
                let p = field_precision(self.field_size[i]);
                let s = field_scale(self.field_size[i]);
                let c = numeric_digit_count(self.field_true_size[i]);
                // Move an existing exact-string buffer out of the row value so
                // the decoder can reuse its capacity only on the fallback
                // path. Decimal/float values never become `NzValue::Numeric`.
                let previous = std::mem::replace(out, NzValue::Null);
                let mut exact = match previous {
                    NzValue::Numeric(value) => value,
                    _ => String::new(),
                };
                match get_cs_numeric_into(&row[off.min(row.len())..], p, s, c, &mut exact)
                    .map_err(NzError::Protocol)?
                {
                    NumericDecodedInto::Number(n) => *out = NzValue::Float8(n),
                    NumericDecodedInto::Decimal(value) => *out = NzValue::Decimal(value),
                    NumericDecodedInto::Exact => *out = NzValue::Numeric(exact),
                }
            }
            _ => {
                let text = String::from_utf8_lossy(slice_of(row, off, fld_len));
                let target = string_value_slot(out, StringValueKind::Text);
                target.clear();
                target.push_str(&text);
            }
        }
        Ok(())
    }
}

fn slice_of(r: &[u8], o: usize, n: usize) -> &[u8] {
    let end = (o + n).min(r.len());
    let start = o.min(r.len()).min(end);
    &r[start..end]
}

fn protocol_trunc(field_ix: usize) -> NzError {
    NzError::Protocol(format!(
        "Invalid RowStandard payload: field {field_ix} is truncated; reconnect is required."
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn be32(v: i32) -> [u8; 4] {
        v.to_be_bytes()
    }

    #[test]
    fn parses_text_row_description() {
        // count=1, "ONE\0", oid=23, len=4, mod=-1, format=0
        let mut data = 1u16.to_be_bytes().to_vec();
        data.extend_from_slice(b"ONE\0");
        data.extend_from_slice(&be32(23));
        data.extend_from_slice(&4i16.to_be_bytes());
        data.extend_from_slice(&be32(-1));
        data.push(0);
        let cols = parse_row_description(&data).unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "ONE");
        assert_eq!(cols[0].type_oid, 23);
        assert_eq!(cols[0].type_name(), "INTEGER");
    }

    #[test]
    fn parses_binary_descriptor() {
        // Header: version=0, nullsAllowed=1, sizeWord=1, sizeWordSize=2,
        // numFixed=1, numVarying=1, fixedSize=8, maxRecord=64, numFields=2
        let mut data = Vec::new();
        for v in [0i32, 1, 1, 2, 1, 1, 7, 64, 2] {
            data.extend_from_slice(&be32(v));
        }
        // Field 0: fixed INT4 at offset 2, no null
        for v in [3i32, 4, 4, 2, 0, 0, 0, 4, 0] {
            data.extend_from_slice(&be32(v));
        }
        // Field 1: varying VARCHAR, varIndex 0
        for v in [16i32, -1, -1, 0, 0, 1, 1, 0, 0] {
            data.extend_from_slice(&be32(v));
        }
        data.extend_from_slice(&be32(0)); // dateStyle
        data.extend_from_slice(&be32(0)); // euroDates

        let desc = DbosTupleDesc::parse(&data, None).unwrap();
        assert_eq!(desc.num_fields, 2);
        assert_eq!(desc.field_type, vec![3, 16]);

        // Row: 2 reserved + null bitmap(1B) + fixed(4B at offset 2) + varying
        // phys fields: f0 → bit0 of byte0, f1 → bit1
        let mut row = vec![0u8, 0];
        row.push(0b0000_0000); // no nulls (bit set = null)
        row.extend_from_slice(&42i32.to_le_bytes()); // fixed field at offset 2? field_offset=2
                                                     // careful: fixed field offset 2 lands on the bitmap byte — offset is
                                                     // relative to row start; the server lays this out, we mirror it.
                                                     // var field: len(u16 incl self) + data
        row.extend_from_slice(&6u16.to_le_bytes());
        row.extend_from_slice(b"abcd");

        // Re-parse with corrected offsets: put int at offset 2 means it
        // overlaps bitmap; adjust descriptor to offset 3 instead.
        let mut data2 = data.clone();
        data2[36 + 12..36 + 16].copy_from_slice(&be32(3));
        let desc = DbosTupleDesc::parse(&data2, None).unwrap();
        let mut row = vec![0u8, 0];
        row.push(0);
        row.extend_from_slice(&42i32.to_le_bytes());
        row.extend_from_slice(&6u16.to_le_bytes());
        row.extend_from_slice(b"abcd");
        let vals = desc.parse_row(&row).unwrap();
        assert_eq!(vals[0], NzValue::Int4(42));
        assert_eq!(vals[1], NzValue::Text("abcd".into()));

        let mut reused = vals;
        let text_ptr = match &reused[1] {
            NzValue::Text(text) => text.as_ptr(),
            other => panic!("expected text value, got {other:?}"),
        };
        let mut row2 = vec![0u8, 0, 0];
        row2.extend_from_slice(&43i32.to_le_bytes());
        row2.extend_from_slice(&6u16.to_le_bytes());
        row2.extend_from_slice(b"efgh");
        desc.parse_row_into(&row2, &mut reused).unwrap();
        assert_eq!(reused[0], NzValue::Int4(43));
        assert_eq!(reused[1], NzValue::Text("efgh".into()));
        match &reused[1] {
            NzValue::Text(text) => assert_eq!(text.as_ptr(), text_ptr),
            other => panic!("expected text value, got {other:?}"),
        }
    }

    #[test]
    fn binary_numeric_uses_decimal_then_exact_fallback() {
        fn words(value: u128) -> Vec<u8> {
            let mut raw = value;
            let mut out = vec![0u8; 16];
            for i in (0..4).rev() {
                out[i * 4..i * 4 + 4].copy_from_slice(&(raw as u32).to_le_bytes());
                raw >>= 32;
            }
            out
        }

        let decimal_unscaled = 12_345_678_901_234_567_887_654_321u128;
        let wide_unscaled = 92_328_162_514_264_337_598_743_950_777u128;
        let desc = DbosTupleDesc {
            nulls_allowed: 0,
            num_fields: 2,
            field_type: vec![nz_type::NZ_TYPE_NUMERIC; 2],
            field_size: vec![(26 << 8) | 8, (38 << 8) | 8],
            field_true_size: vec![16, 16],
            field_offset: vec![2, 18],
            field_null_allowed: vec![false; 2],
            field_null_byte_offset: vec![0; 2],
            field_null_bit_mask: vec![0; 2],
            field_fixed_size: vec![16, 16],
            ..Default::default()
        };
        let mut row = vec![0u8, 0];
        row.extend_from_slice(&words(decimal_unscaled));
        row.extend_from_slice(&words(wide_unscaled));

        let values = desc.parse_row(&row).unwrap();
        assert_eq!(
            values[0],
            NzValue::Decimal("123456789012345678.87654321".parse().unwrap())
        );
        assert_eq!(
            values[1],
            NzValue::Numeric("923281625142643375987.43950777".into())
        );
    }

    fn desc(type_oid: i32, type_len: i16, type_mod: i32) -> ColumnDesc {
        ColumnDesc {
            name: "c".into(),
            type_oid,
            type_len,
            type_mod,
            format: 0,
        }
    }

    #[test]
    fn type_names_match_the_reference_table() {
        for (oid, expected) in [
            (16, "BOOL"),
            (17, "BYTEA"),
            (19, "NAME"),
            (20, "BIGINT"),
            (21, "SMALLINT"),
            (23, "INTEGER"),
            (25, "TEXT"),
            (26, "OID"),
            (700, "REAL"),
            (701, "DOUBLE"),
            (702, "ABSTIME"),
            (1042, "CHAR"),
            (1043, "VARCHAR"),
            (1082, "DATE"),
            (1083, "TIME"),
            (1114, "TIMESTAMP"),
            (1184, "TIMESTAMPTZ"),
            (1186, "INTERVAL"),
            (1266, "TIMETZ"),
            (1700, "NUMERIC"),
            (2500, "BYTEINT"),
            (2522, "NCHAR"),
            (2530, "NVARCHAR"),
        ] {
            assert_eq!(desc(oid, -1, -1).type_name(), expected, "oid {oid}");
        }
        // 15 is Netezza's internal CHAR; the C# and Node tables both map it.
        assert_eq!(desc(15, -1, -1).type_name(), "CHAR");
        // Unknown OIDs are reported rather than silently mislabelled.
        assert_eq!(desc(99999, -1, -1).type_name(), "OID(99999)");
    }

    #[test]
    fn declared_type_names_use_the_typmod_length() {
        // Live wire values: `type_len` is -1 for character types and the
        // declared size rides in `type_mod` as length + 16.
        assert_eq!(desc(1043, -1, 32 + 16).declared_type_name(), "VARCHAR(32)");
        assert_eq!(desc(2530, -1, 20 + 16).declared_type_name(), "NVARCHAR(20)");
        assert_eq!(desc(1042, -1, 5 + 16).declared_type_name(), "CHAR(5)");
        assert_eq!(desc(2522, -1, 6 + 16).declared_type_name(), "NCHAR(6)");
        // NUMERIC(10,4): type_mod = ((10 << 16) | 4) + 16.
        assert_eq!(
            desc(1700, 19, ((10 << 16) | 4) + 16).declared_type_name(),
            "NUMERIC(10,4)"
        );

        // Fixed-width types stay bare. BYTEINT in particular must not grow a
        // bogus "(1)" from its 1-byte storage width.
        assert_eq!(desc(2500, 1, -1).declared_type_name(), "BYTEINT");
        assert_eq!(desc(23, 4, -1).declared_type_name(), "INTEGER");
        assert_eq!(desc(16, 1, -1).declared_type_name(), "BOOL");
        // TEXT carries a typmod but is not a sized character type.
        assert_eq!(desc(25, -1, 17).declared_type_name(), "TEXT");
        // NUMERIC without a precision keeps the base name.
        assert_eq!(desc(1700, -1, -1).declared_type_name(), "NUMERIC");
        assert_eq!(desc(1700, -1, 16).declared_type_name(), "NUMERIC");
        // A named character type with no declared length stays bare too.
        assert_eq!(desc(1043, -1, -1).declared_type_name(), "VARCHAR");
    }

    #[test]
    fn null_bitmaps() {
        let mut data = Vec::new();
        for v in [0i32, 1, 1, 2, 1, 0, 8, 64, 1] {
            data.extend_from_slice(&be32(v));
        }
        // Field 0: fixed INT4 at offset 3, null-allowed, physField 0
        for v in [3i32, 4, 4, 3, 0, 0, 1, 4, 0] {
            data.extend_from_slice(&be32(v));
        }
        data.extend_from_slice(&be32(0));
        data.extend_from_slice(&be32(0));
        let desc = DbosTupleDesc::parse(&data, None).unwrap();

        let mut row = vec![0u8, 0];
        row.push(0b0000_0001); // bit0 set → field 0 IS null
        row.extend_from_slice(&0i32.to_le_bytes());
        let vals = desc.parse_row(&row).unwrap();
        assert_eq!(vals[0], NzValue::Null);

        let mut row = vec![0u8, 0];
        row.push(0);
        row.extend_from_slice(&5i32.to_le_bytes());
        let vals = desc.parse_row(&row).unwrap();
        assert_eq!(vals[0], NzValue::Int4(5));
    }
}
