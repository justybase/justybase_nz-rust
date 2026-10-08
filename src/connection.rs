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

//! Synchronous Netezza connection — faithful port of the Node driver
//! `NzConnection.ts` (itself ported from C# `NzConnection.cs` / nzpy).
//!
//! Public API is deliberately shaped after [`tokio-postgres`](https://docs.rs/tokio-postgres)
//! (the natural Rust analog of the C# author's `npgsql` starting point):
//!
//! - [`NzConnection::query`] / [`NzConnection::query_one`] /
//!   [`NzConnection::query_opt`] buffer rows as `Vec<Row>` (tokio-postgres style),
//! - [`NzConnection::execute`] returns affected rows,
//! - [`Row::get`] / [`Row::try_get`] extract typed values via [`FromSql`],
//! - query parameters are `&[&dyn ToSql]` and are escaped client-side
//!   (Netezza simple-query path has no server-side bind — same as all three
//!   reference drivers),
//! - [`NzConnection::transaction`] runs a `BEGIN`/`COMMIT` closure with
//!   `ROLLBACK` on error.
//!
//! ADO.NET-style access (C# parity) is available too: [`NzConnection::query`]
//! also returns the full multi-result-set [`QueryResult`], and
//! [`NzConnection::execute_reader`] hands back an [`NzDataReader`](crate::reader::NzDataReader).

use crate::buffer::ReadBuffer;
use crate::cancel::send_cancel;
use crate::config::NzConnectionConfig;
use crate::error::{parse_backend_error_fields, validate_protocol_length, NzError, NzResult};
use crate::handshake::{handshake, NzStream};
use crate::messages::{
    code, nz_type, parse_command_complete_rows, parse_transaction_state, TransactionState,
};
use crate::params::{substitute_bound_parameters, substitute_parameters, NzParameter};
use crate::tuple_desc::{parse_row_description, ColumnDesc, DbosTupleDesc};
use crate::types::text::{build_simple_query_packet, parse_text_data_row_into};
use crate::types::value::{FromSql, FromSqlRaw, NzValue, RawValue, ToSql};
use bytes::Bytes;
use std::collections::HashMap;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::AsyncRead;

// ---------------------------------------------------------------------------
// Virtual import sources (external-table `l` import)
// ---------------------------------------------------------------------------

pub(crate) enum ImportSource {
    Bytes(Vec<u8>),
    Reader(Box<dyn Read + Send>),
    AsyncReader(Pin<Box<dyn AsyncRead + Send>>),
}

fn import_registry() -> &'static Mutex<HashMap<String, ImportSource>> {
    static REG: OnceLock<Mutex<HashMap<String, ImportSource>>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register an in-memory payload for an external-table import filename.
///
/// When the appliance asks to import `id` (the `DATAOBJECT` path sent in the
/// SQL text), the driver streams these bytes instead of opening a file —
/// the Rust analog of `NzConnection.registerImportStream` in the Node driver.
pub fn register_import_data(id: &str, data: Vec<u8>) {
    if let Ok(mut reg) = import_registry().lock() {
        reg.insert(id.to_string(), ImportSource::Bytes(data));
    }
}

/// Register a one-shot synchronous reader for an external-table import.
///
/// The reader is consumed only when the appliance requests `id`. It is read
/// in bounded chunks, so the entire payload need not reside in memory.
/// Use this with [`NzConnection`]; use [`register_async_import_reader`] with
/// the native Tokio client.
pub fn register_import_reader(id: &str, reader: impl Read + Send + 'static) {
    if let Ok(mut reg) = import_registry().lock() {
        reg.insert(id.to_string(), ImportSource::Reader(Box::new(reader)));
    }
}

/// Register a one-shot Tokio reader for an external-table import.
pub fn register_async_import_reader(id: &str, reader: impl AsyncRead + Send + 'static) {
    if let Ok(mut reg) = import_registry().lock() {
        reg.insert(id.to_string(), ImportSource::AsyncReader(Box::pin(reader)));
    }
}

/// Remove a previously registered virtual import payload.
pub fn unregister_import_data(id: &str) {
    if let Ok(mut reg) = import_registry().lock() {
        reg.remove(id);
    }
}

pub(crate) fn take_import_source(id: &str) -> Option<ImportSource> {
    if cfg!(feature = "compat") {
        import_registry().lock().ok()?.remove(id)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Row — tokio-postgres style
// ---------------------------------------------------------------------------

/// One result row with column metadata and lazily materialized values.
///
/// Text and DBOS rows retain their validated wire payload until values are
/// requested. Compatibility rows created with [`Row::new`] keep owned values
/// directly.
///
/// Values are extracted with [`Row::get`] / [`Row::try_get`] via [`FromSql`],
/// exactly like `tokio_postgres::Row`:
///
/// ```no_run
/// # #[cfg(feature = "compat")]
/// # fn f(reader_row: nz_rust::connection::Row) -> nz_rust::error::NzResult<()> {
/// let id: i32 = reader_row.try_get(0)?;
/// let name: Option<String> = reader_row.try_get("name")?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Row {
    metadata: Arc<RowMetadata>,
    storage: RowStorage,
}

#[derive(Debug)]
pub(crate) struct RowMetadata {
    columns: Arc<[ColumnDesc]>,
    name_index: OnceLock<hashbrown::HashMap<AsciiColumnName, usize>>,
}

#[derive(Debug)]
struct AsciiColumnName(Box<str>);

struct ColumnNameLookup<'a>(&'a str);

impl PartialEq for AsciiColumnName {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(&other.0)
    }
}
impl Eq for AsciiColumnName {}
impl Hash for AsciiColumnName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for byte in self.0.bytes() {
            state.write_u8(byte.to_ascii_lowercase());
        }
    }
}
impl Hash for ColumnNameLookup<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for byte in self.0.bytes() {
            state.write_u8(byte.to_ascii_lowercase());
        }
    }
}
impl hashbrown::Equivalent<AsciiColumnName> for ColumnNameLookup<'_> {
    fn equivalent(&self, key: &AsciiColumnName) -> bool {
        self.0.eq_ignore_ascii_case(&key.0)
    }
}

impl RowMetadata {
    pub(crate) fn new(columns: Arc<[ColumnDesc]>) -> Self {
        Self {
            columns,
            name_index: OnceLock::new(),
        }
    }

    fn position(&self, name: &str) -> Option<usize> {
        self.name_index
            .get_or_init(|| {
                let mut index = hashbrown::HashMap::with_capacity(self.columns.len());
                for (position, column) in self.columns.iter().enumerate() {
                    index
                        .entry(AsciiColumnName(column.name.clone().into_boxed_str()))
                        .or_insert(position);
                }
                index
            })
            .get(&ColumnNameLookup(name))
            .copied()
    }
}

#[derive(Debug, Clone)]
enum RowStorage {
    Values {
        values: Vec<NzValue>,
        /// Binary descriptor for DBOS-decoded rows, so `type_info()` keeps
        /// reporting the wire type after eager decoding (C#/Node parity).
        /// `None` for text rows and `Row::new` compatibility rows.
        dbos: Option<Arc<DbosTupleDesc>>,
    },
    Raw(Arc<RawRowData>),
}

#[derive(Debug)]
struct RawRowData {
    payload: Bytes,
    kind: RawRowKind,
    decoded: OnceLock<Vec<NzValue>>,
    cells: OnceLock<Box<[OnceLock<NzValue>]>>,
    layout_progress: Mutex<RowLayoutProgress>,
}

#[derive(Debug)]
enum RawRowKind {
    Text,
    Dbos { descriptor: Arc<DbosTupleDesc> },
}

#[derive(Debug, Clone, Copy)]
struct RowFieldSpan {
    /// For text rows this is the value start. For DBOS varying fields it is
    /// the length-prefix start, as required by `parse_field_into`.
    start: usize,
    /// End of the encoded value, excluding DBOS alignment padding.
    end: usize,
    is_null: bool,
}

fn pack_row_span(start: usize, end: usize) -> NzResult<u64> {
    let start = u32::try_from(start)
        .map_err(|_| NzError::Protocol("row field offset is out of range".into()))?;
    let end = u32::try_from(end)
        .map_err(|_| NzError::Protocol("row field end is out of range".into()))?;
    Ok(((start as u64) << 32) | end as u64)
}

fn unpack_row_span(packed: u64, is_null: bool) -> RowFieldSpan {
    RowFieldSpan {
        start: (packed >> 32) as u32 as usize,
        end: packed as u32 as usize,
        is_null,
    }
}

#[derive(Debug, Default)]
struct RowLayoutProgress {
    cursor: usize,
    // Each checked start/end pair is packed into 64 bits. DBOS spans are
    // discovered during structural validation; text spans remain progressive.
    spans: Vec<u64>,
}

impl RawRowData {
    fn value(&self, index: usize, columns: &[ColumnDesc]) -> NzResult<&NzValue> {
        if let Some(values) = self.decoded.get() {
            return values
                .get(index)
                .ok_or_else(|| NzError::Config("row index out of range".into()));
        }
        let cells = self
            .cells
            .get_or_init(|| (0..columns.len()).map(|_| OnceLock::new()).collect());
        let cell = cells
            .get(index)
            .ok_or_else(|| NzError::Config("row index out of range".into()))?;
        if cell.get().is_none() {
            let value = self.decode_value(index, columns).map_err(|error| {
                NzError::Protocol(format!(
                    "cannot decode column {index} (type {}): {error}",
                    columns[index].type_oid
                ))
            })?;
            let _ = cell.set(value);
        }
        Ok(cell.get().expect("cell initialized above"))
    }

    fn text_span(&self, index: usize, columns: &[ColumnDesc]) -> NzResult<RowFieldSpan> {
        if index >= columns.len() {
            return Err(NzError::Config("row index out of range".into()));
        }
        let mut progress = self
            .layout_progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.ensure_text_spans(&mut progress, index + 1)?;
        let byte = *self
            .payload
            .get(index / 8)
            .ok_or_else(|| NzError::Protocol("DataRow null bitmap is truncated".into()))?;
        let bit = 7 - (index % 8);
        Ok(unpack_row_span(
            progress.spans[index],
            byte & (1 << bit) == 0,
        ))
    }

    fn ensure_text_spans(&self, progress: &mut RowLayoutProgress, count: usize) -> NzResult<()> {
        while progress.spans.len() < count {
            let field_index = progress.spans.len();
            let byte = self.payload[field_index / 8];
            let bit = 7 - (field_index % 8);
            let (value_start, value_end) = if byte & (1 << bit) == 0 {
                (progress.cursor, progress.cursor)
            } else {
                let prefix_end = progress
                    .cursor
                    .checked_add(4)
                    .ok_or_else(|| NzError::Protocol("DataRow field prefix overflow".into()))?;
                let encoded = i32::from_be_bytes(
                    self.payload
                        .get(progress.cursor..prefix_end)
                        .ok_or_else(|| {
                            NzError::Protocol(format!(
                                "DataRow column {field_index} length is truncated"
                            ))
                        })?
                        .try_into()
                        .unwrap(),
                );
                let value_len = usize::try_from(encoded - 4).map_err(|_| {
                    NzError::Protocol(format!("DataRow column {field_index} length is invalid"))
                })?;
                let value_start = prefix_end;
                let end = value_start.checked_add(value_len).ok_or_else(|| {
                    NzError::Protocol(format!(
                        "DataRow column {field_index} value length overflow"
                    ))
                })?;
                if end > self.payload.len() {
                    return Err(NzError::Protocol(format!(
                        "DataRow column {field_index} value length is invalid"
                    )));
                }
                progress.cursor = end;
                (value_start, end)
            };
            progress.spans.push(pack_row_span(value_start, value_end)?);
        }
        Ok(())
    }

    fn dbos_span(&self, index: usize, descriptor: &DbosTupleDesc) -> NzResult<RowFieldSpan> {
        if index >= descriptor.num_fields {
            return Err(NzError::Config(format!(
                "row field index {index} is out of range"
            )));
        }
        if descriptor.is_field_null(&self.payload, index) {
            return Ok(RowFieldSpan {
                start: 0,
                end: 0,
                is_null: true,
            });
        }
        if descriptor.field_fixed_size[index] != 0 {
            let start = usize::try_from(descriptor.field_offset[index]).map_err(|_| {
                NzError::Protocol(format!("DBOS fixed field {index} has an invalid offset"))
            })?;
            let end = start
                .checked_add(descriptor.fixed_width(index)?)
                .ok_or_else(|| NzError::Protocol("DBOS field offset overflow".into()))?;
            return Ok(RowFieldSpan {
                start,
                end,
                is_null: false,
            });
        }
        // Older descriptors can describe a non-null field with no varying
        // fields in the row. The established DBOS parser locates that field
        // at the start of the fixed-field area in this case.
        if descriptor.num_varying_fields == 0 {
            let start = usize::try_from(descriptor.fixed_fields_size)
                .map_err(|_| NzError::Protocol("DBOS fixed-field area offset is invalid".into()))?;
            if start > self.payload.len() {
                return Err(NzError::Protocol(format!(
                    "DBOS field {index} starts outside the row"
                )));
            }
            return Ok(RowFieldSpan {
                start,
                end: self.payload.len(),
                is_null: false,
            });
        }
        let varying_index = usize::try_from(descriptor.field_offset[index]).map_err(|_| {
            NzError::Protocol(format!("DBOS varying field {index} has an invalid index"))
        })?;
        let mut progress = self
            .layout_progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.ensure_dbos_spans(&mut progress, varying_index + 1, descriptor)?;
        let packed = progress
            .spans
            .get(varying_index)
            .copied()
            .ok_or_else(|| NzError::Protocol("DBOS row layout was not discovered".into()))?;
        Ok(unpack_row_span(packed, false))
    }

    fn ensure_dbos_spans(
        &self,
        progress: &mut RowLayoutProgress,
        count: usize,
        descriptor: &DbosTupleDesc,
    ) -> NzResult<()> {
        while progress.spans.len() < count {
            let field = progress.spans.len();
            if field >= descriptor.num_varying_fields.max(0) as usize {
                return Err(NzError::Protocol(format!(
                    "DBOS varying field {field} index is invalid"
                )));
            }
            let prefix_end = progress
                .cursor
                .checked_add(2)
                .ok_or_else(|| NzError::Protocol("DBOS varying field prefix overflow".into()))?;
            let prefix = self
                .payload
                .get(progress.cursor..prefix_end)
                .ok_or_else(|| {
                    NzError::Protocol(format!(
                        "DBOS varying field {field} length prefix is truncated"
                    ))
                })?;
            let encoded = u16::from_le_bytes(prefix.try_into().unwrap()) as usize;
            let value_end = progress
                .cursor
                .checked_add(encoded)
                .ok_or_else(|| NzError::Protocol("DBOS varying field length overflow".into()))?;
            if encoded < 2 || value_end > self.payload.len() {
                return Err(NzError::Protocol(format!(
                    "DBOS varying field {field} length is invalid"
                )));
            }
            let next_cursor = value_end + usize::from(!encoded.is_multiple_of(2));
            if next_cursor > self.payload.len() {
                return Err(NzError::Protocol(format!(
                    "DBOS varying field {field} padding is truncated"
                )));
            }
            let packed = pack_row_span(progress.cursor, value_end)?;
            progress.cursor = next_cursor;
            progress.spans.push(packed);
        }
        Ok(())
    }

    fn decode_value(&self, index: usize, columns: &[ColumnDesc]) -> NzResult<NzValue> {
        match &self.kind {
            RawRowKind::Text => {
                let span = self.text_span(index, columns)?;
                if span.is_null {
                    return Ok(NzValue::Null);
                }
                let bytes = &self.payload[span.start..span.end];
                let text = std::str::from_utf8(bytes)
                    .map_err(|e| NzError::Protocol(format!("invalid UTF-8 field: {e}")))?;
                let column = columns.get(index).ok_or_else(|| {
                    NzError::Protocol("text row has more fields than its description".into())
                })?;
                crate::types::text::try_parse_text_value(text, column.type_oid, column.type_mod)
            }
            RawRowKind::Dbos { descriptor } => {
                let field = self.dbos_span(index, descriptor)?;
                if field.is_null {
                    return Ok(NzValue::Null);
                }
                let mut value = NzValue::Null;
                descriptor.parse_field_into(&self.payload, field.start, index, &mut value)?;
                Ok(value)
            }
        }
    }

    fn decode_all(&self, columns: &[ColumnDesc]) -> NzResult<Vec<NzValue>> {
        match &self.kind {
            RawRowKind::Text => {
                let mut progress = self
                    .layout_progress
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                self.ensure_text_spans(&mut progress, columns.len())?;
                columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| {
                        let byte = self.payload[index / 8];
                        let bit = 7 - (index % 8);
                        let span = unpack_row_span(progress.spans[index], byte & (1 << bit) == 0);
                        if span.is_null {
                            return Ok(NzValue::Null);
                        }
                        let bytes = &self.payload[span.start..span.end];
                        let text = std::str::from_utf8(bytes)
                            .map_err(|e| NzError::Protocol(format!("invalid UTF-8 field: {e}")))?;
                        crate::types::text::try_parse_text_value(
                            text,
                            column.type_oid,
                            column.type_mod,
                        )
                    })
                    .collect()
            }
            RawRowKind::Dbos { descriptor } => {
                if columns.len() > descriptor.num_fields {
                    return Err(NzError::Config(format!(
                        "row has {} columns but its descriptor declares {} fields",
                        columns.len(),
                        descriptor.num_fields
                    )));
                }
                let varying_count = descriptor.num_varying_fields.max(0) as usize;
                let mut progress = self
                    .layout_progress
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                self.ensure_dbos_spans(&mut progress, varying_count, descriptor)?;
                (0..columns.len())
                    .map(|index| {
                        if descriptor.is_field_null(&self.payload, index) {
                            return Ok(NzValue::Null);
                        }
                        let start = if descriptor.field_fixed_size[index] != 0 {
                            usize::try_from(descriptor.field_offset[index]).map_err(|_| {
                                NzError::Protocol(format!(
                                    "DBOS fixed field {index} has an invalid offset"
                                ))
                            })?
                        } else if varying_count == 0 {
                            usize::try_from(descriptor.fixed_fields_size).map_err(|_| {
                                NzError::Protocol("DBOS fixed-field area offset is invalid".into())
                            })?
                        } else {
                            let varying_index = usize::try_from(descriptor.field_offset[index])
                                .map_err(|_| {
                                    NzError::Protocol(format!(
                                        "DBOS varying field {index} has an invalid index"
                                    ))
                                })?;
                            progress
                                .spans
                                .get(varying_index)
                                .map(|span| (span >> 32) as u32 as usize)
                                .ok_or_else(|| {
                                    NzError::Protocol("DBOS row layout was not discovered".into())
                                })?
                        };
                        let mut value = NzValue::Null;
                        descriptor.parse_field_into(&self.payload, start, index, &mut value)?;
                        Ok(value)
                    })
                    .collect()
            }
        }
    }

    fn field_bytes(&self, index: usize, columns: &[ColumnDesc]) -> NzResult<Option<&[u8]>> {
        match &self.kind {
            RawRowKind::Text => {
                let span = self.text_span(index, columns)?;
                if span.is_null {
                    Ok(None)
                } else {
                    self.payload
                        .get(span.start..span.end)
                        .map(Some)
                        .ok_or_else(|| NzError::Protocol("text row field is out of bounds".into()))
                }
            }
            RawRowKind::Dbos { descriptor } => {
                let field = self.dbos_span(index, descriptor)?;
                if field.is_null {
                    Ok(None)
                } else if descriptor.field_fixed_size.get(index).copied().unwrap_or(0) != 0 {
                    self.payload
                        .get(field.start..field.end)
                        .map(Some)
                        .ok_or_else(|| {
                            NzError::Protocol("DBOS fixed field is out of bounds".into())
                        })
                } else if descriptor.num_varying_fields == 0 {
                    self.payload
                        .get(field.start..field.end)
                        .map(Some)
                        .ok_or_else(|| NzError::Protocol("DBOS field is out of bounds".into()))
                } else {
                    let prefix_end = field
                        .start
                        .checked_add(2)
                        .ok_or_else(|| NzError::Protocol("DBOS field prefix overflow".into()))?;
                    let encoded = u16::from_le_bytes(
                        self.payload
                            .get(field.start..prefix_end)
                            .ok_or_else(|| {
                                NzError::Protocol("DBOS field prefix is truncated".into())
                            })?
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    let value_start = prefix_end;
                    let end = field
                        .start
                        .checked_add(encoded)
                        .ok_or_else(|| NzError::Protocol("DBOS field length overflow".into()))?;
                    self.payload.get(value_start..end).map(Some).ok_or_else(|| {
                        NzError::Protocol("DBOS varying field is out of bounds".into())
                    })
                }
            }
        }
    }
}

/// Column metadata with explicitly separate PostgreSQL OID and DBOS wire type.
#[derive(Debug, Clone, Copy)]
pub struct TypeInfo<'a> {
    pub column: &'a ColumnDesc,
    pub dbos_type: Option<i32>,
    pub wire_format: u8,
}

impl Row {
    pub fn type_info<I: RowIndex>(&self, index: I) -> NzResult<TypeInfo<'_>> {
        let position = index
            .position(self)
            .ok_or_else(|| NzError::Config("column index out of range".into()))?;
        let column = &self.columns()[position];
        let dbos_type = match &self.storage {
            RowStorage::Values { dbos, .. } => dbos.as_ref().map(|desc| desc.field_type[position]),
            RowStorage::Raw(raw) => match &raw.kind {
                RawRowKind::Dbos { descriptor, .. } => Some(descriptor.field_type[position]),
                _ => None,
            },
        };
        Ok(TypeInfo {
            column,
            dbos_type,
            wire_format: if dbos_type.is_some() {
                1
            } else {
                column.format
            },
        })
    }

    pub fn new(columns: Vec<ColumnDesc>, values: Vec<NzValue>) -> Self {
        Row {
            metadata: Arc::new(RowMetadata::new(Arc::from(columns))),
            storage: RowStorage::Values { values, dbos: None },
        }
    }

    /// Eager row sharing the result set's column metadata (no per-row `String`
    /// clones). Used by the buffered protocol loop, mirroring the C# reader
    /// which decodes a full row into `object[]` on `Read()`.
    pub(crate) fn from_shared(columns: Arc<[ColumnDesc]>, values: Vec<NzValue>) -> Self {
        Row {
            metadata: Arc::new(RowMetadata::new(columns)),
            storage: RowStorage::Values { values, dbos: None },
        }
    }

    pub(crate) fn from_shared_metadata(metadata: Arc<RowMetadata>, values: Vec<NzValue>) -> Self {
        Row {
            metadata,
            storage: RowStorage::Values { values, dbos: None },
        }
    }

    /// Eager binary row keeping its descriptor for `type_info()` parity with
    /// the previous lazy representation.
    pub(crate) fn from_shared_dbos(
        columns: Arc<[ColumnDesc]>,
        values: Vec<NzValue>,
        descriptor: Arc<DbosTupleDesc>,
    ) -> Self {
        Row {
            metadata: Arc::new(RowMetadata::new(columns)),
            storage: RowStorage::Values {
                values,
                dbos: Some(descriptor),
            },
        }
    }

    pub(crate) fn from_shared_dbos_metadata(
        metadata: Arc<RowMetadata>,
        values: Vec<NzValue>,
        descriptor: Arc<DbosTupleDesc>,
    ) -> Self {
        Row {
            metadata,
            storage: RowStorage::Values {
                values,
                dbos: Some(descriptor),
            },
        }
    }

    pub(crate) fn from_text_raw(
        columns: Arc<[ColumnDesc]>,
        payload: impl Into<Bytes>,
    ) -> NzResult<Self> {
        Self::from_text_raw_with_metadata(Arc::new(RowMetadata::new(columns)), payload)
    }

    pub(crate) fn from_text_raw_with_metadata(
        metadata: Arc<RowMetadata>,
        payload: impl Into<Bytes>,
    ) -> NzResult<Self> {
        let payload = payload.into();
        crate::types::text::validate_text_row(&payload, &metadata.columns)
            .map_err(NzError::Protocol)?;
        let cursor = metadata.columns.len().div_ceil(8);
        Ok(Self {
            metadata,
            storage: RowStorage::Raw(Arc::new(RawRowData {
                payload,
                kind: RawRowKind::Text,
                decoded: OnceLock::new(),
                cells: OnceLock::new(),
                layout_progress: Mutex::new(RowLayoutProgress {
                    cursor,
                    spans: Vec::new(),
                }),
            })),
        })
    }

    pub(crate) fn from_dbos_raw(
        columns: Arc<[ColumnDesc]>,
        payload: impl Into<Bytes>,
        descriptor: Arc<DbosTupleDesc>,
    ) -> NzResult<Self> {
        Self::from_dbos_raw_with_metadata(Arc::new(RowMetadata::new(columns)), payload, descriptor)
    }

    pub(crate) fn from_dbos_raw_with_metadata(
        metadata: Arc<RowMetadata>,
        payload: impl Into<Bytes>,
        descriptor: Arc<DbosTupleDesc>,
    ) -> NzResult<Self> {
        let payload = payload.into();
        let varying_starts = descriptor.validate_row_layout(&payload)?;
        let cursor = usize::try_from(descriptor.fixed_fields_size.max(0)).unwrap_or(0);
        Ok(Self {
            metadata,
            storage: RowStorage::Raw(Arc::new(RawRowData {
                payload,
                kind: RawRowKind::Dbos {
                    descriptor: descriptor.clone(),
                },
                decoded: OnceLock::new(),
                cells: OnceLock::new(),
                layout_progress: Mutex::new(RowLayoutProgress {
                    cursor,
                    spans: varying_starts,
                }),
            })),
        })
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        match &self.storage {
            RowStorage::Raw(raw) => {
                // DBOS varying starts are prepared by structural validation;
                // text offsets and decoded cells remain progressive.
                let layout_bytes = match &raw.kind {
                    RawRowKind::Text => 0,
                    RawRowKind::Dbos { descriptor } => descriptor
                        .num_varying_fields
                        .max(0) as usize
                        * std::mem::size_of::<u64>(),
                };
                raw.payload
                    .len()
                    .saturating_add(std::mem::size_of::<RawRowData>() + std::mem::size_of::<Row>())
                    .saturating_add(layout_bytes)
            }
            RowStorage::Values { values, .. } => values.iter().map(|value| match value {
                NzValue::Text(s) | NzValue::Numeric(s) | NzValue::Date(s) | NzValue::Time(s) | NzValue::Timestamp(s) | NzValue::Timetz(s) | NzValue::Interval(s) => s.len(),
                NzValue::Bytea(bytes) => bytes.len(), _ => 0,
            } + std::mem::size_of::<NzValue>()).sum(),
        }
    }

    pub fn columns(&self) -> &[ColumnDesc] {
        &self.metadata.columns
    }

    pub fn len(&self) -> usize {
        self.metadata.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns().is_empty()
    }

    pub fn values(&self) -> &[NzValue] {
        self.try_values()
            .unwrap_or_else(|e| panic!("row.values failed: {e}"))
    }

    /// Materialize all fields, preserving decoding errors rather than inventing NULLs.
    pub fn try_values(&self) -> NzResult<&[NzValue]> {
        match &self.storage {
            RowStorage::Values { values, .. } => Ok(values),
            RowStorage::Raw(raw) => {
                if raw.decoded.get().is_none() {
                    let values = raw.decode_all(self.columns())?;
                    let _ = raw.decoded.set(values);
                }
                Ok(raw.decoded.get().expect("row initialized above"))
            }
        }
    }

    /// Raw value by ordinal or name. Panics when out of range / unknown —
    /// use [`Row::try_get_raw`] for a checked variant.
    pub fn raw<I: RowIndex>(&self, idx: I) -> &NzValue {
        match idx.position(self) {
            Some(p) => &self.values()[p],
            None => panic!("row index out of range: {}", idx.describe()),
        }
    }

    /// Borrow the compatibility value by index. This method is intended for
    /// Netezza-specific metadata adapters; typed callers should use
    /// [`Row::try_get`] or [`Row::try_get_raw_value`].
    pub fn try_get_value<I: RowIndex>(&self, idx: I) -> NzResult<&NzValue> {
        match idx.position(self) {
            Some(p) => match &self.storage {
                RowStorage::Values { values, .. } => values
                    .get(p)
                    .ok_or_else(|| NzError::Config("row values do not match columns".into())),
                RowStorage::Raw(raw) => raw.value(p, self.columns()),
            },
            None => Err(NzError::Config(format!(
                "row index out of range: {}",
                idx.describe()
            ))),
        }
    }

    /// Borrow the decoded compatibility value by index.
    ///
    /// This is the original public API and remains separate from the lazy raw
    /// accessors below for source compatibility with downstream `FromSql`
    /// implementations.
    pub fn try_get_raw<I: RowIndex>(&self, idx: I) -> NzResult<&NzValue> {
        self.try_get_value(idx)
    }

    /// Borrow raw field bytes and wire metadata without materializing a new
    /// [`NzValue`].
    pub fn try_get_raw_value<I: RowIndex>(&self, idx: I) -> NzResult<RawValue<'_>> {
        let position = idx.position(self).ok_or_else(|| {
            NzError::Config(format!("row index out of range: {}", idx.describe()))
        })?;
        let column = self.columns().get(position).ok_or_else(|| {
            NzError::Config(format!("row index out of range: ordinal {position}"))
        })?;
        match &self.storage {
            RowStorage::Values { values, dbos } => {
                let value = values
                    .get(position)
                    .ok_or_else(|| NzError::Protocol("row values do not match columns".into()))?;
                let bytes = eager_cell_bytes(value, dbos.as_deref(), column, position);
                Ok(RawValue::from_parts(
                    bytes,
                    Some(value),
                    column.type_oid,
                    column.type_mod,
                    column.format,
                ))
            }
            RowStorage::Raw(raw) => {
                let value = RawValue::from_parts(
                    raw.field_bytes(position, self.columns())?,
                    None,
                    column.type_oid,
                    column.type_mod,
                    column.format,
                );
                match &raw.kind {
                    RawRowKind::Dbos { descriptor } => {
                        let field = raw.dbos_span(position, descriptor)?;
                        Ok(value.with_dbos(descriptor, &raw.payload, field.start, position))
                    }
                    RawRowKind::Text => Ok(value),
                }
            }
        }
    }

    /// Typed extraction. Panics on type mismatch (tokio-postgres `get`
    /// semantics); use [`Row::try_get`] for a `Result`.
    pub fn get<I: RowIndex, T: FromSql>(&self, idx: I) -> T {
        self.try_get(idx)
            .unwrap_or_else(|e| panic!("row.get failed: {e}"))
    }

    /// Checked typed extraction.
    pub fn try_get<I: RowIndex, T: FromSql>(&self, idx: I) -> NzResult<T> {
        T::from_raw(self.try_get_raw_value(idx)?)
    }

    /// Panicking typed extraction through the lazy raw-value interface.
    pub fn get_raw<'a, I: RowIndex, T: FromSqlRaw<'a>>(&'a self, idx: I) -> T {
        self.try_get_raw_typed(idx)
            .unwrap_or_else(|e| panic!("row.get_raw failed: {e}"))
    }

    /// Checked typed extraction through the lazy raw-value interface.
    pub fn try_get_raw_typed<'a, I: RowIndex, T: FromSqlRaw<'a>>(&'a self, idx: I) -> NzResult<T> {
        T::from_sql_raw(self.try_get_raw_value(idx)?)
    }

    /// Row as `name → value` pairs in column order.
    pub fn as_map(&self) -> Vec<(String, NzValue)> {
        self.columns()
            .iter()
            .zip(self.values().iter())
            .map(|(c, v)| (c.name.clone(), v.clone()))
            .collect()
    }
}

/// Index into a [`Row`] by ordinal or (case-insensitive) column name.
pub trait RowIndex {
    fn position(&self, row: &Row) -> Option<usize>;
    fn describe(&self) -> String;
}

impl RowIndex for usize {
    fn position(&self, row: &Row) -> Option<usize> {
        if *self < row.columns().len() {
            Some(*self)
        } else {
            None
        }
    }
    fn describe(&self) -> String {
        format!("ordinal {self}")
    }
}

impl RowIndex for &str {
    fn position(&self, row: &Row) -> Option<usize> {
        row.metadata.position(self)
    }
    fn describe(&self) -> String {
        format!("column '{self}'")
    }
}

impl RowIndex for String {
    fn position(&self, row: &Row) -> Option<usize> {
        row.metadata.position(self)
    }
    fn describe(&self) -> String {
        format!("column '{self}'")
    }
}

// ---------------------------------------------------------------------------
// ResultSet / QueryResult — ADO.NET + buffered-query surface
// ---------------------------------------------------------------------------

/// One result set: the rows of a single statement plus its column metadata.
#[derive(Debug, Clone)]
pub struct ResultSet {
    pub columns: Vec<ColumnDesc>,
    pub rows: Vec<Row>,
    /// Per-column nullability from the binary tuple descriptor
    /// (`RowDescriptionStandard`). `None` on the text path, where the server
    /// does not report it.
    pub nullability: Option<Vec<bool>>,
}

impl ResultSet {
    /// Build a result set without nullability information (text path).
    pub fn new(columns: Vec<ColumnDesc>, rows: Vec<Row>) -> Self {
        ResultSet {
            columns,
            rows,
            nullability: None,
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Buffered result of [`crate::Client::query_multi`]: every result set of a
/// (possibly multi-statement) batch, the accumulated affected-row count and
/// the server notices collected while draining the response.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub result_sets: Vec<ResultSet>,
    pub rows_affected: i64,
    pub notices: Vec<String>,
}

/// Receives rows while the protocol response is being decoded.
///
/// Implementations should apply backpressure when forwarding a row to a
/// result store. Returning an error aborts delivery, sends a best-effort
/// out-of-band cancel, and drains the backend response through ReadyForQuery
/// before returning the sink error. The connection remains reusable when that
/// drain succeeds.
pub trait QueryStreamSink {
    fn on_columns(
        &mut self,
        result_set_index: usize,
        columns: &[ColumnDesc],
        nullability: Option<&[bool]>,
    ) -> NzResult<()>;

    fn on_row(&mut self, result_set_index: usize, row: Row) -> NzResult<()>;

    /// Borrowed hot-path callback used by the compatibility
    /// `NzConnection::execute_stream` API.
    ///
    /// The default preserves the owned [`Row`] callback contract. A consumer
    /// that only needs to inspect/count values can override this method and
    /// avoid cloning row metadata and values for every row.
    fn on_values(
        &mut self,
        result_set_index: usize,
        columns: &[ColumnDesc],
        values: &[NzValue],
    ) -> NzResult<()> {
        self.on_row(
            result_set_index,
            Row::new(columns.to_vec(), values.to_vec()),
        )
    }

    fn on_notice(&mut self, _message: &str) -> NzResult<()> {
        Ok(())
    }

    /// Called for each server command completion in a simple-query batch.
    fn on_command_complete(&mut self, _tag: &str, _rows_affected: i64) -> NzResult<()> {
        Ok(())
    }
}

struct DiscardSink;
impl QueryStreamSink for DiscardSink {
    fn on_columns(&mut self, _: usize, _: &[ColumnDesc], _: Option<&[bool]>) -> NzResult<()> {
        Ok(())
    }
    fn on_row(&mut self, _: usize, _: Row) -> NzResult<()> {
        Ok(())
    }
    fn on_values(&mut self, _: usize, _: &[ColumnDesc], _: &[NzValue]) -> NzResult<()> {
        Ok(())
    }
}

/// Metadata and counters returned after a streaming execution.
#[derive(Debug, Clone, Default)]
pub struct StreamSummary {
    pub result_sets: Vec<StreamResultSet>,
    pub rows_affected: i64,
    pub notices: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct StreamResultSet {
    pub columns: Vec<ColumnDesc>,
    pub nullability: Option<Vec<bool>>,
    pub row_count: u64,
}

impl QueryResult {
    /// Move the first result set's rows without cloning them.
    pub fn into_rows(self) -> Vec<Row> {
        self.result_sets
            .into_iter()
            .next()
            .map(|set| set.rows)
            .unwrap_or_default()
    }
    /// Rows of the first result set (empty when the batch returns none).
    pub fn rows(&self) -> &[Row] {
        self.result_sets
            .first()
            .map(|s| s.rows.as_slice())
            .unwrap_or(&[])
    }

    /// Total buffered rows across all result sets.
    pub fn row_count(&self) -> usize {
        self.result_sets.iter().map(|s| s.rows.len()).sum()
    }

    /// Columns of the first result set.
    pub fn columns(&self) -> &[ColumnDesc] {
        self.result_sets
            .first()
            .map(|s| s.columns.as_slice())
            .unwrap_or(&[])
    }
}

// ---------------------------------------------------------------------------
// NzCommand — C# parity
// ---------------------------------------------------------------------------

/// A SQL command bound to a connection (port of C# `NzCommand`).
///
/// The command holds the SQL text plus client-side parameters (`$1, $2, …`
/// escaped into the text on send — there is no server-side prepare on the
/// Netezza simple-query path). After execution it carries `records_affected`
/// and the server `notices`, like the C# and Node drivers.
#[derive(Debug, Clone)]
pub struct NzCommand {
    pub command_text: String,
    pub parameters: Vec<NzValue>,
    /// Optional C#/ADO.NET-style named or `?` positional parameters. When
    /// non-empty these take precedence over `parameters` during execution.
    pub bound_parameters: Vec<NzParameter>,
    pub records_affected: i64,
    pub notices: Vec<String>,
    pub command_timeout: u64,
}

impl NzCommand {
    pub fn new(sql: &str) -> Self {
        NzCommand {
            command_text: sql.to_string(),
            parameters: Vec::new(),
            bound_parameters: Vec::new(),
            records_affected: -1,
            notices: Vec::new(),
            command_timeout: 30,
        }
    }

    pub fn set_parameters(mut self, params: Vec<NzValue>) -> Self {
        self.parameters = params;
        self
    }

    pub fn add_parameter(mut self, value: NzValue) -> Self {
        self.parameters.push(value);
        self
    }

    pub fn set_bound_parameters(mut self, params: Vec<NzParameter>) -> Self {
        self.bound_parameters = params;
        self
    }

    pub fn add_named_parameter(mut self, name: &str, value: NzValue) -> Self {
        self.bound_parameters.push(NzParameter::named(name, value));
        self
    }

    pub fn add_positional_parameter(mut self, value: NzValue) -> Self {
        self.bound_parameters.push(NzParameter::positional(value));
        self
    }
}

// ---------------------------------------------------------------------------
// NzConnection
// ---------------------------------------------------------------------------

/// Synchronous Netezza connection.
///
/// Typical use (tokio-postgres flavor):
///
/// ```no_run
/// # #[cfg(feature = "compat")]
/// # fn demo() {
/// use nz_rust::{NzConnection, NzConnectionConfig};
/// use nz_rust::types::value::ToSql;
///
/// let cfg = NzConnectionConfig::new("nz-host", "JUST_DATA", "admin", "password");
/// let mut conn = NzConnection::connect(&cfg).unwrap();
/// let rows = conn.query_rows("SELECT a FROM t WHERE a = $1", &[&42i32]).unwrap();
/// for row in &rows {
///     let a: i32 = row.try_get(0).unwrap();
///     println!("{a}");
/// }
/// # }
/// ```
pub struct NzConnection {
    config: NzConnectionConfig,
    stream: Option<NzStream>,
    buffer: ReadBuffer,
    row_var_starts_scratch: Vec<usize>,
    backend_process_id: i32,
    backend_secret_key: i32,
    command_number: i32,
    command_generation: u64,
    connected: bool,
    protocol_faulted: bool,
    executing: bool,
    in_transaction: bool,
    export_file: Option<File>,
    import_source: Option<(String, ImportSource)>,
    /// Absolute deadline for the command currently being drained.  Keeping
    /// the deadline on the connection lets every low-level buffered read
    /// enforce one wall-clock budget, including multi-row payloads.
    command_deadline: Option<Instant>,
    /// Set after a local timeout/cancel when the server may still be sending
    /// the abandoned command's terminal response. The next command must wait
    /// for ReadyForQuery before writing its packet.
    protocol_sync_required: bool,
    /// True while a backend message has been partially consumed (its type
    /// byte was read but not its whole body). A timeout in that state leaves
    /// the read position inside a payload, which cannot be resynchronized.
    frame_in_progress: bool,
}

impl NzConnection {
    // -- lifecycle ---------------------------------------------------------

    /// Open a TCP connection and run the handshake + authentication.
    pub fn connect(config: &NzConnectionConfig) -> NzResult<Self> {
        config.validate()?;
        let timeout = Duration::from_secs(config.connection_timeout.max(1));
        let addr_str = if config.host.contains(':') && !config.host.starts_with('[') {
            format!("[{}]:{}", config.host, config.port)
        } else {
            format!("{}:{}", config.host, config.port)
        };
        let addrs: Vec<_> = addr_str.to_socket_addrs().map_err(NzError::Io)?.collect();
        if addrs.is_empty() {
            return Err(NzError::Closed(format!("cannot resolve {addr_str}")));
        }
        let mut last_err: Option<NzError> = None;
        for addr in addrs {
            match TcpStream::connect_timeout(&addr, timeout) {
                Ok(stream) => {
                    let mut conn = NzConnection {
                        config: config.clone(),
                        stream: Some(NzStream::Plain(stream)),
                        buffer: ReadBuffer::new(),
                        row_var_starts_scratch: Vec::new(),
                        backend_process_id: 0,
                        backend_secret_key: 0,
                        command_number: 0,
                        command_generation: 0,
                        connected: false,
                        protocol_faulted: false,
                        executing: false,
                        in_transaction: false,
                        export_file: None,
                        import_source: None,
                        command_deadline: None,
                        protocol_sync_required: false,
                        frame_in_progress: false,
                    };
                    conn.finish_connect()?;
                    return Ok(conn);
                }
                Err(e) => last_err = Some(NzError::Io(e)),
            }
        }
        Err(last_err.unwrap_or_else(|| NzError::Closed(format!("cannot connect to {addr_str}"))))
    }

    /// Connect from a `netezza://` / `nz://` URI.
    pub fn connect_with_str(connection_string: &str) -> NzResult<Self> {
        let cfg = crate::config::parse_connection_string(connection_string)?;
        Self::connect(&cfg)
    }

    fn finish_connect(&mut self) -> NzResult<()> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| NzError::Closed("no stream".into()))?;
        stream.set_nodelay(true).ok();
        let ct = Duration::from_secs(self.config.connection_timeout.max(1));
        stream.set_read_timeout(Some(ct)).ok();
        stream.set_write_timeout(Some(ct)).ok();
        // Handshake borrows the stream + buffer; clone config to avoid aliasing.
        let config = self.config.clone();
        let hs = {
            let stream = self.stream.take().unwrap();
            handshake(stream, &mut self.buffer, &config)?
        };
        self.stream = Some(hs.0);
        self.backend_process_id = hs.1.backend_process_id;
        self.backend_secret_key = hs.1.backend_secret_key;
        self.connected = true;
        self.in_transaction = false;
        Ok(())
    }

    /// Close the socket. Idempotent.
    pub fn close(&mut self) {
        self.in_transaction = false;
        self.connected = false;
        self.export_file.take();
        self.import_source.take();
        if let Some(mut s) = self.stream.take() {
            let _ = s.shutdown();
        }
        self.buffer.clear();
    }

    pub fn is_closed(&self) -> bool {
        !self.connected || self.stream.is_none()
    }

    /// Non-blocking check, for pools, that an idle session's socket is still
    /// usable: the peer has not closed it and sent nothing unsolicited. Bytes
    /// that are expected (the tail of a cancelled response awaiting resync)
    /// do not count against it.
    pub(crate) fn idle_socket_is_healthy(&self) -> bool {
        let Some(stream) = self.stream.as_ref() else {
            return false;
        };
        if !self.connected || self.executing {
            return false;
        }
        // NUL padding between messages is normal (the appliance pads after
        // ReadyForQuery) and is skipped by the parser.
        if self.buffer.slice().iter().any(|&b| b != 0) {
            return self.protocol_sync_required;
        }
        let mut pending = [0u8; 64];
        if stream.set_nonblocking(true).is_err() {
            return false;
        }
        let peeked = stream.peek(&mut pending);
        let restored = stream.set_nonblocking(false).is_ok()
            && stream
                .set_read_timeout(Some(Duration::from_secs(
                    self.config.connection_timeout.max(1),
                )))
                .is_ok();
        // Pending TLS records (e.g. TLS 1.3 session tickets) are not
        // protocol data, so only plaintext sessions reject unsolicited bytes.
        let plaintext = matches!(stream, NzStream::Plain(_));
        restored
            && match peeked {
                Err(error) => error.kind() == std::io::ErrorKind::WouldBlock,
                Ok(0) => false,
                Ok(n) if pending[..n].iter().all(|&b| b == 0) => true,
                Ok(_) => !plaintext || self.protocol_sync_required,
            }
    }

    /// True while an explicit transaction opened with `BEGIN` is still open.
    ///
    /// Netezza `ReadyForQuery` carries no transaction-status byte, so the
    /// state is tracked from the statements this driver sends (same as the
    /// Node/C# drivers). It errs towards an extra `ROLLBACK`: harmless on
    /// Netezza (a notice), while a leaked open transaction would poison the
    /// next pooled checkout.
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    pub fn backend_process_id(&self) -> i32 {
        self.backend_process_id
    }

    pub fn backend_secret_key(&self) -> i32 {
        self.backend_secret_key
    }

    pub fn config(&self) -> &NzConnectionConfig {
        &self.config
    }

    /// Change the active Netezza catalog without reconnecting.
    pub fn change_database(&mut self, database: &str) -> NzResult<()> {
        let catalog = validate_catalog_identifier(database)?;
        if self.is_closed() {
            return Err(NzError::Closed(
                "Connection must be open to change database".into(),
            ));
        }
        if self.in_transaction {
            return Err(NzError::Config(
                "Cannot change database while a transaction is active".into(),
            ));
        }
        if self.config.database.eq_ignore_ascii_case(&catalog) {
            return Ok(());
        }

        self.batch_execute(&format!("SET CATALOG {catalog}"))?;
        self.config.database = catalog;
        Ok(())
    }

    /// Out-of-band cancel of the running command (PostgreSQL-style 16-byte
    /// cancel packet on a fresh connection).
    pub fn cancel(&self) -> NzResult<()> {
        send_cancel(
            &self.config,
            self.backend_process_id,
            self.backend_secret_key,
        )
    }

    // -- query surface (tokio-postgres flavor) ------------------------------

    /// Buffer all result sets of `sql` (tokio-postgres `query` returns rows;
    /// here the full [`QueryResult`] is returned so multi-statement batches
    /// and notices stay visible — use `.rows()` for the first set).
    ///
    /// Rows are decoded eagerly while draining the response (C#/Node `Read()`
    /// parity): a corrupt cell fails the whole `query()` instead of surfacing
    /// only when that column is read. Use `execute_stream` to process rows
    /// without retaining them.
    pub fn query(&mut self, sql: &str, params: &[&dyn ToSql]) -> NzResult<QueryResult> {
        let values: Vec<NzValue> = params.iter().map(|p| p.to_nz_value()).collect();
        self.query_values(sql, &values)
    }

    /// [`NzConnection::query`] with pre-built [`NzValue`] parameters.
    pub fn query_values(&mut self, sql: &str, params: &[NzValue]) -> NzResult<QueryResult> {
        let final_sql = substitute_parameters(sql, params).map_err(NzError::Config)?;
        self.run_batch(&final_sql)
    }

    /// Execute a buffered query with an explicit wall-clock timeout.
    pub fn query_with_timeout(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
        timeout: Option<Duration>,
    ) -> NzResult<QueryResult> {
        let values: Vec<NzValue> = params.iter().map(|p| p.to_nz_value()).collect();
        self.query_values_with_timeout(sql, &values, timeout)
    }

    /// Execute a buffered query with pre-built values and an explicit timeout.
    pub fn query_values_with_timeout(
        &mut self,
        sql: &str,
        params: &[NzValue],
        timeout: Option<Duration>,
    ) -> NzResult<QueryResult> {
        let final_sql = substitute_parameters(sql, params).map_err(NzError::Config)?;
        self.run_batch_with_duration(&final_sql, timeout)
    }

    /// First-set rows only (closest to `tokio_postgres::Client::query`).
    pub fn query_rows(&mut self, sql: &str, params: &[&dyn ToSql]) -> NzResult<Vec<Row>> {
        Ok(self.query(sql, params)?.into_rows())
    }

    /// Exactly one row; errors when the first set holds none.
    pub fn query_one(&mut self, sql: &str, params: &[&dyn ToSql]) -> NzResult<Row> {
        let rows = self.query_rows(sql, params)?;
        if rows.len() != 1 {
            return Err(NzError::Config(format!(
                "query_one: expected one row, got {}",
                rows.len()
            )));
        }
        Ok(rows.into_iter().next().expect("one row checked above"))
    }

    /// Zero or one row; errors when more than one row is returned.
    pub fn query_opt(&mut self, sql: &str, params: &[&dyn ToSql]) -> NzResult<Option<Row>> {
        let rows = self.query_rows(sql, params)?;
        if rows.len() > 1 {
            return Err(NzError::Config(format!(
                "query_opt: expected at most one row, got {}",
                rows.len()
            )));
        }
        Ok(rows.into_iter().next())
    }

    /// Bind a one-shot import reader to this operation, without a global registry.
    /// The appliance must request exactly `id`; unused readers are dropped on return.
    pub fn query_with_import_reader(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
        id: &str,
        reader: impl Read + Send + 'static,
    ) -> NzResult<QueryResult> {
        if id.is_empty() || id.contains('\0') {
            return Err(NzError::Config("invalid import identifier".into()));
        }
        self.import_source = Some((id.to_owned(), ImportSource::Reader(Box::new(reader))));
        let result = self.query(sql, params);
        self.import_source.take();
        result
    }

    /// Execute a non-query batch; returns affected rows (`-1` for DDL, same
    /// as the C# driver when the backend reports no count).
    pub fn execute(&mut self, sql: &str, params: &[&dyn ToSql]) -> NzResult<i64> {
        let values: Vec<NzValue> = params.iter().map(|p| p.to_nz_value()).collect();
        self.execute_values(sql, &values)
    }

    pub fn execute_values(&mut self, sql: &str, params: &[NzValue]) -> NzResult<i64> {
        let final_sql = substitute_parameters(sql, params).map_err(NzError::Config)?;
        Ok(self
            .run_batch_stream(&final_sql, self.config.command_timeout, &mut DiscardSink)?
            .rows_affected)
    }

    /// Execute a non-query with an explicit wall-clock timeout.
    pub fn execute_with_timeout(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
        timeout: Option<Duration>,
    ) -> NzResult<i64> {
        let values: Vec<NzValue> = params.iter().map(|p| p.to_nz_value()).collect();
        self.execute_values_with_timeout(sql, &values, timeout)
    }

    /// Execute a non-query with pre-built values and an explicit timeout.
    pub fn execute_values_with_timeout(
        &mut self,
        sql: &str,
        params: &[NzValue],
        timeout: Option<Duration>,
    ) -> NzResult<i64> {
        let final_sql = substitute_parameters(sql, params).map_err(NzError::Config)?;
        Ok(self
            .run_batch_stream_with_duration(&final_sql, timeout, &mut DiscardSink)?
            .rows_affected)
    }

    /// Execute without parameters and discard rows (DDL / `SET` / scripts).
    pub fn batch_execute(&mut self, sql: &str) -> NzResult<()> {
        self.run_batch_stream(sql, self.config.command_timeout, &mut DiscardSink)
            .map(|_| ())
    }

    /// Streaming reader (ADO.NET style). The response is buffered through the
    /// normal protocol loop and then handed to the reader, which exposes
    /// `read()` / `next_result()` navigation over the buffered sets.
    pub fn execute_reader(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> NzResult<crate::reader::NzDataReader> {
        let result = self.query(sql, params)?;
        Ok(crate::reader::NzDataReader::from_result(result))
    }

    /// Execute a query and forward each decoded row to `sink` before reading
    /// the next backend message. This is the bounded-memory API used by the
    /// desktop workbench; the older [`NzConnection::query`] API remains
    /// buffered for compatibility.
    pub fn execute_stream<S: QueryStreamSink>(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
        sink: &mut S,
    ) -> NzResult<StreamSummary> {
        let values: Vec<NzValue> = params.iter().map(|p| p.to_nz_value()).collect();
        self.execute_stream_values(sql, &values, sink)
    }

    /// Streaming execution with already materialized [`NzValue`] parameters.
    pub fn execute_stream_values<S: QueryStreamSink>(
        &mut self,
        sql: &str,
        params: &[NzValue],
        sink: &mut S,
    ) -> NzResult<StreamSummary> {
        let final_sql = substitute_parameters(sql, params).map_err(NzError::Config)?;
        self.run_batch_stream(&final_sql, self.config.command_timeout, sink)
    }

    /// Stream rows with an explicit wall-clock timeout.
    pub fn execute_stream_with_timeout<S: QueryStreamSink>(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
        timeout: Option<Duration>,
        sink: &mut S,
    ) -> NzResult<StreamSummary> {
        let values: Vec<NzValue> = params.iter().map(|p| p.to_nz_value()).collect();
        self.execute_stream_values_with_timeout(sql, &values, timeout, sink)
    }

    /// Stream pre-built values with an explicit wall-clock timeout.
    pub fn execute_stream_values_with_timeout<S: QueryStreamSink>(
        &mut self,
        sql: &str,
        params: &[NzValue],
        timeout: Option<Duration>,
        sink: &mut S,
    ) -> NzResult<StreamSummary> {
        let final_sql = substitute_parameters(sql, params).map_err(NzError::Config)?;
        self.run_batch_stream_with_duration(&final_sql, timeout, sink)
    }

    // -- ADO.NET / C# flavor -------------------------------------------------

    pub fn create_command(&self, sql: &str, params: Vec<NzValue>) -> NzCommand {
        NzCommand {
            command_text: sql.to_string(),
            parameters: params,
            bound_parameters: Vec::new(),
            records_affected: -1,
            notices: Vec::new(),
            command_timeout: self.config.command_timeout,
        }
    }

    /// Execute an [`NzCommand`], filling `records_affected` + `notices`.
    pub fn execute_command(&mut self, command: &mut NzCommand) -> NzResult<()> {
        let final_sql = if command.bound_parameters.is_empty() {
            substitute_parameters(&command.command_text, &command.parameters)
        } else {
            substitute_bound_parameters(&command.command_text, &command.bound_parameters)
        }
        .map_err(NzError::Config)?;
        let res = self.run_batch_with_duration(
            &final_sql,
            (command.command_timeout > 0).then(|| Duration::from_secs(command.command_timeout)),
        )?;
        command.records_affected = res.rows_affected;
        command.notices = res.notices;
        Ok(())
    }

    // -- transactions --------------------------------------------------------

    pub fn begin_transaction(&mut self) -> NzResult<()> {
        self.batch_execute("BEGIN")?;
        Ok(())
    }

    pub fn commit(&mut self) -> NzResult<()> {
        self.batch_execute("COMMIT")?;
        Ok(())
    }

    pub fn rollback(&mut self) -> NzResult<()> {
        // A rollback with no open transaction is only a notice on Netezza,
        // so this stays safe to call defensively (pool release path).
        self.batch_execute("ROLLBACK")?;
        Ok(())
    }

    /// Run `f` inside `BEGIN`/`COMMIT`; `ROLLBACK` on error (Node `transaction()` parity).
    pub fn transaction<T>(&mut self, f: impl FnOnce(&mut Self) -> NzResult<T>) -> NzResult<T> {
        self.begin_transaction()?;
        match f(self) {
            Ok(v) => {
                self.commit()?;
                Ok(v)
            }
            Err(e) => {
                let _ = self.rollback();
                Err(e)
            }
        }
    }

    // -- core protocol loop ---------------------------------------------------

    fn assert_can_execute(&self) -> NzResult<()> {
        if self.protocol_faulted {
            return Err(NzError::Protocol(
                "Connection protocol is invalid; reconnect is required".into(),
            ));
        }
        if !self.connected || self.stream.is_none() {
            return Err(NzError::Closed("Connection is closed".into()));
        }
        if self.executing {
            return Err(NzError::Config(
                "Connection is already executing a command".into(),
            ));
        }
        Ok(())
    }

    fn mark_protocol_fault(&mut self) {
        self.protocol_faulted = true;
        self.connected = false;
        self.export_file.take();
        self.import_source.take();
        if let Some(mut s) = self.stream.take() {
            let _ = s.shutdown();
        }
    }

    /// Retire the socket after an error that leaves its read position
    /// unknown. A framing fault poisons the session; EOF or a transport error
    /// (other than a command timeout, which resynchronizes via cancel) closes
    /// it so `is_closed()` reports the truth and pools never reuse it.
    fn mark_faulted_after(&mut self, error: &NzError) {
        if error.is_protocol_fault() {
            self.mark_protocol_fault();
        } else if matches!(error, NzError::Closed(_))
            || (matches!(error, NzError::Io(_)) && !is_command_timeout(error))
        {
            self.mark_protocol_fault();
            self.protocol_faulted = false;
        }
    }

    fn run_batch(&mut self, sql: &str) -> NzResult<QueryResult> {
        self.run_batch_with_duration(
            sql,
            (self.config.command_timeout > 0)
                .then(|| Duration::from_secs(self.config.command_timeout)),
        )
    }

    fn run_batch_with_duration(
        &mut self,
        sql: &str,
        timeout: Option<Duration>,
    ) -> NzResult<QueryResult> {
        if sql.contains('\0') {
            return Err(NzError::Config("SQL contains NUL".into()));
        }
        self.assert_can_execute()?;
        self.executing = true;
        self.command_deadline = timeout.map(|duration| Instant::now() + duration);
        let prev_in_tx = self.in_transaction;
        // Track the pending transaction state before the packet leaves, so a
        // failed BEGIN still settles on the safe (open) side.
        let (pending_state, had_start) = transaction_state_of(sql);
        if let Some(open) = pending_state {
            self.in_transaction = open;
        }
        let sent = self.send_query_packet(sql);
        if let Err(e) = sent {
            self.in_transaction = prev_in_tx;
            // A write timeout can occur after only part of the packet was
            // accepted by the socket. Treat the session as potentially busy
            // and use the same cancel/resync path as a read timeout.
            if is_command_timeout(&e) {
                self.protocol_sync_required = true;
                let _ = self.cancel();
            }
            self.mark_faulted_after(&e);
            self.executing = false;
            self.command_deadline = None;
            return Err(e);
        }
        let _ = had_start;
        let result = self.drain_response();
        self.executing = false;
        self.command_deadline = None;
        match result {
            Ok(mut qr) => {
                if qr.rows_affected < 0 && qr.row_count() > 0 {
                    // SELECT-style CommandComplete without a count: C# reports
                    // drained rows; mirror that.
                    qr.rows_affected = qr.row_count() as i64;
                }
                Ok(qr)
            }
            Err(e) => {
                // A failed COMMIT/ROLLBACK/END/ABORT must not clear an open
                // transaction, or the pool would skip its rollback-on-release.
                self.restore_tx_on_error(sql, prev_in_tx);
                self.mark_faulted_after(&e);
                Err(e)
            }
        }
    }

    fn run_batch_stream<S: QueryStreamSink>(
        &mut self,
        sql: &str,
        command_timeout: u64,
        sink: &mut S,
    ) -> NzResult<StreamSummary> {
        self.run_batch_stream_with_duration(
            sql,
            (command_timeout > 0).then(|| Duration::from_secs(command_timeout)),
            sink,
        )
    }

    fn run_batch_stream_with_duration<S: QueryStreamSink>(
        &mut self,
        sql: &str,
        timeout: Option<Duration>,
        sink: &mut S,
    ) -> NzResult<StreamSummary> {
        if sql.contains('\0') {
            return Err(NzError::Config("SQL contains NUL".into()));
        }
        self.assert_can_execute()?;
        self.executing = true;
        self.command_deadline = timeout.map(|duration| Instant::now() + duration);
        let prev_in_tx = self.in_transaction;
        let (pending_state, had_start) = transaction_state_of(sql);
        if let Some(open) = pending_state {
            self.in_transaction = open;
        }
        let sent = self.send_query_packet(sql);
        if let Err(e) = sent {
            self.in_transaction = prev_in_tx;
            if is_command_timeout(&e) {
                self.protocol_sync_required = true;
                let _ = self.cancel();
            }
            self.mark_faulted_after(&e);
            self.executing = false;
            self.command_deadline = None;
            return Err(e);
        }
        let _ = had_start;
        let result = self.drain_response_stream(sink);
        self.executing = false;
        self.command_deadline = None;
        match result {
            Ok((_summary, Some(sink_error))) => {
                self.restore_tx_on_error(sql, prev_in_tx);
                Err(sink_error)
            }
            Ok((mut summary, None)) => {
                if summary.rows_affected < 0 {
                    let row_count: u64 = summary.result_sets.iter().map(|set| set.row_count).sum();
                    if row_count > 0 {
                        summary.rows_affected = row_count as i64;
                    }
                }
                Ok(summary)
            }
            Err(e) => {
                self.restore_tx_on_error(sql, prev_in_tx);
                self.mark_faulted_after(&e);
                Err(e)
            }
        }
    }

    fn restore_tx_on_error(&mut self, sql: &str, prev: bool) {
        let (state, had_start) = transaction_state_of(sql);
        match state {
            Some(false) => self.in_transaction = prev || had_start,
            None => self.in_transaction = prev,
            Some(true) => self.in_transaction = true,
        }
    }

    fn send_query_packet(&mut self, sql: &str) -> NzResult<()> {
        self.ensure_protocol_synced(sql)?;
        self.command_number += 1;
        if self.command_number > 100_000 {
            self.command_number = 1;
        }
        self.command_generation = self.command_generation.wrapping_add(1);
        let packet = build_simple_query_packet(sql, self.command_number);
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| NzError::Closed("Connection is closed".into()))?;
        stream.set_read_timeout(self.command_deadline.map(|deadline| {
            deadline
                .saturating_duration_since(Instant::now())
                .max(Duration::from_nanos(1))
        }))?;
        apply_deadline_to_stream(self.command_deadline, stream, true)?;
        match stream.write_all(&packet) {
            Ok(()) => {
                stream.flush().map_err(NzError::Io)?;
                Ok(())
            }
            Err(e) => Err(map_io_command_error(e)),
        }
    }

    /// Drain one full backend response (up to `Z` / `L`) into a [`QueryResult`].
    fn drain_response(&mut self) -> NzResult<QueryResult> {
        let (result, _, sink_error) = self.drain_response_with_sink(None)?;
        debug_assert!(sink_error.is_none());
        Ok(result)
    }

    fn drain_response_stream<S: QueryStreamSink>(
        &mut self,
        sink: &mut S,
    ) -> NzResult<(StreamSummary, Option<NzError>)> {
        let (result, result_sets, sink_error) = self.drain_response_with_sink(Some(sink))?;
        Ok((
            StreamSummary {
                result_sets,
                rows_affected: result.rows_affected,
                notices: result.notices,
            },
            sink_error,
        ))
    }

    fn drain_response_with_sink(
        &mut self,
        mut sink: Option<&mut dyn QueryStreamSink>,
    ) -> NzResult<(QueryResult, Vec<StreamResultSet>, Option<NzError>)> {
        let mut sets: Vec<ResultSet> = Vec::new();
        let mut current: Option<ResultSet> = None;
        let mut cached_columns: Option<Vec<ColumnDesc>> = None;
        let mut tupdesc = DbosTupleDesc::default();
        let mut shared_tupdesc: Option<Arc<DbosTupleDesc>> = None;
        let mut has_tupdesc = false;
        // Per-column nullability reported by the binary tuple descriptor.
        let mut nullability: Option<Vec<bool>> = None;
        let mut rows_affected: i64 = -1;
        let mut notices: Vec<String> = Vec::new();
        let mut stream_result_sets: Vec<StreamResultSet> = Vec::new();
        let mut stream_columns_sent: Vec<bool> = Vec::new();
        let mut next_result_set_index = 0usize;
        let mut current_result_set_index = None;
        let mut stream_values: Vec<NzValue> = Vec::new();
        let mut stream_var_starts: Vec<usize> = Vec::new();
        let mut row_columns: Option<Arc<[ColumnDesc]>> = None;
        let mut row_metadata: Option<Arc<RowMetadata>> = None;
        let mut error: Option<NzError> = None;
        let mut sink_error: Option<NzError> = None;
        let mut sink_cancel_sent = false;

        macro_rules! cancel_if_sink_aborted {
            ($new_error:expr) => {
                if $new_error && !sink_cancel_sent {
                    sink_cancel_sent = true;
                    self.protocol_sync_required = true;
                    let _ = self.cancel();
                }
            };
        }

        let outcome: NzResult<()> = (|| {
            loop {
                self.frame_in_progress = false;
                let msg_type = self.read_type_byte()?;
                self.frame_in_progress = true;
                match msg_type {
                    // -- external-table protocol (before the shared 4-byte header) --
                    b'u' => {
                        self.handle_export_start()?;
                        continue;
                    }
                    b'U' => {
                        self.handle_export_data()?;
                        continue;
                    }
                    b'l' => {
                        self.handle_import()?;
                        continue;
                    }
                    b'x' => {
                        self.skip_bytes_raw(4)?;
                        continue;
                    }
                    b'e' => {
                        self.handle_ext_log()?;
                        continue;
                    }
                    code::ROW_STANDARD => {
                        // Shared 4-byte header is NOT a length here (commonly 1);
                        // the DBOS row length governs the frame.
                        self.skip_frame_header()?;
                        if sink.is_some() {
                            self.read_dbos_tuple_into(
                                &tupdesc,
                                has_tupdesc,
                                &mut stream_values,
                                &mut stream_var_starts,
                            )?;
                            let result_set_index = if let Some(index) = current_result_set_index {
                                index
                            } else {
                                let columns = cached_columns
                                    .clone()
                                    .unwrap_or_else(|| tupdesc.to_column_descs());
                                ensure_stream_result_set(
                                    &mut stream_result_sets,
                                    &mut stream_columns_sent,
                                    &mut current_result_set_index,
                                    columns,
                                    nullability.clone(),
                                    &mut next_result_set_index,
                                )
                            };
                            let new_sink_error = emit_stream_row(
                                &mut sink,
                                &mut stream_result_sets,
                                &mut stream_columns_sent,
                                result_set_index,
                                &stream_values,
                                &mut sink_error,
                            );
                            cancel_if_sink_aborted!(new_sink_error);
                            while self.try_read_available_dbos_row(
                                &tupdesc,
                                has_tupdesc,
                                &mut stream_values,
                            )? {
                                let new_sink_error = emit_stream_row(
                                    &mut sink,
                                    &mut stream_result_sets,
                                    &mut stream_columns_sent,
                                    result_set_index,
                                    &stream_values,
                                    &mut sink_error,
                                );
                                cancel_if_sink_aborted!(new_sink_error);
                            }
                        } else {
                            // Eager buffered decode (C# `Read()` parity): parse
                            // the row straight from the socket buffer into an
                            // owned value vector. No per-row payload `Vec`,
                            // layout `Vec` or `Arc<RawRowData>` — only the
                            // retained `Vec<NzValue>` plus cell strings.
                            let columns = row_columns
                                .clone()
                                .or_else(|| {
                                    cached_columns
                                        .as_ref()
                                        .map(|columns| Arc::from(columns.as_slice()))
                                })
                                .unwrap_or_else(|| Arc::from(tupdesc.to_column_descs()));
                            let descriptor = shared_tupdesc.clone().ok_or_else(|| {
                                NzError::Protocol("DBOS row descriptor is missing".into())
                            })?;
                            let mut values =
                                Vec::with_capacity(columns.len().max(tupdesc.num_fields));
                            let mut var_starts = std::mem::take(&mut self.row_var_starts_scratch);
                            self.read_dbos_tuple_into(
                                descriptor.as_ref(),
                                has_tupdesc,
                                &mut values,
                                &mut var_starts,
                            )?;
                            self.row_var_starts_scratch = var_starts;
                            let metadata = row_metadata
                                .get_or_insert_with(|| Arc::new(RowMetadata::new(columns.clone())))
                                .clone();
                            let row = Row::from_shared_dbos_metadata(
                                metadata,
                                values,
                                descriptor.clone(),
                            );
                            push_existing_row(&mut current, row, &nullability, &mut row_columns)?;
                            loop {
                                let columns = row_columns.clone().unwrap_or_else(|| {
                                    cached_columns
                                        .as_ref()
                                        .map(|columns| Arc::from(columns.as_slice()))
                                        .unwrap_or_else(|| Arc::from(tupdesc.to_column_descs()))
                                });
                                let mut values =
                                    Vec::with_capacity(columns.len().max(tupdesc.num_fields));
                                if !self.try_read_available_dbos_row(
                                    descriptor.as_ref(),
                                    has_tupdesc,
                                    &mut values,
                                )? {
                                    break;
                                }
                                let metadata = row_metadata
                                    .get_or_insert_with(|| {
                                        Arc::new(RowMetadata::new(columns.clone()))
                                    })
                                    .clone();
                                let row = Row::from_shared_dbos_metadata(
                                    metadata,
                                    values,
                                    descriptor.clone(),
                                );
                                push_existing_row(
                                    &mut current,
                                    row,
                                    &nullability,
                                    &mut row_columns,
                                )?;
                            }
                        }
                        continue;
                    }
                    _ => {}
                }

                // All other messages: shared 4-byte header, then length+payload.
                self.skip_frame_header()?;

                match msg_type {
                    code::COMMAND_COMPLETE => {
                        let len = self.read_len("commandCompletePayload")?;
                        let data = self.read_payload(len, "commandCompletePayload")?;
                        let text = String::from_utf8_lossy(&data).to_string();
                        let n = parse_command_complete_rows(&text);
                        if n >= 0 {
                            rows_affected = if rows_affected < 0 {
                                n
                            } else {
                                rows_affected + n
                            };
                        }
                        if sink.is_some() {
                            let new_sink_error = finish_stream_result_set(
                                &mut sink,
                                &mut stream_result_sets,
                                &mut stream_columns_sent,
                                &mut current_result_set_index,
                                &mut sink_error,
                            );
                            cancel_if_sink_aborted!(new_sink_error);
                        } else {
                            flush_current(&mut current, &mut sets, &mut row_columns);
                            row_metadata = None;
                        }
                        if sink_error.is_none() {
                            if let Some(sink) = sink.as_deref_mut() {
                                let new_sink_error = record_stream_sink_result(
                                    &mut sink_error,
                                    sink.on_command_complete(text.trim_matches('\0').trim(), n),
                                );
                                cancel_if_sink_aborted!(new_sink_error);
                            }
                        }
                    }
                    code::READY_FOR_QUERY | code::READY_FOR_QUERY_ALT => {
                        if sink.is_some() {
                            finish_stream_result_set(
                                &mut sink,
                                &mut stream_result_sets,
                                &mut stream_columns_sent,
                                &mut current_result_set_index,
                                &mut sink_error,
                            );
                        } else {
                            flush_current(&mut current, &mut sets, &mut row_columns);
                        }
                        self.protocol_sync_required = false;
                        break;
                    }
                    code::CONTROL_ZERO | code::CONTROL_A => {}
                    code::EMPTY_QUERY_RESPONSE => {
                        let len = self.read_len("emptyQueryPayload")?;
                        if len > 0 {
                            let _ = self.read_payload(len, "emptyQueryPayload")?;
                        }
                    }
                    code::BACKEND_PAYLOAD_P => {
                        let len = self.read_len("backendPayloadP")?;
                        if len > 0 {
                            let _ = self.read_payload(len, "backendPayloadP")?;
                        }
                    }
                    code::ERROR_RESPONSE => {
                        let len = self.read_len("errorResponsePayload")?;
                        let data = self.read_payload(len, "errorResponsePayload")?;
                        let db = parse_backend_error_fields(&data);
                        if error.is_none() {
                            error = Some(NzError::Database(Box::new(db)));
                        }
                    }
                    code::NOTICE_RESPONSE => {
                        let len = self.read_len("noticeResponsePayload")?;
                        let data = self.read_payload(len, "noticeResponsePayload")?;
                        let msg = parse_backend_error_fields(&data).message;
                        if !msg.is_empty() {
                            if sink_error.is_none() {
                                if let Some(sink) = sink.as_deref_mut() {
                                    let new_sink_error = record_stream_sink_result(
                                        &mut sink_error,
                                        sink.on_notice(&msg),
                                    );
                                    cancel_if_sink_aborted!(new_sink_error);
                                }
                            }
                            notices.push(msg);
                        }
                    }
                    code::ROW_DESCRIPTION => {
                        let len = self.read_len("rowDescriptionPayload")?;
                        let data = self.read_payload(len, "rowDescriptionPayload")?;
                        let cols = parse_row_description(&data)?;
                        if sink.is_some() {
                            let new_sink_error = finish_stream_result_set(
                                &mut sink,
                                &mut stream_result_sets,
                                &mut stream_columns_sent,
                                &mut current_result_set_index,
                                &mut sink_error,
                            );
                            cancel_if_sink_aborted!(new_sink_error);
                            let _ = ensure_stream_result_set(
                                &mut stream_result_sets,
                                &mut stream_columns_sent,
                                &mut current_result_set_index,
                                cols.clone(),
                                None,
                                &mut next_result_set_index,
                            );
                        } else {
                            flush_current(&mut current, &mut sets, &mut row_columns);
                            row_metadata = None;
                        }
                        cached_columns = Some(cols.clone());
                        nullability = None; // text path reports no nullability
                        if sink.is_none() {
                            row_columns = Some(Arc::from(cols.as_slice()));
                            row_metadata =
                                Some(Arc::new(RowMetadata::new(Arc::from(cols.as_slice()))));
                            current = Some(ResultSet {
                                columns: cols,
                                rows: Vec::new(),
                                nullability: None,
                            });
                        }
                    }
                    code::DATA_ROW => {
                        let len = self.read_len("dataRowPayload")?;
                        let data = self.read_payload(len, "dataRowPayload")?;
                        if sink.is_some() {
                            let cols = current_columns_ref(&current, &cached_columns)?;
                            parse_text_data_row_into(&data, cols, &mut stream_values)
                                .map_err(NzError::Protocol)?;
                            let result_set_index = if let Some(index) = current_result_set_index {
                                index
                            } else {
                                let columns = cols.to_vec();
                                ensure_stream_result_set(
                                    &mut stream_result_sets,
                                    &mut stream_columns_sent,
                                    &mut current_result_set_index,
                                    columns,
                                    nullability.clone(),
                                    &mut next_result_set_index,
                                )
                            };
                            let new_sink_error = emit_stream_row(
                                &mut sink,
                                &mut stream_result_sets,
                                &mut stream_columns_sent,
                                result_set_index,
                                &stream_values,
                                &mut sink_error,
                            );
                            cancel_if_sink_aborted!(new_sink_error);
                        } else {
                            // Eager text decode sharing one column `Arc` for
                            // the whole result set (no per-row `String` clones
                            // of column names, no layout `Vec`).
                            let columns = row_columns.clone().or_else(|| {
                                cached_columns
                                    .as_ref()
                                    .map(|cols| Arc::from(cols.as_slice()))
                            }).or_else(|| {
                                current.as_ref().map(|set| Arc::from(set.columns.as_slice()))
                            }).ok_or_else(|| {
                                NzError::Protocol(
                                    "Invalid DataRow sequence: row description is missing; reconnect is required.".into(),
                                )
                            })?;
                            let mut values = Vec::with_capacity(columns.len());
                            parse_text_data_row_into(&data, &columns, &mut values)
                                .map_err(NzError::Protocol)?;
                            let metadata = row_metadata
                                .get_or_insert_with(|| Arc::new(RowMetadata::new(columns.clone())))
                                .clone();
                            let row = Row::from_shared_metadata(metadata, values);
                            push_existing_row(&mut current, row, &nullability, &mut row_columns)?;
                        }
                    }
                    code::ROW_DESCRIPTION_STANDARD => {
                        let len = self.read_len("rowDescriptionStandardPayload")?;
                        let data = self.read_payload(len, "rowDescriptionStandardPayload")?;
                        let descriptor =
                            Arc::new(DbosTupleDesc::parse(&data, cached_columns.as_deref())?);
                        tupdesc = (*descriptor).clone();
                        shared_tupdesc = Some(descriptor);
                        has_tupdesc = true;
                        nullability = Some(tupdesc.field_null_allowed.clone());
                        // Binary rows that arrive without a text description get
                        // best-effort `col1..N` names (Node parity).
                        if current.is_none() && cached_columns.is_none() {
                            cached_columns = Some(tupdesc.to_column_descs());
                            if sink.is_none() {
                                row_metadata = Some(Arc::new(RowMetadata::new(Arc::from(
                                    cached_columns
                                        .as_ref()
                                        .expect("columns stored above")
                                        .as_slice(),
                                ))));
                            }
                        }
                        if let Some(set) = current.as_mut() {
                            set.nullability = nullability.clone();
                        }
                        if sink.is_some() {
                            let columns = cached_columns
                                .clone()
                                .unwrap_or_else(|| tupdesc.to_column_descs());
                            let result_set_index = ensure_stream_result_set(
                                &mut stream_result_sets,
                                &mut stream_columns_sent,
                                &mut current_result_set_index,
                                columns,
                                nullability.clone(),
                                &mut next_result_set_index,
                            );
                            stream_result_sets[result_set_index].nullability = nullability.clone();
                            let new_sink_error = emit_stream_columns(
                                &mut sink,
                                &mut stream_result_sets,
                                &mut stream_columns_sent,
                                result_set_index,
                                &mut sink_error,
                            );
                            cancel_if_sink_aborted!(new_sink_error);
                        }
                    }
                    _ => {
                        let len = self.read_len("unknownMessagePayload")?;
                        if len > 0 {
                            let _ = self.read_payload(len, "unknownMessagePayload")?;
                        }
                    }
                }
            }
            Ok(())
        })();

        // Restore blocking timeouts to the connection-level default.
        if let Some(s) = self.stream.as_ref() {
            let ct = Duration::from_secs(self.config.connection_timeout.max(1));
            s.set_read_timeout(Some(ct)).ok();
        }
        // Drop high-water read capacity so repeated large queries report
        // stable per-query allocations instead of pinning ~16 MB on the
        // connection after the first burst.
        self.buffer.shrink_if_large();

        match outcome {
            Err(e) => {
                if is_command_timeout(&e) {
                    // Mirror the Node driver: reject now, cancel out-of-band so
                    // the appliance stops the abandoned execution. The cancel
                    // response is asynchronous, so the next command must
                    // drain it before sending a new packet.
                    let _ = self.cancel();
                    if self.frame_in_progress {
                        // The orphaned response cannot be framed from inside
                        // a payload: retire the socket instead of guessing.
                        self.mark_faulted_after(&NzError::Closed(
                            "command timed out inside a backend message".into(),
                        ));
                    } else {
                        self.protocol_sync_required = true;
                    }
                    return Err(NzError::Timeout("Command execution timeout".into()));
                }
                Err(e)
            }
            Ok(()) => {
                if sink.is_some() {
                    finish_stream_result_set(
                        &mut sink,
                        &mut stream_result_sets,
                        &mut stream_columns_sent,
                        &mut current_result_set_index,
                        &mut sink_error,
                    );
                } else {
                    flush_current(&mut current, &mut sets, &mut row_columns);
                }
                if let Some(sink_error) = sink_error {
                    Ok((
                        QueryResult {
                            result_sets: sets,
                            rows_affected,
                            notices,
                        },
                        stream_result_sets,
                        Some(sink_error),
                    ))
                } else if let Some(e) = error {
                    Err(e)
                } else {
                    Ok((
                        QueryResult {
                            result_sets: sets,
                            rows_affected,
                            notices,
                        },
                        stream_result_sets,
                        None,
                    ))
                }
            }
        }
    }

    // -- low-level reads ------------------------------------------------------

    fn stream_take(&mut self) -> NzResult<StreamGuard> {
        // Temporarily take the stream out so `ReadBuffer` (which borrows it)
        // and `self` don't alias. Callers must restore via `stream_restore`.
        let s = self
            .stream
            .take()
            .ok_or_else(|| NzError::Closed("Connection is closed".into()))?;
        Ok(StreamGuard {
            stream: s,
            deadline: self.command_deadline,
        })
    }

    fn stream_restore(&mut self, guard: StreamGuard, buf: ReadBuffer) {
        self.stream = Some(guard.stream);
        self.buffer = buf;
    }

    fn read_type_byte(&mut self) -> NzResult<u8> {
        let mut guard = self.stream_take()?;
        let mut buf = self.buffer.take();
        let r = (|| loop {
            let b = buf.read_byte(&mut guard)?;
            if b != 0 {
                return Ok(b);
            }
        })();
        self.stream_restore(guard, buf);
        r
    }

    fn skip_frame_header(&mut self) -> NzResult<()> {
        let mut guard = self.stream_take()?;
        let mut buf = self.buffer.take();
        let r = buf.skip(&mut guard, 4);
        self.stream_restore(guard, buf);
        r
    }

    fn read_len(&mut self, field: &str) -> NzResult<usize> {
        let mut guard = self.stream_take()?;
        let mut buf = self.buffer.take();
        let r = (|| {
            let len = buf.read_i32(&mut guard)?;
            let len = validate_protocol_length(len, field, true)?;
            Ok(len as usize)
        })();
        self.stream_restore(guard, buf);
        r
    }

    fn read_payload(&mut self, len: usize, field: &str) -> NzResult<Vec<u8>> {
        let _ = field;
        let mut guard = self.stream_take()?;
        let mut buf = self.buffer.take();
        let r = buf.read_bytes(&mut guard, len);
        self.stream_restore(guard, buf);
        r
    }

    fn read_dbos_tuple_into(
        &mut self,
        tupdesc: &DbosTupleDesc,
        has_tupdesc: bool,
        values: &mut Vec<NzValue>,
        var_starts: &mut Vec<usize>,
    ) -> NzResult<()> {
        if !has_tupdesc {
            return Err(NzError::Protocol(
                "Invalid RowStandard sequence: row description is missing; reconnect is required."
                    .into(),
            ));
        }
        let mut guard = self.stream_take()?;
        let mut buf = self.buffer.take();
        let r = (|| {
            // The first four bytes are a DBOS row marker/reserved field; the
            // second word is the payload length. Read them directly instead
            // of allocating a short-lived header Vec for every row.
            buf.skip(&mut guard, 4)?;
            let row_len = buf.read_i32(&mut guard)?;
            let row_len = validate_protocol_length(row_len, "rowStandardPayload", false)?;
            let payload = buf.peek_bytes(&mut guard, row_len as usize)?;
            let result = tupdesc.parse_row_into_with_scratch(payload, values, var_starts);
            if result.is_ok() {
                buf.advance(row_len as usize);
            }
            result
        })();
        self.stream_restore(guard, buf);
        r
    }

    /// Decode one complete DBOS row already buffered after the first row of a
    /// response. The Python C extension has an equivalent batch path; keeping
    /// this scan non-blocking leaves incomplete frames for the normal protocol
    /// loop to finish.
    fn try_read_available_dbos_row(
        &mut self,
        tupdesc: &DbosTupleDesc,
        has_tupdesc: bool,
        values: &mut Vec<NzValue>,
    ) -> NzResult<bool> {
        if !has_tupdesc {
            return Err(NzError::Protocol(
                "Invalid RowStandard sequence: row description is missing; reconnect is required."
                    .into(),
            ));
        }

        let available = self.buffer.slice();
        if available.len() < 13 || available[0] != code::ROW_STANDARD {
            return Ok(false);
        }

        // Frame layout: type byte, shared 4-byte header, DBOS reserved word,
        // big-endian payload length, and the row payload.
        let row_len = i32::from_be_bytes(available[9..13].try_into().unwrap());
        let row_len = validate_protocol_length(row_len, "rowStandardPayload", false)? as usize;
        let frame_len = 13usize
            .checked_add(row_len)
            .ok_or_else(|| NzError::Protocol("DBOS row frame length overflow".into()))?;
        if available.len() < frame_len {
            return Ok(false);
        }

        let mut var_starts = std::mem::take(&mut self.row_var_starts_scratch);
        let result =
            tupdesc.parse_row_into_with_scratch(&available[13..frame_len], values, &mut var_starts);
        self.row_var_starts_scratch = var_starts;
        result?;
        self.buffer.advance(frame_len);
        Ok(true)
    }

    // -- protocol sync (orphaned-response drain) --------------------------------
    //
    // If a previous command left an unread response on the wire (abandoned
    // reader, timeout + cancel race), consume it up to ReadyForQuery before
    // sending the next query — otherwise the leftover RowDescription/DataRow
    // would be attributed to the new command. Port of
    // `NzConnection._ensureProtocolSynced` (Node) / `EnsureProtocolSynced` (C#).

    fn ensure_protocol_synced(&mut self, context: &str) -> NzResult<()> {
        if self.stream.is_none() {
            return Ok(());
        }
        let must_wait_for_ready = self.protocol_sync_required;
        let ct = Duration::from_secs(self.config.connection_timeout.max(1));
        // Non-blocking probe for already-arrived bytes.
        {
            let s = self.stream.as_mut().unwrap();
            s.set_nonblocking(true).ok();
            // `pull_available` loops until WouldBlock; on a blocking socket it
            // would hang, hence the temporary nonblocking mode.
            let _ = self.buffer.pull_available(s);
            s.set_nonblocking(false).ok();
            // Restore the configured read timeout (nonblocking mode clears it
            // on some platforms).
            s.set_read_timeout(Some(ct)).ok();
        }

        if self.buffer.available() == 0 && !must_wait_for_ready {
            return Ok(());
        }
        self.buffer.discard_leading_nulls();
        if self.buffer.available() == 0 && !must_wait_for_ready {
            return Ok(());
        }

        let deadline = Instant::now() + Duration::from_millis(2000);
        // Give the tail of an orphaned response a short window to arrive.
        if let Some(s) = self.stream.as_ref() {
            s.set_read_timeout(Some(Duration::from_millis(250))).ok();
        }
        let r = self.drain_orphaned(context, deadline);
        if let Some(s) = self.stream.as_ref() {
            s.set_read_timeout(Some(ct)).ok();
        }
        if r.is_ok() {
            self.protocol_sync_required = false;
        }
        r
    }

    fn drain_orphaned(&mut self, context: &str, deadline: Instant) -> NzResult<()> {
        loop {
            // Wait for at least the type byte.
            if !self.wait_buffered(1, deadline)? {
                return Err(protocol_sync_error(
                    context,
                    "orphaned response incomplete (no ReadyForQuery)",
                ));
            }
            let msg_type = {
                let mut guard = self.stream_take()?;
                let mut buf = self.buffer.take();
                let r = buf.read_byte(&mut guard);
                self.stream_restore(guard, buf);
                r?
            };
            if msg_type == 0 {
                continue;
            }
            if !self.wait_buffered(4, deadline)? {
                return Err(protocol_sync_error(
                    context,
                    &format!("truncated orphaned header for type 0x{msg_type:02x}"),
                ));
            }
            if msg_type == code::ROW_STANDARD {
                self.skip_frame_header()?;
                // DBOS header (8) then row payload.
                if !self.wait_buffered(8, deadline)? {
                    return Err(protocol_sync_error(
                        context,
                        "truncated orphaned RowStandard header",
                    ));
                }
                let row_len: i32 = {
                    let mut guard = self.stream_take()?;
                    let mut buf = self.buffer.take();
                    let r: NzResult<i32> = (|| {
                        buf.ensure_data(&mut guard, 8)?;
                        let h = buf.read_bytes(&mut guard, 8)?;
                        Ok(i32::from_be_bytes(h[4..8].try_into().unwrap()))
                    })();
                    self.stream_restore(guard, buf);
                    r?
                };
                if validate_protocol_length(row_len, "orphanedRowStandardPayload", false).is_err() {
                    return Err(protocol_sync_error(
                        context,
                        &format!("invalid orphaned RowStandard rowLength={row_len}"),
                    ));
                }
                if !self.wait_buffered(8 + row_len as usize, deadline)? {
                    return Err(protocol_sync_error(
                        context,
                        "truncated orphaned RowStandard payload",
                    ));
                }
                self.skip_bytes_raw(8 + row_len as usize)?;
                continue;
            }
            self.skip_frame_header()?;
            if msg_type == code::READY_FOR_QUERY || msg_type == code::READY_FOR_QUERY_ALT {
                self.buffer.discard_leading_nulls();
                return Ok(());
            }
            if msg_type == code::EMPTY_QUERY_RESPONSE
                || msg_type == code::CONTROL_ZERO
                || msg_type == code::CONTROL_A
            {
                continue;
            }
            // Length-prefixed orphaned message: read its length, then skip.
            if !self.wait_buffered(4, deadline)? {
                return Err(protocol_sync_error(
                    context,
                    &format!("truncated orphaned length for type 0x{msg_type:02x}"),
                ));
            }
            let len = {
                let mut guard = self.stream_take()?;
                let mut buf = self.buffer.take();
                let r = buf.read_i32(&mut guard);
                self.stream_restore(guard, buf);
                r?
            };
            if validate_protocol_length(len, "orphanedPayload", true).is_err() {
                return Err(protocol_sync_error(
                    context,
                    &format!("invalid orphaned length={len} for type 0x{msg_type:02x}"),
                ));
            }
            if len > 0 {
                if !self.wait_buffered(len as usize, deadline)? {
                    return Err(protocol_sync_error(
                        context,
                        &format!("truncated orphaned payload for type 0x{msg_type:02x}"),
                    ));
                }
                self.skip_bytes_raw(len as usize)?;
            }
        }
    }

    /// Block (up to `deadline`) until `n` bytes are buffered or readable.
    fn wait_buffered(&mut self, n: usize, deadline: Instant) -> NzResult<bool> {
        loop {
            if self.buffer.available() >= n {
                return Ok(true);
            }
            // Try a non-blocking pull first.
            if let Some(s) = self.stream.as_mut() {
                s.set_nonblocking(true).ok();
                let _ = self.buffer.pull_available(s);
                s.set_nonblocking(false).ok();
            }
            if self.buffer.available() >= n {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            // Short blocking wait for more data.
            if self.stream.is_some() {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if let Some(s) = self.stream.as_mut() {
                    s.set_read_timeout(Some(remaining.min(Duration::from_millis(250))))
                        .ok();
                }
                // Peek via a blocking read into the buffer machinery: a
                // timeout here just means "no more orphaned bytes yet".
                let want = (self.buffer.available() + 1).min(n);
                let mut guard = self.stream_take()?;
                let mut buf = self.buffer.take();
                let r = buf.ensure_data(&mut guard, want);
                let avail = buf.available();
                self.stream_restore(guard, buf);
                match r {
                    Ok(()) => {
                        if avail >= n {
                            return Ok(true);
                        }
                    }
                    Err(NzError::Io(e))
                        if e.kind() == std::io::ErrorKind::TimedOut
                            || e.kind() == std::io::ErrorKind::WouldBlock =>
                    {
                        // no data yet — loop until the deadline
                    }
                    Err(_) => return Ok(false),
                }
            } else {
                return Ok(false);
            }
        }
    }

    fn skip_bytes_raw(&mut self, n: usize) -> NzResult<()> {
        let mut guard = self.stream_take()?;
        let mut buf = self.buffer.take();
        let r = buf.skip(&mut guard, n);
        self.stream_restore(guard, buf);
        r
    }

    // -- external-table protocol -------------------------------------------------
    //
    // Bulk path used by `CREATE EXTERNAL TABLE … USING (DATAOBJECT …)` /
    // `CREATE TABLE … AS SELECT …` over external data. Ports the Node
    // `ExternalTableHandler` (C# `HandleExternalTableProtocolMessage`): the
    // appliance opens side channels inline in the session, so the driver must
    // answer them or the query hangs.

    fn handle_export_start(&mut self) -> NzResult<()> {
        // After the type byte: frame header (4) + 10 + 16 + filenameLen(4) + name.
        self.skip_frame_header()?;
        self.skip_bytes_raw(10)?;
        self.skip_bytes_raw(16)?;
        let len = self.read_len("externalTableExportFilename")?;
        if len == 0 {
            return Err(NzError::Protocol(
                "Invalid external-table export filename length; reconnect is required.".into(),
            ));
        }
        let name = self.read_payload(len, "externalTableExportFilename")?;
        let filename = crate::external::decode_filename(&name)?;
        match self
            .config
            .external_files
            .resolve(std::path::Path::new(&filename))
            .and_then(File::create)
        {
            Ok(f) => {
                self.export_file = Some(f);
                self.write_all_raw(&[0, 0, 0, 0])?;
                Ok(())
            }
            Err(_) => {
                // Tell the appliance the open failed (C#/Node parity: status 1).
                self.write_all_raw(&1i32.to_be_bytes())?;
                Ok(())
            }
        }
    }

    fn handle_export_data(&mut self) -> NzResult<()> {
        // Frame header (4) + 4 reserved, then status loop.
        self.skip_frame_header()?;
        self.skip_bytes_raw(4)?;
        self.consume_export_stream()
    }

    fn consume_export_stream(&mut self) -> NzResult<()> {
        loop {
            let status = {
                let mut guard = self.stream_take()?;
                let mut buf = self.buffer.take();
                let r = buf.read_i32(&mut guard);
                self.stream_restore(guard, buf);
                r?
            };
            match status {
                1 => {
                    // DATA
                    let n: usize = {
                        let mut guard = self.stream_take()?;
                        let mut buf = self.buffer.take();
                        let r: NzResult<usize> = (|| {
                            let v = buf.read_i32(&mut guard)?;
                            Ok(
                                validate_protocol_length(v, "externalTableExportDataChunk", true)?
                                    as usize,
                            )
                        })();
                        self.stream_restore(guard, buf);
                        r?
                    };
                    if n > 0 {
                        let chunk = self.read_payload(n, "externalTableExportDataChunk")?;
                        if let Some(f) = self.export_file.as_mut() {
                            f.write_all(&chunk).map_err(NzError::Io)?;
                        }
                    }
                }
                3 => {
                    // DONE
                    if let Some(mut f) = self.export_file.take() {
                        f.flush().map_err(NzError::Io)?;
                    }
                    return Ok(());
                }
                2 => {
                    // ERROR: len(u16) + message
                    let len: usize = {
                        let mut guard = self.stream_take()?;
                        let mut buf = self.buffer.take();
                        let r: NzResult<usize> = (|| {
                            buf.ensure_data(&mut guard, 2)?;
                            let b = buf.read_bytes(&mut guard, 2)?;
                            Ok(u16::from_be_bytes(b[..2].try_into().unwrap()) as usize)
                        })();
                        self.stream_restore(guard, buf);
                        r?
                    };
                    if len > 0 {
                        let _ = self.read_payload(len, "externalTableErrorMessage")?;
                    }
                    self.export_file.take();
                    return Ok(());
                }
                _ => {
                    self.export_file.take();
                    return Ok(());
                }
            }
        }
    }

    fn handle_import(&mut self) -> NzResult<()> {
        // 8 reserved, NUL-terminated filename, hostVersion(4); then the driver
        // answers clientVersion(4)=1, reads format(4)+bufSize(4) and streams DATA/DONE.
        self.skip_bytes_raw(8)?;
        let mut name_bytes: Vec<u8> = Vec::new();
        loop {
            let b = self.read_payload(1, "externalTableImportFilename")?;
            if b[0] == 0 {
                break;
            }
            name_bytes.push(b[0]);
            if name_bytes.len() > 4096 {
                return Err(NzError::Protocol(
                    "Invalid external-table import filename; reconnect is required.".into(),
                ));
            }
        }
        let filename = crate::external::decode_filename(&name_bytes)?;
        let mut guard = self.stream_take()?;
        let mut buf = self.buffer.take();
        let header = (|| {
            let _host_version = buf.read_i32(&mut guard)?;
            Ok::<_, NzError>(())
        })();
        self.stream_restore(guard, buf);
        header?;
        self.write_all_raw(&1i32.to_be_bytes())?;
        let mut guard = self.stream_take()?;
        let mut buf = self.buffer.take();
        let cfg: NzResult<usize> = (|| {
            let _format = buf.read_i32(&mut guard)?;
            let size = buf.read_i32(&mut guard)?;
            Ok(validate_protocol_length(size, "externalTableImportBufferSize", true)? as usize)
        })();
        self.stream_restore(guard, buf);
        let buf_size = cfg?.max(1);

        // Virtual stream first, then the filesystem (Node parity).
        let source = if self
            .import_source
            .as_ref()
            .is_some_and(|(id, _)| id == &filename)
        {
            self.import_source.take().map(|(_, source)| source)
        } else {
            take_import_source(&filename)
        };
        if let Some(source) = source {
            return match source {
                ImportSource::Bytes(data) => {
                    self.send_import_reader(std::io::Cursor::new(data), buf_size)
                }
                ImportSource::Reader(reader) => self.send_import_reader(reader, buf_size),
                ImportSource::AsyncReader(_) => {
                    self.write_all_raw(&2i32.to_be_bytes())?;
                    Ok(())
                }
            };
        }
        match self
            .config
            .external_files
            .resolve(std::path::Path::new(&filename))
            .and_then(File::open)
        {
            Ok(file) => self.send_import_reader(file, buf_size),
            Err(_) => {
                // File missing → ERROR status (C#/Node parity).
                self.write_all_raw(&2i32.to_be_bytes())?;
                Ok(())
            }
        }
    }

    fn send_import_reader(&mut self, mut reader: impl Read, buffer_size: usize) -> NzResult<()> {
        // DATA chunks: status(4)=1 + len(4) + bytes, then DONE status(4)=3.
        let mut chunk = vec![0; buffer_size.clamp(1, 64 * 1024)];
        loop {
            let count = match reader.read(&mut chunk) {
                Ok(count) => count,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    self.write_all_raw(&2i32.to_be_bytes())?;
                    return Ok(());
                }
            };
            if count == 0 {
                break;
            }
            let mut header = [0u8; 8];
            header[..4].copy_from_slice(&1i32.to_be_bytes());
            header[4..].copy_from_slice(&(count as i32).to_be_bytes());
            self.write_all_raw(&header)?;
            self.write_all_raw(&chunk[..count])?;
        }
        self.write_all_raw(&3i32.to_be_bytes())
    }

    fn handle_ext_log(&mut self) -> NzResult<()> {
        // Frame header (4, consumed as generic 4), logDirLen(4), dir, NUL?,
        // filename NUL-terminated, logType(4), then length-prefixed chunks to EOF(0).
        self.skip_frame_header()?;
        let len = self.read_len("fileTransfer.logDirectoryLength")?;
        if len == 0 {
            return Err(NzError::Protocol(
                "Invalid external-table log directory length; reconnect is required.".into(),
            ));
        }
        // `len` counts the trailing NUL (C# ValidateProtocolLengthAfterOverhead).
        let payload_len = len.saturating_sub(1);
        let dir = self.read_payload(payload_len, "fileTransfer.logDirectoryPayloadLength")?;
        let _ = self.read_payload(1, "fileTransfer.logDirectoryTerminator")?;
        let log_dir = crate::external::decode_filename(&dir)?;
        let mut name_bytes: Vec<u8> = Vec::new();
        loop {
            let b = self.read_payload(1, "fileTransfer.logFilename")?;
            if b[0] == 0 {
                break;
            }
            name_bytes.push(b[0]);
            if name_bytes.len() > 4096 {
                return Err(NzError::Protocol("external filename is too long".into()));
            }
        }
        let filename = crate::external::decode_filename(&name_bytes)?;
        let log_type = {
            let mut guard = self.stream_take()?;
            let mut buf = self.buffer.take();
            let r = buf.read_i32(&mut guard);
            self.stream_restore(guard, buf);
            r?
        };
        let ext = match log_type {
            1 => ".nzlog",
            2 => ".nzbad",
            3 => ".nzstats",
            _ => ".log",
        };
        let path = std::path::Path::new(&log_dir).join(format!("{filename}{ext}"));
        let mut file = self
            .config
            .external_files
            .resolve(&path)
            .and_then(File::create)
            .ok();
        loop {
            let n: usize = {
                let mut guard = self.stream_take()?;
                let mut buf = self.buffer.take();
                let r: NzResult<usize> = (|| {
                    let v = buf.read_i32(&mut guard)?;
                    Ok(validate_protocol_length(v, "externalTableLogChunk", true)? as usize)
                })();
                self.stream_restore(guard, buf);
                r?
            };
            if n == 0 {
                break;
            }
            let chunk = self.read_payload(n, "externalTableLogChunk")?;
            if let Some(f) = file.as_mut() {
                let _ = f.write_all(&chunk);
            }
        }
        if let Some(mut f) = file {
            let _ = f.flush();
        }
        Ok(())
    }

    fn write_all_raw(&mut self, data: &[u8]) -> NzResult<()> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| NzError::Closed("Connection is closed".into()))?;
        apply_deadline_to_stream(self.command_deadline, stream, true)?;
        stream.write_all(data).map_err(map_io_command_error)?;
        stream.flush().map_err(map_io_command_error)?;
        Ok(())
    }
}

impl Drop for NzConnection {
    fn drop(&mut self) {
        self.close();
    }
}

// ---------------------------------------------------------------------------
// Response-set assembly helpers
// ---------------------------------------------------------------------------

fn flush_current(
    current: &mut Option<ResultSet>,
    sets: &mut Vec<ResultSet>,
    row_columns: &mut Option<Arc<[ColumnDesc]>>,
) {
    if let Some(set) = current.take() {
        // Keep empty sets only when they carry columns (a real result shape);
        // a bare CommandComplete-only batch yields no sets at all.
        if !set.rows.is_empty() || !set.columns.is_empty() {
            sets.push(set);
        }
    }
    *row_columns = None;
}

fn ensure_stream_result_set(
    result_sets: &mut Vec<StreamResultSet>,
    columns_sent: &mut Vec<bool>,
    current: &mut Option<usize>,
    columns: Vec<ColumnDesc>,
    nullability: Option<Vec<bool>>,
    next_index: &mut usize,
) -> usize {
    if let Some(index) = *current {
        return index;
    }
    let index = *next_index;
    *next_index += 1;
    result_sets.push(StreamResultSet {
        columns,
        nullability,
        row_count: 0,
    });
    columns_sent.push(false);
    *current = Some(index);
    index
}

fn emit_stream_columns(
    sink: &mut Option<&mut dyn QueryStreamSink>,
    result_sets: &mut [StreamResultSet],
    columns_sent: &mut [bool],
    result_set_index: usize,
    sink_error: &mut Option<NzError>,
) -> bool {
    if sink_error.is_some() {
        columns_sent[result_set_index] = true;
        return false;
    }
    if columns_sent[result_set_index] {
        return false;
    }
    if let Some(sink) = sink.as_deref_mut() {
        let result_set = &result_sets[result_set_index];
        let new_sink_error = record_stream_sink_result(
            sink_error,
            sink.on_columns(
                result_set_index,
                &result_set.columns,
                result_set.nullability.as_deref(),
            ),
        );
        columns_sent[result_set_index] = true;
        return new_sink_error;
    }
    columns_sent[result_set_index] = true;
    false
}

fn emit_stream_row(
    sink: &mut Option<&mut dyn QueryStreamSink>,
    result_sets: &mut [StreamResultSet],
    columns_sent: &mut [bool],
    result_set_index: usize,
    values: &[NzValue],
    sink_error: &mut Option<NzError>,
) -> bool {
    let mut new_sink_error = emit_stream_columns(
        sink,
        result_sets,
        columns_sent,
        result_set_index,
        sink_error,
    );
    if sink_error.is_none() {
        if let Some(sink) = sink.as_deref_mut() {
            let columns = &result_sets[result_set_index].columns;
            new_sink_error |= record_stream_sink_result(
                sink_error,
                sink.on_values(result_set_index, columns, values),
            );
        }
    }
    result_sets[result_set_index].row_count += 1;
    new_sink_error
}

fn record_stream_sink_result(sink_error: &mut Option<NzError>, result: NzResult<()>) -> bool {
    if sink_error.is_some() {
        return false;
    }
    if let Err(error) = result {
        *sink_error = Some(error);
        true
    } else {
        false
    }
}

fn finish_stream_result_set(
    sink: &mut Option<&mut dyn QueryStreamSink>,
    result_sets: &mut [StreamResultSet],
    columns_sent: &mut [bool],
    current: &mut Option<usize>,
    sink_error: &mut Option<NzError>,
) -> bool {
    if let Some(index) = current.take() {
        return emit_stream_columns(sink, result_sets, columns_sent, index, sink_error);
    }
    false
}

fn current_columns_ref<'a>(
    current: &'a Option<ResultSet>,
    cached: &'a Option<Vec<ColumnDesc>>,
) -> NzResult<&'a [ColumnDesc]> {
    if let Some(set) = current {
        Ok(&set.columns)
    } else if let Some(cols) = cached {
        Ok(cols)
    } else {
        Err(NzError::Protocol(
            "Invalid DataRow sequence: row description is missing; reconnect is required.".into(),
        ))
    }
}

/// Borrow wire-identical bytes for an eagerly decoded cell.
///
/// Buffered `query()` rows drop the original frame, so only byte-preserving
/// variants can answer `RawValue::as_bytes()`. Binary variable-length text is
/// appended verbatim by the DBOS decoder and opaque binary fields are stored
/// as their raw payload; text-path `Text` keeps the exact field bytes. CHAR /
/// NCHAR trim padding and every numeric/temporal type re-encodes, so those
/// return `None` rather than wrong bytes.
fn eager_cell_bytes<'a>(
    value: &'a NzValue,
    dbos: Option<&DbosTupleDesc>,
    column: &ColumnDesc,
    position: usize,
) -> Option<&'a [u8]> {
    match value {
        NzValue::Text(text) => match dbos {
            Some(descriptor) => matches!(
                descriptor.field_type.get(position).copied(),
                Some(nz_type::NZ_TYPE_VARCHAR)
                    | Some(nz_type::NZ_TYPE_VAR_FIXED_CHAR)
                    | Some(nz_type::NZ_TYPE_JSON)
                    | Some(nz_type::NZ_TYPE_JSONPATH)
            )
            .then(|| text.as_bytes()),
            None if column.format == 0 => Some(text.as_bytes()),
            None => None,
        },
        NzValue::Bytea(bytes) if dbos.is_some() => Some(bytes.as_slice()),
        _ => None,
    }
}

fn push_existing_row(
    current: &mut Option<ResultSet>,
    row: Row,
    nullability: &Option<Vec<bool>>,
    row_columns: &mut Option<Arc<[ColumnDesc]>>,
) -> NzResult<()> {
    if current.is_none() {
        let columns = row.columns().to_vec();
        if !columns.is_empty() && row.len() != columns.len() {
            return Err(NzError::Protocol(format!(
                "Row/column count mismatch: {} values for {} columns; reconnect is required.",
                row.len(),
                columns.len()
            )));
        }
        *row_columns = Some(Arc::from(columns.as_slice()));
        *current = Some(ResultSet {
            columns,
            rows: Vec::new(),
            nullability: nullability.clone(),
        });
    }

    let set = current.as_mut().expect("result set initialized above");
    if !set.columns.is_empty() && row.len() != set.columns.len() {
        return Err(NzError::Protocol(format!(
            "Row/column count mismatch: {} values for {} columns; reconnect is required.",
            row.len(),
            set.columns.len()
        )));
    }
    if row_columns.is_none() {
        *row_columns = Some(Arc::from(set.columns.as_slice()));
    }
    set.rows.push(row);
    Ok(())
}

// ---------------------------------------------------------------------------
// Transaction-state + error mapping helpers
// ---------------------------------------------------------------------------

fn transaction_state_of(sql: &str) -> (Option<bool>, bool) {
    match parse_transaction_state(sql) {
        (TransactionState::Opened, had) => (Some(true), had),
        (TransactionState::Closed, had) => (Some(false), had),
        (TransactionState::Unchanged, had) => (None, had),
    }
}

fn protocol_sync_error(context: &str, detail: &str) -> NzError {
    let preview: String = context.split_whitespace().collect::<Vec<_>>().join(" ");
    let preview = preview.chars().take(80).collect::<String>();
    NzError::Protocol(format!(
        "Connection protocol out of sync before executing \"{preview}\": {detail}. Reconnect required."
    ))
}

fn is_command_timeout(e: &NzError) -> bool {
    match e {
        NzError::Timeout(_) => true,
        NzError::Io(io) => {
            io.kind() == std::io::ErrorKind::TimedOut || io.kind() == std::io::ErrorKind::WouldBlock
        }
        _ => false,
    }
}

fn apply_deadline_to_stream(
    deadline: Option<Instant>,
    stream: &NzStream,
    write: bool,
) -> NzResult<()> {
    let Some(deadline) = deadline else {
        if write {
            stream.set_write_timeout(None)?;
        } else {
            stream.set_read_timeout(None)?;
        }
        return Ok(());
    };
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(NzError::Timeout("Command execution timeout".into()));
    }
    if write {
        stream
            .set_write_timeout(Some(remaining))
            .map_err(NzError::Io)?;
    } else {
        stream
            .set_read_timeout(Some(remaining))
            .map_err(NzError::Io)?;
    }
    Ok(())
}

/// Reads that fail with a timeout become [`NzError::Timeout`]; any other I/O
/// error during a command is a transport failure.
fn map_io_command_error(e: std::io::Error) -> NzError {
    match e.kind() {
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => {
            NzError::Timeout("Command execution timeout".into())
        }
        _ => NzError::Io(e),
    }
}

fn validate_catalog_identifier(database: &str) -> NzResult<String> {
    let catalog = database.trim();
    let bytes = catalog.as_bytes();
    if bytes.is_empty() {
        return Err(NzError::Config(
            "Database name cannot be null or empty".into(),
        ));
    }
    if !bytes[0].is_ascii_alphabetic() && bytes[0] != b'_' {
        return Err(NzError::Config(
            "Database name must be a valid unquoted identifier".into(),
        ));
    }
    if bytes[1..]
        .iter()
        .any(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_' && *byte != b'$')
    {
        return Err(NzError::Config(
            "Database name must be a valid unquoted identifier".into(),
        ));
    }
    Ok(catalog.to_string())
}

struct StreamGuard {
    stream: NzStream,
    deadline: Option<Instant>,
}

impl Read for StreamGuard {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if let Some(deadline) = self.deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "command execution timeout",
                ));
            }
            self.stream.set_read_timeout(Some(remaining))?;
        }
        self.stream.read(buf)
    }
}

impl Write for StreamGuard {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Some(deadline) = self.deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "command execution timeout",
                ));
            }
            self.stream.set_write_timeout(Some(remaining))?;
        }
        self.stream.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn row_get_by_index_and_name() {
        let cols = vec![
            ColumnDesc {
                name: "ID".into(),
                type_oid: 23,
                type_len: 4,
                type_mod: -1,
                format: 0,
            },
            ColumnDesc {
                name: "NAME".into(),
                type_oid: 1043,
                type_len: -1,
                type_mod: -1,
                format: 0,
            },
        ];
        let row = Row::new(cols, vec![NzValue::Int4(7), NzValue::Text("x".into())]);
        assert_eq!(row.try_get::<_, i32>(0).unwrap(), 7);
        assert_eq!(row.try_get::<_, String>("name").unwrap(), "x");
        assert_eq!(row.try_get::<_, Option<i32>>(0).unwrap(), Some(7));
        assert!(row.try_get::<_, i32>("missing").is_err());
    }

    #[test]
    fn column_name_index_is_lazy_case_insensitive_and_keeps_first_duplicate() {
        let row = Row::new(
            vec![
                ColumnDesc {
                    name: "ID".into(),
                    type_oid: 23,
                    type_len: 4,
                    type_mod: -1,
                    format: 0,
                },
                ColumnDesc {
                    name: "id".into(),
                    type_oid: 23,
                    type_len: 4,
                    type_mod: -1,
                    format: 0,
                },
            ],
            vec![NzValue::Int4(7), NzValue::Int4(8)],
        );
        assert!(row.metadata.name_index.get().is_none());
        assert_eq!(row.try_get::<_, i32>("Id").unwrap(), 7);
        assert!(row.metadata.name_index.get().is_some());
        let cloned = row.clone();
        assert!(Arc::ptr_eq(&row.metadata, &cloned.metadata));
        assert_eq!(cloned.try_get::<_, i32>("iD").unwrap(), 7);
    }

    #[test]
    fn raw_text_row_decodes_only_requested_cells() {
        let columns: Arc<[ColumnDesc]> = Arc::from(vec![
            ColumnDesc {
                name: "ID".into(),
                type_oid: 23,
                type_len: 4,
                type_mod: -1,
                format: 0,
            },
            ColumnDesc {
                name: "NAME".into(),
                type_oid: 25,
                type_len: -1,
                type_mod: -1,
                format: 0,
            },
        ]);
        let payload = vec![
            0b1100_0000, // both fields are present
            0,
            0,
            0,
            5,
            b'4',
            0,
            0,
            0,
            7,
            b'n',
            b'e',
            b't',
        ];
        let row = Row::from_text_raw(columns, payload).unwrap();
        let RowStorage::Raw(raw) = &row.storage else {
            panic!("expected raw row");
        };
        assert!(raw.layout_progress.lock().unwrap().spans.is_empty());
        let raw = row.try_get_raw_value(0).unwrap();
        assert_eq!(raw.as_bytes(), Some(b"4" as &[u8]));
        let RowStorage::Raw(raw_storage) = &row.storage else {
            panic!("expected raw row");
        };
        assert_eq!(raw_storage.layout_progress.lock().unwrap().spans.len(), 1);
        assert_eq!(row.try_get::<_, i32>(0).unwrap(), 4);
        let name: &str = row.try_get_raw_typed(1).unwrap();
        assert_eq!(name, "net");
        assert_eq!(raw_storage.layout_progress.lock().unwrap().spans.len(), 2);
        assert_eq!(row.values()[0], NzValue::Int4(4));
        assert_eq!(row.values()[1], NzValue::Text("net".into()));
    }

    #[test]
    fn raw_text_row_validates_later_columns_before_exposing_first_column() {
        let columns: Arc<[ColumnDesc]> = Arc::from(vec![
            ColumnDesc {
                name: "FIRST".into(),
                type_oid: 23,
                type_len: 4,
                type_mod: -1,
                format: 0,
            },
            ColumnDesc {
                name: "SECOND".into(),
                type_oid: 25,
                type_len: -1,
                type_mod: -1,
                format: 0,
            },
        ]);
        let mut payload = vec![0b1100_0000];
        payload.extend_from_slice(&5i32.to_be_bytes());
        payload.push(b'7');
        payload.extend_from_slice(&3i32.to_be_bytes());
        assert!(Row::from_text_raw(columns, payload).is_err());
    }

    #[test]
    fn available_dbos_rows_are_batched_without_consuming_next_message() {
        let tupdesc = DbosTupleDesc {
            nulls_allowed: 0,
            fixed_fields_size: 6,
            num_fields: 1,
            field_type: vec![crate::messages::nz_type::NZ_TYPE_INT],
            field_size: vec![4],
            field_true_size: vec![4],
            field_offset: vec![2],
            field_phys_field: vec![0],
            field_null_allowed: vec![false],
            field_null_byte_offset: vec![0],
            field_null_bit_mask: vec![0],
            field_fixed_size: vec![4],
            ..Default::default()
        };

        fn frame(value: i32) -> Vec<u8> {
            let mut payload = vec![0, 0];
            payload.extend_from_slice(&value.to_le_bytes());
            let mut out = vec![code::ROW_STANDARD];
            out.extend_from_slice(&[0; 4]);
            out.extend_from_slice(&[0; 4]);
            out.extend_from_slice(&(payload.len() as i32).to_be_bytes());
            out.extend_from_slice(&payload);
            out
        }

        let mut wire = frame(7);
        wire.extend_from_slice(&frame(8));
        wire.extend_from_slice(&[code::COMMAND_COMPLETE, 0, 0, 0, 0]);

        let mut connection = NzConnection {
            config: NzConnectionConfig::default(),
            stream: None,
            buffer: ReadBuffer::new(),
            row_var_starts_scratch: Vec::new(),
            backend_process_id: 0,
            backend_secret_key: 0,
            command_number: 0,
            command_generation: 0,
            connected: false,
            protocol_faulted: false,
            executing: false,
            in_transaction: false,
            export_file: None,
            import_source: None,
            command_deadline: None,
            protocol_sync_required: false,
            frame_in_progress: false,
        };
        connection
            .buffer
            .pull_available(&mut Cursor::new(wire))
            .unwrap();

        let mut values = Vec::new();
        assert!(connection
            .try_read_available_dbos_row(&tupdesc, true, &mut values)
            .unwrap());
        assert_eq!(values, vec![NzValue::Int4(7)]);
        values.clear();
        assert!(connection
            .try_read_available_dbos_row(&tupdesc, true, &mut values)
            .unwrap());
        assert_eq!(values, vec![NzValue::Int4(8)]);
        assert!(!connection
            .try_read_available_dbos_row(&tupdesc, true, &mut values)
            .unwrap());
        assert_eq!(connection.buffer.slice()[0], code::COMMAND_COMPLETE);
    }

    #[test]
    fn eager_rows_borrow_only_wire_identical_bytes() {
        let columns: Arc<[ColumnDesc]> = Arc::from(vec![
            ColumnDesc {
                name: "V".into(),
                type_oid: 1043,
                type_len: -1,
                type_mod: -1,
                format: 1,
            },
            ColumnDesc {
                name: "N".into(),
                type_oid: 1700,
                type_len: -1,
                type_mod: -1,
                format: 1,
            },
        ]);
        let desc = Arc::new(DbosTupleDesc {
            num_fields: 2,
            field_type: vec![
                crate::messages::nz_type::NZ_TYPE_VARCHAR,
                crate::messages::nz_type::NZ_TYPE_NUMERIC,
            ],
            field_size: vec![10, (7 << 8) | 2],
            field_true_size: vec![10, 4],
            ..Default::default()
        });
        let row = Row::from_shared_dbos(
            columns,
            vec![NzValue::Text("abc".into()), NzValue::Float8(12.34)],
            desc,
        );
        // VARCHAR bytes are preserved verbatim; NUMERIC was re-encoded.
        assert_eq!(
            row.try_get_raw_value(0).unwrap().as_bytes(),
            Some(b"abc" as &[u8])
        );
        assert_eq!(row.try_get_raw_value(1).unwrap().as_bytes(), None);
        // The decoded value still drives typed extraction.
        assert_eq!(
            row.try_get::<_, crate::NzNumeric>(1).unwrap().to_string(),
            "12.34"
        );
    }

    #[test]
    fn transaction_state_tracks_batches() {
        let (open, _) = transaction_state_of("BEGIN");
        assert_eq!(open, Some(true));
        let (closed, _) = transaction_state_of("BEGIN; COMMIT;");
        assert_eq!(closed, Some(false));
        let (none, _) = transaction_state_of("SELECT CASE WHEN 1=1 THEN 2 END");
        assert_eq!(none, None);
    }

    #[test]
    fn command_complete_accumulates() {
        assert_eq!(super::parse_command_complete_rows("INSERT 0 1"), 1);
        assert_eq!(super::parse_command_complete_rows("SELECT 10"), 10);
    }

    #[test]
    fn catalog_identifier_validation_matches_reference_driver() {
        assert_eq!(
            validate_catalog_identifier(" JUST_DATA ").unwrap(),
            "JUST_DATA"
        );
        for invalid in [
            "",
            "   ",
            "1db",
            "db-name",
            "db name",
            "db;select",
            "\"db\"",
        ] {
            assert!(validate_catalog_identifier(invalid).is_err(), "{invalid:?}");
        }
    }
    #[test]
    fn long_binary_varchar_and_single_field_access_preserve_data() {
        for size in [32760usize, 32766, 32767, 40000, 64000] {
            let desc = Arc::new(DbosTupleDesc {
                num_fields: 2,
                num_fixed_fields: 1,
                num_varying_fields: 1,
                fixed_fields_size: 6,
                field_type: vec![3, 16],
                field_size: vec![4, size as i32],
                field_true_size: vec![4, size as i32],
                field_offset: vec![2, 0],
                field_fixed_size: vec![4, 0],
                field_null_byte_offset: vec![2, 2],
                field_null_bit_mask: vec![1, 2],
                ..Default::default()
            });
            let mut bytes = vec![0, 0];
            bytes.extend_from_slice(&123i32.to_le_bytes());
            bytes.extend_from_slice(&((size + 2) as u16).to_le_bytes());
            bytes.extend(std::iter::repeat_n(b'x', size));
            if !size.is_multiple_of(2) {
                bytes.push(0);
            }
            let row = Row::from_dbos_raw(Arc::from(desc.to_column_descs()), bytes, desc).unwrap();
            let RowStorage::Raw(raw) = &row.storage else {
                panic!("expected raw row");
            };
            assert!(raw.decoded.get().is_none());
            assert!(raw.cells.get().is_none());
            assert_eq!(raw.layout_progress.lock().unwrap().spans.len(), 1);
            assert_eq!(row.try_get::<_, i32>(0).unwrap(), 123);
            assert_eq!(raw.layout_progress.lock().unwrap().spans.len(), 1);
            assert_eq!(row.try_get::<_, String>(1).unwrap().len(), size);
            assert_eq!(raw.layout_progress.lock().unwrap().spans.len(), 1);
            assert_eq!(row.try_values().unwrap()[0], NzValue::Int4(123));
        }
    }

    #[test]
    fn lazy_dbos_rows_keep_no_varying_field_fallback() {
        let descriptor = Arc::new(DbosTupleDesc {
            num_fields: 1,
            num_fixed_fields: 0,
            num_varying_fields: 0,
            fixed_fields_size: 2,
            field_type: vec![crate::messages::nz_type::NZ_TYPE_INT],
            field_size: vec![4],
            field_true_size: vec![4],
            // The legacy fallback ignores this offset when there are no
            // varying fields and uses fixed_fields_size instead.
            field_offset: vec![0],
            field_fixed_size: vec![0],
            field_null_byte_offset: vec![0],
            field_null_bit_mask: vec![0],
            ..Default::default()
        });
        let mut payload = vec![0, 0];
        payload.extend_from_slice(&42i32.to_le_bytes());
        let row = Row::from_dbos_raw(Arc::from(descriptor.to_column_descs()), payload, descriptor)
            .unwrap();

        assert_eq!(row.try_get::<_, i32>(0).unwrap(), 42);
        assert_eq!(
            row.try_get_raw_value(0).unwrap().as_bytes(),
            Some(&42i32.to_le_bytes()[..])
        );
        assert_eq!(row.try_values().unwrap(), &[NzValue::Int4(42)]);
    }

    #[test]
    fn dbos_row_validation_rejects_a_malformed_later_varying_field() {
        let descriptor = Arc::new(DbosTupleDesc {
            num_fields: 2,
            num_fixed_fields: 1,
            num_varying_fields: 1,
            fixed_fields_size: 6,
            field_type: vec![3, 16],
            field_size: vec![4, 8],
            field_true_size: vec![4, 8],
            field_offset: vec![2, 0],
            field_fixed_size: vec![4, 0],
            field_null_byte_offset: vec![0, 0],
            field_null_bit_mask: vec![0, 0],
            ..Default::default()
        });
        let mut payload = vec![0, 0];
        payload.extend_from_slice(&7i32.to_le_bytes());
        payload.extend_from_slice(&1u16.to_le_bytes());
        assert!(
            Row::from_dbos_raw(Arc::from(descriptor.to_column_descs()), payload, descriptor,)
                .is_err()
        );
    }

    #[test]
    fn a_decode_error_is_not_sql_null_and_does_not_poison_other_fields() {
        let desc = Arc::new(DbosTupleDesc {
            num_fields: 2,
            num_fixed_fields: 2,
            fixed_fields_size: 8,
            field_type: vec![3, 15],
            field_size: vec![4, 2],
            field_true_size: vec![4, 2],
            field_offset: vec![2, 6],
            field_fixed_size: vec![4, 2],
            field_null_byte_offset: vec![0, 0],
            field_null_bit_mask: vec![0, 0],
            ..Default::default()
        });
        let mut bytes = vec![0, 0];
        bytes.extend_from_slice(&123i32.to_le_bytes());
        bytes.extend([0xff, 0xff]);
        let row = Row::from_dbos_raw(Arc::from(desc.to_column_descs()), bytes, desc).unwrap();
        assert!(row.try_get::<_, Option<String>>(1).is_err());
        assert!(row.try_values().is_err());
        assert_eq!(row.try_get::<_, i32>(0).unwrap(), 123);
    }

    #[test]
    fn null_binary_scalars_do_not_decode_as_zero_or_false() {
        // Bitmap byte 0 with both null bits set; payload bytes are zero.
        let desc = Arc::new(DbosTupleDesc {
            num_fields: 2,
            num_fixed_fields: 2,
            fixed_fields_size: 6,
            nulls_allowed: 1,
            field_type: vec![12, 3],
            field_size: vec![1, 4],
            field_true_size: vec![1, 4],
            field_offset: vec![1, 2],
            field_fixed_size: vec![1, 4],
            field_null_byte_offset: vec![0, 0],
            field_null_bit_mask: vec![1, 2],
            ..Default::default()
        });
        let bytes = vec![0b11, 0, 0, 0, 0, 0];
        let row = Row::from_dbos_raw(Arc::from(desc.to_column_descs()), bytes, desc).unwrap();
        assert!(row.try_get::<_, bool>(0).is_err());
        assert!(row.try_get::<_, i32>(1).is_err());
        assert!(row.try_get::<_, f64>(1).is_err());
        assert_eq!(row.try_get::<_, Option<bool>>(0).unwrap(), None);
        assert_eq!(row.try_get::<_, Option<i32>>(1).unwrap(), None);
    }
}
