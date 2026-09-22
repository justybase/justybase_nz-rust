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
    code, parse_command_complete_rows, parse_transaction_state, TransactionState,
};
use crate::params::{substitute_bound_parameters, substitute_parameters, NzParameter};
use crate::tuple_desc::{parse_row_description, ColumnDesc, DbosTupleDesc};
use crate::types::text::{build_simple_query_packet, parse_text_data_row_into, text_row_layout};
use crate::types::value::{FromSql, FromSqlRaw, NzValue, RawValue, ToSql};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::ops::Range;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Virtual import streams (external-table `l` import from memory)
// ---------------------------------------------------------------------------

fn import_registry() -> &'static Mutex<HashMap<String, Vec<u8>>> {
    static REG: OnceLock<Mutex<HashMap<String, Vec<u8>>>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register an in-memory payload for an external-table import filename.
///
/// When the appliance asks to import `id` (the `DATAOBJECT` path sent in the
/// SQL text), the driver streams these bytes instead of opening a file —
/// the Rust analog of `NzConnection.registerImportStream` in the Node driver.
pub fn register_import_data(id: &str, data: Vec<u8>) {
    if let Ok(mut reg) = import_registry().lock() {
        reg.insert(id.to_string(), data);
    }
}

/// Remove a previously registered virtual import payload.
pub fn unregister_import_data(id: &str) {
    if let Ok(mut reg) = import_registry().lock() {
        reg.remove(id);
    }
}

pub(crate) fn take_import_data(id: &str) -> Option<Vec<u8>> {
    import_registry().lock().ok()?.remove(id)
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
/// # use nz_rust::NzConnection;
/// # fn f(reader_row: nz_rust::connection::Row) -> nz_rust::error::NzResult<()> {
/// let id: i32 = reader_row.try_get(0)?;
/// let name: Option<String> = reader_row.try_get("name")?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Row {
    columns: Arc<[ColumnDesc]>,
    storage: RowStorage,
}

#[derive(Debug, Clone)]
enum RowStorage {
    Values(Vec<NzValue>),
    Raw(Arc<RawRowData>),
}

#[derive(Debug)]
struct RawRowData {
    payload: Arc<[u8]>,
    kind: RawRowKind,
    decoded: OnceLock<Vec<NzValue>>,
}

#[derive(Debug)]
enum RawRowKind {
    Text {
        fields: Vec<Option<Range<usize>>>,
    },
    Dbos {
        descriptor: Arc<DbosTupleDesc>,
        fields: Vec<crate::tuple_desc::DbosFieldLayout>,
    },
}

impl RawRowData {
    fn decode_value(&self, index: usize, columns: &[ColumnDesc]) -> NzResult<NzValue> {
        match &self.kind {
            RawRowKind::Text { fields } => {
                let Some(range) = fields.get(index).and_then(|range| range.as_ref()) else {
                    return Ok(NzValue::Null);
                };
                let bytes = &self.payload[range.clone()];
                let text = String::from_utf8_lossy(bytes);
                let column = columns.get(index).ok_or_else(|| {
                    NzError::Protocol("text row has more fields than its description".into())
                })?;
                Ok(crate::types::text::parse_text_value(
                    &text,
                    column.type_oid,
                    column.type_mod,
                ))
            }
            RawRowKind::Dbos { descriptor, fields } => {
                let field = fields.get(index).ok_or_else(|| {
                    NzError::Config(format!("row field index {index} is out of range"))
                })?;
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
            RawRowKind::Text { fields } => fields
                .iter()
                .enumerate()
                .map(|(index, range)| match range {
                    None => Ok(NzValue::Null),
                    Some(range) => {
                        let bytes = &self.payload[range.clone()];
                        let text = String::from_utf8_lossy(bytes);
                        let column = columns.get(index).ok_or_else(|| {
                            NzError::Protocol(
                                "text row has more fields than its description".into(),
                            )
                        })?;
                        Ok(crate::types::text::parse_text_value(
                            &text,
                            column.type_oid,
                            column.type_mod,
                        ))
                    }
                })
                .collect(),
            RawRowKind::Dbos { .. } => (0..columns.len())
                .map(|index| self.decode_value(index, columns))
                .collect(),
        }
    }

    fn field_bytes(&self, index: usize) -> Option<&[u8]> {
        match &self.kind {
            RawRowKind::Text { fields } => fields
                .get(index)
                .and_then(|range| range.as_ref())
                .map(|range| &self.payload[range.clone()]),
            RawRowKind::Dbos { descriptor, fields } => {
                let field = fields.get(index)?;
                if field.is_null {
                    None
                } else if descriptor.field_fixed_size.get(index).copied().unwrap_or(0) != 0 {
                    let size = descriptor.field_fixed_size[index] as usize;
                    let end = field.start.checked_add(size)?;
                    self.payload.get(field.start..end)
                } else {
                    let prefix_end = field.start.checked_add(2)?;
                    let encoded = u16::from_le_bytes(
                        self.payload.get(field.start..prefix_end)?.try_into().ok()?,
                    ) as usize;
                    let value_start = prefix_end;
                    let end = field.start.checked_add(encoded)?;
                    self.payload.get(value_start..end)
                }
            }
        }
    }
}

impl Row {
    pub fn new(columns: Vec<ColumnDesc>, values: Vec<NzValue>) -> Self {
        Row {
            columns: Arc::from(columns),
            storage: RowStorage::Values(values),
        }
    }

    pub(crate) fn from_text_raw(columns: Arc<[ColumnDesc]>, payload: Vec<u8>) -> NzResult<Self> {
        let fields = text_row_layout(&payload, &columns).map_err(NzError::Protocol)?;
        Ok(Self {
            columns,
            storage: RowStorage::Raw(Arc::new(RawRowData {
                payload: Arc::from(payload),
                kind: RawRowKind::Text { fields },
                decoded: OnceLock::new(),
            })),
        })
    }

    pub(crate) fn from_dbos_raw(
        columns: Arc<[ColumnDesc]>,
        payload: Vec<u8>,
        descriptor: Arc<DbosTupleDesc>,
    ) -> NzResult<Self> {
        let fields = descriptor.row_layout(&payload)?;
        Ok(Self {
            columns,
            storage: RowStorage::Raw(Arc::new(RawRowData {
                payload: Arc::from(payload),
                kind: RawRowKind::Dbos { descriptor, fields },
                decoded: OnceLock::new(),
            })),
        })
    }

    pub fn columns(&self) -> &[ColumnDesc] {
        &self.columns
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn values(&self) -> &[NzValue] {
        match &self.storage {
            RowStorage::Values(values) => values,
            RowStorage::Raw(raw) => raw.decoded.get_or_init(|| {
                raw.decode_all(&self.columns)
                    .unwrap_or_else(|_| vec![NzValue::Null; self.columns.len()])
            }),
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
            Some(p) => Ok(&self.values()[p]),
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
        let column = self.columns.get(position).ok_or_else(|| {
            NzError::Config(format!("row index out of range: ordinal {position}"))
        })?;
        match &self.storage {
            RowStorage::Values(values) => Ok(RawValue::from_decoded(
                &values[position],
                column.type_oid,
                column.type_mod,
                column.format,
            )),
            RowStorage::Raw(raw) => Ok(RawValue::from_parts(
                raw.field_bytes(position),
                None,
                column.type_oid,
                column.type_mod,
                column.format,
            )),
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
        T::from_sql(self.try_get_raw(idx)?)
    }

    /// Panicking typed extraction through the lazy raw-value interface.
    pub fn get_raw<'a, I: RowIndex, T: FromSqlRaw<'a>>(&'a self, idx: I) -> T {
        self.try_get_raw_typed(idx)
            .unwrap_or_else(|e| panic!("row.get_raw failed: {e}"))
    }

    /// Checked typed extraction through the lazy raw-value interface.
    pub fn try_get_raw_typed<'a, I: RowIndex, T: FromSqlRaw<'a>>(&'a self, idx: I) -> NzResult<T> {
        let position = idx.position(self).ok_or_else(|| {
            NzError::Config(format!("row index out of range: {}", idx.describe()))
        })?;
        let column = self.columns.get(position).ok_or_else(|| {
            NzError::Config(format!("row index out of range: ordinal {position}"))
        })?;
        match &self.storage {
            RowStorage::Values(values) => T::from_sql_raw(RawValue::from_decoded(
                &values[position],
                column.type_oid,
                column.type_mod,
                column.format,
            )),
            RowStorage::Raw(raw) => {
                if raw.decoded.get().is_none() {
                    let decoded = raw.decode_all(&self.columns)?;
                    let _ = raw.decoded.set(decoded);
                }
                let decoded = raw.decoded.get().ok_or_else(|| {
                    NzError::Protocol("lazy row value cache was not initialized".into())
                })?;
                let value = decoded.get(position).ok_or_else(|| {
                    NzError::Config(format!("row index out of range: ordinal {position}"))
                })?;
                T::from_sql_raw(RawValue::from_parts(
                    raw.field_bytes(position),
                    Some(value),
                    column.type_oid,
                    column.type_mod,
                    column.format,
                ))
            }
        }
    }

    /// Row as `name → value` pairs in column order.
    pub fn as_map(&self) -> Vec<(String, NzValue)> {
        self.columns
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
        if *self < row.columns.len() {
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
        row.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(self))
    }
    fn describe(&self) -> String {
        format!("column '{self}'")
    }
}

impl RowIndex for String {
    fn position(&self, row: &Row) -> Option<usize> {
        row.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(self))
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

/// Buffered result of [`NzConnection::query`]: every result set of a
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
/// result store. Returning an error aborts the stream; the connection is then
/// considered unsafe to reuse because the remaining backend response has not
/// necessarily been drained.
pub trait QueryStreamSink {
    fn on_columns(
        &mut self,
        result_set_index: usize,
        columns: &[ColumnDesc],
        nullability: Option<&[bool]>,
    ) -> NzResult<()>;

    fn on_row(&mut self, result_set_index: usize, row: Row) -> NzResult<()>;

    /// Borrowed hot-path callback used by [`NzConnection::execute_stream`].
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
    /// Absolute deadline for the command currently being drained.  Keeping
    /// the deadline on the connection lets every low-level buffered read
    /// enforce one wall-clock budget, including multi-row payloads.
    command_deadline: Option<Instant>,
    /// Set after a local timeout/cancel when the server may still be sending
    /// the abandoned command's terminal response. The next command must wait
    /// for ReadyForQuery before writing its packet.
    protocol_sync_required: bool,
}

impl NzConnection {
    // -- lifecycle ---------------------------------------------------------

    /// Open a TCP connection and run the handshake + authentication.
    pub fn connect(config: &NzConnectionConfig) -> NzResult<Self> {
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
                        command_deadline: None,
                        protocol_sync_required: false,
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
        if let Some(mut s) = self.stream.take() {
            let _ = s.shutdown();
        }
        self.buffer.clear();
    }

    pub fn is_closed(&self) -> bool {
        !self.connected || self.stream.is_none()
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
        Ok(self.query(sql, params)?.rows().to_vec())
    }

    /// Exactly one row; errors when the first set holds none.
    pub fn query_one(&mut self, sql: &str, params: &[&dyn ToSql]) -> NzResult<Row> {
        let rows = self.query_rows(sql, params)?;
        rows.into_iter()
            .next()
            .ok_or_else(|| NzError::Config("query_one: no rows returned".into()))
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

    /// Execute a non-query batch; returns affected rows (`-1` for DDL, same
    /// as the C# driver when the backend reports no count).
    pub fn execute(&mut self, sql: &str, params: &[&dyn ToSql]) -> NzResult<i64> {
        let values: Vec<NzValue> = params.iter().map(|p| p.to_nz_value()).collect();
        self.execute_values(sql, &values)
    }

    pub fn execute_values(&mut self, sql: &str, params: &[NzValue]) -> NzResult<i64> {
        let final_sql = substitute_parameters(sql, params).map_err(NzError::Config)?;
        let res = self.run_batch(&final_sql)?;
        Ok(res.rows_affected)
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
            .run_batch_with_duration(&final_sql, timeout)?
            .rows_affected)
    }

    /// Execute without parameters and discard rows (DDL / `SET` / scripts).
    pub fn batch_execute(&mut self, sql: &str) -> NzResult<()> {
        self.run_batch(sql).map(|_| ())
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
        if let Some(mut s) = self.stream.take() {
            let _ = s.shutdown();
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
            if e.is_protocol_fault() {
                self.mark_protocol_fault();
            }
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
                if e.is_protocol_fault() {
                    self.mark_protocol_fault();
                }
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
            if e.is_protocol_fault() {
                self.mark_protocol_fault();
            }
            self.executing = false;
            self.command_deadline = None;
            return Err(e);
        }
        let _ = had_start;
        let result = self.drain_response_stream(sink);
        self.executing = false;
        self.command_deadline = None;
        match result {
            Ok(mut summary) => {
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
                if e.is_protocol_fault() {
                    self.mark_protocol_fault();
                }
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
        let (result, _) = self.drain_response_with_sink(None)?;
        Ok(result)
    }

    fn drain_response_stream<S: QueryStreamSink>(
        &mut self,
        sink: &mut S,
    ) -> NzResult<StreamSummary> {
        let (result, result_sets) = self.drain_response_with_sink(Some(sink))?;
        Ok(StreamSummary {
            result_sets,
            rows_affected: result.rows_affected,
            notices: result.notices,
        })
    }

    fn drain_response_with_sink(
        &mut self,
        mut sink: Option<&mut dyn QueryStreamSink>,
    ) -> NzResult<(QueryResult, Vec<StreamResultSet>)> {
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
        let mut error: Option<NzError> = None;

        let outcome: NzResult<()> = (|| {
            loop {
                let msg_type = self.read_type_byte()?;
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
                            emit_stream_row(
                                &mut sink,
                                &mut stream_result_sets,
                                &mut stream_columns_sent,
                                result_set_index,
                                &stream_values,
                            )?;
                            while self.try_read_available_dbos_row(
                                &tupdesc,
                                has_tupdesc,
                                &mut stream_values,
                            )? {
                                emit_stream_row(
                                    &mut sink,
                                    &mut stream_result_sets,
                                    &mut stream_columns_sent,
                                    result_set_index,
                                    &stream_values,
                                )?;
                            }
                        } else {
                            let payload = self.read_dbos_payload(has_tupdesc)?;
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
                            let row = Row::from_dbos_raw(columns, payload, descriptor)?;
                            push_existing_row(&mut current, row, &nullability, &mut row_columns)?;
                            while let Some(next_payload) =
                                self.try_read_available_dbos_payload(has_tupdesc)?
                            {
                                let columns = row_columns.clone().unwrap_or_else(|| {
                                    Arc::from(cached_columns.as_deref().unwrap_or(&[]))
                                });
                                let descriptor = shared_tupdesc.clone().ok_or_else(|| {
                                    NzError::Protocol("DBOS row descriptor is missing".into())
                                })?;
                                let row = Row::from_dbos_raw(columns, next_payload, descriptor)?;
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
                            finish_stream_result_set(
                                &mut sink,
                                &mut stream_result_sets,
                                &mut stream_columns_sent,
                                &mut current_result_set_index,
                            )?;
                        } else {
                            flush_current(&mut current, &mut sets, &mut row_columns);
                        }
                    }
                    code::READY_FOR_QUERY | code::READY_FOR_QUERY_ALT => {
                        if sink.is_some() {
                            finish_stream_result_set(
                                &mut sink,
                                &mut stream_result_sets,
                                &mut stream_columns_sent,
                                &mut current_result_set_index,
                            )?;
                        } else {
                            flush_current(&mut current, &mut sets, &mut row_columns);
                        }
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
                        let msg = String::from_utf8_lossy(&data)
                            .replace('\0', "")
                            .trim()
                            .to_string();
                        if !msg.is_empty() {
                            if let Some(sink) = sink.as_deref_mut() {
                                sink.on_notice(&msg).map_err(stream_sink_error)?;
                            }
                            notices.push(msg);
                        }
                    }
                    code::ROW_DESCRIPTION => {
                        let len = self.read_len("rowDescriptionPayload")?;
                        let data = self.read_payload(len, "rowDescriptionPayload")?;
                        let cols = parse_row_description(&data)?;
                        if sink.is_some() {
                            finish_stream_result_set(
                                &mut sink,
                                &mut stream_result_sets,
                                &mut stream_columns_sent,
                                &mut current_result_set_index,
                            )?;
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
                        }
                        cached_columns = Some(cols.clone());
                        nullability = None; // text path reports no nullability
                        if sink.is_none() {
                            row_columns = Some(Arc::from(cols.as_slice()));
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
                            emit_stream_row(
                                &mut sink,
                                &mut stream_result_sets,
                                &mut stream_columns_sent,
                                result_set_index,
                                &stream_values,
                            )?;
                        } else {
                            let cols = current_columns(&current, &cached_columns)?;
                            let columns = row_columns
                                .clone()
                                .unwrap_or_else(|| Arc::from(cols.as_slice()));
                            let row = Row::from_text_raw(columns, data)?;
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
                            emit_stream_columns(
                                &mut sink,
                                &mut stream_result_sets,
                                &mut stream_columns_sent,
                                result_set_index,
                            )?;
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

        match outcome {
            Err(e) => {
                if is_command_timeout(&e) {
                    // Mirror the Node driver: reject now, cancel out-of-band so
                    // the appliance stops the abandoned execution. The cancel
                    // response is asynchronous, so the next command must
                    // drain it before sending a new packet.
                    self.protocol_sync_required = true;
                    let _ = self.cancel();
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
                    )?;
                } else {
                    flush_current(&mut current, &mut sets, &mut row_columns);
                }
                if let Some(e) = error {
                    Err(e)
                } else {
                    Ok((
                        QueryResult {
                            result_sets: sets,
                            rows_affected,
                            notices,
                        },
                        stream_result_sets,
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
        let mut buf = std::mem::take(&mut self.buffer);
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
        let mut buf = std::mem::take(&mut self.buffer);
        let r = buf.skip(&mut guard, 4);
        self.stream_restore(guard, buf);
        r
    }

    fn read_len(&mut self, field: &str) -> NzResult<usize> {
        let mut guard = self.stream_take()?;
        let mut buf = std::mem::take(&mut self.buffer);
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
        let mut buf = std::mem::take(&mut self.buffer);
        let r = buf.read_bytes(&mut guard, len);
        self.stream_restore(guard, buf);
        r
    }

    /// Read one complete DBOS row payload after the shared frame header has
    /// already been consumed. Keeping the wire framing separate from value
    /// decoding lets buffered rows retain their original bytes and defer
    /// conversion until `Row::try_get` is called.
    fn read_dbos_payload(&mut self, has_tupdesc: bool) -> NzResult<Vec<u8>> {
        if !has_tupdesc {
            return Err(NzError::Protocol(
                "Invalid RowStandard sequence: row description is missing; reconnect is required."
                    .into(),
            ));
        }
        let mut guard = self.stream_take()?;
        let mut buf = std::mem::take(&mut self.buffer);
        let result = (|| {
            buf.skip(&mut guard, 4)?;
            let row_len = buf.read_i32(&mut guard)?;
            let row_len = validate_protocol_length(row_len, "rowStandardPayload", false)? as usize;
            buf.read_bytes(&mut guard, row_len)
        })();
        self.stream_restore(guard, buf);
        result
    }

    /// Return the next complete DBOS row already buffered after the first row
    /// of a response. An incomplete frame is left untouched for the normal
    /// blocking protocol loop.
    fn try_read_available_dbos_payload(&mut self, has_tupdesc: bool) -> NzResult<Option<Vec<u8>>> {
        if !has_tupdesc {
            return Err(NzError::Protocol(
                "Invalid RowStandard sequence: row description is missing; reconnect is required."
                    .into(),
            ));
        }
        let available = self.buffer.slice();
        if available.len() < 13 || available[0] != code::ROW_STANDARD {
            return Ok(None);
        }
        let row_len = i32::from_be_bytes(available[9..13].try_into().unwrap());
        let row_len = validate_protocol_length(row_len, "rowStandardPayload", false)? as usize;
        let frame_len = 13usize
            .checked_add(row_len)
            .ok_or_else(|| NzError::Protocol("DBOS row frame length overflow".into()))?;
        if available.len() < frame_len {
            return Ok(None);
        }
        let payload = available[13..frame_len].to_vec();
        self.buffer.advance(frame_len);
        Ok(Some(payload))
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
        let mut buf = std::mem::take(&mut self.buffer);
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
                let mut buf = std::mem::take(&mut self.buffer);
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
                    let mut buf = std::mem::take(&mut self.buffer);
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
                let mut buf = std::mem::take(&mut self.buffer);
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
                let mut buf = std::mem::take(&mut self.buffer);
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
        let mut buf = std::mem::take(&mut self.buffer);
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
        let filename = String::from_utf8_lossy(&name).replace('\0', "");
        match File::create(&filename) {
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
                let mut buf = std::mem::take(&mut self.buffer);
                let r = buf.read_i32(&mut guard);
                self.stream_restore(guard, buf);
                r?
            };
            match status {
                1 => {
                    // DATA
                    let n: usize = {
                        let mut guard = self.stream_take()?;
                        let mut buf = std::mem::take(&mut self.buffer);
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
                        let mut buf = std::mem::take(&mut self.buffer);
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
        let filename = String::from_utf8_lossy(&name_bytes).to_string();
        let mut guard = self.stream_take()?;
        let mut buf = std::mem::take(&mut self.buffer);
        let header = (|| {
            let _host_version = buf.read_i32(&mut guard)?;
            Ok::<_, NzError>(())
        })();
        self.stream_restore(guard, buf);
        header?;
        self.write_all_raw(&1i32.to_be_bytes())?;
        let mut guard = self.stream_take()?;
        let mut buf = std::mem::take(&mut self.buffer);
        let cfg: NzResult<usize> = (|| {
            let _format = buf.read_i32(&mut guard)?;
            let size = buf.read_i32(&mut guard)?;
            Ok(validate_protocol_length(size, "externalTableImportBufferSize", true)? as usize)
        })();
        self.stream_restore(guard, buf);
        let buf_size = cfg?.max(1);

        // Virtual stream first, then the filesystem (Node parity).
        if let Some(data) = take_import_data(&filename) {
            return self.send_import_bytes(&data, buf_size);
        }
        match std::fs::read(&filename) {
            Ok(data) => self.send_import_bytes(&data, buf_size),
            Err(_) => {
                // File missing → ERROR status (C#/Node parity).
                self.write_all_raw(&2i32.to_be_bytes())?;
                Ok(())
            }
        }
    }

    fn send_import_bytes(&mut self, data: &[u8], buffer_size: usize) -> NzResult<()> {
        // DATA chunks: status(4)=1 + len(4) + bytes, then DONE status(4)=3.
        let chunk_size = buffer_size.max(1);
        let mut off = 0;
        while off < data.len() {
            let chunk = &data[off..(off + chunk_size).min(data.len())];
            let mut header = Vec::with_capacity(8);
            header.extend_from_slice(&1i32.to_be_bytes());
            header.extend_from_slice(&(chunk.len() as i32).to_be_bytes());
            self.write_all_raw(&header)?;
            self.write_all_raw(chunk)?;
            off += chunk.len();
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
        let log_dir = String::from_utf8_lossy(&dir).to_string();
        let mut name_bytes: Vec<u8> = Vec::new();
        loop {
            let b = self.read_payload(1, "fileTransfer.logFilename")?;
            if b[0] == 0 {
                break;
            }
            name_bytes.push(b[0]);
            if name_bytes.len() > 4096 {
                break;
            }
        }
        let filename = String::from_utf8_lossy(&name_bytes).to_string();
        let log_type = {
            let mut guard = self.stream_take()?;
            let mut buf = std::mem::take(&mut self.buffer);
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
        let mut file = File::create(&path).ok();
        loop {
            let n: usize = {
                let mut guard = self.stream_take()?;
                let mut buf = std::mem::take(&mut self.buffer);
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
) -> NzResult<()> {
    if columns_sent[result_set_index] {
        return Ok(());
    }
    if let Some(sink) = sink.as_deref_mut() {
        let result_set = &result_sets[result_set_index];
        sink.on_columns(
            result_set_index,
            &result_set.columns,
            result_set.nullability.as_deref(),
        )
        .map_err(stream_sink_error)?;
    }
    columns_sent[result_set_index] = true;
    Ok(())
}

fn emit_stream_row(
    sink: &mut Option<&mut dyn QueryStreamSink>,
    result_sets: &mut [StreamResultSet],
    columns_sent: &mut [bool],
    result_set_index: usize,
    values: &[NzValue],
) -> NzResult<()> {
    emit_stream_columns(sink, result_sets, columns_sent, result_set_index)?;
    if let Some(sink) = sink.as_deref_mut() {
        let columns = &result_sets[result_set_index].columns;
        sink.on_values(result_set_index, columns, values)
            .map_err(stream_sink_error)?;
    }
    result_sets[result_set_index].row_count += 1;
    Ok(())
}

fn stream_sink_error(error: NzError) -> NzError {
    match error {
        NzError::Protocol(_) => error,
        other => NzError::Protocol(format!(
            "stream sink aborted before the backend response was drained: {other}; reconnect required"
        )),
    }
}

fn finish_stream_result_set(
    sink: &mut Option<&mut dyn QueryStreamSink>,
    result_sets: &mut [StreamResultSet],
    columns_sent: &mut [bool],
    current: &mut Option<usize>,
) -> NzResult<()> {
    if let Some(index) = current.take() {
        emit_stream_columns(sink, result_sets, columns_sent, index)?;
    }
    Ok(())
}

fn current_columns(
    current: &Option<ResultSet>,
    cached: &Option<Vec<ColumnDesc>>,
) -> NzResult<Vec<ColumnDesc>> {
    if let Some(set) = current {
        Ok(set.columns.clone())
    } else if let Some(cols) = cached {
        Ok(cols.clone())
    } else {
        Err(NzError::Protocol(
            "Invalid DataRow sequence: row description is missing; reconnect is required.".into(),
        ))
    }
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
        let raw = row.try_get_raw_value(0).unwrap();
        assert_eq!(raw.as_bytes(), Some(b"4" as &[u8]));
        assert_eq!(row.try_get::<_, i32>(0).unwrap(), 4);
        let name: &str = row.try_get_raw_typed(1).unwrap();
        assert_eq!(name, "net");
        assert_eq!(row.values()[0], NzValue::Int4(4));
        assert_eq!(row.values()[1], NzValue::Text("net".into()));
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
            command_deadline: None,
            protocol_sync_required: false,
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
    fn sink_failure_is_connection_fatal() {
        let error = stream_sink_error(NzError::Config("consumer stopped".into()));
        assert!(error.is_protocol_fault());
        assert!(error.to_string().contains("reconnect required"));
    }
}
