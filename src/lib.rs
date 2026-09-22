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

//! Pure Rust driver for IBM Netezza / PureData System for Analytics.
//!
//! 1:1 port of the proven Node.js driver (`@justybase/netezza-driver`,
//! itself a TypeScript reimplementation of the C# `JustyBase.NetezzaDriver`).
//! The handshake, authentication (plain / MD5 / SHA256), dual text and binary
//! row formats, Netezza numeric decoding and the defensive protocol-sync logic
//! mirror the Node implementation byte for byte (validated against live
//! appliance captures).
//!
//! The crate README contains the complete integration guide, including
//! connection strings, pooling, metadata, bounded streaming, TLS, testing and
//! the current C# / Node / Rust performance measurements.
//!
//! Choose [`NzConnection::query`] for small or moderate buffered results and
//! [`NzConnection::execute_stream`] for bounded-memory consumption. The
//! [`AsyncNzConnection`] facade moves blocking protocol work to Tokio's
//! blocking pool; it is useful in async applications while preserving the
//! driver's single-connection serialization semantics.
//!
//! # Quick start
//! ```no_run
//! use nz_rust::{NzConnection, NzConnectionConfig};
//!
//! let cfg = NzConnectionConfig {
//!     host: "nz-host".into(),
//!     database: "JUST_DATA".into(),
//!     user: "admin".into(),
//!     password: "password".into(),
//!     ..Default::default()
//! };
//! let mut conn = NzConnection::connect(&cfg).unwrap();
//! let result = conn.query("SELECT 1 AS ONE", &[]).unwrap();
//! for row in &result.result_sets[0].rows {
//!     println!("{:?}", row);
//! }
//! ```

pub mod async_pool;
pub mod asynchronous;
pub mod blocking;
pub mod buffer;
pub mod cancel;
pub mod config;
pub mod connection;
pub mod error;
pub mod export;
pub mod handshake;
pub mod messages;
pub mod metadata;
pub mod native_async;
pub mod params;
pub mod pool;
pub mod reader;
pub mod tuple_desc;
pub mod types;

pub use async_pool::{AsyncNzPool, AsyncNzPoolConfig, AsyncPooledConnection};
pub use asynchronous::AsyncNzConnection;
pub use blocking::BlockingClient;
pub use config::{parse_connection_string, NzConnectionConfig, SecurityLevel};
pub use connection::{
    register_import_data, unregister_import_data, NzCommand, NzConnection, QueryResult,
    QueryStreamSink, ResultSet, Row, RowIndex, StreamResultSet, StreamSummary,
};
pub use error::{NzDatabaseError, NzError, NzResult};
pub use export::{render_value, result_to_text, write_result_to_txt};
pub use metadata::{
    NzColumnInfo, NzConstraintInfo, NzDatabaseInfo, NzDistributionKeyInfo, NzFunctionInfo,
    NzMetadata, NzObjectDetailInfo, NzObjectInfo, NzOrganizeKeyInfo, NzProcedureInfo,
    NzSessionInfo, NzSynonymInfo, NzTableInfo, NzTableSizeInfo, NzViewInfo,
};
pub use native_async::{connect, Client, Connection, RowStream};
pub use params::{substitute_bound_parameters, NzParameter};
pub use pool::{NzPool, NzPoolConfig, PooledConnection};
pub use reader::{ColumnDataType, ColumnMetadata, NzDataReader, SchemaRow, SchemaTable};
pub use rust_decimal::Decimal;
pub use tuple_desc::{ColumnDesc, DbosTupleDesc};
pub use types::value::{FromSql, FromSqlRaw, NzValue, RawValue, ToSql};

/// Netezza client type identifiers sent during the handshake.
///
/// Known values (see Node driver `clientTypes.ts` / C# `ClientTypeId`):
/// the handshake identifies the session to the appliance for auditing and
/// gating of server-side features.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientTypeId(pub i16);

impl ClientTypeId {
    pub const INVALID: i16 = -1;
    pub const NONE: i16 = 0;
    pub const SQL: i16 = 1;
    pub const SQL_ODBC: i16 = 2;
    pub const SQL_JDBC: i16 = 3;
    pub const LOAD: i16 = 4;
    pub const CLIENT: i16 = 5;
    pub const BNR: i16 = 6;
    pub const RECLAIM: i16 = 7;
    pub const UNKNOWN: i16 = 8;
    pub const SQL_OLEDB: i16 = 9;
    pub const INTERNAL: i16 = 10;
    pub const SQL_DOTNET: i16 = 11;
    pub const SQL_GOLANG: i16 = 12;
    pub const SQL_PYTHON: i16 = 13;
    pub const UNKNOWN2: i16 = 14;
    pub const NODE: i16 = 15;
}

/// Sanity clamp for a client-supplied client type (port of `normalizeClientType`).
pub fn normalize_client_type(value: i16) -> i16 {
    // Any signed 16-bit value is accepted by the server; keep the pass-through
    // but reserve this hook for future validation, mirroring the Node driver.
    value
}
