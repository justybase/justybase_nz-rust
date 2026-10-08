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
//! Use [`Client`] in Tokio applications, [`blocking::Client`] in synchronous
//! code, and [`Pool`] or [`blocking::Pool`] when connections should be reused.
//! Older connection and reader APIs are available with the `compat` feature.
//!
//! # Quick start
//! ```no_run
//! use nz_rust::{Client, NzConnectionConfig};
//!
//! # async fn example() -> nz_rust::NzResult<()> {
//! let config = NzConnectionConfig::new("nz-host", "JUST_DATA", "admin", "secret");
//! // `Client::connect` spawns the protocol task itself. To drive it on your own
//! // executor instead, use `nz_rust::connect`, which returns `(Client, Connection)`.
//! let client = Client::connect(&config).await?;
//! let row = client.query_one("SELECT 1 AS one", &[]).await?;
//! let value: i32 = row.try_get("one")?;
//! println!("{value}");
//! client.close().await?;
//! # Ok(())
//! # }
//! ```

#[cfg(feature = "compat")]
#[allow(dead_code)]
pub mod async_pool;
#[cfg(not(feature = "compat"))]
#[allow(dead_code)]
mod async_pool;
#[cfg(feature = "compat")]
#[allow(dead_code)]
pub mod asynchronous;
#[cfg(not(feature = "compat"))]
#[allow(dead_code)]
mod asynchronous;
pub mod blocking;
pub mod buffer;
pub mod cancel;
pub mod config;
#[cfg(feature = "compat")]
#[allow(dead_code)]
pub mod connection;
#[cfg(not(feature = "compat"))]
#[allow(dead_code)]
mod connection;
pub mod error;
pub mod export;
pub mod handshake;
pub mod messages;
#[cfg(feature = "compat")]
#[allow(dead_code)]
pub mod metadata;
#[cfg(not(feature = "compat"))]
#[allow(dead_code)]
mod metadata;
pub mod native_async;
pub mod params;
#[cfg(feature = "compat")]
#[allow(dead_code)]
pub mod pool;
#[cfg(not(feature = "compat"))]
#[allow(dead_code)]
mod pool;
#[cfg(feature = "compat")]
#[allow(dead_code)]
pub mod reader;
#[cfg(not(feature = "compat"))]
#[allow(dead_code)]
mod reader;
pub mod tuple_desc;
pub mod types;

#[cfg(feature = "compat")]
pub use async_pool::{AsyncNzPool, AsyncNzPoolConfig, AsyncPooledConnection};
pub use async_pool::{Pool, PoolConfig, PooledRowStream, PooledTransaction};
#[cfg(feature = "compat")]
pub use asynchronous::AsyncNzConnection;
#[cfg(feature = "compat")]
pub use blocking::{BlockingClient, LegacyTransaction};
pub use config::{parse_connection_string, ConfigBuilder, NzConnectionConfig, SecurityLevel};
#[cfg(feature = "compat")]
pub use connection::{NzCommand, NzConnection};
pub use connection::{
    QueryResult, QueryStreamSink, ResultSet, Row, RowIndex, StreamResultSet, StreamSummary,
};
pub use error::{NzDatabaseError, NzError, NzResult};
pub use export::{render_value, result_to_text, write_result_to_txt, TextExportSink};
#[cfg(feature = "compat")]
pub use metadata::NzMetadata;
pub use metadata::{
    AsyncMetadata, CatalogSnapshot, NzColumnInfo, NzConstraintInfo, NzDatabaseInfo,
    NzDdlBatchResult, NzDetailedColumnInfo, NzDistributionKeyInfo, NzFunctionInfo, NzGroupInfo,
    NzObjectDetailInfo, NzObjectInfo, NzOrganizeKeyInfo, NzProcedureInfo, NzQueryHistoryInfo,
    NzSequenceInfo, NzSessionInfo, NzSynonymInfo, NzTableInfo, NzTableKeyInfo, NzTableSizeInfo,
    NzUserInfo, NzViewInfo,
};
pub use native_async::{
    connect, Client, Connection, QueryEventStream, QueryOptions, QueryStreamEvent, RowBatchStream,
    RowStream, Transaction,
};
pub use params::{substitute_bound_parameters, NzParameter};
#[cfg(feature = "compat")]
pub use pool::{NzPool, NzPoolConfig, PooledConnection};
#[cfg(feature = "compat")]
pub use reader::{ColumnDataType, ColumnMetadata, NzDataReader, SchemaRow, SchemaTable};
pub use rust_decimal::Decimal;
pub use tuple_desc::{ColumnDesc, DbosTupleDesc};
#[cfg(feature = "chrono")]
pub use types::value::NzTimeTz;
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

pub use types::exact_numeric::NzNumeric;

pub mod external;
pub use external::ExternalFilePolicy;

pub use types::temporal::{NzDate, NzInterval, NzTime, NzTimestamp, NzTimetz};

/// Legacy import registry and ADO.NET-style APIs, enabled explicitly for migration.
#[cfg(feature = "compat")]
pub mod compat {
    pub use crate::async_pool::{AsyncNzPool, AsyncNzPoolConfig, AsyncPooledConnection};
    pub use crate::asynchronous::AsyncNzConnection;
    pub use crate::blocking::{BlockingClient, LegacyTransaction};
    pub use crate::connection::{
        register_async_import_reader, register_import_data, register_import_reader,
        unregister_import_data, NzCommand, NzConnection,
    };
    pub use crate::metadata::NzMetadata;
    pub use crate::pool::{NzPool, NzPoolConfig, PooledConnection};
    pub use crate::reader::{ColumnDataType, ColumnMetadata, NzDataReader, SchemaRow, SchemaTable};
}
#[cfg(feature = "compat")]
pub use compat::{
    register_async_import_reader, register_import_data, register_import_reader,
    unregister_import_data,
};
