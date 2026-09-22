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

//! Native Tokio transport and a tokio-postgres-shaped client facade.
//!
//! The legacy [`crate::asynchronous::AsyncNzConnection`] remains available as
//! a compatibility wrapper.  This module is the new transport: socket reads
//! and writes are performed by Tokio directly, while one connection still
//! serializes SQL operations because that is a property of the Netezza
//! simple-query protocol.

use crate::config::{NzConnectionConfig, SecurityLevel};
use crate::connection::{QueryResult, Row};
use crate::error::{parse_backend_error_fields, validate_protocol_length, NzError, NzResult};
use crate::messages::{code, parse_command_complete_rows};
use crate::params::substitute_parameters;
use crate::tuple_desc::{parse_row_description, ColumnDesc, DbosTupleDesc};
use crate::types::value::{NzValue, ToSql};
use bytes::{Buf, BytesMut};
use futures_core::Stream;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

const CP_VERSION_6: i16 = 6;
const CP_VERSION_4: i16 = 4;
const CP_VERSION_5: i16 = 5;
const CP_VERSION_2: i16 = 2;
const PG_PROTOCOL_3: i16 = 3;
const PG_PROTOCOL_5: i16 = 5;
const HSV2_CLIENT_BEGIN: i16 = 1;
const HSV2_DB: i16 = 2;
const HSV2_USER: i16 = 3;
const HSV2_REMOTE_PID: i16 = 6;
const HSV2_CLIENT_TYPE: i16 = 8;
const HSV2_PROTOCOL: i16 = 9;
const HSV2_SSL_NEGOTIATE: i16 = 11;
const HSV2_SSL_CONNECT: i16 = 12;
const HSV2_APPNAME: i16 = 13;
const HSV2_CLIENT_OS: i16 = 14;
const HSV2_CLIENT_HOST_NAME: i16 = 15;
const HSV2_CLIENT_OS_USER: i16 = 16;
const HSV2_64BIT_VARLENA_ENABLED: i16 = 17;
const HSV2_CLIENT_DONE: i16 = 1000;
const AUTH_REQ_OK: i32 = 0;
const AUTH_REQ_PASSWORD: i32 = 3;
const AUTH_REQ_MD5: i32 = 5;
const AUTH_REQ_SHA256: i32 = 6;
const MAX_BUFFERED_FRAME: usize = 128 * 1024 * 1024;

enum AsyncTransport {
    Plain(TcpStream),
    #[cfg(feature = "ssl")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

type AsyncResultSet = (Arc<[ColumnDesc]>, Vec<Row>, Option<Vec<bool>>);

impl AsyncRead for AsyncTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.as_mut().get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "ssl")]
            Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for AsyncTransport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.as_mut().get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, data),
            #[cfg(feature = "ssl")]
            Self::Tls(stream) => Pin::new(stream).poll_write(cx, data),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.as_mut().get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "ssl")]
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.as_mut().get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "ssl")]
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

type Response<T> = oneshot::Sender<NzResult<T>>;

enum Request {
    Query {
        sql: String,
        response: Response<QueryResult>,
    },
    Stream {
        sql: String,
        rows: mpsc::Sender<NzResult<Row>>,
    },
    Close {
        response: Response<()>,
    },
}

/// Bounded row stream for one query. The connection task keeps draining the
/// wire while `poll_next` applies backpressure through the bounded channel.
pub struct RowStream {
    receiver: mpsc::Receiver<NzResult<Row>>,
}

impl Stream for RowStream {
    type Item = NzResult<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().receiver.poll_recv(cx)
    }
}

/// Tokio-native Netezza client.  Clones share one serialized protocol queue.
#[derive(Clone)]
pub struct Client {
    requests: mpsc::Sender<Request>,
}

/// Background protocol driver returned by [`connect`].
pub struct Connection {
    future: Pin<Box<dyn Future<Output = NzResult<()>> + Send>>,
}

impl Future for Connection {
    type Output = NzResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // The boxed future is pinned when Connection is constructed and never
        // moved afterwards.  `Pin<&mut Connection>` therefore permits polling
        // it without exposing the session internals.
        self.get_mut().future.as_mut().poll(cx)
    }
}

/// Connect using Tokio and return a cloneable client plus its protocol task.
pub async fn connect(config: &NzConnectionConfig) -> NzResult<(Client, Connection)> {
    let session = AsyncSession::connect(config).await?;
    let (sender, receiver) = mpsc::channel(32);
    let future = Box::pin(run_connection(session, receiver));
    Ok((Client { requests: sender }, Connection { future }))
}

impl Client {
    /// Connect and spawn the protocol task internally.
    pub async fn connect(config: &NzConnectionConfig) -> NzResult<Self> {
        let (client, connection) = connect(config).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    pub async fn connect_with_str(connection_string: &str) -> NzResult<Self> {
        let config = crate::parse_connection_string(connection_string)?;
        Self::connect(&config).await
    }

    /// Execute one result-producing statement and return its first result set.
    /// A multi-result response is rejected after it has been drained; use
    /// [`Client::query_multi`] for Netezza scripts.
    pub async fn query(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> NzResult<Vec<Row>> {
        let result = self.query_multi(sql, params).await?;
        if result.result_sets.len() > 1 {
            return Err(NzError::Config(
                "query returned multiple result sets; use query_multi".into(),
            ));
        }
        Ok(result
            .result_sets
            .into_iter()
            .next()
            .map(|set| set.rows)
            .unwrap_or_default())
    }

    pub async fn query_multi(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> NzResult<QueryResult> {
        let values: Vec<NzValue> = params.iter().map(|value| value.to_nz_value()).collect();
        let sql = substitute_parameters(sql, &values).map_err(NzError::Config)?;
        self.send_query(sql).await
    }

    /// Start a bounded row stream for the first result set.
    ///
    /// The connection task continues consuming protocol frames while the
    /// returned stream is paused, but only a small bounded channel is allowed
    /// to accumulate rows. Additional Netezza result sets are drained for
    /// protocol safety and reported as an item error.
    pub async fn query_stream(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> NzResult<RowStream> {
        let values: Vec<NzValue> = params.iter().map(|value| value.to_nz_value()).collect();
        let sql = substitute_parameters(sql, &values).map_err(NzError::Config)?;
        let (sender, receiver) = mpsc::channel(32);
        self.requests
            .send(Request::Stream { sql, rows: sender })
            .await
            .map_err(|_| NzError::Closed("connection task is closed".into()))?;
        Ok(RowStream { receiver })
    }

    pub async fn query_one(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> NzResult<Row> {
        let rows = self.query(sql, params).await?;
        rows.into_iter()
            .next()
            .ok_or_else(|| NzError::Config("query_one: no rows returned".into()))
    }

    pub async fn query_opt(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> NzResult<Option<Row>> {
        let rows = self.query(sql, params).await?;
        match rows.len() {
            0 => Ok(None),
            1 => Ok(rows.into_iter().next()),
            count => Err(NzError::Config(format!(
                "query_opt: expected at most one row, got {count}"
            ))),
        }
    }

    pub async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> NzResult<i64> {
        let result = self.query_multi(sql, params).await?;
        Ok(result.rows_affected)
    }

    pub async fn batch_execute(&self, sql: &str) -> NzResult<()> {
        self.send_query(sql.to_owned()).await.map(|_| ())
    }

    pub async fn close(&self) -> NzResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.requests
            .send(Request::Close { response: sender })
            .await
            .map_err(|_| NzError::Closed("connection task is closed".into()))?;
        receiver
            .await
            .map_err(|_| NzError::Closed("connection task is closed".into()))?
    }

    async fn send_query(&self, sql: String) -> NzResult<QueryResult> {
        let (sender, receiver) = oneshot::channel();
        self.requests
            .send(Request::Query {
                sql,
                response: sender,
            })
            .await
            .map_err(|_| NzError::Closed("connection task is closed".into()))?;
        receiver
            .await
            .map_err(|_| NzError::Closed("connection task is closed".into()))?
    }
}

async fn run_connection(
    mut session: AsyncSession,
    mut receiver: mpsc::Receiver<Request>,
) -> NzResult<()> {
    while let Some(request) = receiver.recv().await {
        match request {
            Request::Query { sql, response } => {
                let result = session.query(&sql).await;
                // A database error is a completed protocol exchange.  The
                // server has returned to ReadyForQuery, so the session is
                // still safe to use.  Transport, protocol and timeout errors
                // can leave an incomplete response and must terminate the
                // task instead.
                let reusable = matches!(&result, Ok(_) | Err(NzError::Database(_)));
                let _ = response.send(result);
                if !reusable {
                    return Ok(());
                }
            }
            Request::Stream { sql, rows } => {
                if let Err(error) = session.stream_query(&sql, &rows).await {
                    let reusable = matches!(&error, NzError::Database(_));
                    let _ = rows.send(Err(error)).await;
                    if !reusable {
                        return Ok(());
                    }
                }
            }
            Request::Close { response } => {
                let result = session.close().await;
                let _ = response.send(result);
                return Ok(());
            }
        }
    }
    session.close().await
}

struct AsyncSession {
    stream: Option<AsyncTransport>,
    buffer: BytesMut,
    config: NzConnectionConfig,
    command_number: i32,
    backend_process_id: i32,
    backend_secret_key: i32,
    export_file: Option<File>,
}

impl AsyncSession {
    async fn connect(config: &NzConnectionConfig) -> NzResult<Self> {
        let timeout = Duration::from_secs(config.connection_timeout.max(1));
        let addr = format_host_port(config);
        let stream = tokio::time::timeout(timeout, TcpStream::connect(&addr))
            .await
            .map_err(|_| {
                NzError::Timeout(format!("connection timeout while connecting to {addr}"))
            })?
            .map_err(NzError::Io)?;
        stream.set_nodelay(true).map_err(NzError::Io)?;
        let mut session = Self {
            stream: Some(AsyncTransport::Plain(stream)),
            buffer: BytesMut::with_capacity(65_536),
            config: config.clone(),
            command_number: 0,
            backend_process_id: 0,
            backend_secret_key: 0,
            export_file: None,
        };
        tokio::time::timeout(timeout, session.handshake())
            .await
            .map_err(|_| NzError::Timeout("handshake timeout".into()))??;
        Ok(session)
    }

    async fn close(&mut self) -> NzResult<()> {
        if let Some(mut stream) = self.stream.take() {
            stream.shutdown().await.map_err(NzError::Io)
        } else {
            Ok(())
        }
    }

    fn stream_mut(&mut self) -> NzResult<&mut AsyncTransport> {
        self.stream
            .as_mut()
            .ok_or_else(|| NzError::Closed("async connection is closed".into()))
    }

    async fn handshake(&mut self) -> NzResult<()> {
        let version = self.negotiate_version().await?;
        let database = self.config.database.clone();
        self.write_cstring_frame(HSV2_DB, &database).await?;
        self.expect_ack("database").await?;

        self.write_i32_frame(HSV2_SSL_NEGOTIATE, self.config.security_level.as_i32())
            .await?;
        match self.read_byte().await? {
            b'N' if self.config.security_level != SecurityLevel::OnlySecuredSession => {}
            b'S' => {
                self.write_i16_frame(HSV2_SSL_CONNECT, &[]).await?;
                self.upgrade_tls().await?;
                self.expect_ack("sslHandshake").await?;
            }
            b'N' => {
                return Err(NzError::Protocol(
                    "server refused secure session, but OnlySecuredSession was requested".into(),
                ));
            }
            b'E' => return Err(self.read_backend_error("secureSessionError").await),
            other => {
                return Err(NzError::Protocol(format!(
                    "handshake secure-session response: unexpected byte 0x{other:02x}"
                )))
            }
        }

        let client_os = std::env::consts::OS;
        let user = self.config.user.clone();
        self.write_cstring_frame(HSV2_USER, &user).await?;
        self.expect_ack("user").await?;
        if version == CP_VERSION_4 || version == CP_VERSION_6 {
            let app_name = self.config.app_name.clone();
            self.write_cstring_frame(HSV2_APPNAME, &app_name).await?;
            self.expect_ack("appname").await?;
            self.write_cstring_frame(HSV2_CLIENT_OS, client_os).await?;
            self.expect_ack("clientOs").await?;
            let host_name = self.config.client_host_name.clone();
            self.write_cstring_frame(HSV2_CLIENT_HOST_NAME, &host_name)
                .await?;
            self.expect_ack("clientHostName").await?;
            let os_user = self.config.os_user.clone();
            self.write_cstring_frame(HSV2_CLIENT_OS_USER, &os_user)
                .await?;
            self.expect_ack("clientOsUser").await?;
        }
        self.write_i16_frame(HSV2_PROTOCOL, &[PG_PROTOCOL_3, PG_PROTOCOL_5])
            .await?;
        self.expect_ack("remotePid").await?;
        self.write_i32_frame(HSV2_REMOTE_PID, std::process::id() as i32)
            .await?;
        self.expect_ack("clientType").await?;
        self.write_i16_frame(
            HSV2_CLIENT_TYPE,
            &[crate::normalize_client_type(self.config.client_type)],
        )
        .await?;
        if version >= CP_VERSION_5 {
            self.expect_ack("64bitVarlena").await?;
            self.write_i16_frame(HSV2_64BIT_VARLENA_ENABLED, &[1])
                .await?;
        }
        self.expect_ack("clientDone").await?;
        self.write_i16_frame(HSV2_CLIENT_DONE, &[]).await?;

        match self.read_byte().await? {
            b'R' => {}
            b'E' => return Err(self.read_backend_error("authenticationError").await),
            other => {
                return Err(NzError::Protocol(format!(
                    "authentication: unexpected response byte 0x{other:02x}"
                )))
            }
        }
        let areq = self.read_i32().await?;
        match areq {
            AUTH_REQ_OK => {}
            AUTH_REQ_PASSWORD => {
                let mut payload = self.config.password.as_bytes().to_vec();
                payload.push(0);
                self.write_auth_response(&payload).await?;
            }
            AUTH_REQ_MD5 => {
                let salt = self.read_bytes(2).await?;
                let mut data = Vec::with_capacity(salt.len() + self.config.password.len());
                data.extend_from_slice(&salt);
                data.extend_from_slice(self.config.password.as_bytes());
                let digest = md5::compute(data);
                self.write_auth_response(
                    format!("{}\0", base64_encode(&digest.0).trim_end_matches('=')).as_bytes(),
                )
                .await?;
            }
            AUTH_REQ_SHA256 => {
                let salt = self.read_bytes(2).await?;
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                hasher.update(&salt);
                hasher.update(self.config.password.as_bytes());
                let digest = hasher.finalize();
                self.write_auth_response(format!("{}\0", base64_encode(&digest)).as_bytes())
                    .await?;
            }
            other => {
                return Err(NzError::Protocol(format!(
                    "unsupported authentication request {other}"
                )))
            }
        }

        loop {
            let msg = self.read_byte().await?;
            if msg == 0 {
                continue;
            }
            if msg != b'R' && msg != b'E' {
                let _ = self.read_bytes(4).await?;
            }
            match msg {
                b'K' => {
                    let _ = self.read_bytes(4).await?;
                    self.backend_process_id = self.read_i32().await?;
                    self.backend_secret_key = self.read_i32().await?;
                }
                b'Z' => return Ok(()),
                b'N' => {
                    let len = self.read_i32().await?;
                    let len =
                        validate_protocol_length(len, "connectionCompleteNotice", true)? as usize;
                    let _ = self.read_bytes(len).await?;
                }
                b'R' => {
                    let _ = self.read_i32().await?;
                }
                b'E' => return Err(self.read_backend_error("connectionCompleteError").await),
                other => {
                    return Err(NzError::Protocol(format!(
                        "connection complete: unexpected byte 0x{other:02x}"
                    )))
                }
            }
        }
    }

    async fn negotiate_version(&mut self) -> NzResult<i16> {
        let mut version = CP_VERSION_6;
        loop {
            self.write_i16_frame(HSV2_CLIENT_BEGIN, &[version]).await?;
            match self.read_byte().await? {
                b'N' => return Ok(version),
                b'M' => {
                    version = match self.read_byte().await? {
                        b'2' => CP_VERSION_2,
                        b'4' => CP_VERSION_4,
                        b'5' => CP_VERSION_5,
                        b'3' => 3,
                        other => {
                            return Err(NzError::Protocol(format!(
                                "unknown handshake version byte {other:?}"
                            )))
                        }
                    };
                }
                b'E' => return Err(self.read_backend_error("handshakeNegotiationError").await),
                other => {
                    return Err(NzError::Protocol(format!(
                        "handshake negotiation: unexpected byte 0x{other:02x}"
                    )))
                }
            }
        }
    }

    async fn expect_ack(&mut self, stage: &str) -> NzResult<()> {
        match self.read_byte().await? {
            b'N' => Ok(()),
            b'E' => Err(self.read_backend_error(stage).await),
            other => Err(NzError::Protocol(format!(
                "handshake {stage}: unexpected byte 0x{other:02x}"
            ))),
        }
    }

    async fn query(&mut self, sql: &str) -> NzResult<QueryResult> {
        let timeout = (self.config.command_timeout > 0)
            .then(|| Duration::from_secs(self.config.command_timeout));
        let future = self.query_inner(sql);
        if let Some(timeout) = timeout {
            tokio::time::timeout(timeout, future)
                .await
                .map_err(|_| NzError::Timeout("command timeout".into()))?
        } else {
            future.await
        }
    }

    async fn query_inner(&mut self, sql: &str) -> NzResult<QueryResult> {
        self.command_number = (self.command_number % 100_000) + 1;
        let mut packet = Vec::with_capacity(sql.len() + 6);
        packet.push(b'P');
        packet.extend_from_slice(&self.command_number.to_be_bytes());
        packet.extend_from_slice(sql.as_bytes());
        packet.push(0);
        self.stream_mut()?
            .write_all(&packet)
            .await
            .map_err(NzError::Io)?;
        self.drain_response().await
    }

    async fn stream_query(
        &mut self,
        sql: &str,
        rows: &mpsc::Sender<NzResult<Row>>,
    ) -> NzResult<()> {
        let timeout = (self.config.command_timeout > 0)
            .then(|| Duration::from_secs(self.config.command_timeout));
        let future = self.stream_query_inner(sql, rows);
        if let Some(timeout) = timeout {
            tokio::time::timeout(timeout, future)
                .await
                .map_err(|_| NzError::Timeout("command timeout".into()))?
        } else {
            future.await
        }
    }

    async fn stream_query_inner(
        &mut self,
        sql: &str,
        rows: &mpsc::Sender<NzResult<Row>>,
    ) -> NzResult<()> {
        self.command_number = (self.command_number % 100_000) + 1;
        let mut packet = Vec::with_capacity(sql.len() + 6);
        packet.push(b'P');
        packet.extend_from_slice(&self.command_number.to_be_bytes());
        packet.extend_from_slice(sql.as_bytes());
        packet.push(0);
        self.stream_mut()?
            .write_all(&packet)
            .await
            .map_err(NzError::Io)?;
        self.drain_response_stream(rows).await
    }

    async fn drain_response(&mut self) -> NzResult<QueryResult> {
        let mut result_sets = Vec::new();
        let mut current: Option<AsyncResultSet> = None;
        let mut cached_columns: Option<Arc<[ColumnDesc]>> = None;
        let mut tupdesc: Option<Arc<DbosTupleDesc>> = None;
        let mut rows_affected = -1i64;
        let mut notices = Vec::new();
        let mut error: Option<NzError> = None;

        loop {
            let msg_type = self.read_message_type().await?;
            match msg_type {
                code::ROW_STANDARD => {
                    let _ = self.read_bytes(4).await?;
                    let _reserved = self.read_i32().await?;
                    let row_len = validate_protocol_length(
                        self.read_i32().await?,
                        "rowStandardPayload",
                        false,
                    )? as usize;
                    let descriptor = tupdesc.as_ref().ok_or_else(|| {
                        NzError::Protocol("DBOS row received before descriptor".into())
                    })?;
                    let payload = self.read_bytes(row_len).await?;
                    let columns = cached_columns
                        .clone()
                        .unwrap_or_else(|| Arc::from(descriptor.to_column_descs()));
                    let row = Row::from_dbos_raw(columns.clone(), payload, descriptor.clone())?;
                    let set = current.get_or_insert_with(|| (columns, Vec::new(), None));
                    set.1.push(row);
                    continue;
                }
                b'u' => {
                    self.handle_export_start().await?;
                    continue;
                }
                b'U' => {
                    self.handle_export_data().await?;
                    continue;
                }
                b'l' => {
                    self.handle_import().await?;
                    continue;
                }
                b'x' => {
                    let _ = self.read_bytes(4).await?;
                    continue;
                }
                b'e' => {
                    self.handle_ext_log().await?;
                    continue;
                }
                _ => {}
            }

            let _shared = self.read_bytes(4).await?;
            match msg_type {
                code::ROW_DESCRIPTION => {
                    let len = validate_protocol_length(
                        self.read_i32().await?,
                        "rowDescriptionPayload",
                        false,
                    )? as usize;
                    let data = self.read_bytes(len).await?;
                    finish_async_result_set(&mut result_sets, &mut current);
                    let columns: Arc<[ColumnDesc]> = Arc::from(parse_row_description(&data)?);
                    cached_columns = Some(columns.clone());
                    current = Some((columns, Vec::new(), None));
                }
                code::DATA_ROW => {
                    let len =
                        validate_protocol_length(self.read_i32().await?, "dataRowPayload", false)?
                            as usize;
                    let data = self.read_bytes(len).await?;
                    let columns = current
                        .as_ref()
                        .map(|set| set.0.clone())
                        .or_else(|| cached_columns.clone())
                        .ok_or_else(|| {
                            NzError::Protocol("DataRow received before RowDescription".into())
                        })?;
                    let row = Row::from_text_raw(columns.clone(), data)?;
                    let set = current.get_or_insert_with(|| (columns, Vec::new(), None));
                    set.1.push(row);
                }
                code::ROW_DESCRIPTION_STANDARD => {
                    let len = validate_protocol_length(
                        self.read_i32().await?,
                        "rowDescriptionStandardPayload",
                        false,
                    )? as usize;
                    let data = self.read_bytes(len).await?;
                    let descriptor =
                        Arc::new(DbosTupleDesc::parse(&data, cached_columns.as_deref())?);
                    let columns = cached_columns
                        .clone()
                        .unwrap_or_else(|| Arc::from(descriptor.to_column_descs()));
                    tupdesc = Some(descriptor);
                    let set = current.get_or_insert_with(|| (columns, Vec::new(), None));
                    set.2 = Some(
                        tupdesc
                            .as_ref()
                            .expect("descriptor stored above")
                            .field_null_allowed
                            .clone(),
                    );
                }
                code::COMMAND_COMPLETE => {
                    let len = validate_protocol_length(
                        self.read_i32().await?,
                        "commandCompletePayload",
                        false,
                    )? as usize;
                    let data = self.read_bytes(len).await?;
                    let text = String::from_utf8_lossy(&data);
                    let count = parse_command_complete_rows(&text);
                    if count >= 0 {
                        rows_affected = if rows_affected < 0 {
                            count
                        } else {
                            rows_affected + count
                        };
                    }
                    finish_async_result_set(&mut result_sets, &mut current);
                }
                code::NOTICE_RESPONSE => {
                    let len =
                        validate_protocol_length(self.read_i32().await?, "noticePayload", true)?
                            as usize;
                    let data = self.read_bytes(len).await?;
                    let message = String::from_utf8_lossy(&data)
                        .replace('\0', "")
                        .trim()
                        .to_owned();
                    if !message.is_empty() {
                        notices.push(message);
                    }
                }
                code::ERROR_RESPONSE => {
                    let len =
                        validate_protocol_length(self.read_i32().await?, "errorPayload", false)?
                            as usize;
                    let data = self.read_bytes(len).await?;
                    // ERROR_RESPONSE is followed by ReadyForQuery.  Keep
                    // reading so a normal SQL error cannot poison the next
                    // request on this session.
                    if error.is_none() {
                        error = Some(NzError::Database(Box::new(parse_backend_error_fields(
                            &data,
                        ))));
                    }
                }
                code::READY_FOR_QUERY | code::READY_FOR_QUERY_ALT => {
                    finish_async_result_set(&mut result_sets, &mut current);
                    if let Some(error) = error {
                        return Err(error);
                    }
                    return Ok(QueryResult {
                        result_sets,
                        rows_affected,
                        notices,
                    });
                }
                code::CONTROL_ZERO | code::CONTROL_A => {}
                code::EMPTY_QUERY_RESPONSE | code::BACKEND_PAYLOAD_P => {
                    let len =
                        validate_protocol_length(self.read_i32().await?, "ignoredPayload", true)?
                            as usize;
                    let _ = self.read_bytes(len).await?;
                }
                _ => {
                    let len =
                        validate_protocol_length(self.read_i32().await?, "unknownPayload", true)?
                            as usize;
                    if len > 0 {
                        let _ = self.read_bytes(len).await?;
                    }
                }
            }
        }
    }

    async fn drain_response_stream(&mut self, rows: &mpsc::Sender<NzResult<Row>>) -> NzResult<()> {
        let mut cached_columns: Option<Arc<[ColumnDesc]>> = None;
        let mut descriptor: Option<Arc<DbosTupleDesc>> = None;
        let mut current_columns: Option<Arc<[ColumnDesc]>> = None;
        let mut result_set_index = 0usize;
        let mut result_set_started = false;
        let mut stream_open = true;
        let mut multiple_result_error_sent = false;
        let mut error: Option<NzError> = None;

        loop {
            let msg_type = self.read_message_type().await?;
            match msg_type {
                code::ROW_STANDARD => {
                    let _ = self.read_bytes(4).await?;
                    let _reserved = self.read_i32().await?;
                    let row_len = validate_protocol_length(
                        self.read_i32().await?,
                        "rowStandardPayload",
                        false,
                    )? as usize;
                    let payload = self.read_bytes(row_len).await?;
                    let descriptor = descriptor.as_ref().ok_or_else(|| {
                        NzError::Protocol("DBOS row received before descriptor".into())
                    })?;
                    let columns = current_columns
                        .clone()
                        .or_else(|| cached_columns.clone())
                        .unwrap_or_else(|| Arc::from(descriptor.to_column_descs()));
                    let row = Row::from_dbos_raw(columns, payload, descriptor.clone())?;
                    if result_set_index == 0 && stream_open {
                        if rows.send(Ok(row)).await.is_err() {
                            stream_open = false;
                        }
                    } else if result_set_index > 0 && stream_open && !multiple_result_error_sent {
                        multiple_result_error_sent = true;
                        if rows
                            .send(Err(NzError::Config(
                                "query_stream only exposes the first result set".into(),
                            )))
                            .await
                            .is_err()
                        {
                            stream_open = false;
                        }
                    }
                    continue;
                }
                b'u' => {
                    self.handle_export_start().await?;
                    continue;
                }
                b'U' => {
                    self.handle_export_data().await?;
                    continue;
                }
                b'l' => {
                    self.handle_import().await?;
                    continue;
                }
                b'x' => {
                    let _ = self.read_bytes(4).await?;
                    continue;
                }
                b'e' => {
                    self.handle_ext_log().await?;
                    continue;
                }
                _ => {}
            }

            let _shared = self.read_bytes(4).await?;
            match msg_type {
                code::ROW_DESCRIPTION => {
                    let len = validate_protocol_length(
                        self.read_i32().await?,
                        "rowDescriptionPayload",
                        false,
                    )? as usize;
                    let data = self.read_bytes(len).await?;
                    if result_set_index > 0 && stream_open && !multiple_result_error_sent {
                        multiple_result_error_sent = true;
                        if rows
                            .send(Err(NzError::Config(
                                "query_stream only exposes the first result set".into(),
                            )))
                            .await
                            .is_err()
                        {
                            stream_open = false;
                        }
                    }
                    current_columns = Some(Arc::from(parse_row_description(&data)?));
                    cached_columns = current_columns.clone();
                    descriptor = None;
                    result_set_started = true;
                }
                code::DATA_ROW => {
                    let len =
                        validate_protocol_length(self.read_i32().await?, "dataRowPayload", false)?
                            as usize;
                    let data = self.read_bytes(len).await?;
                    let columns = current_columns.clone().ok_or_else(|| {
                        NzError::Protocol("DataRow received before RowDescription".into())
                    })?;
                    let row = Row::from_text_raw(columns, data)?;
                    if result_set_index == 0 && stream_open {
                        if rows.send(Ok(row)).await.is_err() {
                            stream_open = false;
                        }
                    } else if result_set_index > 0 && stream_open && !multiple_result_error_sent {
                        multiple_result_error_sent = true;
                        if rows
                            .send(Err(NzError::Config(
                                "query_stream only exposes the first result set".into(),
                            )))
                            .await
                            .is_err()
                        {
                            stream_open = false;
                        }
                    }
                }
                code::ROW_DESCRIPTION_STANDARD => {
                    let len = validate_protocol_length(
                        self.read_i32().await?,
                        "rowDescriptionStandardPayload",
                        false,
                    )? as usize;
                    let data = self.read_bytes(len).await?;
                    if result_set_index > 0 && stream_open && !multiple_result_error_sent {
                        multiple_result_error_sent = true;
                        if rows
                            .send(Err(NzError::Config(
                                "query_stream only exposes the first result set".into(),
                            )))
                            .await
                            .is_err()
                        {
                            stream_open = false;
                        }
                    }
                    let parsed = Arc::new(DbosTupleDesc::parse(&data, cached_columns.as_deref())?);
                    descriptor = Some(parsed);
                    if current_columns.is_none() {
                        current_columns = cached_columns.clone().or_else(|| {
                            descriptor.as_ref().map(|d| Arc::from(d.to_column_descs()))
                        });
                    }
                    result_set_started = true;
                }
                code::COMMAND_COMPLETE => {
                    let len = validate_protocol_length(
                        self.read_i32().await?,
                        "commandCompletePayload",
                        false,
                    )? as usize;
                    let _ = self.read_bytes(len).await?;
                    if result_set_started {
                        result_set_index += 1;
                        result_set_started = false;
                        current_columns = None;
                        descriptor = None;
                    }
                }
                code::NOTICE_RESPONSE => {
                    let len =
                        validate_protocol_length(self.read_i32().await?, "noticePayload", true)?
                            as usize;
                    let _ = self.read_bytes(len).await?;
                }
                code::ERROR_RESPONSE => {
                    let len =
                        validate_protocol_length(self.read_i32().await?, "errorPayload", false)?
                            as usize;
                    let data = self.read_bytes(len).await?;
                    if error.is_none() {
                        error = Some(NzError::Database(Box::new(parse_backend_error_fields(
                            &data,
                        ))));
                    }
                }
                code::READY_FOR_QUERY | code::READY_FOR_QUERY_ALT => {
                    if let Some(error) = error {
                        return Err(error);
                    }
                    return Ok(());
                }
                code::CONTROL_ZERO | code::CONTROL_A => {}
                code::EMPTY_QUERY_RESPONSE | code::BACKEND_PAYLOAD_P => {
                    let len =
                        validate_protocol_length(self.read_i32().await?, "ignoredPayload", true)?
                            as usize;
                    let _ = self.read_bytes(len).await?;
                }
                _ => {
                    let len =
                        validate_protocol_length(self.read_i32().await?, "unknownPayload", true)?
                            as usize;
                    if len > 0 {
                        let _ = self.read_bytes(len).await?;
                    }
                }
            }
        }
    }

    async fn handle_export_start(&mut self) -> NzResult<()> {
        self.skip_bytes(4).await?;
        self.skip_bytes(10).await?;
        self.skip_bytes(16).await?;
        let length =
            validate_protocol_length(self.read_i32().await?, "externalTableExportFilename", true)?
                as usize;
        if length == 0 {
            return Err(NzError::Protocol(
                "Invalid external-table export filename length".into(),
            ));
        }
        let name = self.read_bytes(length).await?;
        let filename = String::from_utf8_lossy(&name).replace('\0', "");
        match File::create(&filename).await {
            Ok(file) => {
                self.export_file = Some(file);
                self.write_raw(&[0, 0, 0, 0]).await
            }
            Err(_) => self.write_raw(&1i32.to_be_bytes()).await,
        }
    }

    async fn handle_export_data(&mut self) -> NzResult<()> {
        self.skip_bytes(4).await?;
        self.skip_bytes(4).await?;
        loop {
            match self.read_i32().await? {
                1 => {
                    let length = validate_protocol_length(
                        self.read_i32().await?,
                        "externalTableExportDataChunk",
                        true,
                    )? as usize;
                    let chunk = self.read_bytes(length).await?;
                    if let Some(file) = self.export_file.as_mut() {
                        file.write_all(&chunk).await.map_err(NzError::Io)?;
                    }
                }
                3 => {
                    if let Some(mut file) = self.export_file.take() {
                        file.flush().await.map_err(NzError::Io)?;
                    }
                    return Ok(());
                }
                2 => {
                    let length =
                        u16::from_be_bytes(self.read_bytes(2).await?.try_into().map_err(|_| {
                            NzError::Protocol("truncated external-table error length".into())
                        })?) as usize;
                    if length > 0 {
                        let _ = self.read_bytes(length).await?;
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

    async fn handle_import(&mut self) -> NzResult<()> {
        self.skip_bytes(8).await?;
        let mut name = Vec::new();
        loop {
            let byte = self.read_byte().await?;
            if byte == 0 {
                break;
            }
            name.push(byte);
            if name.len() > 4096 {
                return Err(NzError::Protocol(
                    "Invalid external-table import filename".into(),
                ));
            }
        }
        let filename = String::from_utf8_lossy(&name).into_owned();
        let _host_version = self.read_i32().await?;
        self.write_raw(&1i32.to_be_bytes()).await?;
        let _format = self.read_i32().await?;
        let buffer_size = validate_protocol_length(
            self.read_i32().await?,
            "externalTableImportBufferSize",
            true,
        )? as usize;
        let data = if let Some(data) = crate::connection::take_import_data(&filename) {
            data
        } else {
            match tokio::fs::read(&filename).await {
                Ok(data) => data,
                Err(_) => {
                    self.write_raw(&2i32.to_be_bytes()).await?;
                    return Ok(());
                }
            }
        };
        let chunk_size = buffer_size.max(1);
        let mut offset = 0;
        while offset < data.len() {
            let end = (offset + chunk_size).min(data.len());
            let chunk = &data[offset..end];
            let mut frame = Vec::with_capacity(8 + chunk.len());
            frame.extend_from_slice(&1i32.to_be_bytes());
            frame.extend_from_slice(&(chunk.len() as i32).to_be_bytes());
            frame.extend_from_slice(chunk);
            self.write_raw(&frame).await?;
            offset = end;
        }
        self.write_raw(&3i32.to_be_bytes()).await
    }

    async fn handle_ext_log(&mut self) -> NzResult<()> {
        self.skip_bytes(4).await?;
        let length = validate_protocol_length(
            self.read_i32().await?,
            "fileTransfer.logDirectoryLength",
            true,
        )? as usize;
        if length == 0 {
            return Err(NzError::Protocol(
                "Invalid external-table log directory length".into(),
            ));
        }
        let directory = self.read_bytes(length.saturating_sub(1)).await?;
        let _ = self.read_bytes(1).await?;
        let mut name = Vec::new();
        loop {
            let byte = self.read_byte().await?;
            if byte == 0 {
                break;
            }
            name.push(byte);
            if name.len() > 4096 {
                break;
            }
        }
        let log_type = self.read_i32().await?;
        let extension = match log_type {
            1 => ".nzlog",
            2 => ".nzbad",
            3 => ".nzstats",
            _ => ".log",
        };
        let path = std::path::Path::new(String::from_utf8_lossy(&directory).trim_end_matches('\0'))
            .join(format!("{}{extension}", String::from_utf8_lossy(&name)));
        let mut file = File::create(path).await.ok();
        loop {
            let length =
                validate_protocol_length(self.read_i32().await?, "externalTableLogChunk", true)?
                    as usize;
            if length == 0 {
                break;
            }
            let chunk = self.read_bytes(length).await?;
            if let Some(file) = file.as_mut() {
                file.write_all(&chunk).await.map_err(NzError::Io)?;
            }
        }
        if let Some(mut file) = file {
            file.flush().await.map_err(NzError::Io)?;
        }
        Ok(())
    }

    async fn skip_bytes(&mut self, length: usize) -> NzResult<()> {
        let _ = self.read_bytes(length).await?;
        Ok(())
    }

    async fn write_raw(&mut self, data: &[u8]) -> NzResult<()> {
        self.stream_mut()?
            .write_all(data)
            .await
            .map_err(NzError::Io)?;
        self.stream_mut()?.flush().await.map_err(NzError::Io)
    }

    async fn read_backend_error(&mut self, stage: &str) -> NzError {
        let length = match self.read_i32().await {
            Ok(value) => value,
            Err(error) => return error,
        };
        let length = match validate_protocol_length(length, stage, false) {
            Ok(value) if value >= 4 => value as usize - 4,
            Ok(_) => return NzError::Protocol(format!("invalid {stage} frame length")),
            Err(error) => return error,
        };
        match self.read_bytes(length).await {
            Ok(data) => NzError::Database(Box::new(parse_backend_error_fields(&data))),
            Err(error) => error,
        }
    }

    #[cfg(feature = "ssl")]
    async fn upgrade_tls(&mut self) -> NzResult<()> {
        let transport = self
            .stream
            .take()
            .ok_or_else(|| NzError::Closed("async connection is closed".into()))?;
        let AsyncTransport::Plain(stream) = transport else {
            self.stream = Some(transport);
            return Ok(());
        };

        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let Some(path) = &self.config.ssl_cert_path {
            let pem = std::fs::File::open(path).map_err(NzError::Io)?;
            let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(pem))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| NzError::Config(format!("invalid TLS CA certificate: {error}")))?;
            for cert in certs {
                roots.add(cert).map_err(|error| {
                    NzError::Config(format!("invalid TLS CA certificate: {error}"))
                })?;
            }
        }
        let builder = rustls::ClientConfig::builder();
        let client_config = if self.config.reject_unauthorized {
            builder.with_root_certificates(roots).with_no_client_auth()
        } else {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AsyncNoCertificateVerification))
                .with_no_client_auth()
        };
        let host = self
            .config
            .host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(&self.config.host);
        let name = rustls::pki_types::ServerName::try_from(host.to_string()).map_err(|_| {
            NzError::Config(format!("invalid TLS server name: {}", self.config.host))
        })?;
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
        let tls = connector
            .connect(name, stream)
            .await
            .map_err(|error| NzError::Config(format!("TLS handshake failed: {error}")))?;
        self.stream = Some(AsyncTransport::Tls(Box::new(tls)));
        Ok(())
    }

    #[cfg(not(feature = "ssl"))]
    async fn upgrade_tls(&mut self) -> NzResult<()> {
        Err(NzError::Unsupported(
            "TLS requested but the `ssl` feature is disabled".into(),
        ))
    }

    async fn write_frame(&mut self, opcode: i16, payload: &[u8]) -> NzResult<()> {
        let len = (6 + payload.len()) as i32;
        self.stream_mut()?
            .write_all(&len.to_be_bytes())
            .await
            .map_err(NzError::Io)?;
        self.stream_mut()?
            .write_all(&opcode.to_be_bytes())
            .await
            .map_err(NzError::Io)?;
        self.stream_mut()?
            .write_all(payload)
            .await
            .map_err(NzError::Io)
    }

    async fn write_cstring_frame(&mut self, opcode: i16, value: &str) -> NzResult<()> {
        let mut payload = value.as_bytes().to_vec();
        payload.push(0);
        self.write_frame(opcode, &payload).await
    }

    async fn write_i16_frame(&mut self, opcode: i16, values: &[i16]) -> NzResult<()> {
        let mut payload = Vec::with_capacity(values.len() * 2);
        for value in values {
            payload.extend_from_slice(&value.to_be_bytes());
        }
        self.write_frame(opcode, &payload).await
    }

    async fn write_i32_frame(&mut self, opcode: i16, value: i32) -> NzResult<()> {
        self.write_frame(opcode, &value.to_be_bytes()).await
    }

    async fn write_auth_response(&mut self, payload: &[u8]) -> NzResult<()> {
        let len = (4 + payload.len()) as i32;
        self.stream_mut()?
            .write_all(&len.to_be_bytes())
            .await
            .map_err(NzError::Io)?;
        self.stream_mut()?
            .write_all(payload)
            .await
            .map_err(NzError::Io)
    }

    async fn fill(&mut self, needed: usize) -> NzResult<()> {
        if needed > MAX_BUFFERED_FRAME {
            return Err(NzError::Protocol("async frame is too large".into()));
        }
        while self.buffer.len() < needed {
            let stream = self
                .stream
                .as_mut()
                .ok_or_else(|| NzError::Closed("async connection is closed".into()))?;
            let read = stream
                .read_buf(&mut self.buffer)
                .await
                .map_err(NzError::Io)?;
            if read == 0 {
                return Err(NzError::Closed("socket closed during async read".into()));
            }
        }
        Ok(())
    }

    async fn read_byte(&mut self) -> NzResult<u8> {
        self.fill(1).await?;
        Ok(self.buffer.get_u8())
    }

    async fn read_message_type(&mut self) -> NzResult<u8> {
        loop {
            let message_type = self.read_byte().await?;
            if message_type != 0 {
                return Ok(message_type);
            }
        }
    }

    async fn read_i32(&mut self) -> NzResult<i32> {
        self.fill(4).await?;
        Ok(self.buffer.get_i32())
    }

    async fn read_bytes(&mut self, len: usize) -> NzResult<Vec<u8>> {
        validate_protocol_length(len as i32, "asyncPayload", true)?;
        self.fill(len).await?;
        Ok(self.buffer.split_to(len).to_vec())
    }
}

#[cfg(feature = "ssl")]
#[derive(Debug)]
struct AsyncNoCertificateVerification;

#[cfg(feature = "ssl")]
impl rustls::client::danger::ServerCertVerifier for AsyncNoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn finish_async_result_set(
    result_sets: &mut Vec<crate::connection::ResultSet>,
    current: &mut Option<AsyncResultSet>,
) {
    if let Some((columns, rows, nullability)) = current.take() {
        result_sets.push(crate::connection::ResultSet {
            columns: columns.to_vec(),
            rows,
            nullability,
        });
    }
}

fn format_host_port(config: &NzConnectionConfig) -> String {
    if config.host.contains(':') && !config.host.starts_with('[') {
        format!("[{}]:{}", config.host, config.port)
    } else {
        format!("{}:{}", config.host, config.port)
    }
}

fn base64_encode(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            TABLE[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}
