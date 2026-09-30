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
use bytes::{Buf, Bytes, BytesMut};
use futures_core::Stream;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Notify, OwnedSemaphorePermit, Semaphore};

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

#[derive(Default)]
struct Control {
    active: AtomicU64,
    next: AtomicU64,
    cancelled: AtomicU64,
    closing: AtomicBool,
    closed: AtomicBool,
    transaction: AtomicBool,
    changed: Notify,
    finished: Notify,
}

impl Control {
    async fn interrupted(&self, generation: u64) {
        loop {
            let changed = self.changed.notified();
            if self.closing.load(Ordering::Acquire)
                || self.cancelled.load(Ordering::Acquire) == generation
            {
                return;
            }
            changed.await;
        }
    }
    fn cancel(&self) {
        let generation = self.active.load(Ordering::Acquire);
        if generation != 0 {
            self.cancelled.fetch_max(generation, Ordering::AcqRel);
            self.changed.notify_waiters();
        }
    }
}

#[derive(Default)]
struct NoticeHistory {
    items: VecDeque<String>,
    bytes: usize,
    dropped: u64,
}
impl NoticeHistory {
    fn push(&mut self, message: &str) {
        const LIMIT: usize = 1024 * 1024;
        if message.len() > LIMIT {
            self.dropped += 1;
            return;
        }
        while self.items.len() >= 1000 || self.bytes + message.len() > LIMIT {
            let Some(previous) = self.items.pop_front() else {
                break;
            };
            self.bytes -= previous.len();
            self.dropped += 1;
        }
        self.bytes += message.len();
        self.items.push_back(message.to_owned());
    }
}

struct QueuedEvent {
    value: NzResult<QueryStreamEvent>,
    _permit: OwnedSemaphorePermit,
}
struct QueuedBatch {
    rows: Vec<Row>,
    _permit: OwnedSemaphorePermit,
}
const STREAM_BYTES: usize = 8 * 1024 * 1024;
const BATCH_ROWS: usize = 256;
const BATCH_BYTES: usize = 1024 * 1024;
struct EventSender {
    sender: mpsc::Sender<QueuedEvent>,
    budget: Arc<Semaphore>,
    control: Arc<Control>,
}

struct BatchSender {
    sender: mpsc::Sender<QueuedBatch>,
    budget: Arc<Semaphore>,
    control: Arc<Control>,
}
impl BatchSender {
    async fn send(&self, rows: Vec<Row>) -> Result<(), ()> {
        if rows.is_empty() {
            return Ok(());
        }
        let generation = self.control.active.load(Ordering::Acquire);
        let bytes = rows
            .iter()
            .map(Row::retained_bytes)
            .fold(0usize, usize::saturating_add)
            .saturating_add(rows.len() * std::mem::size_of::<Row>())
            .clamp(1, STREAM_BYTES) as u32;
        tokio::select! {
            biased;
            _ = self.control.interrupted(generation) => Err(()),
            result = async {
                let permit = self.budget.clone().acquire_many_owned(bytes).await.map_err(|_| ())?;
                self.sender.send(QueuedBatch { rows, _permit: permit }).await.map_err(|_| ())
            } => result,
        }
    }
}
impl EventSender {
    async fn send(&self, event: NzResult<QueryStreamEvent>) -> Result<(), ()> {
        let generation = self.control.active.load(Ordering::Acquire);
        tokio::select! {
            biased;
            _ = self.control.interrupted(generation) => Err(()),
            result = async {
                let size = match &event {
                    Ok(QueryStreamEvent::Row(row)) => row.retained_bytes(),
                    Ok(QueryStreamEvent::Notice(text)) => text.len() + 64,
                    Ok(QueryStreamEvent::ResultSetStart { columns, .. }) => columns.iter().map(|c| c.name.len() + std::mem::size_of::<ColumnDesc>()).sum::<usize>(),
                    _ => 64,
                }.clamp(1, STREAM_BYTES) as u32;
                let permit = self.budget.clone().acquire_many_owned(size).await.map_err(|_| ())?;
                self.sender.send(QueuedEvent { value: event, _permit: permit }).await.map_err(|_| ())
            } => result,
        }
    }
}

enum Request {
    Query {
        sql: String,
        discard: bool,
        options: Option<QueryOptions>,
        import_source: Option<(String, crate::connection::ImportSource)>,
        _lease: Option<tokio::sync::OwnedMutexGuard<()>>,
        response: Response<QueryResult>,
    },
    Stream {
        sql: String,
        options: Option<QueryOptions>,
        _lease: Option<tokio::sync::OwnedMutexGuard<()>>,
        events: Option<EventSender>,
        batch: Option<BatchSender>,
        notices: Arc<Mutex<NoticeHistory>>,
        terminal: Response<()>,
    },
}

/// Bounded row stream for one query. The connection task keeps draining the
/// wire while `poll_next` applies backpressure through the bounded channel.
pub struct RowStream {
    receiver: mpsc::Receiver<QueuedEvent>,
    notices: Arc<Mutex<NoticeHistory>>,
    terminal: Option<oneshot::Receiver<NzResult<()>>>,
    first_set_only_error: bool,
}

/// A row or a server notice observed while a native Tokio query is running.
#[derive(Debug)]
pub enum QueryStreamEvent {
    Row(Row),
    Notice(String),
    ResultSetStart {
        index: usize,
        columns: Arc<[ColumnDesc]>,
        nullability: Option<Vec<bool>>,
    },
    ResultSetEnd {
        index: usize,
        row_count: u64,
    },
    CommandComplete {
        tag: String,
        rows_affected: i64,
    },
}

/// Bounded stream of rows and notices in wire order.
pub struct QueryEventStream {
    receiver: mpsc::Receiver<QueuedEvent>,
    notices: Arc<Mutex<NoticeHistory>>,
    terminal: Option<oneshot::Receiver<NzResult<()>>>,
}

impl QueryEventStream {
    /// Snapshot of all notices received so far.
    /// Number of notices evicted from the capped history (events remain ordered).
    pub fn dropped_notices(&self) -> u64 {
        self.notices
            .lock()
            .map(|history| history.dropped)
            .unwrap_or_default()
    }

    pub fn notices(&self) -> Vec<String> {
        self.notices
            .lock()
            .map(|items| items.items.iter().cloned().collect())
            .unwrap_or_default()
    }
}

impl Stream for QueryEventStream {
    type Item = NzResult<QueryStreamEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match poll_event(&mut this.receiver, cx) {
            Poll::Ready(None) => poll_terminal(&mut this.terminal, cx),
            result => result,
        }
    }
}

impl RowStream {
    /// Snapshot notices received so far while this result stream is running.
    /// Number of notices evicted from the capped history (events remain ordered).
    pub fn dropped_notices(&self) -> u64 {
        self.notices
            .lock()
            .map(|history| history.dropped)
            .unwrap_or_default()
    }

    pub fn notices(&self) -> Vec<String> {
        self.notices
            .lock()
            .map(|notices| notices.items.iter().cloned().collect())
            .unwrap_or_default()
    }
}

impl Stream for RowStream {
    type Item = NzResult<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let receiver = &mut this.receiver;
        loop {
            match poll_event(receiver, cx) {
                Poll::Ready(Some(Ok(QueryStreamEvent::ResultSetStart { index, .. })))
                    if index > 0 =>
                {
                    if !this.first_set_only_error {
                        this.first_set_only_error = true;
                        receiver.close();
                        return Poll::Ready(Some(Err(NzError::Config("query_stream only exposes the first result set; use query_stream_events".into()))));
                    }
                }
                Poll::Ready(Some(Ok(
                    QueryStreamEvent::Notice(_)
                    | QueryStreamEvent::ResultSetStart { .. }
                    | QueryStreamEvent::ResultSetEnd { .. }
                    | QueryStreamEvent::CommandComplete { .. },
                ))) => continue,
                Poll::Ready(Some(Ok(QueryStreamEvent::Row(row)))) => {
                    if !this.first_set_only_error {
                        return Poll::Ready(Some(Ok(row)));
                    }
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(None) => {
                    return match poll_terminal(&mut this.terminal, cx) {
                        Poll::Ready(Some(Err(NzError::Cancelled(_))))
                            if this.first_set_only_error =>
                        {
                            Poll::Ready(None)
                        }
                        Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
                        Poll::Ready(_) => Poll::Ready(None),
                        Poll::Pending => Poll::Pending,
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn poll_event(
    receiver: &mut mpsc::Receiver<QueuedEvent>,
    cx: &mut Context<'_>,
) -> Poll<Option<NzResult<QueryStreamEvent>>> {
    receiver
        .poll_recv(cx)
        .map(|event| event.map(|event| event.value))
}

/// Batches from the bounded native row stream. A single large row is returned alone.
pub struct RowBatchStream {
    receiver: mpsc::Receiver<QueuedBatch>,
    notices: Arc<Mutex<NoticeHistory>>,
    terminal: Option<oneshot::Receiver<NzResult<()>>>,
}
impl RowBatchStream {
    /// Snapshot of notices received while this batch stream is running.
    pub fn notices(&self) -> Vec<String> {
        self.notices
            .lock()
            .map(|history| history.items.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Number of notices evicted from the capped history.
    pub fn dropped_notices(&self) -> u64 {
        self.notices
            .lock()
            .map(|history| history.dropped)
            .unwrap_or_default()
    }
}
impl Stream for RowBatchStream {
    type Item = NzResult<Vec<Row>>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.receiver).poll_recv(cx) {
            Poll::Ready(Some(batch)) => Poll::Ready(Some(Ok(batch.rows))),
            Poll::Ready(None) => poll_batch_terminal(&mut this.terminal, cx),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn poll_batch_terminal(
    terminal: &mut Option<oneshot::Receiver<NzResult<()>>>,
    cx: &mut Context<'_>,
) -> Poll<Option<NzResult<Vec<Row>>>> {
    let Some(receiver) = terminal.as_mut() else {
        return Poll::Ready(None);
    };
    match Pin::new(receiver).poll(cx) {
        Poll::Pending => Poll::Pending,
        Poll::Ready(result) => {
            *terminal = None;
            match result {
                Ok(Ok(())) => Poll::Ready(None),
                Ok(Err(error)) => Poll::Ready(Some(Err(error))),
                Err(_) => Poll::Ready(Some(Err(NzError::Closed("connection task stopped".into())))),
            }
        }
    }
}

fn poll_terminal(
    terminal: &mut Option<oneshot::Receiver<NzResult<()>>>,
    cx: &mut Context<'_>,
) -> Poll<Option<NzResult<QueryStreamEvent>>> {
    let Some(receiver) = terminal.as_mut() else {
        return Poll::Ready(None);
    };
    match Pin::new(receiver).poll(cx) {
        Poll::Pending => Poll::Pending,
        Poll::Ready(result) => {
            *terminal = None;
            match result {
                Ok(Ok(())) => Poll::Ready(None),
                Ok(Err(error)) => Poll::Ready(Some(Err(error))),
                Err(_) => Poll::Ready(Some(Err(NzError::Closed("connection task stopped".into())))),
            }
        }
    }
}

/// Options applied when a statement starts executing, including response fetch time.
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// An absolute execution deadline; `None` disables it for this operation.
    pub timeout: Option<Duration>,
}

/// Tokio-native Netezza client.  Clones share one serialized protocol queue.
#[derive(Clone)]
pub struct Client {
    requests: mpsc::Sender<Request>,
    control: Arc<Control>,
    session: Arc<tokio::sync::Mutex<()>>,
    exclusive: bool,
}

/// Background protocol driver returned by [`connect`].
pub struct Connection {
    control: Arc<Control>,
    future: Pin<Box<dyn Future<Output = NzResult<()>> + Send>>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.control.closed.store(true, Ordering::Release);
        self.control.finished.notify_waiters();
    }
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
    let control = Arc::new(Control::default());
    let future = Box::pin(run_connection(session, receiver, control.clone()));
    Ok((
        Client {
            requests: sender,
            control: control.clone(),
            session: Arc::new(tokio::sync::Mutex::new(())),
            exclusive: false,
        },
        Connection {
            future,
            control: control.clone(),
        },
    ))
}

impl Client {
    async fn acquire_session(&self) -> NzResult<Option<tokio::sync::OwnedMutexGuard<()>>> {
        if self.exclusive {
            return Ok(None);
        }
        tokio::select! {
            guard = self.session.clone().lock_owned() => Ok(Some(guard)),
            _ = async {
                loop {
                    let finished = self.control.finished.notified();
                    if self.is_closed() { break; }
                    finished.await;
                }
            } => Err(NzError::Closed("connection is closed".into())),
        }
    }

    /// Begin a transaction holding exclusive ownership across client clones.
    /// Dropping the guard schedules rollback before the session is released.
    pub async fn transaction(&self) -> NzResult<Transaction> {
        let guard = self
            .acquire_session()
            .await?
            .ok_or_else(|| NzError::Config("nested transaction is unsupported".into()))?;
        if self.in_transaction() {
            return Err(NzError::Config("nested transaction is unsupported".into()));
        }
        let mut client = self.clone();
        client.exclusive = true;
        let transaction = Transaction {
            client,
            guard: Some(guard),
        };
        transaction.client.batch_execute("BEGIN").await?;
        Ok(transaction)
    }

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
        self.send_query(sql, false, None).await
    }

    /// Execute with an explicit deadline independent of the connection default.
    pub async fn query_with_options(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
        options: QueryOptions,
    ) -> NzResult<Vec<Row>> {
        let values: Vec<NzValue> = params.iter().map(|value| value.to_nz_value()).collect();
        let sql = substitute_parameters(sql, &values).map_err(NzError::Config)?;
        let result = self.send_query(sql, false, Some(options)).await?;
        if result.result_sets.len() > 1 {
            return Err(NzError::Config(
                "query returned multiple result sets; use query_multi".into(),
            ));
        }
        Ok(result.into_rows())
    }

    /// Execute with a reader owned by this operation, rather than global state.
    pub async fn query_with_import_reader(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
        id: &str,
        reader: impl AsyncRead + Send + 'static,
    ) -> NzResult<QueryResult> {
        if id.is_empty() || id.contains('\0') {
            return Err(NzError::Config("invalid import identifier".into()));
        }
        let values: Vec<NzValue> = params.iter().map(|value| value.to_nz_value()).collect();
        let sql = substitute_parameters(sql, &values).map_err(NzError::Config)?;
        self.send_query_with_source(
            sql,
            true,
            None,
            Some((
                id.to_owned(),
                crate::connection::ImportSource::AsyncReader(Box::pin(reader)),
            )),
        )
        .await
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
        let stream = self.query_stream_events(sql, params).await?;
        Ok(RowStream {
            receiver: stream.receiver,
            notices: stream.notices,
            terminal: stream.terminal,
            first_set_only_error: false,
        })
    }

    /// Stream rows and notices in their arrival order while the query runs.
    /// The bounded channel applies backpressure to both event kinds.
    pub async fn query_stream_events(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> NzResult<QueryEventStream> {
        self.start_stream(sql, params, None).await
    }
    /// Start an event stream with an execution deadline covering response fetches.
    pub async fn query_stream_events_with_options(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
        options: QueryOptions,
    ) -> NzResult<QueryEventStream> {
        self.start_stream(sql, params, Some(options)).await
    }
    pub async fn query_stream_with_options(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
        options: QueryOptions,
    ) -> NzResult<RowStream> {
        let stream = self.start_stream(sql, params, Some(options)).await?;
        Ok(RowStream {
            receiver: stream.receiver,
            notices: stream.notices,
            terminal: stream.terminal,
            first_set_only_error: false,
        })
    }
    async fn start_stream(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
        options: Option<QueryOptions>,
    ) -> NzResult<QueryEventStream> {
        let values: Vec<NzValue> = params.iter().map(|value| value.to_nz_value()).collect();
        let sql = substitute_parameters(sql, &values).map_err(NzError::Config)?;
        let (sender, receiver) = mpsc::channel(256);
        let notices = Arc::new(Mutex::new(NoticeHistory::default()));
        let (terminal, completion) = oneshot::channel();
        self.requests
            .send(Request::Stream {
                sql,
                options,
                _lease: self.acquire_session().await?,
                events: Some(EventSender {
                    sender,
                    control: self.control.clone(),
                    budget: Arc::new(Semaphore::new(STREAM_BYTES)),
                }),
                batch: None,
                terminal,
                notices: notices.clone(),
            })
            .await
            .map_err(|_| NzError::Closed("connection task is closed".into()))?;
        Ok(QueryEventStream {
            receiver,
            notices,
            terminal: Some(completion),
        })
    }

    /// Fetch rows in batches bounded by 256 rows and 1 MiB (except a single large row).
    pub async fn query_batches(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> NzResult<RowBatchStream> {
        let values: Vec<NzValue> = params.iter().map(|value| value.to_nz_value()).collect();
        let sql = substitute_parameters(sql, &values).map_err(NzError::Config)?;
        let (sender, receiver) = mpsc::channel(16);
        let notices = Arc::new(Mutex::new(NoticeHistory::default()));
        let (terminal, completion) = oneshot::channel();
        self.requests
            .send(Request::Stream {
                sql,
                options: None,
                _lease: self.acquire_session().await?,
                events: None,
                batch: Some(BatchSender {
                    sender,
                    control: self.control.clone(),
                    budget: Arc::new(Semaphore::new(STREAM_BYTES)),
                }),
                terminal,
                notices: notices.clone(),
            })
            .await
            .map_err(|_| NzError::Closed("connection task is closed".into()))?;
        Ok(RowBatchStream {
            receiver,
            notices,
            terminal: Some(completion),
        })
    }

    pub async fn query_one(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> NzResult<Row> {
        let rows = self.query(sql, params).await?;
        if rows.len() != 1 {
            return Err(NzError::Config(format!(
                "query_one: expected one row, got {}",
                rows.len()
            )));
        }
        Ok(rows.into_iter().next().expect("one row checked above"))
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
        let values: Vec<NzValue> = params.iter().map(|value| value.to_nz_value()).collect();
        let sql = substitute_parameters(sql, &values).map_err(NzError::Config)?;
        Ok(self.send_query(sql, true, None).await?.rows_affected)
    }

    pub async fn batch_execute(&self, sql: &str) -> NzResult<()> {
        self.send_query(sql.to_owned(), true, None)
            .await
            .map(|_| ())
    }

    /// Interrupt the active statement through an independent control path.
    pub async fn cancel(&self) -> NzResult<()> {
        if self.is_closed() {
            return Err(NzError::Closed("connection is closed".into()));
        }
        self.control.cancel();
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.requests.is_closed() || self.control.closed.load(Ordering::Acquire)
    }

    pub fn in_transaction(&self) -> bool {
        self.control.transaction.load(Ordering::Acquire)
    }

    pub async fn close(&self) -> NzResult<()> {
        self.control.closing.store(true, Ordering::Release);
        self.control.changed.notify_waiters();
        loop {
            let finished = self.control.finished.notified();
            if self.is_closed() {
                return Ok(());
            }
            finished.await;
        }
    }

    pub(crate) fn stop(&self) {
        self.control.closing.store(true, Ordering::Release);
        self.control.changed.notify_waiters();
    }

    async fn send_query(
        &self,
        sql: String,
        discard: bool,
        options: Option<QueryOptions>,
    ) -> NzResult<QueryResult> {
        self.send_query_with_source(sql, discard, options, None)
            .await
    }
    async fn send_query_with_source(
        &self,
        sql: String,
        discard: bool,
        options: Option<QueryOptions>,
        import_source: Option<(String, crate::connection::ImportSource)>,
    ) -> NzResult<QueryResult> {
        if sql.contains('\0') {
            return Err(NzError::Config("SQL contains NUL".into()));
        }
        let (sender, receiver) = oneshot::channel();
        self.requests
            .send(Request::Query {
                sql,
                discard,
                options,
                import_source,
                _lease: self.acquire_session().await?,
                response: sender,
            })
            .await
            .map_err(|_| NzError::Closed("connection task is closed".into()))?;
        receiver
            .await
            .map_err(|_| NzError::Closed("connection task is closed".into()))?
    }
}

/// Exclusive native transaction. Explicitly commit or roll back to await cleanup.
pub struct Transaction {
    client: Client,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}
impl Transaction {
    pub async fn query(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> NzResult<Vec<Row>> {
        self.client.query(sql, params).await
    }
    pub async fn query_multi(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> NzResult<QueryResult> {
        self.client.query_multi(sql, params).await
    }
    pub async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> NzResult<i64> {
        self.client.execute(sql, params).await
    }
    pub async fn batch_execute(&self, sql: &str) -> NzResult<()> {
        self.client.batch_execute(sql).await
    }
    pub async fn commit(mut self) -> NzResult<()> {
        self.client.batch_execute("COMMIT").await?;
        self.guard.take();
        Ok(())
    }
    pub async fn rollback(mut self) -> NzResult<()> {
        self.client.batch_execute("ROLLBACK").await?;
        self.guard.take();
        Ok(())
    }
}
impl Drop for Transaction {
    fn drop(&mut self) {
        let Some(guard) = self.guard.take() else {
            return;
        };
        let client = self.client.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if !matches!(
                    tokio::time::timeout(Duration::from_secs(5), client.batch_execute("ROLLBACK"))
                        .await,
                    Ok(Ok(()))
                ) {
                    client.stop();
                }
                drop(guard);
            });
        } else {
            client.stop();
        }
    }
}

struct DriverGuard(Arc<Control>);
impl Drop for DriverGuard {
    fn drop(&mut self) {
        self.0.active.store(0, Ordering::Release);
        self.0.closed.store(true, Ordering::Release);
        self.0.finished.notify_waiters();
    }
}

async fn cancel_backend(config: &NzConnectionConfig, pid: i32, key: i32) -> NzResult<()> {
    if pid == 0 || key == 0 {
        return Err(NzError::Protocol(
            "backend did not provide cancellation keys".into(),
        ));
    }
    let mut stream = TcpStream::connect(format_host_port(config)).await?;
    stream
        .write_all(&crate::cancel::build_cancel_packet(pid, key))
        .await?;
    stream.shutdown().await?;
    Ok(())
}

// Keep the response future pinned during cancellation: dropping it in the middle
// of a frame would lose the parser's current position.
async fn drive_operation<T, F, A>(
    future: F,
    abandoned: A,
    control: &Control,
    generation: u64,
    config: &NzConnectionConfig,
    options: Option<&QueryOptions>,
    backend: (i32, i32),
) -> (NzResult<T>, bool)
where
    F: Future<Output = NzResult<T>>,
    A: Future<Output = ()>,
{
    tokio::pin!(future);
    tokio::pin!(abandoned);
    let deadline = async {
        let timeout = options.map(|options| options.timeout).unwrap_or_else(|| {
            (config.command_timeout != 0).then(|| Duration::from_secs(config.command_timeout))
        });
        if let Some(timeout) = timeout {
            tokio::time::sleep(timeout).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::pin!(deadline);
    let timed_out = tokio::select! {
        result = &mut future => {
            let reusable = matches!(result, Ok(_) | Err(NzError::Database(_)));
            return (result, reusable);
        }
        _ = &mut deadline => true,
        _ = control.interrupted(generation) => false,
        _ = &mut abandoned => false,
    };
    control.cancelled.fetch_max(generation, Ordering::AcqRel);
    control.changed.notify_waiters();
    let cleanup = async {
        // A query may already have completed when cancellation is requested.
        // ReadyForQuery still proves reuse is safe if the cancel socket fails.
        let _ = cancel_backend(config, backend.0, backend.1).await;
        match future.await {
            Ok(_) | Err(NzError::Database(_)) => Ok(()),
            Err(error) => Err(error),
        }
    };
    let reusable = matches!(
        tokio::time::timeout(Duration::from_secs(5), cleanup).await,
        Ok(Ok(()))
    );
    let error = if timed_out {
        NzError::Timeout("command timeout".into())
    } else {
        NzError::Cancelled("statement cancelled".into())
    };
    (Err(error), reusable)
}

async fn wait_stream_sink_closed(events: &Option<EventSender>, batch: &Option<BatchSender>) {
    match (events, batch) {
        (Some(events), Some(batch)) => {
            tokio::select! {
                _ = events.sender.closed() => {},
                _ = batch.sender.closed() => {},
            }
        }
        (Some(events), None) => events.sender.closed().await,
        (None, Some(batch)) => batch.sender.closed().await,
        (None, None) => std::future::pending().await,
    }
}

async fn run_connection(
    mut session: AsyncSession,
    mut receiver: mpsc::Receiver<Request>,
    control: Arc<Control>,
) -> NzResult<()> {
    let _guard = DriverGuard(control.clone());
    loop {
        let request = tokio::select! {
            biased;
            _ = async { if !control.closing.load(Ordering::Acquire) { control.interrupted(u64::MAX).await; } } => break,
            request = receiver.recv() => match request { Some(request) => request, None => break },
        };
        let generation = control.next.fetch_add(1, Ordering::AcqRel) + 1;
        control.active.store(generation, Ordering::Release);
        let config = session.config.clone();
        let pid = session.backend_process_id;
        let key = session.backend_secret_key;
        let reusable = match request {
            Request::Query {
                sql,
                discard,
                options,
                import_source,
                _lease,
                mut response,
            } => {
                if response.is_closed() {
                    control.active.store(0, Ordering::Release);
                    continue;
                }
                session.import_source = import_source;
                let (result, reusable) = drive_operation(
                    session.query_inner(&sql, discard),
                    response.closed(),
                    &control,
                    generation,
                    &config,
                    options.as_ref(),
                    (pid, key),
                )
                .await;
                control
                    .transaction
                    .store(session.transaction, Ordering::Release);
                let _ = response.send(result);
                reusable
            }
            Request::Stream {
                sql,
                options,
                _lease,
                events,
                batch,
                notices,
                terminal,
            } => {
                if events
                    .as_ref()
                    .is_some_and(|sender| sender.sender.is_closed())
                    || batch
                        .as_ref()
                        .is_some_and(|sender| sender.sender.is_closed())
                {
                    control.active.store(0, Ordering::Release);
                    continue;
                }
                let (result, reusable) = drive_operation(
                    session.stream_query_inner(&sql, events.as_ref(), batch.as_ref(), &notices),
                    wait_stream_sink_closed(&events, &batch),
                    &control,
                    generation,
                    &config,
                    options.as_ref(),
                    (pid, key),
                )
                .await;
                control
                    .transaction
                    .store(session.transaction, Ordering::Release);
                let _ = terminal.send(result);
                reusable
            }
        };
        session.export_file.take();
        session.import_source.take();
        control.active.store(0, Ordering::Release);
        if !reusable {
            break;
        }
    }
    tokio::time::timeout(Duration::from_secs(5), session.close())
        .await
        .map_err(|_| NzError::Timeout("connection close timeout".into()))?
}

struct AsyncSession {
    stream: Option<AsyncTransport>,
    buffer: BytesMut,
    config: NzConnectionConfig,
    command_number: i32,
    backend_process_id: i32,
    backend_secret_key: i32,
    export_file: Option<File>,
    import_source: Option<(String, crate::connection::ImportSource)>,
    transaction: bool,
}

impl AsyncSession {
    async fn connect(config: &NzConnectionConfig) -> NzResult<Self> {
        config.validate()?;
        let timeout = Duration::from_secs(config.connection_timeout.max(1));
        let deadline = tokio::time::Instant::now() + timeout;
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
            import_source: None,
            transaction: false,
        };
        tokio::time::timeout_at(deadline, session.handshake())
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
                self.skip_bytes(4).await?;
            }
            match msg {
                b'K' => {
                    self.skip_bytes(4).await?;
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

    async fn query_inner(&mut self, sql: &str, discard: bool) -> NzResult<QueryResult> {
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
        self.drain_response_inner(None, None, None, discard).await
    }

    async fn stream_query_inner(
        &mut self,
        sql: &str,
        events: Option<&EventSender>,
        batch: Option<&BatchSender>,
        notices: &Arc<Mutex<NoticeHistory>>,
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
        self.drain_response_stream(events, batch, notices).await
    }

    async fn drain_response_inner(
        &mut self,
        events: Option<&EventSender>,
        batch: Option<&BatchSender>,
        stream_notices: Option<&Arc<Mutex<NoticeHistory>>>,
        discard: bool,
    ) -> NzResult<QueryResult> {
        let mut set_index = 0;
        let mut row_count = 0;
        let mut started = false;
        let mut result_sets = Vec::new();
        let mut current: Option<AsyncResultSet> = None;
        let mut cached_columns: Option<Arc<[ColumnDesc]>> = None;
        let mut tupdesc: Option<Arc<DbosTupleDesc>> = None;
        let mut rows_affected = -1i64;
        let mut notices = Vec::new();
        let mut error: Option<NzError> = None;
        let mut batch_extra_result_set = false;
        let mut batch_rows = Vec::with_capacity(BATCH_ROWS);
        let mut batch_bytes = 0usize;

        loop {
            let msg_type = self.read_message_type().await?;
            match msg_type {
                code::ROW_STANDARD => {
                    self.skip_bytes(4).await?;
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
                    row_count += 1;
                    if let Some(events) = events {
                        emit_result_start(events, set_index, &set.0, &set.2, &mut started).await;
                        let _ = events.send(Ok(QueryStreamEvent::Row(row))).await;
                    } else if let Some(batch) = batch {
                        if !batch_extra_result_set {
                            let _ =
                                push_stream_batch(batch, &mut batch_rows, &mut batch_bytes, row)
                                    .await;
                        }
                    } else if !discard {
                        set.1.push(row);
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
                    self.skip_bytes(4).await?;
                    continue;
                }
                b'e' => {
                    self.handle_ext_log().await?;
                    continue;
                }
                _ => {}
            }

            self.skip_bytes(4).await?;
            match msg_type {
                code::ROW_DESCRIPTION => {
                    let len = validate_protocol_length(
                        self.read_i32().await?,
                        "rowDescriptionPayload",
                        false,
                    )? as usize;
                    let data = self.read_bytes(len).await?;
                    finish_response_set(
                        &mut result_sets,
                        &mut current,
                        events,
                        batch,
                        &mut batch_rows,
                        &mut batch_bytes,
                        &mut set_index,
                        &mut row_count,
                        &mut started,
                    )
                    .await;
                    if batch.is_some() && set_index > 0 {
                        batch_extra_result_set = true;
                    }
                    let columns: Arc<[ColumnDesc]> = Arc::from(parse_row_description(&data)?);
                    cached_columns = Some(columns.clone());
                    tupdesc = None;
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
                    row_count += 1;
                    if let Some(events) = events {
                        emit_result_start(events, set_index, &set.0, &set.2, &mut started).await;
                        let _ = events.send(Ok(QueryStreamEvent::Row(row))).await;
                    } else if let Some(batch) = batch {
                        if !batch_extra_result_set {
                            let _ =
                                push_stream_batch(batch, &mut batch_rows, &mut batch_bytes, row)
                                    .await;
                        }
                    } else if !discard {
                        set.1.push(row);
                    }
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
                    match crate::messages::parse_transaction_state(text.trim_end_matches('\0')).0 {
                        crate::messages::TransactionState::Opened => self.transaction = true,
                        crate::messages::TransactionState::Closed => self.transaction = false,
                        _ => {}
                    }
                    let count = parse_command_complete_rows(&text);
                    if count >= 0 {
                        rows_affected = if rows_affected < 0 {
                            count
                        } else {
                            rows_affected + count
                        };
                    }
                    if let Some(events) = events {
                        if let Some(set) = &current {
                            emit_result_start(events, set_index, &set.0, &set.2, &mut started)
                                .await;
                        }
                        let _ = events
                            .send(Ok(QueryStreamEvent::CommandComplete {
                                tag: text.trim_matches('\0').to_owned(),
                                rows_affected: count,
                            }))
                            .await;
                    }
                    finish_response_set(
                        &mut result_sets,
                        &mut current,
                        events,
                        batch,
                        &mut batch_rows,
                        &mut batch_bytes,
                        &mut set_index,
                        &mut row_count,
                        &mut started,
                    )
                    .await;
                }
                code::NOTICE_RESPONSE => {
                    let len =
                        validate_protocol_length(self.read_i32().await?, "noticePayload", true)?
                            as usize;
                    let data = self.read_bytes(len).await?;
                    let message = parse_backend_error_fields(&data).message;
                    if !message.is_empty() {
                        if let Some(history) = stream_notices {
                            if let Ok(mut history) = history.lock() {
                                history.push(&message);
                            }
                        }
                        if let Some(events) = events {
                            let _ = events.send(Ok(QueryStreamEvent::Notice(message))).await;
                        } else if batch.is_none() {
                            notices.push(message);
                        }
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
                    finish_response_set(
                        &mut result_sets,
                        &mut current,
                        events,
                        batch,
                        &mut batch_rows,
                        &mut batch_bytes,
                        &mut set_index,
                        &mut row_count,
                        &mut started,
                    )
                    .await;
                    if let Some(error) = error {
                        return Err(error);
                    }
                    if batch_extra_result_set {
                        return Err(NzError::Config(
                            "query_batches only exposes the first result set; use query_stream_events for multiple result sets".into(),
                        ));
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

    async fn drain_response_stream(
        &mut self,
        events: Option<&EventSender>,
        batch: Option<&BatchSender>,
        notices: &Arc<Mutex<NoticeHistory>>,
    ) -> NzResult<()> {
        self.drain_response_inner(events, batch, Some(notices), false)
            .await
            .map(|_| ())
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
        let filename = crate::external::decode_filename(&name)?;
        let opened = match self
            .config
            .external_files
            .resolve(std::path::Path::new(&filename))
        {
            Ok(path) => File::create(path).await,
            Err(error) => Err(error),
        };
        match opened {
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
                        u16::from_be_bytes(self.read_bytes(2).await?.as_ref().try_into().map_err(
                            |_| NzError::Protocol("truncated external-table error length".into()),
                        )?) as usize;
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
        let filename = crate::external::decode_filename(&name)?;
        let _host_version = self.read_i32().await?;
        self.write_raw(&1i32.to_be_bytes()).await?;
        let _format = self.read_i32().await?;
        let buffer_size = validate_protocol_length(
            self.read_i32().await?,
            "externalTableImportBufferSize",
            true,
        )? as usize;
        let source = if self
            .import_source
            .as_ref()
            .is_some_and(|(id, _)| id == &filename)
        {
            self.import_source.take().map(|(_, source)| source)
        } else {
            crate::connection::take_import_source(&filename)
        };
        if let Some(source) = source {
            return match source {
                crate::connection::ImportSource::Bytes(data) => {
                    self.send_import_reader(std::io::Cursor::new(data), buffer_size)
                        .await
                }
                crate::connection::ImportSource::AsyncReader(reader) => {
                    self.send_import_reader(reader, buffer_size).await
                }
                crate::connection::ImportSource::Reader(_) => {
                    self.write_raw(&2i32.to_be_bytes()).await?;
                    Ok(())
                }
            };
        }
        let opened = match self
            .config
            .external_files
            .resolve(std::path::Path::new(&filename))
        {
            Ok(path) => File::open(path).await,
            Err(error) => Err(error),
        };
        match opened {
            Ok(file) => self.send_import_reader(file, buffer_size).await,
            Err(_) => {
                self.write_raw(&2i32.to_be_bytes()).await?;
                Ok(())
            }
        }
    }

    async fn send_import_reader(
        &mut self,
        mut reader: impl AsyncRead + Unpin,
        buffer_size: usize,
    ) -> NzResult<()> {
        let mut chunk = vec![0; buffer_size.clamp(1, 64 * 1024)];
        loop {
            let count = match reader.read(&mut chunk).await {
                Ok(count) => count,
                Err(_) => {
                    self.write_raw(&2i32.to_be_bytes()).await?;
                    return Ok(());
                }
            };
            if count == 0 {
                break;
            }
            let mut header = [0u8; 8];
            header[..4].copy_from_slice(&1i32.to_be_bytes());
            header[4..].copy_from_slice(&(count as i32).to_be_bytes());
            self.write_raw(&header).await?;
            self.write_raw(&chunk[..count]).await?;
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
                return Err(NzError::Protocol(
                    "external log filename is too long".into(),
                ));
            }
        }
        let log_type = self.read_i32().await?;
        let extension = match log_type {
            1 => ".nzlog",
            2 => ".nzbad",
            3 => ".nzstats",
            _ => ".log",
        };
        let directory = crate::external::decode_filename(&directory)?;
        let name = crate::external::decode_filename(&name)?;
        let path = std::path::Path::new(&directory).join(format!("{name}{extension}"));
        let mut file = match self.config.external_files.resolve(&path) {
            Ok(path) => File::create(path).await.ok(),
            Err(_) => None,
        };
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
        self.fill(length).await?;
        self.buffer.advance(length);
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

    async fn read_bytes(&mut self, len: usize) -> NzResult<Bytes> {
        validate_protocol_length(len as i32, "asyncPayload", true)?;
        self.fill(len).await?;
        Ok(self.buffer.split_to(len).freeze())
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

async fn emit_result_start(
    events: &EventSender,
    index: usize,
    columns: &Arc<[ColumnDesc]>,
    nullability: &Option<Vec<bool>>,
    started: &mut bool,
) {
    if !*started {
        *started = true;
        let _ = events
            .send(Ok(QueryStreamEvent::ResultSetStart {
                index,
                columns: columns.clone(),
                nullability: nullability.clone(),
            }))
            .await;
    }
}

async fn push_stream_batch(
    sender: &BatchSender,
    rows: &mut Vec<Row>,
    bytes: &mut usize,
    row: Row,
) -> Result<(), ()> {
    let row_bytes = row.retained_bytes();
    if !rows.is_empty() && bytes.saturating_add(row_bytes) > BATCH_BYTES {
        flush_stream_batch(sender, rows, bytes).await?;
    }
    *bytes = bytes.saturating_add(row_bytes);
    rows.push(row);
    if rows.len() >= BATCH_ROWS || *bytes >= BATCH_BYTES {
        flush_stream_batch(sender, rows, bytes).await?;
    }
    Ok(())
}

async fn flush_stream_batch(
    sender: &BatchSender,
    rows: &mut Vec<Row>,
    bytes: &mut usize,
) -> Result<(), ()> {
    if rows.is_empty() {
        return Ok(());
    }
    *bytes = 0;
    sender.send(std::mem::take(rows)).await
}

#[allow(clippy::too_many_arguments)]
async fn finish_response_set(
    results: &mut Vec<crate::connection::ResultSet>,
    current: &mut Option<AsyncResultSet>,
    events: Option<&EventSender>,
    batch: Option<&BatchSender>,
    batch_rows: &mut Vec<Row>,
    batch_bytes: &mut usize,
    index: &mut usize,
    rows: &mut u64,
    started: &mut bool,
) {
    if let Some((columns, values, nullability)) = current.take() {
        if let Some(batch) = batch {
            let _ = flush_stream_batch(batch, batch_rows, batch_bytes).await;
        }
        if let Some(events) = events {
            emit_result_start(events, *index, &columns, &nullability, started).await;
            let _ = events
                .send(Ok(QueryStreamEvent::ResultSetEnd {
                    index: *index,
                    row_count: *rows,
                }))
                .await;
        } else if batch.is_none() {
            results.push(crate::connection::ResultSet {
                columns: columns.to_vec(),
                rows: values,
                nullability,
            });
        }
        *index += 1;
        *rows = 0;
        *started = false;
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn notice_history_caps_bytes_count_and_records_evictions() {
        let mut history = NoticeHistory::default();
        for _ in 0..2000 {
            history.push("notice");
        }
        assert_eq!(history.items.len(), 1000);
        assert_eq!(history.dropped, 1000);
        history.push(&"x".repeat(1024 * 1024));
        assert_eq!(history.items.len(), 1);
        assert_eq!(history.bytes, 1024 * 1024);
        history.push(&"x".repeat(1024 * 1024 + 1));
        assert_eq!(history.bytes, 1024 * 1024);
        assert_eq!(history.dropped, 2001);
    }
    #[tokio::test]
    async fn byte_budget_blocks_until_consumer_releases_payload() {
        let control = Arc::new(Control::default());
        control.active.store(1, Ordering::Release);
        let (sender, mut receiver) = mpsc::channel(8);
        let events = Arc::new(EventSender {
            sender,
            control,
            budget: Arc::new(Semaphore::new(1024)),
        });
        events
            .send(Ok(QueryStreamEvent::Notice("x".repeat(700))))
            .await
            .unwrap();
        let next = events.clone();
        let second = tokio::spawn(async move {
            next.send(Ok(QueryStreamEvent::Notice("y".repeat(700))))
                .await
        });
        tokio::task::yield_now().await;
        assert!(!second.is_finished());
        drop(receiver.recv().await.unwrap());
        tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(receiver.recv().await.unwrap());
        assert_eq!(events.budget.available_permits(), 1024);
    }

    #[tokio::test]
    async fn producer_batches_rows_at_the_configured_limit() {
        let control = Arc::new(Control::default());
        control.active.store(1, Ordering::Release);
        let (sender, mut receiver) = mpsc::channel(4);
        let batches = BatchSender {
            sender,
            control,
            budget: Arc::new(Semaphore::new(STREAM_BYTES)),
        };
        let row = Row::new(
            vec![ColumnDesc {
                name: "ONE".into(),
                type_oid: 23,
                type_len: 4,
                type_mod: -1,
                format: 0,
            }],
            vec![NzValue::Int4(7)],
        );
        let mut pending = Vec::with_capacity(BATCH_ROWS);
        let mut bytes = 0;
        for _ in 0..300 {
            push_stream_batch(&batches, &mut pending, &mut bytes, row.clone())
                .await
                .unwrap();
        }
        flush_stream_batch(&batches, &mut pending, &mut bytes)
            .await
            .unwrap();
        let first = receiver.recv().await.unwrap();
        let second = receiver.recv().await.unwrap();
        assert_eq!(first.rows.len(), 256);
        assert_eq!(second.rows.len(), 44);
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn producer_emits_an_oversized_row_as_a_singleton_batch() {
        let control = Arc::new(Control::default());
        control.active.store(1, Ordering::Release);
        let (sender, mut receiver) = mpsc::channel(2);
        let batches = BatchSender {
            sender,
            control,
            budget: Arc::new(Semaphore::new(STREAM_BYTES)),
        };
        let row = Row::new(
            vec![ColumnDesc {
                name: "TXT".into(),
                type_oid: 1043,
                type_len: -1,
                type_mod: -1,
                format: 0,
            }],
            vec![NzValue::Text("x".repeat(BATCH_BYTES + 1))],
        );
        let mut pending = Vec::with_capacity(BATCH_ROWS);
        let mut bytes = 0;
        push_stream_batch(&batches, &mut pending, &mut bytes, row)
            .await
            .unwrap();
        let batch = receiver.recv().await.unwrap();
        assert_eq!(batch.rows.len(), 1);
        assert!(batch.rows[0].retained_bytes() > BATCH_BYTES);
        assert!(pending.is_empty());
    }
}
