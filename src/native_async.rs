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
//! The legacy `AsyncNzConnection` remains available with the `compat` feature
//! as a compatibility wrapper. This module is the new transport: socket reads
//! and writes are performed by Tokio directly, while one connection still
//! serializes SQL operations because that is a property of the Netezza
//! simple-query protocol.

use crate::config::{NzConnectionConfig, SecurityLevel};
use crate::connection::{QueryResult, Row, RowMetadata};
use crate::error::{parse_backend_error_fields, validate_protocol_length, NzError, NzResult};
use crate::handshake_common;
use crate::messages::{code, parse_command_complete_rows};
use crate::params::SqlTemplate;
use crate::tuple_desc::{parse_row_description, ColumnDesc, DbosTupleDesc};
use crate::types::text::parse_text_data_row_into;
use crate::types::value::ToSql;
use bytes::{Buf, Bytes, BytesMut};
use futures_core::Stream;
use std::collections::{HashMap, VecDeque};
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
const HSV2_CLIENT_BEGIN: i16 = 1;
const HSV2_DB: i16 = 2;
const HSV2_SSL_NEGOTIATE: i16 = 11;
const HSV2_SSL_CONNECT: i16 = 12;
const MAX_BUFFERED_FRAME: usize = 128 * 1024 * 1024;
const SQL_TEMPLATE_CACHE_ENTRIES: usize = 128;
const SQL_TEMPLATE_CACHE_BYTES: usize = 1024 * 1024;
const SQL_TEMPLATE_CACHE_ENTRY_MAX: usize = 64 * 1024;

enum AsyncTransport {
    Plain(TcpStream),
    #[cfg(feature = "ssl")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

type AsyncResultSet = (
    Arc<[ColumnDesc]>,
    Arc<RowMetadata>,
    Vec<Row>,
    Option<Vec<bool>>,
);

#[derive(Debug, Clone, Copy)]
enum RowPolicy {
    AllRows,
    AllRowsEager,
    FirstExact,
    FirstOptional,
    Discard,
}

impl RowPolicy {
    fn retains(self, result_set_index: usize, first_set_row_count: u64) -> bool {
        match self {
            Self::AllRows | Self::AllRowsEager => true,
            Self::FirstExact | Self::FirstOptional => {
                result_set_index == 0 && first_set_row_count == 0
            }
            Self::Discard => false,
        }
    }

    fn eagerly_decodes(self) -> bool {
        matches!(self, Self::AllRowsEager)
    }
}

struct ParsedQueryResult {
    result: QueryResult,
    first_set_row_count: u64,
}

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
enum QueuedStreamItem {
    Event(QueuedEvent),
    Rows(PooledRowBatch),
}
struct QueuedBatch {
    rows: Vec<Row>,
    _permit: OwnedSemaphorePermit,
}
const STREAM_BYTES: usize = 8 * 1024 * 1024;
const BATCH_ROWS: usize = 256;
const BATCH_BYTES: usize = 1024 * 1024;
const STREAM_BATCH_ROWS: usize = 256;
const STREAM_BATCH_BYTES: usize = 1024 * 1024;
const STREAM_EVENT_CHANNEL_BATCHES: usize = 6;
// A single reusable block per stream bounds retained rows to 256, including
// while it is queued or held by the public stream adapter.
const STREAM_BATCH_POOL_SIZE: usize = 1;

struct StreamBatchPool {
    free: Mutex<Vec<Vec<Option<Row>>>>,
    slots: Arc<Semaphore>,
}

impl StreamBatchPool {
    fn new() -> Self {
        let mut free = Vec::with_capacity(STREAM_BATCH_POOL_SIZE);
        for _ in 0..STREAM_BATCH_POOL_SIZE {
            free.push(Vec::with_capacity(STREAM_BATCH_ROWS));
        }
        Self {
            free: Mutex::new(free),
            slots: Arc::new(Semaphore::new(STREAM_BATCH_POOL_SIZE)),
        }
    }

    async fn acquire(self: &Arc<Self>, control: &Control) -> Result<PooledRowBatch, ()> {
        let generation = control.active.load(Ordering::Acquire);
        let slot = tokio::select! {
            biased;
            _ = control.interrupted(generation) => return Err(()),
            permit = self.slots.clone().acquire_owned() => permit.map_err(|_| ())?,
        };
        let rows = self
            .free
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop()
            .expect("batch slot permits and free buffers stay in sync");
        Ok(PooledRowBatch {
            pool: self.clone(),
            rows,
            len: 0,
            next: 0,
            bytes: 0,
            row_permit: None,
            staging_permit: None,
            _slot: slot,
        })
    }

    fn recycle(&self, mut rows: Vec<Option<Row>>) {
        rows.clear();
        self.free
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(rows);
    }
}

struct PooledRowBatch {
    pool: Arc<StreamBatchPool>,
    rows: Vec<Option<Row>>,
    len: usize,
    next: usize,
    bytes: usize,
    row_permit: Option<OwnedSemaphorePermit>,
    staging_permit: Option<OwnedSemaphorePermit>,
    _slot: OwnedSemaphorePermit,
}

impl PooledRowBatch {
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn push(&mut self, row: Row, bytes: usize) {
        debug_assert!(self.len < STREAM_BATCH_ROWS);
        self.rows.push(Some(row));
        self.len += 1;
        self.bytes = self.bytes.saturating_add(bytes);
    }

    fn pop_row(&mut self) -> Option<Row> {
        let row = self.rows.get_mut(self.next)?.take()?;
        self.next += 1;
        Some(row)
    }

    fn drained(&self) -> bool {
        self.next >= self.len
    }
}

impl Drop for PooledRowBatch {
    fn drop(&mut self) {
        let rows = std::mem::take(&mut self.rows);
        self.pool.recycle(rows);
    }
}

struct EventSender {
    sender: mpsc::Sender<QueuedStreamItem>,
    budget: Arc<Semaphore>,
    batch_pool: Arc<StreamBatchPool>,
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
                self.sender.send(QueuedStreamItem::Event(QueuedEvent { value: event, _permit: permit })).await.map_err(|_| ())
            } => result,
        }
    }

    async fn push_row(&self, batch: &mut Option<PooledRowBatch>, row: Row) -> Result<(), ()> {
        let row_bytes = row.retained_bytes().clamp(1, STREAM_BYTES);
        if batch.as_ref().is_some_and(|batch| {
            !batch.is_empty()
                && (batch.len >= STREAM_BATCH_ROWS
                    || batch.bytes.saturating_add(row_bytes) > STREAM_BATCH_BYTES)
        }) {
            self.flush_rows(batch).await?;
        }
        if batch.is_none() {
            let mut acquired = self.batch_pool.acquire(&self.control).await?;
            if row_bytes <= STREAM_BATCH_BYTES {
                let generation = self.control.active.load(Ordering::Acquire);
                let permit = tokio::select! {
                    biased;
                    _ = self.control.interrupted(generation) => return Err(()),
                    permit = self.budget.clone().acquire_many_owned(STREAM_BATCH_BYTES as u32) => permit.map_err(|_| ())?,
                };
                acquired.staging_permit = Some(permit);
            }
            *batch = Some(acquired);
        }
        let should_flush = {
            let current = batch.as_mut().expect("batch acquired above");
            current.push(row, row_bytes);
            current.len >= STREAM_BATCH_ROWS || current.bytes >= STREAM_BATCH_BYTES
        };
        if should_flush {
            self.flush_rows(batch).await?;
        }
        Ok(())
    }

    async fn flush_rows(&self, batch: &mut Option<PooledRowBatch>) -> Result<(), ()> {
        let Some(rows) = batch.take() else {
            return Ok(());
        };
        let generation = self.control.active.load(Ordering::Acquire);
        let bytes = rows.bytes.clamp(1, STREAM_BYTES) as u32;
        let permit = tokio::select! {
            biased;
            _ = self.control.interrupted(generation) => return Err(()),
            permit = self.budget.clone().acquire_many_owned(bytes) => permit.map_err(|_| ())?,
        };
        let mut rows = rows;
        rows.row_permit = Some(permit);
        rows.staging_permit.take();
        tokio::select! {
            biased;
            _ = self.control.interrupted(generation) => Err(()),
            result = self.sender.send(QueuedStreamItem::Rows(rows)) => result.map_err(|_| ()),
        }
    }
}

enum Request {
    /// Report whether the idle session's socket is still usable.
    Probe {
        _lease: Option<tokio::sync::OwnedMutexGuard<()>>,
        response: oneshot::Sender<bool>,
    },
    Query {
        sql: String,
        row_policy: RowPolicy,
        options: Option<QueryOptions>,
        import_source: Option<(String, crate::connection::ImportSource)>,
        _lease: Option<tokio::sync::OwnedMutexGuard<()>>,
        response: Response<ParsedQueryResult>,
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
    receiver: mpsc::Receiver<QueuedStreamItem>,
    pending_rows: Option<PooledRowBatch>,
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
    receiver: mpsc::Receiver<QueuedStreamItem>,
    pending_rows: Option<PooledRowBatch>,
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
        match poll_event(&mut this.receiver, &mut this.pending_rows, cx) {
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
        loop {
            match poll_event(&mut this.receiver, &mut this.pending_rows, cx) {
                Poll::Ready(Some(Ok(QueryStreamEvent::ResultSetStart { index, .. })))
                    if index > 0 =>
                {
                    if !this.first_set_only_error {
                        this.first_set_only_error = true;
                        this.receiver.close();
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
    receiver: &mut mpsc::Receiver<QueuedStreamItem>,
    pending_rows: &mut Option<PooledRowBatch>,
    cx: &mut Context<'_>,
) -> Poll<Option<NzResult<QueryStreamEvent>>> {
    loop {
        if let Some(batch) = pending_rows.as_mut() {
            if let Some(row) = batch.pop_row() {
                if batch.drained() {
                    *pending_rows = None;
                }
                return Poll::Ready(Some(Ok(QueryStreamEvent::Row(row))));
            }
            *pending_rows = None;
        }

        match receiver.poll_recv(cx) {
            Poll::Ready(Some(QueuedStreamItem::Event(event))) => {
                return Poll::Ready(Some(event.value));
            }
            Poll::Ready(Some(QueuedStreamItem::Rows(batch))) => *pending_rows = Some(batch),
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        }
    }
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
    sql_templates: Arc<Mutex<SqlTemplateCache>>,
    exclusive: bool,
}

#[derive(Default)]
struct SqlTemplateCache {
    entries: HashMap<Arc<str>, CachedSqlTemplate>,
    retained_bytes: usize,
    clock: u64,
}

struct CachedSqlTemplate {
    template: Arc<SqlTemplate>,
    retained_bytes: usize,
    last_used: u64,
}

impl SqlTemplateCache {
    fn get(&mut self, sql: &str) -> Option<Arc<SqlTemplate>> {
        self.clock = self.clock.wrapping_add(1).max(1);
        let entry = self.entries.get_mut(sql)?;
        entry.last_used = self.clock;
        Some(entry.template.clone())
    }

    fn insert(&mut self, template: Arc<SqlTemplate>) {
        let retained_bytes = template.retained_bytes();
        if retained_bytes > SQL_TEMPLATE_CACHE_ENTRY_MAX
            || retained_bytes > SQL_TEMPLATE_CACHE_BYTES
        {
            return;
        }
        let key = template.source();
        if self.entries.contains_key(key.as_ref()) {
            return;
        }
        while self.entries.len() >= SQL_TEMPLATE_CACHE_ENTRIES
            || self.retained_bytes.saturating_add(retained_bytes) > SQL_TEMPLATE_CACHE_BYTES
        {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone());
            let Some(oldest) = oldest else {
                break;
            };
            if let Some(removed) = self.entries.remove(oldest.as_ref()) {
                self.retained_bytes = self.retained_bytes.saturating_sub(removed.retained_bytes);
            }
        }
        self.clock = self.clock.wrapping_add(1).max(1);
        self.retained_bytes = self.retained_bytes.saturating_add(retained_bytes);
        self.entries.insert(
            key,
            CachedSqlTemplate {
                template,
                retained_bytes,
                last_used: self.clock,
            },
        );
    }
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
            sql_templates: Arc::new(Mutex::new(SqlTemplateCache::default())),
            exclusive: false,
        },
        Connection {
            future,
            control: control.clone(),
        },
    ))
}

impl Client {
    fn render_sql(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<String, String> {
        let cached = self
            .sql_templates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(sql);
        let template = match cached {
            Some(template) => template,
            None => {
                let template = Arc::new(SqlTemplate::parse(sql)?);
                self.sql_templates
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(template.clone());
                template
            }
        };
        template.render(params.len(), |index, output| {
            params[index].write_sql(output)
        })
    }

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
        let sql = self.render_sql(sql, params).map_err(NzError::Config)?;
        let result = self
            .send_query(sql, RowPolicy::AllRowsEager, None)
            .await?
            .result;
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
        let sql = self.render_sql(sql, params).map_err(NzError::Config)?;
        self.send_query(sql, RowPolicy::AllRowsEager, None)
            .await
            .map(|parsed| parsed.result)
    }

    /// Execute with an explicit deadline independent of the connection default.
    pub async fn query_with_options(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
        options: QueryOptions,
    ) -> NzResult<Vec<Row>> {
        let sql = self.render_sql(sql, params).map_err(NzError::Config)?;
        let result = self
            .send_query(sql, RowPolicy::AllRowsEager, Some(options))
            .await?
            .result;
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
        let sql = self.render_sql(sql, params).map_err(NzError::Config)?;
        self.send_query_with_source(
            sql,
            RowPolicy::Discard,
            None,
            Some((
                id.to_owned(),
                crate::connection::ImportSource::AsyncReader(Box::pin(reader)),
            )),
        )
        .await
        .map(|parsed| parsed.result)
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
            pending_rows: stream.pending_rows,
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
            pending_rows: stream.pending_rows,
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
        let sql = self.render_sql(sql, params).map_err(NzError::Config)?;
        let (sender, receiver) = mpsc::channel(STREAM_EVENT_CHANNEL_BATCHES);
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
                    // One reusable block per stream: a partially consumed
                    // stream that still holds its rows must not starve the
                    // producer of a later stream on the same connection.
                    batch_pool: Arc::new(StreamBatchPool::new()),
                }),
                batch: None,
                terminal,
                notices: notices.clone(),
            })
            .await
            .map_err(|_| NzError::Closed("connection task is closed".into()))?;
        Ok(QueryEventStream {
            receiver,
            pending_rows: None,
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
        let sql = self.render_sql(sql, params).map_err(NzError::Config)?;
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
        let sql = self.render_sql(sql, params).map_err(NzError::Config)?;
        let parsed = self.send_query(sql, RowPolicy::FirstExact, None).await?;
        if parsed.result.result_sets.len() > 1 {
            return Err(NzError::Config(
                "query returned multiple result sets; use query_multi".into(),
            ));
        }
        if parsed.first_set_row_count != 1 {
            return Err(NzError::Config(format!(
                "query_one: expected one row, got {}",
                parsed.first_set_row_count
            )));
        }
        Ok(parsed
            .result
            .into_rows()
            .into_iter()
            .next()
            .expect("one row checked above"))
    }

    pub async fn query_opt(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> NzResult<Option<Row>> {
        let sql = self.render_sql(sql, params).map_err(NzError::Config)?;
        let parsed = self.send_query(sql, RowPolicy::FirstOptional, None).await?;
        if parsed.result.result_sets.len() > 1 {
            return Err(NzError::Config(
                "query returned multiple result sets; use query_multi".into(),
            ));
        }
        match parsed.first_set_row_count {
            0 => Ok(None),
            1 => Ok(parsed.result.into_rows().into_iter().next()),
            count => Err(NzError::Config(format!(
                "query_opt: expected at most one row, got {count}"
            ))),
        }
    }

    pub async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> NzResult<i64> {
        let sql = self.render_sql(sql, params).map_err(NzError::Config)?;
        Ok(self
            .send_query(sql, RowPolicy::Discard, None)
            .await?
            .result
            .rows_affected)
    }

    pub async fn batch_execute(&self, sql: &str) -> NzResult<()> {
        self.send_query(sql.to_owned(), RowPolicy::Discard, None)
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

    /// Pool checkout probe: `false` when the idle session can no longer be
    /// used (peer closed it, unsolicited data, or the driver has stopped).
    pub(crate) async fn probe_idle(&self) -> bool {
        if self.is_closed() {
            return false;
        }
        let Ok(lease) = self.acquire_session().await else {
            return false;
        };
        let (sender, receiver) = oneshot::channel();
        if self
            .requests
            .send(Request::Probe {
                _lease: lease,
                response: sender,
            })
            .await
            .is_err()
        {
            return false;
        }
        receiver.await.unwrap_or(false)
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
        row_policy: RowPolicy,
        options: Option<QueryOptions>,
    ) -> NzResult<ParsedQueryResult> {
        self.send_query_with_source(sql, row_policy, options, None)
            .await
    }
    async fn send_query_with_source(
        &self,
        sql: String,
        row_policy: RowPolicy,
        options: Option<QueryOptions>,
        import_source: Option<(String, crate::connection::ImportSource)>,
    ) -> NzResult<ParsedQueryResult> {
        if sql.contains('\0') {
            return Err(NzError::Config("SQL contains NUL".into()));
        }
        let (sender, receiver) = oneshot::channel();
        self.requests
            .send(Request::Query {
                sql,
                row_policy,
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
            Request::Probe { _lease, response } => {
                let healthy = session.idle_socket_is_healthy().await;
                let _ = response.send(healthy);
                healthy
            }
            Request::Query {
                sql,
                row_policy,
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
                    session.query_inner(&sql, row_policy),
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
    // A client may have reserved channel capacity before the loop ended and
    // still push its request afterwards; such a request would never be
    // answered. Close the queue and drain it until every outstanding permit
    // is released, so each late caller observes `Closed` instead of hanging.
    receiver.close();
    while receiver.recv().await.is_some() {}
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

    /// Zero-wait check that the peer has neither closed the idle socket nor
    /// sent unsolicited bytes (which would desynchronize the next response).
    async fn idle_socket_is_healthy(&mut self) -> bool {
        // NUL padding between messages is normal (the appliance pads after
        // ReadyForQuery) and is skipped by the parser.
        if self.buffer.iter().any(|&b| b != 0) {
            return false;
        }
        let mut pending = [0u8; 64];
        match self.stream.as_mut() {
            None => false,
            // Elapsed: nothing to read, the session is idle and open.
            Some(AsyncTransport::Plain(stream)) => {
                match tokio::time::timeout(Duration::ZERO, stream.peek(&mut pending)).await {
                    Err(_elapsed) => true,
                    Ok(Ok(n)) => n > 0 && pending[..n].iter().all(|&b| b == 0),
                    Ok(Err(_)) => false,
                }
            }
            // Pending TLS records (e.g. TLS 1.3 session tickets) are not
            // protocol data; only EOF or a socket error retires the session.
            #[cfg(feature = "ssl")]
            Some(AsyncTransport::Tls(stream)) => !matches!(
                tokio::time::timeout(Duration::ZERO, stream.get_ref().0.peek(&mut pending)).await,
                Ok(Ok(0) | Err(_))
            ),
        }
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

        let options = handshake_common::option_plan(
            version,
            handshake_common::HandshakeClientInfo {
                user: &self.config.user,
                app_name: &self.config.app_name,
                client_os: std::env::consts::OS,
                client_host_name: &self.config.client_host_name,
                os_user: &self.config.os_user,
                remote_pid: std::process::id() as i32,
                client_type: self.config.client_type,
            },
        );
        for option in options {
            self.write_frame(option.opcode, &option.payload).await?;
            if let Some(stage) = option.ack_after {
                self.expect_ack(stage).await?;
            }
        }

        match self.read_byte().await? {
            b'R' => {}
            b'E' => return Err(self.read_backend_error("authenticationError").await),
            other => {
                return Err(NzError::Protocol(format!(
                    "authentication: unexpected response byte 0x{other:02x}"
                )))
            }
        }
        let request = self.read_i32().await?;
        let salt_len = handshake_common::auth_salt_len(request)?;
        let salt = self.read_bytes(salt_len).await?;
        if let Some(payload) =
            handshake_common::auth_response(request, &self.config.password, &salt)?
        {
            self.write_auth_response(&payload).await?;
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
                    let proposed = match self.read_byte().await? {
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
                    handshake_common::validate_version_downgrade(version, proposed)?;
                    version = proposed;
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

    async fn query_inner(
        &mut self,
        sql: &str,
        row_policy: RowPolicy,
    ) -> NzResult<ParsedQueryResult> {
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
        self.drain_response_inner(None, None, None, row_policy)
            .await
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
        row_policy: RowPolicy,
    ) -> NzResult<ParsedQueryResult> {
        let mut set_index = 0;
        let mut row_count = 0;
        let mut first_set_row_count = 0u64;
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
        let mut event_rows: Option<PooledRowBatch> = None;
        let mut dbos_varying_scratch = Vec::new();

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
                    if current.is_none() {
                        let columns = cached_columns
                            .clone()
                            .unwrap_or_else(|| Arc::from(descriptor.to_column_descs()));
                        current = Some((
                            columns.clone(),
                            Arc::new(RowMetadata::new(columns.clone())),
                            Vec::new(),
                            None,
                        ));
                    }
                    let retain = events.is_some()
                        || (batch.is_some() && !batch_extra_result_set)
                        || row_policy.retains(set_index, first_set_row_count);
                    row_count += 1;
                    if set_index == 0 {
                        first_set_row_count += 1;
                    }
                    if !retain {
                        self.discard_exact(row_len).await?;
                        continue;
                    }
                    let payload = self.read_bytes(row_len).await?;
                    let row_metadata = current.as_ref().expect("result set initialized").1.clone();
                    let row = if row_policy.eagerly_decodes() {
                        let mut values = Vec::with_capacity(
                            current.as_ref().expect("result set initialized").0.len(),
                        );
                        descriptor.parse_row_into_with_scratch(
                            &payload,
                            &mut values,
                            &mut dbos_varying_scratch,
                        )?;
                        Row::from_shared_dbos_metadata(row_metadata, values, descriptor.clone())
                    } else {
                        Row::from_dbos_raw_with_metadata(row_metadata, payload, descriptor.clone())?
                    };
                    let set = current.as_mut().expect("result set initialized");
                    if let Some(events) = events {
                        emit_result_start(events, set_index, &set.0, &set.3, &mut started).await;
                        let _ = events.push_row(&mut event_rows, row).await;
                    } else if let Some(batch) = batch {
                        if !batch_extra_result_set {
                            let _ =
                                push_stream_batch(batch, &mut batch_rows, &mut batch_bytes, row)
                                    .await;
                        }
                    } else {
                        set.2.push(row);
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
                        &mut event_rows,
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
                    let metadata = Arc::new(RowMetadata::new(columns.clone()));
                    current = Some((columns, metadata, Vec::new(), None));
                }
                code::DATA_ROW => {
                    let len =
                        validate_protocol_length(self.read_i32().await?, "dataRowPayload", false)?
                            as usize;
                    let columns = current
                        .as_ref()
                        .map(|set| set.0.clone())
                        .or_else(|| cached_columns.clone())
                        .ok_or_else(|| {
                            NzError::Protocol("DataRow received before RowDescription".into())
                        })?;
                    if current.is_none() {
                        current = Some((
                            columns.clone(),
                            Arc::new(RowMetadata::new(columns.clone())),
                            Vec::new(),
                            None,
                        ));
                    }
                    let retain = events.is_some()
                        || (batch.is_some() && !batch_extra_result_set)
                        || row_policy.retains(set_index, first_set_row_count);
                    row_count += 1;
                    if set_index == 0 {
                        first_set_row_count += 1;
                    }
                    if !retain {
                        self.discard_exact(len).await?;
                        continue;
                    }
                    let data = self.read_bytes(len).await?;
                    let row_metadata = current.as_ref().expect("result set initialized").1.clone();
                    let row = if row_policy.eagerly_decodes() {
                        let columns = current.as_ref().expect("result set initialized").0.as_ref();
                        let mut values = Vec::with_capacity(columns.len());
                        parse_text_data_row_into(&data, columns, &mut values)
                            .map_err(NzError::Protocol)?;
                        Row::from_shared_metadata(row_metadata, values)
                    } else {
                        Row::from_text_raw_with_metadata(row_metadata, data)?
                    };
                    let set = current.as_mut().expect("result set initialized");
                    if let Some(events) = events {
                        emit_result_start(events, set_index, &set.0, &set.3, &mut started).await;
                        let _ = events.push_row(&mut event_rows, row).await;
                    } else if let Some(batch) = batch {
                        if !batch_extra_result_set {
                            let _ =
                                push_stream_batch(batch, &mut batch_rows, &mut batch_bytes, row)
                                    .await;
                        }
                    } else {
                        set.2.push(row);
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
                    let metadata = current
                        .as_ref()
                        .map(|set| set.1.clone())
                        .unwrap_or_else(|| Arc::new(RowMetadata::new(columns.clone())));
                    let set = current.get_or_insert_with(|| (columns, metadata, Vec::new(), None));
                    set.3 = Some(
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
                            emit_result_start(events, set_index, &set.0, &set.3, &mut started)
                                .await;
                        }
                        let _ = events.flush_rows(&mut event_rows).await;
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
                        &mut event_rows,
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
                            let _ = events.flush_rows(&mut event_rows).await;
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
                        &mut event_rows,
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
                    return Ok(ParsedQueryResult {
                        result: QueryResult {
                            result_sets,
                            rows_affected,
                            notices,
                        },
                        first_set_row_count,
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
        self.drain_response_inner(events, batch, Some(notices), RowPolicy::AllRows)
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

    /// Consume a known payload without retaining it. Bytes already read ahead
    /// are advanced in place; the remainder is drained through a fixed-size
    /// scratch buffer so large discarded rows never grow `self.buffer`.
    async fn discard_exact(&mut self, length: usize) -> NzResult<()> {
        let buffered = length.min(self.buffer.len());
        self.buffer.advance(buffered);
        let mut remaining = length - buffered;
        let mut scratch = [0u8; 16 * 1024];
        while remaining > 0 {
            let take = remaining.min(scratch.len());
            let read = self
                .stream_mut()?
                .read(&mut scratch[..take])
                .await
                .map_err(NzError::Io)?;
            if read == 0 {
                return Err(NzError::Closed(
                    "socket closed while discarding async row".into(),
                ));
            }
            remaining -= read;
        }
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
    event_rows: &mut Option<PooledRowBatch>,
    batch_rows: &mut Vec<Row>,
    batch_bytes: &mut usize,
    index: &mut usize,
    rows: &mut u64,
    started: &mut bool,
) {
    if let Some(events) = events {
        let _ = events.flush_rows(event_rows).await;
    }
    if let Some(batch) = batch {
        let _ = flush_stream_batch(batch, batch_rows, batch_bytes).await;
    }
    if let Some((columns, _metadata, values, nullability)) = current.take() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::value::NzValue;

    fn test_client() -> Client {
        let (requests, _receiver) = mpsc::channel(1);
        Client {
            requests,
            control: Arc::new(Control::default()),
            session: Arc::new(tokio::sync::Mutex::new(())),
            sql_templates: Arc::new(Mutex::new(SqlTemplateCache::default())),
            exclusive: false,
        }
    }

    #[test]
    fn sql_template_cache_is_lru_and_bounded_by_entries_and_bytes() {
        let mut cache = SqlTemplateCache::default();
        for index in 0..SQL_TEMPLATE_CACHE_ENTRIES {
            cache.insert(Arc::new(
                SqlTemplate::parse(&format!("SELECT {index}, $1")).unwrap(),
            ));
        }
        assert_eq!(cache.entries.len(), SQL_TEMPLATE_CACHE_ENTRIES);
        assert!(cache.get("SELECT 0, $1").is_some());
        cache.insert(Arc::new(SqlTemplate::parse("SELECT 128, $1").unwrap()));
        assert!(cache.get("SELECT 0, $1").is_some());
        assert!(cache.get("SELECT 1, $1").is_none());
        assert_eq!(cache.entries.len(), SQL_TEMPLATE_CACHE_ENTRIES);

        let before = cache.entries.len();
        let oversized = format!("SELECT {}", "x".repeat(SQL_TEMPLATE_CACHE_ENTRY_MAX));
        cache.insert(Arc::new(SqlTemplate::parse(&oversized).unwrap()));
        assert_eq!(cache.entries.len(), before);

        for index in 0..20 {
            let sql = format!("SELECT {index} {}", "y".repeat(60 * 1024));
            cache.insert(Arc::new(SqlTemplate::parse(&sql).unwrap()));
        }
        assert!(cache.retained_bytes <= SQL_TEMPLATE_CACHE_BYTES);
    }

    #[test]
    fn native_sql_renderer_calls_to_sql_writer_and_reuses_template() {
        #[derive(Debug)]
        struct DirectLiteral(&'static str);

        impl ToSql for DirectLiteral {
            fn to_nz_value(&self) -> NzValue {
                panic!("native renderer should call write_sql directly")
            }

            fn write_sql(&self, output: &mut String) -> Result<(), String> {
                output.push_str(self.0);
                Ok(())
            }
        }

        let client = test_client();
        let first = DirectLiteral("41");
        let second = DirectLiteral("42");
        assert_eq!(
            client
                .render_sql("SELECT $1", &[&first as &(dyn ToSql + Sync)])
                .unwrap(),
            "SELECT 41"
        );
        assert_eq!(
            client
                .render_sql("SELECT $1", &[&second as &(dyn ToSql + Sync)])
                .unwrap(),
            "SELECT 42"
        );
        assert_eq!(client.sql_templates.lock().unwrap().entries.len(), 1);
    }

    #[test]
    fn first_row_policies_keep_one_row_and_discard_every_tail_row() {
        assert!(RowPolicy::AllRows.retains(0, 0));
        assert!(RowPolicy::AllRows.retains(4, 10));
        assert!(!RowPolicy::AllRows.eagerly_decodes());
        assert!(RowPolicy::AllRowsEager.retains(0, 0));
        assert!(RowPolicy::AllRowsEager.eagerly_decodes());
        assert!(RowPolicy::FirstExact.retains(0, 0));
        assert!(!RowPolicy::FirstExact.retains(0, 1));
        assert!(!RowPolicy::FirstExact.retains(1, 0));
        assert!(RowPolicy::FirstOptional.retains(0, 0));
        assert!(!RowPolicy::FirstOptional.retains(0, 1));
        assert!(!RowPolicy::Discard.retains(0, 0));
    }

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
            batch_pool: Arc::new(StreamBatchPool::new()),
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
    async fn native_event_stream_batches_rows_and_flattens_them_in_order() {
        let control = Arc::new(Control::default());
        control.active.store(1, Ordering::Release);
        let budget = Arc::new(Semaphore::new(16 * 1024 * 1024));
        let initial_permits = budget.available_permits();
        let (sender, mut receiver) = mpsc::channel(4);
        let batch_pool = Arc::new(StreamBatchPool::new());
        let events = EventSender {
            sender,
            control,
            budget: budget.clone(),
            batch_pool: batch_pool.clone(),
        };

        events
            .send(Ok(QueryStreamEvent::Notice("before".into())))
            .await
            .unwrap();
        let mut producer_batch = None;
        for value in 0..STREAM_BATCH_ROWS {
            events
                .push_row(
                    &mut producer_batch,
                    Row::new(
                        vec![ColumnDesc {
                            name: "ONE".into(),
                            type_oid: 23,
                            type_len: 4,
                            type_mod: -1,
                            format: 0,
                        }],
                        vec![NzValue::Int4(value as i32)],
                    ),
                )
                .await
                .unwrap();
        }
        assert!(producer_batch.is_none());
        assert_eq!(batch_pool.slots.available_permits(), 0);
        assert_eq!(receiver.len(), 2, "one notice and one row batch are queued");
        events
            .send(Ok(QueryStreamEvent::Notice("after".into())))
            .await
            .unwrap();

        let mut pending_rows = None;
        let mut delivered = Vec::new();
        while let Some(event) =
            std::future::poll_fn(|cx| poll_event(&mut receiver, &mut pending_rows, cx)).await
        {
            match event.unwrap() {
                QueryStreamEvent::Notice(message) => delivered.push(message),
                QueryStreamEvent::Row(row) => {
                    delivered.push(row.try_get::<_, i32>(0).unwrap().to_string());
                    if !pending_rows.as_ref().is_some_and(PooledRowBatch::drained) {
                        assert!(budget.available_permits() < initial_permits);
                    }
                }
                _ => panic!("unexpected event in row batching test"),
            }
            if delivered.len() == STREAM_BATCH_ROWS + 2 {
                break;
            }
        }

        let mut expected = vec!["before".to_owned()];
        expected.extend((0..STREAM_BATCH_ROWS).map(|value| value.to_string()));
        expected.push("after".to_owned());
        assert_eq!(delivered, expected);
        assert_eq!(budget.available_permits(), initial_permits);
        assert_eq!(batch_pool.slots.available_permits(), STREAM_BATCH_POOL_SIZE);
        assert_eq!(
            batch_pool.free.lock().unwrap().len(),
            STREAM_BATCH_POOL_SIZE
        );
    }

    #[tokio::test]
    async fn native_event_stream_sends_oversized_row_as_a_singleton() {
        let control = Arc::new(Control::default());
        control.active.store(1, Ordering::Release);
        let budget = Arc::new(Semaphore::new(STREAM_BYTES));
        let batch_pool = Arc::new(StreamBatchPool::new());
        let (sender, mut receiver) = mpsc::channel(2);
        let events = EventSender {
            sender,
            control,
            budget: budget.clone(),
            batch_pool: batch_pool.clone(),
        };
        let mut pending = None;
        let row = Row::new(
            vec![ColumnDesc {
                name: "TXT".into(),
                type_oid: 1043,
                type_len: -1,
                type_mod: -1,
                format: 0,
            }],
            vec![NzValue::Text("x".repeat(STREAM_BATCH_BYTES + 1))],
        );

        events.push_row(&mut pending, row).await.unwrap();
        assert!(pending.is_none());
        let Some(QueuedStreamItem::Rows(batch)) = receiver.recv().await else {
            panic!("expected one streamed row batch");
        };
        assert_eq!(batch.len, 1);
        assert!(batch.bytes > STREAM_BATCH_BYTES);
        drop(batch);
        assert_eq!(budget.available_permits(), STREAM_BYTES);
        assert_eq!(batch_pool.slots.available_permits(), STREAM_BATCH_POOL_SIZE);
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
