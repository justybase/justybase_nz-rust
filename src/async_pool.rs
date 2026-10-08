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

//! Cancellation-safe pool for the native Tokio client.

use crate::config::NzConnectionConfig;
use crate::connection::{QueryResult, Row};
use crate::error::{NzError, NzResult};
use crate::native_async::Client;
use crate::types::value::ToSql;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Clone)]
pub struct AsyncNzPoolConfig {
    pub connection: NzConnectionConfig,
    pub max: usize,
    pub min: usize,
    pub idle_timeout: Duration,
    pub wait_timeout: Option<Duration>,
    pub max_uses: Option<u64>,
    pub max_lifetime: Option<Duration>,
    pub rollback_on_release: bool,
}

impl AsyncNzPoolConfig {
    pub fn new(connection: NzConnectionConfig) -> Self {
        Self {
            connection,
            max: 10,
            min: 0,
            idle_timeout: Duration::from_secs(10),
            wait_timeout: None,
            max_uses: None,
            max_lifetime: None,
            rollback_on_release: true,
        }
    }

    fn validate(&self) -> NzResult<()> {
        if self.max == 0 {
            return Err(NzError::Config("Pool max must be positive".into()));
        }
        if self.min > self.max {
            return Err(NzError::Config("Pool min must be between 0 and max".into()));
        }
        if self.max_uses == Some(0) {
            return Err(NzError::Config("Pool max_uses must be positive".into()));
        }
        Ok(())
    }
}

struct IdleConnection {
    conn: Client,
    idle_since: Instant,
    uses: u64,
    born: Instant,
}
struct PoolState {
    idle: Vec<IdleConnection>,
    total: usize,
    closed: bool,
}

/// Native Tokio pool. A semaphore reserves each connection through cleanup.
pub struct AsyncNzPool {
    config: Arc<AsyncNzPoolConfig>,
    state: Arc<Mutex<PoolState>>,
    permits: Arc<Semaphore>,
}

struct Reservation {
    state: Arc<Mutex<PoolState>>,
    registered: bool,
    permit: Option<OwnedSemaphorePermit>,
}
impl Reservation {
    async fn retire(&mut self, conn: Client) -> NzResult<()> {
        let permit = self.permit.take().expect("reservation permit");
        let state = self.state.clone();
        let retirement = tokio::spawn(async move {
            let _ = conn.close().await;
            let mut state = state.lock().expect("pool state lock poisoned");
            state.total = state.total.saturating_sub(1);
            drop(state);
            permit
        });
        self.permit = Some(
            retirement
                .await
                .map_err(|_| NzError::Closed("pool connection retirement task stopped".into()))?,
        );
        Ok(())
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if self.registered {
            let mut state = self.state.lock().expect("pool state lock poisoned");
            state.total = state.total.saturating_sub(1);
        }
    }
}

impl AsyncNzPool {
    /// Create a lazy pool. Use `connect` to prewarm `min` connections.
    pub fn new(config: AsyncNzPoolConfig) -> NzResult<Self> {
        config.validate()?;
        let max = config.max;
        Ok(Self {
            config: Arc::new(config),
            state: Arc::new(Mutex::new(PoolState {
                idle: Vec::new(),
                total: 0,
                closed: false,
            })),
            permits: Arc::new(Semaphore::new(max)),
        })
    }

    /// Create the pool and prewarm its configured minimum.
    pub async fn connect(config: AsyncNzPoolConfig) -> NzResult<Self> {
        let pool = Self::new(config)?;
        let mut holders = Vec::new();
        for _ in 0..pool.config.min {
            holders.push(pool.get().await?);
        }
        for holder in holders {
            holder.release().await;
        }
        Ok(pool)
    }

    pub async fn total_count(&self) -> usize {
        self.state.lock().expect("pool lock").total
    }
    pub async fn idle_count(&self) -> usize {
        self.state.lock().expect("pool lock").idle.len()
    }

    /// Checkout budget includes waiting and connection establishment.
    pub async fn get(&self) -> NzResult<AsyncPooledConnection> {
        let acquire = self.acquire();
        match self.config.wait_timeout {
            Some(timeout) => tokio::time::timeout(timeout, acquire)
                .await
                .map_err(|_| NzError::Timeout("pool checkout timeout".into()))?,
            None => acquire.await,
        }
    }

    async fn acquire(&self) -> NzResult<AsyncPooledConnection> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| NzError::Closed("pool is closed".into()))?;
        let mut reservation = Reservation {
            state: self.state.clone(),
            registered: false,
            permit: Some(permit),
        };
        loop {
            let idle = {
                let mut state = self.state.lock().expect("pool lock");
                if state.closed {
                    return Err(NzError::Closed("pool is closed".into()));
                }
                state.idle.pop()
            };
            let Some(idle) = idle else {
                break;
            };
            let fresh = !idle.conn.is_closed()
                && !self
                    .config
                    .max_lifetime
                    .is_some_and(|limit| idle.born.elapsed() >= limit)
                && !self.config.max_uses.is_some_and(|limit| idle.uses >= limit)
                && (self.config.idle_timeout.is_zero()
                    || idle.idle_since.elapsed() < self.config.idle_timeout
                    || self.state.lock().expect("pool lock").total <= self.config.min);
            // Probe outside the state lock: a socket the server closed while
            // idle must not be handed out.
            let fresh = fresh && idle.conn.probe_idle().await;
            if fresh {
                reservation.registered = true;
                return Ok(AsyncPooledConnection {
                    task: Some(ReturnTask {
                        conn: Some(idle.conn),
                        reservation,
                        config: self.config.clone(),
                        uses: idle.uses + 1,
                        born: idle.born,
                    }),
                });
            }
            reservation.retire(idle.conn).await?;
        }
        {
            let mut state = self.state.lock().expect("pool lock");
            if state.closed {
                return Err(NzError::Closed("pool is closed".into()));
            }
            state.total += 1;
            reservation.registered = true;
        }
        let conn = Client::connect(&self.config.connection).await?;
        if self.state.lock().expect("pool lock").closed {
            conn.stop();
            return Err(NzError::Closed("pool closed while connecting".into()));
        }
        Ok(AsyncPooledConnection {
            task: Some(ReturnTask {
                conn: Some(conn),
                reservation,
                config: self.config.clone(),
                uses: 1,
                born: Instant::now(),
            }),
        })
    }

    pub async fn query(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<QueryResult> {
        let holder = self.get().await?;
        let result = holder.query_multi(sql, params).await;
        holder.release().await;
        result
    }
    pub async fn query_rows(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<Vec<Row>> {
        Ok(self.query(sql, params).await?.into_rows())
    }
    pub async fn execute(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<i64> {
        let holder = self.get().await?;
        let result = holder.execute(sql, params).await;
        holder.release().await;
        result
    }
    pub async fn end(&self) {
        self.permits.close();
        let idle = {
            let mut state = self.state.lock().expect("pool lock");
            state.closed = true;
            let idle = std::mem::take(&mut state.idle);
            state.total = state.total.saturating_sub(idle.len());
            idle
        };
        for idle in idle {
            let _ = idle.conn.close().await;
        }
    }
}

impl Drop for AsyncNzPool {
    fn drop(&mut self) {
        self.permits.close();
        let mut state = self.state.lock().expect("pool lock");
        state.closed = true;
        let idle_count = state.idle.len();
        for idle in state.idle.drain(..) {
            idle.conn.stop();
        }
        // Checked-out reservations decrement their own slots when returned.
        state.total = state.total.saturating_sub(idle_count);
    }
}

struct ReturnTask {
    conn: Option<Client>,
    reservation: Reservation,
    config: Arc<AsyncNzPoolConfig>,
    uses: u64,
    born: Instant,
}
impl Drop for ReturnTask {
    fn drop(&mut self) {
        if let Some(conn) = &self.conn {
            conn.stop();
        }
    }
}
impl ReturnTask {
    async fn finish(mut self) {
        let Some(conn) = self.conn.as_ref() else {
            return;
        };
        // A dropped consumer may still be cancelling. The serialized rollback
        // also waits for that cleanup before the slot becomes idle again.
        let mut destroy = conn.is_closed()
            || self.config.max_uses.is_some_and(|limit| self.uses >= limit)
            || self
                .config
                .max_lifetime
                .is_some_and(|limit| self.born.elapsed() >= limit);
        if !destroy && self.config.rollback_on_release {
            destroy = !matches!(
                tokio::time::timeout(Duration::from_secs(5), conn.batch_execute("ROLLBACK")).await,
                Ok(Ok(()))
            );
        }
        let retained = {
            let mut state = self.reservation.state.lock().expect("pool lock");
            if !destroy && !state.closed {
                state.idle.push(IdleConnection {
                    conn: self.conn.take().expect("connected holder"),
                    idle_since: Instant::now(),
                    uses: self.uses,
                    born: self.born,
                });
                self.reservation.registered = false;
                true
            } else {
                false
            }
        };
        if !retained {
            if let Some(conn) = self.conn.take() {
                let _ = conn.close().await;
            }
        }
    }
}

/// Exclusive pool lease. It exposes operations, never a cloneable underlying client.
pub struct AsyncPooledConnection {
    task: Option<ReturnTask>,
}
impl AsyncPooledConnection {
    fn conn(&self) -> &Client {
        self.task
            .as_ref()
            .and_then(|task| task.conn.as_ref())
            .expect("released lease")
    }
    pub async fn query_multi(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<QueryResult> {
        let params: Vec<&(dyn ToSql + Sync)> =
            params.iter().map(|p| *p as &(dyn ToSql + Sync)).collect();
        self.conn().query_multi(sql, &params).await
    }
    pub async fn query(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<Vec<Row>> {
        let result = self.query_multi(sql, params).await?;
        if result.result_sets.len() > 1 {
            return Err(NzError::Config(
                "use query_multi for multiple result sets".into(),
            ));
        }
        Ok(result.into_rows())
    }
    pub async fn execute(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<i64> {
        let params: Vec<&(dyn ToSql + Sync)> =
            params.iter().map(|p| *p as &(dyn ToSql + Sync)).collect();
        self.conn().execute(sql, &params).await
    }
    pub async fn batch_execute(&self, sql: &str) -> NzResult<()> {
        self.conn().batch_execute(sql).await
    }
    pub async fn cancel(&self) -> NzResult<()> {
        self.conn().cancel().await
    }
    pub fn in_transaction(&self) -> bool {
        self.conn().in_transaction()
    }
    pub async fn release(mut self) {
        if let Some(task) = self.task.take() {
            // Detaching cleanup makes release itself cancellation safe.
            let join = tokio::spawn(task.finish());
            let _ = join.await;
        }
    }
}
impl Drop for AsyncPooledConnection {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(task.finish());
            }
            // Without a running runtime ReturnTask drops the connection and reservation.
        }
    }
}

/// Pool for the standard native API (`query` returns rows, `query_multi` returns sets).
pub struct Pool {
    inner: AsyncNzPool,
}
pub type PoolConfig = AsyncNzPoolConfig;
impl Pool {
    pub fn new(config: PoolConfig) -> NzResult<Self> {
        Ok(Self {
            inner: AsyncNzPool::new(config)?,
        })
    }
    pub async fn connect(config: PoolConfig) -> NzResult<Self> {
        Ok(Self {
            inner: AsyncNzPool::connect(config).await?,
        })
    }
    pub async fn get(&self) -> NzResult<AsyncPooledConnection> {
        self.inner.get().await
    }
    pub async fn query(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<Vec<Row>> {
        let holder = self.get().await?;
        let result = holder.query(sql, params).await;
        holder.release().await;
        result
    }
    pub async fn query_multi(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<QueryResult> {
        self.inner.query(sql, params).await
    }
    pub async fn execute(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<i64> {
        self.inner.execute(sql, params).await
    }
    pub async fn total_count(&self) -> usize {
        self.inner.total_count().await
    }
    pub async fn idle_count(&self) -> usize {
        self.inner.idle_count().await
    }
    pub async fn close(&self) {
        self.inner.end().await
    }
}
impl AsyncPooledConnection {
    pub fn metadata(&self) -> crate::AsyncMetadata<'_> {
        self.conn().metadata()
    }
    /// The returned stream borrows this lease through the last row or cancellation.
    pub async fn query_stream(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> NzResult<PooledRowStream<'_>> {
        Ok(PooledRowStream {
            stream: self.conn().query_stream(sql, params).await?,
            _lease: self,
        })
    }
    pub async fn transaction(&mut self) -> NzResult<PooledTransaction<'_>> {
        let transaction = self.conn().transaction().await?;
        Ok(PooledTransaction {
            transaction: Some(transaction),
            _lease: self,
        })
    }
}
pub struct PooledRowStream<'a> {
    stream: crate::RowStream,
    _lease: &'a AsyncPooledConnection,
}
impl futures_core::Stream for PooledRowStream<'_> {
    type Item = NzResult<Row>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_next(cx)
    }
}
pub struct PooledTransaction<'a> {
    transaction: Option<crate::Transaction>,
    _lease: &'a mut AsyncPooledConnection,
}
impl PooledTransaction<'_> {
    pub async fn query(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> NzResult<Vec<Row>> {
        self.transaction
            .as_ref()
            .expect("active transaction")
            .query(sql, params)
            .await
    }
    pub async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> NzResult<i64> {
        self.transaction
            .as_ref()
            .expect("active transaction")
            .execute(sql, params)
            .await
    }
    pub async fn commit(mut self) -> NzResult<()> {
        self.transaction
            .take()
            .expect("active transaction")
            .commit()
            .await
    }
    pub async fn rollback(mut self) -> NzResult<()> {
        self.transaction
            .take()
            .expect("active transaction")
            .rollback()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> AsyncNzPoolConfig {
        AsyncNzPoolConfig::new(NzConnectionConfig::new("localhost", "db", "u", "p"))
    }

    #[test]
    fn rejects_invalid_limits() {
        let mut value = config();
        value.max = 0;
        assert!(AsyncNzPool::new(value).is_err());

        let mut value = config();
        value.min = 2;
        value.max = 1;
        assert!(AsyncNzPool::new(value).is_err());

        let mut value = config();
        value.max_uses = Some(0);
        assert!(AsyncNzPool::new(value).is_err());
    }

    #[test]
    fn end_closes_empty_pool_and_rejects_future_checkout() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = AsyncNzPool::new(config()).unwrap();
            assert_eq!(pool.total_count().await, 0);
            assert_eq!(pool.idle_count().await, 0);
            pool.end().await;
            assert!(matches!(pool.get().await, Err(NzError::Closed(_))));
        });
    }

    #[test]
    fn async_pool_config_debug_redacts_nested_password() {
        let config = AsyncNzPoolConfig::new(NzConnectionConfig::new(
            "localhost",
            "db",
            "user",
            "pool-secret",
        ));
        let debug = format!("{config:?}");

        assert!(!debug.contains("pool-secret"));
        assert!(debug.contains("[REDACTED]"));
    }
}
