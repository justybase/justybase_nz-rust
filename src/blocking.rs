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

//! Blocking client with the same query semantics as the native async client.

use crate::config::NzConnectionConfig;
#[cfg(feature = "compat")]
use crate::connection::NzConnection;
use crate::connection::{QueryResult, Row};
#[cfg(feature = "compat")]
use crate::connection::{QueryStreamSink, StreamSummary};
use crate::error::{NzError, NzResult};
#[cfg(feature = "compat")]
use crate::types::value::NzValue;
use crate::types::value::ToSql;

/// Synchronous counterpart of [`crate::native_async::Client`].
#[cfg(feature = "compat")]
pub struct BlockingClient {
    inner: NzConnection,
}

#[cfg(feature = "compat")]
impl BlockingClient {
    pub fn connect(config: &NzConnectionConfig) -> NzResult<Self> {
        Ok(Self {
            inner: NzConnection::connect(config)?,
        })
    }

    pub fn connect_with_str(connection_string: &str) -> NzResult<Self> {
        let config = crate::parse_connection_string(connection_string)?;
        Self::connect(&config)
    }

    pub fn query_multi(
        &mut self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> NzResult<QueryResult> {
        let values: Vec<NzValue> = params.iter().map(|value| value.to_nz_value()).collect();
        self.inner.query_values(sql, &values)
    }

    pub fn query(&mut self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> NzResult<Vec<Row>> {
        let result = self.query_multi(sql, params)?;
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

    pub fn query_one(&mut self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> NzResult<Row> {
        let rows = self.query(sql, params)?;
        if rows.len() != 1 {
            return Err(NzError::Config(format!(
                "query_one: expected one row, got {}",
                rows.len()
            )));
        }
        Ok(rows.into_iter().next().expect("one row checked above"))
    }

    pub fn query_opt(
        &mut self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> NzResult<Option<Row>> {
        let rows = self.query(sql, params)?;
        match rows.len() {
            0 => Ok(None),
            1 => Ok(rows.into_iter().next()),
            count => Err(NzError::Config(format!(
                "query_opt: expected at most one row, got {count}"
            ))),
        }
    }

    pub fn execute(&mut self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> NzResult<i64> {
        let values: Vec<NzValue> = params.iter().map(|value| value.to_nz_value()).collect();
        self.inner.execute_values(sql, &values)
    }

    pub fn batch_execute(&mut self, sql: &str) -> NzResult<()> {
        self.inner.batch_execute(sql)
    }

    pub fn execute_stream<S: QueryStreamSink>(
        &mut self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
        sink: &mut S,
    ) -> NzResult<StreamSummary> {
        let values: Vec<NzValue> = params.iter().map(|value| value.to_nz_value()).collect();
        self.inner.execute_stream_values(sql, &values, sink)
    }

    /// Run a closure in a transaction with rollback-on-error semantics.
    pub fn transaction<T>(&mut self, body: impl FnOnce(&mut Self) -> NzResult<T>) -> NzResult<T> {
        self.batch_execute("BEGIN")?;
        match body(self) {
            Ok(value) => {
                self.batch_execute("COMMIT")?;
                Ok(value)
            }
            Err(error) => {
                let _ = self.batch_execute("ROLLBACK");
                Err(error)
            }
        }
    }

    pub fn close(&mut self) {
        self.inner.close();
    }

    pub fn inner(&self) -> &NzConnection {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut NzConnection {
        &mut self.inner
    }
}

/// A transaction exclusively borrowing the blocking client. Drop rolls back.
#[cfg(feature = "compat")]
pub struct LegacyTransaction<'a> {
    client: &'a mut BlockingClient,
    finished: bool,
}
#[cfg(feature = "compat")]
impl BlockingClient {
    pub fn begin_transaction(&mut self) -> NzResult<LegacyTransaction<'_>> {
        if self.inner.in_transaction() {
            return Err(NzError::Config("nested transaction is unsupported".into()));
        }
        self.batch_execute("BEGIN")?;
        Ok(LegacyTransaction {
            client: self,
            finished: false,
        })
    }
}
#[cfg(feature = "compat")]
impl LegacyTransaction<'_> {
    pub fn commit(mut self) -> NzResult<()> {
        self.client.batch_execute("COMMIT")?;
        self.finished = true;
        Ok(())
    }
    pub fn rollback(mut self) -> NzResult<()> {
        self.client.batch_execute("ROLLBACK")?;
        self.finished = true;
        Ok(())
    }
}
#[cfg(feature = "compat")]
impl std::ops::Deref for LegacyTransaction<'_> {
    type Target = BlockingClient;
    fn deref(&self) -> &Self::Target {
        self.client
    }
}
#[cfg(feature = "compat")]
impl std::ops::DerefMut for LegacyTransaction<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client
    }
}
#[cfg(feature = "compat")]
impl Drop for LegacyTransaction<'_> {
    fn drop(&mut self) {
        if !self.finished
            && self
                .client
                .inner
                .execute_values_with_timeout(
                    "ROLLBACK",
                    &[],
                    Some(std::time::Duration::from_secs(5)),
                )
                .is_err()
        {
            self.client.close();
        }
    }
}

/// Blocking client using the same bounded protocol driver as [`crate::Client`].
/// Use outside a Tokio runtime; asynchronous callers should use `crate::Client`.
pub struct Client {
    runtime: std::sync::Arc<tokio::runtime::Runtime>,
    client: crate::Client,
}
impl Client {
    pub fn connect(config: &NzConnectionConfig) -> NzResult<Self> {
        let runtime = std::sync::Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()?,
        );
        let client = runtime.block_on(crate::Client::connect(config))?;
        Ok(Self { runtime, client })
    }
    pub fn connect_with_str(uri: &str) -> NzResult<Self> {
        Self::connect(&crate::parse_connection_string(uri)?)
    }
    pub fn query(&mut self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> NzResult<Vec<Row>> {
        self.runtime.block_on(self.client.query(sql, params))
    }
    pub fn query_multi(
        &mut self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> NzResult<QueryResult> {
        self.runtime.block_on(self.client.query_multi(sql, params))
    }
    pub fn query_one(&mut self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> NzResult<Row> {
        self.runtime.block_on(self.client.query_one(sql, params))
    }
    pub fn query_opt(
        &mut self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> NzResult<Option<Row>> {
        self.runtime.block_on(self.client.query_opt(sql, params))
    }
    pub fn execute(&mut self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> NzResult<i64> {
        self.runtime.block_on(self.client.execute(sql, params))
    }
    pub fn batch_execute(&mut self, sql: &str) -> NzResult<()> {
        self.runtime.block_on(self.client.batch_execute(sql))
    }
    pub fn query_with_options(
        &mut self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
        options: crate::QueryOptions,
    ) -> NzResult<Vec<Row>> {
        self.runtime
            .block_on(self.client.query_with_options(sql, params, options))
    }
    /// Iterator keeps the session exclusively borrowed; drop cancels and drains.
    pub fn query_iter(
        &mut self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> NzResult<RowIter<'_>> {
        let stream = self
            .runtime
            .block_on(self.client.query_stream(sql, params))?;
        Ok(RowIter {
            stream: Some(stream),
            runtime: self.runtime.clone(),
            client: &self.client,
        })
    }
    pub fn transaction(&mut self) -> NzResult<Transaction<'_>> {
        let transaction = self.runtime.block_on(self.client.transaction())?;
        Ok(Transaction {
            transaction: Some(transaction),
            client: self,
        })
    }
    pub fn is_closed(&self) -> bool {
        self.client.is_closed()
    }
    pub fn close(&mut self) -> NzResult<()> {
        self.runtime.block_on(self.client.close())
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.runtime.block_on(self.client.close());
    }
}

/// Bounded synchronous row iterator, including checked terminal errors.
pub struct RowIter<'a> {
    stream: Option<crate::RowStream>,
    runtime: std::sync::Arc<tokio::runtime::Runtime>,
    client: &'a crate::Client,
}
impl Iterator for RowIter<'_> {
    type Item = NzResult<Row>;
    fn next(&mut self) -> Option<Self::Item> {
        use futures_core::Stream;
        let stream = self.stream.as_mut()?;
        self.runtime.block_on(std::future::poll_fn(|cx| {
            std::pin::Pin::new(&mut *stream).poll_next(cx)
        }))
    }
}
impl Drop for RowIter<'_> {
    fn drop(&mut self) {
        self.stream.take();
        let result = self.runtime.block_on(tokio_cleanup_barrier(self.client));
        if result.is_err() {
            self.client.stop();
        }
    }
}
async fn tokio_cleanup_barrier(client: &crate::Client) -> NzResult<()> {
    tokio::time::timeout(std::time::Duration::from_secs(5), client.batch_execute(""))
        .await
        .map_err(|_| NzError::Timeout("blocking cleanup timeout".into()))?
}

/// Transaction sharing the native driver's exclusive guard and rollback semantics.
pub struct Transaction<'a> {
    transaction: Option<crate::Transaction>,
    client: &'a mut Client,
}
impl Transaction<'_> {
    pub fn query(&mut self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> NzResult<Vec<Row>> {
        self.client.runtime.block_on(
            self.transaction
                .as_ref()
                .expect("active transaction")
                .query(sql, params),
        )
    }
    pub fn query_multi(
        &mut self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> NzResult<QueryResult> {
        self.client.runtime.block_on(
            self.transaction
                .as_ref()
                .expect("active transaction")
                .query_multi(sql, params),
        )
    }
    pub fn execute(&mut self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> NzResult<i64> {
        self.client.runtime.block_on(
            self.transaction
                .as_ref()
                .expect("active transaction")
                .execute(sql, params),
        )
    }
    pub fn batch_execute(&mut self, sql: &str) -> NzResult<()> {
        self.client.runtime.block_on(
            self.transaction
                .as_ref()
                .expect("active transaction")
                .batch_execute(sql),
        )
    }
    pub fn commit(mut self) -> NzResult<()> {
        self.client.runtime.block_on(
            self.transaction
                .take()
                .expect("active transaction")
                .commit(),
        )
    }
    pub fn rollback(mut self) -> NzResult<()> {
        self.client.runtime.block_on(
            self.transaction
                .take()
                .expect("active transaction")
                .rollback(),
        )
    }
}
impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        let entered = self.client.runtime.enter();
        let transaction = self.transaction.take();
        let active = transaction.is_some();
        drop(transaction);
        drop(entered);
        if active
            && self
                .client
                .runtime
                .block_on(tokio_cleanup_barrier(&self.client.client))
                .is_err()
        {
            self.client.client.stop();
        }
    }
}

/// Blocking pool sharing the native semaphore and cancellation-safe lease cleanup.
pub struct Pool {
    runtime: std::sync::Arc<tokio::runtime::Runtime>,
    pool: crate::Pool,
}
pub type PoolConfig = crate::PoolConfig;
impl Pool {
    pub fn connect(config: PoolConfig) -> NzResult<Self> {
        let runtime = std::sync::Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()?,
        );
        let pool = runtime.block_on(crate::Pool::connect(config))?;
        Ok(Self { runtime, pool })
    }
    pub fn get(&self) -> NzResult<PooledConnection> {
        let lease = self.runtime.block_on(self.pool.get())?;
        Ok(PooledConnection {
            runtime: self.runtime.clone(),
            lease: Some(lease),
        })
    }
    pub fn query(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<Vec<Row>> {
        self.runtime.block_on(self.pool.query(sql, params))
    }
    pub fn query_multi(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<QueryResult> {
        self.runtime.block_on(self.pool.query_multi(sql, params))
    }
    pub fn execute(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<i64> {
        self.runtime.block_on(self.pool.execute(sql, params))
    }
    pub fn close(&self) {
        self.runtime.block_on(self.pool.close());
    }
    pub fn total_count(&self) -> usize {
        self.runtime.block_on(self.pool.total_count())
    }
    pub fn idle_count(&self) -> usize {
        self.runtime.block_on(self.pool.idle_count())
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        self.close();
    }
}
/// Exclusive blocking pool lease. Drop schedules cleanup on its owning runtime.
pub struct PooledConnection {
    runtime: std::sync::Arc<tokio::runtime::Runtime>,
    lease: Option<crate::async_pool::AsyncPooledConnection>,
}
impl PooledConnection {
    pub fn query(&mut self, sql: &str, params: &[&dyn ToSql]) -> NzResult<Vec<Row>> {
        self.runtime.block_on(
            self.lease
                .as_ref()
                .expect("active lease")
                .query(sql, params),
        )
    }
    pub fn query_multi(&mut self, sql: &str, params: &[&dyn ToSql]) -> NzResult<QueryResult> {
        self.runtime.block_on(
            self.lease
                .as_ref()
                .expect("active lease")
                .query_multi(sql, params),
        )
    }
    pub fn execute(&mut self, sql: &str, params: &[&dyn ToSql]) -> NzResult<i64> {
        self.runtime.block_on(
            self.lease
                .as_ref()
                .expect("active lease")
                .execute(sql, params),
        )
    }
    pub fn batch_execute(&mut self, sql: &str) -> NzResult<()> {
        self.runtime.block_on(
            self.lease
                .as_ref()
                .expect("active lease")
                .batch_execute(sql),
        )
    }
    pub fn release(mut self) {
        if let Some(lease) = self.lease.take() {
            self.runtime.block_on(lease.release());
        }
    }
}
impl Drop for PooledConnection {
    fn drop(&mut self) {
        let _entered = self.runtime.enter();
        self.lease.take();
    }
}
