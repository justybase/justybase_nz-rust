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

//! Tokio-facing API for the stateful Netezza simple-query protocol.

use crate::cancel::send_cancel;
use crate::config::NzConnectionConfig;
use crate::connection::{NzConnection, QueryResult, QueryStreamSink, Row, StreamSummary};
use crate::error::{NzError, NzResult};
use crate::metadata::{
    NzColumnInfo, NzConstraintInfo, NzDatabaseInfo, NzDistributionKeyInfo, NzFunctionInfo,
    NzObjectDetailInfo, NzObjectInfo, NzOrganizeKeyInfo, NzProcedureInfo, NzSessionInfo,
    NzSynonymInfo, NzTableInfo, NzTableSizeInfo, NzViewInfo,
};
use crate::types::value::{NzValue, ToSql};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct ActiveOperation {
    generation: u64,
    config: NzConnectionConfig,
    backend_process_id: i32,
    backend_secret_key: i32,
}

struct AsyncControl {
    active: Mutex<Option<ActiveOperation>>,
    next_generation: AtomicU64,
    closing: AtomicBool,
    closed: AtomicBool,
}

impl AsyncControl {
    fn new() -> Self {
        Self {
            active: Mutex::new(None),
            next_generation: AtomicU64::new(0),
            closing: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        }
    }

    fn begin(self: &Arc<Self>, conn: &NzConnection) -> NzResult<ActiveGuard> {
        if self.closing.load(Ordering::Acquire) || self.closed.load(Ordering::Acquire) {
            return Err(NzError::Closed("connection is closing".into()));
        }
        let generation = self.next_generation.fetch_add(1, Ordering::AcqRel) + 1;
        let mut active = self
            .active
            .lock()
            .map_err(|_| NzError::Closed("async control lock poisoned".into()))?;
        *active = Some(ActiveOperation {
            generation,
            config: conn.config().clone(),
            backend_process_id: conn.backend_process_id(),
            backend_secret_key: conn.backend_secret_key(),
        });
        Ok(ActiveGuard {
            control: self.clone(),
            generation,
        })
    }

    fn cancel(&self) -> NzResult<()> {
        // Hold the control lock while delivering the packet. This closes the
        // stale-cancel race: the worker cannot clear this operation and start
        // another generation until the cancel request has been sent.
        let active = self
            .active
            .lock()
            .map_err(|_| NzError::Closed("async control lock poisoned".into()))?;
        let Some(operation) = active.as_ref() else {
            return Ok(());
        };
        send_cancel(
            &operation.config,
            operation.backend_process_id,
            operation.backend_secret_key,
        )
    }

    fn finish(&self, generation: u64) {
        if let Ok(mut active) = self.active.lock() {
            if active
                .as_ref()
                .is_some_and(|operation| operation.generation == generation)
            {
                *active = None;
            }
        }
    }
}

struct ActiveGuard {
    control: Arc<AsyncControl>,
    generation: u64,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.control.finish(self.generation);
    }
}

/// Legacy async-compatible connection with serialized operations.
///
/// New code that wants native Tokio socket I/O should use
/// [`crate::native_async::Client`]. This facade remains for the richer
/// cancellation, metadata and sink APIs built around the mature blocking
/// protocol implementation.
#[derive(Clone)]
pub struct AsyncNzConnection {
    inner: Arc<Mutex<Option<NzConnection>>>,
    control: Arc<AsyncControl>,
}

impl AsyncNzConnection {
    pub async fn connect(config: &NzConnectionConfig) -> NzResult<Self> {
        let config = config.clone();
        let conn = tokio::task::spawn_blocking(move || NzConnection::connect(&config))
            .await
            .map_err(|e| NzError::Closed(format!("connection task failed: {e}")))??;
        Ok(Self {
            inner: Arc::new(Mutex::new(Some(conn))),
            control: Arc::new(AsyncControl::new()),
        })
    }

    pub async fn connect_with_str(connection_string: &str) -> NzResult<Self> {
        let config = crate::parse_connection_string(connection_string)?;
        Self::connect(&config).await
    }

    pub async fn query(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<QueryResult> {
        let values = params.iter().map(|p| p.to_nz_value()).collect::<Vec<_>>();
        self.query_values(sql, values).await
    }

    pub async fn query_values(&self, sql: &str, params: Vec<NzValue>) -> NzResult<QueryResult> {
        let sql = sql.to_owned();
        self.with_connection(move |conn| conn.query_values(&sql, &params))
            .await
    }

    /// Execute a buffered query with an explicit wall-clock timeout.
    pub async fn query_with_timeout(
        &self,
        sql: &str,
        params: &[&dyn ToSql],
        timeout: Option<Duration>,
    ) -> NzResult<QueryResult> {
        let sql = sql.to_owned();
        let values = params.iter().map(|p| p.to_nz_value()).collect::<Vec<_>>();
        self.with_connection(move |conn| conn.query_values_with_timeout(&sql, &values, timeout))
            .await
    }

    pub async fn query_rows(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<Vec<Row>> {
        Ok(self.query(sql, params).await?.rows().to_vec())
    }

    pub async fn query_one(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<Row> {
        self.query_rows(sql, params)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| NzError::Config("query_one: no rows returned".into()))
    }

    pub async fn query_opt(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<Option<Row>> {
        let rows = self.query_rows(sql, params).await?;
        match rows.as_slice() {
            [] => Ok(None),
            [row] => Ok(Some(row.clone())),
            _ => Err(NzError::Config(format!(
                "query_opt: expected at most one row, got {}",
                rows.len()
            ))),
        }
    }

    pub async fn execute(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<i64> {
        let sql = sql.to_owned();
        let values = params.iter().map(|p| p.to_nz_value()).collect::<Vec<_>>();
        self.with_connection(move |conn| conn.execute_values(&sql, &values))
            .await
    }

    /// Execute a non-query with an explicit wall-clock timeout.
    pub async fn execute_with_timeout(
        &self,
        sql: &str,
        params: &[&dyn ToSql],
        timeout: Option<Duration>,
    ) -> NzResult<i64> {
        let sql = sql.to_owned();
        let values = params.iter().map(|p| p.to_nz_value()).collect::<Vec<_>>();
        self.with_connection(move |conn| conn.execute_values_with_timeout(&sql, &values, timeout))
            .await
    }

    pub async fn batch_execute(&self, sql: &str) -> NzResult<()> {
        let sql = sql.to_owned();
        self.with_connection(move |conn| conn.batch_execute(&sql))
            .await
    }

    /// Execute a query on the blocking worker and forward rows to an owned
    /// sink without buffering the result set. The sink is returned so callers
    /// can inspect counters or collected application state after completion.
    pub async fn execute_stream<S>(
        &self,
        sql: &str,
        params: &[&dyn ToSql],
        mut sink: S,
    ) -> NzResult<(StreamSummary, S)>
    where
        S: QueryStreamSink + Send + 'static,
    {
        let sql = sql.to_owned();
        let values = params.iter().map(|p| p.to_nz_value()).collect::<Vec<_>>();
        self.with_connection(move |conn| {
            let summary = conn.execute_stream_values(&sql, &values, &mut sink)?;
            Ok((summary, sink))
        })
        .await
    }

    /// Stream rows with an explicit wall-clock timeout.
    pub async fn execute_stream_with_timeout<S>(
        &self,
        sql: &str,
        params: &[&dyn ToSql],
        timeout: Option<Duration>,
        mut sink: S,
    ) -> NzResult<(StreamSummary, S)>
    where
        S: QueryStreamSink + Send + 'static,
    {
        let sql = sql.to_owned();
        let values = params.iter().map(|p| p.to_nz_value()).collect::<Vec<_>>();
        self.with_connection(move |conn| {
            let summary =
                conn.execute_stream_values_with_timeout(&sql, &values, timeout, &mut sink)?;
            Ok((summary, sink))
        })
        .await
    }

    pub async fn begin_transaction(&self) -> NzResult<()> {
        self.batch_execute("BEGIN").await
    }
    pub async fn commit(&self) -> NzResult<()> {
        self.batch_execute("COMMIT").await
    }
    pub async fn rollback(&self) -> NzResult<()> {
        self.batch_execute("ROLLBACK").await
    }

    pub async fn change_database(&self, database: &str) -> NzResult<()> {
        let database = database.to_owned();
        self.with_connection(move |conn| conn.change_database(&database))
            .await
    }
    pub async fn cancel(&self) -> NzResult<()> {
        let control = self.control.clone();
        tokio::task::spawn_blocking(move || control.cancel())
            .await
            .map_err(|e| NzError::Closed(format!("cancel task failed: {e}")))?
    }

    // Metadata helpers intentionally mirror `NzMetadata` while keeping the
    // async facade free of a long-lived mutable borrow.
    pub async fn schemas(&self) -> NzResult<Vec<String>> {
        self.with_connection(|conn| conn.metadata().schemas()).await
    }

    pub async fn databases(&self) -> NzResult<Vec<NzDatabaseInfo>> {
        self.with_connection(|conn| conn.metadata().databases())
            .await
    }

    pub async fn tables(
        &self,
        schema: Option<&str>,
        pattern: Option<&str>,
    ) -> NzResult<Vec<NzTableInfo>> {
        let schema = schema.map(str::to_owned);
        let pattern = pattern.map(str::to_owned);
        self.with_connection(move |conn| {
            conn.metadata()
                .tables(schema.as_deref(), pattern.as_deref())
        })
        .await
    }

    pub async fn columns(&self, table: &str, schema: Option<&str>) -> NzResult<Vec<NzColumnInfo>> {
        let table = table.to_owned();
        let schema = schema.map(str::to_owned);
        self.with_connection(move |conn| conn.metadata().columns(&table, schema.as_deref()))
            .await
    }

    pub async fn views(&self, schema: Option<&str>) -> NzResult<Vec<NzViewInfo>> {
        let schema = schema.map(str::to_owned);
        self.with_connection(move |conn| conn.metadata().views(schema.as_deref()))
            .await
    }

    pub async fn procedures(&self, schema: Option<&str>) -> NzResult<Vec<NzProcedureInfo>> {
        let schema = schema.map(str::to_owned);
        self.with_connection(move |conn| conn.metadata().procedures(schema.as_deref()))
            .await
    }

    pub async fn distribution_key(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> NzResult<Vec<String>> {
        let table = table.to_owned();
        let schema = schema.map(str::to_owned);
        self.with_connection(move |conn| {
            conn.metadata().distribution_key(&table, schema.as_deref())
        })
        .await
    }

    pub async fn table_sizes(&self, schema: Option<&str>) -> NzResult<Vec<NzTableSizeInfo>> {
        let schema = schema.map(str::to_owned);
        self.with_connection(move |conn| conn.metadata().table_sizes(schema.as_deref()))
            .await
    }

    pub async fn sessions(&self) -> NzResult<Vec<NzSessionInfo>> {
        self.with_connection(|conn| conn.metadata().sessions())
            .await
    }

    pub async fn search_objects(
        &self,
        pattern: &str,
        schema: Option<&str>,
    ) -> NzResult<Vec<NzObjectInfo>> {
        let pattern = pattern.to_owned();
        let schema = schema.map(str::to_owned);
        self.with_connection(move |conn| conn.metadata().objects(&pattern, schema.as_deref()))
            .await
    }

    pub async fn functions(&self, schema: Option<&str>) -> NzResult<Vec<NzFunctionInfo>> {
        let schema = schema.map(str::to_owned);
        self.with_connection(move |conn| conn.metadata().functions(schema.as_deref()))
            .await
    }

    pub async fn synonyms(&self, schema: Option<&str>) -> NzResult<Vec<NzSynonymInfo>> {
        let schema = schema.map(str::to_owned);
        self.with_connection(move |conn| conn.metadata().synonyms(schema.as_deref()))
            .await
    }

    pub async fn constraints(&self, schema: Option<&str>) -> NzResult<Vec<NzConstraintInfo>> {
        let schema = schema.map(str::to_owned);
        self.with_connection(move |conn| conn.metadata().constraints(schema.as_deref()))
            .await
    }

    pub async fn all_distribution_keys(
        &self,
        schema: Option<&str>,
    ) -> NzResult<Vec<NzDistributionKeyInfo>> {
        let schema = schema.map(str::to_owned);
        self.with_connection(move |conn| conn.metadata().all_distribution_keys(schema.as_deref()))
            .await
    }

    pub async fn organize_keys(&self, schema: Option<&str>) -> NzResult<Vec<NzOrganizeKeyInfo>> {
        let schema = schema.map(str::to_owned);
        self.with_connection(move |conn| conn.metadata().organize_keys(schema.as_deref()))
            .await
    }

    pub async fn object_details(&self, schema: Option<&str>) -> NzResult<Vec<NzObjectDetailInfo>> {
        let schema = schema.map(str::to_owned);
        self.with_connection(move |conn| conn.metadata().object_details(schema.as_deref()))
            .await
    }

    pub async fn search_objects_detailed(
        &self,
        pattern: &str,
        schema: Option<&str>,
    ) -> NzResult<Vec<NzObjectDetailInfo>> {
        let pattern = pattern.to_owned();
        let schema = schema.map(str::to_owned);
        self.with_connection(move |conn| {
            conn.metadata()
                .search_objects_detailed(&pattern, schema.as_deref())
        })
        .await
    }

    pub async fn close(&self) {
        self.control.closing.store(true, Ordering::Release);
        let _ = self.cancel().await;
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(mut conn) = guard.take() {
                conn.close();
            }
        }
        self.control.closed.store(true, Ordering::Release);
    }

    pub async fn is_closed(&self) -> bool {
        self.inner
            .lock()
            .map(|g| g.as_ref().map(|c| c.is_closed()).unwrap_or(true))
            .unwrap_or(true)
    }

    pub async fn in_transaction(&self) -> bool {
        self.inner
            .lock()
            .map(|g| g.as_ref().map(|c| c.in_transaction()).unwrap_or(false))
            .unwrap_or(false)
    }

    async fn with_connection<T, F>(&self, f: F) -> NzResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut NzConnection) -> NzResult<T> + Send + 'static,
    {
        let inner = self.inner.clone();
        let control = self.control.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = inner
                .lock()
                .map_err(|_| NzError::Closed("connection lock poisoned".into()))?;
            let conn = guard
                .as_mut()
                .ok_or_else(|| NzError::Closed("connection is closed".into()))?;
            let _active = control.begin(conn)?;
            f(conn)
        })
        .await
        .map_err(|e| NzError::Closed(format!("connection task failed: {e}")))?
    }
}

impl Drop for AsyncNzConnection {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            self.control.closing.store(true, Ordering::Release);
            let _ = self.control.cancel();
            if let Ok(mut guard) = self.inner.lock() {
                if let Some(mut conn) = guard.take() {
                    conn.close();
                }
            }
            self.control.closed.store(true, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation(generation: u64) -> ActiveOperation {
        ActiveOperation {
            generation,
            config: NzConnectionConfig::default(),
            backend_process_id: 5857,
            backend_secret_key: -2_092_017_624,
        }
    }

    #[test]
    fn cancel_without_active_operation_is_a_noop() {
        let control = AsyncControl::new();
        assert!(control.cancel().is_ok());
    }

    #[test]
    fn active_guard_clears_only_its_generation() {
        let control = Arc::new(AsyncControl::new());
        *control.active.lock().unwrap() = Some(operation(7));

        control.finish(6);
        assert_eq!(
            control.active.lock().unwrap().as_ref().unwrap().generation,
            7
        );

        let guard = ActiveGuard {
            control: control.clone(),
            generation: 7,
        };
        drop(guard);
        assert!(control.active.lock().unwrap().is_none());
    }

    #[test]
    fn stale_guard_cannot_clear_new_operation() {
        let control = Arc::new(AsyncControl::new());
        *control.active.lock().unwrap() = Some(operation(8));

        let stale_guard = ActiveGuard {
            control: control.clone(),
            generation: 7,
        };
        drop(stale_guard);
        assert_eq!(
            control.active.lock().unwrap().as_ref().unwrap().generation,
            8
        );
    }
}
