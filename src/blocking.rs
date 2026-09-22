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
use crate::connection::{NzConnection, QueryResult, QueryStreamSink, Row, StreamSummary};
use crate::error::{NzError, NzResult};
use crate::types::value::{NzValue, ToSql};

/// Synchronous counterpart of [`crate::native_async::Client`].
pub struct BlockingClient {
    inner: NzConnection,
}

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
        self.query(sql, params)?
            .into_iter()
            .next()
            .ok_or_else(|| NzError::Config("query_one: no rows returned".into()))
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
        Ok(self.query_multi(sql, params)?.rows_affected)
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
