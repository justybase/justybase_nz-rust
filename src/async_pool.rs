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

//! Tokio connection pool matching the Node driver's checkout semantics.

use crate::asynchronous::AsyncNzConnection;
use crate::config::NzConnectionConfig;
use crate::connection::{QueryResult, Row};
use crate::error::{NzError, NzResult};
use crate::types::value::ToSql;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify};

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
    conn: AsyncNzConnection,
    idle_since: Instant,
    uses: u64,
    born: Instant,
}

struct PoolState {
    idle: Vec<IdleConnection>,
    total: usize,
    closed: bool,
}

pub struct AsyncNzPool {
    config: AsyncNzPoolConfig,
    state: Arc<Mutex<PoolState>>,
    notify: Arc<Notify>,
}

impl AsyncNzPool {
    pub fn new(config: AsyncNzPoolConfig) -> NzResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            state: Arc::new(Mutex::new(PoolState {
                idle: Vec::new(),
                total: 0,
                closed: false,
            })),
            notify: Arc::new(Notify::new()),
        })
    }

    pub async fn total_count(&self) -> usize {
        self.state.lock().await.total
    }

    pub async fn idle_count(&self) -> usize {
        self.state.lock().await.idle.len()
    }

    pub async fn get(&self) -> NzResult<AsyncPooledConnection> {
        let acquire = async {
            loop {
                let (idle, needs_connect, notified) = {
                    let mut state = self.state.lock().await;
                    if state.closed {
                        return Err(NzError::Closed("pool is closed".into()));
                    }
                    if let Some(idle) = state.idle.pop() {
                        (Some(idle), false, None)
                    } else if state.total < self.config.max {
                        state.total += 1;
                        (None, true, None)
                    } else {
                        (None, false, Some(self.notify.notified()))
                    }
                };

                if let Some(idle) = idle {
                    if self.is_fresh(&idle).await {
                        return Ok(AsyncPooledConnection {
                            conn: Some(idle.conn),
                            pool: self.state.clone(),
                            notify: self.notify.clone(),
                            config: self.config.clone(),
                            uses: idle.uses + 1,
                            born: idle.born,
                        });
                    }
                    let mut state = self.state.lock().await;
                    state.total = state.total.saturating_sub(1);
                    drop(state);
                    idle.conn.close().await;
                    continue;
                }
                if needs_connect {
                    return Ok(AsyncPooledConnection::new_connecting(
                        self.state.clone(),
                        self.notify.clone(),
                        self.config.clone(),
                    ));
                }
                if let Some(wait) = notified {
                    wait.await;
                }
            }
        };

        let holder = match self.config.wait_timeout {
            Some(timeout) => tokio::time::timeout(timeout, acquire)
                .await
                .map_err(|_| NzError::Timeout("pool checkout timeout".into()))?,
            None => acquire.await,
        }?;

        if holder.conn.is_some() {
            return Ok(holder);
        }
        match AsyncNzConnection::connect(&self.config.connection).await {
            Ok(conn) => Ok(AsyncPooledConnection {
                conn: Some(conn),
                ..holder
            }),
            Err(err) => {
                let mut state = self.state.lock().await;
                state.total = state.total.saturating_sub(1);
                drop(state);
                self.notify.notify_one();
                Err(err)
            }
        }
    }

    pub async fn query(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<QueryResult> {
        let holder = self.get().await?;
        let result = holder.conn().query(sql, params).await;
        holder.release_with_error(result.as_ref().err()).await;
        result
    }

    pub async fn query_rows(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<Vec<Row>> {
        Ok(self.query(sql, params).await?.rows().to_vec())
    }

    pub async fn execute(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<i64> {
        let holder = self.get().await?;
        let result = holder.conn().execute(sql, params).await;
        holder.release_with_error(result.as_ref().err()).await;
        result
    }

    pub async fn end(&self) {
        let mut state = self.state.lock().await;
        state.closed = true;
        let idle = std::mem::take(&mut state.idle);
        state.total = state.total.saturating_sub(idle.len());
        drop(state);
        for item in idle {
            item.conn.close().await;
        }
        self.notify.notify_waiters();
    }

    async fn is_fresh(&self, idle: &IdleConnection) -> bool {
        if let Some(max_life) = self.config.max_lifetime {
            if idle.born.elapsed() >= max_life {
                return false;
            }
        }
        if let Some(max_uses) = self.config.max_uses {
            if idle.uses >= max_uses {
                return false;
            }
        }
        if !self.config.idle_timeout.is_zero()
            && idle.idle_since.elapsed() >= self.config.idle_timeout
            && self.state.lock().await.total > self.config.min
        {
            return false;
        }
        !idle.conn.is_closed().await
    }
}

pub struct AsyncPooledConnection {
    conn: Option<AsyncNzConnection>,
    pool: Arc<Mutex<PoolState>>,
    notify: Arc<Notify>,
    config: AsyncNzPoolConfig,
    uses: u64,
    born: Instant,
}

impl AsyncPooledConnection {
    fn new_connecting(
        pool: Arc<Mutex<PoolState>>,
        notify: Arc<Notify>,
        config: AsyncNzPoolConfig,
    ) -> Self {
        Self {
            conn: None,
            pool,
            notify,
            config,
            uses: 1,
            born: Instant::now(),
        }
    }

    pub fn conn(&self) -> &AsyncNzConnection {
        self.conn
            .as_ref()
            .expect("pooled connection was not connected")
    }

    pub async fn release(self) {
        self.release_with_error(None).await;
    }

    async fn release_with_error(mut self, err: Option<&NzError>) {
        let Some(conn) = self.conn.take() else {
            return;
        };
        let mut destroy = err.is_some_and(|e| !matches!(e, NzError::Database(_)));
        if self.config.max_uses.is_some_and(|max| self.uses >= max)
            || self
                .config
                .max_lifetime
                .is_some_and(|max| self.born.elapsed() >= max)
            || conn.is_closed().await
        {
            destroy = true;
        }
        if !destroy
            && self.config.rollback_on_release
            && conn.in_transaction().await
            && conn.rollback().await.is_err()
        {
            destroy = true;
        }

        let mut state = self.pool.lock().await;
        if state.closed || destroy {
            state.total = state.total.saturating_sub(1);
            drop(state);
            conn.close().await;
        } else {
            state.idle.push(IdleConnection {
                conn,
                idle_since: Instant::now(),
                uses: self.uses,
                born: self.born,
            });
            drop(state);
        }
        self.notify.notify_one();
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
}
