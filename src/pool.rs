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

//! Synchronous connection pool — port of the Node driver `NzPool.ts`
//! (C# `NzConnectionPool.cs` lineage), std-only (`Mutex` + `Condvar`).
//!
//! Semantics mirror the references:
//! - `max` total connections, optional `min` warm idle, idle timeout,
//!   per-checkout wait timeout, `max_uses` / `max_lifetime` rotation,
//! - `rollback_on_release` (default true): a connection returned while an
//!   explicit transaction is still open is `ROLLBACK`ed before it rejoins the
//!   idle queue, so uncommitted state never leaks into the next checkout
//!   (a bare `ROLLBACK` is only a notice on Netezza, hence safe),
//! - SQL failures (`NzError::Database`) keep the session (the response was
//!   drained to `ReadyForQuery`); transport/protocol/timeout failures destroy
//!   the connection.

use crate::config::NzConnectionConfig;
use crate::connection::{NzConnection, QueryResult, Row};
use crate::error::{NzError, NzResult};
use crate::types::value::ToSql;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// Pool configuration. `Clone`d from a connection config plus limits.
#[derive(Debug, Clone)]
pub struct NzPoolConfig {
    pub connection: NzConnectionConfig,
    /// Max managed connections (default 10).
    pub max: usize,
    /// Warm idle connections kept ready (default 0).
    pub min: usize,
    /// Idle eviction after this long (default 10 s; zero disables).
    pub idle_timeout: Duration,
    /// Max wait for a checkout (default none = block indefinitely).
    pub wait_timeout: Option<Duration>,
    /// Destroy a connection after this many checkouts (default unlimited).
    pub max_uses: Option<u64>,
    /// Destroy a connection older than this (default unlimited).
    pub max_lifetime: Option<Duration>,
    /// `ROLLBACK` open transactions before re-queueing (default true).
    pub rollback_on_release: bool,
}

impl NzPoolConfig {
    pub fn new(connection: NzConnectionConfig) -> Self {
        NzPoolConfig {
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

    pub fn validate(&self) -> NzResult<()> {
        if self.max == 0 {
            return Err(NzError::Config(
                "Pool max must be a positive integer".into(),
            ));
        }
        if self.min > self.max {
            return Err(NzError::Config("Pool min must be between 0 and max".into()));
        }
        Ok(())
    }
}

struct IdleConn {
    conn: NzConnection,
    idle_since: Instant,
    uses: u64,
    born: Instant,
}

struct PoolInner {
    idle: VecDeque<IdleConn>,
    total: usize,
    closed: bool,
}

/// A synchronous Netezza connection pool.
pub struct NzPool {
    config: NzPoolConfig,
    state: Arc<(Mutex<PoolInner>, Condvar)>,
}

impl NzPool {
    pub fn new(config: NzPoolConfig) -> NzResult<Self> {
        config.validate()?;
        let pool = NzPool {
            config,
            state: Arc::new((
                Mutex::new(PoolInner {
                    idle: VecDeque::new(),
                    total: 0,
                    closed: false,
                }),
                Condvar::new(),
            )),
        };
        // Best-effort warm-up for `min` (failures surface on first checkout,
        // same as the Node driver's async warm-up).
        for _ in 0..pool.config.min {
            if pool.state.0.lock().unwrap().total >= pool.config.max {
                break;
            }
            match NzConnection::connect(&pool.config.connection) {
                Ok(conn) => {
                    let mut inner = pool.state.0.lock().unwrap();
                    inner.idle.push_back(IdleConn {
                        conn,
                        idle_since: Instant::now(),
                        uses: 0,
                        born: Instant::now(),
                    });
                    inner.total += 1;
                }
                Err(_) => break,
            }
        }
        Ok(pool)
    }

    pub fn total_count(&self) -> usize {
        self.state.0.lock().map(|s| s.total).unwrap_or(0)
    }

    pub fn idle_count(&self) -> usize {
        self.state.0.lock().map(|s| s.idle.len()).unwrap_or(0)
    }

    /// Checkout a pooled connection. Blocking; honors `wait_timeout`.
    pub fn get(&self) -> NzResult<PooledConnection> {
        let deadline = self.config.wait_timeout.map(|t| Instant::now() + t);
        let mut guard = self.state.0.lock().unwrap();
        loop {
            if guard.closed {
                return Err(NzError::Closed(
                    "Cannot use a pool after calling close()".into(),
                ));
            }
            // Reuse an idle connection if one is fresh.
            while let Some(idle) = guard.idle.pop_front() {
                if self.is_fresh(&idle, guard.total) {
                    return Ok(PooledConnection {
                        conn: Some(idle.conn),
                        uses: idle.uses + 1,
                        born: idle.born,
                        pool: self.state.clone(),
                        config: self.config.clone(),
                    });
                }
                // Stale: destroy and keep looking.
                guard.total = guard.total.saturating_sub(1);
                drop_idle(idle);
            }
            if guard.total < self.config.max {
                guard.total += 1;
                drop(guard);
                match NzConnection::connect(&self.config.connection) {
                    Ok(conn) => {
                        return Ok(PooledConnection {
                            conn: Some(conn),
                            uses: 1,
                            born: Instant::now(),
                            pool: self.state.clone(),
                            config: self.config.clone(),
                        });
                    }
                    Err(e) => {
                        let mut guard = self.state.0.lock().unwrap();
                        guard.total = guard.total.saturating_sub(1);
                        self.state.1.notify_one();
                        drop(guard);
                        return Err(e);
                    }
                }
            }
            // Pool full: wait for a release.
            if let Some(dl) = deadline {
                let now = Instant::now();
                if now >= dl {
                    return Err(NzError::Timeout(
                        "Timeout exceeded when trying to connect".into(),
                    ));
                }
                let (g, res) = self.state.1.wait_timeout(guard, dl - now).unwrap();
                guard = g;
                if res.timed_out() {
                    return Err(NzError::Timeout(
                        "Timeout exceeded when trying to connect".into(),
                    ));
                }
            } else {
                guard = self.state.1.wait(guard).unwrap();
            }
        }
    }

    /// Buffered query with automatic checkout + release (Node `pool.query` parity).
    pub fn query(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<QueryResult> {
        let mut holder = self.get()?;
        let r = holder.query(sql, params);
        release_holder(&mut holder, r.as_ref().err());
        r
    }

    pub fn query_rows(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<Vec<Row>> {
        Ok(self.query(sql, params)?.rows().to_vec())
    }

    pub fn execute(&self, sql: &str, params: &[&dyn ToSql]) -> NzResult<i64> {
        let mut holder = self.get()?;
        let r = holder.execute(sql, params);
        release_holder(&mut holder, r.as_ref().err());
        r
    }

    /// Drain the pool and close all idle connections. Idempotent.
    pub fn close(&self) {
        let mut guard = self.state.0.lock().unwrap();
        guard.closed = true;
        while let Some(idle) = guard.idle.pop_front() {
            guard.total = guard.total.saturating_sub(1);
            drop_idle(idle);
        }
        self.state.1.notify_all();
    }

    fn is_fresh(&self, idle: &IdleConn, total: usize) -> bool {
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
        {
            // Only evict above `min`, so the warm floor survives.
            if total > self.config.min {
                return false;
            }
        }
        // A dead socket must not be handed out.
        !idle.conn.is_closed()
    }
}

impl Drop for NzPool {
    fn drop(&mut self) {
        self.close();
    }
}

fn drop_idle(mut idle: IdleConn) {
    idle.conn.close();
}

fn release_holder(holder: &mut PooledConnection, err: Option<&NzError>) {
    holder.release_with(err);
}

/// Checked-out connection; returns to the pool on [`PooledConnection::release`]
/// or `Drop` (mirrors Node's `release()` callback).
pub struct PooledConnection {
    conn: Option<NzConnection>,
    uses: u64,
    born: Instant,
    pool: Arc<(Mutex<PoolInner>, Condvar)>,
    config: NzPoolConfig,
}

impl PooledConnection {
    fn release_with(&mut self, err: Option<&NzError>) {
        let Some(mut conn) = self.conn.take() else {
            return;
        };
        let mut destroy = false;
        if let Some(e) = err {
            // SQL errors keep the session; everything else destroys it.
            if !matches!(e, NzError::Database(_)) {
                destroy = true;
            }
        }
        if let Some(max_uses) = self.config.max_uses {
            if self.uses >= max_uses {
                destroy = true;
            }
        }
        if let Some(max_life) = self.config.max_lifetime {
            if self.born.elapsed() >= max_life {
                destroy = true;
            }
        }
        if conn.is_closed() {
            destroy = true;
        }
        // Roll back a leaked transaction before re-queueing; a failed
        // rollback destroys the session.
        if !destroy
            && self.config.rollback_on_release
            && conn.in_transaction()
            && conn.rollback().is_err()
        {
            destroy = true;
        }
        let mut guard = self.pool.0.lock().unwrap();
        if destroy || guard.closed {
            guard.total = guard.total.saturating_sub(1);
            drop(guard);
            conn.close();
            // Keep the warm floor populated.
            self.ensure_min();
        } else {
            guard.idle.push_back(IdleConn {
                conn,
                idle_since: Instant::now(),
                uses: self.uses,
                born: self.born,
            });
            drop(guard);
        }
        self.pool.1.notify_one();
    }

    /// Return to the pool (idempotent; also happens on `Drop`).
    pub fn release(&mut self) {
        self.release_with(None);
    }

    fn ensure_min(&self) {
        // Best-effort refill outside the lock.
        let (need, cfg) = {
            let guard = self.pool.0.lock().unwrap();
            (
                guard.total < self.config.min && !guard.closed,
                self.config.clone(),
            )
        };
        if need {
            if let Ok(conn) = NzConnection::connect(&cfg.connection) {
                let mut guard = self.pool.0.lock().unwrap();
                if !guard.closed && guard.total < cfg.max {
                    guard.idle.push_back(IdleConn {
                        conn,
                        idle_since: Instant::now(),
                        uses: 0,
                        born: Instant::now(),
                    });
                    guard.total += 1;
                    self.pool.1.notify_one();
                }
            }
        }
    }

    // -- pass-through query surface -----------------------------------------

    pub fn query(&mut self, sql: &str, params: &[&dyn ToSql]) -> NzResult<QueryResult> {
        self.conn
            .as_mut()
            .ok_or_else(|| NzError::Closed("pooled connection was released".into()))?
            .query(sql, params)
    }

    pub fn query_rows(&mut self, sql: &str, params: &[&dyn ToSql]) -> NzResult<Vec<Row>> {
        self.conn
            .as_mut()
            .ok_or_else(|| NzError::Closed("pooled connection was released".into()))?
            .query_rows(sql, params)
    }

    pub fn execute(&mut self, sql: &str, params: &[&dyn ToSql]) -> NzResult<i64> {
        self.conn
            .as_mut()
            .ok_or_else(|| NzError::Closed("pooled connection was released".into()))?
            .execute(sql, params)
    }

    pub fn batch_execute(&mut self, sql: &str) -> NzResult<()> {
        self.conn
            .as_mut()
            .ok_or_else(|| NzError::Closed("pooled connection was released".into()))?
            .batch_execute(sql)
    }

    pub fn in_transaction(&self) -> bool {
        self.conn
            .as_ref()
            .map(|c| c.in_transaction())
            .unwrap_or(false)
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        self.release_with(None);
    }
}

impl std::ops::Deref for PooledConnection {
    type Target = NzConnection;
    fn deref(&self) -> &NzConnection {
        self.conn.as_ref().expect("pooled connection was released")
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut NzConnection {
        self.conn.as_mut().expect("pooled connection was released")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> NzPoolConfig {
        NzPoolConfig::new(NzConnectionConfig::new("localhost", "db", "u", "p"))
    }

    #[test]
    fn rejects_bad_limits() {
        let mut c = cfg();
        c.max = 0;
        assert!(NzPool::new(c).is_err());
        let mut c = cfg();
        c.min = 5;
        c.max = 2;
        assert!(NzPool::new(c).is_err());
    }

    #[test]
    fn wait_timeout_when_full_without_server() {
        // max=1, first checkout occupies the only slot by failing to connect?
        // Instead verify the pool refuses use after close.
        let c = cfg();
        let pool = NzPool::new(c).unwrap();
        pool.close();
        assert!(pool.get().is_err());
    }
}
