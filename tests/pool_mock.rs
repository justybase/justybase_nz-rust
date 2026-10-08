//! Pool slot accounting, concurrency and connection-rotation rules against the
//! mock backend, for the native `Pool` and the legacy `NzPool`.
//!
//! `SELECT pid` returns the backend process id of the physical session that
//! served it, so reuse and rotation are proven by identity, and the server's
//! accept counter proves how many physical connections were opened.
//!
//! The idle-timeout / max-lifetime cases are inherently wall-clock based (the
//! pools use `std::time::Instant`); they use short limits and sleep for at
//! least three times the limit, so scheduling jitter cannot flip the outcome.

mod support;

use nz_rust::{NzError, NzValue};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use support::*;

/// Behaviour switches shared with the mock handler.
#[derive(Default)]
struct Switches {
    fail_rollback: AtomicBool,
    rollbacks: AtomicUsize,
    live_sessions: AtomicUsize,
    peak_sessions: AtomicUsize,
}

fn start_pool_server() -> (MockServer, Arc<Switches>) {
    let switches = Arc::new(Switches::default());
    let shared = switches.clone();
    let server = MockServer::start(HandshakeScript::default(), move |session| {
        let live = shared.live_sessions.fetch_add(1, Ordering::SeqCst) + 1;
        shared.peak_sessions.fetch_max(live, Ordering::SeqCst);
        let mut die_after_rollback = false;
        while let Some(sql) = session.read_query() {
            let response = match sql.as_str() {
                "SELECT pid" => {
                    let pid = session.pid.to_string();
                    let mut wire = row_description(&[("PID", OID_INT4, 4)]);
                    wire.extend(text_row(&[Some(pid.as_bytes())]));
                    wire.extend(command_complete("SELECT 1"));
                    wire.extend(ready());
                    wire
                }
                "SELECT then_die" => {
                    die_after_rollback = true;
                    select_one()
                }
                "ROLLBACK" => {
                    shared.rollbacks.fetch_add(1, Ordering::SeqCst);
                    if shared.fail_rollback.load(Ordering::SeqCst) {
                        let mut wire = error("25P01", "rollback failed");
                        wire.extend(ready());
                        wire
                    } else {
                        simple_command("ROLLBACK")
                    }
                }
                "BEGIN" => simple_command("BEGIN"),
                _ => select_one(),
            };
            if session.send(&response).is_err() {
                break;
            }
            if die_after_rollback && sql == "ROLLBACK" {
                break;
            }
        }
        shared.live_sessions.fetch_sub(1, Ordering::SeqCst);
    });
    (server, switches)
}

fn pid_of(rows: &[nz_rust::Row]) -> i32 {
    match rows[0].try_values().unwrap() {
        [NzValue::Int4(pid)] => *pid,
        other => panic!("unexpected pid row {other:?}"),
    }
}

fn pool_config(server: &MockServer, max: usize) -> nz_rust::PoolConfig {
    let mut config = nz_rust::PoolConfig::new(server.config());
    config.max = max;
    config.wait_timeout = Some(Duration::from_secs(10));
    config
}

async fn lease_pid(pool: &nz_rust::Pool) -> i32 {
    let lease = pool.get().await.unwrap();
    let pid = pid_of(&lease.query("SELECT pid", &[]).await.unwrap());
    lease.release().await;
    pid
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_max_1_multiple_waiters_share_one_session() {
    let (server, _) = start_pool_server();
    let pool = Arc::new(nz_rust::Pool::new(pool_config(&server, 1)).unwrap());
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let pool = pool.clone();
        tasks.push(tokio::spawn(async move {
            let mut pids = Vec::new();
            for _ in 0..5 {
                pids.push(lease_pid(&pool).await);
            }
            pids
        }));
    }
    for task in tasks {
        for pid in task.await.unwrap() {
            assert_eq!(pid, BASE_PID + 1);
        }
    }
    assert_eq!(server.accepted(), 1);
    assert_eq!(pool.total_count().await, 1);
    assert_eq!(pool.idle_count().await, 1);
    pool.close().await;
    server.assert_no_handler_panics();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_max_4_with_32_concurrent_tasks_never_exceeds_max() {
    let (server, switches) = start_pool_server();
    let pool = Arc::new(nz_rust::Pool::new(pool_config(&server, 4)).unwrap());
    let mut tasks = Vec::new();
    for worker in 0..32 {
        let pool = pool.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..10 {
                let lease = pool.get().await.unwrap();
                if (worker + i) % 7 == 0 {
                    lease.query("SELECT pid", &[]).await.unwrap();
                } else {
                    assert_eq!(lease.query("SELECT 1", &[]).await.unwrap().len(), 1);
                }
                if i % 3 == 0 {
                    drop(lease);
                } else {
                    lease.release().await;
                }
            }
        }));
    }
    for task in tasks {
        tokio::time::timeout(Duration::from_secs(60), task)
            .await
            .expect("worker deadlocked")
            .unwrap();
    }
    assert!(server.accepted() <= 4, "opened {}", server.accepted());
    assert!(switches.peak_sessions.load(Ordering::SeqCst) <= 4);
    // Dropped leases return through a background task.
    assert!(wait_until(Duration::from_secs(10), || {
        futures_executor_block(pool.idle_count()) == futures_executor_block(pool.total_count())
    }));
    assert!(pool.total_count().await <= 4);
    pool.close().await;
    assert_eq!(pool.total_count().await, 0);
    server.assert_no_handler_panics();
}

/// The pool counters only take a std lock; poll them synchronously.
fn futures_executor_block<F: std::future::Future>(future: F) -> F::Output {
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        std::task::Poll::Ready(value) => value,
        std::task::Poll::Pending => panic!("pool counter unexpectedly pending"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_cancelled_waiter_does_not_leak_slot() {
    let (server, _) = start_pool_server();
    let pool = Arc::new(nz_rust::Pool::new(pool_config(&server, 1)).unwrap());
    let holder = pool.get().await.unwrap();
    // A waiter cancelled by a timeout, and one aborted outright.
    assert!(tokio::time::timeout(Duration::from_millis(50), pool.get())
        .await
        .is_err());
    let aborted = {
        let pool = pool.clone();
        tokio::spawn(async move { pool.get().await.map(|_| ()) })
    };
    tokio::task::yield_now().await;
    aborted.abort();
    assert!(aborted.await.unwrap_err().is_cancelled());
    holder.release().await;
    assert_eq!(pool.total_count().await, 1);
    assert_eq!(lease_pid(&pool).await, BASE_PID + 1);
    assert_eq!(server.accepted(), 1);
    pool.close().await;
    server.assert_no_handler_panics();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_close_with_checked_out_connection_and_late_release() {
    let (server, switches) = start_pool_server();
    let pool = nz_rust::Pool::new(pool_config(&server, 2)).unwrap();
    let held = pool.get().await.unwrap();
    let idle = pool.get().await.unwrap();
    idle.release().await;
    assert_eq!(pool.idle_count().await, 1);
    pool.close().await;
    assert!(matches!(pool.get().await, Err(NzError::Closed(_))));
    // The checked-out lease still works until it is returned...
    assert_eq!(held.query("SELECT 1", &[]).await.unwrap().len(), 1);
    // ...and returning it after close() destroys it instead of pooling it.
    held.release().await;
    assert_eq!(pool.total_count().await, 0);
    assert_eq!(pool.idle_count().await, 0);
    assert!(wait_until(Duration::from_secs(5), || switches
        .live_sessions
        .load(Ordering::SeqCst)
        == 0));
    pool.close().await; // idempotent
    server.assert_no_handler_panics();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_drop_checked_out_holder_returns_slot() {
    let (server, _) = start_pool_server();
    let pool = nz_rust::Pool::new(pool_config(&server, 1)).unwrap();
    let lease = pool.get().await.unwrap();
    let pid = pid_of(&lease.query("SELECT pid", &[]).await.unwrap());
    drop(lease);
    // The next checkout waits for the background return, then reuses it.
    assert_eq!(lease_pid(&pool).await, pid);
    assert_eq!(server.accepted(), 1);
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_failed_rollback_destroys_connection() {
    let (server, switches) = start_pool_server();
    let pool = nz_rust::Pool::new(pool_config(&server, 1)).unwrap();
    switches.fail_rollback.store(true, Ordering::SeqCst);
    let first = lease_pid(&pool).await;
    switches.fail_rollback.store(false, Ordering::SeqCst);
    assert_eq!(pool.total_count().await, 0);
    let second = lease_pid(&pool).await;
    assert_ne!(first, second, "connection with failed rollback was reused");
    assert_eq!(server.accepted(), 2);
    pool.close().await;
    server.assert_no_handler_panics();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_server_closed_idle_socket_is_not_reused() {
    let (server, switches) = start_pool_server();
    let pool = nz_rust::Pool::new(pool_config(&server, 1)).unwrap();
    let lease = pool.get().await.unwrap();
    lease.query("SELECT then_die", &[]).await.unwrap();
    lease.release().await;
    // The backend hung up on the idle session.
    assert!(wait_until(Duration::from_secs(5), || switches
        .live_sessions
        .load(Ordering::SeqCst)
        == 0));
    let lease = pool.get().await.unwrap();
    let pid = pid_of(
        &lease
            .query("SELECT pid", &[])
            .await
            .expect("pool handed out a connection whose socket the server closed"),
    );
    lease.release().await;
    assert_eq!(pid, BASE_PID + 2);
    pool.close().await;
    server.assert_no_handler_panics();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_max_uses_rotates_physical_session() {
    for max_uses in [1u64, 2] {
        let (server, _) = start_pool_server();
        let mut config = pool_config(&server, 1);
        config.max_uses = Some(max_uses);
        let pool = nz_rust::Pool::new(config).unwrap();
        let mut pids = Vec::new();
        for _ in 0..6 {
            pids.push(lease_pid(&pool).await);
        }
        let expected: Vec<i32> = (0..6)
            .map(|i| BASE_PID + 1 + (i / max_uses as i32))
            .collect();
        assert_eq!(pids, expected, "max_uses={max_uses}");
        assert_eq!(server.accepted(), 6 / max_uses as usize);
        pool.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_idle_timeout_and_max_lifetime_rotate_sessions() {
    // Idle timeout: an idle connection above the floor is replaced.
    let (server, _) = start_pool_server();
    let mut config = pool_config(&server, 1);
    config.idle_timeout = Duration::from_millis(60);
    let pool = nz_rust::Pool::new(config).unwrap();
    let first = lease_pid(&pool).await;
    assert_eq!(lease_pid(&pool).await, first, "reused before the timeout");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_ne!(lease_pid(&pool).await, first, "idle connection not evicted");
    pool.close().await;

    // Max lifetime: replaced even when used continuously.
    let (server, _) = start_pool_server();
    let mut config = pool_config(&server, 1);
    config.max_lifetime = Some(Duration::from_millis(60));
    config.idle_timeout = Duration::ZERO;
    let pool = nz_rust::Pool::new(config).unwrap();
    let first = lease_pid(&pool).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_ne!(lease_pid(&pool).await, first, "expired connection reused");
    assert_eq!(server.accepted(), 2);
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_min_keeps_a_warm_floor() {
    let (server, _) = start_pool_server();
    let mut config = pool_config(&server, 3);
    config.min = 2;
    config.idle_timeout = Duration::from_millis(60);
    let pool = nz_rust::Pool::connect(config).await.unwrap();
    assert_eq!(server.accepted(), 2, "min connections prewarmed");
    assert_eq!(pool.total_count().await, 2);
    assert_eq!(pool.idle_count().await, 2);
    tokio::time::sleep(Duration::from_millis(200)).await;
    // At the floor, idle connections are not evicted.
    let pid = lease_pid(&pool).await;
    assert!(pid == BASE_PID + 1 || pid == BASE_PID + 2);
    assert_eq!(server.accepted(), 2);
    pool.close().await;
}

#[cfg(feature = "compat")]
mod legacy {
    use super::*;

    fn legacy_config(server: &MockServer, max: usize) -> nz_rust::NzPoolConfig {
        let mut connection = server.config();
        connection.command_timeout = 10;
        let mut config = nz_rust::NzPoolConfig::new(connection);
        config.max = max;
        config.wait_timeout = Some(Duration::from_secs(10));
        config
    }

    fn legacy_pid(pool: &nz_rust::NzPool) -> i32 {
        let mut conn = pool.get().unwrap();
        let result = conn.query("SELECT pid", &[]).unwrap();
        pid_of(&result.result_sets[0].rows)
    }

    #[test]
    fn legacy_pool_max_4_with_32_threads_never_exceeds_max() {
        let (server, switches) = start_pool_server();
        let pool = Arc::new(nz_rust::NzPool::new(legacy_config(&server, 4)).unwrap());
        let threads: Vec<_> = (0..32)
            .map(|_| {
                let pool = pool.clone();
                std::thread::spawn(move || {
                    for _ in 0..10 {
                        let mut conn = pool.get().unwrap();
                        assert_eq!(
                            conn.query("SELECT 1", &[]).unwrap().result_sets[0]
                                .rows
                                .len(),
                            1
                        );
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(server.accepted() <= 4);
        assert!(switches.peak_sessions.load(Ordering::SeqCst) <= 4);
        assert_eq!(pool.idle_count(), pool.total_count());
        pool.close();
        assert_eq!(pool.total_count(), 0);
        server.assert_no_handler_panics();
    }

    #[test]
    fn legacy_pool_max_uses_idle_timeout_and_lifetime_rotate_sessions() {
        for max_uses in [1u64, 2] {
            let (server, _) = start_pool_server();
            let mut config = legacy_config(&server, 1);
            config.max_uses = Some(max_uses);
            let pool = nz_rust::NzPool::new(config).unwrap();
            let pids: Vec<i32> = (0..6).map(|_| legacy_pid(&pool)).collect();
            let expected: Vec<i32> = (0..6)
                .map(|i| BASE_PID + 1 + (i / max_uses as i32))
                .collect();
            assert_eq!(pids, expected, "max_uses={max_uses}");
        }

        let (server, _) = start_pool_server();
        let mut config = legacy_config(&server, 1);
        config.idle_timeout = Duration::from_millis(60);
        let pool = nz_rust::NzPool::new(config).unwrap();
        let first = legacy_pid(&pool);
        assert_eq!(legacy_pid(&pool), first);
        std::thread::sleep(Duration::from_millis(200));
        assert_ne!(legacy_pid(&pool), first, "idle connection not evicted");

        let (server, _) = start_pool_server();
        let mut config = legacy_config(&server, 1);
        config.max_lifetime = Some(Duration::from_millis(60));
        config.idle_timeout = Duration::ZERO;
        let pool = nz_rust::NzPool::new(config).unwrap();
        let first = legacy_pid(&pool);
        std::thread::sleep(Duration::from_millis(200));
        assert_ne!(legacy_pid(&pool), first, "expired connection reused");
    }

    #[test]
    fn legacy_pool_min_floor_close_and_failed_rollback() {
        let (server, switches) = start_pool_server();
        let mut config = legacy_config(&server, 3);
        config.min = 2;
        config.idle_timeout = Duration::from_millis(60);
        let pool = nz_rust::NzPool::new(config).unwrap();
        assert_eq!(server.accepted(), 2);
        std::thread::sleep(Duration::from_millis(200));
        legacy_pid(&pool);
        assert_eq!(server.accepted(), 2, "floor connections evicted");

        // A failed rollback on release destroys the connection.
        {
            let mut conn = pool.get().unwrap();
            conn.batch_execute("BEGIN").unwrap();
            assert!(conn.in_transaction());
            switches.fail_rollback.store(true, Ordering::SeqCst);
        }
        switches.fail_rollback.store(false, Ordering::SeqCst);
        assert!(switches.rollbacks.load(Ordering::SeqCst) >= 1);

        // Close with a checked-out connection; it is destroyed on return.
        let held = pool.get().unwrap();
        pool.close();
        assert!(matches!(pool.get(), Err(NzError::Closed(_))));
        drop(held);
        assert_eq!(pool.total_count(), 0);
        server.assert_no_handler_panics();
    }

    #[test]
    fn legacy_pool_server_closed_idle_socket_is_not_reused() {
        let (server, switches) = start_pool_server();
        let mut config = legacy_config(&server, 1);
        config.rollback_on_release = false;
        let pool = nz_rust::NzPool::new(config).unwrap();
        {
            let mut conn = pool.get().unwrap();
            conn.query("SELECT then_die", &[]).unwrap();
            // Without a rollback on release the mock hangs up on its own
            // after the next statement; send one to trigger it.
            let _ = conn.batch_execute("ROLLBACK");
        }
        assert!(wait_until(Duration::from_secs(5), || switches
            .live_sessions
            .load(Ordering::SeqCst)
            == 0));
        let mut conn = pool.get().unwrap();
        let result = conn
            .query("SELECT pid", &[])
            .expect("pool handed out a connection whose socket the server closed");
        assert_eq!(pid_of(&result.result_sets[0].rows), BASE_PID + 2);
    }
}
