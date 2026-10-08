//! LIVE stress and soak tests (slow; never run by default or in CI).
//!
//! Run with `scripts/test-live.sh --stress`. Intensity is controlled by
//! `NZ_STRESS_QUERIES` (queries per worker, default 100) and
//! `NZ_STRESS_CYCLES` (connect/close iterations, default 100). Each test has
//! a hard timeout, so a deadlock fails instead of hanging.

mod live_support;

use live_support::live_config;
use nz_rust::{Client, NzError, NzValue, Pool, PoolConfig};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn sid_of(rows: &[nz_rust::Row]) -> String {
    rows[0].try_values().unwrap()[0].to_display_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires live Netezza appliance (stress)"]
async fn live_stress_pool_max_4_with_32_workers() {
    const WORKERS: usize = 32;
    const MAX: usize = 4;
    let queries = env_usize("NZ_STRESS_QUERIES", 100);
    let mut config = PoolConfig::new(live_config());
    config.max = MAX;
    config.wait_timeout = Some(Duration::from_secs(120));
    let pool = Arc::new(Pool::new(config).unwrap());
    let sessions: Arc<Mutex<HashSet<String>>> = Arc::default();
    let started = Instant::now();

    let mut workers = Vec::new();
    for worker in 0..WORKERS {
        let pool = pool.clone();
        let sessions = sessions.clone();
        workers.push(tokio::spawn(async move {
            let mut errors = 0usize;
            for i in 0..queries {
                let mut lease = pool.get().await.expect("checkout");
                match (worker + i) % 6 {
                    0 => {
                        let rows = lease.query("SELECT CURRENT_SID", &[]).await.unwrap();
                        sessions.lock().unwrap().insert(sid_of(&rows));
                    }
                    1 => {
                        let value = (worker * 1_000 + i) as i64;
                        let rows = lease
                            .query("SELECT CAST($1 AS BIGINT) AS V, 'x' AS T", &[&value])
                            .await
                            .unwrap();
                        assert_eq!(rows[0].try_get::<_, i64>(0).unwrap(), value);
                    }
                    2 => {
                        let rows = lease
                            .query(
                                "SELECT 1 AS A UNION ALL SELECT 2 UNION ALL SELECT 3 \
                                 UNION ALL SELECT 4 UNION ALL SELECT 5",
                                &[],
                            )
                            .await
                            .unwrap();
                        assert_eq!(rows.len(), 5);
                    }
                    3 => {
                        let tx = lease.transaction().await.unwrap();
                        tx.query("SELECT 1", &[]).await.unwrap();
                        tx.rollback().await.unwrap();
                    }
                    4 => {
                        // An occasional SQL error must leave the leased
                        // session usable for the next statement.
                        match lease.query("SELECT 1/0", &[]).await {
                            Err(NzError::Database(_)) => errors += 1,
                            other => panic!("expected division error, got {other:?}"),
                        }
                        let rows = lease.query("SELECT 7 AS V", &[]).await.unwrap();
                        assert_eq!(rows[0].try_values().unwrap()[0], NzValue::Int4(7));
                    }
                    _ => {
                        lease.query("SELECT 1", &[]).await.unwrap();
                    }
                }
                if i % 5 == 0 {
                    drop(lease);
                } else {
                    lease.release().await;
                }
            }
            errors
        }));
    }
    let joined = tokio::time::timeout(Duration::from_secs(600), async {
        let mut errors = 0;
        for worker in workers {
            errors += worker.await.expect("worker panicked");
        }
        errors
    })
    .await
    .expect("stress workers deadlocked or stalled");
    eprintln!(
        "stress: {WORKERS} workers x {queries} queries in {:?}, {joined} expected SQL errors, \
         {} distinct sessions observed",
        started.elapsed(),
        sessions.lock().unwrap().len()
    );

    assert!(
        sessions.lock().unwrap().len() <= MAX,
        "more physical sessions than pool max: {:?}",
        sessions.lock().unwrap()
    );
    // Dropped leases return through background tasks; wait for quiescence.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let (total, idle) = (pool.total_count().await, pool.idle_count().await);
        assert!(total <= MAX, "total {total} exceeds max");
        if total == idle {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "lost permits: total {total} idle {idle}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Every slot is available again: MAX concurrent checkouts succeed.
    let mut held = Vec::new();
    for _ in 0..MAX {
        held.push(pool.get().await.expect("a slot was lost"));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(300), pool.get())
            .await
            .is_err(),
        "pool exceeded its maximum"
    );
    for lease in held {
        lease.release().await;
    }
    pool.close().await;
    assert_eq!(pool.total_count().await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Netezza appliance (stress)"]
async fn live_stress_connection_cycling_direct_and_pooled() {
    let cycles = env_usize("NZ_STRESS_CYCLES", 100);
    let config = live_config();
    let outcome = tokio::time::timeout(Duration::from_secs(600), async {
        let mut sessions = HashSet::new();
        for _ in 0..cycles {
            let client = Client::connect(&config).await.expect("connect");
            let rows = client.query("SELECT CURRENT_SID", &[]).await.unwrap();
            sessions.insert(sid_of(&rows));
            client.close().await.unwrap();
            assert!(client.is_closed());
        }
        // Each direct connection is its own session.
        assert_eq!(sessions.len(), cycles, "sessions were reused or lost");

        // Pooled: max_uses = 1 forces a physical reconnect on every checkout.
        let mut pool_config = PoolConfig::new(config.clone());
        pool_config.max = 1;
        pool_config.max_uses = Some(1);
        let pool = Pool::new(pool_config).unwrap();
        let mut pooled = HashSet::new();
        for _ in 0..cycles {
            let lease = pool.get().await.unwrap();
            let rows = lease.query("SELECT CURRENT_SID", &[]).await.unwrap();
            pooled.insert(sid_of(&rows));
            lease.release().await;
            assert_eq!(pool.total_count().await, 0);
        }
        assert_eq!(pooled.len(), cycles);
        pool.close().await;
    })
    .await;
    outcome.expect("connection cycling stalled");
}
