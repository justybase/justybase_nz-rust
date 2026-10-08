//! LIVE tests that need administrative privileges on the appliance.
//!
//! Kept out of `live_qualification` so that suite runs with an ordinary
//! account. Run with `scripts/test-live.sh --admin` (the account must be
//! allowed to `DROP SESSION`). Each test only touches sessions it created.

mod live_support;

use live_support::live_config;
use nz_rust::Client;
use std::time::Duration;

async fn connect() -> Client {
    Client::connect(&live_config())
        .await
        .expect("connect to live appliance")
}

/// The appliance kills an idle pooled session (as an administrator or an idle
/// timeout would); the pool must hand out a new session, not the dead one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires live Netezza appliance"]
async fn live_pool_replaces_session_killed_by_the_server() {
    let mut config = nz_rust::PoolConfig::new(live_config());
    config.max = 1;
    let pool = nz_rust::Pool::new(config).unwrap();
    let lease = pool.get().await.unwrap();
    let victim = lease.query("SELECT CURRENT_SID", &[]).await.unwrap()[0]
        .try_values()
        .unwrap()[0]
        .to_display_string();
    lease.release().await;

    // Kill only the session this test created.
    let killer = connect().await;
    killer
        .batch_execute(&format!("DROP SESSION {victim}"))
        .await
        .unwrap_or_else(|e| panic!("DROP SESSION of the test's own session failed: {e}"));
    killer.close().await.unwrap();
    // Let the appliance finish tearing the session down.
    let lease = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let lease = pool.get().await.unwrap();
            match lease.query("SELECT CURRENT_SID", &[]).await {
                Ok(rows) => {
                    let sid = rows[0].try_values().unwrap()[0].to_display_string();
                    if sid != victim {
                        break (lease, sid);
                    }
                    // The kill has not landed yet; keep the session idle.
                    lease.release().await;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(error) => {
                    panic!("pool handed out the killed session; first statement failed: {error}")
                }
            }
        }
    })
    .await
    .expect("killed session was never replaced");
    assert_ne!(lease.1, victim);
    lease.0.release().await;
    pool.close().await;
}
