//! Live integration tests against a real Netezza appliance.
//!
//! These are skipped (reported as passing) unless `NZ_RUN_LIVE_TESTS=1` is
//! set, so `cargo test` stays green without an appliance. Point them at a
//! server with:
//!
//! ```text
//! NZ_RUN_LIVE_TESTS=1 NZ_DEV_HOST=host NZ_DEV_DATABASE=JUST_DATA \
//!   NZ_DEV_USER=admin NZ_DEV_PASSWORD=secret \
//!   cargo test -p nz_rust --test live_driver -- --nocapture
//! ```

use futures_core::Stream;
use nz_rust::{
    ColumnDesc, NzConnection, NzConnectionConfig, NzError, NzPool, NzPoolConfig, NzValue,
    QueryStreamSink, Row,
};

#[derive(Default)]
struct StreamProbe {
    columns: usize,
    rows: usize,
    cells: usize,
    column_events: Vec<(usize, usize)>,
    row_result_sets: Vec<usize>,
}

impl QueryStreamSink for StreamProbe {
    fn on_columns(
        &mut self,
        result_set_index: usize,
        columns: &[ColumnDesc],
        _nullability: Option<&[bool]>,
    ) -> Result<(), NzError> {
        self.columns = columns.len();
        self.column_events.push((result_set_index, columns.len()));
        Ok(())
    }

    fn on_row(&mut self, result_set_index: usize, row: Row) -> Result<(), NzError> {
        self.on_values(result_set_index, row.columns(), row.values())
    }

    fn on_values(
        &mut self,
        result_set_index: usize,
        _columns: &[ColumnDesc],
        values: &[NzValue],
    ) -> Result<(), NzError> {
        self.rows += 1;
        self.cells += values.len();
        self.row_result_sets.push(result_set_index);
        Ok(())
    }
}

/// Returns the live config, or `None` when live tests are not opted into.
fn config() -> Option<NzConnectionConfig> {
    if std::env::var("NZ_RUN_LIVE_TESTS").ok().as_deref() != Some("1") {
        return None;
    }
    let host = std::env::var("NZ_DEV_HOST").ok()?;
    Some(NzConnectionConfig {
        host,
        port: std::env::var("NZ_DEV_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5480),
        database: std::env::var("NZ_DEV_DB")
            .or_else(|_| std::env::var("NZ_DEV_DATABASE"))
            .unwrap_or_else(|_| "JUST_DATA".into()),
        user: std::env::var("NZ_DEV_USER").unwrap_or_else(|_| "admin".into()),
        password: std::env::var("NZ_DEV_PASSWORD")
            .expect("NZ_DEV_PASSWORD is required when NZ_DEV_HOST is set"),
        ..Default::default()
    })
}

#[test]
fn live_query_types_parameters_multi_result_and_transaction() {
    let Some(config) = config() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    let mut conn = NzConnection::connect(&config).expect("connect/authentication");
    let result = conn
        .query(
            "SELECT 1 AS one, 'rust' AS name, 3.1400::numeric(10,4) AS n, DATE '2024-01-02' AS d",
            &[],
        )
        .expect("typed query");
    let row = &result.rows()[0];
    assert_eq!(row.try_get::<_, i32>("one").unwrap(), 1);
    assert_eq!(row.try_get::<_, String>("name").unwrap(), "rust");
    // Trailing zeros are preserved (Node/C# reference parity).
    assert_eq!(row.try_get::<_, String>("n").unwrap(), "3.1400");
    assert_eq!(row.try_get::<_, String>("d").unwrap(), "2024-01-02");

    let params = vec![NzValue::Text("O'Brien".into()), NzValue::Int4(7)];
    let result = conn
        .query_values("SELECT $1 AS name, $2 AS value", &params)
        .unwrap();
    assert_eq!(result.rows()[0].try_get::<_, String>(0).unwrap(), "O'Brien");
    assert_eq!(result.rows()[0].try_get::<_, i32>(1).unwrap(), 7);

    let multi = conn.query("SELECT 1 AS a; SELECT 2 AS b", &[]).unwrap();
    assert_eq!(multi.result_sets.len(), 2);
    assert_eq!(multi.rows()[0].try_get::<_, i32>(0).unwrap(), 1);

    conn.begin_transaction().unwrap();
    assert!(conn.in_transaction());
    conn.rollback().unwrap();
    assert!(!conn.in_transaction());
}

#[test]
fn live_pool_checkout_query_and_release() {
    let Some(config) = config() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    let pool = NzPool::new(NzPoolConfig::new(config)).unwrap();
    let result = pool.query("SELECT 1 AS one", &[]).unwrap();
    assert_eq!(result.rows()[0].try_get::<_, i32>(0).unwrap(), 1);
    pool.close();
}

#[test]
fn live_streaming_sink_receives_rows_without_buffering_result_sets() {
    let Some(config) = config() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    let mut conn = NzConnection::connect(&config).expect("connect/authentication");
    let mut probe = StreamProbe::default();
    let summary = conn
        .execute_stream(
            "SELECT 1 AS one, 'rust' AS name FROM JUST_DATA..DIMDATE LIMIT 10",
            &[],
            &mut probe,
        )
        .expect("stream query");

    assert_eq!(probe.columns, 2);
    assert_eq!(probe.rows, 10);
    assert_eq!(probe.cells, 20);
    assert_eq!(summary.result_sets.len(), 1);
    assert_eq!(summary.result_sets[0].row_count, 10);
}

#[test]
fn live_streaming_sink_preserves_multiple_result_sets() {
    let Some(config) = config() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    let mut conn = NzConnection::connect(&config).expect("connect/authentication");
    let mut probe = StreamProbe::default();
    let summary = conn
        .execute_stream("SELECT 1 AS one; SELECT 2 AS two", &[], &mut probe)
        .expect("multi-result stream query");

    assert_eq!(summary.result_sets.len(), 2);
    assert_eq!(summary.result_sets[0].row_count, 1);
    assert_eq!(summary.result_sets[1].row_count, 1);
    assert_eq!(probe.column_events, vec![(0, 1), (1, 1)]);
    assert_eq!(probe.row_result_sets, vec![0, 1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_native_client_query_and_bounded_stream() {
    let Some(config) = config() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    let (client, connection) = nz_rust::connect(&config).await.expect("native connect");
    let driver = tokio::spawn(connection);
    let row = client
        .query_one("SELECT 1 AS one, 'rust' AS name", &[])
        .await
        .expect("native query");
    assert_eq!(row.try_get::<_, i32>("one").unwrap(), 1);
    assert_eq!(row.try_get::<_, String>("name").unwrap(), "rust");

    let mut stream = client
        .query_stream("SELECT 1 AS one FROM JUST_DATA..DIMDATE LIMIT 10", &[])
        .await
        .expect("native row stream");
    let mut rows = 0;
    while let Some(item) =
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut stream).poll_next(cx)).await
    {
        assert_eq!(item.unwrap().try_get::<_, i32>(0).unwrap(), 1);
        rows += 1;
    }
    assert_eq!(rows, 10);
    client.close().await.expect("native close");
    assert!(driver.await.unwrap().is_ok());
}
