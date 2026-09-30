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
    QueryStreamEvent, QueryStreamSink, Row,
};

#[derive(Default)]
struct StreamProbe {
    columns: usize,
    rows: usize,
    cells: usize,
    column_events: Vec<(usize, usize)>,
    row_result_sets: Vec<usize>,
    notices: Vec<String>,
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

    fn on_notice(&mut self, message: &str) -> Result<(), NzError> {
        self.notices.push(message.to_owned());
        Ok(())
    }
}

#[derive(Default)]
struct AbortAfterFirstRow {
    rows: usize,
}

impl QueryStreamSink for AbortAfterFirstRow {
    fn on_columns(
        &mut self,
        _result_set_index: usize,
        _columns: &[ColumnDesc],
        _nullability: Option<&[bool]>,
    ) -> Result<(), NzError> {
        Ok(())
    }

    fn on_row(&mut self, _result_set_index: usize, _row: Row) -> Result<(), NzError> {
        self.rows += 1;
        Err(NzError::Config("consumer canceled stream".into()))
    }
}

fn unique_name(prefix: &str) -> String {
    format!(
        "{}_{}_{}",
        prefix,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    )
}

/// Returns the live config, or `None` when live tests are not opted into.
fn config() -> Option<NzConnectionConfig> {
    if std::env::var("NZ_RUN_LIVE_TESTS").ok().as_deref() != Some("1") {
        return None;
    }
    let host = std::env::var("NZ_DEV_HOST").ok()?;
    Some(NzConnectionConfig {
        external_files: nz_rust::ExternalFilePolicy::Unrestricted,
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
    assert_eq!(
        row.try_get::<_, nz_rust::Decimal>("n").unwrap(),
        "3.1400".parse::<nz_rust::Decimal>().unwrap()
    );
    #[cfg(feature = "chrono")]
    assert_eq!(
        row.try_get::<_, chrono::NaiveDate>("d").unwrap(),
        chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()
    );

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
fn live_streaming_sink_abort_cancels_and_preserves_same_session() {
    let Some(mut config) = config() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    config.command_timeout = 0;
    let mut conn = NzConnection::connect(&config).expect("connect/authentication");
    let table = unique_name("CANCEL_STREAM_SESSION");
    conn.batch_execute(&format!("CREATE TEMP TABLE {table} AS (SELECT 1 AS COL1)"))
        .expect("create session marker");

    let mut sink = AbortAfterFirstRow::default();
    let error = conn
        .execute_stream(
            "SELECT 1 AS ONE FROM JUST_DATA..DIMDATE LIMIT 10000",
            &[],
            &mut sink,
        )
        .expect_err("sink abort should interrupt row delivery");
    assert!(matches!(error, NzError::Config(message) if message == "consumer canceled stream"));
    assert_eq!(
        sink.rows, 1,
        "rows should stop reaching the sink after abort"
    );

    let mut recovered = false;
    for _ in 0..20 {
        if let Ok(result) = conn.query(&format!("SELECT COL1 FROM {table}"), &[]) {
            assert_eq!(result.rows()[0].try_get::<_, i32>(0).unwrap(), 1);
            recovered = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    assert!(
        recovered,
        "same session should be reusable after streaming sink abort"
    );
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

    let mut batches = client
        .query_batches("SELECT 1 AS one FROM JUST_DATA..DIMDATE LIMIT 10", &[])
        .await
        .expect("native producer-side batches");
    let mut batched_rows = 0;
    while let Some(batch) =
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut batches).poll_next(cx)).await
    {
        let batch = batch.expect("batch");
        assert!(batch.len() <= 256);
        assert!(batch
            .iter()
            .all(|row| row.try_get::<_, i32>(0).unwrap() == 1));
        batched_rows += batch.len();
    }
    assert_eq!(batched_rows, 10);
    client.close().await.expect("native close");
    assert!(driver.await.unwrap().is_ok());
}

#[test]
fn live_notices_arrive_during_streaming_query() {
    let Some(config) = config() else { return };
    let mut conn = NzConnection::connect(&config).unwrap();
    let procedure = unique_name("RUST_NOTICE");
    let sql = format!(
        "CREATE OR REPLACE PROCEDURE {procedure}() RETURNS INTEGER EXECUTE AS OWNER LANGUAGE NZPLSQL AS BEGIN_PROC BEGIN RAISE NOTICE 'rust notice first'; RAISE NOTICE 'rust notice second'; END; END_PROC;"
    );
    conn.batch_execute(&sql).unwrap();
    let mut sink = StreamProbe::default();
    let outcome = conn.execute_stream(&format!("CALL {procedure}()"), &[], &mut sink);
    let _ = conn.batch_execute(&format!("DROP PROCEDURE {procedure}()"));
    outcome.unwrap();
    assert!(
        sink.notices.iter().any(|n| n.contains("rust notice first")),
        "{:?}",
        sink.notices
    );
    assert!(
        sink.notices
            .iter()
            .any(|n| n.contains("rust notice second")),
        "{:?}",
        sink.notices
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_native_stream_emits_procedure_notices() {
    let Some(config) = config() else { return };
    let mut setup = NzConnection::connect(&config).unwrap();
    let procedure = unique_name("RUST_ASYNC_NOTICE");
    setup.batch_execute(&format!(
        "CREATE OR REPLACE PROCEDURE {procedure}() RETURNS INTEGER EXECUTE AS OWNER LANGUAGE NZPLSQL AS BEGIN_PROC BEGIN RAISE NOTICE 'rust async first'; RAISE NOTICE 'rust async second'; END; END_PROC;"
    )).unwrap();
    let (client, connection) = nz_rust::connect(&config).await.unwrap();
    let driver = tokio::spawn(connection);
    let mut events = client
        .query_stream_events(&format!("CALL {procedure}()"), &[])
        .await
        .unwrap();
    let mut notices = Vec::new();
    while let Some(event) =
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut events).poll_next(cx)).await
    {
        if let QueryStreamEvent::Notice(message) = event.unwrap() {
            notices.push(message);
        }
    }
    client.close().await.unwrap();
    driver.await.unwrap().unwrap();
    let _ = setup.batch_execute(&format!("DROP PROCEDURE {procedure}()"));
    assert!(
        notices.iter().any(|n| n.contains("rust async first")),
        "{notices:?}"
    );
    assert!(
        notices.iter().any(|n| n.contains("rust async second")),
        "{notices:?}"
    );
}

#[test]
fn live_large_external_import_and_export_round_trip() {
    let Some(config) = config() else { return };
    let mut conn = NzConnection::connect(&config).unwrap();
    let first = unique_name("RUST_EXT_SRC");
    let second = unique_name("RUST_EXT_DST");
    let file = std::env::temp_dir().join(format!("{}.txt", unique_name("rust_nz_export")));
    let file_sql = file.to_string_lossy().replace('\'', "''");
    let log_dir = std::env::temp_dir().to_string_lossy().replace('\'', "''");
    let import_id = format!("virtual://{}", unique_name("rust_nz_import"));
    let expected_rows = 10_000usize;
    let data: String = (0..expected_rows)
        .map(|i| format!("{i}|value_{i:05}\n"))
        .collect();
    assert!(data.len() > 65_536);
    conn.batch_execute(&format!(
        "CREATE TABLE {first}(id INTEGER, val VARCHAR(32))"
    ))
    .unwrap();
    conn.batch_execute(&format!(
        "CREATE TABLE {second}(id INTEGER, val VARCHAR(32))"
    ))
    .unwrap();
    let data = data.into_bytes();
    let result = (|| -> Result<(), NzError> {
        conn.query_with_import_reader(&format!(
            "INSERT INTO {first} SELECT * FROM EXTERNAL '{import_id}' USING (REMOTESOURCE 'jdbc' DELIMITER '|' LOGDIR '{log_dir}')"
        ), &[], &import_id, std::io::Cursor::new(data))?;
        conn.batch_execute(&format!(
            "CREATE EXTERNAL TABLE '{file_sql}' USING (REMOTESOURCE 'jdbc' DELIMITER '|' LOGDIR '{log_dir}') AS SELECT * FROM {first} ORDER BY id"
        ))?;
        assert!(std::fs::metadata(&file).unwrap().len() > 65_536);
        conn.batch_execute(&format!(
            "INSERT INTO {second} SELECT * FROM EXTERNAL '{file_sql}' USING (REMOTESOURCE 'jdbc' DELIMITER '|' LOGDIR '{log_dir}')"
        ))?;
        let row = conn
            .query(
                &format!("SELECT COUNT(*), MIN(id), MAX(id) FROM {second}"),
                &[],
            )?
            .rows()[0]
            .clone();
        assert_eq!(row.try_get::<_, i64>(0).unwrap(), expected_rows as i64);
        assert_eq!(row.try_get::<_, i32>(1).unwrap(), 0);
        assert_eq!(row.try_get::<_, i32>(2).unwrap(), expected_rows as i32 - 1);
        Ok(())
    })();
    let _ = conn.batch_execute(&format!("DROP TABLE {first}"));
    let _ = conn.batch_execute(&format!("DROP TABLE {second}"));
    let _ = std::fs::remove_file(&file);
    result.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_metadata_helpers_reconstruct_table_view_and_procedure() {
    let Some(config) = config() else { return };
    let mut conn = NzConnection::connect(&config).unwrap();
    let table = unique_name("RUST_META_T");
    let view = unique_name("RUST_META_V");
    let procedure = unique_name("RUST_META_P");
    let synonym = unique_name("RUST_META_S");
    let external = unique_name("RUST_META_E");
    conn.batch_execute(&format!(
        "CREATE TABLE {table}(\"SELECT\" INTEGER, name VARCHAR(30)) DISTRIBUTE ON (\"SELECT\")"
    ))
    .unwrap();
    conn.batch_execute(&format!(
        "CREATE VIEW {view} AS SELECT \"SELECT\", name FROM {table}"
    ))
    .unwrap();
    conn.batch_execute(&format!(
        "COMMENT ON VIEW {view} IS 'DDL round-trip view comment'"
    ))
    .unwrap();
    conn.batch_execute(&format!(
        "COMMENT ON COLUMN {view}.\"SELECT\" IS 'DDL round-trip view column comment'"
    ))
    .unwrap();
    conn.batch_execute(&format!(
        "CREATE OR REPLACE PROCEDURE {procedure}() RETURNS INTEGER EXECUTE AS OWNER LANGUAGE NZPLSQL AS BEGIN_PROC BEGIN RETURN 1; END; END_PROC;"
    )).unwrap();
    conn.batch_execute(&format!(
        "COMMENT ON PROCEDURE {procedure}() IS 'DDL round-trip comment'"
    ))
    .unwrap();
    conn.batch_execute(&format!("CREATE SYNONYM {synonym} FOR {table}"))
        .unwrap();
    conn.batch_execute(&format!(
        "COMMENT ON SYNONYM {synonym} IS 'DDL round-trip comment'"
    ))
    .unwrap();
    conn.batch_execute(&format!(
        "CREATE EXTERNAL TABLE {external}(id INTEGER, label CHAR(10), event_date DATE) USING (DATAOBJECT('/tmp/{external}.txt') FORMAT 'FIXED' RECORDLENGTH 24 RECORDDELIM '\r\n' LAYOUT (BYTES 4, BYTES 10, DATE YMD ' ' BYTES 10))"
    )).unwrap();
    let native = nz_rust::Client::connect(&config).await.unwrap();
    let native_table = native
        .metadata()
        .table_ddl(&table, None, None)
        .await
        .unwrap();
    let native_view = native.metadata().view_ddl(&view, None, None).await.unwrap();
    let native_synonym = native
        .metadata()
        .synonym_ddl(&synonym, None, None)
        .await
        .unwrap();
    let native_external = native
        .metadata()
        .external_table_ddl(&external, None, None)
        .await
        .unwrap();
    let native_procedure = native
        .metadata()
        .procedure_ddl(&procedure, None, None)
        .await
        .unwrap();
    let native_table_batch = native
        .metadata()
        .tables_ddl(None, None, Some(std::slice::from_ref(&table)))
        .await
        .unwrap();
    let native_procedure_batch = native
        .metadata()
        .procedures_ddl(None, None, Some(std::slice::from_ref(&procedure)))
        .await
        .unwrap();
    let native_view_batch = native
        .metadata()
        .views_ddl(None, None, Some(std::slice::from_ref(&view)))
        .await
        .unwrap();
    let result = (|| -> Result<(), NzError> {
        let metadata = &mut conn.metadata();
        assert!(metadata.current_database()?.is_some());
        let table_ddl = metadata.table_ddl(&table, None, None)?;
        let view_ddl = metadata.view_ddl(&view, None, None)?;
        let procedure_ddl = metadata.procedure_ddl(&procedure, None, None)?;
        assert!(table_ddl.contains("CREATE TABLE"));
        assert_eq!(native_table, table_ddl);
        assert_eq!(native_table_batch.len(), 1);
        assert_eq!(native_table_batch[0].error, None);
        assert_eq!(native_table_batch[0].ddl, table_ddl);
        assert!(view_ddl.contains("CREATE OR REPLACE VIEW"));
        assert_eq!(native_view, view_ddl);
        assert_eq!(native_view_batch.len(), 1);
        assert_eq!(native_view_batch[0].error, None);
        assert_eq!(native_view_batch[0].ddl, view_ddl);
        assert!(view_ddl.contains("DDL round-trip view comment"));
        assert!(view_ddl.contains("DDL round-trip view column comment"));
        let view_batch = metadata.views_ddl(None, None, Some(std::slice::from_ref(&view)))?;
        assert_eq!(view_batch.len(), 1);
        assert!(view_batch[0].ddl.contains("DDL round-trip view comment"));
        assert!(view_batch[0]
            .ddl
            .contains("DDL round-trip view column comment"));
        assert!(procedure_ddl.contains("CREATE OR REPLACE PROCEDURE"));
        assert_eq!(native_procedure, procedure_ddl);
        assert_eq!(native_procedure_batch.len(), 1);
        assert_eq!(native_procedure_batch[0].error, None);
        assert_eq!(native_procedure_batch[0].ddl, procedure_ddl);
        let synonym_ddl = metadata.synonym_ddl(&synonym, None, None)?;
        let external_ddl = metadata.external_table_ddl(&external, None, None)?;
        assert!(synonym_ddl.contains("CREATE SYNONYM"));
        assert_eq!(native_synonym, synonym_ddl);
        assert!(external_ddl.contains("CREATE EXTERNAL TABLE"));
        assert_eq!(native_external, external_ddl);
        assert_eq!(
            metadata
                .tables_ddl(None, None, Some(std::slice::from_ref(&table)))?
                .len(),
            1
        );
        conn.batch_execute(&format!("DROP VIEW {view}"))?;
        conn.batch_execute(&format!("DROP PROCEDURE {procedure}()"))?;
        conn.batch_execute(&format!("DROP SYNONYM {synonym}"))?;
        conn.batch_execute(&format!("DROP TABLE {external}"))?;
        conn.batch_execute(&format!("DROP TABLE {table}"))?;
        conn.batch_execute(&table_ddl)?;
        conn.batch_execute(&view_ddl)?;
        conn.batch_execute(&procedure_ddl)?;
        conn.batch_execute(&synonym_ddl)?;
        conn.batch_execute(&external_ddl)?;
        Ok(())
    })();
    let _ = conn.batch_execute(&format!("DROP VIEW {view}"));
    let _ = conn.batch_execute(&format!("DROP PROCEDURE {procedure}()"));
    let _ = conn.batch_execute(&format!("DROP SYNONYM {synonym}"));
    let _ = conn.batch_execute(&format!("DROP TABLE {external}"));
    let _ = conn.batch_execute(&format!("DROP TABLE {table}"));
    native.close().await.unwrap();
    result.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_native_regressions_long_text_exact_numeric_parameters_and_pool_drop() {
    let Some(config) = config() else {
        return;
    };
    let client = nz_rust::Client::connect(&config).await.unwrap();
    let rows = client.query("SELECT repeat('x',40000)::VARCHAR(64000) AS v, 'ABC'::CHAR(10) AS c, 99999999999999999999999999999999999999::NUMERIC(38,0) AS n, 123::INTEGER AS i FROM JUST_DATA..FACTPRODUCTINVENTORY LIMIT 1", &[]).await.unwrap();
    assert_eq!(rows[0].try_get::<_, String>(0).unwrap().len(), 40000);
    assert_eq!(rows[0].try_get_raw_typed::<_, &str>(1).unwrap(), "ABC");
    assert_eq!(
        rows[0]
            .try_get::<_, nz_rust::NzNumeric>(2)
            .unwrap()
            .to_string(),
        "99999999999999999999999999999999999999"
    );
    assert_eq!(rows[0].try_get::<_, i32>(3).unwrap(), 123);
    let value = "a\\b'c";
    assert_eq!(
        client
            .query_one("SELECT $1::VARCHAR(100)", &[&value])
            .await
            .unwrap()
            .try_get::<_, String>(0)
            .unwrap(),
        value
    );
    assert!(client
        .query_one("SELECT 1 UNION ALL SELECT 2", &[])
        .await
        .is_err());
    client.close().await.unwrap();
    let mut options = nz_rust::AsyncNzPoolConfig::new(config);
    options.max = 1;
    options.wait_timeout = Some(std::time::Duration::from_secs(10));
    let pool = nz_rust::AsyncNzPool::new(options).unwrap();
    drop(pool.get().await.unwrap());
    let holder = pool.get().await.unwrap();
    assert_eq!(
        holder.query("SELECT 123", &[]).await.unwrap()[0]
            .try_get::<_, i32>(0)
            .unwrap(),
        123
    );
    holder.release().await;
    assert_eq!(pool.idle_count().await, 1);
    pool.end().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_native_close_completes_while_stream_consumer_is_paused() {
    let Some(config) = config() else {
        return;
    };
    let client = nz_rust::Client::connect(&config).await.unwrap();
    let rows = client
        .query_stream(
            "SELECT 1 FROM JUST_DATA..FACTPRODUCTINVENTORY LIMIT 100000",
            &[],
        )
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    tokio::time::timeout(std::time::Duration::from_secs(8), client.close())
        .await
        .unwrap()
        .unwrap();
    assert!(client.is_closed());
    drop(rows);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_native_metadata_snapshot_and_numeric_temporal_getters() {
    let Some(config) = config() else {
        return;
    };
    let client = nz_rust::Client::connect(&config).await.unwrap();
    let snapshot = client.metadata().snapshot(Some("ADMIN")).await.unwrap();
    assert!(!snapshot.schemas.is_empty());
    assert!(!snapshot.databases.is_empty());
    assert_eq!(
        snapshot.tables,
        client.metadata().tables(Some("ADMIN"), None).await.unwrap()
    );
    for suffix in ["", " FROM JUST_DATA..FACTPRODUCTINVENTORY LIMIT 1"] {
        let sql = format!("SELECT '2000-01-01'::DATE, '01:02:03.123456'::TIME, '2000-01-02 01:02:03.123456'::TIMESTAMP, '-13 months -1 microsecond'::INTERVAL, '100 hours'::INTERVAL, 3.1400::NUMERIC(10,4){suffix}");
        let row = client.query_one(&sql, &[]).await.unwrap();
        assert_eq!(row.try_get::<_, nz_rust::NzDate>(0).unwrap().days, 0);
        assert_eq!(
            row.try_get::<_, nz_rust::NzTime>(1).unwrap().microseconds(),
            3_723_123_456
        );
        assert_eq!(
            row.try_get::<_, nz_rust::NzTimestamp>(2)
                .unwrap()
                .microseconds,
            86_400_000_000 + 3_723_123_456
        );
        assert_eq!(
            row.try_get::<_, nz_rust::NzInterval>(3).unwrap(),
            nz_rust::NzInterval {
                months: -13,
                microseconds: -1
            }
        );
        assert_eq!(
            row.try_get::<_, nz_rust::NzInterval>(4)
                .unwrap()
                .microseconds,
            360_000_000_000
        );
        assert_eq!(
            row.try_get::<_, Option<nz_rust::NzNumeric>>(5)
                .unwrap()
                .unwrap()
                .to_string(),
            "3.1400"
        );
    }
    client.close().await.unwrap();
}

#[test]
fn live_primary_blocking_iterator_transaction_and_pool_cleanup() {
    let Some(config) = config() else {
        return;
    };
    let mut client = nz_rust::blocking::Client::connect(&config).unwrap();
    let mut rows = client
        .query_iter(
            "SELECT 1 FROM JUST_DATA..FACTPRODUCTINVENTORY LIMIT 100000",
            &[],
        )
        .unwrap();
    assert_eq!(
        rows.next().unwrap().unwrap().try_get::<_, i32>(0).unwrap(),
        1
    );
    drop(rows);
    assert_eq!(
        client
            .query_one("SELECT 123", &[])
            .unwrap()
            .try_get::<_, i32>(0)
            .unwrap(),
        123
    );
    {
        let mut transaction = client.transaction().unwrap();
        assert_eq!(
            transaction.query("SELECT 7", &[]).unwrap()[0]
                .try_get::<_, i32>(0)
                .unwrap(),
            7
        );
    }
    assert_eq!(client.query("SELECT 123", &[]).unwrap().len(), 1);
    client.close().unwrap();
    let mut options = nz_rust::blocking::PoolConfig::new(config);
    options.max = 1;
    options.wait_timeout = Some(std::time::Duration::from_secs(10));
    let pool = nz_rust::blocking::Pool::connect(options).unwrap();
    drop(pool.get().unwrap());
    let mut holder = pool.get().unwrap();
    assert_eq!(holder.query("SELECT 7", &[]).unwrap().len(), 1);
    holder.release();
    assert_eq!(pool.idle_count(), 1);
    pool.close();
}
