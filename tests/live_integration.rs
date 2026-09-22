//! Live integration suite — a Rust port of the Node `*.test.js` and C#
//! `*.Tests` live-DB suites.
//!
//! Every test is skipped (reported as passing) unless `NZ_RUN_LIVE_TESTS=1`;
//! it then talks to the appliance configured through the standard lab
//! environment variables:
//!
//! ```text
//! NZ_RUN_LIVE_TESTS=1 \
//! NZ_DEV_HOST=your_netezza_host NZ_DEV_PORT=5480 \
//! NZ_DEV_DATABASE=JUST_DATA NZ_DEV_USER=admin NZ_DEV_PASSWORD=password \
//!   cargo test -p nz_rust --test live_integration -- --nocapture --test-threads=1
//! ```
//!
//! Ported areas: type matrix (text + binary paths), NULL handling, parameters,
//! multi-result sets / `hasRows` / `nextResult` boundaries, transactions,
//! notices, invalid SQL, authentication failure, pooling, schema table.

use nz_rust::types::value::NzValue;
use nz_rust::{NzConnection, NzConnectionConfig, NzError, NzPool, NzPoolConfig};
use std::process;
use std::thread;
use std::time::{Duration, Instant};

const TABLE: &str = "JUST_DATA..DIMDATE";

fn cfg() -> Option<NzConnectionConfig> {
    if std::env::var("NZ_RUN_LIVE_TESTS").ok().as_deref() != Some("1") {
        return None;
    }
    let host = std::env::var("NZ_DEV_HOST").ok()?;
    Some(NzConnectionConfig {
        host,
        port: std::env::var("NZ_DEV_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(5480),
        database: std::env::var("NZ_DEV_DB")
            .or_else(|_| std::env::var("NZ_DEV_DATABASE"))
            .unwrap_or_else(|_| "JUST_DATA".into()),
        user: std::env::var("NZ_DEV_USER").unwrap_or_else(|_| "admin".into()),
        password: std::env::var("NZ_DEV_PASSWORD").expect("NZ_DEV_PASSWORD is required"),
        ..Default::default()
    })
}

macro_rules! live {
    () => {
        match cfg() {
            Some(c) => c,
            None => {
                eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
                return;
            }
        }
    };
}

/// Connect for a live test, or `None` when live tests are not enabled.
fn live_conn() -> Option<NzConnection> {
    Some(NzConnection::connect(&cfg()?).expect("connect"))
}

fn as_f64(v: &NzValue) -> f64 {
    v.to_display_string().parse().expect("numeric")
}

fn unique(suffix: &str) -> String {
    format!("RUST_{}_{}", process::id(), suffix)
}

// ---------------------------------------------------------------------------
// Connection / basic scalar types
// ---------------------------------------------------------------------------

#[test]
fn connection_open_and_close() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    assert!(!conn.is_closed());
    conn.close();
    assert!(conn.is_closed());
}

#[test]
fn metadata_catalog_surface_matches_reference_drivers() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };

    let schemas = conn.metadata().schemas().expect("schemas");
    assert!(!schemas.is_empty());

    let databases = conn.metadata().databases().expect("databases");
    assert!(!databases.is_empty());

    let tables = conn
        .metadata()
        .tables(None, Some("DIMDATE"))
        .expect("tables");
    assert!(tables
        .iter()
        .any(|table| table.name.eq_ignore_ascii_case("DIMDATE")));

    let columns = conn
        .metadata()
        .columns("DIMDATE", Some("ADMIN"))
        .expect("columns");
    assert!(!columns.is_empty());

    let _ = conn.metadata().views(None).expect("views");
    let _ = conn.metadata().procedures(None).expect("procedures");
    let _ = conn.metadata().table_sizes(None).expect("table sizes");
    let _ = conn.metadata().sessions().expect("sessions");
    let _ = conn.metadata().functions(None).expect("functions");
    let _ = conn.metadata().synonyms(None).expect("synonyms");
    let _ = conn
        .metadata()
        .constraints(Some("ADMIN"))
        .expect("constraints");
    let _ = conn
        .metadata()
        .all_distribution_keys(Some("ADMIN"))
        .expect("distribution keys");
    let _ = conn
        .metadata()
        .organize_keys(Some("ADMIN"))
        .expect("organize keys");
    let details = conn
        .metadata()
        .object_details(Some("ADMIN"))
        .expect("object details");
    assert!(!details.is_empty());
    let _ = conn
        .metadata()
        .search_objects_detailed("DIM", Some("ADMIN"))
        .expect("detailed object search");
}

#[test]
fn integer_types_text_and_binary() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    for (sql, expected) in [
        ("SELECT 1", 1i64),
        ("SELECT 15::BYTEINT", 15),
        ("SELECT 1234::SMALLINT", 1234),
        ("SELECT 9223372036854775807::BIGINT", i64::MAX),
    ] {
        for query in [sql.to_string(), format!("{sql} FROM {TABLE} LIMIT 1")] {
            let r = conn.query(&query, &[]).unwrap();
            let v = r.rows()[0].raw(0);
            assert_eq!(as_f64(v) as i64, expected, "query {query}");
        }
    }
}

#[test]
fn float_and_numeric_types() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    for query in [
        "SELECT 3.14::FLOAT".to_string(),
        format!("SELECT 3.14::FLOAT FROM {TABLE} LIMIT 1"),
        "SELECT 3.14159265358979::DOUBLE PRECISION".to_string(),
        format!("SELECT 3.14159265358979::DOUBLE PRECISION FROM {TABLE} LIMIT 1"),
    ] {
        let r = conn.query(&query, &[]).unwrap();
        let v = as_f64(r.rows()[0].raw(0));
        let three = 3.0f64;
        let short_pi = three + 0.14;
        let long_pi = std::f64::consts::PI - 2e-14;
        assert!(
            (v - short_pi).abs() < 0.01 || (v - long_pi).abs() < 1e-10,
            "{query} -> {v}"
        );
    }

    for query in [
        "SELECT 12345.6789::NUMERIC(10,4)".to_string(),
        format!("SELECT 12345.6789::NUMERIC(10,4) FROM {TABLE} LIMIT 1"),
    ] {
        let r = conn.query(&query, &[]).unwrap();
        assert!(
            (as_f64(r.rows()[0].raw(0)) - 12345.6789).abs() < 1e-4,
            "{query}"
        );
    }
}

#[test]
fn high_precision_numeric_is_exact() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    for query in [
        "SELECT 12345678901234567890.1234567890::NUMERIC(38,10)".to_string(),
        format!("SELECT 12345678901234567890.1234567890::NUMERIC(38,10) FROM {TABLE} LIMIT 1"),
    ] {
        let r = conn.query(&query, &[]).unwrap();
        let s = r.rows()[0].try_get::<_, String>(0).unwrap();
        assert!(s.contains("12345678901234567890"), "{query} -> {s}");
    }
}

#[test]
fn string_types_unicode() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    for query in [
        "SELECT 'Hello World'::VARCHAR(100)".to_string(),
        format!("SELECT 'Hello World'::VARCHAR(100) FROM {TABLE} LIMIT 1"),
    ] {
        let r = conn.query(&query, &[]).unwrap();
        assert_eq!(r.rows()[0].try_get::<_, String>(0).unwrap(), "Hello World");
    }
    for query in [
        "SELECT 'ABC'::CHAR(10)".to_string(),
        format!("SELECT 'ABC'::CHAR(10) FROM {TABLE} LIMIT 1"),
    ] {
        let r = conn.query(&query, &[]).unwrap();
        assert_eq!(r.rows()[0].try_get::<_, String>(0).unwrap().trim(), "ABC");
    }
    for query in [
        "SELECT 'Zażółć gęślą jaźń'::NVARCHAR(100)".to_string(),
        format!("SELECT 'Zażółć gęślą jaźń'::NVARCHAR(100) FROM {TABLE} LIMIT 1"),
    ] {
        let r = conn.query(&query, &[]).unwrap();
        let s = r.rows()[0].try_get::<_, String>(0).unwrap();
        assert!(s.contains("Zażółć"), "{query} -> {s}");
    }
}

#[test]
fn boolean_type() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    for (sql, expected) in [
        ("SELECT true::BOOLEAN", true),
        ("SELECT false::BOOLEAN", false),
    ] {
        for query in [sql.to_string(), format!("{sql} FROM {TABLE} LIMIT 1")] {
            let r = conn.query(&query, &[]).unwrap();
            assert_eq!(
                r.rows()[0].try_get::<_, bool>(0).unwrap(),
                expected,
                "{query}"
            );
        }
    }
}

#[test]
fn date_time_timestamp_interval() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };

    for query in [
        "SELECT '2024-12-11'::DATE".to_string(),
        format!("SELECT '2024-12-11'::DATE FROM {TABLE} LIMIT 1"),
    ] {
        let r = conn.query(&query, &[]).unwrap();
        assert_eq!(r.rows()[0].try_get::<_, String>(0).unwrap(), "2024-12-11");
    }

    for query in [
        "SELECT '12:30:45'::TIME".to_string(),
        format!("SELECT '12:30:45'::TIME FROM {TABLE} LIMIT 1"),
    ] {
        let r = conn.query(&query, &[]).unwrap();
        let s = r.rows()[0].try_get::<_, String>(0).unwrap();
        assert!(s.contains("12") && s.contains("30"), "{query} -> {s}");
    }

    for ts in [
        "2024-12-11 14:30:00",
        "2023-03-26 01:30:00",
        "2023-03-31 12:30:00",
    ] {
        for query in [
            format!("SELECT '{ts}'::TIMESTAMP"),
            format!("SELECT '{ts}'::TIMESTAMP FROM {TABLE} LIMIT 1"),
        ] {
            let r = conn.query(&query, &[]).unwrap();
            assert_eq!(r.rows()[0].try_get::<_, String>(0).unwrap(), ts, "{query}");
        }
    }

    for query in [
        "SELECT '5 hours 30 minutes'::INTERVAL".to_string(),
        format!("SELECT '5 hours 30 minutes'::INTERVAL FROM {TABLE} LIMIT 1"),
    ] {
        let r = conn.query(&query, &[]).unwrap();
        let s = r.rows()[0].try_get::<_, String>(0).unwrap();
        assert!(s.contains('5'), "{query} -> {s}");
    }
}

#[test]
fn null_handling() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    for query in [
        "SELECT NULL".to_string(),
        format!("SELECT NULL FROM {TABLE} LIMIT 1"),
        "SELECT NULL::INTEGER".to_string(),
        format!("SELECT NULL::INTEGER FROM {TABLE} LIMIT 1"),
    ] {
        let r = conn.query(&query, &[]).unwrap();
        assert!(r.rows()[0].raw(0).is_null(), "{query}");
    }
}

#[test]
fn mixed_null_and_non_null_strings() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    let r = conn
        .query(
            "SELECT NULL::VARCHAR(10) AS c1, 'abc' AS c2, NULL::NVARCHAR(10) AS c3, \
             'def' AS c4, NULL::NCHAR(10) AS c5",
            &[],
        )
        .unwrap();
    let row = &r.rows()[0];
    assert!(row.raw(0).is_null());
    assert!(row.raw(2).is_null());
    assert!(row.raw(4).is_null());
    assert_eq!(row.try_get::<_, String>(1).unwrap(), "abc");
    assert_eq!(row.try_get::<_, String>(3).unwrap(), "def");
}

#[test]
fn multiple_columns_and_version() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    for query in [
        "SELECT 1 AS col1, 'text' AS col2, 3.14 AS col3".to_string(),
        format!("SELECT 1 AS col1, 'text' AS col2, 3.14 AS col3 FROM {TABLE} LIMIT 1"),
    ] {
        let r = conn.query(&query, &[]).unwrap();
        assert_eq!(r.columns().len(), 3, "{query}");
    }

    let r = conn.query("SELECT version()", &[]).unwrap();
    let v = r.rows()[0].try_get::<_, String>(0).unwrap();
    assert!(v.contains("Release"), "version -> {v}");
}

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

#[test]
fn parameterised_queries() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    let params = vec![
        NzValue::Text("O'Brien".into()),
        NzValue::Int4(7),
        NzValue::Float8(1.5),
    ];
    let r = conn
        .query_values("SELECT $1 AS name, $2 AS value, $3 AS f", &params)
        .unwrap();
    let row = &r.rows()[0];
    assert_eq!(row.try_get::<_, String>(0).unwrap(), "O'Brien");
    assert_eq!(row.try_get::<_, i32>(1).unwrap(), 7);
    assert_eq!(row.try_get::<_, f64>(2).unwrap(), 1.5);

    // Null parameter and typed ToSql slice.
    let r = conn
        .query("SELECT $1::integer AS x", &[&Option::<i32>::None])
        .unwrap();
    assert!(r.rows()[0].raw(0).is_null());
    let r = conn.query("SELECT $1 AS x", &[&42i64]).unwrap();
    assert_eq!(r.rows()[0].try_get::<_, i64>(0).unwrap(), 42);
}

// ---------------------------------------------------------------------------
// Multi-result sets / hasRows / boundaries
// ---------------------------------------------------------------------------

#[test]
fn multi_result_sets() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    let r = conn
        .query(
            &format!("SELECT 1 FROM {TABLE} LIMIT 1; SELECT 2 FROM {TABLE} LIMIT 1"),
            &[],
        )
        .unwrap();
    assert_eq!(r.result_sets.len(), 2);
    assert_eq!(r.result_sets[0].rows[0].try_get::<_, i32>(0).unwrap(), 1);
    assert_eq!(r.result_sets[1].rows[0].try_get::<_, i32>(0).unwrap(), 2);

    let r = conn
        .query("SELECT 1,2;SELECT 2,3; SELECT 3,4; SELECT 4,5", &[])
        .unwrap();
    assert_eq!(r.result_sets.len(), 4);
}

/// Port of the C#/Node `HasRowsTests` helper — drives the reader exactly the
/// way the reference suite does.
fn has_rows_list(conn: &mut NzConnection, sql: &str) -> Vec<bool> {
    let mut reader = conn.execute_reader(sql, &[]).unwrap();
    let mut results = Vec::new();
    loop {
        results.push(reader.has_rows());
        while reader.read().unwrap() {}
        if !reader.next_result().unwrap() {
            break;
        }
    }
    results
}

#[test]
fn has_rows_semantics() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };

    let results = has_rows_list(&mut conn, "delete from JUST_DATA..DIMDATE where 1=2;");
    assert_eq!(results, vec![false]);

    let mixed = format!(
        "SELECT * FROM {TABLE} ORDER BY ROWID LIMIT 0;\
         SELECT * FROM {TABLE} ORDER BY ROWID LIMIT 1;\
         SELECT * FROM {TABLE} ORDER BY ROWID LIMIT 0;\
         SELECT * FROM {TABLE} ORDER BY ROWID LIMIT 1;"
    );
    assert_eq!(
        has_rows_list(&mut conn, &mixed),
        vec![false, true, false, true]
    );

    let deletes_then_select = "delete from JUST_DATA..DIMDATE where 1=2;\
         delete from JUST_DATA..DIMDATE where 1=2;select 10";
    assert_eq!(has_rows_list(&mut conn, deletes_then_select), vec![true]);

    let deletes_then_select2 = format!(
        "SELECT * FROM {TABLE} ORDER BY ROWID LIMIT 0;\
         SELECT * FROM {TABLE} ORDER BY ROWID LIMIT 1;\
         SELECT * FROM {TABLE} ORDER BY ROWID LIMIT 0;\
         delete from JUST_DATA..DIMDATE where 1=2;\
         delete from JUST_DATA..DIMDATE where 1=2;\
         SELECT 11 FROM {TABLE} ORDER BY ROWID LIMIT 10"
    );
    assert_eq!(
        has_rows_list(&mut conn, &deletes_then_select2),
        vec![false, true, false, true]
    );
}

#[test]
fn reader_next_result_navigation() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    let mut reader = conn
        .execute_reader(
            &format!("SELECT 1 AS value FROM {TABLE} ORDER BY ROWID LIMIT 3; SELECT 99 AS value"),
            &[],
        )
        .unwrap();

    assert!(reader.read().unwrap());
    assert_eq!(reader.get_i32(0).unwrap(), 1);
    assert!(reader.next_result().unwrap());
    assert_eq!(reader.field_count(), 1);
    assert!(reader.read().unwrap());
    assert_eq!(reader.get_i32(0).unwrap(), 99);
    assert!(!reader.read().unwrap());
    assert!(!reader.next_result().unwrap());
}

#[test]
fn reader_row_boundaries() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    for limit in [0usize, 1, 499, 500, 501, 1000] {
        let mut reader = conn
            .execute_reader(
                &format!("SELECT * FROM {TABLE} ORDER BY ROWID LIMIT {limit}"),
                &[],
            )
            .unwrap();
        let mut rows = 0;
        while reader.read().unwrap() {
            rows += 1;
        }
        assert_eq!(rows, limit, "limit {limit}");
    }
}

#[test]
fn close_after_partial_read_keeps_session_aligned() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    {
        let mut reader = conn
            .execute_reader(
                &format!("SELECT * FROM {TABLE} ORDER BY ROWID LIMIT 501"),
                &[],
            )
            .unwrap();
        for row in 1..=499 {
            assert!(reader.read().unwrap(), "row {row}");
        }
        reader.close();
    }
    let r = conn.query("SELECT 123 AS value", &[]).unwrap();
    assert_eq!(r.rows()[0].try_get::<_, i32>(0).unwrap(), 123);
}

// ---------------------------------------------------------------------------
// Reader API
// ---------------------------------------------------------------------------

#[test]
fn reader_iteration_and_typed_getters() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    let mut reader = conn
        .execute_reader(
            &format!("SELECT 1 as num, 'abc' as txt FROM {TABLE} LIMIT 3"),
            &[],
        )
        .unwrap();
    let mut count = 0;
    while reader.read().unwrap() {
        assert_eq!(reader.field_count(), 2);
        assert_eq!(reader.get_name(0).unwrap(), "NUM");
        assert_eq!(reader.get_name(1).unwrap(), "TXT");
        count += 1;
    }
    assert_eq!(count, 3);

    let mut reader = conn
        .execute_reader(
            &format!(
                "SELECT 42 as int_col, 3.14 as float_col, 'hello' as str_col FROM {TABLE} LIMIT 1"
            ),
            &[],
        )
        .unwrap();
    assert!(reader.read().unwrap());
    assert_eq!(reader.get_i32(0).unwrap(), 42);
    assert!((reader.get_f64(1).unwrap() - (3.0 + 0.14)).abs() < 0.01);
    assert_eq!(reader.get_string(2).unwrap().as_deref(), Some("hello"));
}

#[test]
fn schema_table_reflects_columns() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    let reader = conn
        .execute_reader(
            "SELECT CAST(42 AS INTEGER) AS INT_COL, \
             CAST('2024-01-01' AS DATE) AS DATE_COL, \
             CAST(123.45 AS NUMERIC(10,2)) AS NUMERIC_COL, \
             CAST('x' AS VARCHAR(17)) AS TXT_COL",
            &[],
        )
        .unwrap();
    let schema = reader.get_schema_table().unwrap();
    assert_eq!(schema.columns_count, 4);

    let by_name = |n: &str| {
        schema
            .rows
            .iter()
            .find(|r| r.column_name.eq_ignore_ascii_case(n))
            .unwrap_or_else(|| panic!("missing {n}"))
    };
    assert_eq!(by_name("INT_COL").provider_type, 23);
    assert_eq!(by_name("NUMERIC_COL").numeric_precision, 10);
    assert_eq!(by_name("NUMERIC_COL").numeric_scale, 2);
    assert_eq!(by_name("TXT_COL").column_size, 17);
    assert_eq!(by_name("TXT_COL").column_ordinal, 4);
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

#[test]
fn transaction_rollback() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    let table = unique("TX_RB");
    conn.batch_execute(&format!("DROP TABLE {table} IF EXISTS"))
        .unwrap();

    conn.begin_transaction().unwrap();
    assert!(conn.in_transaction());
    conn.batch_execute(&format!(
        "CREATE TABLE {table}(c1 numeric(10,5), c2 varchar(10), c3 nchar(5))"
    ))
    .unwrap();
    conn.batch_execute(&format!(
        "INSERT INTO {table} VALUES (123.54, 'xcfd', 'xyz')"
    ))
    .unwrap();
    conn.rollback().unwrap();
    assert!(!conn.in_transaction());

    let err = conn.query(&format!("SELECT * FROM {table}"), &[]);
    assert!(
        matches!(err, Err(NzError::Database(_))),
        "table should not exist after rollback"
    );
}

#[test]
fn transaction_commit() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    let table = unique("TX_CM");
    conn.batch_execute(&format!("DROP TABLE {table} IF EXISTS"))
        .unwrap();

    conn.begin_transaction().unwrap();
    conn.batch_execute(&format!(
        "CREATE TABLE {table}(c1 numeric(10,5), c2 varchar(10), c3 nchar(5))"
    ))
    .unwrap();
    conn.batch_execute(&format!(
        "INSERT INTO {table} VALUES (123.54, 'xcfd', 'xyz')"
    ))
    .unwrap();
    conn.commit().unwrap();

    let r = conn.query(&format!("SELECT * FROM {table}"), &[]).unwrap();
    assert_eq!(r.row_count(), 1);
    assert!((as_f64(r.rows()[0].raw(0)) - 123.54).abs() < 0.01);
    conn.batch_execute(&format!("DROP TABLE {table} IF EXISTS"))
        .unwrap();
}

#[test]
fn transaction_closure_commits_and_rolls_back() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    let table = unique("TX_FN");
    conn.batch_execute(&format!("DROP TABLE {table} IF EXISTS"))
        .unwrap();

    conn.transaction(|c| {
        c.batch_execute(&format!("CREATE TABLE {table}(x int)"))?;
        c.batch_execute(&format!("INSERT INTO {table} VALUES (1)"))?;
        Ok(())
    })
    .unwrap();
    let r = conn.query(&format!("SELECT x FROM {table}"), &[]).unwrap();
    assert_eq!(r.row_count(), 1);

    let res: Result<(), NzError> = conn.transaction(|c| {
        c.batch_execute(&format!("INSERT INTO {table} VALUES (2)"))?;
        Err(NzError::Config("intentional".into()))
    });
    assert!(res.is_err());
    let r = conn.query(&format!("SELECT x FROM {table}"), &[]).unwrap();
    assert_eq!(r.row_count(), 1, "rollback must discard the second insert");

    conn.batch_execute(&format!("DROP TABLE {table} IF EXISTS"))
        .unwrap();
}

// ---------------------------------------------------------------------------
// Notices, errors, authentication
// ---------------------------------------------------------------------------

#[test]
fn notices_from_procedure_are_collected() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    let proc = format!("JUST_DATA.ADMIN.{}", unique("NOTICE"));
    // Clean up any leftovers first.
    let _ = conn.batch_execute(&format!("DROP PROCEDURE {proc}"));

    let create = format!(
        "CREATE OR REPLACE PROCEDURE {proc}() RETURNS INTEGER EXECUTE AS OWNER LANGUAGE NZPLSQL AS \
         BEGIN_PROC BEGIN RAISE NOTICE 'The customer name is alpha'; \
         RAISE NOTICE 'The customer location is beta'; END; END_PROC;"
    );
    conn.batch_execute(&create).unwrap();

    let result = conn.query(&format!("CALL {proc}();"), &[]).unwrap();
    let joined = result.notices.join("\n");
    assert!(
        joined.contains("The customer name is alpha"),
        "notices: {joined:?}"
    );
    assert!(
        joined.contains("The customer location is beta"),
        "notices: {joined:?}"
    );

    let _ = conn.batch_execute(&format!("DROP PROCEDURE {proc}"));
}

#[test]
fn invalid_sql_throws() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1 to run against a live appliance");
        return;
    };
    for sql in [
        "SELECT 1,,2;SELECT 1,2",
        "SELECT 1/0",
        "SELECT 'X'::INT",
        "SELECT * FROM NO_SUCH_TABLE_RUST_XYZ",
    ] {
        let err = conn.query(sql, &[]);
        assert!(
            matches!(err, Err(NzError::Database(_))),
            "expected error for {sql}: {err:?}"
        );
    }
    // The session stays usable after a clean SQL error.
    let r = conn.query("SELECT 1", &[]).unwrap();
    assert_eq!(r.rows()[0].try_get::<_, i32>(0).unwrap(), 1);
}

#[test]
fn invalid_password_is_rejected() {
    let Some(mut c) = cfg() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1");
        return;
    };
    c.password = "definitely-not-the-password".into();
    let err = NzConnection::connect(&c);
    assert!(err.is_err(), "connect with bad password must fail");
}

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

#[test]
fn pool_basics() {
    let pool = NzPool::new(NzPoolConfig::new(live!())).unwrap();
    pool.execute("SELECT 1", &[]).unwrap();
    let r = pool.query("SELECT 12345 AS val", &[]).unwrap();
    assert_eq!(r.row_count(), 1);
    assert_eq!(r.rows()[0].try_get::<_, i32>(0).unwrap(), 12345);
    assert_eq!(pool.total_count(), 1);
    assert_eq!(pool.idle_count(), 1);
    pool.close();
}

#[test]
fn pool_max_connections_and_release() {
    let mut cfg = NzPoolConfig::new(live!());
    cfg.max = 2;
    let pool = NzPool::new(cfg).unwrap();

    let h1 = pool.get().unwrap();
    let h2 = pool.get().unwrap();
    assert_eq!(pool.total_count(), 2);
    assert_eq!(pool.idle_count(), 0);
    drop(h1);
    drop(h2);
    assert_eq!(pool.idle_count(), 2);
    pool.close();
}

#[test]
fn pool_keeps_session_after_sql_error() {
    let pool = NzPool::new(NzPoolConfig::new(live!())).unwrap();
    {
        let mut holder = pool.get().unwrap();
        holder
            .batch_execute(&format!(
                "CREATE TEMP TABLE {} (X INT)",
                unique("KEEPALIVE")
            ))
            .unwrap();
    }
    let err = pool.query("SELECT * FROM NO_SUCH_TABLE_RUST_POOL", &[]);
    assert!(matches!(err, Err(NzError::Database(_))));
    assert_eq!(pool.total_count(), 1);
    assert_eq!(pool.idle_count(), 1);
    pool.close();
}

#[test]
fn pool_rolls_back_open_transaction_on_release() {
    let mut cfg = NzPoolConfig::new(live!());
    cfg.max = 1;
    let pool = NzPool::new(cfg).unwrap();
    let table = unique("POOL_TX");

    {
        let mut holder = pool.get().unwrap();
        holder
            .batch_execute(&format!("CREATE TEMP TABLE {table} (X INT)"))
            .unwrap();
        assert!(!holder.in_transaction());
        holder.begin_transaction().unwrap();
        assert!(holder.in_transaction());
        holder
            .batch_execute(&format!("INSERT INTO {table} VALUES (1)"))
            .unwrap();
    }

    let mut holder = pool.get().unwrap();
    assert!(
        !holder.in_transaction(),
        "release must have rolled back the transaction"
    );
    let r = holder
        .query(&format!("SELECT COUNT(*) FROM {table}"), &[])
        .unwrap();
    assert_eq!(as_f64(r.rows()[0].raw(0)) as i64, 0);
    pool.close();
}

#[test]
fn pool_tracks_transaction_state() {
    let pool = NzPool::new(NzPoolConfig::new(live!())).unwrap();
    let mut holder = pool.get().unwrap();
    holder.execute("BEGIN", &[]).unwrap();
    assert!(holder.in_transaction());
    holder.execute("COMMIT", &[]).unwrap();
    assert!(!holder.in_transaction());
    holder.execute("BEGIN", &[]).unwrap();
    holder.query("SELECT 1 AS n", &[]).unwrap();
    assert!(holder.in_transaction());
    holder.rollback().unwrap();
    assert!(!holder.in_transaction());
    pool.close();
}

// ---------------------------------------------------------------------------
// Cancellation / timeouts
// ---------------------------------------------------------------------------

/// Port of the reference `HEAVY_SQL` (TimeoutTests/CancelTests): a grouped
/// distinct-count over a 30000×30000 cartesian product — reliably slow.
const HEAVY_SQL: &str = "\
    SELECT F1.PRODUCTKEY, COUNT(DISTINCT (F1.PRODUCTKEY / F2.PRODUCTKEY)) \
    FROM ( SELECT * FROM JUST_DATA..FACTPRODUCTINVENTORY LIMIT 30000) F1, \
         ( SELECT * FROM JUST_DATA..FACTPRODUCTINVENTORY LIMIT 30000) F2 \
    GROUP BY 1 LIMIT 500";

#[test]
fn command_timeout_preserves_same_session_after_abort() {
    let Some(mut c) = cfg() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1");
        return;
    };
    c.command_timeout = 4;
    let mut conn = NzConnection::connect(&c).unwrap();
    let table = unique("TIMEOUT_SESSION_TEST");
    conn.batch_execute(&format!("CREATE TEMP TABLE {table} (COL1 INT)"))
        .unwrap();
    conn.batch_execute(&format!("INSERT INTO {table} VALUES (1)"))
        .unwrap();

    // Match the Python/C# regression: repeat the timeout on one backend and
    // prove that session-scoped state remains queryable after each abort.
    for iteration in 0..2 {
        let start = Instant::now();
        let err = conn.query(HEAVY_SQL, &[]);
        let elapsed = start.elapsed();
        assert!(err.is_err(), "heavy query must time out; got {err:?}");
        assert!(
            matches!(&err, Err(NzError::Timeout(_))),
            "expected timeout on iteration {iteration}: {err:?}"
        );
        assert!(
            elapsed >= Duration::from_secs(2),
            "timeout fired too early: {elapsed:?}"
        );

        let mut recovered = false;
        for _ in 0..20 {
            if let Ok(r) = conn.query(&format!("SELECT COL1 FROM {table}"), &[]) {
                assert_eq!(r.rows()[0].try_get::<_, i32>(0).unwrap(), 1);
                recovered = true;
                break;
            }
            thread::sleep(Duration::from_millis(500));
        }
        assert!(
            recovered,
            "the same session must survive command timeout on iteration {iteration}"
        );
    }
}

// ---------------------------------------------------------------------------
// AdditionalFullTests — large data, complex SQL, resilience, result sets
// ---------------------------------------------------------------------------

const DIMACCOUNT: &str = "JUST_DATA..DIMACCOUNT";

#[test]
fn fetch_1000_and_2000_rows() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1");
        return;
    };
    for limit in [1000usize, 2000] {
        let mut reader = conn
            .execute_reader(&format!("SELECT * FROM {TABLE} LIMIT {limit}"), &[])
            .unwrap();
        let mut count = 0;
        while reader.read().unwrap() {
            count += 1;
        }
        assert_eq!(count, limit);
    }
}

#[test]
fn fetch_all_rows_from_small_table() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1");
        return;
    };
    let mut reader = conn
        .execute_reader(&format!("SELECT * FROM {DIMACCOUNT}"), &[])
        .unwrap();
    let mut count = 0;
    while reader.read().unwrap() {
        for i in 0..reader.field_count() {
            reader.get_value(i).unwrap();
        }
        count += 1;
    }
    assert!(count > 0);
}

#[test]
fn subquery_union_and_cte() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1");
        return;
    };
    let r = conn
        .query("SELECT * FROM (SELECT 1 as x, 2 as y) sub", &[])
        .unwrap();
    assert_eq!(r.rows()[0].try_get::<_, i32>(0).unwrap(), 1);
    assert_eq!(r.rows()[0].try_get::<_, i32>(1).unwrap(), 2);

    let r = conn
        .query(
            "SELECT 1 as x UNION ALL SELECT 2 as x UNION ALL SELECT 3 as x",
            &[],
        )
        .unwrap();
    let vals: Vec<i32> = r
        .rows()
        .iter()
        .map(|row| row.try_get::<_, i32>(0).unwrap())
        .collect();
    assert_eq!(vals, vec![1, 2, 3]);

    let r = conn
        .query("WITH cte AS (SELECT 1 as x) SELECT * FROM cte", &[])
        .unwrap();
    assert_eq!(r.rows()[0].try_get::<_, i32>(0).unwrap(), 1);
}

#[test]
fn window_group_by_order_by_distinct() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1");
        return;
    };

    let r = conn
        .query(
            &format!(
                "SELECT ROW_NUMBER() OVER (ORDER BY DATEKEY) as rn, DATEKEY FROM {TABLE} LIMIT 10"
            ),
            &[],
        )
        .unwrap();
    let mut prev = 0i64;
    for row in r.rows() {
        let rn = row.try_get::<_, i32>(0).unwrap() as i64;
        assert!(rn > prev);
        prev = rn;
    }

    let r = conn
        .query(
            &format!("SELECT DATEKEY, COUNT(*) as cnt FROM {TABLE} GROUP BY DATEKEY HAVING COUNT(*) > 0 LIMIT 10"),
            &[],
        )
        .unwrap();
    assert!(!r.rows().is_empty());
    for row in r.rows() {
        assert!(as_f64(row.raw(1)) > 0.0);
    }

    let r = conn
        .query(
            &format!("SELECT DATEKEY FROM {TABLE} ORDER BY DATEKEY LIMIT 10"),
            &[],
        )
        .unwrap();
    let mut prev = i64::MIN;
    for row in r.rows() {
        let key = as_f64(row.raw(0)) as i64;
        assert!(key >= prev);
        prev = key;
    }

    let r = conn
        .query(
            &format!("SELECT DISTINCT DATEKEY FROM {TABLE} LIMIT 10"),
            &[],
        )
        .unwrap();
    assert!(!r.rows().is_empty());
}

#[test]
fn numeric_negative_and_bigint_min() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1");
        return;
    };

    let r = conn
        .query("SELECT 1234567890.123456789::NUMERIC(19,9)", &[])
        .unwrap();
    assert!((as_f64(r.rows()[0].raw(0)) - 1234567890.1234567).abs() < 1e-6);

    let r = conn
        .query("SELECT -123, -456.789, -999999999999999", &[])
        .unwrap();
    assert_eq!(r.rows()[0].try_get::<_, i32>(0).unwrap(), -123);
    assert!((as_f64(r.rows()[0].raw(1)) + 456.789).abs() < 1e-3);

    let r = conn
        .query("SELECT (-9223372036854775808)::BIGINT", &[])
        .unwrap();
    let s = r.rows()[0].raw(0).to_display_string();
    assert!(s.contains("9223372036854775808"), "bigint min -> {s}");
}

#[test]
fn date_time_edge_cases() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1");
        return;
    };

    for (sql, expected) in [
        ("SELECT '2024-02-29'::DATE", "2024-02-29"),
        ("SELECT '2024-01-31'::DATE", "2024-01-31"),
        ("SELECT '2024-04-30'::DATE", "2024-04-30"),
    ] {
        let r = conn.query(sql, &[]).unwrap();
        assert_eq!(r.rows()[0].try_get::<_, String>(0).unwrap(), expected);
    }

    let r = conn
        .query("SELECT '0001-01-01'::DATE, '9999-12-31'::DATE", &[])
        .unwrap();
    assert!(!r.rows()[0].raw(0).is_null());
    assert!(!r.rows()[0].raw(1).is_null());

    let r = conn
        .query("SELECT NOW(), CURRENT_DATE, CURRENT_TIME", &[])
        .unwrap();
    for i in 0..3 {
        assert!(!r.rows()[0].raw(i).is_null(), "column {i}");
    }
}

#[test]
fn empty_string_handling() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1");
        return;
    };
    let r = conn.query("SELECT ''::VARCHAR(10)", &[]).unwrap();
    let v = r.rows()[0].raw(0);
    assert!(v.is_null() || v.to_display_string().is_empty());
}

#[test]
fn connection_resilience_and_multiple_commands() {
    let Some(c) = cfg() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1");
        return;
    };

    // Multiple connections sequentially.
    for _ in 0..3 {
        let mut conn = NzConnection::connect(&c).unwrap();
        let r = conn.query("SELECT 1", &[]).unwrap();
        assert_eq!(r.rows()[0].try_get::<_, i32>(0).unwrap(), 1);
        conn.close();
    }

    // Connection stays usable after a failed query.
    let mut conn = NzConnection::connect(&c).unwrap();
    assert!(conn.query("SELECT INVALID", &[]).is_err());
    let r = conn.query("SELECT 1", &[]).unwrap();
    assert_eq!(r.rows()[0].try_get::<_, i32>(0).unwrap(), 1);

    // Multiple commands on the same connection.
    for i in 0..5 {
        let r = conn.query(&format!("SELECT {i}"), &[]).unwrap();
        assert_eq!(r.rows()[0].try_get::<_, i32>(0).unwrap(), i);
    }
}

#[test]
fn multiple_result_sets_with_varying_column_counts() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1");
        return;
    };
    let mut reader = conn
        .execute_reader("SELECT 1; SELECT 1, 2; SELECT 1, 2, 3", &[])
        .unwrap();

    assert_eq!(reader.field_count(), 1);
    assert!(reader.read().unwrap());
    assert!(!reader.read().unwrap());

    assert!(reader.next_result().unwrap());
    assert_eq!(reader.field_count(), 2);
    assert!(reader.read().unwrap());

    assert!(reader.next_result().unwrap());
    assert_eq!(reader.field_count(), 3);
    assert!(reader.read().unwrap());

    assert!(!reader.next_result().unwrap());

    // Table data across two result sets.
    let mut reader = conn
        .execute_reader(
            &format!("SELECT * FROM {TABLE} LIMIT 5; SELECT * FROM {DIMACCOUNT} LIMIT 5"),
            &[],
        )
        .unwrap();
    let mut c1 = 0;
    while reader.read().unwrap() {
        c1 += 1;
    }
    assert_eq!(c1, 5);
    assert!(reader.next_result().unwrap());
    let mut c2 = 0;
    while reader.read().unwrap() {
        c2 += 1;
    }
    assert_eq!(c2, 5);
}

#[test]
fn empty_result_set_followed_by_data() {
    let Some(mut conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1");
        return;
    };
    let mut reader = conn
        .execute_reader(&format!("SELECT * FROM {TABLE} WHERE 1=0; SELECT 1"), &[])
        .unwrap();
    assert!(!reader.read().unwrap());
    assert!(reader.next_result().unwrap());
    assert!(reader.read().unwrap());
    assert_eq!(reader.get_i32(0).unwrap(), 1);
}

#[test]
fn command_timeout_property_defaults_and_set() {
    let Some(conn) = live_conn() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1");
        return;
    };
    let mut cmd = conn.create_command("SELECT 1", vec![]);
    assert_eq!(cmd.command_timeout, 30);
    cmd.command_timeout = 60;
    assert_eq!(cmd.command_timeout, 60);
}

#[test]
fn out_of_band_cancel_interrupts_query_and_preserves_session() {
    let Some(mut c) = cfg() else {
        eprintln!("skipping: set NZ_RUN_LIVE_TESTS=1");
        return;
    };
    c.command_timeout = 0; // manual cancellation only
    let mut conn = NzConnection::connect(&c).unwrap();

    // Temp table proves the same backend session survives the cancel.
    conn.batch_execute("CREATE TEMP TABLE RUST_CANCEL_TEST AS (SELECT 1 AS COL1)")
        .unwrap();

    let pid = conn.backend_process_id();
    let key = conn.backend_secret_key();
    let cancel_cfg = c.clone();
    let handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(1500));
        let _ = nz_rust::cancel::send_cancel(&cancel_cfg, pid, key);
    });

    // The buffered driver receives the server's cancellation error.
    let err = conn.query(HEAVY_SQL, &[]);
    let _ = handle.join();
    assert!(err.is_err(), "cancelled query should fail; got {err:?}");

    // Session preserved: the temp table is still visible.
    let mut ok = false;
    for _ in 0..20 {
        if let Ok(r) = conn.query("SELECT COL1 FROM RUST_CANCEL_TEST", &[]) {
            assert_eq!(r.rows()[0].try_get::<_, i32>(0).unwrap(), 1);
            ok = true;
            break;
        }
        thread::sleep(Duration::from_millis(500));
    }
    assert!(ok, "session must survive a manual cancel");
}

// ---------------------------------------------------------------------------
// Column metadata (C# `GetDataTypeName` / Node `_getTypeNameFromOid` parity)
// ---------------------------------------------------------------------------

/// The declared name of every common type must match the reference tables.
///
/// This also pins the wire encoding that the resolver depends on: character
/// types report `type_len = -1` and carry their size in `type_mod` as
/// `length + 16`, which is why the length can only come from the modifier.
#[test]
fn column_type_metadata_matches_the_reference_table() {
    let mut conn = match live_conn() {
        Some(c) => c,
        None => return,
    };
    let sql = "SELECT 'a'::VARCHAR(32) AS V, 'b'::NVARCHAR(20) AS NV, 'c'::CHAR(5) AS C, \
               'd'::NCHAR(6) AS NC, 1.5::NUMERIC(10,4) AS N, 1::BYTEINT AS BI, \
               1::INT4 AS I4, 1::SMALLINT AS I2, 1::BIGINT AS I8, \
               1::REAL AS R4, 1::DOUBLE PRECISION AS R8, TRUE AS B, 'x'::TEXT AS T, \
               DATE '2024-01-01' AS D, TIME '01:02:03' AS TM, \
               TIMESTAMP '2024-01-01 01:02:03' AS TS, NULL::INT4 AS NULLY";
    let result = conn.query(sql, &[]).expect("query");
    let set = &result.result_sets[0];

    for (name, want) in [
        ("V", "VARCHAR(32)"),
        ("NV", "NVARCHAR(20)"),
        ("C", "CHAR(5)"),
        ("NC", "NCHAR(6)"),
        ("N", "NUMERIC(10,4)"),
        ("BI", "BYTEINT"),
        ("I4", "INTEGER"),
        ("I2", "SMALLINT"),
        ("I8", "BIGINT"),
        ("R4", "REAL"),
        ("R8", "DOUBLE"),
        ("B", "BOOL"),
        ("T", "TEXT"),
        ("D", "DATE"),
        ("TM", "TIME"),
        // Netezza's `TIMESTAMP` literal is an alias for `TIMESTAMPTZ`, so the
        // server reports OID 1184 for it, not 1114.
        ("TS", "TIMESTAMPTZ"),
        ("NULLY", "INTEGER"),
    ] {
        let col = set
            .columns
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("column {name} missing from the result"));
        assert_eq!(col.declared_type_name(), want, "declared name for {name}");
        // The declared name is the base name plus any length suffix.
        assert!(
            col.type_name() == want || want.starts_with(&col.type_name()),
            "base type name for {name} is {:?}",
            col.type_name()
        );
    }

    // No column may fall through to the `OID(n)` fallback.
    let unknown: Vec<&str> = set
        .columns
        .iter()
        .filter(|c| c.type_name().starts_with("OID("))
        .map(|c| c.name.as_str())
        .collect();
    assert!(unknown.is_empty(), "unmapped OIDs for columns {unknown:?}");
}

/// `reader::ColumnMetadata` (the Node-style resolver) must agree on the
/// declared length for the same types.
#[test]
fn reader_metadata_agrees_with_the_descriptor() {
    let mut conn = match live_conn() {
        Some(c) => c,
        None => return,
    };
    let sql = "SELECT 'a'::VARCHAR(32) AS V, 'b'::NVARCHAR(20) AS NV, \
               1.5::NUMERIC(10,4) AS N, 1::BYTEINT AS BI";
    let result = conn.query(sql, &[]).expect("query");
    let set = &result.result_sets[0];

    // Node spellings: INT4/NVARCHAR etc., plus the declared length.
    for (name, declared) in [
        ("V", "VARCHAR(32)"),
        ("NV", "NVARCHAR(20)"),
        ("N", "NUMERIC(10,4)"),
    ] {
        let col = set.columns.iter().find(|c| c.name == name).expect("column");
        assert_eq!(col.declared_type_name(), declared);
    }
    let bi = set.columns.iter().find(|c| c.name == "BI").expect("column");
    assert_eq!(bi.type_name(), "BYTEINT");
    assert_eq!(
        bi.declared_type_name(),
        "BYTEINT",
        "no bogus length on BYTEINT"
    );
}
