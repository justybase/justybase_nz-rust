//! Opt-in performance scenarios matching nzpy_extended's `performance_test.py`.
//!
//! Run with `NZ_RUN_PERF_TESTS=1` and the existing `NZ_DEV_*` connection
//! variables. This is intentionally a test target so the benchmark does not
//! run during the normal appliance-independent test suite.

use nz_rust::{NzConnection, NzConnectionConfig};
use std::env;
use std::time::Instant;

fn env_or(name: &str, fallback: &str) -> String {
    env::var(name).unwrap_or_else(|_| fallback.to_string())
}

#[test]
fn nzpy_extended_six_type_performance_scenarios() {
    if env::var("NZ_RUN_PERF_TESTS").ok().as_deref() != Some("1") {
        eprintln!("skipping: set NZ_RUN_PERF_TESTS=1 to run the live performance benchmark");
        return;
    }

    let host = env::var("NZ_DEV_HOST").expect("NZ_DEV_HOST is required");
    let port = env_or("NZ_DEV_PORT", "5480")
        .parse()
        .expect("NZ_DEV_PORT must be an integer");
    let database = env::var("NZ_DEV_DB")
        .or_else(|_| env::var("NZ_DEV_DATABASE"))
        .unwrap_or_else(|_| "JUST_DATA".into());
    let user = env_or("NZ_DEV_USER", "admin");
    let password = env::var("NZ_DEV_PASSWORD").expect("NZ_DEV_PASSWORD is required");
    let row_limit = env_or("NZ_ROWS", "100000");
    let repeats: usize = env_or("NZ_PERF_REPEATS", "1")
        .parse()
        .expect("NZ_PERF_REPEATS must be an integer");
    assert!(repeats > 0, "NZ_PERF_REPEATS must be positive");
    let source = env_or("NZ_BENCH_SOURCE_TABLE", "JUST_DATA..FACTPRODUCTINVENTORY");

    let queries = [
        (
            "integer_types",
            format!(
                "SELECT (RANDOM()*10000)::INT AS col_int, (RANDOM()*10000)::BIGINT AS col_bigint, (RANDOM()*100)::SMALLINT AS col_smallint, (RANDOM()*10)::BYTEINT AS col_byteint FROM {source} LIMIT {row_limit}"
            ),
        ),
        (
            "numeric_types",
            format!(
                "SELECT (RANDOM()*10000)::NUMERIC(20,4) AS col_numeric, (RANDOM()*10000)::DECIMAL(18,2) AS col_decimal, (RANDOM()*10000)::REAL AS col_real, (RANDOM()*10000)::DOUBLE PRECISION AS col_double FROM {source} LIMIT {row_limit}"
            ),
        ),
        (
            "string_types",
            format!(
                "SELECT (RANDOM()*10000)::VARCHAR(50) AS col_varchar, (RANDOM()*10000)::NVARCHAR(50) AS col_nvarchar, (RANDOM()*10000)::CHAR(20) AS col_char FROM {source} LIMIT {row_limit}"
            ),
        ),
        (
            "datetime_types",
            format!(
                "SELECT CURRENT_DATE + (RANDOM()*365)::INT AS col_date, CURRENT_TIME AS col_time, CURRENT_TIMESTAMP AS col_timestamp FROM {source} LIMIT {row_limit}"
            ),
        ),
        (
            "boolean_types",
            format!(
                "SELECT CASE WHEN RANDOM() > 0.5 THEN TRUE ELSE FALSE END AS col_bool, CASE WHEN RANDOM() > 0.5 THEN TRUE ELSE FALSE END AS col_boolean FROM {source} LIMIT {row_limit}"
            ),
        ),
        (
            "all_types",
            format!(
                "SELECT (RANDOM()*10000)::INT AS col_int, (RANDOM()*10000)::BIGINT AS col_bigint, (RANDOM()*100)::SMALLINT AS col_smallint, (RANDOM()*10)::BYTEINT AS col_byteint, (RANDOM()*10000)::NUMERIC(20,4) AS col_numeric, (RANDOM()*10000)::DECIMAL(18,2) AS col_decimal, (RANDOM()*10000)::REAL AS col_real, (RANDOM()*10000)::DOUBLE PRECISION AS col_double, (RANDOM()*10000)::VARCHAR(50) AS col_varchar, (RANDOM()*10000)::NVARCHAR(50) AS col_nvarchar, (RANDOM()*10000)::CHAR(20) AS col_char, CURRENT_DATE + (RANDOM()*365)::INT AS col_date, CURRENT_TIME AS col_time, CURRENT_TIMESTAMP AS col_timestamp, CASE WHEN RANDOM() > 0.5 THEN TRUE ELSE FALSE END AS col_bool FROM {source} LIMIT {row_limit}"
            ),
        ),
    ];

    println!("nzpy_extended-compatible Rust benchmark");
    println!("database={database} source_table={source} rows={row_limit} repeats={repeats}");
    let expected_rows = row_limit.parse::<usize>().unwrap();
    for (name, sql) in queries {
        let mut samples = Vec::with_capacity(repeats);
        for repeat in 1..=repeats {
            let config = NzConnectionConfig {
                host: host.clone(),
                port,
                database: database.clone(),
                user: user.clone(),
                password: password.clone(),
                ..Default::default()
            };
            let connect_start = Instant::now();
            let mut connection = NzConnection::connect(&config).expect("connect");
            let connect_ms = connect_start.elapsed().as_secs_f64() * 1000.0;
            let query_start = Instant::now();
            let result = connection.query(&sql, &[]).expect("query");
            // `nzpy_extended` returns already materialized Python values from
            // fetchall(). Force the same work on Rust so the primary rows/s
            // metric does not compare eager Python decoding with lazy Rust
            // row storage. Lazy-row latency remains visible in the query
            // API itself; this loop is the apples-to-apples metric.
            let decoded_cells: usize = result.rows().iter().map(|row| row.values().len()).sum();
            let query_ms = query_start.elapsed().as_secs_f64() * 1000.0;
            let rows = result.rows().len();
            let columns = result
                .rows()
                .first()
                .map(|row| row.columns().len())
                .unwrap_or(0);
            let rows_per_second = rows as f64 / (query_ms / 1000.0);
            samples.push(rows_per_second);
            println!(
                "{name:<16} run={repeat:>2} connect={connect_ms:>9.3} ms query={query_ms:>12.3} ms rows={rows:>7} columns={columns:>2} cells={decoded_cells:>7} rows/s={rows_per_second:>12.2}"
            );
            assert_eq!(rows, expected_rows, "{name} row count");
        }
        if repeats > 1 {
            samples.sort_by(f64::total_cmp);
            println!(
                "{name:<16} median rows/s={:>12.2}",
                samples[samples.len() / 2]
            );
        }
    }
}
