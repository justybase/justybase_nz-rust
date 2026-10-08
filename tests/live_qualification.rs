//! LIVE qualification suite: self-contained boundary and recovery tests
//! against a real Netezza appliance.
//!
//! Every object is a session TEMP table (or `RUST_<pid>_<micros>_<n>` named),
//! so the suite runs on an empty test database and leaves nothing behind.
//! Tests are `#[ignore]`d; run them with `scripts/test-live.sh` or
//! `cargo test --features compat --test live_qualification -- --ignored --test-threads=1`.
//!
//! Text vs binary paths: a `SELECT` of literals is answered by the host with
//! text rows (`T`/`D`); a `SELECT` from a table is answered by the SPUs with
//! DBOS rows (`X`/`Y`). Each matrix runs both, and asserts which one it got
//! through `ResultSet::nullability` (only present on the DBOS path). Where
//! the expected rendering is server-defined (temporal, NUMERIC scale), the
//! binary decode is compared against the server's own text rendering.

mod live_support;

use futures_core::Stream;
use live_support::{live_config, unique_name};
use nz_rust::{Client, Decimal, NzConnection, NzError, NzNumeric, NzValue, QueryOptions};
use std::pin::Pin;
use std::time::Duration;

async fn connect() -> Client {
    Client::connect(&live_config())
        .await
        .expect("connect to live appliance")
}

fn legacy() -> NzConnection {
    NzConnection::connect(&live_config()).expect("legacy connect")
}

async fn next<S: Stream + Unpin>(stream: &mut S) -> Option<S::Item> {
    tokio::time::timeout(
        Duration::from_secs(120),
        std::future::poll_fn(|cx| Pin::new(&mut *stream).poll_next(cx)),
    )
    .await
    .expect("stream stalled")
}

fn values(rows: &[nz_rust::Row]) -> Vec<Vec<NzValue>> {
    rows.iter()
        .map(|row| row.try_values().expect("decode row").to_vec())
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Path {
    Text,
    Dbos,
}

/// Run `sql` through every native read path (eager, lazy stream, batches)
/// and the legacy engine (on `legacy`, which must hold the same TEMP
/// objects), assert they agree, and assert the wire path.
async fn read_all_paths(
    client: &Client,
    legacy: &mut NzConnection,
    sql: &str,
    path: Path,
) -> Vec<Vec<NzValue>> {
    let multi = client
        .query_multi(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(multi.result_sets.len(), 1, "{sql}");
    assert_eq!(
        multi.result_sets[0].nullability.is_some(),
        path == Path::Dbos,
        "{sql}: unexpected wire path"
    );
    let eager = values(&multi.result_sets[0].rows);

    let mut stream = client.query_stream(sql, &[]).await.unwrap();
    let mut lazy = Vec::new();
    while let Some(row) = next(&mut stream).await {
        let row = row.unwrap();
        // Decode single cells last-to-first through the lazy accessor too.
        let mut cells = vec![NzValue::Null; row.len()];
        for index in (0..row.len()).rev() {
            cells[index] = row.try_get_value(index).unwrap().clone();
        }
        lazy.push(cells);
    }
    assert_eq!(lazy, eager, "{sql}: query_stream differs from query");

    let mut batches = client.query_batches(sql, &[]).await.unwrap();
    let mut batched = Vec::new();
    while let Some(batch) = next(&mut batches).await {
        batched.extend(values(&batch.unwrap()));
    }
    assert_eq!(batched, eager, "{sql}: query_batches differs from query");

    let legacy_result = legacy.query(sql, &[]).unwrap();
    assert_eq!(
        values(&legacy_result.result_sets[0].rows),
        eager,
        "{sql}: legacy engine differs from native"
    );
    eager
}

/// Execute the same setup statements on the native client and the legacy
/// connection (TEMP tables are per session).
async fn setup(client: &Client, legacy: &mut NzConnection, statements: &[String]) {
    for statement in statements {
        client
            .batch_execute(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
        legacy
            .batch_execute(statement)
            .unwrap_or_else(|e| panic!("legacy {statement}: {e}"));
    }
}

/// Interval text to `(months, microseconds)`. Netezza stores an interval as
/// microseconds plus months, so "1 day", "24:00:00" and "1 days 00:00:00" are
/// the same value; the server and the binary decoder just spell it differently.
fn interval_parts(text: &str) -> (i64, i64) {
    let mut months = 0i64;
    let mut micros = 0i64;
    let tokens: Vec<&str> = text.split_whitespace().collect();
    let mut i = 0;
    while i < tokens.len() {
        let token = tokens[i];
        if token.contains(':') {
            let negative = token.starts_with('-');
            let body = token.trim_start_matches(['-', '+']);
            let (h, m, sec, frac) = nz_rust::types::datetime::parse_time_text(body);
            let value = ((h * 60 + m) * 60 + sec) * 1_000_000 + frac;
            micros += if negative { -value } else { value };
            i += 1;
            continue;
        }
        let amount: i64 = token
            .parse()
            .unwrap_or_else(|_| panic!("interval {text:?}: bad token {token:?}"));
        let unit = tokens.get(i + 1).copied().unwrap_or_default();
        match unit.trim_end_matches('s') {
            "year" => months += amount * 12,
            "mon" | "month" => months += amount,
            "day" => micros += amount * 86_400_000_000,
            "hour" => micros += amount * 3_600_000_000,
            "min" | "minute" => micros += amount * 60_000_000,
            "sec" | "second" => micros += amount * 1_000_000,
            other => panic!("interval {text:?}: unknown unit {other:?}"),
        }
        i += 2;
    }
    (months, micros)
}

async fn session_id(client: &Client) -> String {
    let rows = client.query("SELECT CURRENT_SID", &[]).await.unwrap();
    rows[0].try_values().unwrap()[0].to_display_string()
}

fn legacy_session_id(conn: &mut NzConnection) -> String {
    let result = conn.query("SELECT CURRENT_SID", &[]).unwrap();
    result.result_sets[0].rows[0].try_values().unwrap()[0].to_display_string()
}

// ---------------------------------------------------------------------------
// P11: integer boundaries
// ---------------------------------------------------------------------------

const INTEGER_TYPES: [(&str, i64, i64); 4] = [
    ("BYTEINT", i8::MIN as i64, i8::MAX as i64),
    ("SMALLINT", i16::MIN as i64, i16::MAX as i64),
    ("INTEGER", i32::MIN as i64, i32::MAX as i64),
    ("BIGINT", i64::MIN, i64::MAX),
];

fn integer_points(min: i64, max: i64) -> [i64; 7] {
    [min, min + 1, -1, 0, 1, max - 1, max]
}

fn check_integer_row(row: &nz_rust::Row, values: &[NzValue], expected: &[i64], label: &str) {
    for (index, (value, want)) in values.iter().zip(expected).enumerate() {
        assert_eq!(
            value.to_display_string(),
            want.to_string(),
            "{label} col {index}"
        );
        assert_eq!(
            row.try_get::<_, i64>(index).unwrap(),
            *want,
            "{label} i64 col {index}"
        );
        if let Ok(narrow) = i16::try_from(*want) {
            assert_eq!(row.try_get::<_, i16>(index).unwrap(), narrow, "{label} i16");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Netezza appliance"]
async fn live_integer_boundaries_text_and_binary_paths() {
    let client = connect().await;
    let mut conn = legacy();

    // Text path: one literal row per type.
    for (ty, min, max) in INTEGER_TYPES {
        let points = integer_points(min, max);
        let columns: Vec<String> = points
            .iter()
            .enumerate()
            .map(|(i, v)| format!("CAST('{v}' AS {ty}) AS C{i}"))
            .collect();
        let sql = format!("SELECT {}", columns.join(", "));
        let rows = read_all_paths(&client, &mut conn, &sql, Path::Text).await;
        let typed = client.query(&sql, &[]).await.unwrap();
        check_integer_row(&typed[0], &rows[0], &points, &format!("text {ty}"));
    }

    // Binary path: a TEMP table holding every boundary of every type.
    let table = unique_name("RUST_INT");
    let mut statements = vec![format!(
        "CREATE TEMP TABLE {table} (ID INTEGER, B BYTEINT, S SMALLINT, I INTEGER, G BIGINT) \
         DISTRIBUTE ON RANDOM"
    )];
    for id in 0..7 {
        let cells: Vec<String> = INTEGER_TYPES
            .iter()
            .map(|(ty, min, max)| format!("CAST('{}' AS {ty})", integer_points(*min, *max)[id]))
            .collect();
        statements.push(format!(
            "INSERT INTO {table} VALUES ({id}, {})",
            cells.join(", ")
        ));
    }
    setup(&client, &mut conn, &statements).await;
    let sql = format!("SELECT B, S, I, G FROM {table} ORDER BY ID");
    let rows = read_all_paths(&client, &mut conn, &sql, Path::Dbos).await;
    let typed = client.query(&sql, &[]).await.unwrap();
    for id in 0..7 {
        let expected: Vec<i64> = INTEGER_TYPES
            .iter()
            .map(|(_, min, max)| integer_points(*min, *max)[id])
            .collect();
        check_integer_row(&typed[id], &rows[id], &expected, &format!("dbos row {id}"));
    }

    // Bound parameters render boundary literals that round-trip exactly.
    for value in [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX] {
        let rows = client
            .query("SELECT CAST($1 AS BIGINT) AS V", &[&value])
            .await
            .unwrap();
        assert_eq!(rows[0].try_get::<_, i64>(0).unwrap(), value);
    }
    client.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// P12: VARCHAR / NVARCHAR length boundaries
// ---------------------------------------------------------------------------

const VARCHAR_LENGTHS: [usize; 16] = [
    0, 1, 255, 256, 1023, 1024, 4095, 4096, 8191, 8192, 32765, 32766, 32767, 32768, 40000, 60000,
];

/// `A` + filler + `Z` (or shorter forms), so head and tail are checkable.
fn varchar_expr(length: usize) -> String {
    match length {
        0 => "''".into(),
        1 => "'A'".into(),
        n => format!("'A' || REPEAT('x', {}) || 'Z'", n - 2),
    }
}

fn check_varchar(value: &NzValue, length: usize, label: &str) {
    let NzValue::Text(text) = value else {
        panic!(
            "{label}: expected text, got {}",
            value.to_display_string().len()
        );
    };
    assert_eq!(text.len(), length, "{label}: length");
    if length >= 1 {
        assert!(text.starts_with('A'), "{label}: head");
    }
    if length >= 2 {
        assert!(text.ends_with('Z'), "{label}: tail");
        assert!(
            text[1..length - 1].bytes().all(|b| b == b'x'),
            "{label}: body"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Netezza appliance"]
async fn live_varchar_length_boundaries_text_and_binary_paths() {
    let client = connect().await;
    let mut conn = legacy();
    let table = unique_name("RUST_VC");
    setup(
        &client,
        &mut conn,
        &[format!(
            "CREATE TEMP TABLE {table} (ID INTEGER, V VARCHAR(64000)) DISTRIBUTE ON RANDOM"
        )],
    )
    .await;
    for length in VARCHAR_LENGTHS {
        let text_sql = format!(
            "SELECT CAST({} AS VARCHAR(64000)) AS V",
            varchar_expr(length)
        );
        let rows = read_all_paths(&client, &mut conn, &text_sql, Path::Text).await;
        check_varchar(&rows[0][0], length, &format!("text {length}"));
        setup(
            &client,
            &mut conn,
            &[format!(
                "INSERT INTO {table} VALUES ({length}, {})",
                varchar_expr(length)
            )],
        )
        .await;
    }
    let sql = format!("SELECT ID, V FROM {table} ORDER BY ID");
    let rows = read_all_paths(&client, &mut conn, &sql, Path::Dbos).await;
    assert_eq!(rows.len(), VARCHAR_LENGTHS.len());
    for (row, length) in rows.iter().zip(VARCHAR_LENGTHS) {
        assert_eq!(row[0].to_display_string(), length.to_string());
        // Netezza stores '' in VARCHAR as an empty string, not NULL.
        check_varchar(&row[1], length, &format!("dbos {length}"));
    }
    // Typed getters on the critical boundaries.
    let typed = client.query(&sql, &[]).await.unwrap();
    for row in &typed {
        let length: i64 = row.try_get(0).unwrap();
        let text: String = row.try_get(1).unwrap();
        assert_eq!(text.len() as i64, length);
        let borrowed: &str = row.try_get_raw_typed(1).unwrap();
        assert_eq!(borrowed.len() as i64, length);
    }
    client.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Netezza appliance"]
async fn live_unicode_nvarchar_bytes_differ_from_chars() {
    let client = connect().await;
    let mut conn = legacy();
    let samples = [
        "Zażółć gęślą jaźń",
        "ZAŻÓŁĆ GĘŚLĄ JAŹŃ",
        "日本語テキスト",
        "Ελληνικά κείμενα",
        "emoji 😀🚀 end",
    ];
    let table = unique_name("RUST_NV");
    setup(
        &client,
        &mut conn,
        &[format!(
            "CREATE TEMP TABLE {table} (ID INTEGER, V NVARCHAR(16000)) DISTRIBUTE ON RANDOM"
        )],
    )
    .await;
    let mut expected = Vec::new();
    for (id, sample) in samples.iter().enumerate() {
        assert_ne!(sample.len(), sample.chars().count());
        let sql = format!("SELECT CAST('{sample}' AS NVARCHAR(200)) AS V");
        let rows = read_all_paths(&client, &mut conn, &sql, Path::Text).await;
        assert_eq!(rows[0][0], NzValue::Text((*sample).into()), "text {sample}");
        setup(
            &client,
            &mut conn,
            &[format!("INSERT INTO {table} VALUES ({id}, '{sample}')")],
        )
        .await;
        expected.push(vec![
            NzValue::Int4(id as i32),
            NzValue::Text((*sample).into()),
        ]);
    }
    // A long multi-byte value: 10 000 chars, 20 000 bytes.
    setup(
        &client,
        &mut conn,
        &[format!(
            "INSERT INTO {table} VALUES (99, REPEAT('ż', 10000))"
        )],
    )
    .await;
    let sql = format!("SELECT ID, V FROM {table} ORDER BY ID");
    let rows = read_all_paths(&client, &mut conn, &sql, Path::Dbos).await;
    assert_eq!(&rows[..samples.len()], expected.as_slice());
    let NzValue::Text(long) = &rows[samples.len()][1] else {
        panic!("long NVARCHAR is not text");
    };
    assert_eq!(long.chars().count(), 10_000);
    assert_eq!(long.len(), 20_000);
    assert!(long.chars().all(|c| c == 'ż'));
    client.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// P13: NUMERIC exactness
// ---------------------------------------------------------------------------

const NUMERIC_TYPES: [(u32, u32); 12] = [
    (1, 0),
    (9, 0),
    (18, 0),
    (19, 0),
    (28, 0),
    (38, 0),
    (10, 4),
    (20, 10),
    (28, 14),
    (38, 10),
    (38, 20),
    (38, 38),
];

/// Values that fit NUMERIC(p, s) exactly (no rounding), as decimal strings.
fn numeric_values(precision: u32, scale: u32) -> Vec<String> {
    let integer_digits = precision - scale;
    let mut out = Vec::new();
    for candidate in [
        "0", "1", "-1", "0.0001", "-0.0001", "3.14", "-3.14", "0.5", "-7",
    ] {
        let (int_part, frac_part) = candidate
            .trim_start_matches('-')
            .split_once('.')
            .unwrap_or((candidate.trim_start_matches('-'), ""));
        let int_len = if int_part == "0" {
            0
        } else {
            int_part.len() as u32
        };
        if int_len <= integer_digits && frac_part.len() as u32 <= scale {
            out.push(candidate.to_string());
        }
    }
    // Largest and smallest representable values, all nines.
    let nines_int = "9".repeat(integer_digits as usize);
    let nines_frac = "9".repeat(scale as usize);
    let max = match (integer_digits, scale) {
        (0, _) => format!("0.{nines_frac}"),
        (_, 0) => nines_int,
        _ => format!("{nines_int}.{nines_frac}"),
    };
    out.push(max.clone());
    out.push(format!("-{max}"));
    // Smallest positive step.
    if scale > 0 {
        out.push(format!("0.{}1", "0".repeat(scale as usize - 1)));
    }
    // Values beyond f64 precision (17+ significant digits) when they fit.
    if integer_digits >= 19 {
        out.push(format!("1{}1", "0".repeat(17)));
    }
    if integer_digits >= 18 {
        out.push("-123456789012345678".into());
    }
    out
}

/// The canonical text of `value` at `scale` (trailing zeros padded).
fn at_scale(value: &str, scale: u32) -> String {
    let negative = value.starts_with('-');
    let digits = value.trim_start_matches('-');
    let (int_part, frac_part) = digits.split_once('.').unwrap_or((digits, ""));
    let int_part = if int_part.is_empty() { "0" } else { int_part };
    let mut frac = frac_part.to_string();
    while (frac.len() as u32) < scale {
        frac.push('0');
    }
    let is_zero = int_part.bytes().all(|b| b == b'0') && frac.bytes().all(|b| b == b'0');
    let sign = if negative && !is_zero { "-" } else { "" };
    if scale == 0 {
        format!("{sign}{int_part}")
    } else {
        format!("{sign}{int_part}.{frac}")
    }
}

fn check_numeric(row: &nz_rust::Row, index: usize, expected: &str, label: &str) {
    let value = row.try_get_value(index).unwrap();
    // The text path documents NUMERIC of up to 15 significant digits that
    // round-trip as Float8 (reference-driver parity); everything else, and the
    // whole binary path, must be exact including scale.
    if let NzValue::Float8(float) = value {
        assert_eq!(*float, expected.parse::<f64>().unwrap(), "{label}: Float8");
        let exact: NzNumeric = row.try_get(index).unwrap();
        if label.starts_with("dbos") {
            // The binary path decodes NzNumeric from the raw field, so the
            // declared scale and trailing zeros survive the Float8 value.
            assert_eq!(exact.to_string(), expected, "{label}: NzNumeric exact");
        } else {
            assert_eq!(
                exact.to_string().parse::<f64>().unwrap(),
                *float,
                "{label}: NzNumeric from Float8"
            );
        }
        return;
    }
    assert_eq!(value.to_display_string(), expected, "{label}: NzValue");
    let exact: NzNumeric = row.try_get(index).unwrap();
    assert_eq!(exact.to_string(), expected, "{label}: NzNumeric");
    let parsed: NzNumeric = expected.parse().unwrap();
    assert_eq!(
        exact.coefficient(),
        parsed.coefficient(),
        "{label}: coefficient"
    );
    assert_eq!(exact.scale(), parsed.scale(), "{label}: scale");
    let text: String = row
        .try_get(index)
        .unwrap_or_else(|e| panic!("{label}: String getter: {e}"));
    assert_eq!(text, expected, "{label}: String");
    // Decimal only where 96 bits and scale <= 28 hold the value exactly.
    if Decimal::from_str_exact(expected).is_ok() {
        let got: Decimal = row.try_get(index).unwrap();
        assert_eq!(got.to_string(), expected, "{label}: Decimal keeps scale");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Netezza appliance"]
async fn live_numeric_matrix_is_exact_on_text_and_binary_paths() {
    let client = connect().await;
    let mut conn = legacy();
    for (precision, scale) in NUMERIC_TYPES {
        let ty = format!("NUMERIC({precision},{scale})");
        let inputs = numeric_values(precision, scale);
        let expected: Vec<String> = inputs.iter().map(|v| at_scale(v, scale)).collect();

        let columns: Vec<String> = inputs
            .iter()
            .enumerate()
            .map(|(i, v)| format!("CAST('{v}' AS {ty}) AS C{i}"))
            .collect();
        let text_sql = format!("SELECT {}", columns.join(", "));
        read_all_paths(&client, &mut conn, &text_sql, Path::Text).await;
        let rows = client.query(&text_sql, &[]).await.unwrap();
        for (index, want) in expected.iter().enumerate() {
            check_numeric(&rows[0], index, want, &format!("text {ty} {want}"));
        }

        let table = unique_name("RUST_NUM");
        let mut statements = vec![format!(
            "CREATE TEMP TABLE {table} (ID INTEGER, V {ty}) DISTRIBUTE ON RANDOM"
        )];
        for (id, input) in inputs.iter().enumerate() {
            statements.push(format!(
                "INSERT INTO {table} VALUES ({id}, CAST('{input}' AS {ty}))"
            ));
        }
        setup(&client, &mut conn, &statements).await;
        let sql = format!("SELECT V FROM {table} ORDER BY ID");
        read_all_paths(&client, &mut conn, &sql, Path::Dbos).await;
        let rows = client.query(&sql, &[]).await.unwrap();
        for (id, want) in expected.iter().enumerate() {
            check_numeric(&rows[id], 0, want, &format!("dbos {ty} {want}"));
        }
        // Binary decode equals the server's own text rendering.
        let server_text = client
            .query(
                &format!("SELECT CAST(V AS VARCHAR(100)) FROM {table} ORDER BY ID"),
                &[],
            )
            .await
            .unwrap();
        for (id, want) in expected.iter().enumerate() {
            assert_eq!(
                server_text[id].try_values().unwrap()[0].to_display_string(),
                *want,
                "server rendering {ty}"
            );
        }
    }
    client.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// P14: NULL matrix
// ---------------------------------------------------------------------------

/// `(type, non-NULL literal)` for every supported type.
const NULL_MATRIX_TYPES: [(&str, &str); 18] = [
    ("BOOLEAN", "TRUE"),
    ("BYTEINT", "1"),
    ("SMALLINT", "2"),
    ("INTEGER", "3"),
    ("BIGINT", "4"),
    ("REAL", "1.5"),
    ("DOUBLE PRECISION", "2.5"),
    ("NUMERIC(10,2)", "3.25"),
    ("CHAR(5)", "'ab'"),
    ("VARCHAR(10)", "'cd'"),
    ("NCHAR(5)", "'ef'"),
    ("NVARCHAR(10)", "'gh'"),
    ("DATE", "'2024-02-29'"),
    ("TIME", "'12:34:56'"),
    ("TIMESTAMP", "'2024-02-29 12:34:56.123456'"),
    ("TIMETZ", "'12:34:56+02'"),
    ("INTERVAL", "'1 day'"),
    ("VARBINARY(10)", "HEX_TO_BINARY('0102')"),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Netezza appliance"]
async fn live_null_matrix_for_every_type_and_alternating_wide_rows() {
    let client = connect().await;
    let mut conn = legacy();
    // 40 columns cycling through the types; row 0 has NULL in even
    // columns, row 1 in odd columns.
    const COLUMNS: usize = 40;
    let column_type = |i: usize| NULL_MATRIX_TYPES[i % NULL_MATRIX_TYPES.len()];
    let table = unique_name("RUST_NULLS");
    let definitions: Vec<String> = (0..COLUMNS)
        .map(|i| format!("C{i} {}", column_type(i).0))
        .collect();
    let mut statements = vec![format!(
        "CREATE TEMP TABLE {table} (ID INTEGER, {}) DISTRIBUTE ON RANDOM",
        definitions.join(", ")
    )];
    for id in 0..2 {
        let cells: Vec<String> = (0..COLUMNS)
            .map(|i| {
                let (ty, literal) = column_type(i);
                if i % 2 == id {
                    format!("CAST(NULL AS {ty})")
                } else {
                    format!("CAST({literal} AS {ty})")
                }
            })
            .collect();
        statements.push(format!(
            "INSERT INTO {table} VALUES ({id}, {})",
            cells.join(", ")
        ));
    }
    setup(&client, &mut conn, &statements).await;
    let projection: Vec<String> = (0..COLUMNS).map(|i| format!("C{i}")).collect();
    let dbos_sql = format!("SELECT {} FROM {table} ORDER BY ID", projection.join(", "));
    let dbos = read_all_paths(&client, &mut conn, &dbos_sql, Path::Dbos).await;

    // The same values as literals on the text path.
    let text_rows: Vec<String> = (0..2)
        .map(|id| {
            let cells: Vec<String> = (0..COLUMNS)
                .map(|i| {
                    let (ty, literal) = column_type(i);
                    if i % 2 == id {
                        format!("CAST(NULL AS {ty}) AS C{i}")
                    } else {
                        format!("CAST({literal} AS {ty}) AS C{i}")
                    }
                })
                .collect();
            format!("SELECT {}", cells.join(", "))
        })
        .collect();
    for id in 0..2 {
        let text = read_all_paths(&client, &mut conn, &text_rows[id], Path::Text).await;
        for column in 0..COLUMNS {
            let (ty, _) = column_type(column);
            let null_expected = column % 2 == id;
            assert_eq!(
                dbos[id][column].is_null(),
                null_expected,
                "dbos row {id} {ty}"
            );
            assert_eq!(
                text[0][column].is_null(),
                null_expected,
                "text row {id} {ty}"
            );
            if !null_expected {
                let (binary, host) = (
                    dbos[id][column].to_display_string(),
                    text[0][column].to_display_string(),
                );
                if ty.starts_with("VARBINARY") {
                    // Known representation difference: the binary path yields
                    // Bytea (`0x0102`), the host text path yields text (`X'0102'`).
                    assert_eq!(
                        binary.trim_start_matches("0x").to_uppercase(),
                        host.trim_start_matches("X'")
                            .trim_end_matches('\'')
                            .to_uppercase(),
                        "VARBINARY bytes differ"
                    );
                } else if ty == "INTERVAL" {
                    assert_eq!(interval_parts(&binary), interval_parts(&host), "{ty}");
                } else if ty.starts_with("CHAR") || ty.starts_with("NCHAR") {
                    // Known path difference: the text path keeps CHAR padding
                    // (`ab   `), the binary path trims it (`ab`).
                    assert_eq!(binary.trim_end(), host.trim_end(), "{ty}");
                } else {
                    assert_eq!(binary, host, "binary decode differs from text for {ty}");
                }
            }
        }
    }
    client.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// P15: wide rows
// ---------------------------------------------------------------------------

fn wide_expression(i: usize) -> String {
    match i % 5 {
        0 => format!("CAST({i} AS INTEGER)"),
        1 => format!("CAST('v{i}' AS VARCHAR(20))"),
        2 => format!("CAST('{i}.25' AS NUMERIC(12,2))"),
        3 => "CAST(NULL AS INTEGER)".into(),
        _ => format!("DATE '2024-01-01' + {i}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Netezza appliance"]
async fn live_wide_rows_around_byte_and_column_boundaries() {
    let client = connect().await;
    let mut conn = legacy();
    for width in [32usize, 64, 127, 128, 129, 256, 257, 513] {
        let columns: Vec<String> = (0..width)
            .map(|i| format!("{} AS C{i}", wide_expression(i)))
            .collect();
        let text_sql = format!("SELECT {}", columns.join(", "));
        let text = read_all_paths(&client, &mut conn, &text_sql, Path::Text).await;
        let table = unique_name("RUST_WIDE");
        setup(
            &client,
            &mut conn,
            &[format!(
                "CREATE TEMP TABLE {table} AS {text_sql} DISTRIBUTE ON RANDOM"
            )],
        )
        .await;
        let dbos_sql = format!("SELECT * FROM {table}");
        let dbos = read_all_paths(&client, &mut conn, &dbos_sql, Path::Dbos).await;
        assert_eq!(text[0].len(), width);
        assert_eq!(dbos[0].len(), width);
        for i in 0..width {
            assert_eq!(text[0][i].is_null(), i % 5 == 3, "width {width} col {i}");
            assert_eq!(
                dbos[0][i].to_display_string(),
                text[0][i].to_display_string(),
                "width {width} col {i}"
            );
        }
        assert_eq!(text[0][0].to_display_string(), "0");
        let multi = client.query_multi(&dbos_sql, &[]).await.unwrap();
        let names: Vec<String> = multi.result_sets[0]
            .columns
            .iter()
            .map(|c| c.name.to_uppercase())
            .collect();
        assert_eq!(names[width - 1], format!("C{}", width - 1));
    }
    client.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// P16: temporal values
// ---------------------------------------------------------------------------

const TEMPORAL_CASES: [(&str, &str); 25] = [
    ("DATE", "0001-01-01"),
    ("DATE", "1999-12-31"),
    ("DATE", "2000-01-01"),
    ("DATE", "2000-02-29"),
    ("DATE", "2024-02-29"),
    ("DATE", "9999-12-31"),
    ("TIME", "00:00:00"),
    ("TIME", "23:59:59"),
    ("TIME", "12:34:56.123456"),
    ("TIME", "00:00:00.000001"),
    ("TIMESTAMP", "1970-01-01 00:00:00"),
    ("TIMESTAMP", "1999-12-31 23:59:59.999999"),
    ("TIMESTAMP", "2000-01-01 00:00:00"),
    ("TIMESTAMP", "2000-01-01 00:00:00.000001"),
    ("TIMESTAMP", "2038-01-19 03:14:08"),
    ("TIMESTAMP", "0001-01-01 00:00:00"),
    ("TIMESTAMP", "9999-12-31 23:59:59.999999"),
    ("INTERVAL", "0 seconds"),
    ("INTERVAL", "1 day"),
    ("INTERVAL", "-1 day"),
    ("INTERVAL", "1 year 2 months"),
    ("INTERVAL", "36 hours"),
    ("INTERVAL", "0.000001 seconds"),
    ("TIMETZ", "12:00:00+05:30"),
    ("TIMETZ", "23:59:59-08"),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Netezza appliance"]
async fn live_temporal_extremes_binary_matches_server_text() {
    let client = connect().await;
    let mut conn = legacy();
    let table = unique_name("RUST_TEMPORAL");
    let mut statements = vec![format!(
        "CREATE TEMP TABLE {table} (ID INTEGER, D DATE, T TIME, TS TIMESTAMP, I INTERVAL, TZ TIMETZ) \
         DISTRIBUTE ON RANDOM"
    )];
    for (id, (ty, literal)) in TEMPORAL_CASES.iter().enumerate() {
        let mut cells = ["NULL"; 5];
        let slot = ["DATE", "TIME", "TIMESTAMP", "INTERVAL", "TIMETZ"]
            .iter()
            .position(|t| t == ty)
            .unwrap();
        let cast = format!("CAST('{literal}' AS {ty})");
        cells[slot] = &cast;
        statements.push(format!(
            "INSERT INTO {table} VALUES ({id}, {})",
            cells.join(", ")
        ));
    }
    setup(&client, &mut conn, &statements).await;
    let sql = format!("SELECT D, T, TS, I, TZ FROM {table} ORDER BY ID");
    let binary = read_all_paths(&client, &mut conn, &sql, Path::Dbos).await;
    let text_sql = format!(
        "SELECT CAST(D AS VARCHAR(64)), CAST(T AS VARCHAR(64)), CAST(TS AS VARCHAR(64)), \
         CAST(I AS VARCHAR(64)), CAST(TZ AS VARCHAR(64)) FROM {table} ORDER BY ID"
    );
    let server_text = values(&client.query(&text_sql, &[]).await.unwrap());
    for (id, (ty, literal)) in TEMPORAL_CASES.iter().enumerate() {
        for column in 0..5 {
            let decoded = binary[id][column].to_display_string();
            let server = server_text[id][column].to_display_string();
            if column == 3 && !binary[id][column].is_null() {
                assert_eq!(
                    interval_parts(&decoded),
                    interval_parts(&server),
                    "INTERVAL '{literal}': {decoded:?} vs server {server:?}"
                );
            } else {
                assert_eq!(decoded, server, "{ty} '{literal}' column {column}");
            }
        }
        // The text path of the same literal agrees as well.
        let text = values(
            &client
                .query(&format!("SELECT CAST('{literal}' AS {ty})"), &[])
                .await
                .unwrap(),
        );
        let slot = ["DATE", "TIME", "TIMESTAMP", "INTERVAL", "TIMETZ"]
            .iter()
            .position(|t| t == ty)
            .unwrap();
        if *ty == "INTERVAL" {
            assert_eq!(
                interval_parts(&text[0][0].to_display_string()),
                interval_parts(&binary[id][slot].to_display_string()),
                "INTERVAL '{literal}' text vs binary"
            );
        } else {
            assert_eq!(
                text[0][0].to_display_string(),
                binary[id][slot].to_display_string(),
                "{ty} '{literal}' text vs binary"
            );
        }
    }
    // Hand-checked anchors.
    assert_eq!(binary[0][0].to_display_string(), "0001-01-01");
    assert_eq!(binary[4][0].to_display_string(), "2024-02-29");
    assert_eq!(binary[5][0].to_display_string(), "9999-12-31");
    assert_eq!(binary[8][1].to_display_string(), "12:34:56.123456");
    assert_eq!(
        binary[13][2].to_display_string(),
        "2000-01-01 00:00:00.000001"
    );
    client.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// P17/P18: transaction recovery and session identity
// ---------------------------------------------------------------------------

/// A TEMP table large enough that a 4-way cross join runs for minutes.
async fn slow_source(client: &Client) -> String {
    let table = unique_name("RUST_SLOW");
    client
        .batch_execute(&format!(
            "CREATE TEMP TABLE {table} AS SELECT 1 AS X DISTRIBUTE ON RANDOM"
        ))
        .await
        .unwrap();
    for _ in 0..10 {
        client
            .batch_execute(&format!("INSERT INTO {table} SELECT X FROM {table}"))
            .await
            .unwrap();
    }
    table
}

fn slow_query(table: &str) -> String {
    format!("SELECT COUNT(*) FROM {table} A, {table} B, {table} C, {table} D")
}

async fn count_rows(client: &Client, table: &str) -> i64 {
    let rows = client
        .query(&format!("SELECT COUNT(*) FROM {table}"), &[])
        .await
        .unwrap();
    rows[0].try_get(0).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Netezza appliance"]
async fn live_transaction_error_then_rollback_and_commit() {
    let client = connect().await;
    let sid = session_id(&client).await;
    let table = unique_name("RUST_TX");
    client
        .batch_execute(&format!("CREATE TEMP TABLE {table} (X INTEGER)"))
        .await
        .unwrap();

    // Error inside a transaction, then ROLLBACK.
    client.batch_execute("BEGIN").await.unwrap();
    assert!(client.in_transaction());
    client
        .batch_execute(&format!("INSERT INTO {table} VALUES (1)"))
        .await
        .unwrap();
    let error = client
        .query("SELECT * FROM RUST_NO_SUCH_TABLE_X", &[])
        .await;
    assert!(matches!(error, Err(NzError::Database(_))), "{error:?}");
    client.batch_execute("ROLLBACK").await.unwrap();
    assert!(!client.in_transaction());
    assert_eq!(count_rows(&client, &table).await, 0);

    // Error inside a transaction, then COMMIT: whatever the appliance does,
    // the driver's state must match the database afterwards.
    client.batch_execute("BEGIN").await.unwrap();
    client
        .batch_execute(&format!("INSERT INTO {table} VALUES (2)"))
        .await
        .unwrap();
    assert!(client
        .query("SELECT * FROM RUST_NO_SUCH_TABLE_X", &[])
        .await
        .is_err());
    let commit = client.batch_execute("COMMIT").await;
    eprintln!("COMMIT after error: {commit:?}");
    assert!(!client.in_transaction(), "COMMIT must end the transaction");
    let committed = count_rows(&client, &table).await;
    eprintln!("rows visible after COMMIT following an error: {committed}");
    assert!(committed == 0 || committed == 1);
    // Still the same healthy session.
    assert_eq!(session_id(&client).await, sid);
    client.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Netezza appliance"]
async fn live_drop_transaction_rolls_back_and_temp_visibility() {
    let client = connect().await;
    let table = unique_name("RUST_TXD");
    client
        .batch_execute(&format!("CREATE TEMP TABLE {table} (X INTEGER)"))
        .await
        .unwrap();
    {
        let tx = client.transaction().await.unwrap();
        tx.batch_execute(&format!("INSERT INTO {table} VALUES (1)"))
            .await
            .unwrap();
        // Visible inside the transaction.
        let rows = tx
            .query(&format!("SELECT COUNT(*) FROM {table}"), &[])
            .await
            .unwrap();
        assert_eq!(rows[0].try_get::<_, i64>(0).unwrap(), 1);
        // Dropped without commit.
    }
    // The next statement waits for the drop's rollback.
    assert_eq!(count_rows(&client, &table).await, 0);
    assert!(!client.in_transaction());

    // A TEMP table created inside a rolled-back transaction disappears.
    let inner = unique_name("RUST_TXT");
    let tx = client.transaction().await.unwrap();
    tx.batch_execute(&format!("CREATE TEMP TABLE {inner} (X INTEGER)"))
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let gone = client
        .query(&format!("SELECT COUNT(*) FROM {inner}"), &[])
        .await;
    assert!(matches!(gone, Err(NzError::Database(_))), "{gone:?}");

    // Committed work persists.
    let tx = client.transaction().await.unwrap();
    tx.batch_execute(&format!("INSERT INTO {table} VALUES (7)"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(count_rows(&client, &table).await, 1);
    client.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Netezza appliance"]
async fn live_cancel_and_timeout_recover_the_same_session_inside_transactions() {
    let client = connect().await;
    let sid = session_id(&client).await;
    let source = slow_source(&client).await;
    let table = unique_name("RUST_TXC");
    client
        .batch_execute(&format!("CREATE TEMP TABLE {table} (X INTEGER)"))
        .await
        .unwrap();

    // Cancel inside a transaction.
    client.batch_execute("BEGIN").await.unwrap();
    client
        .batch_execute(&format!("INSERT INTO {table} VALUES (1)"))
        .await
        .unwrap();
    let runner = client.clone();
    let sql = slow_query(&source);
    let slow = tokio::spawn(async move { runner.query(&sql, &[]).await });
    tokio::time::sleep(Duration::from_millis(800)).await;
    client.cancel().await.unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(60), slow)
        .await
        .expect("cancel did not stop the query")
        .unwrap();
    assert!(matches!(outcome, Err(NzError::Cancelled(_))), "{outcome:?}");
    assert!(!client.is_closed());
    // The appliance aborts a cancelled transaction and then silently ignores
    // statements (a NOTICE, no error and no rows) until it ends.
    let ignored = client.query_multi("SELECT CURRENT_SID", &[]).await.unwrap();
    assert!(ignored.result_sets.is_empty());
    assert!(
        ignored
            .notices
            .iter()
            .any(|n| n.contains("transaction is aborted")),
        "{:?}",
        ignored.notices
    );
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        session_id(&client).await,
        sid,
        "cancel replaced the session"
    );
    assert!(!client.in_transaction());
    assert_eq!(count_rows(&client, &table).await, 0);

    // Timeout inside a transaction.
    client.batch_execute("BEGIN").await.unwrap();
    client
        .batch_execute(&format!("INSERT INTO {table} VALUES (2)"))
        .await
        .unwrap();
    let timed_out = client
        .query_with_options(
            &slow_query(&source),
            &[],
            QueryOptions {
                timeout: Some(Duration::from_millis(800)),
            },
        )
        .await;
    assert!(
        matches!(timed_out, Err(NzError::Timeout(_))),
        "{timed_out:?}"
    );
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        session_id(&client).await,
        sid,
        "timeout replaced the session"
    );
    assert_eq!(count_rows(&client, &table).await, 0);

    // A SQL error keeps the session too.
    assert!(client.query("SELECT 1/0", &[]).await.is_err());
    assert_eq!(session_id(&client).await, sid);
    client.close().await.unwrap();
}

#[test]
#[ignore = "requires live Netezza appliance"]
fn live_legacy_cancel_timeout_and_error_preserve_backend_session() {
    let mut conn = legacy();
    let pid = conn.backend_process_id();
    let sid = legacy_session_id(&mut conn);
    let table = unique_name("RUST_SLOW");
    conn.batch_execute(&format!(
        "CREATE TEMP TABLE {table} AS SELECT 1 AS X DISTRIBUTE ON RANDOM"
    ))
    .unwrap();
    for _ in 0..10 {
        conn.batch_execute(&format!("INSERT INTO {table} SELECT X FROM {table}"))
            .unwrap();
    }
    let timed_out =
        conn.query_values_with_timeout(&slow_query(&table), &[], Some(Duration::from_millis(800)));
    assert!(
        matches!(timed_out, Err(NzError::Timeout(_))),
        "{timed_out:?}"
    );
    assert!(!conn.is_closed());
    assert_eq!(
        legacy_session_id(&mut conn),
        sid,
        "timeout replaced the session"
    );
    assert_eq!(conn.backend_process_id(), pid);
    assert!(conn
        .query("SELECT * FROM RUST_NO_SUCH_TABLE_X", &[])
        .is_err());
    assert_eq!(legacy_session_id(&mut conn), sid);
    conn.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires live Netezza appliance"]
async fn live_pool_session_identity_reuse_and_rotation() {
    // Reuse: max 1, same session every time.
    let mut config = nz_rust::PoolConfig::new(live_config());
    config.max = 1;
    let pool = nz_rust::Pool::new(config).unwrap();
    let mut seen = Vec::new();
    for _ in 0..3 {
        let lease = pool.get().await.unwrap();
        let rows = lease.query("SELECT CURRENT_SID", &[]).await.unwrap();
        seen.push(rows[0].try_values().unwrap()[0].to_display_string());
        lease.release().await;
    }
    assert!(
        seen.windows(2).all(|w| w[0] == w[1]),
        "pool did not reuse: {seen:?}"
    );
    pool.close().await;

    // max_uses = 2 rotates every second checkout.
    let mut config = nz_rust::PoolConfig::new(live_config());
    config.max = 1;
    config.max_uses = Some(2);
    let pool = nz_rust::Pool::new(config).unwrap();
    let mut seen = Vec::new();
    for _ in 0..4 {
        let lease = pool.get().await.unwrap();
        let rows = lease.query("SELECT CURRENT_SID", &[]).await.unwrap();
        seen.push(rows[0].try_values().unwrap()[0].to_display_string());
        lease.release().await;
    }
    assert_eq!(seen[0], seen[1]);
    assert_ne!(seen[1], seen[2], "max_uses did not rotate");
    assert_eq!(seen[2], seen[3]);
    pool.close().await;

    // max_lifetime rotates an expired session.
    let mut config = nz_rust::PoolConfig::new(live_config());
    config.max = 1;
    config.max_lifetime = Some(Duration::from_millis(500));
    config.idle_timeout = Duration::ZERO;
    let pool = nz_rust::Pool::new(config).unwrap();
    let lease = pool.get().await.unwrap();
    let first = lease.query("SELECT CURRENT_SID", &[]).await.unwrap()[0]
        .try_values()
        .unwrap()[0]
        .to_display_string();
    lease.release().await;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let lease = pool.get().await.unwrap();
    let second = lease.query("SELECT CURRENT_SID", &[]).await.unwrap()[0]
        .try_values()
        .unwrap()[0]
        .to_display_string();
    lease.release().await;
    assert_ne!(first, second, "max_lifetime did not rotate");
    pool.close().await;
}
