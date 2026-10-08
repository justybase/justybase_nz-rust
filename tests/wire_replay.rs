//! Replay of golden wire fixtures captured from a real appliance
//! (`tests/fixtures/wire/*.bin`, see its README for the format).
//!
//! Each fixture is served by the mock backend, record by record, under the
//! whole fragmentation matrix, and the decoded results are compared with
//! expectations written by hand from the SQL literals that produced them.
//! A missing or malformed fixture is a failure, not a skip.

mod support;

use nz_rust::{NzError, NzValue, QueryResult};
use std::path::PathBuf;
use std::sync::Arc;
use support::*;

type Records = Vec<(String, Vec<u8>)>;

fn load(name: &str) -> Records {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/wire")
        .join(format!("{name}.bin"));
    let data = std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    assert_eq!(&data[..8], b"NZWIRE01", "{name}: bad magic");
    let mut offset = 8;
    let mut u32_at = |data: &[u8], offset: &mut usize| {
        let value = u32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap()) as usize;
        *offset += 4;
        value
    };
    let count = u32_at(&data, &mut offset);
    let mut records = Vec::new();
    for _ in 0..count {
        let sql_len = u32_at(&data, &mut offset);
        let sql = String::from_utf8(data[offset..offset + sql_len].to_vec()).unwrap();
        offset += sql_len;
        let response_len = u32_at(&data, &mut offset);
        let response = data[offset..offset + response_len].to_vec();
        offset += response_len;
        assert_eq!(
            response[response.len() - 5],
            b'Z',
            "{name}: not ReadyForQuery-terminated"
        );
        records.push((sql, response));
    }
    assert_eq!(offset, data.len(), "{name}: trailing bytes");
    records
}

fn server_for(records: Records, chunking: Chunking) -> MockServer {
    let records = Arc::new(records);
    MockServer::start(
        HandshakeScript {
            chunking,
            ..Default::default()
        },
        move |session| {
            for (sql, response) in records.iter() {
                assert_eq!(session.read_query().as_deref(), Some(sql.as_str()));
                if session.send(response).is_err() {
                    return;
                }
            }
            // Keep the session open until the client hangs up.
            while session.read_query().is_some() {}
        },
    )
}

type Outcome = Result<QueryResult, NzError>;

async fn replay_native(records: &Records, chunking: Chunking) -> Vec<Outcome> {
    let server = server_for(records.clone(), chunking);
    let client = nz_rust::Client::connect(&server.config()).await.unwrap();
    let mut outcomes = Vec::new();
    for (sql, _) in records {
        outcomes.push(client.query_multi(sql, &[]).await);
    }
    client.close().await.unwrap();
    server.assert_no_handler_panics();
    outcomes
}

#[cfg(feature = "compat")]
fn replay_legacy(records: &Records, chunking: Chunking) -> Vec<Outcome> {
    let server = server_for(records.clone(), chunking);
    let mut conn = nz_rust::NzConnection::connect(&server.config()).unwrap();
    let outcomes = records
        .iter()
        .map(|(sql, _)| conn.query(sql, &[]))
        .collect();
    conn.close();
    server.assert_no_handler_panics();
    outcomes
}

/// Run `check` on the native result under every chunking, and on the legacy
/// result under a sample of them.
async fn replay_all(name: &str, check: impl Fn(&[Outcome], &str)) {
    let records = load(name);
    for chunking in Chunking::matrix() {
        let outcomes = replay_native(&records, chunking).await;
        check(&outcomes, &format!("{name} native {chunking:?}"));
    }
    #[cfg(feature = "compat")]
    for chunking in [Chunking::Whole, Chunking::Fixed(3), Chunking::Fixed(1)] {
        let records = records.clone();
        let outcomes = tokio::task::spawn_blocking(move || replay_legacy(&records, chunking))
            .await
            .unwrap();
        check(&outcomes, &format!("{name} legacy {chunking:?}"));
    }
}

fn rows(outcome: &Outcome, set: usize, label: &str) -> Vec<Vec<String>> {
    let result = outcome.as_ref().unwrap_or_else(|e| panic!("{label}: {e}"));
    result.result_sets[set]
        .rows
        .iter()
        .map(|row| {
            row.try_values()
                .unwrap_or_else(|e| panic!("{label}: {e}"))
                .iter()
                .map(NzValue::to_display_string)
                .collect()
        })
        .collect()
}

fn strs(row: &[&str]) -> Vec<String> {
    row.iter().map(|s| (*s).to_string()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_text_types() {
    replay_all("text_types", |outcomes, label| {
        assert_eq!(
            rows(&outcomes[0], 0, label),
            [strs(&[
                "-2147483648",
                "Zażółć gęślą jaźń",
                "3.1400",
                "2024-02-29",
                "2024-02-29 12:34:56.123456",
                "true",
                "1.5",
                "12:34:56",
            ])],
            "{label}"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_dbos_types() {
    replay_all("dbos_types", |outcomes, label| {
        assert!(
            outcomes[0].is_ok() && outcomes[1].is_ok() && outcomes[2].is_ok(),
            "{label}"
        );
        assert_eq!(outcomes[1].as_ref().unwrap().rows_affected, 1, "{label}");
        let result = outcomes[3].as_ref().unwrap();
        assert!(
            result.result_sets[0].nullability.is_some(),
            "{label}: not DBOS"
        );
        assert_eq!(
            rows(&outcomes[3], 0, label),
            [
                strs(&[
                    "-2147483648",
                    "Zażółć gęślą jaźń",
                    "3.1400",
                    "2024-02-29",
                    "2024-02-29 12:34:56.123456",
                    "true",
                    "1.5",
                    "12:34:56",
                    "-9223372036854775807",
                    "-32768",
                    "-128",
                ]),
                strs(&[
                    "2147483647",
                    "",
                    "-0.0001",
                    "0001-01-01",
                    "1999-12-31 23:59:59.999999",
                    "false",
                    "-2.5",
                    "00:00:00",
                    "9223372036854775807",
                    "32767",
                    "127",
                ]),
            ],
            "{label}"
        );
        // The empty VARCHAR is an empty string, not NULL.
        let row = &result.result_sets[0].rows[1];
        assert_eq!(row.try_get_value(1).unwrap(), &NzValue::Text(String::new()));
        let big: i64 = row.try_get(8).unwrap();
        assert_eq!(big, i64::MAX, "{label}");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_null_matrix() {
    replay_all("null_matrix", |outcomes, label| {
        let expected: Vec<String> = (0..33)
            .map(|i| match (i % 2, i % 3 == 2) {
                (0, _) => "NULL".to_string(),
                (_, false) => format!("{i}"),
                (_, true) => format!("v{i}"),
            })
            .collect();
        assert_eq!(
            rows(&outcomes[0], 0, label),
            [expected.clone()],
            "{label} text"
        );
        assert_eq!(rows(&outcomes[3], 0, label), [expected], "{label} dbos");
        let result = outcomes[3].as_ref().unwrap();
        for (i, null) in result.result_sets[0].rows[0]
            .try_values()
            .unwrap()
            .iter()
            .map(NzValue::is_null)
            .enumerate()
        {
            assert_eq!(null, i % 2 == 0, "{label}: column {i}");
        }
    })
    .await;
}

/// `(precision, scale, values as inserted)` in capture order.
const NUMERIC_CASES: [(u32, u32, &[&str]); 6] = [
    (1, 0, &["0", "9", "-9"]),
    (18, 0, &["0", "999999999999999999", "-999999999999999999"]),
    (38, 0, &["0", "99999999999999999999999999999999999999"]),
    (10, 4, &["0", "3.1400", "-0.0001", "999999.9999"]),
    (
        38,
        10,
        &["1.0000000001", "-12345678901234567890123456.7890123456"],
    ),
    (
        38,
        38,
        &["0.99999999999999999999999999999999999999", "-0.5"],
    ),
];

fn at_scale(value: &str, scale: u32) -> String {
    let negative = value.starts_with('-');
    let digits = value.trim_start_matches('-');
    let (int_part, frac) = digits.split_once('.').unwrap_or((digits, ""));
    let mut frac = frac.to_string();
    while (frac.len() as u32) < scale {
        frac.push('0');
    }
    let zero = int_part.bytes().all(|b| b == b'0') && frac.bytes().all(|b| b == b'0');
    let sign = if negative && !zero { "-" } else { "" };
    if scale == 0 {
        format!("{sign}{int_part}")
    } else {
        format!("{sign}{int_part}.{frac}")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_numeric_matrix() {
    replay_all("numeric_matrix", |outcomes, label| {
        let mut statement = 0;
        for (_, scale, values) in NUMERIC_CASES {
            statement += 1 + values.len(); // CREATE + INSERTs
            let expected: Vec<String> = values.iter().map(|v| at_scale(v, scale)).collect();
            // Binary path (SELECT from the TEMP table): exact, scale kept.
            let dbos = outcomes[statement].as_ref().unwrap();
            assert!(dbos.result_sets[0].nullability.is_some(), "{label}");
            for (row, want) in dbos.result_sets[0].rows.iter().zip(&expected) {
                let exact: nz_rust::NzNumeric = row.try_get(0).unwrap();
                assert_eq!(&exact.to_string(), want, "{label} dbos");
            }
            // Text path (SELECT of CASTs): exact unless surfaced as Float8.
            let text = outcomes[statement + 1].as_ref().unwrap();
            let row = &text.result_sets[0].rows[0];
            for (index, want) in expected.iter().enumerate() {
                match row.try_get_value(index).unwrap() {
                    NzValue::Float8(f) => {
                        assert_eq!(*f, want.parse::<f64>().unwrap(), "{label} text {want}")
                    }
                    other => assert_eq!(&other.to_display_string(), want, "{label} text"),
                }
            }
            statement += 2;
        }
        assert_eq!(statement, outcomes.len(), "{label}");
    })
    .await;
}

fn check_long(value: &str, length: usize, label: &str) {
    assert_eq!(value.len(), length, "{label}");
    assert!(value.starts_with('A') && value.ends_with('Z'), "{label}");
    assert!(value[1..length - 1].bytes().all(|b| b == b'x'), "{label}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_varchar_32767_boundaries() {
    replay_all("varchar_32767", |outcomes, label| {
        for (i, length) in [32766usize, 32767, 32768].into_iter().enumerate() {
            let text = rows(&outcomes[2 + 2 * i], 0, label);
            check_long(&text[0][0], length, &format!("{label} text {length}"));
        }
        let table = rows(outcomes.last().unwrap(), 0, label);
        assert_eq!(table.len(), 3);
        for (row, length) in table.iter().zip([32766usize, 32767, 32768]) {
            assert_eq!(row[0], length.to_string());
            check_long(&row[1], length, &format!("{label} dbos {length}"));
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_multi_result() {
    replay_all("multi_result", |outcomes, label| {
        let result = outcomes[0].as_ref().unwrap();
        assert_eq!(result.result_sets.len(), 3, "{label}");
        assert_eq!(rows(&outcomes[0], 0, label), [strs(&["1"])]);
        assert_eq!(rows(&outcomes[0], 1, label), [strs(&["x", "2"])]);
        assert_eq!(rows(&outcomes[0], 2, label), [strs(&["NULL"])]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_notices() {
    replay_all("notice", |outcomes, label| {
        for (index, text) in [
            (0, "ROLLBACK: no transaction in progress"),
            (1, "COMMIT: no transaction in progress"),
        ] {
            let result = outcomes[index].as_ref().unwrap();
            assert!(
                result.notices.iter().any(|n| n.contains(text)),
                "{label}: {:?}",
                result.notices
            );
        }
        assert!(outcomes[2].as_ref().unwrap().notices.is_empty(), "{label}");
        assert_eq!(rows(&outcomes[3], 0, label), [strs(&["1"])]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_errors_then_success_on_the_same_session() {
    replay_all("error", |outcomes, label| {
        match &outcomes[0] {
            Err(NzError::Database(db)) => {
                assert!(db.message.contains("found a keyword"), "{label}")
            }
            other => panic!("{label}: expected database error, got {other:?}"),
        }
        match &outcomes[1] {
            Err(NzError::Database(db)) => assert!(db.message.contains("Divide by 0"), "{label}"),
            other => panic!("{label}: expected database error, got {other:?}"),
        }
        assert_eq!(rows(&outcomes[2], 0, label), [strs(&["1"])]);
    })
    .await;
}

/// Every fixture response truncated at many cut points, followed by EOF, must
/// yield a clean error — never a panic, hang or bogus success.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncated_fixture_responses_fail_cleanly() {
    for name in [
        "text_types",
        "dbos_types",
        "null_matrix",
        "multi_result",
        "notice",
        "error",
    ] {
        let records = load(name);
        for (index, (_, response)) in records.iter().enumerate() {
            let step = (response.len() / 40).max(1);
            let cuts: Vec<usize> = (1..response.len().min(24))
                .chain((24..response.len()).step_by(step))
                .collect();
            for cut in cuts {
                let mut served: Records = records[..index].to_vec();
                served.push((records[index].0.clone(), response[..cut].to_vec()));
                let server = MockServer::start(HandshakeScript::default(), {
                    let served = Arc::new(served);
                    move |session| {
                        for (sql, response) in served.iter() {
                            assert_eq!(session.read_query().as_deref(), Some(sql.as_str()));
                            let _ = session.send(response);
                        }
                        // Dropping the session closes the socket mid-response.
                    }
                });
                let client = nz_rust::Client::connect(&server.config()).await.unwrap();
                let mut failed = false;
                for (sql, _) in &records[..=index] {
                    let outcome = tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        client.query_multi(sql, &[]),
                    )
                    .await
                    .unwrap_or_else(|_| panic!("{name}[{index}] cut {cut}: client hung"));
                    if let Err(error) = outcome {
                        if sql == &records[index].0 {
                            assert!(
                                matches!(
                                    error,
                                    NzError::Closed(_) | NzError::Io(_) | NzError::Protocol(_)
                                ),
                                "{name}[{index}] cut {cut}: {error:?}"
                            );
                            failed = true;
                        }
                    }
                }
                assert!(
                    failed,
                    "{name}[{index}] cut {cut}: truncated response accepted"
                );
                server.assert_no_handler_panics();
            }
        }
    }
}
