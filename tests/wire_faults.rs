//! Protocol-fault classification and poisoned-connection handling.
//!
//! A SQL error reported through a well-formed ErrorResponse + ReadyForQuery
//! leaves the session synchronized and reusable. A framing fault (bad length,
//! truncated frame, out-of-order message, EOF mid-frame) leaves the socket in
//! an unknown position: the connection must become unusable, must never
//! receive another query, and a pool must replace it with a new physical
//! connection.

mod support;

use nz_rust::NzError;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::*;

/// A malformed backend response. `close` hangs up right after `bytes`.
struct Fault {
    name: &'static str,
    bytes: Vec<u8>,
    close: bool,
}

fn faults() -> Vec<Fault> {
    let two_columns = row_description(&[("ONE", OID_INT4, 4), ("TXT", OID_VARCHAR, -1)]);
    let with_columns = |tail: Vec<u8>| {
        let mut wire = two_columns.clone();
        wire.extend(tail);
        wire
    };
    let dbos_prefix = || {
        let layout = DbosLayout {
            kinds: vec![DbosKind::Int4, DbosKind::Varchar(8)],
            phys: vec![0, 1],
            nulls_allowed: true,
        };
        let mut wire = two_columns.clone();
        wire.extend(dbos_descriptor(&layout));
        wire
    };
    let mut bad_cell = vec![0b1000_0000];
    bad_cell.extend_from_slice(&2i32.to_be_bytes()); // smaller than its own prefix
    let mut overlong_cell = vec![0b1000_0000];
    overlong_cell.extend_from_slice(&1_000i32.to_be_bytes());
    overlong_cell.extend_from_slice(b"12");
    // DBOS row: 2 reserved + 1 bitmap + int4 + varying length 0xFFFF.
    let mut bad_varying = vec![0, 0, 0];
    bad_varying.extend_from_slice(&5i32.to_le_bytes());
    bad_varying.extend_from_slice(&0xFFFFu16.to_le_bytes());
    let mut negative_dbos_row = vec![b'Y', 0, 0, 0, 0];
    negative_dbos_row.extend_from_slice(&0i32.to_be_bytes());
    negative_dbos_row.extend_from_slice(&(-5i32).to_be_bytes());

    vec![
        Fault {
            name: "row-description-negative-length",
            bytes: frame_header(b'T', -1),
            close: false,
        },
        Fault {
            name: "row-description-zero-length",
            bytes: frame_header(b'T', 0),
            close: false,
        },
        Fault {
            name: "data-row-length-max-plus-one",
            bytes: with_columns(frame_header(b'D', nz_rust::error::MAX_PROTOCOL_PAYLOAD + 1)),
            close: false,
        },
        Fault {
            name: "data-row-length-i32-max",
            bytes: with_columns(frame_header(b'D', i32::MAX)),
            close: false,
        },
        Fault {
            name: "notice-length-2-billion",
            bytes: frame_header(b'N', 2_000_000_000),
            close: false,
        },
        Fault {
            name: "error-length-negative",
            bytes: frame_header(b'E', i32::MIN),
            close: false,
        },
        Fault {
            name: "command-complete-length-i32-max",
            bytes: frame_header(b'C', i32::MAX),
            close: false,
        },
        Fault {
            name: "data-row-cell-length-below-prefix",
            bytes: with_columns(frame(b'D', &bad_cell)),
            close: false,
        },
        Fault {
            name: "data-row-cell-length-beyond-frame",
            bytes: with_columns(frame(b'D', &overlong_cell)),
            close: false,
        },
        Fault {
            name: "data-row-before-row-description",
            bytes: text_row(&[Some(b"1")]),
            close: false,
        },
        Fault {
            name: "dbos-row-before-descriptor",
            bytes: with_columns(dbos_row_frame(&[0, 0, 0, 1, 0, 0, 0])),
            close: false,
        },
        Fault {
            name: "dbos-row-negative-length",
            bytes: {
                let mut wire = dbos_prefix();
                wire.extend(negative_dbos_row.clone());
                wire
            },
            close: false,
        },
        Fault {
            name: "dbos-varying-length-beyond-row",
            bytes: {
                let mut wire = dbos_prefix();
                wire.extend(dbos_row_frame(&bad_varying));
                wire
            },
            close: false,
        },
        Fault {
            name: "dbos-descriptor-field-count-out-of-range",
            bytes: {
                let mut payload = vec![0u8; 32];
                payload.extend_from_slice(&1_000_000i32.to_be_bytes());
                with_columns(frame(b'X', &payload))
            },
            close: false,
        },
        Fault {
            name: "truncated-frame-then-eof",
            bytes: {
                let mut wire = with_columns(frame_header(b'D', 100));
                wire.extend_from_slice(&[0x80, 0, 0, 0]);
                wire
            },
            close: true,
        },
        Fault {
            name: "eof-inside-frame-header",
            bytes: vec![b'T', 0, 0],
            close: true,
        },
        Fault {
            name: "eof-before-any-response",
            bytes: Vec::new(),
            close: true,
        },
    ]
}

/// Records every query each physical connection receives.
type QueryLog = Arc<Mutex<Vec<(usize, String)>>>;

fn start_fault_server() -> (MockServer, QueryLog) {
    let log: QueryLog = Arc::default();
    let server_log = log.clone();
    let server = MockServer::start(HandshakeScript::default(), move |session| {
        while let Some(sql) = session.read_query() {
            server_log
                .lock()
                .unwrap()
                .push((session.index, sql.clone()));
            if let Some(name) = sql.strip_prefix("SELECT fault ") {
                let fault = faults()
                    .into_iter()
                    .find(|fault| fault.name == name)
                    .expect("known fault");
                if session.send(&fault.bytes).is_err() || fault.close {
                    return;
                }
                // Keep the socket open: a poisoned client must hang up rather
                // than send another query on it.
                continue;
            }
            let response = match sql.as_str() {
                "SELECT sql_error" => {
                    let mut wire = error("42S02", "relation does not exist");
                    wire.extend(ready());
                    wire
                }
                "ROLLBACK" => simple_command("ROLLBACK"),
                _ => select_one(),
            };
            if session.send(&response).is_err() {
                return;
            }
        }
    });
    (server, log)
}

fn queries_on(log: &QueryLog, index: usize) -> Vec<String> {
    log.lock()
        .unwrap()
        .iter()
        .filter(|(i, _)| *i == index)
        .map(|(_, sql)| sql.clone())
        .collect()
}

fn assert_fault_error(name: &str, error: &NzError) {
    assert!(
        matches!(
            error,
            NzError::Protocol(_) | NzError::Closed(_) | NzError::Io(_)
        ),
        "{name}: expected Protocol/Closed/Io, got {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protocol_fault_marks_connection_unusable() {
    let (server, log) = start_fault_server();
    for (n, fault) in faults().into_iter().enumerate() {
        let index = n + 1;
        let client = nz_rust::Client::connect(&server.config()).await.unwrap();
        let sql = format!("SELECT fault {}", fault.name);
        let result = tokio::time::timeout(Duration::from_secs(10), client.query(&sql, &[]))
            .await
            .unwrap_or_else(|_| panic!("{}: client hung on malformed input", fault.name));
        assert_fault_error(fault.name, &result.unwrap_err());
        // The next request must fail without touching the socket.
        let next = tokio::time::timeout(Duration::from_secs(10), client.query("SELECT 1", &[]))
            .await
            .unwrap_or_else(|_| panic!("{}: request after the fault hung", fault.name));
        assert!(next.is_err(), "{}: poisoned client was reused", fault.name);
        assert!(
            wait_until(Duration::from_secs(5), || client.is_closed()),
            "{}: client not closed",
            fault.name
        );
        assert_eq!(queries_on(&log, index), [sql], "{}", fault.name);
        assert_eq!(server.accepted(), index);
    }
    server.assert_no_handler_panics();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn database_error_keeps_connection_reusable() {
    let (server, log) = start_fault_server();
    let client = nz_rust::Client::connect(&server.config()).await.unwrap();
    for _ in 0..3 {
        match client.query("SELECT sql_error", &[]).await {
            Err(NzError::Database(db)) => assert_eq!(db.code.as_deref(), Some("42S02")),
            other => panic!("expected database error, got {other:?}"),
        }
        assert_eq!(client.query("SELECT ok", &[]).await.unwrap().len(), 1);
        assert!(!client.is_closed());
    }
    assert_eq!(server.accepted(), 1);
    assert_eq!(queries_on(&log, 1).len(), 6);

    let mut options = nz_rust::PoolConfig::new(server.config());
    options.max = 1;
    let pool = nz_rust::Pool::new(options).unwrap();
    for _ in 0..3 {
        let lease = pool.get().await.unwrap();
        assert!(matches!(
            lease.query("SELECT sql_error", &[]).await,
            Err(NzError::Database(_))
        ));
        lease.release().await;
    }
    assert_eq!(server.accepted(), 2, "pool reused one physical connection");
    pool.close().await;
    server.assert_no_handler_panics();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protocol_fault_async_pool_retires_connection() {
    let (server, log) = start_fault_server();
    let mut options = nz_rust::PoolConfig::new(server.config());
    options.max = 1;
    options.wait_timeout = Some(Duration::from_secs(10));
    let pool = nz_rust::Pool::new(options).unwrap();
    for (n, fault) in faults().into_iter().enumerate() {
        let lease = pool.get().await.unwrap();
        let sql = format!("SELECT fault {}", fault.name);
        let error = lease.query(&sql, &[]).await.unwrap_err();
        assert_fault_error(fault.name, &error);
        lease.release().await;
        assert_eq!(
            pool.total_count().await,
            0,
            "{}: faulted lease kept",
            fault.name
        );
        // The next checkout is served by a fresh physical connection.
        let lease = pool.get().await.unwrap();
        assert_eq!(lease.query("SELECT ok", &[]).await.unwrap().len(), 1);
        lease.release().await;
        let faulted_index = 2 * n + 1;
        assert_eq!(queries_on(&log, faulted_index), [sql], "{}", fault.name);
        assert_eq!(server.accepted(), 2 * n + 2, "{}", fault.name);
        // Retire the healthy connection so the next fault gets a fresh index.
        let lease = pool.get().await.unwrap();
        let _ = lease
            .query("SELECT fault eof-before-any-response", &[])
            .await;
        lease.release().await;
        assert_eq!(server.accepted(), 2 * n + 2);
        assert_eq!(pool.total_count().await, 0);
    }
    pool.close().await;
    server.assert_no_handler_panics();
}

#[test]
fn protocol_fault_blocking_pool_retires_connection() {
    let (server, _log) = start_fault_server();
    let mut options = nz_rust::PoolConfig::new(server.config());
    options.max = 1;
    let pool = nz_rust::blocking::Pool::connect(options).unwrap();
    let mut conn = pool.get().unwrap();
    assert_fault_error(
        "blocking",
        &conn
            .query("SELECT fault data-row-length-i32-max", &[])
            .unwrap_err(),
    );
    drop(conn);
    let mut conn = pool.get().unwrap();
    assert_eq!(conn.query("SELECT ok", &[]).unwrap().len(), 1);
    drop(conn);
    assert_eq!(server.accepted(), 2);
    pool.close();
    server.assert_no_handler_panics();
}

/// Regression: a request issued while the driver task was shutting down
/// after a fatal error could be enqueued after the task's last read and then
/// wait forever for a response. Every such request must fail promptly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requests_racing_driver_shutdown_fail_promptly() {
    let server = MockServer::start(HandshakeScript::default(), |session| {
        // Read the first query, then hang up without answering.
        let _ = session.read_query();
    });
    for iteration in 0..500 {
        let client = nz_rust::Client::connect(&server.config()).await.unwrap();
        assert!(client.query("SELECT 1", &[]).await.is_err());
        let next = async {
            if iteration % 2 == 0 {
                client.query("SELECT 2", &[]).await.map(|_| ())
            } else {
                match client.query_stream("SELECT 2", &[]).await {
                    Ok(mut stream) => match std::future::poll_fn(|cx| {
                        futures_core::Stream::poll_next(std::pin::Pin::new(&mut stream), cx)
                    })
                    .await
                    {
                        Some(Ok(_)) => Ok(()),
                        Some(Err(error)) => Err(error),
                        None => Err(NzError::Closed("stream ended".into())),
                    },
                    Err(error) => Err(error),
                }
            }
        };
        let outcome = tokio::time::timeout(Duration::from_secs(10), next)
            .await
            .unwrap_or_else(|_| panic!("iteration {iteration}: request hung after driver exit"));
        assert!(outcome.is_err(), "iteration {iteration}: {outcome:?}");
    }
    server.assert_no_handler_panics();
}

/// Native counterpart of the legacy mid-payload timeout regression: the
/// native driver keeps parsing the interrupted frame, so payload bytes that
/// resemble `ReadyForQuery` can never be mistaken for message boundaries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_timeout_inside_payload_never_resyncs_into_stale_rows() {
    let server = MockServer::start(HandshakeScript::default(), |session| {
        assert_eq!(session.read_query().as_deref(), Some("SELECT slow"));
        let mut payload = ready();
        payload.extend(row_description(&[("ONE", OID_INT4, 4)]));
        payload.extend(text_row(&[Some(b"666")]));
        payload.extend(command_complete("SELECT 1"));
        payload.extend(ready());
        let mut wire = row_description(&[("BLOB", OID_VARCHAR, -1)]);
        wire.extend(frame_header(b'D', payload.len() as i32));
        session.send(&wire).unwrap();
        assert!(session.cancels.wait_for(1, Duration::from_secs(10)));
        let mut tail = payload;
        tail.extend(cancelled_response());
        session.send(&tail).unwrap();
        session.serve_all(|sql| {
            assert_eq!(sql, "SELECT recovered");
            select_one()
        });
    });
    let client = nz_rust::Client::connect(&server.config()).await.unwrap();
    let timed_out = client
        .query_with_options(
            "SELECT slow",
            &[],
            nz_rust::QueryOptions {
                timeout: Some(Duration::from_millis(200)),
            },
        )
        .await;
    assert!(
        matches!(timed_out, Err(NzError::Timeout(_))),
        "{timed_out:?}"
    );
    for _ in 0..2 {
        match client.query("SELECT recovered", &[]).await {
            Ok(rows) => assert_eq!(
                rows[0].try_values().unwrap(),
                [
                    nz_rust::NzValue::Int4(1),
                    nz_rust::NzValue::Text("row".into())
                ],
                "stale rows from the cancelled statement"
            ),
            Err(error) => {
                assert_fault_error("native-timeout-resync", &error);
                break;
            }
        }
    }
    server.assert_no_handler_panics();
}

#[cfg(feature = "compat")]
mod legacy {
    use super::*;

    fn legacy_config(server: &MockServer) -> nz_rust::NzConnectionConfig {
        let mut config = server.config();
        // Bound every wait so a regression fails instead of hanging.
        config.command_timeout = 10;
        config
    }

    #[test]
    fn legacy_protocol_fault_marks_connection_unusable() {
        let (server, log) = start_fault_server();
        for (n, fault) in faults().into_iter().enumerate() {
            let mut conn = nz_rust::NzConnection::connect(&legacy_config(&server)).unwrap();
            let sql = format!("SELECT fault {}", fault.name);
            assert_fault_error(fault.name, &conn.query(&sql, &[]).unwrap_err());
            assert!(
                conn.is_closed(),
                "{}: legacy connection still open",
                fault.name
            );
            assert!(
                conn.query("SELECT 1", &[]).is_err(),
                "{}: poisoned legacy connection was reused",
                fault.name
            );
            assert_eq!(queries_on(&log, n + 1), [sql], "{}", fault.name);
        }
        server.assert_no_handler_panics();
    }

    #[test]
    fn legacy_database_error_keeps_connection_reusable() {
        let (server, _log) = start_fault_server();
        let mut conn = nz_rust::NzConnection::connect(&legacy_config(&server)).unwrap();
        for _ in 0..3 {
            assert!(matches!(
                conn.query("SELECT sql_error", &[]),
                Err(NzError::Database(_))
            ));
            assert!(!conn.is_closed());
            assert_eq!(
                conn.query("SELECT ok", &[]).unwrap().result_sets[0]
                    .rows
                    .len(),
                1
            );
        }
        assert_eq!(server.accepted(), 1);
    }

    #[test]
    fn protocol_fault_pool_retires_connection() {
        let (server, log) = start_fault_server();
        let mut options = nz_rust::NzPoolConfig::new(legacy_config(&server));
        options.max = 1;
        options.wait_timeout = Some(Duration::from_secs(10));
        let pool = nz_rust::NzPool::new(options).unwrap();
        for (n, fault) in faults().into_iter().enumerate() {
            let sql = format!("SELECT fault {}", fault.name);
            {
                // Through the guard (Deref), so the pool only sees the
                // connection state, not the error value.
                let mut conn = pool.get().unwrap();
                assert_fault_error(fault.name, &conn.query(&sql, &[]).unwrap_err());
            }
            assert_eq!(
                pool.total_count(),
                0,
                "{}: faulted connection pooled",
                fault.name
            );
            {
                let mut conn = pool.get().unwrap();
                assert_eq!(
                    conn.query("SELECT ok", &[]).unwrap().result_sets[0]
                        .rows
                        .len(),
                    1
                );
            }
            assert_eq!(queries_on(&log, 2 * n + 1), [sql], "{}", fault.name);
            {
                let mut conn = pool.get().unwrap();
                let _ = conn.query("SELECT fault eof-before-any-response", &[]);
            }
            assert_eq!(server.accepted(), 2 * n + 2, "{}", fault.name);
        }
        server.assert_no_handler_panics();
    }

    /// Regression for a legacy resynchronization hazard: a command timeout
    /// that fires after a frame header was consumed but before its payload
    /// arrived leaves the read position *inside* the payload. Treating the
    /// payload bytes as message boundaries can mistake attacker- or
    /// data-controlled bytes for `ReadyForQuery` and hand stale rows to the
    /// next query. The connection must either return the correct result or
    /// fail — never data from the cancelled statement.
    #[test]
    fn legacy_timeout_inside_payload_never_resyncs_into_stale_rows() {
        let server = MockServer::start(HandshakeScript::default(), |session| {
            assert_eq!(session.read_query().as_deref(), Some("SELECT slow"));
            // A payload whose first bytes look like ReadyForQuery followed by
            // a complete, plausible response carrying the value 666.
            let mut payload = ready();
            payload.extend(row_description(&[("ONE", OID_INT4, 4)]));
            payload.extend(text_row(&[Some(b"666")]));
            payload.extend(command_complete("SELECT 1"));
            payload.extend(ready());
            let mut wire = row_description(&[("BLOB", OID_VARCHAR, -1)]);
            wire.extend(frame_header(b'D', payload.len() as i32));
            session.send(&wire).unwrap();
            assert!(session.cancels.wait_for(1, Duration::from_secs(10)));
            let mut tail = payload;
            tail.extend(cancelled_response());
            session.send(&tail).unwrap();
            while let Some(sql) = session.read_query() {
                assert_eq!(sql, "SELECT recovered");
                if session.send(&select_one()).is_err() {
                    return;
                }
            }
        });
        let mut conn = nz_rust::NzConnection::connect(&server.config()).unwrap();
        let timed_out =
            conn.query_values_with_timeout("SELECT slow", &[], Some(Duration::from_millis(200)));
        assert!(
            matches!(timed_out, Err(NzError::Timeout(_))),
            "{timed_out:?}"
        );
        for _ in 0..2 {
            match conn.query("SELECT recovered", &[]) {
                Ok(result) => {
                    let values = result.result_sets[0].rows[0].try_values().unwrap().to_vec();
                    assert_eq!(
                        values,
                        [
                            nz_rust::NzValue::Int4(1),
                            nz_rust::NzValue::Text("row".into())
                        ],
                        "stale rows from the cancelled statement"
                    );
                }
                Err(error) => {
                    assert_fault_error("timeout-resync", &error);
                    assert!(conn.is_closed());
                    break;
                }
            }
        }
        server.assert_no_handler_panics();
    }
}
